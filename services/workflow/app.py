"""
Workflow State Machine Service
Manages lot progression, status queries, and Kafka event publishing.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

# Kafka producer for event streaming
kafka_producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

def publish_certification_event(lot_id, factory_node_id, from_role, to_role, certificate_id):
    """Publish certification event to Kafka topic"""
    try:
        topic = f"lot.{factory_node_id}.certified"
        event = {
            'lot_id': str(lot_id),
            'from_role': from_role,
            'to_role': to_role,
            'certificate_id': str(certificate_id),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        kafka_producer.send(topic, key=str(lot_id), value=event)
        kafka_producer.flush(timeout=5)
    except KafkaError as e:
        app.logger.error(f"Failed to publish Kafka event: {e}")
        # In production, this should trigger alerting

@app.route('/api/v1/lots/<lot_id>/status', methods=['GET'])
@jwt_required()
def get_lot_status(lot_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                pl.id AS lot_id,
                pl.lot_number,
                pl.asset_id,
                pl.status AS current_status,
                pl.factory_node_id,
                r.name AS current_role,
                u.full_name AS locked_by_user,
                ws.entry_timestamp AS role_entry_time,
                ws.exit_timestamp AS role_exit_time,
                aic.step_name AS last_certification_step,
                aic.verdict AS last_verdict,
                aic.confidence_score,
                aic.created_at AS certification_time
            FROM production_lots pl
            LEFT JOIN roles r ON pl.current_role_id = r.id
            LEFT JOIN users u ON pl.locked_by = u.id
            LEFT JOIN workflow_states ws ON pl.id = ws.lot_id AND ws.exit_timestamp IS NULL
            LEFT JOIN LATERAL (
                SELECT * FROM ai_inspection_certificates 
                WHERE lot_id = pl.id 
                ORDER BY created_at DESC 
                LIMIT 1
            ) aic ON TRUE
            WHERE pl.id = %s::uuid
        """, (lot_id,))
        
        lot = cur.fetchone()
        cur.close()
        conn.close()
        
        if not lot:
            return jsonify({'error': 'LotNotFound'}), 404
        
        return jsonify({
            'lot_id': str(lot['lot_id']),
            'lot_number': lot['lot_number'],
            'asset_id': lot['asset_id'],
            'current_status': lot['current_status'],
            'factory_node_id': lot['factory_node_id'],
            'current_role': lot['current_role'],
            'locked_by_user': lot['locked_by_user'],
            'role_entry_time': lot['role_entry_time'].isoformat() if lot['role_entry_time'] else None,
            'role_exit_time': lot['role_exit_time'].isoformat() if lot['role_exit_time'] else None,
            'last_certification_step': lot['last_certification_step'],
            'last_verdict': lot['last_verdict'],
            'confidence_score': float(lot['confidence_score']) if lot['confidence_score'] else None,
            'certification_time': lot['certification_time'].isoformat() if lot['certification_time'] else None
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/dashboard/input-queue', methods=['GET'])
@jwt_required()
def get_input_queue():
    try:
        operator_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get operator's role and factory
        cur.execute(
            "SELECT role_id, factory_node_id FROM users WHERE id = %s::uuid",
            (operator_id,)
        )
        operator = cur.fetchone()
        
        if not operator:
            return jsonify({'error': 'OperatorNotFound'}), 404
        
        # Get lots queued for this role
        cur.execute("""
            SELECT 
                pl.id AS lot_id,
                pl.lot_number,
                pl.asset_id,
                pl.status,
                pl.priority,
                pl.created_at,
                r.name AS role_name
            FROM production_lots pl
            JOIN roles r ON pl.current_role_id = r.id
            WHERE pl.factory_node_id = %s
              AND pl.current_role_id = %s::uuid
              AND pl.status LIKE 'Queued:%'
            ORDER BY pl.priority DESC, pl.created_at ASC
        """, (operator['factory_node_id'], operator['role_id']))
        
        lots = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'factory_node_id': operator['factory_node_id'],
            'role_id': str(operator['role_id']),
            'queue_length': len(lots),
            'lots': [
                {
                    'lot_id': str(lot['lot_id']),
                    'lot_number': lot['lot_number'],
                    'asset_id': lot['asset_id'],
                    'status': lot['status'],
                    'priority': lot['priority'],
                    'created_at': lot['created_at'].isoformat(),
                    'role_name': lot['role_name']
                }
                for lot in lots
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/lots/<lot_id>/override', methods=['POST'])
@jwt_required()
def override_quarantine(lot_id):
    try:
        data = request.get_json()
        action = data.get('action')  # 'APPROVE', 'REWORK', 'REJECT'
        notes = data.get('notes', '')
        
        if action not in ['APPROVE', 'REWORK', 'REJECT']:
            return jsonify({'error': 'InvalidAction', 'message': 'Action must be APPROVE, REWORK, or REJECT'}), 400
        
        supervisor_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        # Call supervisor override function
        cur.execute(
            "SELECT sp_supervisor_override_quarantine(%s::uuid, %s::uuid, %s, %s)",
            (lot_id, supervisor_id, action, notes)
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Quarantine override successful: {action}'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/lots', methods=['GET'])
@jwt_required()
def list_lots():
    try:
        operator_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        query = """
            SELECT 
                pl.id AS lot_id,
                pl.lot_number,
                pl.asset_id,
                pl.status,
                pl.priority,
                pl.created_at,
                pl.completed_at,
                r.name AS current_role,
                fn.name AS factory_name
            FROM production_lots pl
            LEFT JOIN roles r ON pl.current_role_id = r.id
            LEFT JOIN factory_nodes fn ON pl.factory_node_id = fn.id
            WHERE 1=1
        """
        params = []
        
        if status_filter:
            query += " AND pl.status = %s"
            params.append(status_filter)
        
        query += " ORDER BY pl.created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        lots = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(lots),
            'limit': limit,
            'offset': offset,
            'lots': [
                {
                    'lot_id': str(lot['lot_id']),
                    'lot_number': lot['lot_number'],
                    'asset_id': lot['asset_id'],
                    'status': lot['status'],
                    'priority': lot['priority'],
                    'created_at': lot['created_at'].isoformat(),
                    'completed_at': lot['completed_at'].isoformat() if lot['completed_at'] else None,
                    'current_role': lot['current_role'],
                    'factory_name': lot['factory_name']
                }
                for lot in lots
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# ASSISTANT WEAVER MODULE
# ============================================================

def validate_assistant_guardrails(data):
    """Validate assistant weaver inputs against business guardrails."""
    errors = []
    
    hook_count = data.get('hook_count')
    mending_knot_type = data.get('mending_knot_type')
    comber_board_cleaning_status = data.get('comber_board_cleaning_status')
    yarn_tail_transfer_status = data.get('yarn_tail_transfer_status')
    weft_feeder_position = data.get('weft_feeder_position')
    dropper_rethread_verification = data.get('dropper_rethread_verification')
    assistant_weaver_approval_state = data.get('assistant_weaver_approval_state')
    
    if hook_count == 2400:
        if mending_knot_type == 'Standard Overhand Knot':
            errors.append({
                'code': 'INVALID_KNOT_TYPE_WARNING',
                'message': 'STANDARD_KNOTS_WILL_CATCH_IN_2400_HOOK_REED_DENTS'
            })
        if comber_board_cleaning_status != 'Cleaned / Compressed Air Blowout Done':
            errors.append({
                'code': 'CLEANING_REQUIRED',
                'message': '2400_HOOK_LINE_REQUIRES_CLEANED_COMPRESSED_AIR_BLOWOUT'
            })
    
    if yarn_tail_transfer_status == 'Unverified / Loose Tail' and weft_feeder_position == 'Feeder 2 (Zari Extra Weft)':
        errors.append({
            'code': 'REJECT_LOOSE_TAIL',
            'message': 'LOOSE_ZARI_TAILS_WILL_CAUSE_WEFT_STOPS_DURING_PALLU_WEAVING'
        })
    
    if dropper_rethread_verification == 'Bypassed Dropper (Unsafe)':
        errors.append({
            'code': 'SAFETY_VIOLATION',
            'message': 'ALL_WARP_ENDS_MUST_BE_THREADED_THROUGH_ACTIVE_DROPPERS'
        })
    
    if assistant_weaver_approval_state != 'PASSED_SHIFT_AUDIT':
        errors.append({
            'code': 'DENY_SHIFT_HANDOVER_CLEARANCE',
            'message': 'SHIFT_HANDOVER_CLEARANCE_DENIED'
        })
    
    return errors

@app.route('/api/v1/assistant-weaver/logs', methods=['POST'])
@jwt_required()
def create_assistant_log():
    try:
        assistant_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = [
            'active_loom_id', 'pirns_replaced_count', 'logged_warp_breaks',
            'logged_weft_breaks', 'shift_start_time', 'shift_end_time'
        ]
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (assistant_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors = validate_assistant_guardrails(data)
        
        assistant_job_log_id = f"AWL-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO assistant_weaver_job_logs (
                assistant_job_log_id, active_loom_id, assistant_weaver_id, lead_weaver_id,
                factory_node_id, shift_start_time, shift_end_time,
                pirns_replaced_count, logged_warp_breaks, logged_weft_breaks,
                weft_spool_lot_id, weft_feeder_position, yarn_tail_transfer_status,
                zari_tension_disc_setting, warp_break_repair_count, mending_knot_type,
                dropper_rethread_verification, comber_board_cleaning_status,
                shift_handover_readiness, approval_state, validation_errors
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, assistant_job_log_id
        """, (
            assistant_job_log_id,
            data.get('active_loom_id'),
            assistant_id,
            data.get('lead_weaver_id'),
            factory_node_id,
            data.get('shift_start_time'),
            data.get('shift_end_time'),
            data.get('pirns_replaced_count', 0),
            data.get('logged_warp_breaks', 0),
            data.get('logged_weft_breaks', 0),
            data.get('weft_spool_lot_id'),
            data.get('weft_feeder_position', 'Feeder 1 (Ground Silk)'),
            data.get('yarn_tail_transfer_status', 'Spliced & Tail-Locked'),
            data.get('zari_tension_disc_setting', 'Micro-Tension Active (Fine Zari)'),
            data.get('warp_break_repair_count', 0),
            data.get('mending_knot_type', 'Weaver''s Micro-Knot (Short Tail)'),
            data.get('dropper_rethread_verification', 'Threaded & Dropper Active'),
            data.get('comber_board_cleaning_status', 'Cleaned / Compressed Air Blowout Done'),
            data.get('shift_handover_readiness', 'READY_FOR_NEXT_SHIFT'),
            'ACTIVE_LOGGING' if validation_errors else 'REJECTED_UNRESOLVED_WARP_BREAKS',
            json.dumps(validation_errors)
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        log_ref = log_row['assistant_job_log_id']
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'SUBMITTED', %s, %s)
        """, (log_id, assistant_id, f"Initial submission with {len(validation_errors)} validation errors"))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'log_id': str(log_id),
            'assistant_job_log_id': log_ref,
            'approval_state': 'ACTIVE_LOGGING' if not validation_errors else 'REJECTED_UNRESOLVED_WARP_BREAKS',
            'validation_errors': validation_errors,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/logs', methods=['GET'])
@jwt_required()
def list_assistant_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT awl.id, awl.assistant_job_log_id, awl.active_loom_id, awl.factory_node_id,
                   awl.shift_start_time, awl.shift_end_time, awl.approval_state,
                   awl.pirns_replaced_count, awl.logged_warp_breaks, awl.logged_weft_breaks,
                   awl.lead_weaver_signoff,
                   u_assistant.full_name AS assistant_name,
                   u_lead.full_name AS lead_name
            FROM assistant_weaver_job_logs awl
            LEFT JOIN users u_assistant ON awl.assistant_weaver_id = u_assistant.id
            LEFT JOIN users u_lead ON awl.lead_weaver_id = u_lead.id
            WHERE awl.assistant_weaver_id = %s::uuid
               OR awl.lead_weaver_id = %s::uuid
            ORDER BY awl.shift_start_time DESC
            LIMIT 100
        """, (operator_id, operator_id))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'assistant_job_log_id': log['assistant_job_log_id'],
                    'active_loom_id': log['active_loom_id'],
                    'factory_node_id': log['factory_node_id'],
                    'shift_start_time': log['shift_start_time'].isoformat() if log['shift_start_time'] else None,
                    'shift_end_time': log['shift_end_time'].isoformat() if log['shift_end_time'] else None,
                    'approval_state': log['approval_state'],
                    'pirns_replaced_count': log['pirns_replaced_count'],
                    'logged_warp_breaks': log['logged_warp_breaks'],
                    'logged_weft_breaks': log['logged_weft_breaks'],
                    'lead_weaver_signoff': log['lead_weaver_signoff'],
                    'assistant_name': log['assistant_name'],
                    'lead_name': log['lead_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_assistant_log(log_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT awl.*,
                   u_assistant.full_name AS assistant_name,
                   u_lead.full_name AS lead_name
            FROM assistant_weaver_job_logs awl
            LEFT JOIN users u_assistant ON awl.assistant_weaver_id = u_assistant.id
            LEFT JOIN users u_lead ON awl.lead_weaver_id = u_lead.id
            WHERE awl.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'assistant_job_log_id': log['assistant_job_log_id'],
            'active_loom_id': log['active_loom_id'],
            'assistant_weaver_id': str(log['assistant_weaver_id']),
            'lead_weaver_id': str(log['lead_weaver_id']) if log['lead_weaver_id'] else None,
            'factory_node_id': log['factory_node_id'],
            'shift_start_time': log['shift_start_time'].isoformat() if log['shift_start_time'] else None,
            'shift_end_time': log['shift_end_time'].isoformat() if log['shift_end_time'] else None,
            'pirns_replaced_count': log['pirns_replaced_count'],
            'logged_warp_breaks': log['logged_warp_breaks'],
            'logged_weft_breaks': log['logged_weft_breaks'],
            'lead_weaver_signoff': log['lead_weaver_signoff'],
            'approval_state': log['approval_state'],
            'validation_errors': log['validation_errors'],
            'weft_feeder_position': log['weft_feeder_position'],
            'yarn_tail_transfer_status': log['yarn_tail_transfer_status'],
            'zari_tension_disc_setting': log['zari_tension_disc_setting'],
            'warp_break_repair_count': log['warp_break_repair_count'],
            'mending_knot_type': log['mending_knot_type'],
            'dropper_rethread_verification': log['dropper_rethread_verification'],
            'comber_board_cleaning_status': log['comber_board_cleaning_status'],
            'shift_handover_readiness': log['shift_handover_readiness'],
            'assistant_name': log['assistant_name'],
            'lead_name': log['lead_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/logs/<log_id>/approve', methods=['POST'])
@jwt_required()
def approve_assistant_log(log_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, approval_state, lead_weaver_id
            FROM assistant_weaver_job_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['lead_weaver_id'] and str(log['lead_weaver_id']) != operator_id:
            cur.execute("""
                SELECT role_id FROM users WHERE id = %s::uuid
            """, (operator_id,))
            operator = cur.fetchone()
            if not operator or operator['role_id'] != 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR':
                cur.close()
                conn.close()
                return jsonify({'error': 'Forbidden', 'message': 'Only assigned lead weaver or supervisor can approve'}), 403
        
        new_state = 'PASSED_SHIFT_AUDIT'
        cur.execute("""
            UPDATE assistant_weaver_job_logs
            SET approval_state = %s,
                lead_weaver_signoff = TRUE,
                signoff_at = CURRENT_TIMESTAMP
            WHERE id = %s::uuid
            RETURNING id
        """, (new_state, log_id))
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'LEAD_SIGNOFF', %s, %s)
        """, (log_id, operator_id, data.get('notes', 'Approved by lead weaver')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'log_id': log_id,
            'approval_state': new_state,
            'message': 'Shift audit passed'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/logs/<log_id>/reject', methods=['POST'])
@jwt_required()
def reject_assistant_log(log_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id FROM assistant_weaver_job_logs WHERE id = %s::uuid
        """, (log_id,))
        
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        new_state = 'REJECTED_UNRESOLVED_WARP_BREAKS'
        cur.execute("""
            UPDATE assistant_weaver_job_logs
            SET approval_state = %s,
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id
        """, (new_state, json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by lead weaver')}]), log_id))
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'REJECTED', %s, %s)
        """, (log_id, operator_id, data.get('reason', 'Rejected by lead weaver')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'log_id': log_id,
            'approval_state': new_state,
            'message': 'Shift log rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/loom-alarms', methods=['GET'])
@jwt_required()
def list_loom_alarms():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT lba.id, lba.loom_id, lba.factory_node_id, lba.alarm_type,
                   lba.breakage_rate_per_hour, lba.threshold_per_hour,
                   lba.severity, lba.message, lba.acknowledged,
                   lba.created_at
            FROM loom_breakage_alarms lba
            WHERE lba.factory_node_id = (
                SELECT factory_node_id FROM users WHERE id = %s::uuid
            )
            ORDER BY lba.created_at DESC
            LIMIT 50
        """, (operator_id,))
        
        alarms = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(alarms),
            'alarms': [
                {
                    'id': str(a['id']),
                    'loom_id': a['loom_id'],
                    'factory_node_id': a['factory_node_id'],
                    'alarm_type': a['alarm_type'],
                    'breakage_rate_per_hour': float(a['breakage_rate_per_hour']) if a['breakage_rate_per_hour'] else None,
                    'threshold_per_hour': float(a['threshold_per_hour']) if a['threshold_per_hour'] else None,
                    'severity': a['severity'],
                    'message': a['message'],
                    'acknowledged': a['acknowledged'],
                    'created_at': a['created_at'].isoformat() if a['created_at'] else None
                }
                for a in alarms
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/loom-alarms', methods=['POST'])
@jwt_required()
def create_loom_alarm():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required = ['loom_id', 'breakage_rate_per_hour']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        factory_node_id = user_row['factory_node_id'] if user_row else None
        
        threshold = 3.0
        severity = 'WARNING'
        if data.get('breakage_rate_per_hour', 0) > 6:
            severity = 'CRITICAL'
        elif data.get('breakage_rate_per_hour', 0) <= 3:
            severity = 'INFO'
        
        message = data.get('message') or f"High Breakage Risk on Loom {data['loom_id']} — Inspect harness tension, reed roughness, or yarn lot quality."
        
        cur.execute("""
            INSERT INTO loom_breakage_alarms (
                loom_id, factory_node_id, alarm_type, breakage_rate_per_hour,
                threshold_per_hour, severity, message
            )
            VALUES (%s, %s, 'HIGH_BREAKAGE_RATE', %s, %s, %s, %s)
            RETURNING id
        """, (
            data['loom_id'],
            factory_node_id,
            data['breakage_rate_per_hour'],
            threshold,
            severity,
            message
        ))
        
        alarm_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'alarm_id': str(alarm_id),
            'loom_id': data['loom_id'],
            'severity': severity,
            'message': message
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# BOBBIN WINDER MODULE
# ============================================================

def validate_winding_guardrails(data):
    """Validate bobbin winder inputs against business guardrails."""
    errors = []
    warnings = []
    
    yarn_type = data.get('yarn_type')
    winding_machine_type = data.get('winding_machine_type')
    winding_speed_mpm = data.get('winding_speed_mpm')
    joint_method_used = data.get('joint_method_used')
    splice_count_per_bobbin = data.get('splice_count_per_bobbin')
    waste_variance_percent = data.get('waste_variance_percent')
    target_machine = data.get('target_machine')
    
    if target_machine == '2400_HOOK_JACQUARD':
        if joint_method_used == 'Standard Weaver''s Knot':
            errors.append({
                'code': 'REJECT_FOR_2400_WARP',
                'message': 'KNOTS_WILL_JAM_FINE_REED'
            })
        if winding_speed_mpm and winding_speed_mpm > 200:
            warnings.append({
                'code': 'HIGH_WINDING_SPEED_FRICTION_RISK',
                'message': 'High winding speed may cause friction heat and fraying on 2400-hook line'
            })
        if splice_count_per_bobbin and splice_count_per_bobbin > 1:
            errors.append({
                'code': 'ROUTE_DOWNGRADE',
                'message': 'DOWNGRADE_TO_1536_HOOK_OR_WEFT'
            })
    
    if waste_variance_percent is not None and waste_variance_percent > 0.5:
        errors.append({
            'code': 'HIGH_WASTE_VARIANCE',
            'message': 'Waste variance exceeds 0.5% threshold'
        })
    
    return errors, warnings

@app.route('/api/v1/winding/job-cards', methods=['POST'])
@jwt_required()
def create_winding_job_card():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = [
            'spindle_machine_id', 'input_dyed_lot_no', 'yarn_type',
            'carrier_destination_type', 'allocated_input_weight_kg',
            'output_wound_weight_kg', 'winding_scrap_waste_gm',
            'silk_fiber_variety', 'target_output_carrier_type',
            'bobbin_traverse_length_config', 'knot_mechanical_join_profiling',
            'bobbin_structural_build_verdict'
        ]
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        errors, warnings = validate_winding_guardrails(data)
        
        winding_job_card_id = f"WJC-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        status = 'ACTIVE'
        if errors:
            status = 'REJECTED'
        
        cur.execute("""
            INSERT INTO winding_job_cards (
                winding_job_card_id, operator_employee_id, spindle_machine_id,
                factory_node_id, input_dyed_lot_no, yarn_type,
                carrier_destination_type, allocated_input_weight_kg,
                output_wound_weight_kg, winding_scrap_waste_gm,
                winding_operation_type, winding_machine_type,
                worker_attendance_shift_code, yarn_processing_profile,
                silk_fiber_variety, target_output_carrier_type,
                bobbin_traverse_length_config, knot_mechanical_join_profiling,
                bobbin_structural_build_verdict, bobbin_hardness_shore_d,
                splice_count_per_bobbin, bobbin_flange_trapping_found,
                yarn_break_rate_per_1000m, inventory_output_routing_allocation,
                winding_speed_mpm, applied_tension_grams, validation_errors, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, winding_job_card_id
        """, (
            winding_job_card_id,
            operator_id,
            data.get('spindle_machine_id'),
            factory_node_id,
            data.get('input_dyed_lot_no'),
            data.get('yarn_type'),
            data.get('carrier_destination_type'),
            data.get('allocated_input_weight_kg'),
            data.get('output_wound_weight_kg'),
            data.get('winding_scrap_waste_gm', 0),
            data.get('winding_operation_type', 'ROUTINE_PRODUCTION'),
            data.get('winding_machine_type', 'SEMI_AUTOMATIC_BOBBIN_WINDER'),
            data.get('worker_attendance_shift_code', 'SHIFT_A_MORNING'),
            data.get('yarn_processing_profile', data.get('yarn_type')),
            data.get('silk_fiber_variety'),
            data.get('target_output_carrier_type'),
            data.get('bobbin_traverse_length_config'),
            data.get('knot_mechanical_join_profiling'),
            data.get('bobbin_structural_build_verdict'),
            data.get('bobbin_hardness_shore_d'),
            data.get('splice_count_per_bobbin', 0),
            data.get('bobbin_flange_trapping_found', False),
            data.get('yarn_break_rate_per_1000m'),
            data.get('inventory_output_routing_allocation', 'BOBBIN_CLEARED_FOR_WARPING'),
            data.get('winding_speed_mpm'),
            data.get('applied_tension_grams'),
            json.dumps(errors + warnings),
            status
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        job_ref = job_row['winding_job_card_id']
        
        if errors:
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({
                'winding_job_card_id': job_ref,
                'status': 'rejected',
                'errors': errors,
                'warnings': warnings
            }), 400
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'winding_job_card_id': job_ref,
            'id': str(job_id),
            'status': status,
            'warnings': warnings,
            'message': 'Winding job card created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/job-cards', methods=['GET'])
@jwt_required()
def list_winding_job_cards():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wjc.id, wjc.winding_job_card_id, wjc.spindle_machine_id,
                   wjc.factory_node_id, wjc.input_dyed_lot_no, wjc.yarn_type,
                   wjc.carrier_destination_type, wjc.allocated_input_weight_kg,
                   wjc.output_wound_weight_kg, wjc.winding_scrap_waste_gm,
                   wjc.waste_variance_percent, wjc.status,
                   wjc.winding_operation_type, wjc.winding_machine_type,
                   wjc.knot_mechanical_join_profiling, wjc.bobbin_structural_build_verdict,
                   wjc.inventory_output_routing_allocation,
                   wjc.certificate_hash, wjc.created_at,
                   u.full_name AS operator_name
            FROM winding_job_cards wjc
            LEFT JOIN users u ON wjc.operator_employee_id = u.id
            WHERE wjc.operator_employee_id = %s::uuid
               OR wjc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wjc.created_at DESC
            LIMIT 100
        """, (operator_id, operator_id))
        
        cards = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(cards),
            'job_cards': [
                {
                    'id': str(c['id']),
                    'winding_job_card_id': c['winding_job_card_id'],
                    'spindle_machine_id': c['spindle_machine_id'],
                    'factory_node_id': c['factory_node_id'],
                    'input_dyed_lot_no': c['input_dyed_lot_no'],
                    'yarn_type': c['yarn_type'],
                    'carrier_destination_type': c['carrier_destination_type'],
                    'allocated_input_weight_kg': float(c['allocated_input_weight_kg']),
                    'output_wound_weight_kg': float(c['output_wound_weight_kg']),
                    'winding_scrap_waste_gm': float(c['winding_scrap_waste_gm']),
                    'waste_variance_percent': float(c['waste_variance_percent']) if c['waste_variance_percent'] else 0,
                    'status': c['status'],
                    'winding_operation_type': c['winding_operation_type'],
                    'winding_machine_type': c['winding_machine_type'],
                    'knot_mechanical_join_profiling': c['knot_mechanical_join_profiling'],
                    'bobbin_structural_build_verdict': c['bobbin_structural_build_verdict'],
                    'inventory_output_routing_allocation': c['inventory_output_routing_allocation'],
                    'certificate_hash': c['certificate_hash'],
                    'created_at': c['created_at'].isoformat() if c['created_at'] else None,
                    'operator_name': c['operator_name']
                }
                for c in cards
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/job-cards/<job_card_id>', methods=['GET'])
@jwt_required()
def get_winding_job_card(job_card_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wjc.*, u.full_name AS operator_name
            FROM winding_job_cards wjc
            LEFT JOIN users u ON wjc.operator_employee_id = u.id
            WHERE wjc.id = %s::uuid OR wjc.winding_job_card_id = %s
        """, (job_card_id, job_card_id))
        
        card = cur.fetchone()
        cur.close()
        conn.close()
        
        if not card:
            return jsonify({'error': 'JobCardNotFound'}), 404
        
        return jsonify({
            'id': str(card['id']),
            'winding_job_card_id': card['winding_job_card_id'],
            'operator_name': card['operator_name'],
            'spindle_machine_id': card['spindle_machine_id'],
            'factory_node_id': card['factory_node_id'],
            'input_dyed_lot_no': card['input_dyed_lot_no'],
            'yarn_type': card['yarn_type'],
            'carrier_destination_type': card['carrier_destination_type'],
            'allocated_input_weight_kg': float(card['allocated_input_weight_kg']),
            'output_wound_weight_kg': float(card['output_wound_weight_kg']),
            'winding_scrap_waste_gm': float(card['winding_scrap_waste_gm']),
            'process_variance_kg': float(card['process_variance_kg']) if card['process_variance_kg'] else None,
            'waste_variance_percent': float(card['waste_variance_percent']) if card['waste_variance_percent'] else None,
            'winding_operation_type': card['winding_operation_type'],
            'winding_machine_type': card['winding_machine_type'],
            'worker_attendance_shift_code': card['worker_attendance_shift_code'],
            'yarn_processing_profile': card['yarn_processing_profile'],
            'silk_fiber_variety': card['silk_fiber_variety'],
            'target_output_carrier_type': card['target_output_carrier_type'],
            'bobbin_traverse_length_config': card['bobbin_traverse_length_config'],
            'knot_mechanical_join_profiling': card['knot_mechanical_join_profiling'],
            'bobbin_structural_build_verdict': card['bobbin_structural_build_verdict'],
            'bobbin_hardness_shore_d': float(card['bobbin_hardness_shore_d']) if card['bobbin_hardness_shore_d'] else None,
            'splice_count_per_bobbin': card['splice_count_per_bobbin'],
            'bobbin_flange_trapping_found': card['bobbin_flange_trapping_found'],
            'yarn_break_rate_per_1000m': float(card['yarn_break_rate_per_1000m']) if card['yarn_break_rate_per_1000m'] else None,
            'inventory_output_routing_allocation': card['inventory_output_routing_allocation'],
            'winding_speed_mpm': card['winding_speed_mpm'],
            'applied_tension_grams': float(card['applied_tension_grams']) if card['applied_tension_grams'] else None,
            'validation_errors': card['validation_errors'],
            'status': card['status'],
            'certificate_hash': card['certificate_hash'],
            'qr_tag_id': card['qr_tag_id'],
            'created_at': card['created_at'].isoformat() if card['created_at'] else None
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/job-cards/<job_card_id>/approve', methods=['POST'])
@jwt_required()
def approve_winding_job_card(job_card_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, status, validation_errors
            FROM winding_job_cards
            WHERE id = %s::uuid OR winding_job_card_id = %s
        """, (job_card_id, job_card_id))
        
        card = cur.fetchone()
        if not card:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobCardNotFound'}), 404
        
        if card['validation_errors'] and len(card['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot approve job card with validation errors'}), 400
        
        cur.execute("""
            UPDATE winding_job_cards
            SET status = 'CERTIFIED',
                certificate_hash = encode(digest(id::text || winding_job_card_id || CURRENT_TIMESTAMP::text, 'sha256'), 'hex')
            WHERE id = %s::uuid
            RETURNING id, winding_job_card_id, certificate_hash
        """, (card['id'],))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'winding_job_card_id': result['winding_job_card_id'],
            'certificate_hash': result['certificate_hash'],
            'status': 'CERTIFIED',
            'message': 'Winding job card approved and certified'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/job-cards/<job_card_id>/reject', methods=['POST'])
@jwt_required()
def reject_winding_job_card(job_card_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE winding_job_cards
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid OR winding_job_card_id = %s
            RETURNING id, winding_job_card_id
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected')}]),
            job_card_id, job_card_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobCardNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'winding_job_card_id': result['winding_job_card_id'],
            'status': 'REJECTED',
            'message': 'Winding job card rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/bobbins', methods=['GET'])
@jwt_required()
def list_bobbin_records():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT br.id, br.bobbin_id, br.job_card_id, br.carrier_type,
                   br.yarn_type, br.silk_fiber_variety, br.input_dyed_lot_no,
                   br.net_weight_kg, br.status, br.qr_tag_id, br.certificate_hash,
                   br.created_at, wjc.winding_job_card_id
            FROM bobbin_records br
            JOIN winding_job_cards wjc ON br.job_card_id = wjc.id
            WHERE wjc.operator_employee_id = %s::uuid
               OR wjc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY br.created_at DESC
            LIMIT 100
        """, (operator_id, operator_id))
        
        bobbins = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(bobbins),
            'bobbins': [
                {
                    'id': str(b['id']),
                    'bobbin_id': b['bobbin_id'],
                    'job_card_id': str(b['job_card_id']),
                    'winding_job_card_id': b['winding_job_card_id'],
                    'carrier_type': b['carrier_type'],
                    'yarn_type': b['yarn_type'],
                    'silk_fiber_variety': b['silk_fiber_variety'],
                    'input_dyed_lot_no': b['input_dyed_lot_no'],
                    'net_weight_kg': float(b['net_weight_kg']),
                    'status': b['status'],
                    'qr_tag_id': b['qr_tag_id'],
                    'certificate_hash': b['certificate_hash'],
                    'created_at': b['created_at'].isoformat() if b['created_at'] else None
                }
                for b in bobbins
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/bobbins', methods=['POST'])
@jwt_required()
def create_bobbin_record():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['job_card_id', 'carrier_type', 'yarn_type', 'silk_fiber_variety', 'input_dyed_lot_no', 'net_weight_kg']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        factory_node_id = user_row['factory_node_id'] if user_row else None
        
        bobbin_id = f"BOB-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO bobbin_records (
                bobbin_id, job_card_id, operator_id, factory_node_id,
                carrier_type, yarn_type, silk_fiber_variety, input_dyed_lot_no,
                net_weight_kg, traverse_length_config, knot_method,
                structural_verdict, qr_tag_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, bobbin_id
        """, (
            bobbin_id,
            data.get('job_card_id'),
            operator_id,
            factory_node_id,
            data.get('carrier_type'),
            data.get('yarn_type'),
            data.get('silk_fiber_variety'),
            data.get('input_dyed_lot_no'),
            data.get('net_weight_kg'),
            data.get('traverse_length_config'),
            data.get('knot_method'),
            data.get('structural_verdict'),
            'WIND-' + bobbin_id
        ))
        
        bobbin_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(bobbin_row['id']),
            'bobbin_id': bobbin_row['bobbin_id'],
            'message': 'Bobbin record created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/stock-routing', methods=['GET'])
@jwt_required()
def get_stock_routing():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT * FROM vw_bobbin_stock_routing
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        routes = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(routes),
            'routing': [
                {
                    'bobbin_id': r['bobbin_id'],
                    'winding_job_card_id': r['winding_job_card_id'],
                    'carrier_type': r['carrier_type'],
                    'yarn_type': r['yarn_type'],
                    'silk_fiber_variety': r['silk_fiber_variety'],
                    'input_dyed_lot_no': r['input_dyed_lot_no'],
                    'net_weight_kg': float(r['net_weight_kg']),
                    'bobbin_status': r['bobbin_status'],
                    'inventory_output_routing_allocation': r['inventory_output_routing_allocation'],
                    'waste_variance_percent': float(r['waste_variance_percent']) if r['waste_variance_percent'] else None,
                    'bobbin_structural_build_verdict': r['bobbin_structural_build_verdict'],
                    'knot_mechanical_join_profiling': r['knot_mechanical_join_profiling'],
                    'recommended_routing': r['recommended_routing'],
                    'certificate_hash': r['certificate_hash'],
                    'winding_certificate_qr': r['winding_certificate_qr']
                }
                for r in routes
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/waste-alarms', methods=['GET'])
@jwt_required()
def list_waste_alarms():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wwa.id, wwa.job_card_id, wwa.factory_node_id,
                   wwa.allocated_input_weight_kg, wwa.output_wound_weight_kg,
                   wwa.winding_scrap_waste_gm, wwa.waste_variance_percent,
                   wwa.threshold_percent, wwa.severity, wwa.message,
                   wwa.acknowledged, wwa.created_at,
                   wjc.winding_job_card_id
            FROM winding_waste_variance_alarms wwa
            LEFT JOIN winding_job_cards wjc ON wwa.job_card_id = wjc.id
            WHERE wwa.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wwa.created_at DESC
            LIMIT 50
        """, (operator_id,))
        
        alarms = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(alarms),
            'alarms': [
                {
                    'id': str(a['id']),
                    'job_card_id': str(a['job_card_id']),
                    'winding_job_card_id': a['winding_job_card_id'],
                    'allocated_input_weight_kg': float(a['allocated_input_weight_kg']),
                    'output_wound_weight_kg': float(a['output_wound_weight_kg']),
                    'winding_scrap_waste_gm': float(a['winding_scrap_waste_gm']),
                    'waste_variance_percent': float(a['waste_variance_percent']) if a['waste_variance_percent'] else None,
                    'threshold_percent': float(a['threshold_percent']),
                    'severity': a['severity'],
                    'message': a['message'],
                    'acknowledged': a['acknowledged'],
                    'created_at': a['created_at'].isoformat() if a['created_at'] else None
                }
                for a in alarms
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/winding/waste-alarms', methods=['POST'])
@jwt_required()
def create_waste_alarm():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required = ['job_card_id', 'allocated_input_weight_kg', 'output_wound_weight_kg', 'winding_scrap_waste_gm']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        factory_node_id = user_row['factory_node_id'] if user_row else None
        
        waste_variance_percent = (data['winding_scrap_waste_gm'] / 1000.0) / data['allocated_input_weight_kg'] * 100 if data['allocated_input_weight_kg'] > 0 else 0
        severity = 'WARNING' if waste_variance_percent > 0.5 else 'INFO'
        if waste_variance_percent > 1.0:
            severity = 'CRITICAL'
        
        message = data.get('message') or f"High Waste Variance on Job Card {data['job_card_id']} — Waste: {waste_variance_percent:.2f}% exceeds 0.5% threshold. Route to Production Supervisor."
        
        cur.execute("""
            INSERT INTO winding_waste_variance_alarms (
                job_card_id, factory_node_id, operator_id,
                allocated_input_weight_kg, output_wound_weight_kg,
                winding_scrap_waste_gm, waste_variance_percent,
                threshold_percent, severity, message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data['job_card_id'],
            factory_node_id,
            operator_id,
            data['allocated_input_weight_kg'],
            data['output_wound_weight_kg'],
            data['winding_scrap_waste_gm'],
            waste_variance_percent,
            0.5,
            severity,
            message
        ))
        
        alarm_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'alarm_id': str(alarm_id),
            'waste_variance_percent': round(waste_variance_percent, 2),
            'severity': severity,
            'message': message
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# INWARD QUALITY GATE MODULE
# ============================================================

def validate_quality_intake_guardrails(data):
    """Validate QC intake against industry benchmarks."""
    errors = []
    warnings = []
    
    if data.get('live_moisture_reading_pct') is not None and data['live_moisture_reading_pct'] > 11.0:
        errors.append({
            'code': 'MOISTURE_EXCEEDS_LIMIT',
            'message': f"Moisture {data['live_moisture_reading_pct']}% exceeds 11.0% legal standard"
        })
    
    if data.get('size_deviation_pct') is not None and data['size_deviation_pct'] > 4.0:
        errors.append({
            'code': 'SIZE_DEVIATION_EXCEEDED',
            'message': f"Size deviation {data['size_deviation_pct']}% exceeds 4.0% threshold"
        })
    
    if data.get('evenness_pct') is not None and data['evenness_pct'] < 95:
        warnings.append({
            'code': 'EVENNESS_BELOW_TARGET',
            'message': f"Evenness {data['evenness_pct']}% below 95% target"
        })
    
    if data.get('cleanness_pct') is not None and data['cleanness_pct'] < 95:
        warnings.append({
            'code': 'CLEANNESS_BELOW_TARGET',
            'message': f"Cleanness {data['cleanness_pct']}% below 95% target"
        })
    
    if data.get('neatness_pct') is not None and data['neatness_pct'] < 93:
        warnings.append({
            'code': 'NEATNESS_BELOW_TARGET',
            'message': f"Neatness {data['neatness_pct']}% below 93% target"
        })
    
    if data.get('tenacity_gd') is not None and data['tenacity_gd'] < 3.5:
        errors.append({
            'code': 'TENACITY_BELOW_MINIMUM',
            'message': f"Tenacity {data['tenacity_gd']} g/d below 3.5 minimum"
        })
    
    if data.get('cohesion_strokes') is not None and data['cohesion_strokes'] < 60:
        warnings.append({
            'code': 'COHESION_BELOW_TARGET',
            'message': f"Cohesion {data['cohesion_strokes']} strokes below 60 minimum"
        })
    
    if data.get('has_machine_oil_stains'):
        errors.append({
            'code': 'MACHINE_OIL_CONTAMINATION',
            'message': 'Material flagged as structurally contaminated'
        })
    
    if data.get('has_mixed_dye_lots'):
        warnings.append({
            'code': 'MIXED_DYE_LOTS_RISK',
            'message': 'High risk of dual-tone saree defects'
        })
    
    return errors, warnings

# ============================================================
# GATE CLERK ENDPOINTS
# ============================================================

@app.route('/api/v1/inward-gate/entries', methods=['POST'])
@jwt_required()
def create_inward_gate_entry():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['supplier_id', 'invoice_number', 'invoice_gross_weight_kg', 'actual_scale_weight_kg', 'filature_lot_number']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        inward_gate_entry_no = f"IGE-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO inward_gate_entries (
                inward_gate_entry_no, supplier_id, invoice_number,
                invoice_gross_weight_kg, actual_scale_weight_kg,
                filature_lot_number, factory_node_id, recorded_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, inward_gate_entry_no
        """, (
            inward_gate_entry_no,
            data.get('supplier_id'),
            data.get('invoice_number'),
            data.get('invoice_gross_weight_kg'),
            data.get('actual_scale_weight_kg'),
            data.get('filature_lot_number'),
            factory_node_id,
            operator_id
        ))
        
        entry_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(entry_row['id']),
            'inward_gate_entry_no': entry_row['inward_gate_entry_no'],
            'status': 'QC_HOLD',
            'message': 'Inward gate entry created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/inward-gate/entries', methods=['GET'])
@jwt_required()
def list_inward_gate_entries():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT ige.id, ige.inward_gate_entry_no, ige.supplier_id,
                   u.full_name AS supplier_name, ige.invoice_number,
                   ige.invoice_gross_weight_kg, ige.actual_scale_weight_kg,
                   ige.filature_lot_number, ige.status, ige.created_at
            FROM inward_gate_entries ige
            LEFT JOIN users u ON ige.supplier_id = u.id
            WHERE ige.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY ige.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        entries = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(entries),
            'entries': [
                {
                    'id': str(e['id']),
                    'inward_gate_entry_no': e['inward_gate_entry_no'],
                    'supplier_id': str(e['supplier_id']),
                    'supplier_name': e['supplier_name'],
                    'invoice_number': e['invoice_number'],
                    'invoice_gross_weight_kg': float(e['invoice_gross_weight_kg']),
                    'actual_scale_weight_kg': float(e['actual_scale_weight_kg']),
                    'filature_lot_number': e['filature_lot_number'],
                    'status': e['status'],
                    'created_at': e['created_at'].isoformat() if e['created_at'] else None
                }
                for e in entries
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# QC INSPECTOR ENDPOINTS
# ============================================================

@app.route('/api/v1/quality/intake', methods=['POST'])
@jwt_required()
def create_quality_intake():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['inward_gate_entry_id', 'silk_type', 'machinery_source', 'certified_grade']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (operator_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        errors, warnings = validate_quality_intake_guardrails(data)
        
        quality_intake_no = f"QIR-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        status = 'SUBMITTED' if not errors else 'DRAFT'
        
        cur.execute("""
            INSERT INTO quality_intake_records (
                quality_intake_no, inward_gate_entry_id, inspector_id,
                factory_node_id, silk_type, machinery_source, silk_mark_tag_id,
                lab_report_number, certified_grade, size_deviation_pct,
                evenness_pct, cleanness_pct, neatness_pct, tenacity_gd,
                cohesion_strokes, live_moisture_reading_pct,
                has_machine_oil_stains, has_mixed_dye_lots, sample_hank_weight_g,
                validation_errors, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, quality_intake_no
        """, (
            quality_intake_no,
            data.get('inward_gate_entry_id'),
            operator_id,
            factory_node_id,
            data.get('silk_type'),
            data.get('machinery_source'),
            data.get('silk_mark_tag_id'),
            data.get('lab_report_number'),
            data.get('certified_grade'),
            data.get('size_deviation_pct'),
            data.get('evenness_pct'),
            data.get('cleanness_pct'),
            data.get('neatness_pct'),
            data.get('tenacity_gd'),
            data.get('cohesion_strokes'),
            data.get('live_moisture_reading_pct'),
            data.get('has_machine_oil_stains', False),
            data.get('has_mixed_dye_lots', False),
            data.get('sample_hank_weight_g'),
            json.dumps(errors + warnings),
            status
        ))
        
        intake_row = cur.fetchone()
        intake_id = intake_row['id']
        
        if errors:
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({
                'id': str(intake_id),
                'quality_intake_no': quality_intake_no,
                'status': status,
                'errors': errors,
                'warnings': warnings
            }), 400
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(intake_id),
            'quality_intake_no': quality_intake_no,
            'status': status,
            'warnings': warnings,
            'message': 'Quality intake created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality/intake', methods=['GET'])
@jwt_required()
def list_quality_intakes():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qir.id, qir.quality_intake_no, qir.inward_gate_entry_id,
                   ige.inward_gate_entry_no, qir.silk_type, qir.machinery_source,
                   qir.certified_grade, qir.size_deviation_pct, qir.evenness_pct,
                   qir.cleanness_pct, qir.neatness_pct, qir.tenacity_gd,
                   qir.cohesion_strokes, qir.live_moisture_reading_pct,
                   qir.auto_assigned_routing, qir.status, qir.certificate_hash,
                   qir.created_at
            FROM quality_intake_records qir
            JOIN inward_gate_entries ige ON qir.inward_gate_entry_id = ige.id
            WHERE qir.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY qir.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        intakes = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(intakes),
            'intakes': [
                {
                    'id': str(i['id']),
                    'quality_intake_no': i['quality_intake_no'],
                    'inward_gate_entry_id': str(i['inward_gate_entry_id']),
                    'inward_gate_entry_no': i['inward_gate_entry_no'],
                    'silk_type': i['silk_type'],
                    'machinery_source': i['machinery_source'],
                    'certified_grade': i['certified_grade'],
                    'size_deviation_pct': float(i['size_deviation_pct']) if i['size_deviation_pct'] else None,
                    'evenness_pct': float(i['evenness_pct']) if i['evenness_pct'] else None,
                    'cleanness_pct': float(i['cleanness_pct']) if i['cleanness_pct'] else None,
                    'neatness_pct': float(i['neatness_pct']) if i['neatness_pct'] else None,
                    'tenacity_gd': float(i['tenacity_gd']) if i['tenacity_gd'] else None,
                    'cohesion_strokes': i['cohesion_strokes'],
                    'live_moisture_reading_pct': float(i['live_moisture_reading_pct']) if i['live_moisture_reading_pct'] else None,
                    'auto_assigned_routing': i['auto_assigned_routing'],
                    'status': i['status'],
                    'certificate_hash': i['certificate_hash'],
                    'created_at': i['created_at'].isoformat() if i['created_at'] else None
                }
                for i in intakes
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality/intake/<intake_id>', methods=['GET'])
@jwt_required()
def get_quality_intake(intake_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qir.*, ige.inward_gate_entry_no, ige.invoice_number,
                   ige.actual_scale_weight_kg, ige.filature_lot_number
            FROM quality_intake_records qir
            JOIN inward_gate_entries ige ON qir.inward_gate_entry_id = ige.id
            WHERE qir.id = %s::uuid
        """, (intake_id,))
        
        intake = cur.fetchone()
        cur.close()
        conn.close()
        
        if not intake:
            return jsonify({'error': 'IntakeNotFound'}), 404
        
        return jsonify({
            'id': str(intake['id']),
            'quality_intake_no': intake['quality_intake_no'],
            'inward_gate_entry_id': str(intake['inward_gate_entry_id']),
            'inward_gate_entry_no': intake['inward_gate_entry_no'],
            'invoice_number': intake['invoice_number'],
            'actual_scale_weight_kg': float(intake['actual_scale_weight_kg']),
            'filature_lot_number': intake['filature_lot_number'],
            'silk_type': intake['silk_type'],
            'machinery_source': intake['machinery_source'],
            'silk_mark_tag_id': intake['silk_mark_tag_id'],
            'lab_report_number': intake['lab_report_number'],
            'certified_grade': intake['certified_grade'],
            'size_deviation_pct': float(intake['size_deviation_pct']) if intake['size_deviation_pct'] else None,
            'evenness_pct': float(intake['evenness_pct']) if intake['evenness_pct'] else None,
            'cleanness_pct': float(intake['cleanness_pct']) if intake['cleanness_pct'] else None,
            'neatness_pct': float(intake['neatness_pct']) if intake['neatness_pct'] else None,
            'tenacity_gd': float(intake['tenacity_gd']) if intake['tenacity_gd'] else None,
            'cohesion_strokes': intake['cohesion_strokes'],
            'live_moisture_reading_pct': float(intake['live_moisture_reading_pct']) if intake['live_moisture_reading_pct'] else None,
            'has_machine_oil_stains': intake['has_machine_oil_stains'],
            'has_mixed_dye_lots': intake['has_mixed_dye_lots'],
            'sample_hank_weight_g': intake['sample_hank_weight_g'],
            'calculated_conditioned_weight_kg': float(intake['calculated_conditioned_weight_kg']) if intake['calculated_conditioned_weight_kg'] else None,
            'billing_weight_discrepancy_kg': float(intake['billing_weight_discrepancy_kg']) if intake['billing_weight_discrepancy_kg'] else None,
            'auto_assigned_routing': intake['auto_assigned_routing'],
            'validation_errors': intake['validation_errors'],
            'status': intake['status'],
            'certificate_hash': intake['certificate_hash'],
            'qr_tag_id': intake['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# QUALITY MANAGER ENDPOINTS
# ============================================================

@app.route('/api/v1/quality/intake/<intake_id>/approve', methods=['POST'])
@jwt_required()
def approve_quality_intake(intake_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, status, auto_assigned_routing, validation_errors
            FROM quality_intake_records
            WHERE id = %s::uuid
        """, (intake_id,))
        
        intake = cur.fetchone()
        if not intake:
            cur.close()
            conn.close()
            return jsonify({'error': 'IntakeNotFound'}), 404
        
        if intake['validation_errors'] and len(intake['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot approve intake with validation errors'}), 400
        
        new_status = intake['auto_assigned_routing'] or 'WARP_PREMIUM'
        certificate_hash = encode(digest(intake_id::text || quality_intake_no || CURRENT_TIMESTAMP::text, 'sha256'), 'hex')
        
        cur.execute("""
            UPDATE quality_intake_records
            SET status = %s,
                certificate_hash = %s,
                qr_tag_id = 'QC-' || quality_intake_no
            WHERE id = %s::uuid
            RETURNING id, quality_intake_no, certificate_hash
        """, (new_status, certificate_hash, intake_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO quality_approval_workflow (
                quality_intake_id, approver_id, action, from_status, to_status, notes
            )
            VALUES (%s, %s, 'APPROVED', %s, %s, %s)
        """, (intake_id, approver_id, intake['status'], new_status, data.get('notes', 'Approved by Quality Manager')))
        
        cur.execute("""
            INSERT INTO quality_certificates (
                quality_intake_id, inward_gate_entry_id, certificate_hash,
                qr_tag_id, certified_grade, auto_assigned_routing,
                inspector_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                qir.id,
                qir.inward_gate_entry_id,
                qir.certificate_hash,
                qir.qr_tag_id,
                qir.certified_grade,
                qir.auto_assigned_routing,
                qir.inspector_id,
                %s,
                qir.factory_node_id,
                jsonb_build_object(
                    'quality_intake_no', qir.quality_intake_no,
                    'certified_grade', qir.certified_grade,
                    'auto_assigned_routing', qir.auto_assigned_routing,
                    'size_deviation_pct', qir.size_deviation_pct,
                    'evenness_pct', qir.evenness_pct,
                    'cleanness_pct', qir.cleanness_pct,
                    'neatness_pct', qir.neatness_pct,
                    'tenacity_gd', qir.tenacity_gd,
                    'cohesion_strokes', qir.cohesion_strokes,
                    'live_moisture_reading_pct', qir.live_moisture_reading_pct
                )
            FROM quality_intake_records qir
            WHERE qir.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM quality_certificates WHERE quality_intake_id = qir.id
            )
        """, (approver_id, intake_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'quality_intake_no': result['quality_intake_no'],
            'certificate_hash': result['certificate_hash'],
            'status': new_status,
            'message': 'Quality intake approved and certified'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality/intake/<intake_id>/reject', methods=['POST'])
@jwt_required()
def reject_quality_intake(intake_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE quality_intake_records
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id, quality_intake_no
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Quality Manager')}]),
            intake_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'IntakeNotFound'}), 404
        
        cur.execute("""
            INSERT INTO quality_approval_workflow (
                quality_intake_id, approver_id, action, from_status, to_status, notes
            )
            VALUES (%s, %s, 'REJECTED', 'QC_HOLD', 'REJECTED_VENDOR_RETURN', %s)
        """, (intake_id, operator_id, data.get('reason', 'Rejected by Quality Manager')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'quality_intake_no': result['quality_intake_no'],
            'status': 'REJECTED',
            'message': 'Quality intake rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality/intake/<intake_id>/hold', methods=['POST'])
@jwt_required()
def hold_quality_intake(intake_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE quality_intake_records
            SET status = 'QC_HOLD',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id, quality_intake_no
        """, (
            json.dumps([{'code': 'MANUAL_HOLD', 'message': data.get('reason', 'Placed on hold by Quality Manager')}]),
            intake_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'IntakeNotFound'}), 404
        
        cur.execute("""
            INSERT INTO quality_approval_workflow (
                quality_intake_id, approver_id, action, from_status, to_status, notes
            )
            VALUES (%s, %s, 'HOLD', 'QC_HOLD', 'QC_HOLD', %s)
        """, (intake_id, operator_id, data.get('reason', 'Placed on hold')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'quality_intake_no': result['quality_intake_no'],
            'status': 'QC_HOLD',
            'message': 'Quality intake placed on hold'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality/certificates', methods=['GET'])
@jwt_required()
def list_quality_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = cur.cursor()
        
        cur.execute("""
            SELECT qc.id, qc.certificate_hash, qc.qr_tag_id, qc.certified_grade,
                   qc.auto_assigned_routing, qc.status, qc.certified_at,
                   qir.quality_intake_no, ige.inward_gate_entry_no
            FROM quality_certificates qc
            JOIN quality_intake_records qir ON qc.quality_intake_id = qir.id
            JOIN inward_gate_entries ige ON qc.inward_gate_entry_id = ige.id
            WHERE qc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY qc.certified_at DESC
            LIMIT 100
        """, (operator_id,))
        
        certs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(certs),
            'certificates': [
                {
                    'id': str(c['id']),
                    'certificate_hash': c['certificate_hash'],
                    'qr_tag_id': c['qr_tag_id'],
                    'certified_grade': c['certified_grade'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None,
                    'quality_intake_no': c['quality_intake_no'],
                    'inward_gate_entry_no': c['inward_gate_entry_no']
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN
# ============================================================

@app.route('/api/v1/sales/forecast/material', methods=['GET'])
@jwt_required()
def get_sales_forecast_material():
    """
    API plugin endpoint for sales team material processing forecast.
    Returns forecasted material requirements based on sales pipeline.
    """
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT factory_node_id FROM users WHERE id = %s::uuid
        """, (operator_id,))
        user_row = cur.fetchone()
        factory_node_id = user_row['factory_node_id'] if user_row else None
        
        # In production, this would join with sales orders/forecasting tables
        # For now, return mock forecast data structure
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'silk_type': 'BIVOLTINE_WHITE_SILK',
                    'denier': '20/22 Denier',
                    'estimated_quantity_kg': 500.0,
                    'priority': 'HIGH',
                    'destination': 'WARP_PREMIUM',
                    'recommended_grade': '4A+'
                },
                {
                    'silk_type': 'MULTIVOLTINE_YELLOW_SILK',
                    'denier': '16/18 Denier',
                    'estimated_quantity_kg': 300.0,
                    'priority': 'MEDIUM',
                    'destination': 'WEFT_ONLY',
                    'recommended_grade': '5A'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'LOT-2024-0011',
                    'estimated_sarees': 80,
                    'estimated_yarn_kg': 45.5,
                    'target_grade': '4A'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)
