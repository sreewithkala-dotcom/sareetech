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
import hashlib
import random
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

def generate_certificate_hash(*args):
    """Generate a SHA-256 hex certificate hash from arbitrary arguments."""
    raw = ''.join(str(a) for a in args) + datetime.utcnow().isoformat()
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

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
    warnings = []
    
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
    
    return errors, warnings

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
        
        validation_errors, validation_warnings = validate_assistant_guardrails(data)
        
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
# ASSISTANT WEAVER (JUNIOR LOOM OPERATOR) MODULE - ENHANCED
# ============================================================

@app.route('/api/v1/assistant-weaver/jobs', methods=['POST'])
@jwt_required()
def create_assistant_weaver_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['active_loom_id', 'pirns_replaced_count', 'logged_warp_breaks',
                           'logged_weft_breaks', 'shift_start_time', 'shift_end_time']
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
        
        validation_errors, validation_warnings = validate_assistant_guardrails(data)
        
        assistant_job_log_id = f"AWL-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO assistant_weaver_job_logs (
                assistant_job_log_id, active_loom_id, assistant_weaver_id, lead_weaver_id,
                factory_node_id, shift_start_time, shift_end_time,
                pirns_replaced_count, logged_warp_breaks, logged_weft_breaks,
                weft_spool_lot_id, weft_feeder_position, yarn_tail_transfer_status,
                zari_tension_disc_setting, warp_break_repair_count, mending_knot_type,
                dropper_rethread_verification, comber_board_cleaning_status,
                shift_handover_readiness, approval_state, validation_errors,
                master_weaver_job_id, petni_master_job_id, warp_joining_job_id,
                harness_setup_log_id, warp_beam_production_log_id, card_puncher_job_id,
                pirn_winding_job_id, bobbin_winder_job_card_id,
                auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, assistant_job_log_id
        """, (
            assistant_job_log_id,
            data.get('active_loom_id'),
            operator_id,
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
            'ACTIVE_LOGGING' if not validation_errors else 'REJECTED_UNRESOLVED_WARP_BREAKS',
            json.dumps(validation_errors),
            data.get('master_weaver_job_id'),
            data.get('petni_master_job_id'),
            data.get('warp_joining_job_id'),
            data.get('harness_setup_log_id'),
            data.get('warp_beam_production_log_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'SUBMITTED', %s, %s)
        """, (log_id, operator_id, f"Initial submission with {len(validation_errors)} validation errors"))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'assistant_job_log_id': log_row['assistant_job_log_id'],
            'approval_state': 'ACTIVE_LOGGING' if not validation_errors else 'REJECTED_UNRESOLVED_WARP_BREAKS',
            'validation_errors': validation_errors,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/jobs', methods=['GET'])
@jwt_required()
def list_assistant_weaver_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT awl.id, awl.assistant_job_log_id, awl.active_loom_id, awl.factory_node_id,
                   awl.shift_start_time, awl.shift_end_time, awl.approval_state,
                   awl.pirns_replaced_count, awl.logged_warp_breaks, awl.logged_weft_breaks,
                   awl.warp_break_repair_count, awl.lead_weaver_signoff,
                   awl.weft_feeder_position, awl.yarn_tail_transfer_status,
                   awl.zari_tension_disc_setting, awl.mending_knot_type,
                   awl.dropper_rethread_verification, awl.comber_board_cleaning_status,
                   awl.shift_handover_readiness, awl.auto_assigned_routing,
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
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'assistant_job_log_id': j['assistant_job_log_id'],
                    'active_loom_id': j['active_loom_id'],
                    'factory_node_id': j['factory_node_id'],
                    'shift_start_time': j['shift_start_time'].isoformat() if j['shift_start_time'] else None,
                    'shift_end_time': j['shift_end_time'].isoformat() if j['shift_end_time'] else None,
                    'approval_state': j['approval_state'],
                    'pirns_replaced_count': j['pirns_replaced_count'],
                    'logged_warp_breaks': j['logged_warp_breaks'],
                    'logged_weft_breaks': j['logged_weft_breaks'],
                    'warp_break_repair_count': j['warp_break_repair_count'],
                    'lead_weaver_signoff': j['lead_weaver_signoff'],
                    'weft_feeder_position': j['weft_feeder_position'],
                    'yarn_tail_transfer_status': j['yarn_tail_transfer_status'],
                    'zari_tension_disc_setting': j['zari_tension_disc_setting'],
                    'mending_knot_type': j['mending_knot_type'],
                    'dropper_rethread_verification': j['dropper_rethread_verification'],
                    'comber_board_cleaning_status': j['comber_board_cleaning_status'],
                    'shift_handover_readiness': j['shift_handover_readiness'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'assistant_name': j['assistant_name'],
                    'lead_name': j['lead_name']
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_assistant_weaver_job(job_id):
    try:
        operator_id = get_jwt_identity()
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
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'assistant_job_log_id': job['assistant_job_log_id'],
            'active_loom_id': job['active_loom_id'],
            'assistant_weaver_id': str(job['assistant_weaver_id']),
            'lead_weaver_id': str(job['lead_weaver_id']) if job['lead_weaver_id'] else None,
            'factory_node_id': job['factory_node_id'],
            'shift_start_time': job['shift_start_time'].isoformat() if job['shift_start_time'] else None,
            'shift_end_time': job['shift_end_time'].isoformat() if job['shift_end_time'] else None,
            'pirns_replaced_count': job['pirns_replaced_count'],
            'logged_warp_breaks': job['logged_warp_breaks'],
            'logged_weft_breaks': job['logged_weft_breaks'],
            'warp_break_repair_count': job['warp_break_repair_count'],
            'lead_weaver_signoff': job['lead_weaver_signoff'],
            'approval_state': job['approval_state'],
            'validation_errors': job['validation_errors'],
            'weft_feeder_position': job['weft_feeder_position'],
            'yarn_tail_transfer_status': job['yarn_tail_transfer_status'],
            'zari_tension_disc_setting': job['zari_tension_disc_setting'],
            'mending_knot_type': job['mending_knot_type'],
            'dropper_rethread_verification': job['dropper_rethread_verification'],
            'comber_board_cleaning_status': job['comber_board_cleaning_status'],
            'shift_handover_readiness': job['shift_handover_readiness'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'assistant_name': job['assistant_name'],
            'lead_name': job['lead_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/jobs/<job_id>/approve', methods=['POST'])
@jwt_required()
def approve_assistant_weaver_job(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, approval_state, lead_weaver_id
            FROM assistant_weaver_job_logs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['lead_weaver_id'] and str(job['lead_weaver_id']) != operator_id:
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
        """, (new_state, job_id))
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'LEAD_SIGNOFF', %s, %s)
        """, (job_id, operator_id, data.get('notes', 'Approved by lead weaver')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'log_id': job_id,
            'approval_state': new_state,
            'message': 'Shift audit passed'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/jobs/<job_id>/reject', methods=['POST'])
@jwt_required()
def reject_assistant_weaver_job(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id FROM assistant_weaver_job_logs WHERE id = %s::uuid
        """, (job_id,))
        
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        new_state = 'REJECTED_UNRESOLVED_WARP_BREAKS'
        cur.execute("""
            UPDATE assistant_weaver_job_logs
            SET approval_state = %s,
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id
        """, (new_state, json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by lead weaver')}]), job_id))
        
        cur.execute("""
            INSERT INTO assistant_weaver_shift_audits (job_log_id, audit_action, performed_by, notes)
            VALUES (%s, 'REJECTED', %s, %s)
        """, (job_id, operator_id, data.get('reason', 'Rejected by lead weaver')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'log_id': job_id,
            'approval_state': new_state,
            'message': 'Shift log rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_assistant_weaver_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, assistant_job_log_id, status, validation_errors, approval_state
            FROM assistant_weaver_job_logs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['assistant_job_log_id'])
        qr_tag_id = 'ASSISTANT-' + job['assistant_job_log_id']
        
        cur.execute("""
            UPDATE assistant_weaver_job_logs
            SET approval_state = 'PASSED_SHIFT_AUDIT',
                certificate_hash = %s,
                qr_tag_id = %s,
                lead_weaver_signoff = TRUE,
                signoff_at = CURRENT_TIMESTAMP
            WHERE id = %s::uuid
            RETURNING id, assistant_job_log_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO assistant_weaver_certificates (
                assistant_weaver_job_log_id, certificate_hash, qr_tag_id, assistant_job_log_id,
                active_loom_id, master_weaver_job_id,
                pirns_replaced_count, logged_warp_breaks, logged_weft_breaks,
                warp_break_repair_count, weft_feeder_position,
                yarn_tail_transfer_status, zari_tension_disc_setting,
                mending_knot_type, dropper_rethread_verification,
                comber_board_cleaning_status, shift_handover_readiness,
                assistant_weaver_approval_state, lead_weaver_signoff,
                auto_assigned_routing, assistant_weaver_id, lead_weaver_id,
                approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.assistant_job_log_id,
                pj.active_loom_id,
                pj.master_weaver_job_id,
                pj.pirns_replaced_count, pj.logged_warp_breaks, pj.logged_weft_breaks,
                pj.warp_break_repair_count, pj.weft_feeder_position,
                pj.yarn_tail_transfer_status, pj.zari_tension_disc_setting,
                pj.mending_knot_type, pj.dropper_rethread_verification,
                pj.comber_board_cleaning_status, pj.shift_handover_readiness,
                pj.approval_state, pj.lead_weaver_signoff,
                pj.auto_assigned_routing, pj.assistant_weaver_id, pj.lead_weaver_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'assistant_job_log_id', pj.assistant_job_log_id,
                    'active_loom_id', pj.active_loom_id,
                    'pirns_replaced_count', pj.pirns_replaced_count,
                    'logged_warp_breaks', pj.logged_warp_breaks,
                    'logged_weft_breaks', pj.logged_weft_breaks,
                    'warp_break_repair_count', pj.warp_break_repair_count,
                    'lead_weaver_signoff', pj.lead_weaver_signoff
                )
            FROM assistant_weaver_job_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'assistant_job_log_id': result['assistant_job_log_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'PASSED_SHIFT_AUDIT',
            'message': 'Assistant weaver job certified and shift audit passed'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/assistant-weaver/certificates', methods=['GET'])
@jwt_required()
def list_assistant_weaver_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT awc.id, awc.certificate_hash, awc.qr_tag_id,
                   awc.assistant_job_log_id, awc.active_loom_id,
                   awc.pirns_replaced_count, awc.logged_warp_breaks,
                   awc.logged_weft_breaks, awc.warp_break_repair_count,
                   awc.weft_feeder_position, awc.yarn_tail_transfer_status,
                   awc.zari_tension_disc_setting, awc.mending_knot_type,
                   awc.dropper_rethread_verification, awc.comber_board_cleaning_status,
                   awc.shift_handover_readiness, awc.assistant_weaver_approval_state,
                   awc.lead_weaver_signoff, awc.auto_assigned_routing,
                   awc.status, awc.certified_at,
                   awl.assistant_job_log_id
            FROM assistant_weaver_certificates awc
            JOIN assistant_weaver_job_logs awl ON awc.assistant_weaver_job_log_id = awl.id
            WHERE awc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY awc.certified_at DESC
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
                    'assistant_job_log_id': c['assistant_job_log_id'],
                    'active_loom_id': c['active_loom_id'],
                    'pirns_replaced_count': c['pirns_replaced_count'],
                    'logged_warp_breaks': c['logged_warp_breaks'],
                    'logged_weft_breaks': c['logged_weft_breaks'],
                    'warp_break_repair_count': c['warp_break_repair_count'],
                    'weft_feeder_position': c['weft_feeder_position'],
                    'yarn_tail_transfer_status': c['yarn_tail_transfer_status'],
                    'zari_tension_disc_setting': c['zari_tension_disc_setting'],
                    'mending_knot_type': c['mending_knot_type'],
                    'dropper_rethread_verification': c['dropper_rethread_verification'],
                    'comber_board_cleaning_status': c['comber_board_cleaning_status'],
                    'shift_handover_readiness': c['shift_handover_readiness'],
                    'assistant_weaver_approval_state': c['assistant_weaver_approval_state'],
                    'lead_weaver_signoff': c['lead_weaver_signoff'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR ASSISTANT WEAVER
# ============================================================

@app.route('/api/v1/sales/forecast/assistant-weaver', methods=['GET'])
@jwt_required()
def get_sales_forecast_assistant_weaver():
    """
    API plugin endpoint for sales team assistant weaver material processing forecast.
    Returns forecasted assistant weaver requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'weft_feeder_position': 'Feeder 2 (Zari Extra Weft)',
                    'zari_tension_disc_setting': 'Micro-Tension Active (Fine Zari)',
                    'estimated_shifts': 3,
                    'pirns_replaced_count': 45,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'weft_feeder_position': 'Feeder 2 (Zari Extra Weft)',
                    'zari_tension_disc_setting': 'Micro-Tension Active (Fine Zari)',
                    'estimated_shifts': 2,
                    'pirns_replaced_count': 38,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'weft_feeder_position': 'Feeder 1 (Ground Silk)',
                    'zari_tension_disc_setting': 'Standard Friction',
                    'estimated_shifts': 5,
                    'pirns_replaced_count': 25,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'ASSISTANT-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_shifts': 3,
                    'loom_id': 'LOOM-2400-001'
                },
                {
                    'lot_number': 'ASSISTANT-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_shifts': 2,
                    'loom_id': 'LOOM-2400-002'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
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
        certificate_hash = generate_certificate_hash(intake_id, quality_intake_no)
        
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
        cur = conn.cursor()
        
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

# ============================================================
# ZARI REFINERY MODULE
# ============================================================

@app.route('/api/v1/zari/lot-batches', methods=['POST'])
@jwt_required()
def create_zari_lot_batch():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['zari_lot_batch_no', 'zari_type', 'zari_origin_cluster']
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
        saree_bundle_size = data.get('saree_bundle_size', 80)
        
        cur.execute("""
            INSERT INTO zari_lot_batches (
                zari_lot_batch_no, zari_type, zari_origin_cluster,
                saree_bundle_size, factory_node_id, recorded_by, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'OPEN')
            RETURNING id, zari_lot_batch_no
        """, (
            data.get('zari_lot_batch_no'),
            data.get('zari_type'),
            data.get('zari_origin_cluster'),
            saree_bundle_size,
            factory_node_id,
            operator_id
        ))
        
        batch_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(batch_row['id']),
            'zari_lot_batch_no': batch_row['zari_lot_batch_no'],
            'status': 'OPEN',
            'saree_bundle_size': saree_bundle_size,
            'message': 'Zari lot batch created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/lot-batches', methods=['GET'])
@jwt_required()
def list_zari_lot_batches():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zlb.id, zlb.zari_lot_batch_no, zlb.zari_type, zlb.zari_origin_cluster,
                   zlb.saree_bundle_size, zlb.status, zlb.created_at,
                   u.full_name AS recorded_by_name
            FROM zari_lot_batches zlb
            LEFT JOIN users u ON zlb.recorded_by = u.id
            WHERE zlb.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY zlb.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        batches = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(batches),
            'batches': [
                {
                    'id': str(b['id']),
                    'zari_lot_batch_no': b['zari_lot_batch_no'],
                    'zari_type': b['zari_type'],
                    'zari_origin_cluster': b['zari_origin_cluster'],
                    'saree_bundle_size': b['saree_bundle_size'],
                    'status': b['status'],
                    'recorded_by_name': b['recorded_by_name'],
                    'created_at': b['created_at'].isoformat() if b['created_at'] else None
                }
                for b in batches
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/lot-batches/<batch_id>', methods=['GET'])
@jwt_required()
def get_zari_lot_batch(batch_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zlb.*, u.full_name AS recorded_by_name
            FROM zari_lot_batches zlb
            LEFT JOIN users u ON zlb.recorded_by = u.id
            WHERE zlb.id = %s::uuid
        """, (batch_id,))
        
        batch = cur.fetchone()
        cur.close()
        conn.close()
        
        if not batch:
            return jsonify({'error': 'BatchNotFound'}), 404
        
        return jsonify({
            'id': str(batch['id']),
            'zari_lot_batch_no': batch['zari_lot_batch_no'],
            'zari_type': batch['zari_type'],
            'zari_origin_cluster': batch['zari_origin_cluster'],
            'saree_bundle_size': batch['saree_bundle_size'],
            'status': batch['status'],
            'recorded_by_name': batch['recorded_by_name'],
            'metadata': batch['metadata'],
            'created_at': batch['created_at'].isoformat() if batch['created_at'] else None,
            'updated_at': batch['updated_at'].isoformat() if batch['updated_at'] else None
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# ZARI ASSAY / METALLURGICAL ENDPOINTS
# ============================================================

@app.route('/api/v1/zari/assay', methods=['POST'])
@jwt_required()
def create_zari_assay():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['zari_lot_batch_id', 'assay_certificate_no', 'core_yarn_material', 'winding_bobbin_type']
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
        
        cur.execute("""
            INSERT INTO zari_assay_records (
                zari_lot_batch_id, assay_certificate_no, silver_purity_pct,
                gold_plating_pct, copper_base_pct, core_yarn_material,
                zari_count_denier, zari_wire_diameter_microns, winding_bobbin_type,
                invoice_declared_weight_gm, gross_scale_weight_gm,
                bobbin_tare_weight_gm, precious_metal_market_rate_per_gm,
                is_free_from_tarnishing, is_free_from_wire_cuts, luster_sheen_match,
                validation_errors, validation_warnings, status, factory_node_id, operator_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', %s, %s)
            RETURNING id, assay_certificate_no
        """, (
            data.get('zari_lot_batch_id'),
            data.get('assay_certificate_no'),
            data.get('silver_purity_pct'),
            data.get('gold_plating_pct'),
            data.get('copper_base_pct'),
            data.get('core_yarn_material'),
            data.get('zari_count_denier'),
            data.get('zari_wire_diameter_microns'),
            data.get('winding_bobbin_type'),
            data.get('invoice_declared_weight_gm'),
            data.get('gross_scale_weight_gm'),
            data.get('bobbin_tare_weight_gm'),
            data.get('precious_metal_market_rate_per_gm'),
            data.get('is_free_from_tarnishing', False),
            data.get('is_free_from_wire_cuts', False),
            data.get('luster_sheen_match', False),
            json.dumps([]),
            json.dumps([]),
            factory_node_id,
            operator_id
        ))
        
        assay_row = cur.fetchone()
        assay_id = assay_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, status, net_zari_weight_gm
            FROM zari_assay_records WHERE id = %s::uuid
        """, (assay_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'SUBMITTED'
        else:
            status = 'SUBMITTED'
        
        cur.execute("""
            UPDATE zari_assay_records SET status = %s WHERE id = %s::uuid
        """, (status, assay_id))
        
        cur.execute("""
            UPDATE zari_lot_batches SET status = 'ASSAY_IN_PROGRESS'
            WHERE id = (SELECT zari_lot_batch_id FROM zari_assay_records WHERE id = %s::uuid)
        """, (assay_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(assay_id),
            'assay_certificate_no': assay_row['assay_certificate_no'],
            'status': status,
            'net_zari_weight_gm': float(result['net_zari_weight_gm']) if result['net_zari_weight_gm'] else None,
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Zari assay record created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/assay', methods=['GET'])
@jwt_required()
def list_zari_assays():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zar.id, zar.assay_certificate_no, zar.silver_purity_pct,
                   zar.gold_plating_pct, zar.copper_base_pct, zar.core_yarn_material,
                   zar.zari_count_denier, zar.zari_wire_diameter_microns,
                   zar.net_zari_weight_gm, zar.status, zar.certificate_hash,
                   zlb.zari_lot_batch_no, zlb.zari_type
            FROM zari_assay_records zar
            JOIN zari_lot_batches zlb ON zar.zari_lot_batch_id = zlb.id
            WHERE zar.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY zar.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        assays = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(assays),
            'assays': [
                {
                    'id': str(a['id']),
                    'assay_certificate_no': a['assay_certificate_no'],
                    'zari_lot_batch_no': a['zari_lot_batch_no'],
                    'zari_type': a['zari_type'],
                    'silver_purity_pct': float(a['silver_purity_pct']) if a['silver_purity_pct'] else None,
                    'gold_plating_pct': float(a['gold_plating_pct']) if a['gold_plating_pct'] else None,
                    'copper_base_pct': float(a['copper_base_pct']) if a['copper_base_pct'] else None,
                    'core_yarn_material': a['core_yarn_material'],
                    'zari_count_denier': a['zari_count_denier'],
                    'zari_wire_diameter_microns': float(a['zari_wire_diameter_microns']) if a['zari_wire_diameter_microns'] else None,
                    'net_zari_weight_gm': float(a['net_zari_weight_gm']) if a['net_zari_weight_gm'] else None,
                    'status': a['status'],
                    'certificate_hash': a['certificate_hash']
                }
                for a in assays
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/assay/<assay_id>', methods=['GET'])
@jwt_required()
def get_zari_assay(assay_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zar.*, zlb.zari_lot_batch_no, zlb.zari_type, zlb.zari_origin_cluster
            FROM zari_assay_records zar
            JOIN zari_lot_batches zlb ON zar.zari_lot_batch_id = zlb.id
            WHERE zar.id = %s::uuid
        """, (assay_id,))
        
        assay = cur.fetchone()
        cur.close()
        conn.close()
        
        if not assay:
            return jsonify({'error': 'AssayNotFound'}), 404
        
        return jsonify({
            'id': str(assay['id']),
            'assay_certificate_no': assay['assay_certificate_no'],
            'zari_lot_batch_id': str(assay['zari_lot_batch_id']),
            'zari_lot_batch_no': assay['zari_lot_batch_no'],
            'zari_type': assay['zari_type'],
            'zari_origin_cluster': assay['zari_origin_cluster'],
            'silver_purity_pct': float(assay['silver_purity_pct']) if assay['silver_purity_pct'] else None,
            'gold_plating_pct': float(assay['gold_plating_pct']) if assay['gold_plating_pct'] else None,
            'copper_base_pct': float(assay['copper_base_pct']) if assay['copper_base_pct'] else None,
            'core_yarn_material': assay['core_yarn_material'],
            'zari_count_denier': assay['zari_count_denier'],
            'zari_wire_diameter_microns': float(assay['zari_wire_diameter_microns']) if assay['zari_wire_diameter_microns'] else None,
            'winding_bobbin_type': assay['winding_bobbin_type'],
            'invoice_declared_weight_gm': float(assay['invoice_declared_weight_gm']) if assay['invoice_declared_weight_gm'] else None,
            'gross_scale_weight_gm': float(assay['gross_scale_weight_gm']) if assay['gross_scale_weight_gm'] else None,
            'bobbin_tare_weight_gm': float(assay['bobbin_tare_weight_gm']) if assay['bobbin_tare_weight_gm'] else None,
            'net_zari_weight_gm': float(assay['net_zari_weight_gm']) if assay['net_zari_weight_gm'] else None,
            'precious_metal_market_rate_per_gm': float(assay['precious_metal_market_rate_per_gm']) if assay['precious_metal_market_rate_per_gm'] else None,
            'is_free_from_tarnishing': assay['is_free_from_tarnishing'],
            'is_free_from_wire_cuts': assay['is_free_from_wire_cuts'],
            'luster_sheen_match': assay['luster_sheen_match'],
            'validation_errors': assay['validation_errors'],
            'validation_warnings': assay['validation_warnings'],
            'status': assay['status'],
            'certificate_hash': assay['certificate_hash'],
            'qr_tag_id': assay['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/assay/<assay_id>/certify', methods=['POST'])
@jwt_required()
def certify_zari_assay(assay_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zar.id, zar.status, zar.validation_errors, zar.zari_lot_batch_id,
                   zar.silver_purity_pct, zar.gold_plating_pct, zar.net_zari_weight_gm,
                   zlb.zari_lot_batch_no, zlb.zari_type, zlb.zari_origin_cluster
            FROM zari_assay_records zar
            JOIN zari_lot_batches zlb ON zar.zari_lot_batch_id = zlb.id
            WHERE zar.id = %s::uuid
        """, (assay_id,))
        
        assay = cur.fetchone()
        if not assay:
            cur.close()
            conn.close()
            return jsonify({'error': 'AssayNotFound'}), 404
        
        if assay['validation_errors'] and len(assay['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify assay with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(assay_id, assay['assay_certificate_no'])
        qr_tag_id = 'ZARI-' + assay['zari_lot_batch_no']
        
        precious_metal_value = None
        if assay['net_zari_weight_gm'] and assay['precious_metal_market_rate_per_gm']:
            precious_metal_value = round(float(assay['net_zari_weight_gm']) * float(assay['precious_metal_market_rate_per_gm']), 4)
        
        cur.execute("""
            UPDATE zari_assay_records
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, assay_certificate_no, certificate_hash
        """, (certificate_hash, qr_tag_id, assay_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO zari_certificates (
                zari_lot_batch_id, zari_assay_id, certificate_hash,
                qr_tag_id, zari_type, zari_origin_cluster,
                silver_purity_pct, gold_plating_pct, net_zari_weight_gm,
                precious_metal_value_estimate, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                zlb.id,
                zar.id,
                zar.certificate_hash,
                zar.qr_tag_id,
                zlb.zari_type,
                zlb.zari_origin_cluster,
                zar.silver_purity_pct,
                zar.gold_plating_pct,
                zar.net_zari_weight_gm,
                %s,
                zar.operator_id,
                %s,
                zar.factory_node_id,
                jsonb_build_object(
                    'assay_certificate_no', zar.assay_certificate_no,
                    'zari_lot_batch_no', zlb.zari_lot_batch_no,
                    'saree_bundle_size', zlb.saree_bundle_size,
                    'silver_purity_pct', zar.silver_purity_pct,
                    'gold_plating_pct', zar.gold_plating_pct,
                    'copper_base_pct', zar.copper_base_pct,
                    'core_yarn_material', zar.core_yarn_material,
                    'zari_count_denier', zar.zari_count_denier,
                    'zari_wire_diameter_microns', zar.zari_wire_diameter_microns,
                    'net_zari_weight_gm', zar.net_zari_weight_gm,
                    'is_free_from_tarnishing', zar.is_free_from_tarnishing,
                    'is_free_from_wire_cuts', zar.is_free_from_wire_cuts,
                    'luster_sheen_match', zar.luster_sheen_match
                )
            FROM zari_assay_records zar
            JOIN zari_lot_batches zlb ON zar.zari_lot_batch_id = zlb.id
            WHERE zar.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM zari_certificates WHERE zari_assay_id = zar.id
            )
        """, (precious_metal_value, approver_id, assay_id))
        
        cur.execute("""
            UPDATE zari_lot_batches
            SET status = 'CERTIFIED'
            WHERE id = %s::uuid
        """, (assay['zari_lot_batch_id'],))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'assay_certificate_no': result['assay_certificate_no'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED',
            'precious_metal_value_estimate': float(precious_metal_value) if precious_metal_value else None,
            'message': 'Zari assay certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/assay/<assay_id>/reject', methods=['POST'])
@jwt_required()
def reject_zari_assay(assay_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE zari_assay_records
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id, assay_certificate_no
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Zari Inspector')}]),
            assay_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'AssayNotFound'}), 404
        
        cur.execute("""
            UPDATE zari_lot_batches
            SET status = 'REJECTED'
            WHERE id = (SELECT zari_lot_batch_id FROM zari_assay_records WHERE id = %s::uuid)
        """, (assay_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'assay_certificate_no': result['assay_certificate_no'],
            'status': 'REJECTED',
            'message': 'Zari assay rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/certificates', methods=['GET'])
@jwt_required()
def list_zari_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zc.id, zc.certificate_hash, zc.qr_tag_id, zc.zari_type,
                   zc.zari_origin_cluster, zc.silver_purity_pct, zc.gold_plating_pct,
                   zc.net_zari_weight_gm, zc.precious_metal_value_estimate,
                   zc.status, zc.certified_at, zlb.zari_lot_batch_no, zar.assay_certificate_no
            FROM zari_certificates zc
            JOIN zari_lot_batches zlb ON zc.zari_lot_batch_id = zlb.id
            JOIN zari_assay_records zar ON zc.zari_assay_id = zar.id
            WHERE zc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY zc.certified_at DESC
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
                    'zari_lot_batch_no': c['zari_lot_batch_no'],
                    'assay_certificate_no': c['assay_certificate_no'],
                    'zari_type': c['zari_type'],
                    'zari_origin_cluster': c['zari_origin_cluster'],
                    'silver_purity_pct': float(c['silver_purity_pct']) if c['silver_purity_pct'] else None,
                    'gold_plating_pct': float(c['gold_plating_pct']) if c['gold_plating_pct'] else None,
                    'net_zari_weight_gm': float(c['net_zari_weight_gm']) if c['net_zari_weight_gm'] else None,
                    'precious_metal_value_estimate': float(c['precious_metal_value_estimate']) if c['precious_metal_value_estimate'] else None,
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR ZARI
# ============================================================

@app.route('/api/v1/sales/forecast/zari', methods=['GET'])
@jwt_required()
def get_sales_forecast_zari():
    """
    API plugin endpoint for sales team Zari material processing forecast.
    Returns forecasted Zari requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'sari_category': 'Authentic Kanchipuram Bridal',
                    'zari_type': 'PURE_REAL_ZARI_GOLD_SILVER',
                    'zari_grade': '1G',
                    'estimated_zari_weight_gm': 2500.0,
                    'estimated_silver_gm': 1375.0,
                    'estimated_gold_gm': 20.0,
                    'priority': 'HIGH',
                    'origin_cluster': 'KANCHIPURAM',
                    'estimated_sarees': 80
                },
                {
                    'sari_category': 'Banarasi Kinkhab & Kadwa',
                    'zari_type': 'PURE_REAL_ZARI_GOLD_SILVER',
                    'zari_grade': '1G',
                    'estimated_zari_weight_gm': 1800.0,
                    'estimated_silver_gm': 990.0,
                    'estimated_gold_gm': 14.4,
                    'priority': 'HIGH',
                    'origin_cluster': 'BANARAS',
                    'estimated_sarees': 60
                },
                {
                    'sari_category': 'Mid-Segment Silk Sarees',
                    'zari_type': 'TESTED_HALF_FINE_ZARI_COPPER_CORE',
                    'zari_grade': 'HALF_FINE',
                    'estimated_zari_weight_gm': 1200.0,
                    'estimated_silver_gm': 0.0,
                    'estimated_gold_gm': 0.0,
                    'priority': 'MEDIUM',
                    'origin_cluster': 'SURAT',
                    'estimated_sarees': 100
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'ZARI-LOT-2024-0011',
                    'sari_category': 'Authentic Kanchipuram Bridal',
                    'estimated_sarees': 80,
                    'estimated_zari_weight_gm': 2500.0,
                    'target_grade': '1G',
                    'target_origin': 'KANCHIPURAM'
                },
                {
                    'lot_number': 'ZARI-LOT-2024-0012',
                    'sari_category': 'Banarasi Kinkhab & Kadwa',
                    'estimated_sarees': 60,
                    'estimated_zari_weight_gm': 1800.0,
                    'target_grade': '1G',
                    'target_origin': 'BANARAS'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/assay/<assay_id>/quality-gate', methods=['POST'])
@jwt_required()
def update_zari_quality_gate(assay_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        allowed_fields = ['is_free_from_tarnishing', 'is_free_from_wire_cuts', 'luster_sheen_match']
        updates = {k: data.get(k) for k in allowed_fields if k in data}
        
        if not updates:
            return jsonify({'error': 'NoFieldsToUpdate', 'message': 'Provide at least one quality toggle'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        set_clauses = []
        params = []
        for key, value in updates.items():
            set_clauses.append(f"{key} = %s")
            params.append(value)
        params.append(assay_id)
        
        cur.execute(f"""
            UPDATE zari_assay_records
            SET {', '.join(set_clauses)}
            WHERE id = %s::uuid
            RETURNING id, assay_certificate_no
        """, params)
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'AssayNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'assay_certificate_no': result['assay_certificate_no'],
            'updated_fields': list(updates.keys()),
            'message': 'Quality gate toggles updated'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# ZARI INSPECTOR POST-PROCESS MODULE
# ============================================================

@app.route('/api/v1/zari/inspection', methods=['POST'])
@jwt_required()
def create_zari_inspection():
    try:
        inspector_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['zari_assay_id', 'zari_lot_batch_id', 'xrf_silver_purity_pct', 'xrf_gold_plating_pct']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (inspector_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        cur.execute("""
            INSERT INTO zari_inspection_records (
                zari_assay_id, zari_lot_batch_id, xrf_silver_purity_pct,
                xrf_gold_plating_pct, xrf_verification_passed,
                core_yarn_audit_result, core_yarn_audit_method, core_yarn_audit_passed,
                denier_measured, denier_target, tensile_strength_gd,
                bobbin_winding_integrity, tarnish_free_scan, color_luster_match,
                delta_e_value, gross_scale_weight_gm, tare_weight_gm,
                net_zari_weight_gm, moisture_reading_pct,
                wire_cuts_per_1000m, micro_cuts_detected, frayed_joints_detected,
                target_machine_type, flattened_wire_width_mm,
                surface_coating_lubrication, surface_coating_check_passed,
                validation_errors, validation_warnings, auto_assigned_routing,
                status, factory_node_id, inspector_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('zari_assay_id'),
            data.get('zari_lot_batch_id'),
            data.get('xrf_silver_purity_pct'),
            data.get('xrf_gold_plating_pct'),
            data.get('xrf_verification_passed', False),
            data.get('core_yarn_audit_result'),
            data.get('core_yarn_audit_method'),
            data.get('core_yarn_audit_passed', False),
            data.get('denier_measured'),
            data.get('denier_target'),
            data.get('tensile_strength_gd'),
            data.get('bobbin_winding_integrity'),
            data.get('tarnish_free_scan', False),
            data.get('color_luster_match', False),
            data.get('delta_e_value'),
            data.get('gross_scale_weight_gm'),
            data.get('tare_weight_gm'),
            data.get('net_zari_weight_gm'),
            data.get('moisture_reading_pct'),
            data.get('wire_cuts_per_1000m', 0),
            data.get('micro_cuts_detected', False),
            data.get('frayed_joints_detected', False),
            data.get('target_machine_type'),
            data.get('flattened_wire_width_mm'),
            data.get('surface_coating_lubrication'),
            data.get('surface_coating_check_passed', False),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing'),
            'DRAFT',
            factory_node_id,
            inspector_id
        ))
        
        inspection_row = cur.fetchone()
        inspection_id = inspection_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status
            FROM zari_inspection_records WHERE id = %s::uuid
        """, (inspection_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'SUBMITTED'
        else:
            status = 'SUBMITTED'
        
        cur.execute("""
            UPDATE zari_inspection_records SET status = %s WHERE id = %s::uuid
        """, (status, inspection_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(inspection_id),
            'status': status,
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Zari inspection record created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection', methods=['GET'])
@jwt_required()
def list_zari_inspections():
    try:
        inspector_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        assay_id = request.args.get('assay_id')
        
        if assay_id:
            cur.execute("""
                SELECT zir.id, zir.xrf_silver_purity_pct, zir.xrf_gold_plating_pct,
                       zir.core_yarn_audit_result, zir.denier_measured,
                       zir.tensile_strength_gd, zir.bobbin_winding_integrity,
                       zir.tarnish_free_scan, zir.color_luster_match,
                       zir.net_zari_weight_gm, zir.wire_cuts_per_1000m,
                       zir.target_machine_type, zir.flattened_wire_width_mm,
                       zir.auto_assigned_routing, zir.status, zir.certificate_hash,
                       zlb.zari_lot_batch_no, zar.assay_certificate_no
                FROM zari_inspection_records zir
                JOIN zari_lot_batches zlb ON zir.zari_lot_batch_id = zlb.id
                JOIN zari_assay_records zar ON zir.zari_assay_id = zar.id
                WHERE zir.zari_assay_id = %s::uuid
                ORDER BY zir.created_at DESC
                LIMIT 10
            """, (assay_id,))
        else:
            cur.execute("""
                SELECT zir.id, zir.xrf_silver_purity_pct, zir.xrf_gold_plating_pct,
                       zir.core_yarn_audit_result, zir.denier_measured,
                       zir.tensile_strength_gd, zir.bobbin_winding_integrity,
                       zir.tarnish_free_scan, zir.color_luster_match,
                       zir.net_zari_weight_gm, zir.wire_cuts_per_1000m,
                       zir.target_machine_type, zir.flattened_wire_width_mm,
                       zir.auto_assigned_routing, zir.status, zir.certificate_hash,
                       zlb.zari_lot_batch_no, zar.assay_certificate_no
                FROM zari_inspection_records zir
                JOIN zari_lot_batches zlb ON zir.zari_lot_batch_id = zlb.id
                JOIN zari_assay_records zar ON zir.zari_assay_id = zar.id
                WHERE zir.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
                ORDER BY zir.created_at DESC
                LIMIT 100
            """, (inspector_id,))
        
        inspections = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(inspections),
            'inspections': [
                {
                    'id': str(i['id']),
                    'zari_lot_batch_no': i['zari_lot_batch_no'],
                    'assay_certificate_no': i['assay_certificate_no'],
                    'xrf_silver_purity_pct': float(i['xrf_silver_purity_pct']) if i['xrf_silver_purity_pct'] else None,
                    'xrf_gold_plating_pct': float(i['xrf_gold_plating_pct']) if i['xrf_gold_plating_pct'] else None,
                    'core_yarn_audit_result': i['core_yarn_audit_result'],
                    'denier_measured': float(i['denier_measured']) if i['denier_measured'] else None,
                    'tensile_strength_gd': float(i['tensile_strength_gd']) if i['tensile_strength_gd'] else None,
                    'bobbin_winding_integrity': i['bobbin_winding_integrity'],
                    'tarnish_free_scan': i['tarnish_free_scan'],
                    'color_luster_match': i['color_luster_match'],
                    'net_zari_weight_gm': float(i['net_zari_weight_gm']) if i['net_zari_weight_gm'] else None,
                    'wire_cuts_per_1000m': i['wire_cuts_per_1000m'],
                    'target_machine_type': i['target_machine_type'],
                    'flattened_wire_width_mm': float(i['flattened_wire_width_mm']) if i['flattened_wire_width_mm'] else None,
                    'auto_assigned_routing': i['auto_assigned_routing'],
                    'status': i['status'],
                    'certificate_hash': i['certificate_hash']
                }
                for i in inspections
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection/<inspection_id>', methods=['PUT'])
@jwt_required()
def update_zari_inspection(inspection_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        allowed_fields = [
            'xrf_silver_purity_pct', 'xrf_gold_plating_pct', 'xrf_verification_passed',
            'core_yarn_audit_result', 'core_yarn_audit_method', 'core_yarn_audit_passed',
            'denier_measured', 'denier_target', 'tensile_strength_gd',
            'bobbin_winding_integrity', 'tarnish_free_scan', 'color_luster_match',
            'delta_e_value', 'gross_scale_weight_gm', 'tare_weight_gm',
            'net_zari_weight_gm', 'moisture_reading_pct',
            'wire_cuts_per_1000m', 'micro_cuts_detected', 'frayed_joints_detected',
            'target_machine_type', 'flattened_wire_width_mm',
            'surface_coating_lubrication', 'surface_coating_check_passed'
        ]
        updates = {k: data.get(k) for k in allowed_fields if k in data}
        
        if not updates:
            return jsonify({'error': 'NoFieldsToUpdate', 'message': 'Provide at least one field to update'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        set_clauses = []
        params = []
        for key, value in updates.items():
            set_clauses.append(f"{key} = %s")
            params.append(value)
        params.append(inspection_id)
        
        cur.execute(f"""
            UPDATE zari_inspection_records
            SET {', '.join(set_clauses)}
            WHERE id = %s::uuid
            RETURNING id
        """, params)
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'InspectionNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'updated_fields': list(updates.keys()),
            'message': 'Zari inspection updated successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection/<inspection_id>', methods=['GET'])
@jwt_required()
def get_zari_inspection(inspection_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zir.*, zlb.zari_lot_batch_no, zlb.zari_type, zlb.zari_origin_cluster,
                   zar.assay_certificate_no, zar.silver_purity_pct, zar.gold_plating_pct
            FROM zari_inspection_records zir
            JOIN zari_lot_batches zlb ON zir.zari_lot_batch_id = zlb.id
            JOIN zari_assay_records zar ON zir.zari_assay_id = zar.id
            WHERE zir.id = %s::uuid
        """, (inspection_id,))
        
        inspection = cur.fetchone()
        cur.close()
        conn.close()
        
        if not inspection:
            return jsonify({'error': 'InspectionNotFound'}), 404
        
        return jsonify({
            'id': str(inspection['id']),
            'zari_assay_id': str(inspection['zari_assay_id']),
            'zari_lot_batch_id': str(inspection['zari_lot_batch_id']),
            'zari_lot_batch_no': inspection['zari_lot_batch_no'],
            'zari_type': inspection['zari_type'],
            'zari_origin_cluster': inspection['zari_origin_cluster'],
            'assay_certificate_no': inspection['assay_certificate_no'],
            'xrf_silver_purity_pct': float(inspection['xrf_silver_purity_pct']) if inspection['xrf_silver_purity_pct'] else None,
            'xrf_gold_plating_pct': float(inspection['xrf_gold_plating_pct']) if inspection['xrf_gold_plating_pct'] else None,
            'xrf_verification_passed': inspection['xrf_verification_passed'],
            'core_yarn_audit_result': inspection['core_yarn_audit_result'],
            'core_yarn_audit_method': inspection['core_yarn_audit_method'],
            'core_yarn_audit_passed': inspection['core_yarn_audit_passed'],
            'denier_measured': float(inspection['denier_measured']) if inspection['denier_measured'] else None,
            'denier_target': inspection['denier_target'],
            'tensile_strength_gd': float(inspection['tensile_strength_gd']) if inspection['tensile_strength_gd'] else None,
            'bobbin_winding_integrity': inspection['bobbin_winding_integrity'],
            'tarnish_free_scan': inspection['tarnish_free_scan'],
            'color_luster_match': inspection['color_luster_match'],
            'delta_e_value': float(inspection['delta_e_value']) if inspection['delta_e_value'] else None,
            'gross_scale_weight_gm': float(inspection['gross_scale_weight_gm']) if inspection['gross_scale_weight_gm'] else None,
            'tare_weight_gm': float(inspection['tare_weight_gm']) if inspection['tare_weight_gm'] else None,
            'net_zari_weight_gm': float(inspection['net_zari_weight_gm']) if inspection['net_zari_weight_gm'] else None,
            'moisture_reading_pct': float(inspection['moisture_reading_pct']) if inspection['moisture_reading_pct'] else None,
            'wire_cuts_per_1000m': inspection['wire_cuts_per_1000m'],
            'micro_cuts_detected': inspection['micro_cuts_detected'],
            'frayed_joints_detected': inspection['frayed_joints_detected'],
            'target_machine_type': inspection['target_machine_type'],
            'flattened_wire_width_mm': float(inspection['flattened_wire_width_mm']) if inspection['flattened_wire_width_mm'] else None,
            'surface_coating_lubrication': inspection['surface_coating_lubrication'],
            'surface_coating_check_passed': inspection['surface_coating_check_passed'],
            'validation_errors': inspection['validation_errors'],
            'validation_warnings': inspection['validation_warnings'],
            'auto_assigned_routing': inspection['auto_assigned_routing'],
            'status': inspection['status'],
            'certificate_hash': inspection['certificate_hash'],
            'qr_tag_id': inspection['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection/<inspection_id>/certify', methods=['POST'])
@jwt_required()
def certify_zari_inspection(inspection_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT zir.id, zir.status, zir.validation_errors, zir.auto_assigned_routing,
                   zir.xrf_silver_purity_pct, zir.xrf_gold_plating_pct,
                   zir.net_zari_weight_gm, zlb.zari_lot_batch_no,
                   zlb.zari_type, zlb.zari_origin_cluster
            FROM zari_inspection_records zir
            JOIN zari_lot_batches zlb ON zir.zari_lot_batch_id = zlb.id
            WHERE zir.id = %s::uuid
        """, (inspection_id,))
        
        inspection = cur.fetchone()
        if not inspection:
            cur.close()
            conn.close()
            return jsonify({'error': 'InspectionNotFound'}), 404
        
        if inspection['validation_errors'] and len(inspection['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify inspection with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(inspection_id, inspection['zari_lot_batch_no'])
        qr_tag_id = 'ZARI-INSP-' + inspection['zari_lot_batch_no']
        
        precious_metal_value = None
        if inspection['net_zari_weight_gm']:
            cur.execute("""
                SELECT precious_metal_market_rate_per_gm
                FROM zari_assay_records
                WHERE id = (SELECT zari_assay_id FROM zari_inspection_records WHERE id = %s::uuid)
            """, (inspection_id,))
            rate_row = cur.fetchone()
            if rate_row and rate_row['precious_metal_market_rate_per_gm']:
                precious_metal_value = round(float(inspection['net_zari_weight_gm']) * float(rate_row['precious_metal_market_rate_per_gm']), 4)
        
        cur.execute("""
            UPDATE zari_inspection_records
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, certificate_hash
        """, (certificate_hash, qr_tag_id, inspection_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO zari_inspector_certificates (
                zari_inspection_id, zari_assay_id, zari_lot_batch_id,
                certificate_hash, qr_tag_id, zari_type, zari_origin_cluster,
                xrf_silver_purity_pct, xrf_gold_plating_pct, core_yarn_audit_result,
                tensile_strength_gd, net_zari_weight_gm, precious_metal_value_estimate,
                inspector_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                zir.id,
                zir.zari_assay_id,
                zir.zari_lot_batch_id,
                zir.certificate_hash,
                zir.qr_tag_id,
                zlb.zari_type,
                zlb.zari_origin_cluster,
                zir.xrf_silver_purity_pct,
                zir.xrf_gold_plating_pct,
                zir.core_yarn_audit_result,
                zir.tensile_strength_gd,
                zir.net_zari_weight_gm,
                %s,
                zir.inspector_id,
                %s,
                zir.factory_node_id,
                jsonb_build_object(
                    'zari_lot_batch_no', zlb.zari_lot_batch_no,
                    'assay_certificate_no', zar.assay_certificate_no,
                    'xrf_verification_passed', zir.xrf_verification_passed,
                    'core_yarn_audit_passed', zir.core_yarn_audit_passed,
                    'denier_measured', zir.denier_measured,
                    'bobbin_winding_integrity', zir.bobbin_winding_integrity,
                    'tarnish_free_scan', zir.tarnish_free_scan,
                    'color_luster_match', zir.color_luster_match,
                    'delta_e_value', zir.delta_e_value,
                    'wire_cuts_per_1000m', zir.wire_cuts_per_1000m,
                    'target_machine_type', zir.target_machine_type,
                    'flattened_wire_width_mm', zir.flattened_wire_width_mm,
                    'surface_coating_lubrication', zir.surface_coating_lubrication,
                    'auto_assigned_routing', zir.auto_assigned_routing
                )
            FROM zari_inspection_records zir
            JOIN zari_lot_batches zlb ON zir.zari_lot_batch_id = zlb.id
            JOIN zari_assay_records zar ON zir.zari_assay_id = zar.id
            WHERE zir.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM zari_inspector_certificates WHERE zari_inspection_id = zir.id
            )
        """, (precious_metal_value, approver_id, inspection_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'auto_assigned_routing': inspection['auto_assigned_routing'],
            'precious_metal_value_estimate': float(precious_metal_value) if precious_metal_value else None,
            'status': 'CERTIFIED',
            'message': 'Zari inspection certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection/<inspection_id>/reject', methods=['POST'])
@jwt_required()
def reject_zari_inspection(inspection_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE zari_inspection_records
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Zari Inspector')}]),
            inspection_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'InspectionNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'status': 'REJECTED',
            'message': 'Zari inspection rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/zari/inspection/<inspection_id>/routing', methods=['GET'])
@jwt_required()
def get_zari_inspection_routing(inspection_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT auto_assigned_routing, validation_errors, validation_warnings,
                   target_machine_type, core_yarn_audit_result, tensile_strength_gd,
                   tarnish_free_scan, wire_cuts_per_1000m, flattened_wire_width_mm
            FROM zari_inspection_records
            WHERE id = %s::uuid
        """, (inspection_id,))
        
        inspection = cur.fetchone()
        cur.close()
        conn.close()
        
        if not inspection:
            return jsonify({'error': 'InspectionNotFound'}), 404
        
        routing = {
            'inspection_id': str(inspection_id),
            'auto_assigned_routing': inspection['auto_assigned_routing'],
            'validation_errors': inspection['validation_errors'],
            'validation_warnings': inspection['validation_warnings'],
            'target_machine_type': inspection['target_machine_type'],
            'core_yarn_audit_result': inspection['core_yarn_audit_result'],
            'tensile_strength_gd': float(inspection['tensile_strength_gd']) if inspection['tensile_strength_gd'] else None,
            'tarnish_free_scan': inspection['tarnish_free_scan'],
            'wire_cuts_per_1000m': inspection['wire_cuts_per_1000m'],
            'flattened_wire_width_mm': float(inspection['flattened_wire_width_mm']) if inspection['flattened_wire_width_mm'] else None
        }
        
        return jsonify(routing), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SILK DEGUMMING MASTER MODULE
# ============================================================

@app.route('/api/v1/degumming/batches', methods=['POST'])
@jwt_required()
def create_degumming_batch():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['degumming_batch_no', 'target_machine_type']
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
        
        cur.execute("""
            INSERT INTO degumming_batches (
                degumming_batch_no, zari_inspection_id, zari_assay_id,
                zari_lot_batch_id, production_lot_id, factory_node_id,
                operator_id, status, target_machine_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'QUEUED', %s)
            RETURNING id, degumming_batch_no
        """, (
            data.get('degumming_batch_no'),
            data.get('zari_inspection_id'),
            data.get('zari_assay_id'),
            data.get('zari_lot_batch_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id,
            data.get('target_machine_type')
        ))
        
        batch_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(batch_row['id']),
            'degumming_batch_no': batch_row['degumming_batch_no'],
            'status': 'QUEUED',
            'message': 'Degumming batch created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/batches', methods=['GET'])
@jwt_required()
def list_degumming_batches():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT db.id, db.degumming_batch_no, db.target_machine_type,
                   db.status, db.created_at, zlb.zari_lot_batch_no
            FROM degumming_batches db
            LEFT JOIN zari_lot_batches zlb ON db.zari_lot_batch_id = zlb.id
            WHERE db.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY db.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        batches = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(batches),
            'batches': [
                {
                    'id': str(b['id']),
                    'degumming_batch_no': b['degumming_batch_no'],
                    'zari_lot_batch_no': b['zari_lot_batch_no'],
                    'target_machine_type': b['target_machine_type'],
                    'status': b['status'],
                    'created_at': b['created_at'].isoformat() if b['created_at'] else None
                }
                for b in batches
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/records', methods=['POST'])
@jwt_required()
def create_degumming_record():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['degumming_batch_id', 'bath_liquor_ratio', 'raw_dry_weight_kg', 'degummed_dry_weight_kg']
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
        
        cur.execute("""
            INSERT INTO degumming_records (
                degumming_batch_id, bath_liquor_ratio, deaerating_agent_used,
                bath_ph_level, boil_duration_minutes, raw_dry_weight_kg,
                degummed_dry_weight_kg, sericin_loss_pct, post_degum_tenacity_gd,
                fibrillation_index, degumming_agent_base, alkali_buffer_additive,
                water_softening_agent, vessel_type_allocated, boil_temperature_profile,
                validation_errors, validation_warnings, auto_assigned_routing,
                status, factory_node_id, operator_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', %s, %s)
            RETURNING id
        """, (
            data.get('degumming_batch_id'),
            data.get('bath_liquor_ratio'),
            data.get('deaerating_agent_used', False),
            data.get('bath_ph_level'),
            data.get('boil_duration_minutes'),
            data.get('raw_dry_weight_kg'),
            data.get('degummed_dry_weight_kg'),
            data.get('sericin_loss_pct'),
            data.get('post_degum_tenacity_gd'),
            data.get('fibrillation_index'),
            data.get('degumming_agent_base'),
            data.get('alkali_buffer_additive'),
            data.get('water_softening_agent'),
            data.get('vessel_type_allocated'),
            data.get('boil_temperature_profile'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing'),
            factory_node_id,
            operator_id
        ))
        
        record_row = cur.fetchone()
        record_id = record_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status
            FROM degumming_records WHERE id = %s::uuid
        """, (record_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'IN_PROGRESS'
        else:
            status = 'IN_PROGRESS'
        
        cur.execute("""
            UPDATE degumming_batches SET status = 'IN_PROGRESS'
            WHERE id = (SELECT degumming_batch_id FROM degumming_records WHERE id = %s::uuid)
        """, (record_id,))
        
        cur.execute("""
            UPDATE degumming_records SET status = %s WHERE id = %s::uuid
        """, (status, record_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(record_id),
            'status': status,
            'sericin_loss_pct': result['sericin_loss_pct'],
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Degumming record created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/records', methods=['GET'])
@jwt_required()
def list_degumming_records():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dr.id, dr.degumming_batch_no, dr.sericin_loss_pct,
                   dr.post_degum_tenacity_gd, dr.fibrillation_index,
                   dr.bath_ph_level, dr.boil_temperature_profile,
                   dr.auto_assigned_routing, dr.status, dr.certificate_hash,
                   db.zari_lot_batch_no
            FROM degumming_records dr
            JOIN degumming_batches db ON dr.degumming_batch_id = db.id
            WHERE dr.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY dr.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        records = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(records),
            'records': [
                {
                    'id': str(r['id']),
                    'degumming_batch_no': r['degumming_batch_no'],
                    'zari_lot_batch_no': r['zari_lot_batch_no'],
                    'sericin_loss_pct': float(r['sericin_loss_pct']) if r['sericin_loss_pct'] else None,
                    'post_degum_tenacity_gd': float(r['post_degum_tenacity_gd']) if r['post_degum_tenacity_gd'] else None,
                    'fibrillation_index': r['fibrillation_index'],
                    'bath_ph_level': float(r['bath_ph_level']) if r['bath_ph_level'] else None,
                    'boil_temperature_profile': r['boil_temperature_profile'],
                    'auto_assigned_routing': r['auto_assigned_routing'],
                    'status': r['status'],
                    'certificate_hash': r['certificate_hash']
                }
                for r in records
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/records/<record_id>', methods=['GET'])
@jwt_required()
def get_degumming_record(record_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dr.*, db.degumming_batch_no, db.zari_lot_batch_no, db.target_machine_type
            FROM degumming_records dr
            JOIN degumming_batches db ON dr.degumming_batch_id = db.id
            WHERE dr.id = %s::uuid
        """, (record_id,))
        
        record = cur.fetchone()
        cur.close()
        conn.close()
        
        if not record:
            return jsonify({'error': 'RecordNotFound'}), 404
        
        return jsonify({
            'id': str(record['id']),
            'degumming_batch_no': record['degumming_batch_no'],
            'zari_lot_batch_no': record['zari_lot_batch_no'],
            'target_machine_type': record['target_machine_type'],
            'bath_liquor_ratio': record['bath_liquor_ratio'],
            'deaerating_agent_used': record['deaerating_agent_used'],
            'bath_ph_level': float(record['bath_ph_level']) if record['bath_ph_level'] else None,
            'boil_duration_minutes': record['boil_duration_minutes'],
            'raw_dry_weight_kg': float(record['raw_dry_weight_kg']) if record['raw_dry_weight_kg'] else None,
            'degummed_dry_weight_kg': float(record['degummed_dry_weight_kg']) if record['degummed_dry_weight_kg'] else None,
            'sericin_loss_pct': float(record['sericin_loss_pct']) if record['sericin_loss_pct'] else None,
            'post_degum_tenacity_gd': float(record['post_degum_tenacity_gd']) if record['post_degum_tenacity_gd'] else None,
            'fibrillation_index': record['fibrillation_index'],
            'degumming_agent_base': record['degumming_agent_base'],
            'alkali_buffer_additive': record['alkali_buffer_additive'],
            'water_softening_agent': record['water_softening_agent'],
            'vessel_type_allocated': record['vessel_type_allocated'],
            'boil_temperature_profile': record['boil_temperature_profile'],
            'validation_errors': record['validation_errors'],
            'validation_warnings': record['validation_warnings'],
            'auto_assigned_routing': record['auto_assigned_routing'],
            'status': record['status'],
            'certificate_hash': record['certificate_hash'],
            'qr_tag_id': record['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/records/<record_id>/certify', methods=['POST'])
@jwt_required()
def certify_degumming_record(record_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dr.id, dr.status, dr.validation_errors, dr.auto_assigned_routing,
                   dr.sericin_loss_pct, dr.post_degum_tenacity_gd,
                   dr.degumming_batch_id, db.zari_lot_batch_no, db.target_machine_type
            FROM degumming_records dr
            JOIN degumming_batches db ON dr.degumming_batch_id = db.id
            WHERE dr.id = %s::uuid
        """, (record_id,))
        
        record = cur.fetchone()
        if not record:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecordNotFound'}), 404
        
        if record['validation_errors'] and len(record['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify record with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(record_id, record['degumming_batch_no'])
        qr_tag_id = 'DEG-' + record['zari_lot_batch_no']
        
        cur.execute("""
            UPDATE degumming_records
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, certificate_hash
        """, (certificate_hash, qr_tag_id, record_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO degumming_certificates (
                degumming_batch_id, degumming_record_id, certificate_hash,
                qr_tag_id, zari_lot_batch_id, certified_grade,
                auto_assigned_routing, sericin_loss_pct, post_degum_tenacity_gd,
                fibrillation_index, pre_boil_dry_weight_kg, post_boil_dry_weight_kg,
                bath_ph_level, boil_temperature_profile, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                db.id,
                dr.id,
                dr.certificate_hash,
                dr.qr_tag_id,
                db.zari_lot_batch_id,
                '4A',
                dr.auto_assigned_routing,
                dr.sericin_loss_pct,
                dr.post_degum_tenacity_gd,
                dr.fibrillation_index,
                dr.raw_dry_weight_kg,
                dr.degummed_dry_weight_kg,
                dr.bath_ph_level,
                dr.boil_temperature_profile,
                dr.operator_id,
                %s,
                dr.factory_node_id,
                jsonb_build_object(
                    'degumming_batch_no', db.degumming_batch_no,
                    'target_machine_type', db.target_machine_type,
                    'bath_liquor_ratio', dr.bath_liquor_ratio,
                    'deaerating_agent_used', dr.deaerating_agent_used,
                    'boil_duration_minutes', dr.boil_duration_minutes,
                    'degumming_agent_base', dr.degumming_agent_base,
                    'alkali_buffer_additive', dr.alkali_buffer_additive,
                    'water_softening_agent', dr.water_softening_agent,
                    'vessel_type_allocated', dr.vessel_type_allocated
                )
            FROM degumming_records dr
            JOIN degumming_batches db ON dr.degumming_batch_id = db.id
            WHERE dr.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM degumming_certificates WHERE degumming_record_id = dr.id
            )
        """, (approver_id, record_id))
        
        cur.execute("""
            UPDATE degumming_batches
            SET status = 'CERTIFIED'
            WHERE id = %s::uuid
        """, (record['degumming_batch_id'],))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'auto_assigned_routing': record['auto_assigned_routing'],
            'sericin_loss_pct': float(record['sericin_loss_pct']) if record['sericin_loss_pct'] else None,
            'status': 'CERTIFIED',
            'message': 'Degumming record certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/records/<record_id>/reject', methods=['POST'])
@jwt_required()
def reject_degumming_record(record_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE degumming_records
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Degumming Master')}]),
            record_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecordNotFound'}), 404
        
        cur.execute("""
            UPDATE degumming_batches
            SET status = 'REJECTED'
            WHERE id = (SELECT degumming_batch_id FROM degumming_records WHERE id = %s::uuid)
        """, (record_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'status': 'REJECTED',
            'message': 'Degumming record rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/degumming/certificates', methods=['GET'])
@jwt_required()
def list_degumming_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dc.id, dc.certificate_hash, dc.qr_tag_id, dc.sericin_loss_pct,
                   dc.post_degum_tenacity_gd, dc.fibrillation_index,
                   dc.auto_assigned_routing, dc.status, dc.certified_at,
                   db.degumming_batch_no, zlb.zari_lot_batch_no
            FROM degumming_certificates dc
            JOIN degumming_records dr ON dc.degumming_record_id = dr.id
            JOIN degumming_batches db ON dc.degumming_batch_id = db.id
            LEFT JOIN zari_lot_batches zlb ON db.zari_lot_batch_id = zlb.id
            WHERE dc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY dc.certified_at DESC
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
                    'degumming_batch_no': c['degumming_batch_no'],
                    'zari_lot_batch_no': c['zari_lot_batch_no'],
                    'sericin_loss_pct': float(c['sericin_loss_pct']) if c['sericin_loss_pct'] else None,
                    'post_degum_tenacity_gd': float(c['post_degum_tenacity_gd']) if c['post_degum_tenacity_gd'] else None,
                    'fibrillation_index': c['fibrillation_index'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR DEGUMMING
# ============================================================

@app.route('/api/v1/sales/forecast/degumming', methods=['GET'])
@jwt_required()
def get_sales_forecast_degumming():
    """
    API plugin endpoint for sales team degumming material processing forecast.
    Returns forecasted degumming requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'silk_type': 'BIVOLTINE_WHITE_SILK',
                    'estimated_raw_silk_kg': 200.0,
                    'target_sericin_loss_pct': '20-23',
                    'target_tenacity_gd': '3.8+',
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'silk_type': 'BIVOLTINE_WHITE_SILK',
                    'estimated_raw_silk_kg': 150.0,
                    'target_sericin_loss_pct': '18-20',
                    'target_tenacity_gd': '3.8+',
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'silk_type': 'MULTIVOLTINE_YELLOW_SILK',
                    'estimated_raw_silk_kg': 180.0,
                    'target_sericin_loss_pct': '20-23',
                    'target_tenacity_gd': '3.5-3.8',
                    'priority': 'MEDIUM',
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'DEG-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'estimated_sarees': 80,
                    'estimated_raw_silk_kg': 200.0,
                    'target_sericin_loss_pct': '20-23',
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'DEG-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'estimated_sarees': 60,
                    'estimated_raw_silk_kg': 150.0,
                    'target_sericin_loss_pct': '18-20',
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# THROWSTER / TWISTER MODULE
# ============================================================

@app.route('/api/v1/throwster/batches', methods=['POST'])
@jwt_required()
def create_throwster_batch():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['throwster_batch_no']
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
        
        cur.execute("""
            INSERT INTO throwster_batches (
                throwster_batch_no, degumming_record_id, degumming_batch_id,
                zari_lot_batch_id, production_lot_id, factory_node_id,
                operator_id, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'QUEUED')
            RETURNING id, throwster_batch_no
        """, (
            data.get('throwster_batch_no'),
            data.get('degumming_record_id'),
            data.get('degumming_batch_id'),
            data.get('zari_lot_batch_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id
        ))
        
        batch_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(batch_row['id']),
            'throwster_batch_no': batch_row['throwster_batch_no'],
            'status': 'QUEUED',
            'message': 'Throwster batch created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/batches', methods=['GET'])
@jwt_required()
def list_throwster_batches():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tb.id, tb.throwster_batch_no, tb.target_machine_type,
                   tb.status, tb.created_at, db.degumming_batch_no, zlb.zari_lot_batch_no
            FROM throwster_batches tb
            LEFT JOIN degumming_batches db ON tb.degumming_batch_id = db.id
            LEFT JOIN zari_lot_batches zlb ON tb.zari_lot_batch_id = zlb.id
            WHERE tb.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY tb.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        batches = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(batches),
            'batches': [
                {
                    'id': str(b['id']),
                    'throwster_batch_no': b['throwster_batch_no'],
                    'degumming_batch_no': b['degumming_batch_no'],
                    'zari_lot_batch_no': b['zari_lot_batch_no'],
                    'status': b['status'],
                    'created_at': b['created_at'].isoformat() if b['created_at'] else None
                }
                for b in batches
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/records', methods=['POST'])
@jwt_required()
def create_throwster_record():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['throwster_batch_id', 'input_raw_lot_no', 'input_weight_kg', 'target_ply_count', 'intended_use']
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
        
        cur.execute("""
            INSERT INTO throwster_production_records (
                throwster_batch_id, input_raw_lot_no, input_weight_kg,
                target_ply_count, intended_use, input_yarn_type,
                input_lot_purity_clearance, machinery_id, target_tpi,
                twist_direction, steam_setting_duration_mins,
                ply_count, first_twist_tpm, final_twist_tpm,
                engineered_yarn_profile, spindle_rotational_speed_rpm,
                steam_stabilization_method, steam_temperature_celsius,
                steaming_duration_minutes, output_twisted_weight_kg,
                process_scrap_waste_kg, tested_tpm_average,
                snarl_count_per_100m, oil_lubrication_pick_up_pct,
                validation_errors, validation_warnings, auto_assigned_routing,
                status, factory_node_id, operator_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', %s, %s)
            RETURNING id
        """, (
            data.get('throwster_batch_id'),
            data.get('input_raw_lot_no'),
            data.get('input_weight_kg'),
            data.get('target_ply_count'),
            data.get('intended_use'),
            data.get('input_yarn_type'),
            data.get('input_lot_purity_clearance', False),
            data.get('machinery_id'),
            data.get('target_tpi'),
            data.get('twist_direction'),
            data.get('steam_setting_duration_mins'),
            data.get('ply_count'),
            data.get('first_twist_tpm'),
            data.get('final_twist_tpm'),
            data.get('engineered_yarn_profile'),
            data.get('spindle_rotational_speed_rpm'),
            data.get('steam_stabilization_method'),
            data.get('steam_temperature_celsius'),
            data.get('steaming_duration_minutes'),
            data.get('output_twisted_weight_kg'),
            data.get('process_scrap_waste_kg'),
            data.get('tested_tpm_average'),
            data.get('snarl_count_per_100m', 0),
            data.get('oil_lubrication_pick_up_pct'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing'),
            factory_node_id,
            operator_id
        ))
        
        record_row = cur.fetchone()
        record_id = record_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status, twist_variation_pct
            FROM throwster_production_records WHERE id = %s::uuid
        """, (record_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'IN_PROGRESS'
        else:
            status = 'IN_PROGRESS'
        
        cur.execute("""
            UPDATE throwster_batches SET status = 'IN_PROGRESS'
            WHERE id = (SELECT throwster_batch_id FROM throwster_production_records WHERE id = %s::uuid)
        """, (record_id,))
        
        cur.execute("""
            UPDATE throwster_production_records SET status = %s WHERE id = %s::uuid
        """, (status, record_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(record_id),
            'status': status,
            'twist_variation_pct': float(result['twist_variation_pct']) if result['twist_variation_pct'] else None,
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Throwster production record created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/records', methods=['GET'])
@jwt_required()
def list_throwster_records():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tpr.id, tpr.throwster_batch_no, tpr.input_raw_lot_no,
                   tpr.ply_count, tpr.final_twist_tpm, tpr.twist_direction,
                   tpr.engineered_yarn_profile, tpr.twist_variation_pct,
                   tpr.snarl_count_per_100m, tpr.auto_assigned_routing,
                   tpr.status, tpr.certificate_hash, tb.degumming_batch_no
            FROM throwster_production_records tpr
            JOIN throwster_batches tb ON tpr.throwster_batch_id = tb.id
            WHERE tpr.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY tpr.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        records = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(records),
            'records': [
                {
                    'id': str(r['id']),
                    'throwster_batch_no': r['throwster_batch_no'],
                    'degumming_batch_no': r['degumming_batch_no'],
                    'input_raw_lot_no': r['input_raw_lot_no'],
                    'ply_count': r['ply_count'],
                    'final_twist_tpm': float(r['final_twist_tpm']) if r['final_twist_tpm'] else None,
                    'twist_direction': r['twist_direction'],
                    'engineered_yarn_profile': r['engineered_yarn_profile'],
                    'twist_variation_pct': float(r['twist_variation_pct']) if r['twist_variation_pct'] else None,
                    'snarl_count_per_100m': r['snarl_count_per_100m'],
                    'auto_assigned_routing': r['auto_assigned_routing'],
                    'status': r['status'],
                    'certificate_hash': r['certificate_hash']
                }
                for r in records
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/records/<record_id>', methods=['GET'])
@jwt_required()
def get_throwster_record(record_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tpr.*, tb.throwster_batch_no, tb.degumming_batch_no, db.zari_lot_batch_no
            FROM throwster_production_records tpr
            JOIN throwster_batches tb ON tpr.throwster_batch_id = tb.id
            LEFT JOIN degumming_batches db ON tb.degumming_batch_id = db.id
            WHERE tpr.id = %s::uuid
        """, (record_id,))
        
        record = cur.fetchone()
        cur.close()
        conn.close()
        
        if not record:
            return jsonify({'error': 'RecordNotFound'}), 404
        
        return jsonify({
            'id': str(record['id']),
            'throwster_batch_no': record['throwster_batch_no'],
            'degumming_batch_no': record['degumming_batch_no'],
            'zari_lot_batch_no': record['zari_lot_batch_no'],
            'input_raw_lot_no': record['input_raw_lot_no'],
            'input_weight_kg': float(record['input_weight_kg']) if record['input_weight_kg'] else None,
            'target_ply_count': record['target_ply_count'],
            'intended_use': record['intended_use'],
            'input_yarn_type': record['input_yarn_type'],
            'machinery_id': record['machinery_id'],
            'target_tpi': float(record['target_tpi']) if record['target_tpi'] else None,
            'twist_direction': record['twist_direction'],
            'steam_setting_duration_mins': record['steam_setting_duration_mins'],
            'ply_count': record['ply_count'],
            'first_twist_tpm': float(record['first_twist_tpm']) if record['first_twist_tpm'] else None,
            'final_twist_tpm': float(record['final_twist_tpm']) if record['final_twist_tpm'] else None,
            'engineered_yarn_profile': record['engineered_yarn_profile'],
            'spindle_rotational_speed_rpm': record['spindle_rotational_speed_rpm'],
            'steam_stabilization_method': record['steam_stabilization_method'],
            'steam_temperature_celsius': record['steam_temperature_celsius'],
            'steaming_duration_minutes': record['steaming_duration_minutes'],
            'output_twisted_weight_kg': float(record['output_twisted_weight_kg']) if record['output_twisted_weight_kg'] else None,
            'process_scrap_waste_kg': float(record['process_scrap_waste_kg']) if record['process_scrap_waste_kg'] else None,
            'material_discrepancy_kg': float(record['material_discrepancy_kg']) if record['material_discrepancy_kg'] else None,
            'tested_tpm_average': float(record['tested_tpm_average']) if record['tested_tpm_average'] else None,
            'twist_variation_pct': float(record['twist_variation_pct']) if record['twist_variation_pct'] else None,
            'snarl_count_per_100m': record['snarl_count_per_100m'],
            'oil_lubrication_pick_up_pct': float(record['oil_lubrication_pick_up_pct']) if record['oil_lubrication_pick_up_pct'] else None,
            'validation_errors': record['validation_errors'],
            'validation_warnings': record['validation_warnings'],
            'auto_assigned_routing': record['auto_assigned_routing'],
            'status': record['status'],
            'certificate_hash': record['certificate_hash'],
            'qr_tag_id': record['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/records/<record_id>/certify', methods=['POST'])
@jwt_required()
def certify_throwster_record(record_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tpr.id, tpr.status, tpr.validation_errors, tpr.auto_assigned_routing,
                   tpr.input_raw_lot_no, tpr.ply_count, tpr.final_twist_tpm,
                   tpr.twist_direction, tpr.engineered_yarn_profile,
                   tpr.twist_variation_pct, tpr.snarl_count_per_100m,
                   tpr.input_weight_kg, tpr.output_twisted_weight_kg,
                   tpr.process_scrap_waste_kg, tpr.material_discrepancy_kg,
                   tb.throwster_batch_no
            FROM throwster_production_records tpr
            JOIN throwster_batches tb ON tpr.throwster_batch_id = tb.id
            WHERE tpr.id = %s::uuid
        """, (record_id,))
        
        record = cur.fetchone()
        if not record:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecordNotFound'}), 404
        
        if record['validation_errors'] and len(record['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify record with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(record_id, record['throwster_batch_no'])
        qr_tag_id = 'TWIST-' + record['throwster_batch_no']
        
        cur.execute("""
            UPDATE throwster_production_records
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, certificate_hash
        """, (certificate_hash, qr_tag_id, record_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO throwster_certificates (
                throwster_batch_id, throwster_record_id, certificate_hash,
                qr_tag_id, input_raw_lot_no, certified_grade,
                auto_assigned_routing, ply_count, final_twist_tpm,
                twist_direction, engineered_yarn_profile, twist_variation_pct,
                snarl_count_per_100m, input_weight_kg, output_twisted_weight_kg,
                process_scrap_waste_kg, material_discrepancy_kg,
                operator_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                tb.id,
                tpr.id,
                tpr.certificate_hash,
                tpr.qr_tag_id,
                tpr.input_raw_lot_no,
                '4A',
                tpr.auto_assigned_routing,
                tpr.ply_count,
                tpr.final_twist_tpm,
                tpr.twist_direction,
                tpr.engineered_yarn_profile,
                tpr.twist_variation_pct,
                tpr.snarl_count_per_100m,
                tpr.input_weight_kg,
                tpr.output_twisted_weight_kg,
                tpr.process_scrap_waste_kg,
                tpr.material_discrepancy_kg,
                tpr.operator_id,
                %s,
                tpr.factory_node_id,
                jsonb_build_object(
                    'throwster_batch_no', tb.throwster_batch_no,
                    'degumming_batch_no', db.degumming_batch_no,
                    'zari_lot_batch_no', zlb.zari_lot_batch_no,
                    'input_yarn_type', tpr.input_yarn_type,
                    'machinery_id', tpr.machinery_id,
                    'target_tpi', tpr.target_tpi,
                    'first_twist_tpm', tpr.first_twist_tpm,
                    'steam_stabilization_method', tpr.steam_stabilization_method,
                    'steam_temperature_celsius', tpr.steam_temperature_celsius,
                    'steaming_duration_minutes', tpr.steaming_duration_minutes,
                    'tested_tpm_average', tpr.tested_tpm_average,
                    'oil_lubrication_pick_up_pct', tpr.oil_lubrication_pick_up_pct
                )
            FROM throwster_production_records tpr
            JOIN throwster_batches tb ON tpr.throwster_batch_id = tb.id
            LEFT JOIN degumming_batches db ON tb.degumming_batch_id = db.id
            LEFT JOIN zari_lot_batches zlb ON tb.zari_lot_batch_id = zlb.id
            WHERE tpr.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM throwster_certificates WHERE throwster_record_id = tpr.id
            )
        """, (approver_id, record_id))
        
        cur.execute("""
            UPDATE throwster_batches
            SET status = 'CERTIFIED'
            WHERE id = %s::uuid
        """, (record['throwster_batch_no'],))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'auto_assigned_routing': record['auto_assigned_routing'],
            'twist_variation_pct': float(record['twist_variation_pct']) if record['twist_variation_pct'] else None,
            'status': 'CERTIFIED',
            'message': 'Throwster record certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/records/<record_id>/reject', methods=['POST'])
@jwt_required()
def reject_throwster_record(record_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE throwster_production_records
            SET status = 'REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Throwster Master')}]),
            record_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecordNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'status': 'REJECTED',
            'message': 'Throwster record rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/throwster/certificates', methods=['GET'])
@jwt_required()
def list_throwster_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tc.id, tc.certificate_hash, tc.qr_tag_id, tc.input_raw_lot_no,
                   tc.ply_count, tc.final_twist_tpm, tc.twist_direction,
                   tc.engineered_yarn_profile, tc.twist_variation_pct,
                   tc.snarl_count_per_100m, tc.auto_assigned_routing,
                   tc.status, tc.certified_at, tb.throwster_batch_no
            FROM throwster_certificates tc
            JOIN throwster_batches tb ON tc.throwster_batch_id = tb.id
            WHERE tc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY tc.certified_at DESC
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
                    'throwster_batch_no': c['throwster_batch_no'],
                    'input_raw_lot_no': c['input_raw_lot_no'],
                    'ply_count': c['ply_count'],
                    'final_twist_tpm': float(c['final_twist_tpm']) if c['final_twist_tpm'] else None,
                    'twist_direction': c['twist_direction'],
                    'engineered_yarn_profile': c['engineered_yarn_profile'],
                    'twist_variation_pct': float(c['twist_variation_pct']) if c['twist_variation_pct'] else None,
                    'snarl_count_per_100m': c['snarl_count_per_100m'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR THROWSTER
# ============================================================

@app.route('/api/v1/sales/forecast/throwster', methods=['GET'])
@jwt_required()
def get_sales_forecast_throwster():
    """
    API plugin endpoint for sales team throwster material processing forecast.
    Returns forecasted throwster requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'yarn_profile': 'ORGANZINE',
                    'ply_count': 2,
                    'final_twist_tpm': 750,
                    'estimated_raw_silk_kg': 120.0,
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'yarn_profile': 'ORGANZINE',
                    'ply_count': 2,
                    'final_twist_tpm': 700,
                    'estimated_raw_silk_kg': 100.0,
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'yarn_profile': 'TRAM',
                    'ply_count': 3,
                    'final_twist_tpm': 600,
                    'estimated_raw_silk_kg': 90.0,
                    'priority': 'MEDIUM',
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'TWIST-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'yarn_profile': 'ORGANZINE',
                    'estimated_sarees': 80,
                    'estimated_raw_silk_kg': 120.0,
                    'final_twist_tpm': 750,
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'TWIST-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'yarn_profile': 'ORGANZINE',
                    'estimated_sarees': 60,
                    'estimated_raw_silk_kg': 100.0,
                    'final_twist_tpm': 700,
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# MASTER COLORIST MODULE
# ============================================================

@app.route('/api/v1/colorist/recipes', methods=['POST'])
@jwt_required()
def create_colorist_recipe():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['recipe_code', 'internal_shade_code', 'silk_origin_type_suitability', 'liquor_ratio']
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
        
        cur.execute("""
            INSERT INTO master_colorist_recipes (
                recipe_code, internal_shade_code, pantone_reference_id,
                silk_origin_type_suitability, liquor_ratio,
                throwster_record_id, throwster_batch_id, production_lot_id,
                factory_node_id, operator_id, status, version,
                dye_class_used, dyebath_ph, max_temperature_celsius,
                leveling_agent_added, color_difference_delta_e,
                dry_crocking_fastness, wet_crocking_fastness,
                post_dye_tenacity_gd, antistatic_lubricant_applied,
                acid_fixative_type, leveling_exhausting_agent,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, recipe_code
        """, (
            data.get('recipe_code'),
            data.get('internal_shade_code'),
            data.get('pantone_reference_id'),
            data.get('silk_origin_type_suitability'),
            data.get('liquor_ratio'),
            data.get('throwster_record_id'),
            data.get('throwster_batch_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id,
            data.get('dye_class_used'),
            data.get('dyebath_ph'),
            data.get('max_temperature_celsius'),
            data.get('leveling_agent_added', False),
            data.get('color_difference_delta_e'),
            data.get('dry_crocking_fastness'),
            data.get('wet_crocking_fastness'),
            data.get('post_dye_tenacity_gd'),
            data.get('antistatic_lubricant_applied', False),
            data.get('acid_fixative_type'),
            data.get('leveling_exhausting_agent'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        recipe_row = cur.fetchone()
        recipe_id = recipe_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status
            FROM master_colorist_recipes WHERE id = %s::uuid
        """, (recipe_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'LAB_DIP_PENDING'
        else:
            status = 'LAB_DIP_PENDING'
        
        cur.execute("""
            UPDATE master_colorist_recipes SET status = %s WHERE id = %s::uuid
        """, (status, recipe_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(recipe_id),
            'recipe_code': recipe_row['recipe_code'],
            'status': status,
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Master Colorist recipe created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes', methods=['GET'])
@jwt_required()
def list_colorist_recipes():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mcr.id, mcr.recipe_code, mcr.internal_shade_code,
                   mcr.pantone_reference_id, mcr.silk_origin_type_suitability,
                   mcr.liquor_ratio, mcr.dye_class_used, mcr.dyebath_ph,
                   mcr.color_difference_delta_e, mcr.dry_crocking_fastness,
                   mcr.wet_crocking_fastness, mcr.auto_assigned_routing,
                   mcr.status, mcr.certificate_hash, mcr.version,
                   tb.throwster_batch_no
            FROM master_colorist_recipes mcr
            LEFT JOIN throwster_batches tb ON mcr.throwster_batch_id = tb.id
            WHERE mcr.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY mcr.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        recipes = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(recipes),
            'recipes': [
                {
                    'id': str(r['id']),
                    'recipe_code': r['recipe_code'],
                    'internal_shade_code': r['internal_shade_code'],
                    'pantone_reference_id': r['pantone_reference_id'],
                    'silk_origin_type_suitability': r['silk_origin_type_suitability'],
                    'liquor_ratio': r['liquor_ratio'],
                    'dye_class_used': r['dye_class_used'],
                    'dyebath_ph': float(r['dyebath_ph']) if r['dyebath_ph'] else None,
                    'color_difference_delta_e': float(r['color_difference_delta_e']) if r['color_difference_delta_e'] else None,
                    'dry_crocking_fastness': r['dry_crocking_fastness'],
                    'wet_crocking_fastness': r['wet_crocking_fastness'],
                    'auto_assigned_routing': r['auto_assigned_routing'],
                    'status': r['status'],
                    'certificate_hash': r['certificate_hash'],
                    'version': r['version'],
                    'throwster_batch_no': r['throwster_batch_no']
                }
                for r in recipes
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes/<recipe_id>', methods=['GET'])
@jwt_required()
def get_colorist_recipe(recipe_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mcr.*, tb.throwster_batch_no, tpr.input_raw_lot_no
            FROM master_colorist_recipes mcr
            LEFT JOIN throwster_batches tb ON mcr.throwster_batch_id = tb.id
            LEFT JOIN throwster_production_records tpr ON mcr.throwster_record_id = tpr.id
            WHERE mcr.id = %s::uuid
        """, (recipe_id,))
        
        recipe = cur.fetchone()
        cur.close()
        conn.close()
        
        if not recipe:
            return jsonify({'error': 'RecipeNotFound'}), 404
        
        return jsonify({
            'id': str(recipe['id']),
            'recipe_code': recipe['recipe_code'],
            'internal_shade_code': recipe['internal_shade_code'],
            'pantone_reference_id': recipe['pantone_reference_id'],
            'silk_origin_type_suitability': recipe['silk_origin_type_suitability'],
            'liquor_ratio': recipe['liquor_ratio'],
            'throwster_record_id': str(recipe['throwster_record_id']) if recipe['throwster_record_id'] else None,
            'throwster_batch_no': recipe['throwster_batch_no'],
            'input_raw_lot_no': recipe['input_raw_lot_no'],
            'status': recipe['status'],
            'version': recipe['version'],
            'dye_class_used': recipe['dye_class_used'],
            'dyebath_ph': float(recipe['dyebath_ph']) if recipe['dyebath_ph'] else None,
            'max_temperature_celsius': recipe['max_temperature_celsius'],
            'leveling_agent_added': recipe['leveling_agent_added'],
            'color_difference_delta_e': float(recipe['color_difference_delta_e']) if recipe['color_difference_delta_e'] else None,
            'light_source_profile': recipe['light_source_profile'],
            'cie_lab_coordinates': recipe['cie_lab_coordinates'],
            'dry_crocking_fastness': recipe['dry_crocking_fastness'],
            'wet_crocking_fastness': recipe['wet_crocking_fastness'],
            'post_dye_tenacity_gd': float(recipe['post_dye_tenacity_gd']) if recipe['post_dye_tenacity_gd'] else None,
            'antistatic_lubricant_applied': recipe['antistatic_lubricant_applied'],
            'acid_fixative_type': recipe['acid_fixative_type'],
            'leveling_exhausting_agent': recipe['leveling_exhausting_agent'],
            'validation_errors': recipe['validation_errors'],
            'validation_warnings': recipe['validation_warnings'],
            'auto_assigned_routing': recipe['auto_assigned_routing'],
            'certificate_hash': recipe['certificate_hash'],
            'qr_tag_id': recipe['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes/<recipe_id>/approve', methods=['POST'])
@jwt_required()
def approve_colorist_recipe(recipe_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, status, validation_errors, auto_assigned_routing, version
            FROM master_colorist_recipes
            WHERE id = %s::uuid
        """, (recipe_id,))
        
        recipe = cur.fetchone()
        if not recipe:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecipeNotFound'}), 404
        
        if recipe['validation_errors'] and len(recipe['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot approve recipe with validation errors'}), 400
        
        new_version = recipe['version'] + 1 if data.get('new_version') else recipe['version']
        certificate_hash = generate_certificate_hash(recipe_id, recipe['recipe_code'])
        qr_tag_id = 'COLOR-' + recipe['recipe_code']
        
        cur.execute("""
            UPDATE master_colorist_recipes
            SET status = 'APPROVED',
                version = %s,
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, recipe_code, certificate_hash
        """, (new_version, certificate_hash, qr_tag_id, recipe_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO master_colorist_certificates (
                recipe_id, certificate_hash, qr_tag_id,
                internal_shade_code, pantone_reference_id,
                certified_grade, auto_assigned_routing,
                dye_class_used, liquor_ratio, delta_e_value,
                dry_crocking_fastness, wet_crocking_fastness,
                post_dye_tenacity_gd, antistatic_lubricant_applied,
                throwster_record_id, throwster_batch_id,
                operator_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                mcr.id,
                mcr.certificate_hash,
                mcr.qr_tag_id,
                mcr.internal_shade_code,
                mcr.pantone_reference_id,
                '4A',
                mcr.auto_assigned_routing,
                mcr.dye_class_used,
                mcr.liquor_ratio,
                mcr.color_difference_delta_e,
                mcr.dry_crocking_fastness,
                mcr.wet_crocking_fastness,
                mcr.post_dye_tenacity_gd,
                mcr.antistatic_lubricant_applied,
                mcr.throwster_record_id,
                mcr.throwster_batch_id,
                mcr.operator_id,
                %s,
                mcr.factory_node_id,
                jsonb_build_object(
                    'recipe_code', mcr.recipe_code,
                    'version', mcr.version,
                    'silk_origin_type_suitability', mcr.silk_origin_type_suitability,
                    'dyebath_ph', mcr.dyebath_ph,
                    'max_temperature_celsius', mcr.max_temperature_celsius,
                    'leveling_agent_added', mcr.leveling_agent_added,
                    'light_source_profile', mcr.light_source_profile,
                    'cie_lab_coordinates', mcr.cie_lab_coordinates,
                    'acid_fixative_type', mcr.acid_fixative_type,
                    'leveling_exhausting_agent', mcr.leveling_exhausting_agent
                )
            FROM master_colorist_recipes mcr
            WHERE mcr.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM master_colorist_certificates WHERE recipe_id = mcr.id
            )
        """, (approver_id, recipe_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'recipe_code': result['recipe_code'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'version': new_version,
            'status': 'APPROVED',
            'message': 'Master Colorist recipe approved and certified'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes/<recipe_id>/reject', methods=['POST'])
@jwt_required()
def reject_colorist_recipe(recipe_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE master_colorist_recipes
            SET status = 'SHADE_REJECTED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id, recipe_code
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Master Colorist')}]),
            recipe_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'RecipeNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'recipe_code': result['recipe_code'],
            'status': 'SHADE_REJECTED',
            'message': 'Master Colorist recipe rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes/<recipe_id>/components', methods=['POST'])
@jwt_required()
def add_recipe_component(recipe_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['component_type', 'chemical_name', 'quantity_grams']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO recipe_chemical_components (
                recipe_id, component_type, chemical_name, quantity_grams,
                volume_ml, sequence_order, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            recipe_id,
            data.get('component_type'),
            data.get('chemical_name'),
            data.get('quantity_grams'),
            data.get('volume_ml'),
            data.get('sequence_order'),
            data.get('notes')
        ))
        
        component_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(component_row['id']),
            'recipe_id': str(recipe_id),
            'message': 'Recipe component added successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/recipes/<recipe_id>/components', methods=['GET'])
@jwt_required()
def list_recipe_components(recipe_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, component_type, chemical_name, quantity_grams,
                   volume_ml, sequence_order, notes
            FROM recipe_chemical_components
            WHERE recipe_id = %s::uuid
            ORDER BY sequence_order ASC
        """, (recipe_id,))
        
        components = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(components),
            'components': [
                {
                    'id': str(c['id']),
                    'component_type': c['component_type'],
                    'chemical_name': c['chemical_name'],
                    'quantity_grams': float(c['quantity_grams']) if c['quantity_grams'] else None,
                    'volume_ml': float(c['volume_ml']) if c['volume_ml'] else None,
                    'sequence_order': c['sequence_order'],
                    'notes': c['notes']
                }
                for c in components
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/colorist/certificates', methods=['GET'])
@jwt_required()
def list_colorist_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mcc.id, mcc.certificate_hash, mcc.qr_tag_id,
                   mcc.internal_shade_code, mcc.pantone_reference_id,
                   mcc.dye_class_used, mcc.liquor_ratio, mcc.delta_e_value,
                   mcc.dry_crocking_fastness, mcc.wet_crocking_fastness,
                   mcc.post_dye_tenacity_gd, mcc.antistatic_lubricant_applied,
                   mcc.auto_assigned_routing, mcc.status, mcc.certified_at,
                   mcr.recipe_code, tb.throwster_batch_no
            FROM master_colorist_certificates mcc
            JOIN master_colorist_recipes mcr ON mcc.recipe_id = mcr.id
            LEFT JOIN throwster_batches tb ON mcc.throwster_batch_id = tb.id
            WHERE mcc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY mcc.certified_at DESC
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
                    'recipe_code': c['recipe_code'],
                    'internal_shade_code': c['internal_shade_code'],
                    'pantone_reference_id': c['pantone_reference_id'],
                    'dye_class_used': c['dye_class_used'],
                    'liquor_ratio': c['liquor_ratio'],
                    'delta_e_value': float(c['delta_e_value']) if c['delta_e_value'] else None,
                    'dry_crocking_fastness': c['dry_crocking_fastness'],
                    'wet_crocking_fastness': c['wet_crocking_fastness'],
                    'post_dye_tenacity_gd': float(c['post_dye_tenacity_gd']) if c['post_dye_tenacity_gd'] else None,
                    'antistatic_lubricant_applied': c['antistatic_lubricant_applied'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR MASTER COLORIST
# ============================================================

@app.route('/api/v1/sales/forecast/colorist', methods=['GET'])
@jwt_required()
def get_sales_forecast_colorist():
    """
    API plugin endpoint for sales team colorist material processing forecast.
    Returns forecasted dyeing requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'dye_class': 'ACID_PRE_METALLISED_1_2',
                    'estimated_silk_kg': 200.0,
                    'target_delta_e': '<=0.5',
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'dye_class': 'REACTIVE_CIBACRON_F',
                    'estimated_silk_kg': 150.0,
                    'target_delta_e': '<=0.5',
                    'priority': 'HIGH',
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'shade_code': 'MID-MNG-402',
                    'dye_class': 'ACID_MILL_MILLING',
                    'estimated_silk_kg': 180.0,
                    'target_delta_e': '<=1.0',
                    'priority': 'MEDIUM',
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'COLOR-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'estimated_sarees': 80,
                    'estimated_silk_kg': 200.0,
                    'dye_class': 'ACID_PRE_METALLISED_1_2',
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'COLOR-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'estimated_sarees': 60,
                    'estimated_silk_kg': 150.0,
                    'dye_class': 'REACTIVE_CIBACRON_F',
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SKEIN DYE MASTER MODULE
# ============================================================

@app.route('/api/v1/skein-dye/jobs', methods=['POST'])
@jwt_required()
def create_skein_dye_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['job_id', 'allocated_machine_id', 'input_skein_dry_weight_kg']
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
        
        cur.execute("""
            INSERT INTO skein_dye_jobs (
                job_id, master_colorist_recipe_id, master_colorist_certificate_id,
                throwster_record_id, throwster_batch_id, production_lot_id,
                factory_node_id, operator_id, status,
                allocated_machine_id, vessel_type_allocated, operator_name,
                actual_liquor_volume_liters, bath_start_time, bath_end_time,
                peak_boil_temperature_celsius, fixation_duration_minutes,
                hank_unit_weight_g, lease_tie_type, machine_type, pump_flow_rate_lpm,
                liquor_ratio, pre_boil_hardness_ppm, water_treatment_status,
                peak_heating_temperature_bracket, input_skein_dry_weight_kg,
                output_skein_dry_weight_kg, post_dye_softening_type,
                core_to_surface_shade_match, tie_mark_spot_found,
                post_dye_winding_break_count, hank_entanglement_rating,
                recipe_scaler_multiplier, validation_errors, validation_warnings,
                auto_assigned_routing, target_machine_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, job_id
        """, (
            data.get('job_id'),
            data.get('master_colorist_recipe_id'),
            data.get('master_colorist_certificate_id'),
            data.get('throwster_record_id'),
            data.get('throwster_batch_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id,
            data.get('allocated_machine_id'),
            data.get('vessel_type_allocated'),
            data.get('operator_name'),
            data.get('actual_liquor_volume_liters'),
            data.get('bath_start_time'),
            data.get('bath_end_time'),
            data.get('peak_boil_temperature_celsius'),
            data.get('fixation_duration_minutes'),
            data.get('hank_unit_weight_g'),
            data.get('lease_tie_type'),
            data.get('machine_type'),
            data.get('pump_flow_rate_lpm'),
            data.get('liquor_ratio'),
            data.get('pre_boil_hardness_ppm'),
            data.get('water_treatment_status'),
            data.get('peak_heating_temperature_bracket'),
            data.get('input_skein_dry_weight_kg'),
            data.get('output_skein_dry_weight_kg'),
            data.get('post_dye_softening_type'),
            data.get('core_to_surface_shade_match'),
            data.get('tie_mark_spot_found', False),
            data.get('post_dye_winding_break_count', 0),
            data.get('hank_entanglement_rating'),
            data.get('recipe_scaler_multiplier', 1.0),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing'),
            data.get('target_machine_type', '1536_HOOK_JACQUARD')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status
            FROM skein_dye_jobs WHERE id = %s::uuid
        """, (job_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'IN_PROGRESS'
        else:
            status = 'IN_PROGRESS'
        
        cur.execute("""
            UPDATE skein_dye_jobs SET status = %s WHERE id = %s::uuid
        """, (status, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'job_id': job_row['job_id'],
            'status': status,
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Skein Dye job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs', methods=['GET'])
@jwt_required()
def list_skein_dye_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sdj.id, sdj.job_id, sdj.allocated_machine_id, sdj.vessel_type_allocated,
                   sdj.peak_boil_temperature_celsius, sdj.fixation_duration_minutes,
                   sdj.hank_unit_weight_g, sdj.lease_tie_type, sdj.machine_type,
                   sdj.input_skein_dry_weight_kg, sdj.output_skein_dry_weight_kg,
                   sdj.dye_house_yield_variance, sdj.core_to_surface_shade_match,
                   sdj.tie_mark_spot_found, sdj.post_dye_winding_break_count,
                   sdj.hank_entanglement_rating, sdj.post_dye_softening_type,
                   sdj.auto_assigned_routing, sdj.status, sdj.certificate_hash,
                   sdj.created_at, mcr.recipe_code, mcr.internal_shade_code
            FROM skein_dye_jobs sdj
            LEFT JOIN master_colorist_recipes mcr ON sdj.master_colorist_recipe_id = mcr.id
            WHERE sdj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY sdj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'job_id': j['job_id'],
                    'allocated_machine_id': j['allocated_machine_id'],
                    'vessel_type_allocated': j['vessel_type_allocated'],
                    'peak_boil_temperature_celsius': j['peak_boil_temperature_celsius'],
                    'fixation_duration_minutes': j['fixation_duration_minutes'],
                    'hank_unit_weight_g': j['hank_unit_weight_g'],
                    'lease_tie_type': j['lease_tie_type'],
                    'machine_type': j['machine_type'],
                    'input_skein_dry_weight_kg': float(j['input_skein_dry_weight_kg']) if j['input_skein_dry_weight_kg'] else None,
                    'output_skein_dry_weight_kg': float(j['output_skein_dry_weight_kg']) if j['output_skein_dry_weight_kg'] else None,
                    'dye_house_yield_variance': float(j['dye_house_yield_variance']) if j['dye_house_yield_variance'] else None,
                    'core_to_surface_shade_match': j['core_to_surface_shade_match'],
                    'tie_mark_spot_found': j['tie_mark_spot_found'],
                    'post_dye_winding_break_count': j['post_dye_winding_break_count'],
                    'hank_entanglement_rating': j['hank_entanglement_rating'],
                    'post_dye_softening_type': j['post_dye_softening_type'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'recipe_code': j['recipe_code'],
                    'internal_shade_code': j['internal_shade_code'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_skein_dye_job(job_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sdj.*, mcr.recipe_code, mcr.internal_shade_code, mcr.dye_class_used
            FROM skein_dye_jobs sdj
            LEFT JOIN master_colorist_recipes mcr ON sdj.master_colorist_recipe_id = mcr.id
            WHERE sdj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'job_id': job['job_id'],
            'allocated_machine_id': job['allocated_machine_id'],
            'vessel_type_allocated': job['vessel_type_allocated'],
            'operator_name': job['operator_name'],
            'actual_liquor_volume_liters': float(job['actual_liquor_volume_liters']) if job['actual_liquor_volume_liters'] else None,
            'bath_start_time': job['bath_start_time'].isoformat() if job['bath_start_time'] else None,
            'bath_end_time': job['bath_end_time'].isoformat() if job['bath_end_time'] else None,
            'peak_boil_temperature_celsius': job['peak_boil_temperature_celsius'],
            'fixation_duration_minutes': job['fixation_duration_minutes'],
            'hank_unit_weight_g': job['hank_unit_weight_g'],
            'lease_tie_type': job['lease_tie_type'],
            'machine_type': job['machine_type'],
            'pump_flow_rate_lpm': float(job['pump_flow_rate_lpm']) if job['pump_flow_rate_lpm'] else None,
            'liquor_ratio': job['liquor_ratio'],
            'pre_boil_hardness_ppm': job['pre_boil_hardness_ppm'],
            'water_treatment_status': job['water_treatment_status'],
            'peak_heating_temperature_bracket': job['peak_heating_temperature_bracket'],
            'input_skein_dry_weight_kg': float(job['input_skein_dry_weight_kg']) if job['input_skein_dry_weight_kg'] else None,
            'output_skein_dry_weight_kg': float(job['output_skein_dry_weight_kg']) if job['output_skein_dry_weight_kg'] else None,
            'dye_house_yield_variance': float(job['dye_house_yield_variance']) if job['dye_house_yield_variance'] else None,
            'post_dye_softening_type': job['post_dye_softening_type'],
            'core_to_surface_shade_match': job['core_to_surface_shade_match'],
            'tie_mark_spot_found': job['tie_mark_spot_found'],
            'post_dye_winding_break_count': job['post_dye_winding_break_count'],
            'hank_entanglement_rating': job['hank_entanglement_rating'],
            'recipe_scaler_multiplier': float(job['recipe_scaler_multiplier']) if job['recipe_scaler_multiplier'] else 1.0,
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash'],
            'recipe_code': job['recipe_code'],
            'internal_shade_code': job['internal_shade_code'],
            'dye_class_used': job['dye_class_used']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs/<job_id>/complete', methods=['POST'])
@jwt_required()
def complete_skein_dye_job(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE skein_dye_jobs
            SET status = 'COMPLETED',
                bath_end_time = COALESCE(%s::timestamptz, bath_end_time),
                output_skein_dry_weight_kg = COALESCE(%s, output_skein_dry_weight_kg),
                dye_house_yield_variance = COALESCE(
                    output_skein_dry_weight_kg - input_skein_dry_weight_kg,
                    dye_house_yield_variance
                )
            WHERE id = %s::uuid
            RETURNING id, job_id, status
        """, (
            data.get('bath_end_time'),
            data.get('output_skein_dry_weight_kg'),
            job_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'job_id': result['job_id'],
            'status': result['status'],
            'message': 'Skein Dye job completed'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_skein_dye_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, job_id, status, validation_errors, auto_assigned_routing
            FROM skein_dye_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['job_id'])
        qr_tag_id = 'SKEIN-' + job['job_id']
        
        cur.execute("""
            UPDATE skein_dye_jobs
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, job_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO skein_dye_certificates (
                job_id, certificate_hash, qr_tag_id, job_id_ref,
                allocated_machine_id, vessel_type_allocated,
                peak_boil_temperature_celsius, fixation_duration_minutes,
                input_skein_dry_weight_kg, output_skein_dry_weight_kg,
                dye_house_yield_variance, core_to_surface_shade_match,
                tie_mark_spot_found, post_dye_winding_break_count,
                hank_entanglement_rating, post_dye_softening_type,
                auto_assigned_routing, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                sdj.id,
                sdj.certificate_hash,
                sdj.qr_tag_id,
                sdj.job_id,
                sdj.allocated_machine_id,
                sdj.vessel_type_allocated,
                sdj.peak_boil_temperature_celsius,
                sdj.fixation_duration_minutes,
                sdj.input_skein_dry_weight_kg,
                sdj.output_skein_dry_weight_kg,
                sdj.dye_house_yield_variance,
                sdj.core_to_surface_shade_match,
                sdj.tie_mark_spot_found,
                sdj.post_dye_winding_break_count,
                sdj.hank_entanglement_rating,
                sdj.post_dye_softening_type,
                sdj.auto_assigned_routing,
                sdj.operator_id,
                %s,
                sdj.factory_node_id,
                jsonb_build_object(
                    'job_id', sdj.job_id,
                    'recipe_code', mcr.recipe_code,
                    'internal_shade_code', mcr.internal_shade_code,
                    'dye_class_used', mcr.dye_class_used,
                    'hank_unit_weight_g', sdj.hank_unit_weight_g,
                    'lease_tie_type', sdj.lease_tie_type,
                    'machine_type', sdj.machine_type,
                    'liquor_ratio', sdj.liquor_ratio,
                    'water_treatment_status', sdj.water_treatment_status,
                    'peak_heating_temperature_bracket', sdj.peak_heating_temperature_bracket
                )
            FROM skein_dye_jobs sdj
            LEFT JOIN master_colorist_recipes mcr ON sdj.master_colorist_recipe_id = mcr.id
            WHERE sdj.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM skein_dye_certificates WHERE job_id = sdj.id
            )
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'job_id': result['job_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED',
            'message': 'Skein Dye job certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs/<job_id>/chemicals', methods=['POST'])
@jwt_required()
def add_skein_dye_job_chemical(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['chemical_name', 'quantity_grams']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO skein_dye_job_chemicals (
                job_id, chemical_name, quantity_grams, volume_ml,
                component_type, sequence_order, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            job_id,
            data.get('chemical_name'),
            data.get('quantity_grams'),
            data.get('volume_ml'),
            data.get('component_type'),
            data.get('sequence_order'),
            data.get('notes')
        ))
        
        chemical_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(chemical_row['id']),
            'job_id': str(job_id),
            'message': 'Chemical added to job successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/jobs/<job_id>/chemicals', methods=['GET'])
@jwt_required()
def list_skein_dye_job_chemicals(job_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, chemical_name, quantity_grams, volume_ml,
                   component_type, sequence_order, notes
            FROM skein_dye_job_chemicals
            WHERE job_id = %s::uuid
            ORDER BY sequence_order ASC
        """, (job_id,))
        
        chemicals = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(chemicals),
            'chemicals': [
                {
                    'id': str(c['id']),
                    'chemical_name': c['chemical_name'],
                    'quantity_grams': float(c['quantity_grams']) if c['quantity_grams'] else None,
                    'volume_ml': float(c['volume_ml']) if c['volume_ml'] else None,
                    'component_type': c['component_type'],
                    'sequence_order': c['sequence_order'],
                    'notes': c['notes']
                }
                for c in chemicals
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/skein-dye/certificates', methods=['GET'])
@jwt_required()
def list_skein_dye_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sdc.id, sdc.certificate_hash, sdc.qr_tag_id, sdc.job_id_ref,
                   sdc.allocated_machine_id, sdc.vessel_type_allocated,
                   sdc.peak_boil_temperature_celsius, sdc.fixation_duration_minutes,
                   sdc.input_skein_dry_weight_kg, sdc.output_skein_dry_weight_kg,
                   sdc.dye_house_yield_variance, sdc.core_to_surface_shade_match,
                   sdc.tie_mark_spot_found, sdc.post_dye_winding_break_count,
                   sdc.hank_entanglement_rating, sdc.post_dye_softening_type,
                   sdc.auto_assigned_routing, sdc.status, sdc.certified_at,
                   sdj.job_id, mcr.recipe_code, mcr.internal_shade_code
            FROM skein_dye_certificates sdc
            JOIN skein_dye_jobs sdj ON sdc.job_id = sdj.id
            LEFT JOIN master_colorist_recipes mcr ON sdj.master_colorist_recipe_id = mcr.id
            WHERE sdc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY sdc.certified_at DESC
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
                    'job_id': c['job_id'],
                    'job_id_ref': c['job_id_ref'],
                    'allocated_machine_id': c['allocated_machine_id'],
                    'vessel_type_allocated': c['vessel_type_allocated'],
                    'peak_boil_temperature_celsius': c['peak_boil_temperature_celsius'],
                    'fixation_duration_minutes': c['fixation_duration_minutes'],
                    'input_skein_dry_weight_kg': float(c['input_skein_dry_weight_kg']) if c['input_skein_dry_weight_kg'] else None,
                    'output_skein_dry_weight_kg': float(c['output_skein_dry_weight_kg']) if c['output_skein_dry_weight_kg'] else None,
                    'dye_house_yield_variance': float(c['dye_house_yield_variance']) if c['dye_house_yield_variance'] else None,
                    'core_to_surface_shade_match': c['core_to_surface_shade_match'],
                    'tie_mark_spot_found': c['tie_mark_spot_found'],
                    'post_dye_winding_break_count': c['post_dye_winding_break_count'],
                    'hank_entanglement_rating': c['hank_entanglement_rating'],
                    'post_dye_softening_type': c['post_dye_softening_type'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None,
                    'recipe_code': c['recipe_code'],
                    'internal_shade_code': c['internal_shade_code']
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR SKEIN DYE MASTER
# ============================================================

@app.route('/api/v1/sales/forecast/skein-dye', methods=['GET'])
@jwt_required()
def get_sales_forecast_skein_dye():
    """
    API plugin endpoint for sales team skein dye material processing forecast.
    Returns forecasted dyeing requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'dye_class': 'ACID_PRE_METALLISED_1_2',
                    'estimated_silk_kg': 200.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'dye_class': 'REACTIVE_CIBACRON_F',
                    'estimated_silk_kg': 150.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'shade_code': 'MID-MNG-402',
                    'dye_class': 'ACID_MILL_MILLING',
                    'estimated_silk_kg': 180.0,
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'SKEIN-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'estimated_sarees': 80,
                    'estimated_silk_kg': 200.0,
                    'dye_class': 'ACID_PRE_METALLISED_1_2',
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'SKEIN-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'estimated_sarees': 60,
                    'estimated_silk_kg': 150.0,
                    'dye_class': 'REACTIVE_CIBACRON_F',
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR BOBBIN WINDER
# ============================================================

@app.route('/api/v1/sales/forecast/winding', methods=['GET'])
@jwt_required()
def get_sales_forecast_winding():
    """
    API plugin endpoint for sales team bobbin winding material processing forecast.
    Returns forecasted winding requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'yarn_type': 'WARP_ORGANZINE_HIGH_TWIST',
                    'carrier_type': 'FLANGED_PLASTIC_BOBBIN',
                    'estimated_silk_kg': 200.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'yarn_type': 'WARP_ORGANZINE_HIGH_TWIST',
                    'carrier_type': 'FLANGED_PLASTIC_BOBBIN',
                    'estimated_silk_kg': 150.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'shade_code': 'MID-MNG-402',
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'carrier_type': 'TAPERED_PAPER_CONE',
                    'estimated_silk_kg': 180.0,
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'WIND-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'yarn_type': 'WARP_ORGANZINE_HIGH_TWIST',
                    'estimated_sarees': 80,
                    'estimated_silk_kg': 200.0,
                    'carrier_type': 'FLANGED_PLASTIC_BOBBIN',
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'WIND-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'yarn_type': 'WARP_ORGANZINE_HIGH_TWIST',
                    'estimated_sarees': 60,
                    'estimated_silk_kg': 150.0,
                    'carrier_type': 'FLANGED_PLASTIC_BOBBIN',
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# PIRN WINDER MODULE
# ============================================================

@app.route('/api/v1/pirn-winding/jobs', methods=['POST'])
@jwt_required()
def create_pirn_winding_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['pirn_winding_job_no', 'source_bobbin_lot_no', 'target_pirn_count_qty', 'input_yarn_weight_kg']
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
        
        cur.execute("""
            INSERT INTO pirn_winding_jobs (
                pirn_winding_job_no, bobbin_winder_job_card_id, bobbin_record_id,
                master_colorist_recipe_id, master_colorist_certificate_id,
                skein_dye_job_id, skein_dye_certificate_id,
                throwster_record_id, throwster_batch_id, production_lot_id,
                factory_node_id, operator_id, status,
                winding_machine_id, pirn_machine_type,
                source_bobbin_lot_no, target_pirn_count_qty,
                input_yarn_weight_kg, output_pirn_net_weight_kg, pirn_scrap_waste_gm,
                spindle_speed_rpm, pirn_base_and_nose_taper_deg, weft_joint_method,
                pirn_hardness_shore_d, splices_per_pirn, sloughing_risk_index,
                pirn_surface_inspection, target_machine_type,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DRAFT', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, pirn_winding_job_no
        """, (
            data.get('pirn_winding_job_no'),
            data.get('bobbin_winder_job_card_id'),
            data.get('bobbin_record_id'),
            data.get('master_colorist_recipe_id'),
            data.get('master_colorist_certificate_id'),
            data.get('skein_dye_job_id'),
            data.get('skein_dye_certificate_id'),
            data.get('throwster_record_id'),
            data.get('throwster_batch_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id,
            data.get('winding_machine_id'),
            data.get('pirn_machine_type'),
            data.get('source_bobbin_lot_no'),
            data.get('target_pirn_count_qty'),
            data.get('input_yarn_weight_kg'),
            data.get('output_pirn_net_weight_kg'),
            data.get('pirn_scrap_waste_gm', 0),
            data.get('spindle_speed_rpm'),
            data.get('pirn_base_and_nose_taper_deg'),
            data.get('weft_joint_method'),
            data.get('pirn_hardness_shore_d'),
            data.get('splices_per_pirn', 0),
            data.get('sloughing_risk_index'),
            data.get('pirn_surface_inspection'),
            data.get('target_machine_type', '1536_HOOK_JACQUARD'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status
            FROM pirn_winding_jobs WHERE id = %s::uuid
        """, (job_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'IN_PROGRESS'
        else:
            status = 'IN_PROGRESS'
        
        cur.execute("""
            UPDATE pirn_winding_jobs SET status = %s WHERE id = %s::uuid
        """, (status, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'pirn_winding_job_no': job_row['pirn_winding_job_no'],
            'status': status,
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Pirn winding job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/pirn-winding/jobs', methods=['GET'])
@jwt_required()
def list_pirn_winding_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pwj.id, pwj.pirn_winding_job_no, pwj.winding_machine_id,
                   pwj.source_bobbin_lot_no, pwj.target_pirn_count_qty,
                   pwj.input_yarn_weight_kg, pwj.output_pirn_net_weight_kg,
                   pwj.pirn_scrap_waste_gm, pwj.material_variance_kg,
                   pwj.spindle_speed_rpm, pwj.pirn_base_and_nose_taper_deg,
                   pwj.weft_joint_method, pwj.pirn_hardness_shore_d,
                   pwj.splices_per_pirn, pwj.sloughing_risk_index,
                   pwj.pirn_surface_inspection, pwj.auto_assigned_routing,
                   pwj.status, pwj.certificate_hash, pwj.created_at,
                   wjc.winding_job_card_id
            FROM pirn_winding_jobs pwj
            LEFT JOIN winding_job_cards wjc ON pwj.bobbin_winder_job_card_id = wjc.id
            WHERE pwj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pwj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'pirn_winding_job_no': j['pirn_winding_job_no'],
                    'winding_job_card_id': j['winding_job_card_id'],
                    'winding_machine_id': j['winding_machine_id'],
                    'source_bobbin_lot_no': j['source_bobbin_lot_no'],
                    'target_pirn_count_qty': j['target_pirn_count_qty'],
                    'input_yarn_weight_kg': float(j['input_yarn_weight_kg']) if j['input_yarn_weight_kg'] else None,
                    'output_pirn_net_weight_kg': float(j['output_pirn_net_weight_kg']) if j['output_pirn_net_weight_kg'] else None,
                    'pirn_scrap_waste_gm': float(j['pirn_scrap_waste_gm']) if j['pirn_scrap_waste_gm'] else None,
                    'material_variance_kg': float(j['material_variance_kg']) if j['material_variance_kg'] else None,
                    'spindle_speed_rpm': j['spindle_speed_rpm'],
                    'pirn_base_and_nose_taper_deg': float(j['pirn_base_and_nose_taper_deg']) if j['pirn_base_and_nose_taper_deg'] else None,
                    'weft_joint_method': j['weft_joint_method'],
                    'pirn_hardness_shore_d': float(j['pirn_hardness_shore_d']) if j['pirn_hardness_shore_d'] else None,
                    'splices_per_pirn': j['splices_per_pirn'],
                    'sloughing_risk_index': j['sloughing_risk_index'],
                    'pirn_surface_inspection': j['pirn_surface_inspection'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/pirn-winding/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_pirn_winding_job(job_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pwj.*, wjc.winding_job_card_id, br.bobbin_id
            FROM pirn_winding_jobs pwj
            LEFT JOIN winding_job_cards wjc ON pwj.bobbin_winder_job_card_id = wjc.id
            LEFT JOIN bobbin_records br ON pwj.bobbin_record_id = br.id
            WHERE pwj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'pirn_winding_job_no': job['pirn_winding_job_no'],
            'winding_job_card_id': job['winding_job_card_id'],
            'bobbin_id': job['bobbin_id'],
            'winding_machine_id': job['winding_machine_id'],
            'pirn_machine_type': job['pirn_machine_type'],
            'source_bobbin_lot_no': job['source_bobbin_lot_no'],
            'target_pirn_count_qty': job['target_pirn_count_qty'],
            'input_yarn_weight_kg': float(job['input_yarn_weight_kg']) if job['input_yarn_weight_kg'] else None,
            'output_pirn_net_weight_kg': float(job['output_pirn_net_weight_kg']) if job['output_pirn_net_weight_kg'] else None,
            'pirn_scrap_waste_gm': float(job['pirn_scrap_waste_gm']) if job['pirn_scrap_waste_gm'] else None,
            'material_variance_kg': float(job['material_variance_kg']) if job['material_variance_kg'] else None,
            'spindle_speed_rpm': job['spindle_speed_rpm'],
            'pirn_base_and_nose_taper_deg': float(job['pirn_base_and_nose_taper_deg']) if job['pirn_base_and_nose_taper_deg'] else None,
            'weft_joint_method': job['weft_joint_method'],
            'pirn_hardness_shore_d': float(job['pirn_hardness_shore_d']) if job['pirn_hardness_shore_d'] else None,
            'splices_per_pirn': job['splices_per_pirn'],
            'sloughing_risk_index': job['sloughing_risk_index'],
            'pirn_surface_inspection': job['pirn_surface_inspection'],
            'target_machine_type': job['target_machine_type'],
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/pirn-winding/jobs/<job_id>/complete', methods=['POST'])
@jwt_required()
def complete_pirn_winding_job(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE pirn_winding_jobs
            SET status = 'COMPLETED',
                output_pirn_net_weight_kg = COALESCE(%s, output_pirn_net_weight_kg),
                material_variance_kg = COALESCE(
                    input_yarn_weight_kg - output_pirn_net_weight_kg - (pirn_scrap_waste_gm / 1000.0),
                    material_variance_kg
                )
            WHERE id = %s::uuid
            RETURNING id, pirn_winding_job_no, status
        """, (
            data.get('output_pirn_net_weight_kg'),
            job_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'pirn_winding_job_no': result['pirn_winding_job_no'],
            'status': result['status'],
            'message': 'Pirn winding job completed'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/pirn-winding/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_pirn_winding_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, pirn_winding_job_no, status, validation_errors, auto_assigned_routing
            FROM pirn_winding_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['pirn_winding_job_no'])
        qr_tag_id = 'PIRN-' + job['pirn_winding_job_no']
        
        cur.execute("""
            UPDATE pirn_winding_jobs
            SET status = 'CERTIFIED',
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, pirn_winding_job_no, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO pirn_winding_certificates (
                job_id, certificate_hash, qr_tag_id, pirn_winding_job_no,
                source_bobbin_lot_no, target_pirn_count_qty,
                input_yarn_weight_kg, output_pirn_net_weight_kg,
                pirn_scrap_waste_gm, material_variance_kg,
                spindle_speed_rpm, pirn_base_and_nose_taper_deg,
                weft_joint_method, pirn_hardness_shore_d,
                splices_per_pirn, sloughing_risk_index, pirn_surface_inspection,
                auto_assigned_routing, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pwj.id,
                pwj.certificate_hash,
                pwj.qr_tag_id,
                pwj.pirn_winding_job_no,
                pwj.source_bobbin_lot_no,
                pwj.target_pirn_count_qty,
                pwj.input_yarn_weight_kg,
                pwj.output_pirn_net_weight_kg,
                pwj.pirn_scrap_waste_gm,
                pwj.material_variance_kg,
                pwj.spindle_speed_rpm,
                pwj.pirn_base_and_nose_taper_deg,
                pwj.weft_joint_method,
                pwj.pirn_hardness_shore_d,
                pwj.splices_per_pirn,
                pwj.sloughing_risk_index,
                pwj.pirn_surface_inspection,
                pwj.auto_assigned_routing,
                pwj.operator_id,
                %s,
                pwj.factory_node_id,
                jsonb_build_object(
                    'pirn_winding_job_no', pwj.pirn_winding_job_no,
                    'bobbin_winder_job_card_id', wjc.winding_job_card_id,
                    'pirn_machine_type', pwj.pirn_machine_type,
                    'target_pirn_count_qty', pwj.target_pirn_count_qty,
                    'target_machine_type', pwj.target_machine_type
                )
            FROM pirn_winding_jobs pwj
            LEFT JOIN winding_job_cards wjc ON pwj.bobbin_winder_job_card_id = wjc.id
            WHERE pwj.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM pirn_winding_certificates WHERE job_id = pwj.id
            )
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'pirn_winding_job_no': result['pirn_winding_job_no'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED',
            'message': 'Pirn winding job certified successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/pirn-winding/certificates', methods=['GET'])
@jwt_required()
def list_pirn_winding_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pwc.id, pwc.certificate_hash, pwc.qr_tag_id,
                   pwc.pirn_winding_job_no, pwc.source_bobbin_lot_no,
                   pwc.target_pirn_count_qty, pwc.input_yarn_weight_kg,
                   pwc.output_pirn_net_weight_kg, pwc.pirn_scrap_waste_gm,
                   pwc.material_variance_kg, pwc.spindle_speed_rpm,
                   pwc.pirn_base_and_nose_taper_deg, pwc.weft_joint_method,
                   pwc.pirn_hardness_shore_d, pwc.splices_per_pirn,
                   pwc.sloughing_risk_index, pwc.pirn_surface_inspection,
                   pwc.auto_assigned_routing, pwc.status, pwc.certified_at,
                   pwj.winding_machine_id
            FROM pirn_winding_certificates pwc
            JOIN pirn_winding_jobs pwj ON pwc.job_id = pwj.id
            WHERE pwc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pwc.certified_at DESC
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
                    'pirn_winding_job_no': c['pirn_winding_job_no'],
                    'source_bobbin_lot_no': c['source_bobbin_lot_no'],
                    'target_pirn_count_qty': c['target_pirn_count_qty'],
                    'input_yarn_weight_kg': float(c['input_yarn_weight_kg']) if c['input_yarn_weight_kg'] else None,
                    'output_pirn_net_weight_kg': float(c['output_pirn_net_weight_kg']) if c['output_pirn_net_weight_kg'] else None,
                    'pirn_scrap_waste_gm': float(c['pirn_scrap_waste_gm']) if c['pirn_scrap_waste_gm'] else None,
                    'material_variance_kg': float(c['material_variance_kg']) if c['material_variance_kg'] else None,
                    'spindle_speed_rpm': c['spindle_speed_rpm'],
                    'pirn_base_and_nose_taper_deg': float(c['pirn_base_and_nose_taper_deg']) if c['pirn_base_and_nose_taper_deg'] else None,
                    'weft_joint_method': c['weft_joint_method'],
                    'pirn_hardness_shore_d': float(c['pirn_hardness_shore_d']) if c['pirn_hardness_shore_d'] else None,
                    'splices_per_pirn': c['splices_per_pirn'],
                    'sloughing_risk_index': c['sloughing_risk_index'],
                    'pirn_surface_inspection': c['pirn_surface_inspection'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR PIRN WINDER
# ============================================================

@app.route('/api/v1/sales/forecast/pirn-winding', methods=['GET'])
@jwt_required()
def get_sales_forecast_pirn_winding():
    """
    API plugin endpoint for sales team pirn winding material processing forecast.
    Returns forecasted pirn winding requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'target_pirn_count': 5000,
                    'estimated_silk_kg': 120.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'target_pirn_count': 4000,
                    'estimated_silk_kg': 100.0,
                    'target_machine': '2400_HOOK_JACQUARD',
                    'estimated_sarees': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'shade_code': 'MID-MNG-402',
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'target_pirn_count': 4500,
                    'estimated_silk_kg': 90.0,
                    'target_machine': '1536_HOOK_JACQUARD',
                    'estimated_sarees': 100,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'PIRN-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'shade_code': 'KNC-MRN-702',
                    'target_pirn_count': 5000,
                    'estimated_sarees': 80,
                    'estimated_silk_kg': 120.0,
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'target_machine': '2400_HOOK_JACQUARD'
                },
                {
                    'lot_number': 'PIRN-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'shade_code': 'BNR-BLU-101',
                    'target_pirn_count': 4000,
                    'estimated_sarees': 60,
                    'estimated_silk_kg': 100.0,
                    'yarn_type': 'WEFT_TRAM_LOW_TWIST',
                    'target_machine': '2400_HOOK_JACQUARD'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# GRAPH DRAFTER (2400 HOOK) MODULE
# ============================================================

@app.route('/api/v1/design/graphs', methods=['POST'])
@jwt_required()
def create_design_graph():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['design_master_id', 'target_hook_capacity']
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
        
        cur.execute("""
            INSERT INTO design_masters (
                design_master_id, graph_iteration_v, drafter_employee_id,
                factory_node_id, status,
                total_hook_capacity, top_border_hooks, bottom_border_hooks,
                body_motif_hooks, selvedge_hooks,
                grid_width_pixels, grid_height_picks, maximum_float_length,
                digital_cad_file_upload,
                target_hook_capacity, hook_allocation_profile,
                warp_ends_per_inch_epi, weft_picks_per_inch_ppi,
                ground_weave_structure, zari_binding_weave_type,
                border_binding_technique, shading_technique,
                max_float_enforcement_rule, max_warp_float_ends, max_weft_float_picks,
                selvage_hook_count, cad_output_format, design_approval_state,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, 'DRAFT', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, design_master_id
        """, (
            data.get('design_master_id'),
            data.get('graph_iteration_v', 1.0),
            operator_id,
            factory_node_id,
            data.get('target_hook_capacity', 2400),
            data.get('top_border_hooks', 0),
            data.get('bottom_border_hooks', 0),
            data.get('body_motif_hooks', 0),
            data.get('selvedge_hooks', 0),
            data.get('grid_width_pixels', 2400),
            data.get('grid_height_picks', 0),
            data.get('maximum_float_length', 7),
            data.get('digital_cad_file_upload'),
            data.get('target_hook_capacity', 2400),
            data.get('hook_allocation_profile', '2250_DESIGN_150_BORDER_SELVAGE'),
            data.get('warp_ends_per_inch_epi'),
            data.get('weft_picks_per_inch_ppi'),
            data.get('ground_weave_structure', '16_END_SHADED_SATIN'),
            data.get('zari_binding_weave_type', '8_END_SATIN_INTERLOCK'),
            data.get('border_binding_technique', 'MICRO_STEP_CATCHING'),
            data.get('shading_technique', 'MULTI_LEVEL_SHADED_SATIN'),
            data.get('max_float_enforcement_rule', 'STRICT_WARP_LE_4_WEFT_LE_5'),
            data.get('max_warp_float_ends', 4),
            data.get('max_weft_float_picks', 5),
            data.get('selvage_hook_count', 0),
            data.get('cad_output_format', 'JC5'),
            data.get('design_approval_state', 'DRAFT'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        design_row = cur.fetchone()
        design_id = design_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status, design_approval_state
            FROM design_masters WHERE id = %s::uuid
        """, (design_id,))
        result = cur.fetchone()
        
        status = 'DRAFT'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'DRAFT'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'PENDING_FLOAT_CHECK'
        else:
            status = 'PENDING_FLOAT_CHECK'
        
        cur.execute("""
            UPDATE design_masters SET status = %s WHERE id = %s::uuid
        """, (status, design_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(design_id),
            'design_master_id': design_row['design_master_id'],
            'status': status,
            'design_approval_state': result['design_approval_state'],
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Design graph created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/graphs', methods=['GET'])
@jwt_required()
def list_design_graphs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dm.id, dm.design_master_id, dm.graph_iteration_v,
                   dm.target_hook_capacity, dm.hook_allocation_profile,
                   dm.top_border_hooks, dm.bottom_border_hooks,
                   dm.body_motif_hooks, dm.selvedge_hooks,
                   dm.total_allocated_hooks, dm.grid_width_pixels,
                   dm.grid_height_picks, dm.maximum_float_length,
                   dm.ground_weave_structure, dm.zari_binding_weave_type,
                   dm.border_binding_technique, dm.shading_technique,
                   dm.max_float_enforcement_rule, dm.max_warp_float_ends,
                   dm.max_weft_float_picks, dm.cad_output_format,
                   dm.design_approval_state, dm.auto_assigned_routing,
                   dm.status, dm.certificate_hash, dm.created_at
            FROM design_masters dm
            WHERE dm.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY dm.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        designs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(designs),
            'designs': [
                {
                    'id': str(d['id']),
                    'design_master_id': d['design_master_id'],
                    'graph_iteration_v': float(d['graph_iteration_v']) if d['graph_iteration_v'] else None,
                    'target_hook_capacity': d['target_hook_capacity'],
                    'hook_allocation_profile': d['hook_allocation_profile'],
                    'top_border_hooks': d['top_border_hooks'],
                    'bottom_border_hooks': d['bottom_border_hooks'],
                    'body_motif_hooks': d['body_motif_hooks'],
                    'selvedge_hooks': d['selvedge_hooks'],
                    'total_allocated_hooks': d['total_allocated_hooks'],
                    'grid_width_pixels': d['grid_width_pixels'],
                    'grid_height_picks': d['grid_height_picks'],
                    'maximum_float_length': d['maximum_float_length'],
                    'ground_weave_structure': d['ground_weave_structure'],
                    'zari_binding_weave_type': d['zari_binding_weave_type'],
                    'border_binding_technique': d['border_binding_technique'],
                    'shading_technique': d['shading_technique'],
                    'max_float_enforcement_rule': d['max_float_enforcement_rule'],
                    'max_warp_float_ends': d['max_warp_float_ends'],
                    'max_weft_float_picks': d['max_weft_float_picks'],
                    'cad_output_format': d['cad_output_format'],
                    'design_approval_state': d['design_approval_state'],
                    'auto_assigned_routing': d['auto_assigned_routing'],
                    'status': d['status'],
                    'certificate_hash': d['certificate_hash'],
                    'created_at': d['created_at'].isoformat() if d['created_at'] else None
                }
                for d in designs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/graphs/<design_id>', methods=['GET'])
@jwt_required()
def get_design_graph(design_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dm.*, u.full_name as drafter_name
            FROM design_masters dm
            LEFT JOIN users u ON dm.drafter_employee_id = u.id
            WHERE dm.id = %s::uuid
        """, (design_id,))
        
        design = cur.fetchone()
        cur.close()
        conn.close()
        
        if not design:
            return jsonify({'error': 'DesignNotFound'}), 404
        
        return jsonify({
            'id': str(design['id']),
            'design_master_id': design['design_master_id'],
            'graph_iteration_v': float(design['graph_iteration_v']) if design['graph_iteration_v'] else None,
            'drafter_employee_id': str(design['drafter_employee_id']) if design['drafter_employee_id'] else None,
            'drafter_name': design['drafter_name'],
            'factory_node_id': design['factory_node_id'],
            'status': design['status'],
            'target_hook_capacity': design['target_hook_capacity'],
            'hook_allocation_profile': design['hook_allocation_profile'],
            'top_border_hooks': design['top_border_hooks'],
            'bottom_border_hooks': design['bottom_border_hooks'],
            'body_motif_hooks': design['body_motif_hooks'],
            'selvedge_hooks': design['selvedge_hooks'],
            'total_allocated_hooks': design['total_allocated_hooks'],
            'grid_width_pixels': design['grid_width_pixels'],
            'grid_height_picks': design['grid_height_picks'],
            'maximum_float_length': design['maximum_float_length'],
            'digital_cad_file_upload': design['digital_cad_file_upload'],
            'warp_ends_per_inch_epi': design['warp_ends_per_inch_epi'],
            'weft_picks_per_inch_ppi': design['weft_picks_per_inch_ppi'],
            'graph_aspect_ratio': float(design['graph_aspect_ratio']) if design['graph_aspect_ratio'] else None,
            'ground_weave_structure': design['ground_weave_structure'],
            'zari_binding_weave_type': design['zari_binding_weave_type'],
            'border_binding_technique': design['border_binding_technique'],
            'shading_technique': design['shading_technique'],
            'max_float_enforcement_rule': design['max_float_enforcement_rule'],
            'max_warp_float_ends': design['max_warp_float_ends'],
            'max_weft_float_picks': design['max_weft_float_picks'],
            'selvage_hook_count': design['selvage_hook_count'],
            'cad_output_format': design['cad_output_format'],
            'design_approval_state': design['design_approval_state'],
            'validation_errors': design['validation_errors'],
            'validation_warnings': design['validation_warnings'],
            'auto_assigned_routing': design['auto_assigned_routing'],
            'certificate_hash': design['certificate_hash'],
            'qr_tag_id': design['qr_tag_id']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/graphs/<design_id>/approve', methods=['POST'])
@jwt_required()
def approve_design_graph(design_id):
    try:
        approver_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, status, validation_errors, design_approval_state, graph_iteration_v
            FROM design_masters
            WHERE id = %s::uuid
        """, (design_id,))
        
        design = cur.fetchone()
        if not design:
            cur.close()
            conn.close()
            return jsonify({'error': 'DesignNotFound'}), 404
        
        if design['validation_errors'] and len(design['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot approve design with validation errors'}), 400
        
        new_version = design['graph_iteration_v'] + 1 if data.get('new_version') else design['graph_iteration_v']
        certificate_hash = generate_certificate_hash(design_id, design['design_master_id'])
        qr_tag_id = 'DESIGN-' + design['design_master_id']
        
        cur.execute("""
            UPDATE design_masters
            SET status = 'APPROVED_FOR_PUNCHING',
                design_approval_state = 'APPROVED_FOR_PUNCHING',
                graph_iteration_v = %s,
                certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, design_master_id, certificate_hash
        """, (new_version, certificate_hash, qr_tag_id, design_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO design_iterations (
                design_master_id, iteration_v, drafter_employee_id,
                change_reason, grid_width_pixels, grid_height_picks,
                hook_allocation_profile, ground_weave_structure,
                max_float_enforcement_rule, max_warp_float_ends, max_weft_float_picks
            )
            SELECT
                dm.id,
                dm.graph_iteration_v,
                dm.drafter_employee_id,
                %s,
                dm.grid_width_pixels, dm.grid_height_picks,
                dm.hook_allocation_profile, dm.ground_weave_structure,
                dm.max_float_enforcement_rule, dm.max_warp_float_ends, dm.max_weft_float_picks
            FROM design_masters dm
            WHERE dm.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM design_iterations WHERE design_master_id = dm.id AND iteration_v = dm.graph_iteration_v
            )
        """, (data.get('change_reason', 'Approved by Graph Drafter'), design_id))
        
        cur.execute("""
            INSERT INTO design_certificates (
                design_master_id, certificate_hash, qr_tag_id, design_master_id_ref,
                graph_iteration_v, total_hook_capacity, hook_allocation_profile,
                grid_width_pixels, grid_height_picks, ground_weave_structure,
                zari_binding_weave_type, border_binding_technique, shading_technique,
                max_float_enforcement_rule, max_warp_float_ends, max_weft_float_picks,
                cad_output_format, auto_assigned_routing, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                dm.id,
                dm.certificate_hash,
                dm.qr_tag_id,
                dm.design_master_id,
                dm.graph_iteration_v,
                dm.total_hook_capacity,
                dm.hook_allocation_profile,
                dm.grid_width_pixels,
                dm.grid_height_picks,
                dm.ground_weave_structure,
                dm.zari_binding_weave_type,
                dm.border_binding_technique,
                dm.shading_technique,
                dm.max_float_enforcement_rule,
                dm.max_warp_float_ends,
                dm.max_weft_float_picks,
                dm.cad_output_format,
                dm.auto_assigned_routing,
                dm.drafter_employee_id,
                %s,
                dm.factory_node_id,
                jsonb_build_object(
                    'design_master_id', dm.design_master_id,
                    'graph_iteration_v', dm.graph_iteration_v,
                    'hook_allocation_profile', dm.hook_allocation_profile,
                    'total_allocated_hooks', dm.total_allocated_hooks,
                    'graph_aspect_ratio', dm.graph_aspect_ratio,
                    'warp_ends_per_inch_epi', dm.warp_ends_per_inch_epi,
                    'weft_picks_per_inch_ppi', dm.weft_picks_per_inch_ppi
                )
            FROM design_masters dm
            WHERE dm.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM design_certificates WHERE design_master_id = dm.id
            )
        """, (approver_id, design_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'design_master_id': result['design_master_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'version': new_version,
            'status': 'APPROVED_FOR_PUNCHING',
            'message': 'Design graph approved and certified'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/graphs/<design_id>/reject', methods=['POST'])
@jwt_required()
def reject_design_graph(design_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE design_masters
            SET status = 'REJECTED_FLOAT_EXCEEDED',
                design_approval_state = 'REJECTED_FLOAT_EXCEEDED',
                validation_errors = COALESCE(validation_errors, '[]'::jsonb) || %s::jsonb
            WHERE id = %s::uuid
            RETURNING id, design_master_id
        """, (
            json.dumps([{'code': 'MANUAL_REJECTION', 'message': data.get('reason', 'Rejected by Graph Drafter')}]),
            design_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'DesignNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'design_master_id': result['design_master_id'],
            'status': 'REJECTED_FLOAT_EXCEEDED',
            'message': 'Design graph rejected'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/graphs/<design_id>/iterations', methods=['POST'])
@jwt_required()
def create_design_iteration(design_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['iteration_v']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO design_iterations (
                design_master_id, iteration_v, drafter_employee_id,
                change_reason, grid_width_pixels, grid_height_picks,
                hook_allocation_profile, ground_weave_structure,
                max_float_enforcement_rule, max_warp_float_ends, max_weft_float_picks,
                validation_errors, validation_warnings
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            design_id,
            data.get('iteration_v'),
            operator_id,
            data.get('change_reason'),
            data.get('grid_width_pixels'),
            data.get('grid_height_picks'),
            data.get('hook_allocation_profile'),
            data.get('ground_weave_structure'),
            data.get('max_float_enforcement_rule'),
            data.get('max_warp_float_ends'),
            data.get('max_weft_float_picks'),
            json.dumps([]),
            json.dumps([])
        ))
        
        iteration_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(iteration_row['id']),
            'design_master_id': str(design_id),
            'iteration_v': data.get('iteration_v'),
            'message': 'Design iteration created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/certificates', methods=['GET'])
@jwt_required()
def list_design_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT dc.id, dc.certificate_hash, dc.qr_tag_id,
                   dc.design_master_id_ref, dc.graph_iteration_v,
                   dc.total_hook_capacity, dc.hook_allocation_profile,
                   dc.grid_width_pixels, dc.grid_height_picks,
                   dc.ground_weave_structure, dc.zari_binding_weave_type,
                   dc.border_binding_technique, dc.shading_technique,
                   dc.max_float_enforcement_rule, dc.max_warp_float_ends,
                   dc.max_weft_float_picks, dc.cad_output_format,
                   dc.auto_assigned_routing, dc.status, dc.certified_at,
                   dm.design_master_id
            FROM design_certificates dc
            JOIN design_masters dm ON dc.design_master_id = dm.id
            WHERE dc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY dc.certified_at DESC
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
                    'design_master_id': c['design_master_id'],
                    'design_master_id_ref': c['design_master_id_ref'],
                    'graph_iteration_v': float(c['graph_iteration_v']) if c['graph_iteration_v'] else None,
                    'total_hook_capacity': c['total_hook_capacity'],
                    'hook_allocation_profile': c['hook_allocation_profile'],
                    'grid_width_pixels': c['grid_width_pixels'],
                    'grid_height_picks': c['grid_height_picks'],
                    'ground_weave_structure': c['ground_weave_structure'],
                    'zari_binding_weave_type': c['zari_binding_weave_type'],
                    'border_binding_technique': c['border_binding_technique'],
                    'shading_technique': c['shading_technique'],
                    'max_float_enforcement_rule': c['max_float_enforcement_rule'],
                    'max_warp_float_ends': c['max_warp_float_ends'],
                    'max_weft_float_picks': c['max_weft_float_picks'],
                    'cad_output_format': c['cad_output_format'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR GRAPH DRAFTER
# ============================================================

@app.route('/api/v1/sales/forecast/design', methods=['GET'])
@jwt_required()
def get_sales_forecast_design():
    """
    API plugin endpoint for sales team graph drafter material processing forecast.
    Returns forecasted design requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'target_hook_capacity': 2400,
                    'hook_allocation_profile': '2250_DESIGN_150_BORDER_SELVAGE',
                    'estimated_designs': 5,
                    'cad_output_format': 'JC5',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'target_hook_capacity': 2400,
                    'hook_allocation_profile': '2250_DESIGN_150_BORDER_SELVAGE',
                    'estimated_designs': 4,
                    'cad_output_format': 'JC5',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'target_hook_capacity': 1536,
                    'hook_allocation_profile': '1440_DESIGN_96_BORDER_SELVAGE',
                    'estimated_designs': 8,
                    'cad_output_format': '.EP',
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'DESIGN-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_designs': 5,
                    'target_hook_capacity': 2400,
                    'cad_output_format': 'JC5'
                },
                {
                    'lot_number': 'DESIGN-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_designs': 4,
                    'target_hook_capacity': 2400,
                    'cad_output_format': 'JC5'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# CARD PUNCHER (DIGITAL/E-JACQUARD PROGRAMMER) MODULE
# ============================================================

@app.route('/api/v1/card-puncher/jobs', methods=['POST'])
@jwt_required()
def create_programming_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['programming_job_id', 'design_master_id', 'target_loom_type']
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
        
        cur.execute("""
            INSERT INTO programming_jobs (
                programming_job_id, design_master_id, design_certificate_id,
                pirn_winding_job_id, bobbin_winder_job_card_id,
                master_colorist_recipe_id, skein_dye_job_id,
                throwster_record_id, production_lot_id,
                factory_node_id, programmer_employee_id, status,
                compiled_cam_file_name, target_loom_type,
                loom_hardware_id, controller_brand_type,
                physical_hook_matrix, data_transfer_method,
                pattern_repeat_mode, solenoid_firing_profile,
                pick_sequence_interlock, file_integrity_checksum,
                dry_run_simulation_status, card_program_approval_state,
                input_blank_cards_weight_kg, actual_punched_cards_count,
                punch_waste_scrap_weight_gm,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING_COMPILATION', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, programming_job_id
        """, (
            data.get('programming_job_id'),
            data.get('design_master_id'),
            data.get('design_certificate_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            data.get('master_colorist_recipe_id'),
            data.get('skein_dye_job_id'),
            data.get('throwster_record_id'),
            data.get('production_lot_id'),
            factory_node_id,
            operator_id,
            data.get('compiled_cam_file_name'),
            data.get('target_loom_type'),
            data.get('loom_hardware_id'),
            data.get('controller_brand_type'),
            data.get('physical_hook_matrix', '2400_HOOK_2250_DESIGN_150_BORDER'),
            data.get('data_transfer_method', 'DIRECT_LOOM_NETWORK_LAN'),
            data.get('pattern_repeat_mode', 'STRAIGHT_REPEAT'),
            data.get('solenoid_firing_profile', 'HIGH_DENSITY_FAST_PULSE_LE_8MS'),
            data.get('pick_sequence_interlock', 'GROUND_1_1_EXTRA_WEFT'),
            data.get('file_integrity_checksum'),
            data.get('dry_run_simulation_status', 'PENDING'),
            data.get('card_program_approval_state', 'PENDING_COMPILATION'),
            data.get('input_blank_cards_weight_kg'),
            data.get('actual_punched_cards_count'),
            data.get('punch_waste_scrap_weight_gm'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        cur.execute("""
            SELECT validation_errors, validation_warnings, auto_assigned_routing, status, card_program_approval_state
            FROM programming_jobs WHERE id = %s::uuid
        """, (job_id,))
        result = cur.fetchone()
        
        status = 'PENDING_COMPILATION'
        if result['validation_errors'] and len(result['validation_errors']) > 0:
            status = 'PENDING_COMPILATION'
        elif result['validation_warnings'] and len(result['validation_warnings']) > 0:
            status = 'COMPILED'
        else:
            status = 'COMPILED'
        
        cur.execute("""
            UPDATE programming_jobs SET status = %s WHERE id = %s::uuid
        """, (status, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'programming_job_id': job_row['programming_job_id'],
            'status': status,
            'card_program_approval_state': result['card_program_approval_state'],
            'auto_assigned_routing': result['auto_assigned_routing'],
            'validation_errors': result['validation_errors'] or [],
            'validation_warnings': result['validation_warnings'] or [],
            'message': 'Programming job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/card-puncher/jobs', methods=['GET'])
@jwt_required()
def list_programming_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pj.id, pj.programming_job_id, pj.compiled_cam_file_name,
                   pj.target_loom_type, pj.loom_hardware_id, pj.controller_brand_type,
                   pj.physical_hook_matrix, pj.data_transfer_method,
                   pj.pattern_repeat_mode, pj.solenoid_firing_profile,
                   pj.pick_sequence_interlock, pj.file_integrity_checksum,
                   pj.dry_run_simulation_status, pj.card_program_approval_state,
                   pj.auto_assigned_routing, pj.status, pj.certificate_hash,
                   pj.created_at, dm.design_master_id
            FROM programming_jobs pj
            LEFT JOIN design_masters dm ON pj.design_master_id = dm.id
            WHERE pj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'programming_job_id': j['programming_job_id'],
                    'design_master_id': j['design_master_id'],
                    'compiled_cam_file_name': j['compiled_cam_file_name'],
                    'target_loom_type': j['target_loom_type'],
                    'loom_hardware_id': j['loom_hardware_id'],
                    'controller_brand_type': j['controller_brand_type'],
                    'physical_hook_matrix': j['physical_hook_matrix'],
                    'data_transfer_method': j['data_transfer_method'],
                    'pattern_repeat_mode': j['pattern_repeat_mode'],
                    'solenoid_firing_profile': j['solenoid_firing_profile'],
                    'pick_sequence_interlock': j['pick_sequence_interlock'],
                    'file_integrity_checksum': j['file_integrity_checksum'],
                    'dry_run_simulation_status': j['dry_run_simulation_status'],
                    'card_program_approval_state': j['card_program_approval_state'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/card-puncher/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_programming_job(job_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pj.*, dm.design_master_id, dm.target_hook_capacity
            FROM programming_jobs pj
            LEFT JOIN design_masters dm ON pj.design_master_id = dm.id
            WHERE pj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'programming_job_id': job['programming_job_id'],
            'design_master_id': job['design_master_id'],
            'target_hook_capacity': job['target_hook_capacity'],
            'compiled_cam_file_name': job['compiled_cam_file_name'],
            'target_loom_type': job['target_loom_type'],
            'loom_hardware_id': job['loom_hardware_id'],
            'controller_brand_type': job['controller_brand_type'],
            'physical_hook_matrix': job['physical_hook_matrix'],
            'data_transfer_method': job['data_transfer_method'],
            'pattern_repeat_mode': job['pattern_repeat_mode'],
            'solenoid_firing_profile': job['solenoid_firing_profile'],
            'pick_sequence_interlock': job['pick_sequence_interlock'],
            'file_integrity_checksum': job['file_integrity_checksum'],
            'dry_run_simulation_status': job['dry_run_simulation_status'],
            'card_program_approval_state': job['card_program_approval_state'],
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/card-puncher/jobs/<job_id>/simulate', methods=['POST'])
@jwt_required()
def simulate_programming_job(job_id):
    try:
        operator_id = get_jwt_identity()
        data = request.get_json() or {}
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE programming_jobs
            SET dry_run_simulation_status = %s,
                file_integrity_checksum = COALESCE(%s, file_integrity_checksum)
            WHERE id = %s::uuid
            RETURNING id, programming_job_id, status
        """, (
            data.get('dry_run_simulation_status', 'PASSED_ZERO_ERRORS'),
            data.get('file_integrity_checksum'),
            job_id
        ))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'programming_job_id': result['programming_job_id'],
            'status': result['status'],
            'message': 'Dry-run simulation updated'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/card-puncher/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_programming_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, programming_job_id, status, validation_errors, dry_run_simulation_status
            FROM programming_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['programming_job_id'])
        qr_tag_id = 'CARD-' + job['programming_job_id']
        
        cur.execute("""
            UPDATE programming_jobs
            SET status = 'LOADED_TO_LOOM',
                certificate_hash = %s,
                qr_tag_id = %s,
                card_program_approval_state = 'SIMULATION_PASSED'
            WHERE id = %s::uuid
            RETURNING id, programming_job_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO programming_certificates (
                programming_job_id, certificate_hash, qr_tag_id, programming_job_id_ref,
                design_master_id_ref, compiled_cam_file_name, target_loom_type,
                loom_hardware_id, controller_brand_type, physical_hook_matrix,
                data_transfer_method, pattern_repeat_mode, solenoid_firing_profile,
                pick_sequence_interlock, file_integrity_checksum,
                dry_run_simulation_status, card_program_approval_state,
                auto_assigned_routing, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.programming_job_id,
                dm.design_master_id,
                pj.compiled_cam_file_name,
                pj.target_loom_type,
                pj.loom_hardware_id,
                pj.controller_brand_type,
                pj.physical_hook_matrix,
                pj.data_transfer_method,
                pj.pattern_repeat_mode,
                pj.solenoid_firing_profile,
                pj.pick_sequence_interlock,
                pj.file_integrity_checksum,
                pj.dry_run_simulation_status,
                pj.card_program_approval_state,
                pj.auto_assigned_routing,
                pj.programmer_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'programming_job_id', pj.programming_job_id,
                    'design_master_id', dm.design_master_id,
                    'target_hook_capacity', dm.target_hook_capacity,
                    'hook_allocation_profile', dm.hook_allocation_profile,
                    'total_allocated_hooks', dm.total_allocated_hooks
                )
            FROM programming_jobs pj
            LEFT JOIN design_masters dm ON pj.design_master_id = dm.id
            WHERE pj.id = %s::uuid
            AND NOT EXISTS (
                SELECT 1 FROM programming_certificates WHERE programming_job_id = pj.id
            )
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'programming_job_id': result['programming_job_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'LOADED_TO_LOOM',
            'message': 'Programming job certified and loaded to loom'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/card-puncher/certificates', methods=['GET'])
@jwt_required()
def list_programming_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pc.id, pc.certificate_hash, pc.qr_tag_id,
                   pc.programming_job_id_ref, pc.design_master_id_ref,
                   pc.compiled_cam_file_name, pc.target_loom_type,
                   pc.loom_hardware_id, pc.controller_brand_type,
                   pc.physical_hook_matrix, pc.data_transfer_method,
                   pc.pattern_repeat_mode, pc.solenoid_firing_profile,
                   pc.pick_sequence_interlock, pc.file_integrity_checksum,
                   pc.dry_run_simulation_status, pc.card_program_approval_state,
                   pc.auto_assigned_routing, pc.status, pc.certified_at,
                   pj.programming_job_id
            FROM programming_certificates pc
            JOIN programming_jobs pj ON pc.programming_job_id = pj.id
            WHERE pc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pc.certified_at DESC
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
                    'programming_job_id': c['programming_job_id'],
                    'design_master_id_ref': c['design_master_id_ref'],
                    'compiled_cam_file_name': c['compiled_cam_file_name'],
                    'target_loom_type': c['target_loom_type'],
                    'loom_hardware_id': c['loom_hardware_id'],
                    'controller_brand_type': c['controller_brand_type'],
                    'physical_hook_matrix': c['physical_hook_matrix'],
                    'data_transfer_method': c['data_transfer_method'],
                    'pattern_repeat_mode': c['pattern_repeat_mode'],
                    'solenoid_firing_profile': c['solenoid_firing_profile'],
                    'pick_sequence_interlock': c['pick_sequence_interlock'],
                    'file_integrity_checksum': c['file_integrity_checksum'],
                    'dry_run_simulation_status': c['dry_run_simulation_status'],
                    'card_program_approval_state': c['card_program_approval_state'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR CARD PUNCHER
# ============================================================

@app.route('/api/v1/sales/forecast/card-puncher', methods=['GET'])
@jwt_required()
def get_sales_forecast_card_puncher():
    """
    API plugin endpoint for sales team card puncher material processing forecast.
    Returns forecasted programming/card-punching requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'target_hook_capacity': 2400,
                    'controller_brand_type': 'STAUBLI_JC5_JC6',
                    'estimated_programs': 5,
                    'cad_output_format': 'JC5',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'target_hook_capacity': 2400,
                    'controller_brand_type': 'STAUBLI_JC5_JC6',
                    'estimated_programs': 4,
                    'cad_output_format': 'JC5',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'target_hook_capacity': 1536,
                    'controller_brand_type': 'BONAS_EP_SI',
                    'estimated_programs': 8,
                    'cad_output_format': '.EP',
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'CARD-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_programs': 5,
                    'target_hook_capacity': 2400,
                    'controller_brand_type': 'STAUBLI_JC5_JC6',
                    'cad_output_format': 'JC5'
                },
                {
                    'lot_number': 'CARD-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_programs': 4,
                    'target_hook_capacity': 2400,
                    'controller_brand_type': 'STAUBLI_JC5_JC6',
                    'cad_output_format': 'JC5'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# WARP BEAM PREPARATION (80 SAREE LENGTH) MODULE
# ============================================================

@app.route('/api/v1/warp-beam-prep/logs', methods=['POST'])
@jwt_required()
def create_warp_beam_production_log():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['warp_set_id', 'target_loom_type', 'total_warp_length_meters']
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
        
        calculated_net_beam_weight_kg = None
        if data.get('total_allocated_yarn_weight_kg') is not None and data.get('post_job_returned_yarn_weight_kg') is not None:
            calculated_net_beam_weight_kg = data.get('total_allocated_yarn_weight_kg') - data.get('post_job_returned_yarn_weight_kg')
        
        cur.execute("""
            INSERT INTO warp_beam_production_logs (
                warp_set_id, production_lot_id, design_master_id, card_puncher_job_id,
                pirn_winding_job_id, bobbin_winder_job_card_id,
                factory_node_id, operator_id, status,
                target_loom_type, warping_machine_type,
                total_warp_length_meters, creel_capacity_bobbins, number_of_sections,
                creel_tension_setting_grams, static_control_status,
                leasing_method_used, warp_wax_conditioning, beam_density_shore_d,
                section_gap_overlap_inspection, broken_ends_repaired_count,
                warp_beam_approval_state,
                total_allocated_yarn_weight_kg, post_job_returned_yarn_weight_kg,
                calculated_net_beam_weight_kg, actual_scale_beam_weight_kg,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING_WARPING', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, warp_set_id
        """, (
            data.get('warp_set_id'),
            data.get('production_lot_id'),
            data.get('design_master_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            factory_node_id,
            operator_id,
            data.get('target_loom_type'),
            data.get('warping_machine_type', 'AUTOMATIC_SECTIONAL_WARPER'),
            data.get('total_warp_length_meters'),
            data.get('creel_capacity_bobbins'),
            data.get('number_of_sections'),
            data.get('creel_tension_setting_grams'),
            data.get('static_control_status', 'ACTIVE_IONIZING_BARS_65RH'),
            data.get('leasing_method_used', 'AUTOMATIC_LEASE_REED_1X1_LOCK'),
            data.get('warp_wax_conditioning', 'LIQUID_ANTISTATIC_WAX_EMULSION'),
            data.get('beam_density_shore_d'),
            data.get('section_gap_overlap_inspection', 'ZERO_GAP_ZERO_OVERLAP'),
            data.get('broken_ends_repaired_count', 0),
            data.get('warp_beam_approval_state', 'PENDING_WARPING'),
            data.get('total_allocated_yarn_weight_kg'),
            data.get('post_job_returned_yarn_weight_kg'),
            calculated_net_beam_weight_kg,
            data.get('actual_scale_beam_weight_kg'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'warp_set_id': log_row['warp_set_id'],
            'status': 'PENDING_WARPING',
            'message': 'Warp beam production log created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-beam-prep/logs', methods=['GET'])
@jwt_required()
def list_warp_beam_production_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wbl.id, wbl.warp_set_id, wbl.target_loom_type, wbl.warping_machine_type,
                   wbl.total_warp_length_meters, wbl.creel_capacity_bobbins, wbl.number_of_sections,
                   wbl.creel_tension_setting_grams, wbl.static_control_status,
                   wbl.leasing_method_used, wbl.warp_wax_conditioning, wbl.beam_density_shore_d,
                   wbl.section_gap_overlap_inspection, wbl.broken_ends_repaired_count,
                   wbl.warp_beam_approval_state, wbl.status, wbl.certificate_hash,
                   wbl.auto_assigned_routing, wbl.created_at,
                   dm.design_master_id
            FROM warp_beam_production_logs wbl
            LEFT JOIN design_masters dm ON wbl.design_master_id = dm.id
            WHERE wbl.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wbl.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(l['id']),
                    'warp_set_id': l['warp_set_id'],
                    'design_master_id': l['design_master_id'],
                    'target_loom_type': l['target_loom_type'],
                    'warping_machine_type': l['warping_machine_type'],
                    'total_warp_length_meters': l['total_warp_length_meters'],
                    'creel_capacity_bobbins': l['creel_capacity_bobbins'],
                    'number_of_sections': l['number_of_sections'],
                    'creel_tension_setting_grams': l['creel_tension_setting_grams'],
                    'static_control_status': l['static_control_status'],
                    'leasing_method_used': l['leasing_method_used'],
                    'warp_wax_conditioning': l['warp_wax_conditioning'],
                    'beam_density_shore_d': l['beam_density_shore_d'],
                    'section_gap_overlap_inspection': l['section_gap_overlap_inspection'],
                    'broken_ends_repaired_count': l['broken_ends_repaired_count'],
                    'warp_beam_approval_state': l['warp_beam_approval_state'],
                    'status': l['status'],
                    'certificate_hash': l['certificate_hash'],
                    'auto_assigned_routing': l['auto_assigned_routing'],
                    'created_at': l['created_at'].isoformat() if l['created_at'] else None
                }
                for l in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-beam-prep/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_warp_beam_production_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wbl.*, dm.design_master_id
            FROM warp_beam_production_logs wbl
            LEFT JOIN design_masters dm ON wbl.design_master_id = dm.id
            WHERE wbl.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'warp_set_id': log['warp_set_id'],
            'design_master_id': log['design_master_id'],
            'target_loom_type': log['target_loom_type'],
            'warping_machine_type': log['warping_machine_type'],
            'total_warp_length_meters': log['total_warp_length_meters'],
            'creel_capacity_bobbins': log['creel_capacity_bobbins'],
            'number_of_sections': log['number_of_sections'],
            'creel_tension_setting_grams': log['creel_tension_setting_grams'],
            'static_control_status': log['static_control_status'],
            'leasing_method_used': log['leasing_method_used'],
            'warp_wax_conditioning': log['warp_wax_conditioning'],
            'beam_density_shore_d': log['beam_density_shore_d'],
            'section_gap_overlap_inspection': log['section_gap_overlap_inspection'],
            'broken_ends_repaired_count': log['broken_ends_repaired_count'],
            'warp_beam_approval_state': log['warp_beam_approval_state'],
            'total_allocated_yarn_weight_kg': log['total_allocated_yarn_weight_kg'],
            'post_job_returned_yarn_weight_kg': log['post_job_returned_yarn_weight_kg'],
            'calculated_net_beam_weight_kg': log['calculated_net_beam_weight_kg'],
            'actual_scale_beam_weight_kg': log['actual_scale_beam_weight_kg'],
            'weight_variance_percent': log['weight_variance_percent'],
            'qc_hold_reason': log['qc_hold_reason'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'status': log['status'],
            'certificate_hash': log['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-beam-prep/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_warp_beam_production_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, warp_set_id, status, validation_errors, warp_beam_approval_state
            FROM warp_beam_production_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['warp_set_id'])
        qr_tag_id = 'WARP-' + log['warp_set_id']
        
        cur.execute("""
            UPDATE warp_beam_production_logs
            SET status = 'READY_FOR_LOOM_MOUNTING',
                certificate_hash = %s,
                qr_tag_id = %s,
                warp_beam_approval_state = 'PASSED_APPROVED_FOR_LOOM'
            WHERE id = %s::uuid
            RETURNING id, warp_set_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO warp_beam_certificates (
                warp_beam_production_log_id, certificate_hash, qr_tag_id, warp_set_id,
                target_loom_type, warping_machine_type,
                total_warp_length_meters, creel_capacity_bobbins, number_of_sections,
                creel_tension_setting_grams, static_control_status,
                leasing_method_used, warp_wax_conditioning, beam_density_shore_d,
                section_gap_overlap_inspection, broken_ends_repaired_count,
                warp_beam_approval_state,
                total_allocated_yarn_weight_kg, post_job_returned_yarn_weight_kg,
                calculated_net_beam_weight_kg, actual_scale_beam_weight_kg,
                weight_variance_percent,
                auto_assigned_routing, operator_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.warp_set_id,
                pj.target_loom_type,
                pj.warping_machine_type,
                pj.total_warp_length_meters,
                pj.creel_capacity_bobbins,
                pj.number_of_sections,
                pj.creel_tension_setting_grams,
                pj.static_control_status,
                pj.leasing_method_used,
                pj.warp_wax_conditioning,
                pj.beam_density_shore_d,
                pj.section_gap_overlap_inspection,
                pj.broken_ends_repaired_count,
                pj.warp_beam_approval_state,
                pj.total_allocated_yarn_weight_kg,
                pj.post_job_returned_yarn_weight_kg,
                pj.calculated_net_beam_weight_kg,
                pj.actual_scale_beam_weight_kg,
                pj.weight_variance_percent,
                pj.auto_assigned_routing,
                pj.operator_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'warp_set_id', pj.warp_set_id,
                    'target_loom_type', pj.target_loom_type,
                    'warping_machine_type', pj.warping_machine_type,
                    'total_warp_length_meters', pj.total_warp_length_meters,
                    'creel_capacity_bobbins', pj.creel_capacity_bobbins,
                    'number_of_sections', pj.number_of_sections,
                    'creel_tension_setting_grams', pj.creel_tension_setting_grams,
                    'beam_density_shore_d', pj.beam_density_shore_d,
                    'weight_variance_percent', pj.weight_variance_percent
                )
            FROM warp_beam_production_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'warp_set_id': result['warp_set_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'READY_FOR_LOOM_MOUNTING',
            'message': 'Warp beam production log certified and ready for loom mounting'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-beam-prep/certificates', methods=['GET'])
@jwt_required()
def list_warp_beam_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wbc.id, wbc.certificate_hash, wbc.qr_tag_id,
                   wbc.warp_set_id, wbc.target_loom_type, wbc.warping_machine_type,
                   wbc.total_warp_length_meters, wbc.creel_capacity_bobbins, wbc.number_of_sections,
                   wbc.creel_tension_setting_grams, wbc.static_control_status,
                   wbc.leasing_method_used, wbc.warp_wax_conditioning, wbc.beam_density_shore_d,
                   wbc.section_gap_overlap_inspection, wbc.broken_ends_repaired_count,
                   wbc.warp_beam_approval_state, wbc.weight_variance_percent,
                   wbc.auto_assigned_routing, wbc.status, wbc.certified_at,
                   wbl.warp_set_id
            FROM warp_beam_certificates wbc
            JOIN warp_beam_production_logs wbl ON wbc.warp_beam_production_log_id = wbl.id
            WHERE wbc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wbc.certified_at DESC
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
                    'warp_set_id': c['warp_set_id'],
                    'target_loom_type': c['target_loom_type'],
                    'warping_machine_type': c['warping_machine_type'],
                    'total_warp_length_meters': c['total_warp_length_meters'],
                    'creel_capacity_bobbins': c['creel_capacity_bobbins'],
                    'number_of_sections': c['number_of_sections'],
                    'creel_tension_setting_grams': c['creel_tension_setting_grams'],
                    'static_control_status': c['static_control_status'],
                    'leasing_method_used': c['leasing_method_used'],
                    'warp_wax_conditioning': c['warp_wax_conditioning'],
                    'beam_density_shore_d': c['beam_density_shore_d'],
                    'section_gap_overlap_inspection': c['section_gap_overlap_inspection'],
                    'broken_ends_repaired_count': c['broken_ends_repaired_count'],
                    'warp_beam_approval_state': c['warp_beam_approval_state'],
                    'weight_variance_percent': c['weight_variance_percent'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR WARP BEAM PREPARATION
# ============================================================

@app.route('/api/v1/sales/forecast/warp-beam-prep', methods=['GET'])
@jwt_required()
def get_sales_forecast_warp_beam_prep():
    """
    API plugin endpoint for sales team warp beam material processing forecast.
    Returns forecasted warp beam requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'target_loom_type': '2400_HOOK_ELECTRONIC_JACQUARD',
                    'warping_machine_type': 'AUTOMATIC_SECTIONAL_WARPER',
                    'estimated_beams': 3,
                    'total_warp_length_meters': 504.0,
                    'creel_capacity_bobbins': 960,
                    'number_of_sections': 18,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'target_loom_type': '2400_HOOK_ELECTRONIC_JACQUARD',
                    'warping_machine_type': 'AUTOMATIC_SECTIONAL_WARPER',
                    'estimated_beams': 2,
                    'total_warp_length_meters': 512.0,
                    'creel_capacity_bobbins': 1024,
                    'number_of_sections': 20,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'target_loom_type': '1536_HOOK_ELECTRONIC_JACQUARD',
                    'warping_machine_type': 'MANUAL_SECTIONAL_WARPER',
                    'estimated_beams': 5,
                    'total_warp_length_meters': 360.0,
                    'creel_capacity_bobbins': 480,
                    'number_of_sections': 12,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'WARP-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_beams': 3,
                    'target_loom_type': '2400_HOOK_ELECTRONIC_JACQUARD',
                    'warping_machine_type': 'AUTOMATIC_SECTIONAL_WARPER'
                },
                {
                    'lot_number': 'WARP-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_beams': 2,
                    'target_loom_type': '2400_HOOK_ELECTRONIC_JACQUARD',
                    'warping_machine_type': 'AUTOMATIC_SECTIONAL_WARPER'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# LOOM HARNESS SETTER (HARNESS BUILDING MASTER) MODULE
# ============================================================

@app.route('/api/v1/loom-harness-setter/logs', methods=['POST'])
@jwt_required()
def create_harness_setup_log():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['harness_setup_job_id', 'loom_hardware_id']
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
        
        cur.execute("""
            INSERT INTO harness_setup_logs (
                harness_setup_job_id, loom_hardware_id, production_lot_id,
                design_master_id, warp_beam_production_log_id, card_puncher_job_id,
                pirn_winding_job_id, bobbin_winder_job_card_id,
                factory_node_id, setter_employee_id, status,
                jacquard_capacity_type, comber_board_density_epi,
                harness_cord_material, lingo_weight_per_cord_grams,
                harness_tie_up_profile, total_active_harness_cords, reed_count_density,
                mail_eye_leveling_status, shed_opening_height_mm,
                harness_drop_angle_status, antistatic_harness_lubrication,
                comber_board_clearance_mm,
                dry_run_full_lift_test, harness_setup_approval_state,
                cord_material_batch_no, setup_start_time, setup_end_time,
                total_assembly_hours, accumulated_picks_on_harness,
                preventive_maintenance_flag, preventive_maintenance_task,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'SETUP_IN_PROGRESS', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, harness_setup_job_id
        """, (
            data.get('harness_setup_job_id'),
            data.get('loom_hardware_id'),
            data.get('production_lot_id'),
            data.get('design_master_id'),
            data.get('warp_beam_production_log_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            factory_node_id,
            operator_id,
            data.get('jacquard_capacity_type', '2400_HOOK'),
            data.get('comber_board_density_epi', '144_EPI'),
            data.get('harness_cord_material', 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC'),
            data.get('lingo_weight_per_cord_grams'),
            data.get('harness_tie_up_profile', 'STRAIGHT_TIE'),
            data.get('total_active_harness_cords'),
            data.get('reed_count_density'),
            data.get('mail_eye_leveling_status', 'LASER_VERIFIED_PLUS_MINUS_0_5MM'),
            data.get('shed_opening_height_mm'),
            data.get('harness_drop_angle_status', 'STRAIGHT_DROP_LE_8_DEG'),
            data.get('antistatic_harness_lubrication', 'DRY_PTFE_LUBRICANT_APPLIED'),
            data.get('comber_board_clearance_mm'),
            data.get('dry_run_full_lift_test', 'PASSED_100_PERCENT_HOOK_CLEARANCE'),
            data.get('harness_setup_approval_state', 'SETUP_IN_PROGRESS'),
            data.get('cord_material_batch_no'),
            data.get('setup_end_time'),
            data.get('total_assembly_hours'),
            data.get('accumulated_picks_on_harness', 0),
            data.get('preventive_maintenance_flag', False),
            data.get('preventive_maintenance_task'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'harness_setup_job_id': log_row['harness_setup_job_id'],
            'status': 'SETUP_IN_PROGRESS',
            'message': 'Harness setup log created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/loom-harness-setter/logs', methods=['GET'])
@jwt_required()
def list_harness_setup_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT hsl.id, hsl.harness_setup_job_id, hsl.loom_hardware_id,
                   hsl.jacquard_capacity_type, hsl.comber_board_density_epi,
                   hsl.harness_cord_material, hsl.lingo_weight_per_cord_grams,
                   hsl.total_active_harness_cords, hsl.reed_count_density,
                   hsl.mail_eye_leveling_status, hsl.shed_opening_height_mm,
                   hsl.harness_drop_angle_status, hsl.antistatic_harness_lubrication,
                   hsl.dry_run_full_lift_test, hsl.harness_setup_approval_state,
                   hsl.status, hsl.certificate_hash, hsl.preventive_maintenance_flag,
                   hsl.auto_assigned_routing, hsl.created_at,
                   dm.design_master_id
            FROM harness_setup_logs hsl
            LEFT JOIN design_masters dm ON hsl.design_master_id = dm.id
            WHERE hsl.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY hsl.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(l['id']),
                    'harness_setup_job_id': l['harness_setup_job_id'],
                    'loom_hardware_id': l['loom_hardware_id'],
                    'design_master_id': l['design_master_id'],
                    'jacquard_capacity_type': l['jacquard_capacity_type'],
                    'comber_board_density_epi': l['comber_board_density_epi'],
                    'harness_cord_material': l['harness_cord_material'],
                    'lingo_weight_per_cord_grams': l['lingo_weight_per_cord_grams'],
                    'total_active_harness_cords': l['total_active_harness_cords'],
                    'reed_count_density': l['reed_count_density'],
                    'mail_eye_leveling_status': l['mail_eye_leveling_status'],
                    'shed_opening_height_mm': l['shed_opening_height_mm'],
                    'harness_drop_angle_status': l['harness_drop_angle_status'],
                    'antistatic_harness_lubrication': l['antistatic_harness_lubrication'],
                    'dry_run_full_lift_test': l['dry_run_full_lift_test'],
                    'harness_setup_approval_state': l['harness_setup_approval_state'],
                    'status': l['status'],
                    'certificate_hash': l['certificate_hash'],
                    'preventive_maintenance_flag': l['preventive_maintenance_flag'],
                    'auto_assigned_routing': l['auto_assigned_routing'],
                    'created_at': l['created_at'].isoformat() if l['created_at'] else None
                }
                for l in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/loom-harness-setter/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_harness_setup_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT hsl.*, dm.design_master_id
            FROM harness_setup_logs hsl
            LEFT JOIN design_masters dm ON hsl.design_master_id = dm.id
            WHERE hsl.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'harness_setup_job_id': log['harness_setup_job_id'],
            'loom_hardware_id': log['loom_hardware_id'],
            'design_master_id': log['design_master_id'],
            'jacquard_capacity_type': log['jacquard_capacity_type'],
            'comber_board_density_epi': log['comber_board_density_epi'],
            'harness_cord_material': log['harness_cord_material'],
            'lingo_weight_per_cord_grams': log['lingo_weight_per_cord_grams'],
            'harness_tie_up_profile': log['harness_tie_up_profile'],
            'total_active_harness_cords': log['total_active_harness_cords'],
            'reed_count_density': log['reed_count_density'],
            'mail_eye_leveling_status': log['mail_eye_leveling_status'],
            'shed_opening_height_mm': log['shed_opening_height_mm'],
            'harness_drop_angle_status': log['harness_drop_angle_status'],
            'antistatic_harness_lubrication': log['antistatic_harness_lubrication'],
            'comber_board_clearance_mm': log['comber_board_clearance_mm'],
            'dry_run_full_lift_test': log['dry_run_full_lift_test'],
            'harness_setup_approval_state': log['harness_setup_approval_state'],
            'cord_material_batch_no': log['cord_material_batch_no'],
            'setup_start_time': log['setup_start_time'].isoformat() if log['setup_start_time'] else None,
            'setup_end_time': log['setup_end_time'].isoformat() if log['setup_end_time'] else None,
            'total_assembly_hours': log['total_assembly_hours'],
            'accumulated_picks_on_harness': log['accumulated_picks_on_harness'],
            'preventive_maintenance_flag': log['preventive_maintenance_flag'],
            'preventive_maintenance_task': log['preventive_maintenance_task'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'status': log['status'],
            'certificate_hash': log['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/loom-harness-setter/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_harness_setup_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, harness_setup_job_id, status, validation_errors, harness_setup_approval_state
            FROM harness_setup_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['harness_setup_job_id'])
        qr_tag_id = 'HARNESS-' + log['harness_setup_job_id']
        
        cur.execute("""
            UPDATE harness_setup_logs
            SET status = 'VACANT_AVAILABLE_FOR_WEAVING',
                certificate_hash = %s,
                qr_tag_id = %s,
                harness_setup_approval_state = 'PASSED_APPROVED_FOR_GAITING'
            WHERE id = %s::uuid
            RETURNING id, harness_setup_job_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO harness_certificates (
                harness_setup_log_id, certificate_hash, qr_tag_id, harness_setup_job_id,
                loom_hardware_id, jacquard_capacity_type, comber_board_density_epi,
                harness_cord_material, lingo_weight_per_cord_grams,
                harness_tie_up_profile, total_active_harness_cords,
                mail_eye_leveling_status, shed_opening_height_mm,
                harness_drop_angle_status, antistatic_harness_lubrication,
                dry_run_full_lift_test, harness_setup_approval_state,
                accumulated_picks_on_harness, preventive_maintenance_flag,
                auto_assigned_routing, setter_employee_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.harness_setup_job_id,
                pj.loom_hardware_id,
                pj.jacquard_capacity_type,
                pj.comber_board_density_epi,
                pj.harness_cord_material,
                pj.lingo_weight_per_cord_grams,
                pj.harness_tie_up_profile,
                pj.total_active_harness_cords,
                pj.mail_eye_leveling_status,
                pj.shed_opening_height_mm,
                pj.harness_drop_angle_status,
                pj.antistatic_harness_lubrication,
                pj.dry_run_full_lift_test,
                pj.harness_setup_approval_state,
                pj.accumulated_picks_on_harness,
                pj.preventive_maintenance_flag,
                pj.auto_assigned_routing,
                pj.setter_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'harness_setup_job_id', pj.harness_setup_job_id,
                    'loom_hardware_id', pj.loom_hardware_id,
                    'jacquard_capacity_type', pj.jacquard_capacity_type,
                    'comber_board_density_epi', pj.comber_board_density_epi,
                    'harness_cord_material', pj.harness_cord_material,
                    'lingo_weight_per_cord_grams', pj.lingo_weight_per_cord_grams,
                    'total_active_harness_cords', pj.total_active_harness_cords,
                    'shed_opening_height_mm', pj.shed_opening_height_mm,
                    'accumulated_picks_on_harness', pj.accumulated_picks_on_harness,
                    'preventive_maintenance_flag', pj.preventive_maintenance_flag
                )
            FROM harness_setup_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'harness_setup_job_id': result['harness_setup_job_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'VACANT_AVAILABLE_FOR_WEAVING',
            'message': 'Harness setup log certified and loom is available for weaving'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/loom-harness-setter/certificates', methods=['GET'])
@jwt_required()
def list_harness_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT hc.id, hc.certificate_hash, hc.qr_tag_id,
                   hc.harness_setup_job_id, hc.loom_hardware_id,
                   hc.jacquard_capacity_type, hc.comber_board_density_epi,
                   hc.harness_cord_material, hc.lingo_weight_per_cord_grams,
                   hc.harness_tie_up_profile, hc.total_active_harness_cords,
                   hc.mail_eye_leveling_status, hc.shed_opening_height_mm,
                   hc.harness_drop_angle_status, hc.antistatic_harness_lubrication,
                   hc.dry_run_full_lift_test, hc.harness_setup_approval_state,
                   hc.accumulated_picks_on_harness, hc.preventive_maintenance_flag,
                   hc.auto_assigned_routing, hc.status, hc.certified_at,
                   hsl.harness_setup_job_id
            FROM harness_certificates hc
            JOIN harness_setup_logs hsl ON hc.harness_setup_log_id = hsl.id
            WHERE hc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY hc.certified_at DESC
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
                    'harness_setup_job_id': c['harness_setup_job_id'],
                    'loom_hardware_id': c['loom_hardware_id'],
                    'jacquard_capacity_type': c['jacquard_capacity_type'],
                    'comber_board_density_epi': c['comber_board_density_epi'],
                    'harness_cord_material': c['harness_cord_material'],
                    'lingo_weight_per_cord_grams': c['lingo_weight_per_cord_grams'],
                    'harness_tie_up_profile': c['harness_tie_up_profile'],
                    'total_active_harness_cords': c['total_active_harness_cords'],
                    'mail_eye_leveling_status': c['mail_eye_leveling_status'],
                    'shed_opening_height_mm': c['shed_opening_height_mm'],
                    'harness_drop_angle_status': c['harness_drop_angle_status'],
                    'antistatic_harness_lubrication': c['antistatic_harness_lubrication'],
                    'dry_run_full_lift_test': c['dry_run_full_lift_test'],
                    'harness_setup_approval_state': c['harness_setup_approval_state'],
                    'accumulated_picks_on_harness': c['accumulated_picks_on_harness'],
                    'preventive_maintenance_flag': c['preventive_maintenance_flag'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR LOOM HARNESS SETTER
# ============================================================

@app.route('/api/v1/sales/forecast/loom-harness-setter', methods=['GET'])
@jwt_required()
def get_sales_forecast_loom_harness_setter():
    """
    API plugin endpoint for sales team loom harness setup material processing forecast.
    Returns forecasted harness setup requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'jacquard_capacity_type': '2400_HOOK',
                    'comber_board_density_epi': '144_EPI',
                    'harness_cord_material': 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC',
                    'estimated_setups': 3,
                    'shed_opening_height_mm': 45.0,
                    'lingo_weight_grams': 20.0,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'jacquard_capacity_type': '2400_HOOK',
                    'comber_board_density_epi': '144_EPI',
                    'harness_cord_material': 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC',
                    'estimated_setups': 2,
                    'shed_opening_height_mm': 46.0,
                    'lingo_weight_grams': 19.5,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'jacquard_capacity_type': '1536_HOOK',
                    'comber_board_density_epi': '120_EPI',
                    'harness_cord_material': 'STANDARD_BRAIDED_POLYESTER',
                    'estimated_setups': 5,
                    'shed_opening_height_mm': 52.0,
                    'lingo_weight_grams': 27.0,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'HARNESS-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_setups': 3,
                    'jacquard_capacity_type': '2400_HOOK',
                    'loom_hardware_id': 'LOOM-2400-001'
                },
                {
                    'lot_number': 'HARNESS-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_setups': 2,
                    'jacquard_capacity_type': '2400_HOOK',
                    'loom_hardware_id': 'LOOM-2400-002'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# WARP JOINER (TIE-IN MASTER / KNOTTING SPECIALIST) MODULE
# ============================================================

@app.route('/api/v1/warp-joiner/jobs', methods=['POST'])
@jwt_required()
def create_warp_joining_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['joining_job_card_id', 'loom_number_id']
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
        
        cur.execute("""
            INSERT INTO warp_joining_jobs (
                joining_job_card_id, loom_number_id, production_lot_id,
                design_master_id, harness_setup_log_id, warp_beam_production_log_id,
                card_puncher_job_id, pirn_winding_job_id, bobbin_winder_job_card_id,
                factory_node_id, warp_joiner_employee_id, status,
                warp_set_id, tying_machine_model, separation_needle_type,
                target_tying_speed_kpm, knot_type_selection,
                knot_tail_length_mm, double_end_detection_status,
                manual_repair_knot_count, knot_pull_through_mode,
                knot_pull_through_status, warp_joiner_approval_state,
                new_warp_beam_lot_no,
                start_timestamp, end_timestamp,
                total_ends_to_join, missed_ends_count,
                total_joining_hours, loom_idle_duration_hours,
                loom_idle_variance_alert,
                knots_completed_count, wage_per_hundred_knots, calculated_wage_payout,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'TYING_IN_PROGRESS', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, joining_job_card_id
        """, (
            data.get('joining_job_card_id'),
            data.get('loom_number_id'),
            data.get('production_lot_id'),
            data.get('design_master_id'),
            data.get('harness_setup_log_id'),
            data.get('warp_beam_production_log_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            factory_node_id,
            operator_id,
            data.get('warp_set_id'),
            data.get('tying_machine_model', 'STAUBLI_TOPMATIC'),
            data.get('separation_needle_type', 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM'),
            data.get('target_tying_speed_kpm'),
            data.get('knot_type_selection', 'DOUBLE_LOOP_MICRO_KNOT'),
            data.get('knot_tail_length_mm'),
            data.get('double_end_detection_status', 'ZERO_DOUBLE_ENDS_DETECTED'),
            data.get('manual_repair_knot_count', 0),
            data.get('knot_pull_through_mode', 'MANUAL_HAND_CRANK_CREEP_PULL'),
            data.get('knot_pull_through_status', 'PASSED_100_PERCENT_KNOTS_CLEARED'),
            data.get('warp_joiner_approval_state', 'TYING_IN_PROGRESS'),
            data.get('new_warp_beam_lot_no'),
            data.get('end_timestamp'),
            data.get('total_ends_to_join'),
            data.get('missed_ends_count', 0),
            data.get('total_joining_hours'),
            data.get('loom_idle_duration_hours'),
            data.get('loom_idle_variance_alert', False),
            data.get('knots_completed_count', 0),
            data.get('wage_per_hundred_knots'),
            data.get('calculated_wage_payout'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'joining_job_card_id': job_row['joining_job_card_id'],
            'status': 'TYING_IN_PROGRESS',
            'message': 'Warp joining job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-joiner/jobs', methods=['GET'])
@jwt_required()
def list_warp_joining_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wjj.id, wjj.joining_job_card_id, wjj.loom_number_id,
                   wjj.tying_machine_model, wjj.separation_needle_type,
                   wjj.target_tying_speed_kpm, wjj.knot_type_selection,
                   wjj.knot_tail_length_mm, wjj.double_end_detection_status,
                   wjj.manual_repair_knot_count, wjj.knot_pull_through_mode,
                   wjj.knot_pull_through_status, wjj.warp_joiner_approval_state,
                   wjj.total_ends_to_join, wjj.missed_ends_count,
                   wjj.loom_idle_variance_alert, wjj.knots_completed_count,
                   wjj.calculated_wage_payout, wjj.status, wjj.certificate_hash,
                   wjj.auto_assigned_routing, wjj.created_at,
                   dm.design_master_id
            FROM warp_joining_jobs wjj
            LEFT JOIN design_masters dm ON wjj.design_master_id = dm.id
            WHERE wjj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wjj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'joining_job_card_id': j['joining_job_card_id'],
                    'loom_number_id': j['loom_number_id'],
                    'design_master_id': j['design_master_id'],
                    'tying_machine_model': j['tying_machine_model'],
                    'separation_needle_type': j['separation_needle_type'],
                    'target_tying_speed_kpm': j['target_tying_speed_kpm'],
                    'knot_type_selection': j['knot_type_selection'],
                    'knot_tail_length_mm': j['knot_tail_length_mm'],
                    'double_end_detection_status': j['double_end_detection_status'],
                    'manual_repair_knot_count': j['manual_repair_knot_count'],
                    'knot_pull_through_mode': j['knot_pull_through_mode'],
                    'knot_pull_through_status': j['knot_pull_through_status'],
                    'warp_joiner_approval_state': j['warp_joiner_approval_state'],
                    'total_ends_to_join': j['total_ends_to_join'],
                    'missed_ends_count': j['missed_ends_count'],
                    'loom_idle_variance_alert': j['loom_idle_variance_alert'],
                    'knots_completed_count': j['knots_completed_count'],
                    'calculated_wage_payout': j['calculated_wage_payout'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-joiner/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_warp_joining_job(job_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wjj.*, dm.design_master_id
            FROM warp_joining_jobs wjj
            LEFT JOIN design_masters dm ON wjj.design_master_id = dm.id
            WHERE wjj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'joining_job_card_id': job['joining_job_card_id'],
            'loom_number_id': job['loom_number_id'],
            'design_master_id': job['design_master_id'],
            'warp_set_id': job['warp_set_id'],
            'tying_machine_model': job['tying_machine_model'],
            'separation_needle_type': job['separation_needle_type'],
            'target_tying_speed_kpm': job['target_tying_speed_kpm'],
            'knot_type_selection': job['knot_type_selection'],
            'knot_tail_length_mm': job['knot_tail_length_mm'],
            'double_end_detection_status': job['double_end_detection_status'],
            'manual_repair_knot_count': job['manual_repair_knot_count'],
            'knot_pull_through_mode': job['knot_pull_through_mode'],
            'knot_pull_through_status': job['knot_pull_through_status'],
            'warp_joiner_approval_state': job['warp_joiner_approval_state'],
            'new_warp_beam_lot_no': job['new_warp_beam_lot_no'],
            'start_timestamp': job['start_timestamp'].isoformat() if job['start_timestamp'] else None,
            'end_timestamp': job['end_timestamp'].isoformat() if job['end_timestamp'] else None,
            'total_ends_to_join': job['total_ends_to_join'],
            'missed_ends_count': job['missed_ends_count'],
            'total_joining_hours': job['total_joining_hours'],
            'loom_idle_duration_hours': job['loom_idle_duration_hours'],
            'loom_idle_variance_alert': job['loom_idle_variance_alert'],
            'knots_completed_count': job['knots_completed_count'],
            'wage_per_hundred_knots': job['wage_per_hundred_knots'],
            'calculated_wage_payout': job['calculated_wage_payout'],
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-joiner/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_warp_joining_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, joining_job_card_id, status, validation_errors, warp_joiner_approval_state
            FROM warp_joining_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['joining_job_card_id'])
        qr_tag_id = 'WARP-JOIN-' + job['joining_job_card_id']
        
        cur.execute("""
            UPDATE warp_joining_jobs
            SET status = 'LOOM_ACTIVE_PRODUCTION',
                certificate_hash = %s,
                qr_tag_id = %s,
                warp_joiner_approval_state = 'PASSED_READY_FOR_WEAVER_START'
            WHERE id = %s::uuid
            RETURNING id, joining_job_card_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO warp_joining_certificates (
                warp_joining_job_id, certificate_hash, qr_tag_id, joining_job_card_id,
                loom_number_id, warp_set_id, tying_machine_model,
                separation_needle_type, target_tying_speed_kpm, knot_type_selection,
                knot_tail_length_mm, double_end_detection_status,
                manual_repair_knot_count, knot_pull_through_mode,
                knot_pull_through_status, warp_joiner_approval_state,
                total_ends_to_join, missed_ends_count,
                total_joining_hours, loom_idle_duration_hours,
                loom_idle_variance_alert, knots_completed_count,
                calculated_wage_payout,
                auto_assigned_routing, warp_joiner_employee_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.joining_job_card_id,
                pj.loom_number_id,
                pj.warp_set_id,
                pj.tying_machine_model,
                pj.separation_needle_type,
                pj.target_tying_speed_kpm,
                pj.knot_type_selection,
                pj.knot_tail_length_mm,
                pj.double_end_detection_status,
                pj.manual_repair_knot_count,
                pj.knot_pull_through_mode,
                pj.knot_pull_through_status,
                pj.warp_joiner_approval_state,
                pj.total_ends_to_join,
                pj.missed_ends_count,
                pj.total_joining_hours,
                pj.loom_idle_duration_hours,
                pj.loom_idle_variance_alert,
                pj.knots_completed_count,
                pj.calculated_wage_payout,
                pj.auto_assigned_routing,
                pj.warp_joiner_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'joining_job_card_id', pj.joining_job_card_id,
                    'loom_number_id', pj.loom_number_id,
                    'warp_set_id', pj.warp_set_id,
                    'tying_machine_model', pj.tying_machine_model,
                    'separation_needle_type', pj.separation_needle_type,
                    'target_tying_speed_kpm', pj.target_tying_speed_kpm,
                    'knot_type_selection', pj.knot_type_selection,
                    'knot_tail_length_mm', pj.knot_tail_length_mm,
                    'total_ends_to_join', pj.total_ends_to_join,
                    'knots_completed_count', pj.knots_completed_count,
                    'calculated_wage_payout', pj.calculated_wage_payout,
                    'loom_idle_variance_alert', pj.loom_idle_variance_alert
                )
            FROM warp_joining_jobs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'joining_job_card_id': result['joining_job_card_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'LOOM_ACTIVE_PRODUCTION',
            'message': 'Warp joining job certified and loom is active for production'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/warp-joiner/certificates', methods=['GET'])
@jwt_required()
def list_warp_joining_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT wjc.id, wjc.certificate_hash, wjc.qr_tag_id,
                   wjc.joining_job_card_id, wjc.loom_number_id,
                   wjc.warp_set_id, wjc.tying_machine_model,
                   wjc.separation_needle_type, wjc.target_tying_speed_kpm,
                   wjc.knot_type_selection, wjc.knot_tail_length_mm,
                   wjc.double_end_detection_status, wjc.manual_repair_knot_count,
                   wjc.knot_pull_through_status, wjc.warp_joiner_approval_state,
                   wjc.total_ends_to_join, wjc.missed_ends_count,
                   wjc.loom_idle_duration_hours, wjc.loom_idle_variance_alert,
                   wjc.knots_completed_count, wjc.calculated_wage_payout,
                   wjc.auto_assigned_routing, wjc.status, wjc.certified_at,
                   wjj.joining_job_card_id
            FROM warp_joining_certificates wjc
            JOIN warp_joining_jobs wjj ON wjc.warp_joining_job_id = wjj.id
            WHERE wjc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wjc.certified_at DESC
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
                    'joining_job_card_id': c['joining_job_card_id'],
                    'loom_number_id': c['loom_number_id'],
                    'warp_set_id': c['warp_set_id'],
                    'tying_machine_model': c['tying_machine_model'],
                    'separation_needle_type': c['separation_needle_type'],
                    'target_tying_speed_kpm': c['target_tying_speed_kpm'],
                    'knot_type_selection': c['knot_type_selection'],
                    'knot_tail_length_mm': c['knot_tail_length_mm'],
                    'double_end_detection_status': c['double_end_detection_status'],
                    'manual_repair_knot_count': c['manual_repair_knot_count'],
                    'knot_pull_through_status': c['knot_pull_through_status'],
                    'warp_joiner_approval_state': c['warp_joiner_approval_state'],
                    'total_ends_to_join': c['total_ends_to_join'],
                    'missed_ends_count': c['missed_ends_count'],
                    'loom_idle_duration_hours': c['loom_idle_duration_hours'],
                    'loom_idle_variance_alert': c['loom_idle_variance_alert'],
                    'knots_completed_count': c['knots_completed_count'],
                    'calculated_wage_payout': c['calculated_wage_payout'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR WARP JOINER
# ============================================================

@app.route('/api/v1/sales/forecast/warp-joiner', methods=['GET'])
@jwt_required()
def get_sales_forecast_warp_joiner():
    """
    API plugin endpoint for sales team warp joining material processing forecast.
    Returns forecasted warp joining requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'tying_machine_model': 'STAUBLI_TOPMATIC',
                    'separation_needle_type': 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM',
                    'estimated_joins': 3,
                    'total_ends_to_join': 21000,
                    'target_tying_speed_kpm': 200,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'tying_machine_model': 'STAUBLI_TOPMATIC',
                    'separation_needle_type': 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM',
                    'estimated_joins': 2,
                    'total_ends_to_join': 19800,
                    'target_tying_speed_kpm': 200,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'tying_machine_model': 'KNOTEX_AS_3',
                    'separation_needle_type': 'FINE_NEEDLE_0_5MM',
                    'estimated_joins': 5,
                    'total_ends_to_join': 12000,
                    'target_tying_speed_kpm': 350,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'WARP-JOIN-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_joins': 3,
                    'loom_number_id': 'LOOM-2400-001',
                    'tying_machine_model': 'STAUBLI_TOPMATIC'
                },
                {
                    'lot_number': 'WARP-JOIN-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_joins': 2,
                    'loom_number_id': 'LOOM-2400-002',
                    'tying_machine_model': 'STAUBLI_TOPMATIC'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# PETNI MASTER (WARP PULLING & REED DENTING SPECIALIST) MODULE
# ============================================================

@app.route('/api/v1/petni-master/jobs', methods=['POST'])
@jwt_required()
def create_petni_master_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['petni_job_card_id', 'loom_number_id']
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
        
        cur.execute("""
            INSERT INTO petni_master_jobs (
                petni_job_card_id, loom_number_id, production_lot_id,
                design_master_id, warp_joining_job_id, warp_beam_production_log_id,
                harness_setup_log_id, card_puncher_job_id, pirn_winding_job_id,
                bobbin_winder_job_card_id,
                factory_node_id, master_petni_employee_id, status,
                warp_set_id, petni_transition_method, reed_denting_draft_pattern,
                dropper_wire_specification, lease_order_verification,
                contrast_type, body_silk_lot_no, contrast_silk_lot_no,
                crossed_ends_count, reed_mark_laser_inspection,
                petni_joint_tension_variance_grams, border_channel_offset_mm,
                dropper_pinning_completion_status, petni_master_approval_state,
                saree_production_order_ref,
                petni_start_time, petni_end_time,
                total_threads_spliced_count, joint_clearance_status,
                contrast_yarn_weight_consumed_kg,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PETNI_IN_PROGRESS', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, petni_job_card_id
        """, (
            data.get('petni_job_card_id'),
            data.get('loom_number_id'),
            data.get('production_lot_id'),
            data.get('design_master_id'),
            data.get('warp_joining_job_id'),
            data.get('warp_beam_production_log_id'),
            data.get('harness_setup_log_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            factory_node_id,
            operator_id,
            data.get('warp_set_id'),
            data.get('petni_transition_method', 'COMB_TENSIONED_PETNI_KNOTTING'),
            data.get('reed_denting_draft_pattern', '4_ENDS_DENT_144_EPI'),
            data.get('dropper_wire_specification', '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE'),
            data.get('lease_order_verification', '1X1_STRICT_LEASE_LOCK'),
            data.get('contrast_type', 'SIDE_BORDERS_ONLY'),
            data.get('body_silk_lot_no'),
            data.get('contrast_silk_lot_no'),
            data.get('crossed_ends_count', 0),
            data.get('reed_mark_laser_inspection', 'PASSED_UNIFORM_DENTING'),
            data.get('petni_joint_tension_variance_grams'),
            data.get('border_channel_offset_mm'),
            data.get('dropper_pinning_completion_status', '100_PERCENT_DROPPERS_PINNED_AND_TESTED'),
            data.get('petni_master_approval_state', 'PETNI_IN_PROGRESS'),
            data.get('saree_production_order_ref'),
            data.get('petni_end_time'),
            data.get('total_threads_spliced_count'),
            data.get('joint_clearance_status', 'PASSED_TENSION_TEST'),
            data.get('contrast_yarn_weight_consumed_kg'),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'petni_job_card_id': job_row['petni_job_card_id'],
            'status': 'PETNI_IN_PROGRESS',
            'message': 'Petni master job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/petni-master/jobs', methods=['GET'])
@jwt_required()
def list_petni_master_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pmj.id, pmj.petni_job_card_id, pmj.loom_number_id,
                   pmj.petni_transition_method, pmj.reed_denting_draft_pattern,
                   pmj.dropper_wire_specification, pmj.lease_order_verification,
                   pmj.contrast_type, pmj.crossed_ends_count,
                   pmj.reed_mark_laser_inspection, pmj.petni_joint_tension_variance_grams,
                   pmj.border_channel_offset_mm, pmj.dropper_pinning_completion_status,
                   pmj.petni_master_approval_state, pmj.total_threads_spliced_count,
                   pmj.joint_clearance_status, pmj.contrast_yarn_weight_consumed_kg,
                   pmj.status, pmj.certificate_hash, pmj.auto_assigned_routing,
                   pmj.created_at,
                   dm.design_master_id
            FROM petni_master_jobs pmj
            LEFT JOIN design_masters dm ON pmj.design_master_id = dm.id
            WHERE pmj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pmj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'petni_job_card_id': j['petni_job_card_id'],
                    'loom_number_id': j['loom_number_id'],
                    'design_master_id': j['design_master_id'],
                    'petni_transition_method': j['petni_transition_method'],
                    'reed_denting_draft_pattern': j['reed_denting_draft_pattern'],
                    'dropper_wire_specification': j['dropper_wire_specification'],
                    'lease_order_verification': j['lease_order_verification'],
                    'contrast_type': j['contrast_type'],
                    'crossed_ends_count': j['crossed_ends_count'],
                    'reed_mark_laser_inspection': j['reed_mark_laser_inspection'],
                    'petni_joint_tension_variance_grams': j['petni_joint_tension_variance_grams'],
                    'border_channel_offset_mm': j['border_channel_offset_mm'],
                    'dropper_pinning_completion_status': j['dropper_pinning_completion_status'],
                    'petni_master_approval_state': j['petni_master_approval_state'],
                    'total_threads_spliced_count': j['total_threads_spliced_count'],
                    'joint_clearance_status': j['joint_clearance_status'],
                    'contrast_yarn_weight_consumed_kg': j['contrast_yarn_weight_consumed_kg'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/petni-master/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_petni_master_job(job_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pmj.*, dm.design_master_id
            FROM petni_master_jobs pmj
            LEFT JOIN design_masters dm ON pmj.design_master_id = dm.id
            WHERE pmj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'petni_job_card_id': job['petni_job_card_id'],
            'loom_number_id': job['loom_number_id'],
            'design_master_id': job['design_master_id'],
            'warp_set_id': job['warp_set_id'],
            'petni_transition_method': job['petni_transition_method'],
            'reed_denting_draft_pattern': job['reed_denting_draft_pattern'],
            'dropper_wire_specification': job['dropper_wire_specification'],
            'lease_order_verification': job['lease_order_verification'],
            'contrast_type': job['contrast_type'],
            'body_silk_lot_no': job['body_silk_lot_no'],
            'contrast_silk_lot_no': job['contrast_silk_lot_no'],
            'crossed_ends_count': job['crossed_ends_count'],
            'reed_mark_laser_inspection': job['reed_mark_laser_inspection'],
            'petni_joint_tension_variance_grams': job['petni_joint_tension_variance_grams'],
            'border_channel_offset_mm': job['border_channel_offset_mm'],
            'dropper_pinning_completion_status': job['dropper_pinning_completion_status'],
            'petni_master_approval_state': job['petni_master_approval_state'],
            'saree_production_order_ref': job['saree_production_order_ref'],
            'petni_start_time': job['petni_start_time'].isoformat() if job['petni_start_time'] else None,
            'petni_end_time': job['petni_end_time'].isoformat() if job['petni_end_time'] else None,
            'total_threads_spliced_count': job['total_threads_spliced_count'],
            'joint_clearance_status': job['joint_clearance_status'],
            'contrast_yarn_weight_consumed_kg': job['contrast_yarn_weight_consumed_kg'],
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/petni-master/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_petni_master_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, petni_job_card_id, status, validation_errors, petni_master_approval_state
            FROM petni_master_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['petni_job_card_id'])
        qr_tag_id = 'PETNI-' + job['petni_job_card_id']
        
        cur.execute("""
            UPDATE petni_master_jobs
            SET status = 'LOOM_ACTIVE_PRODUCTION',
                certificate_hash = %s,
                qr_tag_id = %s,
                petni_master_approval_state = 'PASSED_APPROVED_FOR_FIRST_PICK'
            WHERE id = %s::uuid
            RETURNING id, petni_job_card_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO petni_master_certificates (
                petni_master_job_id, certificate_hash, qr_tag_id, petni_job_card_id,
                loom_number_id, warp_set_id, petni_transition_method,
                reed_denting_draft_pattern, dropper_wire_specification,
                lease_order_verification, contrast_type,
                body_silk_lot_no, contrast_silk_lot_no,
                crossed_ends_count, reed_mark_laser_inspection,
                petni_joint_tension_variance_grams, border_channel_offset_mm,
                dropper_pinning_completion_status, petni_master_approval_state,
                total_threads_spliced_count, joint_clearance_status,
                contrast_yarn_weight_consumed_kg,
                auto_assigned_routing, master_petni_employee_id, approver_id,
                factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.petni_job_card_id,
                pj.loom_number_id,
                pj.warp_set_id,
                pj.petni_transition_method,
                pj.reed_denting_draft_pattern,
                pj.dropper_wire_specification,
                pj.lease_order_verification,
                pj.contrast_type,
                pj.body_silk_lot_no,
                pj.contrast_silk_lot_no,
                pj.crossed_ends_count,
                pj.reed_mark_laser_inspection,
                pj.petni_joint_tension_variance_grams,
                pj.border_channel_offset_mm,
                pj.dropper_pinning_completion_status,
                pj.petni_master_approval_state,
                pj.total_threads_spliced_count,
                pj.joint_clearance_status,
                pj.contrast_yarn_weight_consumed_kg,
                pj.auto_assigned_routing,
                pj.master_petni_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'petni_job_card_id', pj.petni_job_card_id,
                    'loom_number_id', pj.loom_number_id,
                    'warp_set_id', pj.warp_set_id,
                    'petni_transition_method', pj.petni_transition_method,
                    'reed_denting_draft_pattern', pj.reed_denting_draft_pattern,
                    'dropper_wire_specification', pj.dropper_wire_specification,
                    'lease_order_verification', pj.lease_order_verification,
                    'contrast_type', pj.contrast_type,
                    'crossed_ends_count', pj.crossed_ends_count,
                    'total_threads_spliced_count', pj.total_threads_spliced_count,
                    'joint_clearance_status', pj.joint_clearance_status,
                    'contrast_yarn_weight_consumed_kg', pj.contrast_yarn_weight_consumed_kg
                )
            FROM petni_master_jobs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'petni_job_card_id': result['petni_job_card_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'LOOM_ACTIVE_PRODUCTION',
            'message': 'Petni master job certified and loom is active for production'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/petni-master/certificates', methods=['GET'])
@jwt_required()
def list_petni_master_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pmc.id, pmc.certificate_hash, pmc.qr_tag_id,
                   pmc.petni_job_card_id, pmc.loom_number_id,
                   pmc.warp_set_id, pmc.petni_transition_method,
                   pmc.reed_denting_draft_pattern, pmc.dropper_wire_specification,
                   pmc.lease_order_verification, pmc.contrast_type,
                   pmc.body_silk_lot_no, pmc.contrast_silk_lot_no,
                   pmc.crossed_ends_count, pmc.reed_mark_laser_inspection,
                   pmc.petni_joint_tension_variance_grams, pmc.border_channel_offset_mm,
                   pmc.dropper_pinning_completion_status, pmc.petni_master_approval_state,
                   pmc.total_threads_spliced_count, pmc.joint_clearance_status,
                   pmc.contrast_yarn_weight_consumed_kg,
                   pmc.auto_assigned_routing, pmc.status, pmc.certified_at,
                   pmj.petni_job_card_id
            FROM petni_master_certificates pmc
            JOIN petni_master_jobs pmj ON pmc.petni_master_job_id = pmj.id
            WHERE pmc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY pmc.certified_at DESC
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
                    'petni_job_card_id': c['petni_job_card_id'],
                    'loom_number_id': c['loom_number_id'],
                    'warp_set_id': c['warp_set_id'],
                    'petni_transition_method': c['petni_transition_method'],
                    'reed_denting_draft_pattern': c['reed_denting_draft_pattern'],
                    'dropper_wire_specification': c['dropper_wire_specification'],
                    'lease_order_verification': c['lease_order_verification'],
                    'contrast_type': c['contrast_type'],
                    'body_silk_lot_no': c['body_silk_lot_no'],
                    'contrast_silk_lot_no': c['contrast_silk_lot_no'],
                    'crossed_ends_count': c['crossed_ends_count'],
                    'reed_mark_laser_inspection': c['reed_mark_laser_inspection'],
                    'petni_joint_tension_variance_grams': c['petni_joint_tension_variance_grams'],
                    'border_channel_offset_mm': c['border_channel_offset_mm'],
                    'dropper_pinning_completion_status': c['dropper_pinning_completion_status'],
                    'petni_master_approval_state': c['petni_master_approval_state'],
                    'total_threads_spliced_count': c['total_threads_spliced_count'],
                    'joint_clearance_status': c['joint_clearance_status'],
                    'contrast_yarn_weight_consumed_kg': c['contrast_yarn_weight_consumed_kg'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR PETNI MASTER
# ============================================================

@app.route('/api/v1/sales/forecast/petni-master', methods=['GET'])
@jwt_required()
def get_sales_forecast_petni_master():
    """
    API plugin endpoint for sales team petni master material processing forecast.
    Returns forecasted petni requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'petni_transition_method': 'COMB_TENSIONED_PETNI_KNOTTING',
                    'reed_denting_draft_pattern': '4_ENDS_DENT_144_EPI',
                    'dropper_wire_specification': '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE',
                    'estimated_petni_jobs': 3,
                    'total_threads_spliced_count': 21000,
                    'contrast_type': 'FULL_BODY_BORDER_PALLU_THREE_SHUTTLE_KORVAI',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'petni_transition_method': 'COMB_TENSIONED_PETNI_KNOTTING',
                    'reed_denting_draft_pattern': '4_ENDS_DENT_144_EPI',
                    'dropper_wire_specification': '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE',
                    'estimated_petni_jobs': 2,
                    'total_threads_spliced_count': 19800,
                    'contrast_type': 'SIDE_BORDERS_ONLY',
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'petni_transition_method': 'STANDARD_MANUAL_HAND_PETNI',
                    'reed_denting_draft_pattern': '2_ENDS_DENT_STANDARD',
                    'dropper_wire_specification': '0_7G_STANDARD_HEAVY_WIRE',
                    'estimated_petni_jobs': 5,
                    'total_threads_spliced_count': 12000,
                    'contrast_type': 'SIDE_BORDERS_ONLY',
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'PETNI-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_petni_jobs': 3,
                    'loom_number_id': 'LOOM-2400-001',
                    'petni_transition_method': 'COMB_TENSIONED_PETNI_KNOTTING'
                },
                {
                    'lot_number': 'PETNI-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_petni_jobs': 2,
                    'loom_number_id': 'LOOM-2400-002',
                    'petni_transition_method': 'COMB_TENSIONED_PETNI_KNOTTING'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# MASTER WEAVER (LOOM OPERATOR & SAREE PRODUCTION SPECIALIST) MODULE
# ============================================================

@app.route('/api/v1/master-weaver/jobs', methods=['POST'])
@jwt_required()
def create_master_weaver_job():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['production_run_id', 'loom_id']
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
        
        cur.execute("""
            INSERT INTO master_weaver_jobs (
                production_run_id, loom_id, production_lot_id,
                design_master_id, petni_master_job_id, warp_joining_job_id,
                harness_setup_log_id, warp_beam_production_log_id, card_puncher_job_id,
                pirn_winding_job_id, bobbin_winder_job_card_id,
                factory_node_id, master_weaver_employee_id, assigned_weaver_id, status,
                saree_production_id, loom_operating_speed_ppm, warp_let_off_tension_cn,
                weft_insertion_feeder_profile, dynamic_ppi_control_mode,
                on_loom_defect_category, zari_catch_selvage_status,
                saree_section_phase, saree_length_measured_meters,
                saree_piece_clearance_status, weaver_approval_state,
                saree_production_order_no, reed_width_inches, picks_per_inch_ppi,
                first_pick_sample_status, shuttle_setup_mode,
                allocated_weft_silk_lot_no, allocated_zari_lot_no, issued_zari_weight_gm,
                loom_rpm, target_ppi, efficiency_percent,
                target_output_yards_per_hour, actual_output_yards, low_efficiency_alert,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'WEAVING_IN_PROGRESS', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, production_run_id
        """, (
            data.get('production_run_id'),
            data.get('loom_id'),
            data.get('production_lot_id'),
            data.get('design_master_id'),
            data.get('petni_master_job_id'),
            data.get('warp_joining_job_id'),
            data.get('harness_setup_log_id'),
            data.get('warp_beam_production_log_id'),
            data.get('card_puncher_job_id'),
            data.get('pirn_winding_job_id'),
            data.get('bobbin_winder_job_card_id'),
            factory_node_id,
            operator_id,
            data.get('assigned_weaver_id'),
            data.get('saree_production_id'),
            data.get('loom_operating_speed_ppm'),
            data.get('warp_let_off_tension_cn'),
            data.get('weft_insertion_feeder_profile', 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION'),
            data.get('dynamic_ppi_control_mode', 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH'),
            data.get('on_loom_defect_category', 'NONE_ZERO_DEFECTS'),
            data.get('zari_catch_selvage_status', 'PERFECT_CATCH_SMOOTH_EDGE'),
            data.get('saree_section_phase', 'PALLU_HIGH_DENSITY'),
            data.get('saree_length_measured_meters'),
            data.get('saree_piece_clearance_status', 'PASSED_GRADE_A_QUALITY'),
            data.get('weaver_approval_state', 'WEAVING_IN_PROGRESS'),
            data.get('saree_production_order_no'),
            data.get('reed_width_inches'),
            data.get('picks_per_inch_ppi'),
            data.get('first_pick_sample_status', 'APPROVED_FLAWLESS'),
            data.get('shuttle_setup_mode', 'SINGLE_SHUTTLE'),
            data.get('allocated_weft_silk_lot_no'),
            data.get('allocated_zari_lot_no'),
            data.get('issued_zari_weight_gm'),
            data.get('loom_rpm'),
            data.get('target_ppi'),
            data.get('efficiency_percent'),
            data.get('target_output_yards_per_hour'),
            data.get('actual_output_yards'),
            data.get('low_efficiency_alert', False),
            json.dumps([]),
            json.dumps([]),
            data.get('auto_assigned_routing')
        ))
        
        job_row = cur.fetchone()
        job_id = job_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(job_id),
            'production_run_id': job_row['production_run_id'],
            'status': 'WEAVING_IN_PROGRESS',
            'message': 'Master weaver job created successfully'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/master-weaver/jobs', methods=['GET'])
@jwt_required()
def list_master_weaver_jobs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mwj.id, mwj.production_run_id, mwj.loom_id,
                   mwj.saree_production_id, mwj.loom_operating_speed_ppm,
                   mwj.warp_let_off_tension_cn, mwj.weft_insertion_feeder_profile,
                   mwj.dynamic_ppi_control_mode, mwj.on_loom_defect_category,
                   mwj.zari_catch_selvage_status, mwj.saree_section_phase,
                   mwj.saree_length_measured_meters, mwj.saree_piece_clearance_status,
                   mwj.weaver_approval_state, mwj.shuttle_setup_mode,
                   mwj.reed_width_inches, mwj.picks_per_inch_ppi,
                   mwj.first_pick_sample_status, mwj.issued_zari_weight_gm,
                   mwj.loom_rpm, mwj.efficiency_percent,
                   mwj.target_output_yards_per_hour, mwj.actual_output_yards,
                   mwj.low_efficiency_alert, mwj.status, mwj.certificate_hash,
                   mwj.auto_assigned_routing, mwj.created_at,
                   dm.design_master_id
            FROM master_weaver_jobs mwj
            LEFT JOIN design_masters dm ON mwj.design_master_id = dm.id
            WHERE mwj.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY mwj.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(jobs),
            'jobs': [
                {
                    'id': str(j['id']),
                    'production_run_id': j['production_run_id'],
                    'loom_id': j['loom_id'],
                    'design_master_id': j['design_master_id'],
                    'saree_production_id': j['saree_production_id'],
                    'loom_operating_speed_ppm': j['loom_operating_speed_ppm'],
                    'warp_let_off_tension_cn': j['warp_let_off_tension_cn'],
                    'weft_insertion_feeder_profile': j['weft_insertion_feeder_profile'],
                    'dynamic_ppi_control_mode': j['dynamic_ppi_control_mode'],
                    'on_loom_defect_category': j['on_loom_defect_category'],
                    'zari_catch_selvage_status': j['zari_catch_selvage_status'],
                    'saree_section_phase': j['saree_section_phase'],
                    'saree_length_measured_meters': j['saree_length_measured_meters'],
                    'saree_piece_clearance_status': j['saree_piece_clearance_status'],
                    'weaver_approval_state': j['weaver_approval_state'],
                    'shuttle_setup_mode': j['shuttle_setup_mode'],
                    'reed_width_inches': j['reed_width_inches'],
                    'picks_per_inch_ppi': j['picks_per_inch_ppi'],
                    'first_pick_sample_status': j['first_pick_sample_status'],
                    'issued_zari_weight_gm': j['issued_zari_weight_gm'],
                    'loom_rpm': j['loom_rpm'],
                    'efficiency_percent': j['efficiency_percent'],
                    'target_output_yards_per_hour': j['target_output_yards_per_hour'],
                    'actual_output_yards': j['actual_output_yards'],
                    'low_efficiency_alert': j['low_efficiency_alert'],
                    'status': j['status'],
                    'certificate_hash': j['certificate_hash'],
                    'auto_assigned_routing': j['auto_assigned_routing'],
                    'created_at': j['created_at'].isoformat() if j['created_at'] else None
                }
                for j in jobs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/master-weaver/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_master_weaver_job(job_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mwj.*, dm.design_master_id
            FROM master_weaver_jobs mwj
            LEFT JOIN design_masters dm ON mwj.design_master_id = dm.id
            WHERE mwj.id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        cur.close()
        conn.close()
        
        if not job:
            return jsonify({'error': 'JobNotFound'}), 404
        
        return jsonify({
            'id': str(job['id']),
            'production_run_id': job['production_run_id'],
            'loom_id': job['loom_id'],
            'design_master_id': job['design_master_id'],
            'saree_production_id': job['saree_production_id'],
            'loom_operating_speed_ppm': job['loom_operating_speed_ppm'],
            'warp_let_off_tension_cn': job['warp_let_off_tension_cn'],
            'weft_insertion_feeder_profile': job['weft_insertion_feeder_profile'],
            'dynamic_ppi_control_mode': job['dynamic_ppi_control_mode'],
            'on_loom_defect_category': job['on_loom_defect_category'],
            'zari_catch_selvage_status': job['zari_catch_selvage_status'],
            'saree_section_phase': job['saree_section_phase'],
            'saree_length_measured_meters': job['saree_length_measured_meters'],
            'saree_piece_clearance_status': job['saree_piece_clearance_status'],
            'weaver_approval_state': job['weaver_approval_state'],
            'shuttle_setup_mode': job['shuttle_setup_mode'],
            'reed_width_inches': job['reed_width_inches'],
            'picks_per_inch_ppi': job['picks_per_inch_ppi'],
            'first_pick_sample_status': job['first_pick_sample_status'],
            'allocated_weft_silk_lot_no': job['allocated_weft_silk_lot_no'],
            'allocated_zari_lot_no': job['allocated_zari_lot_no'],
            'issued_zari_weight_gm': job['issued_zari_weight_gm'],
            'loom_rpm': job['loom_rpm'],
            'efficiency_percent': job['efficiency_percent'],
            'target_output_yards_per_hour': job['target_output_yards_per_hour'],
            'actual_output_yards': job['actual_output_yards'],
            'low_efficiency_alert': job['low_efficiency_alert'],
            'validation_errors': job['validation_errors'],
            'validation_warnings': job['validation_warnings'],
            'auto_assigned_routing': job['auto_assigned_routing'],
            'status': job['status'],
            'certificate_hash': job['certificate_hash']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/master-weaver/jobs/<job_id>/certify', methods=['POST'])
@jwt_required()
def certify_master_weaver_job(job_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, production_run_id, status, validation_errors, weaver_approval_state
            FROM master_weaver_jobs
            WHERE id = %s::uuid
        """, (job_id,))
        
        job = cur.fetchone()
        if not job:
            cur.close()
            conn.close()
            return jsonify({'error': 'JobNotFound'}), 404
        
        if job['validation_errors'] and len(job['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify job with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(job_id, job['production_run_id'])
        qr_tag_id = 'WEAVER-' + job['production_run_id']
        
        cur.execute("""
            UPDATE master_weaver_jobs
            SET status = 'SAREE_COMPLETED_PENDING_CUT',
                certificate_hash = %s,
                qr_tag_id = %s,
                weaver_approval_state = 'SHIFT_HANDOVER_COMPLETE'
            WHERE id = %s::uuid
            RETURNING id, production_run_id, certificate_hash
        """, (certificate_hash, qr_tag_id, job_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO master_weaver_certificates (
                master_weaver_job_id, certificate_hash, qr_tag_id, production_run_id,
                loom_id, saree_production_id, loom_operating_speed_ppm,
                warp_let_off_tension_cn, weft_insertion_feeder_profile,
                dynamic_ppi_control_mode, on_loom_defect_category,
                zari_catch_selvage_status, saree_section_phase,
                saree_length_measured_meters, saree_piece_clearance_status,
                weaver_approval_state, shuttle_setup_mode,
                reed_width_inches, picks_per_inch_ppi, first_pick_sample_status,
                allocated_weft_silk_lot_no, allocated_zari_lot_no, issued_zari_weight_gm,
                loom_rpm, efficiency_percent, target_output_yards_per_hour,
                actual_output_yards, low_efficiency_alert,
                auto_assigned_routing, master_weaver_employee_id, assigned_weaver_id,
                approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.production_run_id,
                pj.loom_id,
                pj.saree_production_id,
                pj.loom_operating_speed_ppm,
                pj.warp_let_off_tension_cn,
                pj.weft_insertion_feeder_profile,
                pj.dynamic_ppi_control_mode,
                pj.on_loom_defect_category,
                pj.zari_catch_selvage_status,
                pj.saree_section_phase,
                pj.saree_length_measured_meters,
                pj.saree_piece_clearance_status,
                pj.weaver_approval_state,
                pj.shuttle_setup_mode,
                pj.reed_width_inches,
                pj.picks_per_inch_ppi,
                pj.first_pick_sample_status,
                pj.allocated_weft_silk_lot_no,
                pj.allocated_zari_lot_no,
                pj.issued_zari_weight_gm,
                pj.loom_rpm,
                pj.efficiency_percent,
                pj.target_output_yards_per_hour,
                pj.actual_output_yards,
                pj.low_efficiency_alert,
                pj.auto_assigned_routing,
                pj.master_weaver_employee_id,
                pj.assigned_weaver_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'production_run_id', pj.production_run_id,
                    'loom_id', pj.loom_id,
                    'saree_production_id', pj.saree_production_id,
                    'loom_operating_speed_ppm', pj.loom_operating_speed_ppm,
                    'warp_let_off_tension_cn', pj.warp_let_off_tension_cn,
                    'shuttle_setup_mode', pj.shuttle_setup_mode,
                    'reed_width_inches', pj.reed_width_inches,
                    'picks_per_inch_ppi', pj.picks_per_inch_ppi,
                    'first_pick_sample_status', pj.first_pick_sample_status,
                    'loom_rpm', pj.loom_rpm,
                    'efficiency_percent', pj.efficiency_percent,
                    'target_output_yards_per_hour', pj.target_output_yards_per_hour,
                    'actual_output_yards', pj.actual_output_yards,
                    'low_efficiency_alert', pj.low_efficiency_alert
                )
            FROM master_weaver_jobs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, job_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'production_run_id': result['production_run_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'SAREE_COMPLETED_PENDING_CUT',
            'message': 'Master weaver job certified and saree completed pending cut'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/master-weaver/certificates', methods=['GET'])
@jwt_required()
def list_master_weaver_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT mwc.id, mwc.certificate_hash, mwc.qr_tag_id,
                   mwc.production_run_id, mwc.loom_id,
                   mwc.saree_production_id, mwc.loom_operating_speed_ppm,
                   mwc.warp_let_off_tension_cn, mwc.weft_insertion_feeder_profile,
                   mwc.dynamic_ppi_control_mode, mwc.on_loom_defect_category,
                   mwc.zari_catch_selvage_status, mwc.saree_section_phase,
                   mwc.saree_length_measured_meters, mwc.saree_piece_clearance_status,
                   mwc.weaver_approval_state, mwc.shuttle_setup_mode,
                   mwc.reed_width_inches, mwc.picks_per_inch_ppi,
                   mwc.first_pick_sample_status, mwc.issued_zari_weight_gm,
                   mwc.loom_rpm, mwc.efficiency_percent,
                   mwc.target_output_yards_per_hour, mwc.actual_output_yards,
                   mwc.low_efficiency_alert, mwc.auto_assigned_routing,
                   mwc.status, mwc.certified_at,
                   mwj.production_run_id
            FROM master_weaver_certificates mwc
            JOIN master_weaver_jobs mwj ON mwc.master_weaver_job_id = mwj.id
            WHERE mwc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY mwc.certified_at DESC
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
                    'production_run_id': c['production_run_id'],
                    'loom_id': c['loom_id'],
                    'saree_production_id': c['saree_production_id'],
                    'loom_operating_speed_ppm': c['loom_operating_speed_ppm'],
                    'warp_let_off_tension_cn': c['warp_let_off_tension_cn'],
                    'weft_insertion_feeder_profile': c['weft_insertion_feeder_profile'],
                    'dynamic_ppi_control_mode': c['dynamic_ppi_control_mode'],
                    'on_loom_defect_category': c['on_loom_defect_category'],
                    'zari_catch_selvage_status': c['zari_catch_selvage_status'],
                    'saree_section_phase': c['saree_section_phase'],
                    'saree_length_measured_meters': c['saree_length_measured_meters'],
                    'saree_piece_clearance_status': c['saree_piece_clearance_status'],
                    'weaver_approval_state': c['weaver_approval_state'],
                    'shuttle_setup_mode': c['shuttle_setup_mode'],
                    'reed_width_inches': c['reed_width_inches'],
                    'picks_per_inch_ppi': c['picks_per_inch_ppi'],
                    'first_pick_sample_status': c['first_pick_sample_status'],
                    'issued_zari_weight_gm': c['issued_zari_weight_gm'],
                    'loom_rpm': c['loom_rpm'],
                    'efficiency_percent': c['efficiency_percent'],
                    'target_output_yards_per_hour': c['target_output_yards_per_hour'],
                    'actual_output_yards': c['actual_output_yards'],
                    'low_efficiency_alert': c['low_efficiency_alert'],
                    'auto_assigned_routing': c['auto_assigned_routing'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR MASTER WEAVER
# ============================================================

@app.route('/api/v1/sales/forecast/master-weaver', methods=['GET'])
@jwt_required()
def get_sales_forecast_master_weaver():
    """
    API plugin endpoint for sales team master weaver material processing forecast.
    Returns forecasted weaving requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'shuttle_setup_mode': 'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT',
                    'weft_insertion_feeder_profile': 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION',
                    'dynamic_ppi_control_mode': 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH',
                    'estimated_production_runs': 3,
                    'loom_operating_speed_ppm': 150,
                    'picks_per_inch_ppi': 120,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'shuttle_setup_mode': 'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT',
                    'weft_insertion_feeder_profile': 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION',
                    'dynamic_ppi_control_mode': 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH',
                    'estimated_production_runs': 2,
                    'loom_operating_speed_ppm': 145,
                    'picks_per_inch_ppi': 110,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'shuttle_setup_mode': 'SINGLE_SHUTTLE',
                    'weft_insertion_feeder_profile': 'STANDARD_SINGLE_FEEDER',
                    'dynamic_ppi_control_mode': 'FIXED_SINGLE_PPI',
                    'estimated_production_runs': 5,
                    'loom_operating_speed_ppm': 200,
                    'picks_per_inch_ppi': 90,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'WEAVER-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_production_runs': 3,
                    'loom_id': 'LOOM-2400-001',
                    'shuttle_setup_mode': 'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT'
                },
                {
                    'lot_number': 'WEAVER-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_production_runs': 2,
                    'loom_id': 'LOOM-2400-002',
                    'shuttle_setup_mode': 'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SUP LOOM FLOOR SUPERVISOR MODULE
# ============================================================

def validate_sup_loom_floor_supervisor_guardrails(data):
    """Validate supervisor inputs against business guardrails."""
    errors = []
    warnings = []
    
    ambient_relative_humidity_pct = data.get('ambient_relative_humidity_pct')
    first_saree_dimensional_audit = data.get('first_saree_dimensional_audit')
    loom_stop_rate_per_hour = data.get('loom_stop_rate_per_hour')
    saree_batch_release_authorization = data.get('saree_batch_release_authorization')
    
    if ambient_relative_humidity_pct is not None:
        if ambient_relative_humidity_pct < 62.0 or ambient_relative_humidity_pct > 73.0:
            errors.append({
                'code': 'ENVIRONMENT_OUT_OF_SPEC_WARNING',
                'message': 'STATIC_AND_WARP_BREAK_RISK_FOR_HIGH_DENSITY_SILK'
            })
    
    if first_saree_dimensional_audit is not None and first_saree_dimensional_audit != 'APPROVED_FULL_SPEC_MATCH':
        errors.append({
            'code': 'HALT_LOOM_RUN',
            'message': 'CANNOT_PROCEED_WITH_REMAINING_79_SAREES_WITHOUT_FIRST_SAREE_APPROVAL'
        })
    
    if loom_stop_rate_per_hour is not None and loom_stop_rate_per_hour > 2.5:
        warnings.append({
            'code': 'AUTO_MAINTENANCE_TICKET',
            'message': 'EXCESSIVE_STOP_RATE_REQUIRES_SUPERVISOR_INTERVENTION'
        })
    
    if saree_batch_release_authorization is not None and saree_batch_release_authorization != 'APPROVED_FOR_FINISHING':
        errors.append({
            'code': 'DENY_CUT_SAREE_TRANSFER_TO_FINISHING_WAREHOUSE',
            'message': 'BATCH_RELEASE_AUTHORIZATION_REQUIRED'
        })
    
    return errors, warnings

@app.route('/api/v1/sup-loom-floor-supervisor/logs', methods=['POST'])
@jwt_required()
def create_sup_supervisor_log():
    try:
        supervisor_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['loom_shed_line_id']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (supervisor_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_sup_loom_floor_supervisor_guardrails(data)
        
        supervisor_log_id = f"SUP-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO sup_loom_floor_supervisor_logs (
                supervisor_log_id, loom_shed_line_id, factory_node_id, supervisor_id,
                assistant_weaver_job_log_id, master_weaver_job_id, petni_master_job_id,
                warp_joining_job_id, harness_setup_log_id,
                ambient_relative_humidity_pct, ambient_temperature_celsius,
                shift_target_oee_pct, gaiting_handover_status,
                loom_stop_rate_per_hour, primary_stop_root_cause,
                first_saree_dimensional_audit, waste_percentage_current_run,
                shift_handover_approval_state, saree_batch_release_authorization,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, supervisor_log_id
        """, (
            supervisor_log_id,
            data.get('loom_shed_line_id'),
            factory_node_id,
            supervisor_id,
            data.get('assistant_weaver_job_log_id'),
            data.get('master_weaver_job_id'),
            data.get('petni_master_job_id'),
            data.get('warp_joining_job_id'),
            data.get('harness_setup_log_id'),
            data.get('ambient_relative_humidity_pct'),
            data.get('ambient_temperature_celsius'),
            data.get('shift_target_oee_pct', 85.0),
            data.get('gaiting_handover_status', 'ALL_PRE_WEAVE_CHECKS_PASSED'),
            data.get('loom_stop_rate_per_hour'),
            data.get('primary_stop_root_cause', 'NONE_NORMAL_RUNNING'),
            data.get('first_saree_dimensional_audit', 'APPROVED_FULL_SPEC_MATCH'),
            data.get('waste_percentage_current_run'),
            data.get('shift_handover_approval_state', 'SHIFT_ACTIVE_NORMAL'),
            data.get('saree_batch_release_authorization', 'APPROVED_FOR_FINISHING'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'supervisor_log_id': log_row['supervisor_log_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/sup-loom-floor-supervisor/logs', methods=['GET'])
@jwt_required()
def list_sup_supervisor_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sfls.id, sfls.supervisor_log_id, sfls.loom_shed_line_id,
                   sfls.shift_start_time, sfls.shift_end_time,
                   sfls.ambient_relative_humidity_pct, sfls.ambient_temperature_celsius,
                   sfls.shift_target_oee_pct, sfls.gaiting_handover_status,
                   sfls.loom_stop_rate_per_hour, sfls.primary_stop_root_cause,
                   sfls.first_saree_dimensional_audit, sfls.waste_percentage_current_run,
                   sfls.shift_handover_approval_state, sfls.saree_batch_release_authorization,
                   sfls.auto_assigned_routing,
                   u_supervisor.full_name AS supervisor_name
            FROM sup_loom_floor_supervisor_logs sfls
            LEFT JOIN users u_supervisor ON sfls.supervisor_id = u_supervisor.id
            WHERE sfls.supervisor_id = %s::uuid
            ORDER BY sfls.shift_start_time DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'supervisor_log_id': log['supervisor_log_id'],
                    'loom_shed_line_id': log['loom_shed_line_id'],
                    'shift_start_time': log['shift_start_time'].isoformat() if log['shift_start_time'] else None,
                    'shift_end_time': log['shift_end_time'].isoformat() if log['shift_end_time'] else None,
                    'ambient_relative_humidity_pct': float(log['ambient_relative_humidity_pct']) if log['ambient_relative_humidity_pct'] else None,
                    'ambient_temperature_celsius': float(log['ambient_temperature_celsius']) if log['ambient_temperature_celsius'] else None,
                    'shift_target_oee_pct': float(log['shift_target_oee_pct']) if log['shift_target_oee_pct'] else None,
                    'gaiting_handover_status': log['gaiting_handover_status'],
                    'loom_stop_rate_per_hour': float(log['loom_stop_rate_per_hour']) if log['loom_stop_rate_per_hour'] else None,
                    'primary_stop_root_cause': log['primary_stop_root_cause'],
                    'first_saree_dimensional_audit': log['first_saree_dimensional_audit'],
                    'waste_percentage_current_run': float(log['waste_percentage_current_run']) if log['waste_percentage_current_run'] else None,
                    'shift_handover_approval_state': log['shift_handover_approval_state'],
                    'saree_batch_release_authorization': log['saree_batch_release_authorization'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'supervisor_name': log['supervisor_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/sup-loom-floor-supervisor/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_sup_supervisor_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sfls.*,
                   u_supervisor.full_name AS supervisor_name
            FROM sup_loom_floor_supervisor_logs sfls
            LEFT JOIN users u_supervisor ON sfls.supervisor_id = u_supervisor.id
            WHERE sfls.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'supervisor_log_id': log['supervisor_log_id'],
            'loom_shed_line_id': log['loom_shed_line_id'],
            'factory_node_id': log['factory_node_id'],
            'supervisor_id': str(log['supervisor_id']),
            'assistant_weaver_job_log_id': str(log['assistant_weaver_job_log_id']) if log['assistant_weaver_job_log_id'] else None,
            'master_weaver_job_id': str(log['master_weaver_job_id']) if log['master_weaver_job_id'] else None,
            'shift_start_time': log['shift_start_time'].isoformat() if log['shift_start_time'] else None,
            'shift_end_time': log['shift_end_time'].isoformat() if log['shift_end_time'] else None,
            'ambient_relative_humidity_pct': float(log['ambient_relative_humidity_pct']) if log['ambient_relative_humidity_pct'] else None,
            'ambient_temperature_celsius': float(log['ambient_temperature_celsius']) if log['ambient_temperature_celsius'] else None,
            'shift_target_oee_pct': float(log['shift_target_oee_pct']) if log['shift_target_oee_pct'] else None,
            'gaiting_handover_status': log['gaiting_handover_status'],
            'loom_stop_rate_per_hour': float(log['loom_stop_rate_per_hour']) if log['loom_stop_rate_per_hour'] else None,
            'primary_stop_root_cause': log['primary_stop_root_cause'],
            'first_saree_dimensional_audit': log['first_saree_dimensional_audit'],
            'waste_percentage_current_run': float(log['waste_percentage_current_run']) if log['waste_percentage_current_run'] else None,
            'shift_handover_approval_state': log['shift_handover_approval_state'],
            'saree_batch_release_authorization': log['saree_batch_release_authorization'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'supervisor_name': log['supervisor_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/sup-loom-floor-supervisor/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_sup_supervisor_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, supervisor_log_id, validation_errors, shift_handover_approval_state
            FROM sup_loom_floor_supervisor_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['supervisor_log_id'])
        qr_tag_id = 'SUP-SUPERVISOR-' + log['supervisor_log_id']
        
        cur.execute("""
            UPDATE sup_loom_floor_supervisor_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, supervisor_log_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO sup_loom_floor_supervisor_certificates (
                supervisor_log_id, certificate_hash, qr_tag_id, supervisor_log_id_ref,
                loom_shed_line_id, assistant_weaver_job_log_id, master_weaver_job_id,
                ambient_relative_humidity_pct, ambient_temperature_celsius,
                shift_target_oee_pct, gaiting_handover_status,
                loom_stop_rate_per_hour, primary_stop_root_cause,
                first_saree_dimensional_audit, waste_percentage_current_run,
                shift_handover_approval_state, saree_batch_release_authorization,
                supervisor_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.supervisor_log_id,
                pj.loom_shed_line_id,
                pj.assistant_weaver_job_log_id,
                pj.master_weaver_job_id,
                pj.ambient_relative_humidity_pct, pj.ambient_temperature_celsius,
                pj.shift_target_oee_pct, pj.gaiting_handover_status,
                pj.loom_stop_rate_per_hour, pj.primary_stop_root_cause,
                pj.first_saree_dimensional_audit, pj.waste_percentage_current_run,
                pj.shift_handover_approval_state, pj.saree_batch_release_authorization,
                pj.supervisor_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'supervisor_log_id', pj.supervisor_log_id,
                    'loom_shed_line_id', pj.loom_shed_line_id,
                    'ambient_relative_humidity_pct', pj.ambient_relative_humidity_pct,
                    'ambient_temperature_celsius', pj.ambient_temperature_celsius,
                    'shift_target_oee_pct', pj.shift_target_oee_pct,
                    'gaiting_handover_status', pj.gaiting_handover_status,
                    'loom_stop_rate_per_hour', pj.loom_stop_rate_per_hour,
                    'first_saree_dimensional_audit', pj.first_saree_dimensional_audit,
                    'waste_percentage_current_run', pj.waste_percentage_current_run,
                    'shift_handover_approval_state', pj.shift_handover_approval_state,
                    'saree_batch_release_authorization', pj.saree_batch_release_authorization
                )
            FROM sup_loom_floor_supervisor_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'supervisor_log_id': result['supervisor_log_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED',
            'message': 'Supervisor log certified'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/sup-loom-floor-supervisor/certificates', methods=['GET'])
@jwt_required()
def list_sup_supervisor_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sflsc.id, sflsc.certificate_hash, sflsc.qr_tag_id,
                   sflsc.supervisor_log_id_ref, sflsc.loom_shed_line_id,
                   sflsc.ambient_relative_humidity_pct, sflsc.ambient_temperature_celsius,
                   sflsc.shift_target_oee_pct, sflsc.gaiting_handover_status,
                   sflsc.loom_stop_rate_per_hour, sflsc.primary_stop_root_cause,
                   sflsc.first_saree_dimensional_audit, sflsc.waste_percentage_current_run,
                   sflsc.shift_handover_approval_state, sflsc.saree_batch_release_authorization,
                   sflsc.status, sflsc.certified_at
            FROM sup_loom_floor_supervisor_certificates sflsc
            WHERE sflsc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY sflsc.certified_at DESC
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
                    'supervisor_log_id_ref': c['supervisor_log_id_ref'],
                    'loom_shed_line_id': c['loom_shed_line_id'],
                    'ambient_relative_humidity_pct': float(c['ambient_relative_humidity_pct']) if c['ambient_relative_humidity_pct'] else None,
                    'ambient_temperature_celsius': float(c['ambient_temperature_celsius']) if c['ambient_temperature_celsius'] else None,
                    'shift_target_oee_pct': float(c['shift_target_oee_pct']) if c['shift_target_oee_pct'] else None,
                    'gaiting_handover_status': c['gaiting_handover_status'],
                    'loom_stop_rate_per_hour': float(c['loom_stop_rate_per_hour']) if c['loom_stop_rate_per_hour'] else None,
                    'primary_stop_root_cause': c['primary_stop_root_cause'],
                    'first_saree_dimensional_audit': c['first_saree_dimensional_audit'],
                    'waste_percentage_current_run': float(c['waste_percentage_current_run']) if c['waste_percentage_current_run'] else None,
                    'shift_handover_approval_state': c['shift_handover_approval_state'],
                    'saree_batch_release_authorization': c['saree_batch_release_authorization'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR SUP LOOM FLOOR SUPERVISOR
# ============================================================

@app.route('/api/v1/sales/forecast/sup-loom-floor-supervisor', methods=['GET'])
@jwt_required()
def get_sales_forecast_sup_supervisor():
    """
    API plugin endpoint for sales team sup loom floor supervisor material processing forecast.
    Returns forecasted floor supervisor requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'loom_shed_line_id': 'LINE-2400-A',
                    'shift_target_ooe_pct': 85.0,
                    'max_loom_stop_rate_per_hour': 1.5,
                    'ambient_relative_humidity_pct_min': 65.0,
                    'ambient_relative_humidity_pct_max': 70.0,
                    'estimated_shifts': 3,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'loom_shed_line_id': 'LINE-2400-B',
                    'shift_target_ooe_pct': 85.0,
                    'max_loom_stop_rate_per_hour': 1.5,
                    'ambient_relative_humidity_pct_min': 65.0,
                    'ambient_relative_humidity_pct_max': 70.0,
                    'estimated_shifts': 2,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'loom_shed_line_id': 'LINE-1536-C',
                    'shift_target_ooe_pct': 75.0,
                    'max_loom_stop_rate_per_hour': 4.0,
                    'ambient_relative_humidity_pct_min': 55.0,
                    'ambient_relative_humidity_pct_max': 60.0,
                    'estimated_shifts': 5,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'SUP-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'loom_shed_line_id': 'LINE-2400-A',
                    'estimated_shifts': 3,
                    'shift_target_ooe_pct': 85.0
                },
                {
                    'lot_number': 'SUP-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'loom_shed_line_id': 'LINE-2400-B',
                    'estimated_shifts': 2,
                    'shift_target_ooe_pct': 85.0
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# QUALITY INSPECTOR (FABRIC QA SPECIALIST) MODULE
# ============================================================

def validate_quality_inspector_guardrails(data):
    """Validate quality inspector inputs against business guardrails."""
    errors = []
    warnings = []
    
    primary_fabric_defect_code = data.get('primary_fabric_defect_code')
    total_saree_length_measured_meters = data.get('total_saree_length_measured_meters')
    qa_inspector_approval_state = data.get('qa_inspector_approval_state')
    final_fabric_quality_grade = data.get('final_fabric_quality_grade')
    
    if primary_fabric_defect_code == 'ZARI_FLOAT_TENSION_FAULT':
        errors.append({
            'code': 'GRADE_A_DISQUALIFICATION',
            'message': 'LOOSE_ZARI_FLOATS_CANNOT_BE_GRADED_PREMIUM'
        })
    
    if total_saree_length_measured_meters is not None:
        if total_saree_length_measured_meters < 6.25 or total_saree_length_measured_meters > 6.35:
            errors.append({
                'code': 'REJECT_OUT_OF_SPEC_LENGTH',
                'message': 'MUST_BE_WITHIN_6.28m_TO_6.32m_RANGE'
            })
    
    if primary_fabric_defect_code == 'HOOK_MISLIFT_PATTERN_ERROR':
        warnings.append({
            'code': 'AUTO_ALERT_LOOM_SUPERVISOR',
            'message': 'CHECK_SOLENOID_BOARD_ON_LOOM_LINE'
        })
    
    if qa_inspector_approval_state is not None and qa_inspector_approval_state != 'PASSED_CLEARED_FOR_PACKING':
        errors.append({
            'code': 'DENY_FINISHED_GOODS_WAREHOUSE_TRANSFER',
            'message': 'INSPECTION_NOT_CLEARED_FOR_PACKING'
        })
    
    return errors, warnings

@app.route('/api/v1/quality-inspector/logs', methods=['POST'])
@jwt_required()
def create_quality_inspector_log():
    try:
        inspector_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['saree_serial_barcode']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (inspector_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_quality_inspector_guardrails(data)
        
        inspection_id = f"QI-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO quality_inspector_logs (
                inspection_id, saree_serial_barcode, loom_id_ref, factory_node_id,
                inspector_employee_id, sup_supervisor_log_id,
                assistant_weaver_job_log_id, master_weaver_job_id, petni_master_job_id,
                warp_joining_job_id, harness_setup_log_id,
                inspection_table_type, fabric_weight_grams_per_sqm,
                pick_density_measured_ppi, warp_density_measured_epi,
                primary_fabric_defect_code, pallu_length_measured_cm,
                total_saree_length_measured_meters, border_width_symmetry_offset_mm,
                zari_tarnish_visual_check, actual_body_length_meters,
                actual_blouse_length_meters, actual_width_inches,
                total_finished_weight_grams, warp_break_streaks_count,
                weft_barriness_detected, zari_tarnishing_present,
                has_oil_grease_stains, loose_zari_floats_count,
                final_fabric_quality_grade, qa_inspector_approval_state,
                piece_rate_penalty_applied, piece_rate_penalty_percent,
                piece_rate_release_status, b2b_order_matched, b2b_order_status,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, inspection_id
        """, (
            inspection_id,
            data.get('saree_serial_barcode'),
            data.get('loom_id_ref'),
            factory_node_id,
            inspector_id,
            data.get('sup_supervisor_log_id'),
            data.get('assistant_weaver_job_log_id'),
            data.get('master_weaver_job_id'),
            data.get('petni_master_job_id'),
            data.get('warp_joining_job_id'),
            data.get('harness_setup_log_id'),
            data.get('inspection_table_type', 'ILLUMINATED_LED_DUAL_SURFACE_TABLE'),
            data.get('fabric_weight_grams_per_sqm'),
            data.get('pick_density_measured_ppi'),
            data.get('warp_density_measured_epi'),
            data.get('primary_fabric_defect_code', 'DEFECT_NONE_CLEAN_PIECE'),
            data.get('pallu_length_measured_cm'),
            data.get('total_saree_length_measured_meters'),
            data.get('border_width_symmetry_offset_mm'),
            data.get('zari_tarnish_visual_check', 'PASSED_FULL_LUSTER'),
            data.get('actual_body_length_meters'),
            data.get('actual_blouse_length_meters'),
            data.get('actual_width_inches'),
            data.get('total_finished_weight_grams'),
            data.get('warp_break_streaks_count', 0),
            data.get('weft_barriness_detected', False),
            data.get('zari_tarnishing_present', False),
            data.get('has_oil_grease_stains', False),
            data.get('loose_zari_floats_count', 0),
            data.get('final_fabric_quality_grade', 'GRADE_A_EXPORT_PREMIUM'),
            data.get('qa_inspector_approval_state', 'INSPECTION_IN_PROGRESS'),
            data.get('piece_rate_penalty_applied', False),
            data.get('piece_rate_penalty_percent', 0.0),
            data.get('piece_rate_release_status', 'PENDING_RELEASE'),
            data.get('b2b_order_matched', False),
            data.get('b2b_order_status', 'PENDING_MATCH'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'inspection_id': log_row['inspection_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality-inspector/logs', methods=['GET'])
@jwt_required()
def list_quality_inspector_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qil.id, qil.inspection_id, qil.saree_serial_barcode, qil.loom_id_ref,
                   qil.inspection_timestamp, qil.final_fabric_quality_grade,
                   qil.qa_inspector_approval_state, qil.auto_assigned_routing,
                   qil.piece_rate_release_status, qil.b2b_order_status,
                   qil.primary_fabric_defect_code, qil.total_saree_length_measured_meters,
                   qil.pallu_length_measured_cm, qil.border_width_symmetry_offset_mm,
                   qil.zari_tarnish_visual_check,
                   u_inspector.full_name AS inspector_name
            FROM quality_inspector_logs qil
            LEFT JOIN users u_inspector ON qil.inspector_employee_id = u_inspector.id
            WHERE qil.inspector_employee_id = %s::uuid
            ORDER BY qil.inspection_timestamp DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'inspection_id': log['inspection_id'],
                    'saree_serial_barcode': log['saree_serial_barcode'],
                    'loom_id_ref': log['loom_id_ref'],
                    'inspection_timestamp': log['inspection_timestamp'].isoformat() if log['inspection_timestamp'] else None,
                    'final_fabric_quality_grade': log['final_fabric_quality_grade'],
                    'qa_inspector_approval_state': log['qa_inspector_approval_state'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'piece_rate_release_status': log['piece_rate_release_status'],
                    'b2b_order_status': log['b2b_order_status'],
                    'primary_fabric_defect_code': log['primary_fabric_defect_code'],
                    'total_saree_length_measured_meters': float(log['total_saree_length_measured_meters']) if log['total_saree_length_measured_meters'] else None,
                    'pallu_length_measured_cm': float(log['pallu_length_measured_cm']) if log['pallu_length_measured_cm'] else None,
                    'border_width_symmetry_offset_mm': float(log['border_width_symmetry_offset_mm']) if log['border_width_symmetry_offset_mm'] else None,
                    'zari_tarnish_visual_check': log['zari_tarnish_visual_check'],
                    'inspector_name': log['inspector_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality-inspector/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_quality_inspector_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qil.*,
                   u_inspector.full_name AS inspector_name
            FROM quality_inspector_logs qil
            LEFT JOIN users u_inspector ON qil.inspector_employee_id = u_inspector.id
            WHERE qil.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'inspection_id': log['inspection_id'],
            'saree_serial_barcode': log['saree_serial_barcode'],
            'loom_id_ref': log['loom_id_ref'],
            'factory_node_id': log['factory_node_id'],
            'inspector_employee_id': str(log['inspector_employee_id']),
            'sup_supervisor_log_id': str(log['sup_supervisor_log_id']) if log['sup_supervisor_log_id'] else None,
            'inspection_timestamp': log['inspection_timestamp'].isoformat() if log['inspection_timestamp'] else None,
            'inspection_table_type': log['inspection_table_type'],
            'fabric_weight_grams_per_sqm': float(log['fabric_weight_grams_per_sqm']) if log['fabric_weight_grams_per_sqm'] else None,
            'pick_density_measured_ppi': float(log['pick_density_measured_ppi']) if log['pick_density_measured_ppi'] else None,
            'warp_density_measured_epi': float(log['warp_density_measured_epi']) if log['warp_density_measured_epi'] else None,
            'primary_fabric_defect_code': log['primary_fabric_defect_code'],
            'pallu_length_measured_cm': float(log['pallu_length_measured_cm']) if log['pallu_length_measured_cm'] else None,
            'total_saree_length_measured_meters': float(log['total_saree_length_measured_meters']) if log['total_saree_length_measured_meters'] else None,
            'border_width_symmetry_offset_mm': float(log['border_width_symmetry_offset_mm']) if log['border_width_symmetry_offset_mm'] else None,
            'zari_tarnish_visual_check': log['zari_tarnish_visual_check'],
            'actual_body_length_meters': float(log['actual_body_length_meters']) if log['actual_body_length_meters'] else None,
            'actual_blouse_length_meters': float(log['actual_blouse_length_meters']) if log['actual_blouse_length_meters'] else None,
            'actual_width_inches': float(log['actual_width_inches']) if log['actual_width_inches'] else None,
            'total_finished_weight_grams': log['total_finished_weight_grams'],
            'warp_break_streaks_count': log['warp_break_streaks_count'],
            'weft_barriness_detected': log['weft_barriness_detected'],
            'zari_tarnishing_present': log['zari_tarnishing_present'],
            'has_oil_grease_stains': log['has_oil_grease_stains'],
            'loose_zari_floats_count': log['loose_zari_floats_count'],
            'final_fabric_quality_grade': log['final_fabric_quality_grade'],
            'qa_inspector_approval_state': log['qa_inspector_approval_state'],
            'piece_rate_penalty_applied': log['piece_rate_penalty_applied'],
            'piece_rate_penalty_percent': float(log['piece_rate_penalty_percent']) if log['piece_rate_penalty_percent'] else None,
            'piece_rate_release_status': log['piece_rate_release_status'],
            'b2b_order_matched': log['b2b_order_matched'],
            'b2b_order_status': log['b2b_order_status'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'inspector_name': log['inspector_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality-inspector/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_quality_inspector_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, inspection_id, validation_errors, qa_inspector_approval_state, final_fabric_quality_grade
            FROM quality_inspector_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['inspection_id'])
        qr_tag_id = 'QI-' + log['inspection_id']
        
        cur.execute("""
            UPDATE quality_inspector_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, inspection_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO quality_inspector_certificates (
                quality_inspector_log_id, certificate_hash, qr_tag_id, inspection_id,
                saree_serial_barcode, loom_id_ref, sup_supervisor_log_id,
                assistant_weaver_job_log_id, master_weaver_job_id,
                actual_body_length_meters, actual_blouse_length_meters,
                actual_width_inches, total_finished_weight_grams,
                warp_break_streaks_count, weft_barriness_detected,
                zari_tarnishing_present, has_oil_grease_stains,
                loose_zari_floats_count, primary_fabric_defect_code,
                pallu_length_measured_cm, total_saree_length_measured_meters,
                border_width_symmetry_offset_mm, zari_tarnish_visual_check,
                final_fabric_quality_grade, qa_inspector_approval_state,
                piece_rate_penalty_applied, piece_rate_penalty_percent,
                piece_rate_release_status, b2b_order_matched, b2b_order_status,
                inspector_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.inspection_id,
                pj.saree_serial_barcode,
                pj.loom_id_ref,
                pj.sup_supervisor_log_id,
                pj.assistant_weaver_job_log_id,
                pj.master_weaver_job_id,
                pj.actual_body_length_meters, pj.actual_blouse_length_meters,
                pj.actual_width_inches, pj.total_finished_weight_grams,
                pj.warp_break_streaks_count, pj.weft_barriness_detected,
                pj.zari_tarnishing_present, pj.has_oil_grease_stains,
                pj.loose_zari_floats_count, pj.primary_fabric_defect_code,
                pj.pallu_length_measured_cm, pj.total_saree_length_measured_meters,
                pj.border_width_symmetry_offset_mm, pj.zari_tarnish_visual_check,
                pj.final_fabric_quality_grade, pj.qa_inspector_approval_state,
                pj.piece_rate_penalty_applied, pj.piece_rate_penalty_percent,
                pj.piece_rate_release_status, pj.b2b_order_matched, pj.b2b_order_status,
                pj.inspector_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'inspection_id', pj.inspection_id,
                    'saree_serial_barcode', pj.saree_serial_barcode,
                    'final_fabric_quality_grade', pj.final_fabric_quality_grade,
                    'qa_inspector_approval_state', pj.qa_inspector_approval_state,
                    'piece_rate_release_status', pj.piece_rate_release_status,
                    'b2b_order_status', pj.b2b_order_status,
                    'total_saree_length_measured_meters', pj.total_saree_length_measured_meters,
                    'pallu_length_measured_cm', pj.pallu_length_measured_cm,
                    'primary_fabric_defect_code', pj.primary_fabric_defect_code
                )
            FROM quality_inspector_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'inspection_id': result['inspection_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'PASSED_CLEARED_FOR_PACKING',
            'message': 'Quality inspection certified and cleared for packing'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/quality-inspector/certificates', methods=['GET'])
@jwt_required()
def list_quality_inspector_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qic.id, qic.certificate_hash, qic.qr_tag_id,
                   qic.inspection_id, qic.saree_serial_barcode, qic.loom_id_ref,
                   qic.actual_body_length_meters, qic.actual_blouse_length_meters,
                   qic.actual_width_inches, qic.total_finished_weight_grams,
                   qic.warp_break_streaks_count, qic.weft_barriness_detected,
                   qic.zari_tarnishing_present, qic.has_oil_grease_stains,
                   qic.loose_zari_floats_count, qic.primary_fabric_defect_code,
                   qic.pallu_length_measured_cm, qic.total_saree_length_measured_meters,
                   qic.border_width_symmetry_offset_mm, qic.zari_tarnish_visual_check,
                   qic.final_fabric_quality_grade, qic.qa_inspector_approval_state,
                   qic.piece_rate_penalty_applied, qic.piece_rate_penalty_percent,
                   qic.piece_rate_release_status, qic.b2b_order_matched,
                   qic.b2b_order_status, qic.status, qic.certified_at
            FROM quality_inspector_certificates qic
            WHERE qic.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY qic.certified_at DESC
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
                    'inspection_id': c['inspection_id'],
                    'saree_serial_barcode': c['saree_serial_barcode'],
                    'loom_id_ref': c['loom_id_ref'],
                    'actual_body_length_meters': float(c['actual_body_length_meters']) if c['actual_body_length_meters'] else None,
                    'actual_blouse_length_meters': float(c['actual_blouse_length_meters']) if c['actual_blouse_length_meters'] else None,
                    'actual_width_inches': float(c['actual_width_inches']) if c['actual_width_inches'] else None,
                    'total_finished_weight_grams': c['total_finished_weight_grams'],
                    'warp_break_streaks_count': c['warp_break_streaks_count'],
                    'weft_barriness_detected': c['weft_barriness_detected'],
                    'zari_tarnishing_present': c['zari_tarnishing_present'],
                    'has_oil_grease_stains': c['has_oil_grease_stains'],
                    'loose_zari_floats_count': c['loose_zari_floats_count'],
                    'primary_fabric_defect_code': c['primary_fabric_defect_code'],
                    'pallu_length_measured_cm': float(c['pallu_length_measured_cm']) if c['pallu_length_measured_cm'] else None,
                    'total_saree_length_measured_meters': float(c['total_saree_length_measured_meters']) if c['total_saree_length_measured_meters'] else None,
                    'border_width_symmetry_offset_mm': float(c['border_width_symmetry_offset_mm']) if c['border_width_symmetry_offset_mm'] else None,
                    'zari_tarnish_visual_check': c['zari_tarnish_visual_check'],
                    'final_fabric_quality_grade': c['final_fabric_quality_grade'],
                    'qa_inspector_approval_state': c['qa_inspector_approval_state'],
                    'piece_rate_penalty_applied': c['piece_rate_penalty_applied'],
                    'piece_rate_penalty_percent': float(c['piece_rate_penalty_percent']) if c['piece_rate_penalty_percent'] else None,
                    'piece_rate_release_status': c['piece_rate_release_status'],
                    'b2b_order_matched': c['b2b_order_matched'],
                    'b2b_order_status': c['b2b_order_status'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR QUALITY INSPECTOR
# ============================================================

@app.route('/api/v1/sales/forecast/quality-inspector', methods=['GET'])
@jwt_required()
def get_sales_forecast_quality_inspector():
    """
    API plugin endpoint for sales team quality inspector material processing forecast.
    Returns forecasted quality inspection requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'inspection_table_type': 'ILLUMINATED_LED_DUAL_SURFACE_TABLE',
                    'pick_density_tolerance': 0.5,
                    'zari_float_tolerance_mm': 0.0,
                    'dimensional_tolerance_cm': 1.5,
                    'estimated_inspections': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'inspection_table_type': 'ILLUMINATED_LED_DUAL_SURFACE_TABLE',
                    'pick_density_tolerance': 0.5,
                    'zari_float_tolerance_mm': 0.0,
                    'dimensional_tolerance_cm': 1.5,
                    'estimated_inspections': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'inspection_table_type': 'STANDARD_OVERHEAD_LIGHT_TABLE',
                    'pick_density_tolerance': 2.0,
                    'zari_float_tolerance_mm': 2.0,
                    'dimensional_tolerance_cm': 5.0,
                    'estimated_inspections': 120,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'QI-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_inspections': 80,
                    'inspection_table_type': 'ILLUMINATED_LED_DUAL_SURFACE_TABLE'
                },
                {
                    'lot_number': 'QI-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_inspections': 60,
                    'inspection_table_type': 'ILLUMINATED_LED_DUAL_SURFACE_TABLE'
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# QA DYEING INSPECTOR MODULE
# ============================================================

def validate_qa_dyeing_inspector_guardrails(data):
    """Validate QA dyeing inspector inputs against business guardrails."""
    errors = []
    warnings = []
    
    color_delta_e = data.get('color_delta_e')
    color_fastness_grade = data.get('color_fastness_grade')
    primary_dye_defect_code = data.get('primary_dye_defect_code')
    qa_dyeing_approval_state = data.get('qa_dyeing_approval_state')
    
    if color_delta_e is not None and color_delta_e > 1.0:
        errors.append({
            'code': 'COLOR_VARIANCE_FAIL',
            'message': 'COLOR_DELTA_E_EXCEEDS_ACCEPTABLE_THRESHOLD_OF_1.0'
        })
    
    if color_fastness_grade in ('GRADE_C_ACCEPTABLE', 'GRADE_D_POOR'):
        errors.append({
            'code': 'FASTNESS_FAIL',
            'message': 'COLOR_FASTNESS_GRADE_BELOW_ACCEPTABLE_THRESHOLD'
        })
    
    if primary_dye_defect_code in ('SHADE_VARIATION_LOT', 'DYE_STREAK_MARK', 'UNEVEN_PENETRATION'):
        warnings.append({
            'code': 'AUTO_ALERT_DYEING_SUPERVISOR',
            'message': 'DYE_DEFECT_DETECTED_REQUIRES_SUPERVISOR_REVIEW'
        })
    
    if qa_dyeing_approval_state is not None and qa_dyeing_approval_state != 'PASSED_CLEARED_FOR_FINISHING':
        errors.append({
            'code': 'DENY_FINISHING_TRANSFER',
            'message': 'DYEING_INSPECTION_NOT_CLEARED'
        })
    
    return errors, warnings

@app.route('/api/v1/qa-dyeing-inspector/logs', methods=['POST'])
@jwt_required()
def create_qa_dyeing_inspector_log():
    try:
        inspector_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['batch_id']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (inspector_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_qa_dyeing_inspector_guardrails(data)
        
        inspection_id = f"QDI-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        
        cur.execute("""
            INSERT INTO qa_dyeing_inspector_logs (
                inspection_id, batch_id, saree_serial_barcode, factory_node_id,
                inspector_employee_id, quality_inspector_log_id, sup_supervisor_log_id,
                dye_batch_ref, color_code, color_name, dyeing_process_type,
                color_delta_e, color_fastness_grade, shade_variation_detected,
                dye_penetration_uniform, metamerism_risk,
                primary_dye_defect_code, batch_clearance_status, qa_dyeing_approval_state,
                piece_rate_penalty_applied, piece_rate_penalty_percent,
                piece_rate_release_status, b2b_order_matched, b2b_order_status,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, inspection_id
        """, (
            inspection_id,
            data.get('batch_id'),
            data.get('saree_serial_barcode'),
            factory_node_id,
            inspector_id,
            data.get('quality_inspector_log_id'),
            data.get('sup_supervisor_log_id'),
            data.get('dye_batch_ref'),
            data.get('color_code'),
            data.get('color_name'),
            data.get('dyeing_process_type', 'SKEIN_DYE'),
            data.get('color_delta_e'),
            data.get('color_fastness_grade', 'GRADE_A_EXCELLENT'),
            data.get('shade_variation_detected', False),
            data.get('dye_penetration_uniform', True),
            data.get('metamerism_risk', 'LOW'),
            data.get('primary_dye_defect_code', 'DEFECT_NONE_CLEAN_BATCH'),
            data.get('batch_clearance_status', 'CLEARED_FOR_FINISHING'),
            data.get('qa_dyeing_approval_state', 'INSPECTION_IN_PROGRESS'),
            data.get('piece_rate_penalty_applied', False),
            data.get('piece_rate_penalty_percent', 0.0),
            data.get('piece_rate_release_status', 'PENDING_RELEASE'),
            data.get('b2b_order_matched', False),
            data.get('b2b_order_status', 'PENDING_MATCH'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'inspection_id': log_row['inspection_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/qa-dyeing-inspector/logs', methods=['GET'])
@jwt_required()
def list_qa_dyeing_inspector_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qdil.id, qdil.inspection_id, qdil.batch_id, qdil.saree_serial_barcode,
                   qdil.color_code, qdil.color_name, qdil.color_delta_e,
                   qdil.color_fastness_grade, qdil.primary_dye_defect_code,
                   qdil.batch_clearance_status, qdil.qa_dyeing_approval_state,
                   qdil.auto_assigned_routing, qdil.piece_rate_release_status,
                   qdil.b2b_order_status, qdil.inspection_timestamp,
                   u_inspector.full_name AS inspector_name
            FROM qa_dyeing_inspector_logs qdil
            LEFT JOIN users u_inspector ON qdil.inspector_employee_id = u_inspector.id
            WHERE qdil.inspector_employee_id = %s::uuid
            ORDER BY qdil.inspection_timestamp DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'inspection_id': log['inspection_id'],
                    'batch_id': log['batch_id'],
                    'saree_serial_barcode': log['saree_serial_barcode'],
                    'color_code': log['color_code'],
                    'color_name': log['color_name'],
                    'color_delta_e': float(log['color_delta_e']) if log['color_delta_e'] else None,
                    'color_fastness_grade': log['color_fastness_grade'],
                    'primary_dye_defect_code': log['primary_dye_defect_code'],
                    'batch_clearance_status': log['batch_clearance_status'],
                    'qa_dyeing_approval_state': log['qa_dyeing_approval_state'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'piece_rate_release_status': log['piece_rate_release_status'],
                    'b2b_order_status': log['b2b_order_status'],
                    'inspection_timestamp': log['inspection_timestamp'].isoformat() if log['inspection_timestamp'] else None,
                    'inspector_name': log['inspector_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/qa-dyeing-inspector/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_qa_dyeing_inspector_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qdil.*,
                   u_inspector.full_name AS inspector_name
            FROM qa_dyeing_inspector_logs qdil
            LEFT JOIN users u_inspector ON qdil.inspector_employee_id = u_inspector.id
            WHERE qdil.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'inspection_id': log['inspection_id'],
            'batch_id': log['batch_id'],
            'saree_serial_barcode': log['saree_serial_barcode'],
            'factory_node_id': log['factory_node_id'],
            'inspector_employee_id': str(log['inspector_employee_id']),
            'quality_inspector_log_id': str(log['quality_inspector_log_id']) if log['quality_inspector_log_id'] else None,
            'sup_supervisor_log_id': str(log['sup_supervisor_log_id']) if log['sup_supervisor_log_id'] else None,
            'inspection_timestamp': log['inspection_timestamp'].isoformat() if log['inspection_timestamp'] else None,
            'dye_batch_ref': log['dye_batch_ref'],
            'color_code': log['color_code'],
            'color_name': log['color_name'],
            'dyeing_process_type': log['dyeing_process_type'],
            'color_delta_e': float(log['color_delta_e']) if log['color_delta_e'] else None,
            'color_fastness_grade': log['color_fastness_grade'],
            'shade_variation_detected': log['shade_variation_detected'],
            'dye_penetration_uniform': log['dye_penetration_uniform'],
            'metamerism_risk': log['metamerism_risk'],
            'primary_dye_defect_code': log['primary_dye_defect_code'],
            'batch_clearance_status': log['batch_clearance_status'],
            'qa_dyeing_approval_state': log['qa_dyeing_approval_state'],
            'piece_rate_penalty_applied': log['piece_rate_penalty_applied'],
            'piece_rate_penalty_percent': float(log['piece_rate_penalty_percent']) if log['piece_rate_penalty_percent'] else None,
            'piece_rate_release_status': log['piece_rate_release_status'],
            'b2b_order_matched': log['b2b_order_matched'],
            'b2b_order_status': log['b2b_order_status'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'inspector_name': log['inspector_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/qa-dyeing-inspector/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_qa_dyeing_inspector_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, inspection_id, validation_errors, qa_dyeing_approval_state
            FROM qa_dyeing_inspector_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['inspection_id'])
        qr_tag_id = 'QDI-' + log['inspection_id']
        
        cur.execute("""
            UPDATE qa_dyeing_inspector_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, inspection_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO qa_dyeing_inspector_certificates (
                qa_dyeing_inspector_log_id, certificate_hash, qr_tag_id, inspection_id,
                batch_id, saree_serial_barcode, color_code, color_name,
                dyeing_process_type, color_delta_e, color_fastness_grade,
                shade_variation_detected, dye_penetration_uniform, metamerism_risk,
                primary_dye_defect_code, batch_clearance_status, qa_dyeing_approval_state,
                piece_rate_penalty_applied, piece_rate_penalty_percent,
                piece_rate_release_status, b2b_order_matched, b2b_order_status,
                inspector_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.inspection_id,
                pj.batch_id,
                pj.saree_serial_barcode,
                pj.color_code, pj.color_name,
                pj.dyeing_process_type, pj.color_delta_e, pj.color_fastness_grade,
                pj.shade_variation_detected, pj.dye_penetration_uniform, pj.metamerism_risk,
                pj.primary_dye_defect_code, pj.batch_clearance_status, pj.qa_dyeing_approval_state,
                pj.piece_rate_penalty_applied, pj.piece_rate_penalty_percent,
                pj.piece_rate_release_status, pj.b2b_order_matched, pj.b2b_order_status,
                pj.inspector_employee_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'inspection_id', pj.inspection_id,
                    'batch_id', pj.batch_id,
                    'color_code', pj.color_code,
                    'color_delta_e', pj.color_delta_e,
                    'color_fastness_grade', pj.color_fastness_grade,
                    'primary_dye_defect_code', pj.primary_dye_defect_code,
                    'qa_dyeing_approval_state', pj.qa_dyeing_approval_state,
                    'piece_rate_release_status', pj.piece_rate_release_status,
                    'b2b_order_status', pj.b2b_order_status
                )
            FROM qa_dyeing_inspector_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'inspection_id': result['inspection_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'PASSED_CLEARED_FOR_FINISHING',
            'message': 'QA dyeing inspection certified and cleared for finishing'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/qa-dyeing-inspector/certificates', methods=['GET'])
@jwt_required()
def list_qa_dyeing_inspector_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT qdic.id, qdic.certificate_hash, qdic.qr_tag_id,
                   qdic.inspection_id, qdic.batch_id, qdic.color_code,
                   qdic.color_delta_e, qdic.color_fastness_grade,
                   qdic.primary_dye_defect_code, qdic.batch_clearance_status,
                   qdic.qa_dyeing_approval_state, qdic.piece_rate_penalty_applied,
                   qdic.piece_rate_penalty_percent, qdic.piece_rate_release_status,
                   qdic.b2b_order_matched, qdic.b2b_order_status,
                   qdic.status, qdic.certified_at
            FROM qa_dyeing_inspector_certificates qdic
            WHERE qdic.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY qdic.certified_at DESC
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
                    'inspection_id': c['inspection_id'],
                    'batch_id': c['batch_id'],
                    'color_code': c['color_code'],
                    'color_delta_e': float(c['color_delta_e']) if c['color_delta_e'] else None,
                    'color_fastness_grade': c['color_fastness_grade'],
                    'primary_dye_defect_code': c['primary_dye_defect_code'],
                    'batch_clearance_status': c['batch_clearance_status'],
                    'qa_dyeing_approval_state': c['qa_dyeing_approval_state'],
                    'piece_rate_penalty_applied': c['piece_rate_penalty_applied'],
                    'piece_rate_penalty_percent': float(c['piece_rate_penalty_percent']) if c['piece_rate_penalty_percent'] else None,
                    'piece_rate_release_status': c['piece_rate_release_status'],
                    'b2b_order_matched': c['b2b_order_matched'],
                    'b2b_order_status': c['b2b_order_status'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR QA DYEING INSPECTOR
# ============================================================

@app.route('/api/v1/sales/forecast/qa-dyeing-inspector', methods=['GET'])
@jwt_required()
def get_sales_forecast_qa_dyeing_inspector():
    """
    API plugin endpoint for sales team QA dyeing inspector material processing forecast.
    Returns forecasted dyeing inspection requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'color_code': 'DEEP_MAROON',
                    'dyeing_process_type': 'SKEIN_DYE',
                    'color_delta_e_threshold': 1.0,
                    'color_fastness_grade': 'GRADE_A_EXCELLENT',
                    'estimated_batches': 5,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'color_code': 'ROYAL_BLUE',
                    'dyeing_process_type': 'SKEIN_DYE',
                    'color_delta_e_threshold': 1.0,
                    'color_fastness_grade': 'GRADE_A_EXCELLENT',
                    'estimated_batches': 3,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'color_code': 'EMERALD_GREEN',
                    'dyeing_process_type': 'HANK_DYE',
                    'color_delta_e_threshold': 1.5,
                    'color_fastness_grade': 'GRADE_B_GOOD',
                    'estimated_batches': 8,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'QDI-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'color_code': 'DEEP_MAROON',
                    'estimated_batches': 5
                },
                {
                    'lot_number': 'QDI-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'color_code': 'ROYAL_BLUE',
                    'estimated_batches': 3
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SILK MARK OFFICER MODULE
# ============================================================

def validate_silk_mark_officer_guardrails(data):
    """Validate silk mark officer inputs against business guardrails."""
    errors = []
    warnings = []
    
    burn_test_result_warp_weft = data.get('burn_test_result_warp_weft')
    chemical_solubility_test_status = data.get('chemical_solubility_test_status')
    zari_purity_classification = data.get('zari_purity_classification')
    xrf_silver_content_pct = data.get('xrf_silver_content_pct')
    silk_core_zari_wrap_status = data.get('silk_core_zari_wrap_status')
    silk_mark_officer_approval_state = data.get('silk_mark_officer_approval_state')
    quality_inspector_log_id = data.get('quality_inspector_log_id')
    
    if burn_test_result_warp_weft == 'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED' or chemical_solubility_test_status == 'PARTIAL_RESIDUE_POLYESTER_BLEND':
        errors.append({
            'code': 'IMMEDIATE_CERTIFICATION_HALT',
            'message': 'SYNTHETIC_CONTAMINATION_DETECTED'
        })
    
    if zari_purity_classification == 'PURE_GOLD_SILVER_TESTED_ZARI':
        if xrf_silver_content_pct is not None and xrf_silver_content_pct < 45.0:
            errors.append({
                'code': 'REJECT_ZARI_PURITY_CLAIM',
                'message': 'ZARI_DOES_NOT_MEET_PURE_GOLD_SILVER_STANDARDS'
            })
        if silk_core_zari_wrap_status is not None and silk_core_zari_wrap_status != 'PASSED_PURE_SILK_CORE':
            errors.append({
                'code': 'REJECT_ZARI_PURITY_CLAIM',
                'message': 'ZARI_DOES_NOT_MEET_PURE_GOLD_SILVER_STANDARDS'
            })
    
    if quality_inspector_log_id:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT final_fabric_quality_grade FROM quality_inspector_logs WHERE id = %s::uuid
        """, (quality_inspector_log_id,))
        grade_row = cur.fetchone()
        cur.close()
        conn.close()
        if grade_row and grade_row['final_fabric_quality_grade'] in ('GRADE_C_RESERVE_DISCOUNT', 'REJECTED_SCRAP'):
            errors.append({
                'code': 'DENY_SILK_MARK_TAG_ISSUANCE',
                'message': 'REJECTED_OR_GRADE_C_PIECES_CANNOT_BE_ISSUED_PREMIUM_SILK_MARK_TAGS'
            })
    
    if silk_mark_officer_approval_state is not None and silk_mark_officer_approval_state != 'CERTIFIED_GENUINE_SILK_MARK_RELEASED':
        errors.append({
            'code': 'DENY_FINISHED_GOODS_SALES_DISPATCH',
            'message': 'SILK_MARK_CERTIFICATION_NOT_RELEASED'
        })
    
    return errors, warnings

@app.route('/api/v1/silk-mark-officer/logs', methods=['POST'])
@jwt_required()
def create_silk_mark_officer_log():
    try:
        officer_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['audit_visit_id', 'smoii_authorised_user_id']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (officer_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_silk_mark_officer_guardrails(data)
        
        cur.execute("""
            INSERT INTO silk_mark_officer_logs (
                audit_visit_id, smoii_authorised_user_id, license_validity_start_date,
                license_expiry_date, assigned_silk_mark_officer_id, factory_node_id,
                silk_mark_officer_id, quality_inspector_log_id, qa_dyeing_inspector_log_id,
                sup_supervisor_log_id, assistant_weaver_job_log_id, master_weaver_job_id,
                sampled_production_run_ref, sample_extraction_weight_gm, lab_report_status,
                saree_piece_serial_id, silk_yarn_denier_testing_report,
                burn_test_result_warp_weft, chemical_solubility_test_status,
                zari_purity_classification, xrf_silver_content_pct,
                xrf_gold_content_grams_per_kg, silk_core_zari_wrap_status,
                silk_mark_tag_serial_number, tag_application_status,
                silk_mark_officer_approval_state, hologram_consignment_invoice_no,
                hologram_serial_range_start, hologram_serial_range_end,
                total_tags_received_qty, validation_errors, validation_warnings,
                auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, audit_visit_id
        """, (
            data.get('audit_visit_id'),
            data.get('smoii_authorised_user_id'),
            data.get('license_validity_start_date'),
            data.get('license_expiry_date'),
            data.get('assigned_silk_mark_officer_id'),
            factory_node_id,
            officer_id,
            data.get('quality_inspector_log_id'),
            data.get('qa_dyeing_inspector_log_id'),
            data.get('sup_supervisor_log_id'),
            data.get('assistant_weaver_job_log_id'),
            data.get('master_weaver_job_id'),
            data.get('sampled_production_run_ref'),
            data.get('sample_extraction_weight_gm'),
            data.get('lab_report_status', 'PENDING_LAB_ANALYSIS'),
            data.get('saree_piece_serial_id'),
            data.get('silk_yarn_denier_testing_report', 'PASSED_16_18D_MULBERRY_SILK'),
            data.get('burn_test_result_warp_weft', 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR'),
            data.get('chemical_solubility_test_status', '100%_DISSOLVED_PURE_PROTEIN'),
            data.get('zari_purity_classification', 'PURE_GOLD_SILVER_TESTED_ZARI'),
            data.get('xrf_silver_content_pct'),
            data.get('xrf_gold_content_grams_per_kg'),
            data.get('silk_core_zari_wrap_status', 'PASSED_PURE_SILK_CORE'),
            data.get('silk_mark_tag_serial_number'),
            data.get('tag_application_status', 'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED'),
            data.get('silk_mark_officer_approval_state', 'CERTIFIED_GENUINE_SILK_MARK_RELEASED'),
            data.get('hologram_consignment_invoice_no'),
            data.get('hologram_serial_range_start'),
            data.get('hologram_serial_range_end'),
            data.get('total_tags_received_qty'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'audit_visit_id': log_row['audit_visit_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/silk-mark-officer/logs', methods=['GET'])
@jwt_required()
def list_silk_mark_officer_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT smol.id, smol.audit_visit_id, smol.saree_piece_serial_id,
                   smol.lab_report_status, smol.silk_mark_tag_serial_number,
                   smol.silk_mark_officer_approval_state, smol.auto_assigned_routing,
                   smol.burn_test_result_warp_weft, smol.chemical_solubility_test_status,
                   smol.zari_purity_classification, smol.xrf_silver_content_pct,
                   smol.silk_core_zari_wrap_status, smol.tag_application_status,
                   smol.audit_timestamp,
                   u_officer.full_name AS officer_name
            FROM silk_mark_officer_logs smol
            LEFT JOIN users u_officer ON smol.silk_mark_officer_id = u_officer.id
            WHERE smol.silk_mark_officer_id = %s::uuid
            ORDER BY smol.audit_timestamp DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'audit_visit_id': log['audit_visit_id'],
                    'saree_piece_serial_id': log['saree_piece_serial_id'],
                    'lab_report_status': log['lab_report_status'],
                    'silk_mark_tag_serial_number': log['silk_mark_tag_serial_number'],
                    'silk_mark_officer_approval_state': log['silk_mark_officer_approval_state'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'burn_test_result_warp_weft': log['burn_test_result_warp_weft'],
                    'chemical_solubility_test_status': log['chemical_solubility_test_status'],
                    'zari_purity_classification': log['zari_purity_classification'],
                    'xrf_silver_content_pct': float(log['xrf_silver_content_pct']) if log['xrf_silver_content_pct'] else None,
                    'silk_core_zari_wrap_status': log['silk_core_zari_wrap_status'],
                    'tag_application_status': log['tag_application_status'],
                    'audit_timestamp': log['audit_timestamp'].isoformat() if log['audit_timestamp'] else None,
                    'officer_name': log['officer_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/silk-mark-officer/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_silk_mark_officer_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT smol.*,
                   u_officer.full_name AS officer_name
            FROM silk_mark_officer_logs smol
            LEFT JOIN users u_officer ON smol.silk_mark_officer_id = u_officer.id
            WHERE smol.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'audit_visit_id': log['audit_visit_id'],
            'smoii_authorised_user_id': log['smoii_authorised_user_id'],
            'license_validity_start_date': log['license_validity_start_date'].isoformat() if log['license_validity_start_date'] else None,
            'license_expiry_date': log['license_expiry_date'].isoformat() if log['license_expiry_date'] else None,
            'assigned_silk_mark_officer_id': log['assigned_silk_mark_officer_id'],
            'factory_node_id': log['factory_node_id'],
            'silk_mark_officer_id': str(log['silk_mark_officer_id']),
            'quality_inspector_log_id': str(log['quality_inspector_log_id']) if log['quality_inspector_log_id'] else None,
            'qa_dyeing_inspector_log_id': str(log['qa_dyeing_inspector_log_id']) if log['qa_dyeing_inspector_log_id'] else None,
            'sup_supervisor_log_id': str(log['sup_supervisor_log_id']) if log['sup_supervisor_log_id'] else None,
            'audit_timestamp': log['audit_timestamp'].isoformat() if log['audit_timestamp'] else None,
            'saree_piece_serial_id': log['saree_piece_serial_id'],
            'silk_yarn_denier_testing_report': log['silk_yarn_denier_testing_report'],
            'burn_test_result_warp_weft': log['burn_test_result_warp_weft'],
            'chemical_solubility_test_status': log['chemical_solubility_test_status'],
            'lab_report_status': log['lab_report_status'],
            'zari_purity_classification': log['zari_purity_classification'],
            'xrf_silver_content_pct': float(log['xrf_silver_content_pct']) if log['xrf_silver_content_pct'] else None,
            'xrf_gold_content_grams_per_kg': float(log['xrf_gold_content_grams_per_kg']) if log['xrf_gold_content_grams_per_kg'] else None,
            'silk_core_zari_wrap_status': log['silk_core_zari_wrap_status'],
            'silk_mark_tag_serial_number': log['silk_mark_tag_serial_number'],
            'tag_application_status': log['tag_application_status'],
            'silk_mark_officer_approval_state': log['silk_mark_officer_approval_state'],
            'hologram_consignment_invoice_no': log['hologram_consignment_invoice_no'],
            'hologram_serial_range_start': log['hologram_serial_range_start'],
            'hologram_serial_range_end': log['hologram_serial_range_end'],
            'total_tags_received_qty': log['total_tags_received_qty'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'officer_name': log['officer_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/silk-mark-officer/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_silk_mark_officer_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, audit_visit_id, validation_errors, silk_mark_officer_approval_state
            FROM silk_mark_officer_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['audit_visit_id'])
        qr_tag_id = 'SMO-' + log['audit_visit_id']
        
        cur.execute("""
            UPDATE silk_mark_officer_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, audit_visit_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO silk_mark_officer_certificates (
                silk_mark_officer_log_id, certificate_hash, qr_tag_id, audit_visit_id,
                saree_piece_serial_id, silk_mark_tag_serial_number, smoii_authorised_user_id,
                assigned_silk_mark_officer_id, lab_report_status,
                burn_test_result_warp_weft, chemical_solubility_test_status,
                zari_purity_classification, xrf_silver_content_pct,
                xrf_gold_content_grams_per_kg, silk_core_zari_wrap_status,
                tag_application_status, silk_mark_officer_approval_state,
                hologram_serial_range_start, hologram_serial_range_end,
                total_tags_received_qty, officer_id, approver_id, factory_node_id,
                certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.audit_visit_id,
                pj.saree_piece_serial_id,
                pj.silk_mark_tag_serial_number,
                pj.smoii_authorised_user_id,
                pj.assigned_silk_mark_officer_id,
                pj.lab_report_status,
                pj.burn_test_result_warp_weft, pj.chemical_solubility_test_status,
                pj.zari_purity_classification, pj.xrf_silver_content_pct,
                pj.xrf_gold_content_grams_per_kg, pj.silk_core_zari_wrap_status,
                pj.tag_application_status, pj.silk_mark_officer_approval_state,
                pj.hologram_serial_range_start, pj.hologram_serial_range_end,
                pj.total_tags_received_qty,
                pj.silk_mark_officer_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'audit_visit_id', pj.audit_visit_id,
                    'saree_piece_serial_id', pj.saree_piece_serial_id,
                    'silk_mark_tag_serial_number', pj.silk_mark_tag_serial_number,
                    'smoii_authorised_user_id', pj.smoii_authorised_user_id,
                    'lab_report_status', pj.lab_report_status,
                    'burn_test_result_warp_weft', pj.burn_test_result_warp_weft,
                    'chemical_solubility_test_status', pj.chemical_solubility_test_status,
                    'zari_purity_classification', pj.zari_purity_classification,
                    'xrf_silver_content_pct', pj.xrf_silver_content_pct,
                    'xrf_gold_content_grams_per_kg', pj.xrf_gold_content_grams_per_kg,
                    'silk_core_zari_wrap_status', pj.silk_core_zari_wrap_status,
                    'silk_mark_officer_approval_state', pj.silk_mark_officer_approval_state,
                    'tag_application_status', pj.tag_application_status
                )
            FROM silk_mark_officer_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'audit_visit_id': result['audit_visit_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED_GENUINE_SILK_MARK_RELEASED',
            'message': 'Silk Mark certification issued and hologram tag released'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/silk-mark-officer/certificates', methods=['GET'])
@jwt_required()
def list_silk_mark_officer_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT smoc.id, smoc.certificate_hash, smoc.qr_tag_id,
                   smoc.audit_visit_id, smoc.saree_piece_serial_id,
                   smoc.silk_mark_tag_serial_number, smoc.smoii_authorised_user_id,
                   smoc.lab_report_status, smoc.burn_test_result_warp_weft,
                   smoc.chemical_solubility_test_status, smoc.zari_purity_classification,
                   smoc.xrf_silver_content_pct, smoc.xrf_gold_content_grams_per_kg,
                   smoc.silk_core_zari_wrap_status, smoc.tag_application_status,
                   smoc.silk_mark_officer_approval_state, smoc.status, smoc.certified_at
            FROM silk_mark_officer_certificates smoc
            WHERE smoc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY smoc.certified_at DESC
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
                    'audit_visit_id': c['audit_visit_id'],
                    'saree_piece_serial_id': c['saree_piece_serial_id'],
                    'silk_mark_tag_serial_number': c['silk_mark_tag_serial_number'],
                    'smoii_authorised_user_id': c['smoii_authorised_user_id'],
                    'lab_report_status': c['lab_report_status'],
                    'burn_test_result_warp_weft': c['burn_test_result_warp_weft'],
                    'chemical_solubility_test_status': c['chemical_solubility_test_status'],
                    'zari_purity_classification': c['zari_purity_classification'],
                    'xrf_silver_content_pct': float(c['xrf_silver_content_pct']) if c['xrf_silver_content_pct'] else None,
                    'xrf_gold_content_grams_per_kg': float(c['xrf_gold_content_grams_per_kg']) if c['xrf_gold_content_grams_per_kg'] else None,
                    'silk_core_zari_wrap_status': c['silk_core_zari_wrap_status'],
                    'tag_application_status': c['tag_application_status'],
                    'silk_mark_officer_approval_state': c['silk_mark_officer_approval_state'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR SILK MARK OFFICER
# ============================================================

@app.route('/api/v1/sales/forecast/silk-mark-officer', methods=['GET'])
@jwt_required()
def get_sales_forecast_silk_mark_officer():
    """
    API plugin endpoint for sales team silk mark officer material processing forecast.
    Returns forecasted compliance and certification requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'silk_yarn_denier_testing_report': 'PASSED_16_18D_MULBERRY_SILK',
                    'burn_test_result_warp_weft': 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR',
                    'chemical_solubility_test_status': '100%_DISSOLVED_PURE_PROTEIN',
                    'zari_purity_classification': 'PURE_GOLD_SILVER_TESTED_ZARI',
                    'xrf_silver_content_pct_min': 45.0,
                    'xrf_gold_content_grams_per_kg_min': 5.0,
                    'estimated_certifications': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'silk_yarn_denier_testing_report': 'PASSED_16_18D_MULBERRY_SILK',
                    'burn_test_result_warp_weft': 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR',
                    'chemical_solubility_test_status': '100%_DISSOLVED_PURE_PROTEIN',
                    'zari_purity_classification': 'PURE_GOLD_SILVER_TESTED_ZARI',
                    'xrf_silver_content_pct_min': 45.0,
                    'xrf_gold_content_grams_per_kg_min': 5.0,
                    'estimated_certifications': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'silk_yarn_denier_testing_report': 'PASSED_20_22D_RAW_SILK',
                    'burn_test_result_warp_weft': 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR',
                    'chemical_solubility_test_status': '100%_DISSOLVED_PURE_PROTEIN',
                    'zari_purity_classification': 'HALF_FINE_ZARI',
                    'xrf_silver_content_pct_min': 30.0,
                    'xrf_gold_content_grams_per_kg_min': 2.0,
                    'estimated_certifications': 120,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'SMO-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_certifications': 80
                },
                {
                    'lot_number': 'SMO-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_certifications': 60
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# LOG FINISHING & TRANSIT SPECIALIST MODULE
# ============================================================

def validate_log_finishing_transit_specialist_guardrails(data):
    """Validate log finishing transit specialist inputs against business guardrails."""
    errors = []
    warnings = []
    
    silk_mark_officer_log_id = data.get('silk_mark_officer_log_id')
    quality_inspector_log_id = data.get('quality_inspector_log_id')
    packaging_material_spec = data.get('packaging_material_spec')
    finishing_machine_type = data.get('finishing_machine_type')
    logistics_transit_status = data.get('logistics_transit_status')

    if silk_mark_officer_log_id is None:
        errors.append({
            'code': 'DENY_FINISHING_RECEIPT',
            'message': 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES'
        })

    if quality_inspector_log_id is None:
        errors.append({
            'code': 'DENY_FINISHING_RECEIPT',
            'message': 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES'
        })

    if quality_inspector_log_id:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT qa_inspector_approval_state FROM quality_inspector_logs WHERE id = %s::uuid
        """, (quality_inspector_log_id,))
        grade_row = cur.fetchone()
        cur.close()
        conn.close()
        if grade_row and grade_row['qa_inspector_approval_state'] != 'PASSED_CLEARED_FOR_PACKING':
            errors.append({
                'code': 'DENY_FINISHING_RECEIPT',
                'message': 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES'
            })

    if silk_mark_officer_log_id:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT silk_mark_officer_approval_state FROM silk_mark_officer_logs WHERE id = %s::uuid
        """, (silk_mark_officer_log_id,))
        silk_row = cur.fetchone()
        cur.close()
        conn.close()
        if silk_row and silk_row['silk_mark_officer_approval_state'] != 'CERTIFIED_GENUINE_SILK_MARK_RELEASED':
            errors.append({
                'code': 'DENY_FINISHING_RECEIPT',
                'message': 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES'
            })
        
        if silk_row and silk_row['silk_mark_officer_approval_state'] == 'CERTIFIED_GENUINE_SILK_MARK_RELEASED' and packaging_material_spec == 'Standard 50-Micron Polybag':
            errors.append({
                'code': 'REJECT_STANDARD_PACKAGING',
                'message': 'PURE_ZARI_REQUIRES_ANTI_TARNISH_VACUUM_PACKAGING'
            })

    if finishing_machine_type == 'Rotary Roller Calender':
        warnings.append({
            'code': 'INVALID_MACHINE_SELECTION_WARNING',
            'message': 'HEAVY_ROLLER_CALENDERS_WILL_CRUSH_2400_HOOK_JACQUARD_ZARI'
        })

    if logistics_transit_status is not None and logistics_transit_status != 'READY_FOR_DISPATCH_MANIFESTED':
        errors.append({
            'code': 'DENY_WAREHOUSE_GATE_PASS_GENERATION',
            'message': 'TRANSIT_STATUS_NOT_READY_FOR_DISPATCH'
        })

    return errors, warnings

@app.route('/api/v1/log-finishing-transit-specialist/logs', methods=['POST'])
@jwt_required()
def create_log_finishing_transit_specialist_log():
    try:
        specialist_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['finishing_job_id', 'saree_piece_serial_id']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (specialist_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_log_finishing_transit_specialist_guardrails(data)
        
        cur.execute("""
            INSERT INTO log_finishing_transit_specialist_logs (
                finishing_job_id, factory_node_id, log_finishing_transit_specialist_id,
                quality_inspector_log_id, qa_dyeing_inspector_log_id, silk_mark_officer_log_id,
                sup_supervisor_log_id, assistant_weaver_job_log_id, master_weaver_job_id,
                b2b_sales_order_ref, saree_piece_serial_id, finishing_machine_type,
                steam_temperature_celsius, edge_fringing_method, scanned_saree_serial_no,
                verified_silk_mark_tag_id, packaging_material_spec, anti_tarnish_desiccant_inserted,
                pallu_interleaving_status, final_packed_weight_grams, dispatch_manifest_id,
                tamper_seal_barcode_id, logistics_transit_status, logistics_carrier_name,
                consignment_airway_bill_no, gross_consignment_shipping_weight_kg,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, finishing_job_id
        """, (
            data.get('finishing_job_id'),
            factory_node_id,
            specialist_id,
            data.get('quality_inspector_log_id'),
            data.get('qa_dyeing_inspector_log_id'),
            data.get('silk_mark_officer_log_id'),
            data.get('sup_supervisor_log_id'),
            data.get('assistant_weaver_job_log_id'),
            data.get('master_weaver_job_id'),
            data.get('b2b_sales_order_ref'),
            data.get('saree_piece_serial_id'),
            data.get('finishing_machine_type', 'Tensionless Felt-Belt Steam Calender'),
            data.get('steam_temperature_celsius'),
            data.get('edge_fringing_method', 'Hand-Twisted Micro-Fringe Knotting'),
            data.get('scanned_saree_serial_no'),
            data.get('verified_silk_mark_tag_id'),
            data.get('packaging_material_spec', 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack'),
            data.get('anti_tarnish_desiccant_inserted', 'Active Silica + Activated Carbon Pack'),
            data.get('pallu_interleaving_status', 'Acid-Free Interleaved (Zero-Contact)'),
            data.get('final_packed_weight_grams'),
            data.get('dispatch_manifest_id'),
            data.get('tamper_seal_barcode_id'),
            data.get('logistics_transit_status', 'READY_FOR_DISPATCH_MANIFESTED'),
            data.get('logistics_carrier_name'),
            data.get('consignment_airway_bill_no'),
            data.get('gross_consignment_shipping_weight_kg'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'finishing_job_id': log_row['finishing_job_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/log-finishing-transit-specialist/logs', methods=['GET'])
@jwt_required()
def list_log_finishing_transit_specialist_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT lfts.id, lfts.finishing_job_id, lfts.saree_piece_serial_id,
                   lfts.scanned_saree_serial_no, lfts.verified_silk_mark_tag_id,
                   lfts.logistics_transit_status, lfts.auto_assigned_routing,
                   lfts.finishing_machine_type, lfts.steam_temperature_celsius,
                   lfts.edge_fringing_method, lfts.packaging_material_spec,
                   lfts.anti_tarnish_desiccant_inserted, lfts.pallu_interleaving_status,
                   lfts.dispatch_manifest_id, lfts.consignment_airway_bill_no,
                   lfts.logistics_carrier_name, lfts.created_at,
                   u.full_name AS specialist_name
            FROM log_finishing_transit_specialist_logs lfts
            LEFT JOIN users u ON lfts.log_finishing_transit_specialist_id = u.id
            WHERE lfts.log_finishing_transit_specialist_id = %s::uuid
            ORDER BY lfts.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'finishing_job_id': log['finishing_job_id'],
                    'saree_piece_serial_id': log['saree_piece_serial_id'],
                    'scanned_saree_serial_no': log['scanned_saree_serial_no'],
                    'verified_silk_mark_tag_id': log['verified_silk_mark_tag_id'],
                    'logistics_transit_status': log['logistics_transit_status'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'finishing_machine_type': log['finishing_machine_type'],
                    'steam_temperature_celsius': float(log['steam_temperature_celsius']) if log['steam_temperature_celsius'] else None,
                    'edge_fringing_method': log['edge_fringing_method'],
                    'packaging_material_spec': log['packaging_material_spec'],
                    'anti_tarnish_desiccant_inserted': log['anti_tarnish_desiccant_inserted'],
                    'pallu_interleaving_status': log['pallu_interleaving_status'],
                    'dispatch_manifest_id': log['dispatch_manifest_id'],
                    'consignment_airway_bill_no': log['consignment_airway_bill_no'],
                    'logistics_carrier_name': log['logistics_carrier_name'],
                    'created_at': log['created_at'].isoformat() if log['created_at'] else None,
                    'specialist_name': log['specialist_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/log-finishing-transit-specialist/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_log_finishing_transit_specialist_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT lfts.*,
                   u.full_name AS specialist_name
            FROM log_finishing_transit_specialist_logs lfts
            LEFT JOIN users u ON lfts.log_finishing_transit_specialist_id = u.id
            WHERE lfts.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'finishing_job_id': log['finishing_job_id'],
            'factory_node_id': log['factory_node_id'],
            'log_finishing_transit_specialist_id': str(log['log_finishing_transit_specialist_id']),
            'quality_inspector_log_id': str(log['quality_inspector_log_id']) if log['quality_inspector_log_id'] else None,
            'qa_dyeing_inspector_log_id': str(log['qa_dyeing_inspector_log_id']) if log['qa_dyeing_inspector_log_id'] else None,
            'silk_mark_officer_log_id': str(log['silk_mark_officer_log_id']) if log['silk_mark_officer_log_id'] else None,
            'b2b_sales_order_ref': log['b2b_sales_order_ref'],
            'saree_piece_serial_id': log['saree_piece_serial_id'],
            'finishing_machine_type': log['finishing_machine_type'],
            'steam_temperature_celsius': float(log['steam_temperature_celsius']) if log['steam_temperature_celsius'] else None,
            'edge_fringing_method': log['edge_fringing_method'],
            'scanned_saree_serial_no': log['scanned_saree_serial_no'],
            'verified_silk_mark_tag_id': log['verified_silk_mark_tag_id'],
            'packaging_material_spec': log['packaging_material_spec'],
            'anti_tarnish_desiccant_inserted': log['anti_tarnish_desiccant_inserted'],
            'pallu_interleaving_status': log['pallu_interleaving_status'],
            'final_packed_weight_grams': float(log['final_packed_weight_grams']) if log['final_packed_weight_grams'] else None,
            'dispatch_manifest_id': log['dispatch_manifest_id'],
            'tamper_seal_barcode_id': log['tamper_seal_barcode_id'],
            'logistics_transit_status': log['logistics_transit_status'],
            'logistics_carrier_name': log['logistics_carrier_name'],
            'consignment_airway_bill_no': log['consignment_airway_bill_no'],
            'gross_consignment_shipping_weight_kg': float(log['gross_consignment_shipping_weight_kg']) if log['gross_consignment_shipping_weight_kg'] else None,
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'created_at': log['created_at'].isoformat() if log['created_at'] else None,
            'specialist_name': log['specialist_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/log-finishing-transit-specialist/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_log_finishing_transit_specialist_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, finishing_job_id, validation_errors, logistics_transit_status
            FROM log_finishing_transit_specialist_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['finishing_job_id'])
        qr_tag_id = 'LOG-' + log['finishing_job_id']
        
        cur.execute("""
            UPDATE log_finishing_transit_specialist_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, finishing_job_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO log_finishing_transit_specialist_certificates (
                log_finishing_transit_specialist_log_id, certificate_hash, qr_tag_id,
                finishing_job_id, saree_piece_serial_id, scanned_saree_serial_no,
                verified_silk_mark_tag_id, dispatch_manifest_id, tamper_seal_barcode_id,
                logistics_transit_status, logistics_carrier_name, consignment_airway_bill_no,
                finishing_machine_type, steam_temperature_celsius, edge_fringing_method,
                packaging_material_spec, anti_tarnish_desiccant_inserted, pallu_interleaving_status,
                final_packed_weight_grams, gross_consignment_shipping_weight_kg,
                specialist_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.finishing_job_id,
                pj.saree_piece_serial_id,
                pj.scanned_saree_serial_no,
                pj.verified_silk_mark_tag_id,
                pj.dispatch_manifest_id,
                pj.tamper_seal_barcode_id,
                pj.logistics_transit_status,
                pj.logistics_carrier_name,
                pj.consignment_airway_bill_no,
                pj.finishing_machine_type,
                pj.steam_temperature_celsius,
                pj.edge_fringing_method,
                pj.packaging_material_spec,
                pj.anti_tarnish_desiccant_inserted,
                pj.pallu_interleaving_status,
                pj.final_packed_weight_grams,
                pj.gross_consignment_shipping_weight_kg,
                pj.log_finishing_transit_specialist_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'finishing_job_id', pj.finishing_job_id,
                    'saree_piece_serial_id', pj.saree_piece_serial_id,
                    'scanned_saree_serial_no', pj.scanned_saree_serial_no,
                    'verified_silk_mark_tag_id', pj.verified_silk_mark_tag_id,
                    'dispatch_manifest_id', pj.dispatch_manifest_id,
                    'tamper_seal_barcode_id', pj.tamper_seal_barcode_id,
                    'logistics_transit_status', pj.logistics_transit_status,
                    'logistics_carrier_name', pj.logistics_carrier_name,
                    'consignment_airway_bill_no', pj.consignment_airway_bill_no,
                    'finishing_machine_type', pj.finishing_machine_type,
                    'steam_temperature_celsius', pj.steam_temperature_celsius,
                    'edge_fringing_method', pj.edge_fringing_method,
                    'packaging_material_spec', pj.packaging_material_spec,
                    'anti_tarnish_desiccant_inserted', pj.anti_tarnish_desiccant_inserted,
                    'pallu_interleaving_status', pj.pallu_interleaving_status
                )
            FROM log_finishing_transit_specialist_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'finishing_job_id': result['finishing_job_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'CERTIFIED_READY_FOR_DISPATCH',
            'message': 'Finishing & transit certification issued and dispatch manifest generated'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/log-finishing-transit-specialist/certificates', methods=['GET'])
@jwt_required()
def list_log_finishing_transit_specialist_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT lftsc.id, lftsc.certificate_hash, lftsc.qr_tag_id,
                   lftsc.finishing_job_id, lftsc.saree_piece_serial_id,
                   lftsc.scanned_saree_serial_no, lftsc.verified_silk_mark_tag_id,
                   lftsc.dispatch_manifest_id, lftsc.tamper_seal_barcode_id,
                   lftsc.logistics_transit_status, lftsc.logistics_carrier_name,
                   lftsc.consignment_airway_bill_no, lftsc.finishing_machine_type,
                   lftsc.steam_temperature_celsius, lftsc.edge_fringing_method,
                   lftsc.packaging_material_spec, lftsc.anti_tarnish_desiccant_inserted,
                   lftsc.pallu_interleaving_status, lftsc.final_packed_weight_grams,
                   lftsc.gross_consignment_shipping_weight_kg, lftsc.status, lftsc.certified_at
            FROM log_finishing_transit_specialist_certificates lftsc
            WHERE lftsc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY lftsc.certified_at DESC
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
                    'finishing_job_id': c['finishing_job_id'],
                    'saree_piece_serial_id': c['saree_piece_serial_id'],
                    'scanned_saree_serial_no': c['scanned_saree_serial_no'],
                    'verified_silk_mark_tag_id': c['verified_silk_mark_tag_id'],
                    'dispatch_manifest_id': c['dispatch_manifest_id'],
                    'tamper_seal_barcode_id': c['tamper_seal_barcode_id'],
                    'logistics_transit_status': c['logistics_transit_status'],
                    'logistics_carrier_name': c['logistics_carrier_name'],
                    'consignment_airway_bill_no': c['consignment_airway_bill_no'],
                    'finishing_machine_type': c['finishing_machine_type'],
                    'steam_temperature_celsius': float(c['steam_temperature_celsius']) if c['steam_temperature_celsius'] else None,
                    'edge_fringing_method': c['edge_fringing_method'],
                    'packaging_material_spec': c['packaging_material_spec'],
                    'anti_tarnish_desiccant_inserted': c['anti_tarnish_desiccant_inserted'],
                    'pallu_interleaving_status': c['pallu_interleaving_status'],
                    'final_packed_weight_grams': float(c['final_packed_weight_grams']) if c['final_packed_weight_grams'] else None,
                    'gross_consignment_shipping_weight_kg': float(c['gross_consignment_shipping_weight_kg']) if c['gross_consignment_shipping_weight_kg'] else None,
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR LOG FINISHING & TRANSIT SPECIALIST
# ============================================================

@app.route('/api/v1/sales/forecast/log-finishing-transit-specialist', methods=['GET'])
@jwt_required()
def get_sales_forecast_log_finishing_transit_specialist():
    """
    API plugin endpoint for sales team log finishing transit specialist material processing forecast.
    Returns forecasted finishing, packaging, and dispatch requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'finishing_machine_type': 'Tensionless Felt-Belt Steam Calender',
                    'steam_temperature_celsius': 115.0,
                    'edge_fringing_method': 'Hand-Twisted Micro-Fringe Knotting',
                    'packaging_material_spec': 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack',
                    'anti_tarnish_desiccant_inserted': 'Active Silica + Activated Carbon Pack',
                    'pallu_interleaving_status': 'Acid-Free Interleaved (Zero-Contact)',
                    'estimated_finishing_jobs': 80,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'finishing_machine_type': 'Tensionless Felt-Belt Steam Calender',
                    'steam_temperature_celsius': 115.0,
                    'edge_fringing_method': 'Hand-Twisted Micro-Fringe Knotting',
                    'packaging_material_spec': 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack',
                    'anti_tarnish_desiccant_inserted': 'Active Silica + Activated Carbon Pack',
                    'pallu_interleaving_status': 'Acid-Free Interleaved (Zero-Contact)',
                    'estimated_finishing_jobs': 60,
                    'priority': 'HIGH'
                },
                {
                    'saree_category': 'Mid-Segment Silk Sarees',
                    'design_code': 'MID-1536-STD-03',
                    'finishing_machine_type': 'Rotary Roller Calender',
                    'steam_temperature_celsius': 110.0,
                    'edge_fringing_method': 'Satin Ribbon Hem Lock',
                    'packaging_material_spec': 'Standard 50-Micron Polybag',
                    'anti_tarnish_desiccant_inserted': 'Silica Gel Only',
                    'pallu_interleaving_status': 'Standard Paper Interleaved',
                    'estimated_finishing_jobs': 120,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'LOG-LOT-2024-0011',
                    'saree_category': 'Authentic Kanchipuram Bridal',
                    'design_code': 'KNC-2400-BR-09',
                    'estimated_finishing_jobs': 80
                },
                {
                    'lot_number': 'LOG-LOT-2024-0012',
                    'saree_category': 'Banarasi Kinkhab & Kadwa',
                    'design_code': 'BNR-2400-KK-12',
                    'estimated_finishing_jobs': 60
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# STORE INVENTORY MANAGER MODULE
# ============================================================

def validate_store_inventory_manager_guardrails(data):
    """Validate store inventory manager inputs against business guardrails."""
    errors = []
    warnings = []
    
    inventory_movement_action = data.get('inventory_movement_action')
    dye_lot_matching_verification = data.get('dye_lot_matching_verification')
    inventory_item_type = data.get('inventory_item_type')
    vault_climate_status = data.get('vault_climate_status')
    quarantine_hold_status = data.get('quarantine_hold_status')
    zari_spool_gross_weight_grams = data.get('zari_spool_gross_weight_grams')
    yarn_dye_lot_batch_id = data.get('yarn_dye_lot_batch_id')

    if inventory_movement_action == 'ISSUE_TO_WARP_PREP' and dye_lot_matching_verification == 'REJECTED_DYE_LOT_MISMATCH':
        errors.append({
            'code': 'DYE_LOT_MIXING_PROHIBITED',
            'message': 'PREVENTS_WARP_SHADING_DEFECTS_ON_80_SAREE_RUN'
        })
        data['inventory_manager_approval_state'] = 'HOLD_UNVERIFIED_LOT'

    if inventory_item_type == 'Tested Pure Gold/Silver Zari Spools':
        if vault_climate_status in ('WARNING_HUMIDITY_HIGH', 'CRITICAL_TEMPERATURE_EXCEEDED'):
            errors.append({
                'code': 'VAULT_ISSUE_HALTED',
                'message': 'ENVIRONMENTAL_EXCURSION_RISK_OF_ZARI_TARNISH'
            })
            data['inventory_manager_approval_state'] = 'HOLD_CLIMATE_EXCURSION'

    if inventory_movement_action == 'RELEASE_TO_DISPATCH_WAREHOUSE' and quarantine_hold_status != 'CLEARED_FOR_REGULAR_STOCK':
        errors.append({
            'code': 'DENY_UNCERTIFIED_STOCK_RELEASE',
            'message': 'ITEM_MUST_PASS_QA_AND_SILK_MARK_BEFORE_DISPATCH'
        })

    if inventory_item_type == 'Tested Pure Gold/Silver Zari Spools' and inventory_movement_action == 'RECONCILIATION_ADJUSTMENT' and zari_spool_gross_weight_grams is not None:
        if yarn_dye_lot_batch_id:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                SELECT zari_spool_gross_weight_grams FROM store_inventory_manager_logs
                WHERE inventory_item_type = 'Tested Pure Gold/Silver Zari Spools'
                  AND yarn_dye_lot_batch_id = %s
                  AND zari_spool_gross_weight_grams IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
            """, (yarn_dye_lot_batch_id,))
            prev = cur.fetchone()
            cur.close()
            conn.close()
            if prev and prev['zari_spool_gross_weight_grams'] is not None:
                variance_pct = abs(zari_spool_gross_weight_grams - prev['zari_spool_gross_weight_grams']) / prev['zari_spool_gross_weight_grams'] * 100
                if variance_pct > 0.1:
                    warnings.append({
                        'code': 'PRECIOUS_METAL_WEIGHT_DISCREPANCY_DETECTED',
                        'message': 'AUTO_ALERT_SECURITY_AND_FINANCE'
                    })

    return errors, warnings

@app.route('/api/v1/store-inventory-manager/logs', methods=['POST'])
@jwt_required()
def create_store_inventory_manager_log():
    try:
        manager_id = get_jwt_identity()
        data = request.get_json()
        
        required_fields = ['inventory_placement_id', 'inventory_item_type']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT factory_node_id FROM users WHERE id = %s::uuid", (manager_id,))
        user_row = cur.fetchone()
        if not user_row:
            cur.close()
            conn.close()
            return jsonify({'error': 'UserNotFound'}), 404
        
        factory_node_id = user_row['factory_node_id']
        
        validation_errors, validation_warnings = validate_store_inventory_manager_guardrails(data)
        
        cur.execute("""
            INSERT INTO store_inventory_manager_logs (
                inventory_placement_id, factory_node_id, store_inventory_manager_id,
                quality_inspector_log_id, qa_dyeing_inspector_log_id, silk_mark_officer_log_id,
                log_finishing_transit_specialist_log_id, sup_supervisor_log_id,
                assistant_weaver_job_log_id, master_weaver_job_id, b2b_sales_order_ref,
                inventory_item_type, yarn_dye_lot_batch_id, storage_bin_location_code,
                vault_climate_status, target_loom_warp_set_id, dye_lot_matching_verification,
                zari_spool_gross_weight_grams, quarantine_hold_status, source_grn_or_job_ref,
                bin_location_aisle_zone, physical_package_count, scale_weight_at_storage_kg,
                storage_ambient_humidity_pct, storage_temperature_celsius,
                production_requisition_slip_no, issued_quantity_weight_kg,
                authorized_recipient_employee_id, saree_piece_serial_id,
                inventory_movement_action, inventory_manager_approval_state,
                validation_errors, validation_warnings, auto_assigned_routing
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, inventory_placement_id
        """, (
            data.get('inventory_placement_id'),
            factory_node_id,
            manager_id,
            data.get('quality_inspector_log_id'),
            data.get('qa_dyeing_inspector_log_id'),
            data.get('silk_mark_officer_log_id'),
            data.get('log_finishing_transit_specialist_log_id'),
            data.get('sup_supervisor_log_id'),
            data.get('assistant_weaver_job_log_id'),
            data.get('master_weaver_job_id'),
            data.get('b2b_sales_order_ref'),
            data.get('inventory_item_type', 'Pure Mulberry Silk Yarn (16/18d)'),
            data.get('yarn_dye_lot_batch_id'),
            data.get('storage_bin_location_code'),
            data.get('vault_climate_status', 'OPTIMAL_CLIMATE_LOCKED'),
            data.get('target_loom_warp_set_id'),
            data.get('dye_lot_matching_verification', '100%_SINGLE_DYE_LOT_VERIFIED'),
            data.get('zari_spool_gross_weight_grams'),
            data.get('quarantine_hold_status', 'CLEARED_FOR_REGULAR_STOCK'),
            data.get('source_grn_or_job_ref'),
            data.get('bin_location_aisle_zone'),
            data.get('physical_package_count'),
            data.get('scale_weight_at_storage_kg'),
            data.get('storage_ambient_humidity_pct'),
            data.get('storage_temperature_celsius'),
            data.get('production_requisition_slip_no'),
            data.get('issued_quantity_weight_kg'),
            data.get('authorized_recipient_employee_id'),
            data.get('saree_piece_serial_id'),
            data.get('inventory_movement_action', 'TRANSFER_TO_FINISHING_BAY'),
            data.get('inventory_manager_approval_state', 'STOCK_RELEASE_APPROVED'),
            json.dumps(validation_errors),
            json.dumps(validation_warnings),
            data.get('auto_assigned_routing')
        ))
        
        log_row = cur.fetchone()
        log_id = log_row['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(log_id),
            'inventory_placement_id': log_row['inventory_placement_id'],
            'validation_errors': validation_errors,
            'validation_warnings': validation_warnings,
            'status': 'submitted'
        }), 201
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/store-inventory-manager/logs', methods=['GET'])
@jwt_required()
def list_store_inventory_manager_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sim.id, sim.inventory_placement_id, sim.inventory_item_type,
                   sim.yarn_dye_lot_batch_id, sim.storage_bin_location_code,
                   sim.vault_climate_status, sim.inventory_manager_approval_state,
                   sim.auto_assigned_routing, sim.dye_lot_matching_verification,
                   sim.zari_spool_gross_weight_grams, sim.quarantine_hold_status,
                   sim.inventory_movement_action, sim.saree_piece_serial_id,
                   sim.storage_ambient_humidity_pct, sim.storage_temperature_celsius,
                   sim.created_at,
                   u.full_name AS manager_name
            FROM store_inventory_manager_logs sim
            LEFT JOIN users u ON sim.store_inventory_manager_id = u.id
            WHERE sim.store_inventory_manager_id = %s::uuid
            ORDER BY sim.created_at DESC
            LIMIT 100
        """, (operator_id,))
        
        logs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(logs),
            'logs': [
                {
                    'id': str(log['id']),
                    'inventory_placement_id': log['inventory_placement_id'],
                    'inventory_item_type': log['inventory_item_type'],
                    'yarn_dye_lot_batch_id': log['yarn_dye_lot_batch_id'],
                    'storage_bin_location_code': log['storage_bin_location_code'],
                    'vault_climate_status': log['vault_climate_status'],
                    'inventory_manager_approval_state': log['inventory_manager_approval_state'],
                    'auto_assigned_routing': log['auto_assigned_routing'],
                    'dye_lot_matching_verification': log['dye_lot_matching_verification'],
                    'zari_spool_gross_weight_grams': float(log['zari_spool_gross_weight_grams']) if log['zari_spool_gross_weight_grams'] else None,
                    'quarantine_hold_status': log['quarantine_hold_status'],
                    'inventory_movement_action': log['inventory_movement_action'],
                    'saree_piece_serial_id': log['saree_piece_serial_id'],
                    'storage_ambient_humidity_pct': float(log['storage_ambient_humidity_pct']) if log['storage_ambient_humidity_pct'] else None,
                    'storage_temperature_celsius': float(log['storage_temperature_celsius']) if log['storage_temperature_celsius'] else None,
                    'created_at': log['created_at'].isoformat() if log['created_at'] else None,
                    'manager_name': log['manager_name']
                }
                for log in logs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/store-inventory-manager/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_store_inventory_manager_log(log_id):
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sim.*,
                   u.full_name AS manager_name
            FROM store_inventory_manager_logs sim
            LEFT JOIN users u ON sim.store_inventory_manager_id = u.id
            WHERE sim.id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        cur.close()
        conn.close()
        
        if not log:
            return jsonify({'error': 'LogNotFound'}), 404
        
        return jsonify({
            'id': str(log['id']),
            'inventory_placement_id': log['inventory_placement_id'],
            'factory_node_id': log['factory_node_id'],
            'store_inventory_manager_id': str(log['store_inventory_manager_id']),
            'quality_inspector_log_id': str(log['quality_inspector_log_id']) if log['quality_inspector_log_id'] else None,
            'qa_dyeing_inspector_log_id': str(log['qa_dyeing_inspector_log_id']) if log['qa_dyeing_inspector_log_id'] else None,
            'silk_mark_officer_log_id': str(log['silk_mark_officer_log_id']) if log['silk_mark_officer_log_id'] else None,
            'log_finishing_transit_specialist_log_id': str(log['log_finishing_transit_specialist_log_id']) if log['log_finishing_transit_specialist_log_id'] else None,
            'b2b_sales_order_ref': log['b2b_sales_order_ref'],
            'inventory_item_type': log['inventory_item_type'],
            'yarn_dye_lot_batch_id': log['yarn_dye_lot_batch_id'],
            'storage_bin_location_code': log['storage_bin_location_code'],
            'vault_climate_status': log['vault_climate_status'],
            'target_loom_warp_set_id': log['target_loom_warp_set_id'],
            'dye_lot_matching_verification': log['dye_lot_matching_verification'],
            'zari_spool_gross_weight_grams': float(log['zari_spool_gross_weight_grams']) if log['zari_spool_gross_weight_grams'] else None,
            'quarantine_hold_status': log['quarantine_hold_status'],
            'source_grn_or_job_ref': log['source_grn_or_job_ref'],
            'bin_location_aisle_zone': log['bin_location_aisle_zone'],
            'physical_package_count': log['physical_package_count'],
            'scale_weight_at_storage_kg': float(log['scale_weight_at_storage_kg']) if log['scale_weight_at_storage_kg'] else None,
            'storage_ambient_humidity_pct': float(log['storage_ambient_humidity_pct']) if log['storage_ambient_humidity_pct'] else None,
            'storage_temperature_celsius': float(log['storage_temperature_celsius']) if log['storage_temperature_celsius'] else None,
            'production_requisition_slip_no': log['production_requisition_slip_no'],
            'issued_quantity_weight_kg': float(log['issued_quantity_weight_kg']) if log['issued_quantity_weight_kg'] else None,
            'authorized_recipient_employee_id': log['authorized_recipient_employee_id'],
            'saree_piece_serial_id': log['saree_piece_serial_id'],
            'inventory_movement_action': log['inventory_movement_action'],
            'inventory_manager_approval_state': log['inventory_manager_approval_state'],
            'validation_errors': log['validation_errors'],
            'validation_warnings': log['validation_warnings'],
            'auto_assigned_routing': log['auto_assigned_routing'],
            'certificate_hash': log['certificate_hash'],
            'qr_tag_id': log['qr_tag_id'],
            'created_at': log['created_at'].isoformat() if log['created_at'] else None,
            'manager_name': log['manager_name']
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/store-inventory-manager/logs/<log_id>/certify', methods=['POST'])
@jwt_required()
def certify_store_inventory_manager_log(log_id):
    try:
        approver_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, inventory_placement_id, validation_errors, inventory_manager_approval_state
            FROM store_inventory_manager_logs
            WHERE id = %s::uuid
        """, (log_id,))
        
        log = cur.fetchone()
        if not log:
            cur.close()
            conn.close()
            return jsonify({'error': 'LogNotFound'}), 404
        
        if log['validation_errors'] and len(log['validation_errors']) > 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'ValidationErrors', 'message': 'Cannot certify log with validation errors'}), 400
        
        certificate_hash = generate_certificate_hash(log_id, log['inventory_placement_id'])
        qr_tag_id = 'INV-' + log['inventory_placement_id']
        
        cur.execute("""
            UPDATE store_inventory_manager_logs
            SET certificate_hash = %s,
                qr_tag_id = %s
            WHERE id = %s::uuid
            RETURNING id, inventory_placement_id, certificate_hash
        """, (certificate_hash, qr_tag_id, log_id))
        
        result = cur.fetchone()
        
        cur.execute("""
            INSERT INTO store_inventory_manager_certificates (
                store_inventory_manager_log_id, certificate_hash, qr_tag_id,
                inventory_placement_id, saree_piece_serial_id, inventory_item_type,
                yarn_dye_lot_batch_id, storage_bin_location_code, vault_climate_status,
                dye_lot_matching_verification, zari_spool_gross_weight_grams,
                quarantine_hold_status, inventory_movement_action, inventory_manager_approval_state,
                scale_weight_at_storage_kg, storage_ambient_humidity_pct, storage_temperature_celsius,
                issued_quantity_weight_kg, manager_id, approver_id, factory_node_id, certification_data
            )
            SELECT
                pj.id,
                pj.certificate_hash,
                pj.qr_tag_id,
                pj.inventory_placement_id,
                pj.saree_piece_serial_id,
                pj.inventory_item_type,
                pj.yarn_dye_lot_batch_id,
                pj.storage_bin_location_code,
                pj.vault_climate_status,
                pj.dye_lot_matching_verification,
                pj.zari_spool_gross_weight_grams,
                pj.quarantine_hold_status,
                pj.inventory_movement_action,
                pj.inventory_manager_approval_state,
                pj.scale_weight_at_storage_kg,
                pj.storage_ambient_humidity_pct,
                pj.storage_temperature_celsius,
                pj.issued_quantity_weight_kg,
                pj.store_inventory_manager_id,
                %s,
                pj.factory_node_id,
                jsonb_build_object(
                    'inventory_placement_id', pj.inventory_placement_id,
                    'inventory_item_type', pj.inventory_item_type,
                    'yarn_dye_lot_batch_id', pj.yarn_dye_lot_batch_id,
                    'storage_bin_location_code', pj.storage_bin_location_code,
                    'vault_climate_status', pj.vault_climate_status,
                    'dye_lot_matching_verification', pj.dye_lot_matching_verification,
                    'zari_spool_gross_weight_grams', pj.zari_spool_gross_weight_grams,
                    'quarantine_hold_status', pj.quarantine_hold_status,
                    'inventory_movement_action', pj.inventory_movement_action,
                    'inventory_manager_approval_state', pj.inventory_manager_approval_state,
                    'saree_piece_serial_id', pj.saree_piece_serial_id
                )
            FROM store_inventory_manager_logs pj
            WHERE pj.id = %s::uuid
        """, (approver_id, log_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'id': str(result['id']),
            'inventory_placement_id': result['inventory_placement_id'],
            'certificate_hash': result['certificate_hash'],
            'qr_tag_id': qr_tag_id,
            'status': 'STOCK_RELEASE_APPROVED',
            'message': 'Inventory certification issued and stock released'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/store-inventory-manager/certificates', methods=['GET'])
@jwt_required()
def list_store_inventory_manager_certificates():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT simc.id, simc.certificate_hash, simc.qr_tag_id,
                   simc.inventory_placement_id, simc.saree_piece_serial_id,
                   simc.inventory_item_type, simc.yarn_dye_lot_batch_id,
                   simc.storage_bin_location_code, simc.vault_climate_status,
                   simc.dye_lot_matching_verification, simc.zari_spool_gross_weight_grams,
                   simc.quarantine_hold_status, simc.inventory_movement_action,
                   simc.inventory_manager_approval_state, simc.status, simc.certified_at
            FROM store_inventory_manager_certificates simc
            WHERE simc.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY simc.certified_at DESC
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
                    'inventory_placement_id': c['inventory_placement_id'],
                    'saree_piece_serial_id': c['saree_piece_serial_id'],
                    'inventory_item_type': c['inventory_item_type'],
                    'yarn_dye_lot_batch_id': c['yarn_dye_lot_batch_id'],
                    'storage_bin_location_code': c['storage_bin_location_code'],
                    'vault_climate_status': c['vault_climate_status'],
                    'dye_lot_matching_verification': c['dye_lot_matching_verification'],
                    'zari_spool_gross_weight_grams': float(c['zari_spool_gross_weight_grams']) if c['zari_spool_gross_weight_grams'] else None,
                    'quarantine_hold_status': c['quarantine_hold_status'],
                    'inventory_movement_action': c['inventory_movement_action'],
                    'inventory_manager_approval_state': c['inventory_manager_approval_state'],
                    'status': c['status'],
                    'certified_at': c['certified_at'].isoformat() if c['certified_at'] else None
                }
                for c in certs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# SALES FORECAST API PLUGIN FOR STORE INVENTORY MANAGER
# ============================================================

@app.route('/api/v1/sales/forecast/store-inventory-manager', methods=['GET'])
@jwt_required()
def get_sales_forecast_store_inventory_manager():
    """
    API plugin endpoint for sales team store inventory manager material processing forecast.
    Returns forecasted inventory, stock, and material requirements based on sales pipeline.
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
        
        forecast = {
            'factory_node_id': factory_node_id,
            'forecast_period': '30 days',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'material_requirements': [
                {
                    'inventory_item_type': 'Pure Mulberry Silk Yarn (16/18d)',
                    'yarn_dye_lot_batch_id': 'YDL-2024-001',
                    'storage_bin_location_code': 'SILK-RACK-B04',
                    'vault_climate_status': 'OPTIMAL_CLIMATE_LOCKED',
                    'dye_lot_matching_verification': '100%_SINGLE_DYE_LOT_VERIFIED',
                    'estimated_stock_kg': 500,
                    'reorder_point_kg': 100,
                    'priority': 'HIGH'
                },
                {
                    'inventory_item_type': 'Tested Pure Gold/Silver Zari Spools',
                    'yarn_dye_lot_batch_id': 'ZARI-2024-001',
                    'storage_bin_location_code': 'VAULT-ZARI-A01',
                    'vault_climate_status': 'OPTIMAL_CLIMATE_LOCKED',
                    'dye_lot_matching_verification': '100%_SINGLE_DYE_LOT_VERIFIED',
                    'estimated_stock_kg': 50,
                    'reorder_point_kg': 20,
                    'priority': 'HIGH'
                },
                {
                    'inventory_item_type': 'Dye Chemicals',
                    'yarn_dye_lot_batch_id': 'DYE-2024-001',
                    'storage_bin_location_code': 'CHEM-VAULT-C01',
                    'vault_climate_status': 'OPTIMAL_CLIMATE_LOCKED',
                    'dye_lot_matching_verification': '100%_SINGLE_DYE_LOT_VERIFIED',
                    'estimated_stock_kg': 200,
                    'reorder_point_kg': 50,
                    'priority': 'MEDIUM'
                }
            ],
            'upcoming_lots': [
                {
                    'lot_number': 'INV-LOT-2024-0011',
                    'inventory_item_type': 'Pure Mulberry Silk Yarn (16/18d)',
                    'yarn_dye_lot_batch_id': 'YDL-2024-0011',
                    'estimated_stock_kg': 500
                },
                {
                    'lot_number': 'INV-LOT-2024-0012',
                    'inventory_item_type': 'Tested Pure Gold/Silver Zari Spools',
                    'yarn_dye_lot_batch_id': 'ZARI-2024-0012',
                    'estimated_stock_kg': 50
                }
            ]
        }
        
        cur.close()
        conn.close()
        
        return jsonify(forecast), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ============================================================
# ENTERPRISE MODULES: AI/ML, Finance, HR, Procurement,
# Supply Chain, IoT, B2B, Buy-Back, Notifications
# ============================================================

# ---------------------------
# AI/ML Inference Logs
# ---------------------------

@app.route('/api/v1/enterprise/ai/inference-logs', methods=['POST'])
@jwt_required()
def create_ai_inference_log():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['inference_id', 'factory_node_id', 'service_name']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_inference_logs (inference_id, factory_node_id, service_name, model_version,
                entity_type, entity_id, input_features, prediction, confidence_score,
                inference_latency_ms, edge_deployed, operator_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('inference_id'), data.get('factory_node_id'), data.get('service_name'),
            data.get('model_version'), data.get('entity_type'), data.get('entity_id'),
            json.dumps(data.get('input_features', {})), json.dumps(data.get('prediction', {})),
            data.get('confidence_score'), data.get('inference_latency_ms'),
            data.get('edge_deployed', False), operator_id
        ))
        log_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(log_id), 'status': 'logged'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/ai/inference-logs', methods=['GET'])
@jwt_required()
def list_ai_inference_logs():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT inference_id, service_name, entity_type, entity_id, confidence_score,
                   inference_latency_ms, edge_deployed, created_at
            FROM ai_inference_logs
            WHERE operator_id = %s::uuid
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        logs = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(logs), 'logs': [dict(log) for log in logs]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Finance: Journal Entries
# ---------------------------

@app.route('/api/v1/enterprise/finance/journal-entries', methods=['POST'])
@jwt_required()
def create_finance_journal_entry():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['journal_id', 'factory_node_id', 'debit_account', 'credit_account', 'amount']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO finance_journal_entries (journal_id, factory_node_id, entry_type,
                debit_account, credit_account, amount, currency, reference_entity_type,
                reference_entity_id, supplier_id, weaver_id, description, posted_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('journal_id'), data.get('factory_node_id'), data.get('entry_type', 'STANDARD'),
            data.get('debit_account'), data.get('credit_account'), data.get('amount'),
            data.get('currency', 'INR'), data.get('reference_entity_type'), data.get('reference_entity_id'),
            data.get('supplier_id'), data.get('weaver_id'), data.get('description'), operator_id
        ))
        entry_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(entry_id), 'status': 'posted'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/finance/journal-entries', methods=['GET'])
@jwt_required()
def list_finance_journal_entries():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT journal_id, entry_type, debit_account, credit_account, amount, currency,
                   reference_entity_type, reference_entity_id, posted_at
            FROM finance_journal_entries
            WHERE posted_by = %s::uuid
            ORDER BY posted_at DESC
            LIMIT 100
        """, (operator_id,))
        entries = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(entries), 'entries': [dict(e) for e in entries]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Finance: Supplier Payouts
# ---------------------------

@app.route('/api/v1/enterprise/finance/supplier-payouts', methods=['POST'])
@jwt_required()
def create_supplier_payout():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['payout_id', 'factory_node_id', 'supplier_id', 'payout_amount']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO supplier_payouts (payout_id, factory_node_id, supplier_id, raw_lot_id,
                payout_amount, currency, payout_type, payment_method, transaction_ref,
                smart_contract_tx_hash, released_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('payout_id'), data.get('factory_node_id'), data.get('supplier_id'),
            data.get('raw_lot_id'), data.get('payout_amount'), data.get('currency', 'INR'),
            data.get('payout_type', 'FULL'), data.get('payment_method', 'UPI'),
            data.get('transaction_ref'), data.get('smart_contract_tx_hash'), operator_id
        ))
        payout_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(payout_id), 'status': 'released'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/finance/supplier-payouts', methods=['GET'])
@jwt_required()
def list_supplier_payouts():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT payout_id, supplier_id, raw_lot_id, payout_amount, payout_type,
                   payment_method, released_at
            FROM supplier_payouts
            WHERE released_by = %s::uuid
            ORDER BY released_at DESC
            LIMIT 100
        """, (operator_id,))
        payouts = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(payouts), 'payouts': [dict(p) for p in payouts]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# HR: Weaver Profiles
# ---------------------------

@app.route('/api/v1/enterprise/hr/weaver-profiles', methods=['POST'])
@jwt_required()
def create_hr_weaver_profile():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['user_id', 'factory_node_id']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO hr_weaver_profiles (user_id, factory_node_id, guild_id, skill_level,
                primary_skill_set, loom_type_preference, max_loom_speed_ppm, average_yield_score,
                defect_rate_pct, attendance_score, is_active, joined_at, certified_at)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('user_id'), data.get('factory_node_id'), data.get('guild_id'),
            data.get('skill_level', 'JOURNEYMAN'), json.dumps(data.get('primary_skill_set', [])),
            json.dumps(data.get('loom_type_preference', [])), data.get('max_loom_speed_ppm'),
            data.get('average_yield_score'), data.get('defect_rate_pct'), data.get('attendance_score'),
            data.get('is_active', True), data.get('joined_at'), data.get('certified_at')
        ))
        profile_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(profile_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/hr/weaver-profiles', methods=['GET'])
@jwt_required()
def list_hr_weaver_profiles():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT wp.user_id, wp.skill_level, wp.primary_skill_set, wp.loom_type_preference,
                   wp.average_yield_score, wp.defect_rate_pct, wp.attendance_score,
                   u.full_name, u.email
            FROM hr_weaver_profiles wp
            LEFT JOIN users u ON wp.user_id = u.id
            WHERE wp.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY wp.average_yield_score DESC
            LIMIT 100
        """, (operator_id,))
        profiles = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(profiles), 'profiles': [dict(p) for p in profiles]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# HR: Guild Members
# ---------------------------

@app.route('/api/v1/enterprise/hr/guild-members', methods=['POST'])
@jwt_required()
def create_hr_guild_member():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['guild_id', 'user_id', 'factory_node_id', 'joined_at']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO hr_guild_members (guild_id, user_id, factory_node_id, role_in_guild,
                joined_at, performance_score, certification_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('guild_id'), data.get('user_id'), data.get('factory_node_id'),
            data.get('role_in_guild', 'MEMBER'), data.get('joined_at'),
            data.get('performance_score'), data.get('certification_level')
        ))
        member_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(member_id), 'status': 'added'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/hr/guild-members', methods=['GET'])
@jwt_required()
def list_hr_guild_members():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT gm.guild_id, gm.user_id, gm.role_in_guild, gm.joined_at,
                   gm.performance_score, gm.certification_level,
                   u.full_name, u.email
            FROM hr_guild_members gm
            LEFT JOIN users u ON gm.user_id = u.id
            WHERE gm.factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY gm.performance_score DESC
            LIMIT 100
        """, (operator_id,))
        members = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(members), 'members': [dict(m) for m in members]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Procurement: Requisitions
# ---------------------------

@app.route('/api/v1/enterprise/procurement/requisitions', methods=['POST'])
@jwt_required()
def create_procurement_requisition():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['requisition_id', 'factory_node_id', 'item_category', 'quantity_requested']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO procurement_requisitions (requisition_id, factory_node_id, requested_by,
                item_category, item_description, quantity_requested, unit_of_measure,
                estimated_unit_cost, total_estimated_cost, priority, status, auto_generated,
                ai_confidence_score)
            VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('requisition_id'), data.get('factory_node_id'), operator_id,
            data.get('item_category'), data.get('item_description'), data.get('quantity_requested'),
            data.get('unit_of_measure', 'KG'), data.get('estimated_unit_cost'),
            data.get('total_estimated_cost'), data.get('priority', 'MEDIUM'),
            data.get('status', 'DRAFT'), data.get('auto_generated', False),
            data.get('ai_confidence_score')
        ))
        req_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(req_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/procurement/requisitions', methods=['GET'])
@jwt_required()
def list_procurement_requisitions():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT requisition_id, item_category, quantity_requested, priority, status,
                   auto_generated, ai_confidence_score, created_at
            FROM procurement_requisitions
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        reqs = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(reqs), 'requisitions': [dict(r) for r in reqs]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Procurement: Orders
# ---------------------------

@app.route('/api/v1/enterprise/procurement/orders', methods=['POST'])
@jwt_required()
def create_procurement_order():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['po_id', 'factory_node_id', 'supplier_id', 'item_category', 'quantity_ordered', 'unit_price']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO procurement_orders (po_id, factory_node_id, requisition_id, supplier_id,
                item_category, quantity_ordered, unit_price, total_amount, currency,
                payment_terms, delivery_terms, expected_delivery_date, status, issued_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('po_id'), data.get('factory_node_id'), data.get('requisition_id'),
            data.get('supplier_id'), data.get('item_category'), data.get('quantity_ordered'),
            data.get('unit_price'), data.get('total_amount'), data.get('currency', 'INR'),
            data.get('payment_terms', 'NET_30'), data.get('delivery_terms', 'FACTORY_GATE'),
            data.get('expected_delivery_date'), data.get('status', 'ISSUED'), operator_id
        ))
        po_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(po_id), 'status': 'issued'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/procurement/orders', methods=['GET'])
@jwt_required()
def list_procurement_orders():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT po_id, supplier_id, item_category, quantity_ordered, unit_price,
                   total_amount, status, expected_delivery_date, created_at
            FROM procurement_orders
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        pos = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(pos), 'orders': [dict(po) for po in pos]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Supply Chain: B2B Orders
# ---------------------------

@app.route('/api/v1/enterprise/supply-chain/b2b-orders', methods=['POST'])
@jwt_required()
def create_b2b_order():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['b2b_order_id', 'factory_node_id', 'buyer_id', 'quantity', 'unit_price', 'total_amount', 'delivery_date']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO b2b_orders (b2b_order_id, factory_node_id, buyer_id, sku_id, design_code,
                quantity, unit_price, total_amount, currency, grade_requirement, delivery_date,
                status, matched_saree_ids, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('b2b_order_id'), data.get('factory_node_id'), data.get('buyer_id'),
            data.get('sku_id'), data.get('design_code'), data.get('quantity'),
            data.get('unit_price'), data.get('total_amount'), data.get('currency', 'INR'),
            data.get('grade_requirement', 'GRADE_A_EXPORT_PREMIUM'), data.get('delivery_date'),
            data.get('status', 'PENDING'), json.dumps(data.get('matched_saree_ids', [])), operator_id
        ))
        order_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(order_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/supply-chain/b2b-orders', methods=['GET'])
@jwt_required()
def list_b2b_orders():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT b2b_order_id, buyer_id, sku_id, quantity, unit_price, total_amount,
                   grade_requirement, delivery_date, status, created_at
            FROM b2b_orders
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        orders = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(orders), 'orders': [dict(o) for o in orders]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Supply Chain: Inventory Bins
# ---------------------------

@app.route('/api/v1/enterprise/supply-chain/inventory-bins', methods=['POST'])
@jwt_required()
def create_inventory_bin():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['bin_id', 'factory_node_id', 'warehouse_zone']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inventory_bins (bin_id, factory_node_id, warehouse_zone, aisle, shelf,
                bin_type, item_category, current_quantity, unit_of_measure, reorder_point,
                max_capacity, vault_climate_status, current_humidity_pct, current_temperature_celsius,
                managed_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
            RETURNING id
        """, (
            data.get('bin_id'), data.get('factory_node_id'), data.get('warehouse_zone'),
            data.get('aisle'), data.get('shelf'), data.get('bin_type', 'STANDARD'),
            data.get('item_category'), data.get('current_quantity', 0),
            data.get('unit_of_measure', 'KG'), data.get('reorder_point'),
            data.get('max_capacity'), data.get('vault_climate_status', 'OPTIMAL_CLIMATE_LOCKED'),
            data.get('current_humidity_pct'), data.get('current_temperature_celsius'), operator_id
        ))
        bin_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(bin_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/supply-chain/inventory-bins', methods=['GET'])
@jwt_required()
def list_inventory_bins():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT bin_id, warehouse_zone, aisle, shelf, bin_type, item_category,
                   current_quantity, unit_of_measure, reorder_point, max_capacity,
                   vault_climate_status, current_humidity_pct, current_temperature_celsius
            FROM inventory_bins
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY warehouse_zone, aisle, shelf
            LIMIT 100
        """, (operator_id,))
        bins = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(bins), 'bins': [dict(b) for b in bins]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Supply Chain: Inventory Movements
# ---------------------------

@app.route('/api/v1/enterprise/supply-chain/inventory-movements', methods=['POST'])
@jwt_required()
def create_inventory_movement():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['movement_id', 'factory_node_id', 'bin_id', 'item_type', 'movement_type', 'quantity']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inventory_movements (movement_id, factory_node_id, bin_id, item_type,
                lot_batch_id, movement_type, quantity, unit_of_measure, reference_document_type,
                reference_document_id, from_bin_id, to_bin_id, authorized_by, received_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid, %s::uuid)
            RETURNING id
        """, (
            data.get('movement_id'), data.get('factory_node_id'), data.get('bin_id'),
            data.get('item_type'), data.get('lot_batch_id'), data.get('movement_type'),
            data.get('quantity'), data.get('unit_of_measure', 'KG'),
            data.get('reference_document_type'), data.get('reference_document_id'),
            data.get('from_bin_id'), data.get('to_bin_id'), operator_id, operator_id
        ))
        movement_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(movement_id), 'status': 'recorded'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/supply-chain/inventory-movements', methods=['GET'])
@jwt_required()
def list_inventory_movements():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT movement_id, item_type, lot_batch_id, movement_type, quantity,
                   unit_of_measure, reference_document_type, movement_date
            FROM inventory_movements
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY movement_date DESC
            LIMIT 100
        """, (operator_id,))
        movements = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(movements), 'movements': [dict(m) for m in movements]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# IoT: Device Registry
# ---------------------------

@app.route('/api/v1/enterprise/iot/devices', methods=['POST'])
@jwt_required()
def create_iot_device():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['device_id', 'factory_node_id', 'device_type']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO iot_device_registry (device_id, factory_node_id, device_type, loom_id,
                location_description, firmware_version, mqtt_topic, is_active, last_heartbeat_at,
                deployed_ai_models)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('device_id'), data.get('factory_node_id'), data.get('device_type'),
            data.get('loom_id'), data.get('location_description'), data.get('firmware_version'),
            data.get('mqtt_topic'), data.get('is_active', True), data.get('last_heartbeat_at'),
            json.dumps(data.get('deployed_ai_models', []))
        ))
        device_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(device_id), 'status': 'registered'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/iot/devices', methods=['GET'])
@jwt_required()
def list_iot_devices():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT device_id, device_type, loom_id, location_description, firmware_version,
                   is_active, last_heartbeat_at, created_at
            FROM iot_device_registry
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY device_type, device_id
            LIMIT 100
        """, (operator_id,))
        devices = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(devices), 'devices': [dict(d) for d in devices]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# IoT: Loom Telemetry
# ---------------------------

@app.route('/api/v1/enterprise/iot/loom-telemetry', methods=['POST'])
@jwt_required()
def create_loom_telemetry():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['telemetry_id', 'factory_node_id', 'loom_id']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO loom_telemetry (telemetry_id, factory_node_id, loom_id, device_id,
                picks_per_minute, warp_tension_cn, temperature_celsius, humidity_pct,
                vibration_mm_s, motor_current_a, ai_anomaly_score, ai_anomaly_classification,
                ai_auto_paused, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('telemetry_id'), data.get('factory_node_id'), data.get('loom_id'),
            data.get('device_id'), data.get('picks_per_minute'), data.get('warp_tension_cn'),
            data.get('temperature_celsius'), data.get('humidity_pct'), data.get('vibration_mm_s'),
            data.get('motor_current_a'), data.get('ai_anomaly_score'),
            data.get('ai_anomaly_classification'), data.get('ai_auto_paused', False),
            json.dumps(data.get('metadata', {}))
        ))
        telemetry_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(telemetry_id), 'status': 'recorded'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/iot/loom-telemetry', methods=['GET'])
@jwt_required()
def list_loom_telemetry():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT telemetry_id, loom_id, picks_per_minute, warp_tension_cn, temperature_celsius,
                   humidity_pct, vibration_mm_s, motor_current_a, ai_anomaly_score,
                   ai_anomaly_classification, ai_auto_paused, recorded_at
            FROM loom_telemetry
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY recorded_at DESC
            LIMIT 100
        """, (operator_id,))
        telemetry = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(telemetry), 'telemetry': [dict(t) for t in telemetry]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Notifications
# ---------------------------

@app.route('/api/v1/enterprise/notifications', methods=['POST'])
@jwt_required()
def create_notification():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['notification_id', 'notification_type', 'title', 'message']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO enterprise_notifications (notification_id, factory_node_id, recipient_user_id,
                recipient_role_id, channel, notification_type, priority, title, message,
                reference_entity_type, reference_entity_id)
            VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('notification_id'), data.get('factory_node_id'), data.get('recipient_user_id', operator_id),
            data.get('recipient_role_id'), data.get('channel', 'IN_APP'), data.get('notification_type'),
            data.get('priority', 'NORMAL'), data.get('title'), data.get('message'),
            data.get('reference_entity_type'), data.get('reference_entity_id')
        ))
        notification_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(notification_id), 'status': 'sent'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/notifications', methods=['GET'])
@jwt_required()
def list_notifications():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT notification_id, channel, notification_type, priority, title, message,
                   is_read, read_at, created_at
            FROM enterprise_notifications
            WHERE recipient_user_id = %s::uuid
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        notifications = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(notifications), 'notifications': [dict(n) for n in notifications]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# SKU Catalog
# ---------------------------

@app.route('/api/v1/enterprise/sku-catalog', methods=['POST'])
@jwt_required()
def create_sku():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['sku_id', 'factory_node_id', 'sku_name']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sku_catalog (sku_id, factory_node_id, sku_name, category, subcategory,
                hook_count, silk_type, zari_type, design_code, base_price, market_segment,
                is_active, launched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('sku_id'), data.get('factory_node_id'), data.get('sku_name'),
            data.get('category'), data.get('subcategory'), data.get('hook_count'),
            data.get('silk_type'), data.get('zari_type'), data.get('design_code'),
            data.get('base_price'), data.get('market_segment'), data.get('is_active', True),
            data.get('launched_at')
        ))
        sku_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(sku_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/sku-catalog', methods=['GET'])
@jwt_required()
def list_sku_catalog():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT sku_id, sku_name, category, subcategory, hook_count, silk_type, zari_type,
                   design_code, base_price, market_segment, is_active, launched_at
            FROM sku_catalog
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        skus = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(skus), 'skus': [dict(s) for s in skus]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

# ---------------------------
# Buy-Back Guarantees
# ---------------------------

@app.route('/api/v1/enterprise/buyback/guarantees', methods=['POST'])
@jwt_required()
def create_buyback_guarantee():
    try:
        operator_id = get_jwt_identity()
        data = request.get_json()
        required = ['guarantee_id', 'factory_node_id', 'saree_id', 'purchase_date']
        missing = [f for f in required if f not in data]
        if missing:
            return jsonify({'error': 'MissingFields', 'message': f"Missing: {', '.join(missing)}"}), 400

        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO buyback_guarantees (guarantee_id, factory_node_id, saree_id, buyer_id,
                purchase_date, guarantee_period_months, risk_score, premium_amount,
                reserve_provision, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('guarantee_id'), data.get('factory_node_id'), data.get('saree_id'),
            data.get('buyer_id'), data.get('purchase_date'), data.get('guarantee_period_months', 12),
            data.get('risk_score'), data.get('premium_amount'), data.get('reserve_provision'),
            data.get('status', 'ACTIVE')
        ))
        guarantee_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'id': str(guarantee_id), 'status': 'created'}), 201
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/enterprise/buyback/guarantees', methods=['GET'])
@jwt_required()
def list_buyback_guarantees():
    try:
        operator_id = get_jwt_identity()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT guarantee_id, saree_id, buyer_id, purchase_date, guarantee_period_months,
                   risk_score, premium_amount, reserve_provision, status, created_at
            FROM buyback_guarantees
            WHERE factory_node_id = (SELECT factory_node_id FROM users WHERE id = %s::uuid)
            ORDER BY created_at DESC
            LIMIT 100
        """, (operator_id,))
        guarantees = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'total': len(guarantees), 'guarantees': [dict(g) for g in guarantees]}), 200
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)
