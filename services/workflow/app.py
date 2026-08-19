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
        status_filter = request.args.get('status')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)
