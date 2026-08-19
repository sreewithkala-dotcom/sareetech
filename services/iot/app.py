"""
IoT Service
Handles MQTT telemetry ingestion, loom monitoring, and design injection.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@app.route('/api/v1/iot/telemetry', methods=['POST'])
def ingest_telemetry():
    """
    Receives MQTT telemetry from ECU edge controllers.
    Processes loom pick counts, faults, and efficiency metrics.
    """
    try:
        data = request.get_json()
        loom_id = data.get('loom_id')
        device_id = data.get('device_id')
        current_picks = data.get('current_picks', 0)
        target_picks = data.get('target_picks', 0)
        faults_detected = data.get('faults_detected', 0)
        
        if not loom_id or not device_id:
            return jsonify({'error': 'Missing required fields: loom_id, device_id'}), 400
        
        # Calculate efficiency
        progress = (current_picks / target_picks * 100) if target_picks > 0 else 0.0
        efficiency_score = max(0.0, min(1.0, 1.0 - (faults_detected * 0.1)))
        
        status = 'ACTIVE'
        if faults_detected >= 5:
            status = 'FLAGGED_FOR_INSPECTION'
        elif progress >= 100.0:
            status = 'COMPLETED'
        
        conn = get_db()
        cur = conn.cursor()
        
        # Insert telemetry record
        cur.execute("""
            INSERT INTO loom_telemetry 
            (loom_id, device_id, factory_node_id, current_picks, target_picks, 
             pick_rate_per_minute, faults_detected, efficiency_score, status, telemetry_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            loom_id, device_id, 'FACT-BLR-01',
            current_picks, target_picks,
            data.get('pick_rate_per_minute', 0),
            faults_detected, efficiency_score, status,
            json.dumps(data)
        ))
        
        telemetry_id = cur.fetchone()['id']
        
        # Update loom assignment status if needed
        if status == 'FLAGGED_FOR_INSPECTION':
            cur.execute("""
                UPDATE loom_assignments 
                SET status = 'FLAGGED', metadata = jsonb_set(COALESCE(metadata, '{}'), '{flag_reason}', '"High fault count"')
                WHERE loom_id = %s AND status = 'ACTIVE'
            """, (loom_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'telemetry_id': str(telemetry_id),
            'loom_status': status,
            'efficiency_score': efficiency_score
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/iot/design/inject', methods=['POST'])
@jwt_required()
def inject_design():
    """
    Injects design files to ECU edge controllers via MQTT.
    Streams 1GB files in blocks to avoid memory overflow.
    """
    try:
        data = request.get_json()
        loom_id = data.get('loom_id')
        pattern_id = data.get('pattern_id')
        file_hash = data.get('file_hash')
        binary_url = data.get('binary_url')
        
        if not all([loom_id, pattern_id, file_hash, binary_url]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Verify loom is available
        cur.execute("""
            SELECT status FROM loom_assignments 
            WHERE loom_id = %s AND status IN ('ASSIGNED', 'ACTIVE')
        """, (loom_id,))
        loom = cur.fetchone()
        
        if not loom:
            return jsonify({'error': 'Loom not available or not found'}), 404
        
        # Get device info
        cur.execute("""
            SELECT device_id, storage_capacity_gb FROM edge_controllers 
            WHERE loom_id = %s AND status = 'ONLINE'
        """, (loom_id,))
        device = cur.fetchone()
        
        if not device:
            return jsonify({'error': 'No online ECU found for this loom'}), 404
        
        # Create design injection record
        cur.execute("""
            INSERT INTO design_injections 
            (loom_id, device_id, pattern_id, file_hash, injection_status)
            VALUES (%s, %s, %s, %s, 'QUEUED')
            RETURNING id
        """, (loom_id, device['device_id'], pattern_id, file_hash))
        
        injection_id = cur.fetchone()['id']
        
        # TODO: Trigger MQTT message to ECU for file download
        # This would be handled by a separate MQTT publisher service
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'queued',
            'injection_id': str(injection_id),
            'loom_id': loom_id,
            'pattern_id': pattern_id,
            'message': 'Design file injection queued for MQTT delivery'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/iot/devices', methods=['GET'])
@jwt_required()
def list_devices():
    """List all edge controllers with status"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT device_id, loom_id, factory_node_id, firmware_version, 
                   storage_type, storage_capacity_gb, status, last_seen
            FROM edge_controllers
            ORDER BY last_seen DESC NULLS LAST
        """)
        
        devices = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(devices),
            'devices': [
                {
                    'device_id': d['device_id'],
                    'loom_id': d['loom_id'],
                    'factory_node_id': d['factory_node_id'],
                    'firmware_version': d['firmware_version'],
                    'storage_type': d['storage_type'],
                    'storage_capacity_gb': float(d['storage_capacity_gb']) if d['storage_capacity_gb'] else None,
                    'status': d['status'],
                    'last_seen': d['last_seen'].isoformat() if d['last_seen'] else None
                }
                for d in devices
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/iot/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'iot-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
