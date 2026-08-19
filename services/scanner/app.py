"""
Scanner Service
Handles input/output scanning with pre-step validation and post-step propagation.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime

app = Flask(__name__)

# Database connection
def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@app.route('/api/v1/scanner/input', methods=['POST'])
@jwt_required()
def scanner_input():
    try:
        data = request.get_json()
        asset_id = data.get('asset_id')
        factory_node_id = data.get('factory_node_id')
        workstation_id = data.get('workstation_id')
        
        operator_id = get_jwt_identity()
        
        if not asset_id or not factory_node_id:
            return jsonify({'error': 'Missing required fields'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get current lot
        cur.execute(
            "SELECT id, status, locked_by, current_role_id FROM production_lots WHERE asset_id = %s AND factory_node_id = %s",
            (asset_id, factory_node_id)
        )
        lot = cur.fetchone()
        
        if not lot:
            return jsonify({'error': 'AssetNotFound', 'message': 'Asset not found'}), 404
        
        # Check if locked by another operator
        if lot['locked_by'] and lot['locked_by'] != operator_id:
            return jsonify({
                'error': 'AssetLocked',
                'message': f'Asset is locked by operator {lot["locked_by"]}',
                'locked_by': str(lot['locked_by'])
            }), 409
        
        # Validate status transition
        if not fn_validate_status_transition(lot['status'], 'Queued'):
            return jsonify({
                'error': 'InvalidStatusTransition',
                'current_status': lot['status'],
                'expected': 'Queued or Certified:*',
                'message': f'Cannot scan asset in status: {lot["status"]}'
            }), 409
        
        # Check for duplicate scans
        cur.execute(
            "SELECT COUNT(*) as count FROM scanner_logs WHERE asset_id = %s AND factory_node_id = %s AND scan_type = 'input' AND scan_timestamp > NOW() - INTERVAL '5 minutes'",
            (asset_id, factory_node_id)
        )
        dup_check = cur.fetchone()
        if dup_check['count'] > 0:
            return jsonify({
                'error': 'DuplicateScan',
                'message': 'Asset was scanned within last 5 minutes'
            }), 409
        
        # Determine expected role from workflow transitions
        cur.execute(
            """
            SELECT wt.to_role_id FROM workflow_transitions wt
            WHERE wt.from_role_id = (SELECT id FROM roles WHERE role_id = 'ROLE-SYSTEM-ADMIN')
            AND wt.required_certificate_step = 'Initial'
            LIMIT 1
            """
        )
        first_role = cur.fetchone()
        expected_role_id = first_role['to_role_id'] if first_role else None
        
        # Call stored procedure
        cur.execute(
            "SELECT * FROM sp_validate_scanner_input(%s, %s::uuid, %s, %s::uuid)",
            (asset_id, operator_id, factory_node_id, expected_role_id)
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if not result['validation_passed']:
            return jsonify({
                'error': 'ValidationFailed',
                'message': result['error_message']
            }), 400
        
        return jsonify({
            'success': True,
            'lot_id': str(result['lot_id']),
            'message': 'Input scan validated successfully'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/scanner/output', methods=['POST'])
@jwt_required()
def scanner_output():
    try:
        data = request.get_json()
        asset_id = data.get('asset_id')
        factory_node_id = data.get('factory_node_id')
        
        operator_id = get_jwt_identity()
        
        if not asset_id or not factory_node_id:
            return jsonify({'error': 'Missing required fields'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get lot
        cur.execute(
            "SELECT id, status FROM production_lots WHERE asset_id = %s AND factory_node_id = %s",
            (asset_id, factory_node_id)
        )
        lot = cur.fetchone()
        
        if not lot:
            return jsonify({'error': 'AssetNotFound'}), 404
        
        # Call stored procedure
        cur.execute(
            "SELECT * FROM sp_process_scanner_output(%s::uuid, %s, %s::uuid, %s)",
            (lot['id'], asset_id, operator_id, factory_node_id)
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if not result['success']:
            return jsonify({
                'error': 'OutputScanFailed',
                'message': result['error_message']
            }), 400
        
        response = {
            'success': True,
            'message': result['error_message']
        }
        
        if result['next_role_id']:
            response['next_role_id'] = str(result['next_role_id'])
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

def fn_validate_status_transition(current_status, expected_status):
    """Helper function for status validation"""
    if current_status == expected_status:
        return True
    if current_status.startswith('Certified:'):
        return True
    if current_status == 'Queued':
        return True
    return False

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
