"""
Guild Service
Manages worker/vendor associations, piece-rate calculations, and payment distribution.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime

app = Flask(__name__)

def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@app.route('/api/v1/guilds', methods=['GET'])
@jwt_required()
def list_guilds():
    """List all guilds"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT g.id, g.guild_id, g.name, g.type, g.region, 
                   g.factory_node_id, g.config, g.is_active,
                   COUNT(gm.id) as member_count
            FROM guilds g
            LEFT JOIN guild_members gm ON g.id = gm.guild_id AND gm.is_active = TRUE
            GROUP BY g.id
            ORDER BY g.name
        """)
        
        guilds = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(guilds),
            'guilds': [
                {
                    'id': str(g['id']),
                    'guild_id': g['guild_id'],
                    'name': g['name'],
                    'type': g['type'],
                    'region': g['region'],
                    'factory_node_id': g['factory_node_id'],
                    'config': g['config'],
                    'is_active': g['is_active'],
                    'member_count': g['member_count']
                }
                for g in guilds
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/guilds/<guild_id>/members', methods=['GET'])
@jwt_required()
def list_guild_members(guild_id):
    """List members of a guild"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT gm.id, gm.role_type, gm.skill_level, gm.performance_score,
                   gm.joined_at, gm.is_active,
                   u.id as user_id, u.full_name, u.email, u.employee_id
            FROM guild_members gm
            JOIN users u ON gm.user_id = u.id
            WHERE gm.guild_id = %s::uuid
            ORDER BY gm.performance_score DESC
        """, (guild_id,))
        
        members = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'guild_id': guild_id,
            'total': len(members),
            'members': [
                {
                    'id': str(m['id']),
                    'user_id': str(m['user_id']),
                    'full_name': m['full_name'],
                    'email': m['email'],
                    'employee_id': m['employee_id'],
                    'role_type': m['role_type'],
                    'skill_level': m['skill_level'],
                    'performance_score': float(m['performance_score']) if m['performance_score'] else 0.0,
                    'joined_at': m['joined_at'].isoformat(),
                    'is_active': m['is_active']
                }
                for m in members
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/guilds/<guild_id>/payments', methods=['POST'])
@jwt_required()
def create_payment(guild_id):
    """Create a payment record for a guild member"""
    try:
        data = request.get_json()
        member_id = data.get('member_id')
        payment_type = data.get('payment_type')
        amount = data.get('amount')
        
        if not all([member_id, payment_type, amount]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO guild_payments (guild_id, member_id, payment_type, amount, status)
            VALUES (%s::uuid, %s::uuid, %s, %s, 'PENDING')
            RETURNING id
        """, (guild_id, member_id, payment_type, amount))
        
        payment_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'created',
            'payment_id': str(payment_id),
            'guild_id': guild_id,
            'member_id': member_id,
            'payment_type': payment_type,
            'amount': amount
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/guilds/<guild_id>/payments', methods=['GET'])
@jwt_required()
def list_payments(guild_id):
    """List payments for a guild"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT gp.id, gp.payment_type, gp.amount, gp.currency, gp.status,
                   gp.processed_at, gp.created_at,
                   u.full_name as member_name
            FROM guild_payments gp
            JOIN users u ON gp.member_id = u.id
            WHERE gp.guild_id = %s::uuid
            ORDER BY gp.created_at DESC
            LIMIT 100
        """, (guild_id,))
        
        payments = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'guild_id': guild_id,
            'total': len(payments),
            'payments': [
                {
                    'id': str(p['id']),
                    'member_name': p['member_name'],
                    'payment_type': p['payment_type'],
                    'amount': float(p['amount']),
                    'currency': p['currency'],
                    'status': p['status'],
                    'processed_at': p['processed_at'].isoformat() if p['processed_at'] else None,
                    'created_at': p['created_at'].isoformat()
                }
                for p in payments
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/guilds/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'guild-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5007)
