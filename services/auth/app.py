"""
Auth Service
Unified authentication endpoint for all 24 roles.
Returns JWT with RoleID, FactoryNodeID, and PermittedOperations[] claims.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import uuid
from datetime import datetime, timedelta
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

# JWT Configuration
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'your-secret-key-change-in-production')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=1)
app.config['JWT_ALGORITHM'] = 'HS256'

jwt = JWTManager(app)

def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@app.route('/api/v1/auth/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        email = data.get('email')
        password = data.get('password')
        factory_node_id = data.get('factory_node_id')
        
        if not email or not password or not factory_node_id:
            return jsonify({'error': 'Missing required fields: email, password, factory_node_id'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Find user
        cur.execute("""
            SELECT u.id, u.email, u.password_hash, u.full_name, u.factory_node_id,
                   r.role_id, r.name AS role_name, r.permitted_operations
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.email = %s AND u.factory_node_id = %s AND u.is_active = TRUE
        """, (email, factory_node_id))
        
        user = cur.fetchone()
        
        if not user or not check_password_hash(user['password_hash'], password):
            return jsonify({'error': 'InvalidCredentials', 'message': 'Invalid email or password'}), 401
        
        # Create JWT claims
        additional_claims = {
            'RoleID': user['role_id'],
            'FactoryNodeID': user['factory_node_id'],
            'PermittedOperations': user['permitted_operations'] if isinstance(user['permitted_operations'], list) else [],
            'UserName': user['full_name'],
            'Email': user['email']
        }
        
        # Create access token
        access_token = create_access_token(
            identity=str(user['id']),
            additional_claims=additional_claims
        )
        
        # Store session
        jti = uuid.uuid4().hex
        cur.execute(
            """
            INSERT INTO user_sessions (user_id, jti, factory_node_id, ip_address, user_agent, expires_at)
            VALUES (%s::uuid, %s, %s, %s::inet, %s, %s)
            """,
            (
                user['id'],
                jti,
                factory_node_id,
                request.remote_addr or '127.0.0.1',
                request.headers.get('User-Agent', ''),
                datetime.utcnow() + app.config['JWT_ACCESS_TOKEN_EXPIRES']
            )
        )
        
        # Update last login
        cur.execute(
            "UPDATE users SET last_login = %s, login_count = login_count + 1 WHERE id = %s::uuid",
            (datetime.utcnow(), user['id'])
        )
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'access_token': access_token,
            'token_type': 'Bearer',
            'expires_in': app.config['JWT_ACCESS_TOKEN_EXPIRES'].total_seconds(),
            'jti': jti,
            'user': {
                'id': str(user['id']),
                'email': user['email'],
                'full_name': user['full_name'],
                'role_id': user['role_id'],
                'role_name': user['role_name'],
                'factory_node_id': user['factory_node_id'],
                'permitted_operations': user['permitted_operations']
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/auth/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    try:
        identity = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get user info
        cur.execute("""
            SELECT u.id, r.role_id, r.permitted_operations, u.factory_node_id
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.id = %s::uuid AND u.is_active = TRUE
        """, (identity,))
        
        user = cur.fetchone()
        cur.close()
        conn.close()
        
        if not user:
            return jsonify({'error': 'UserNotFound'}), 404
        
        # Create new access token
        additional_claims = {
            'RoleID': user['role_id'],
            'FactoryNodeID': user['factory_node_id'],
            'PermittedOperations': user['permitted_operations'] if isinstance(user['permitted_operations'], list) else []
        }
        
        access_token = create_access_token(
            identity=identity,
            additional_claims=additional_claims
        )
        
        return jsonify({
            'access_token': access_token,
            'token_type': 'Bearer',
            'expires_in': app.config['JWT_ACCESS_TOKEN_EXPIRES'].total_seconds()
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/auth/logout', methods=['POST'])
@jwt_required()
def logout():
    try:
        identity = get_jwt_identity()
        jti = get_jwt()['jti']
        
        conn = get_db()
        cur = conn.cursor()
        
        # Revoke session
        cur.execute(
            "UPDATE user_sessions SET revoked = TRUE WHERE jti = %s AND user_id = %s::uuid",
            (jti, identity)
        )
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'message': 'Logged out successfully'}), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/auth/me', methods=['GET'])
@jwt_required()
def get_current_user():
    try:
        identity = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT u.id, u.email, u.full_name, u.factory_node_id, u.last_login,
                   r.role_id, r.name AS role_name, r.permitted_operations
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.id = %s::uuid
        """, (identity,))
        
        user = cur.fetchone()
        cur.close()
        conn.close()
        
        if not user:
            return jsonify({'error': 'UserNotFound'}), 404
        
        return jsonify({
            'id': str(user['id']),
            'email': user['email'],
            'full_name': user['full_name'],
            'factory_node_id': user['factory_node_id'],
            'last_login': user['last_login'].isoformat() if user['last_login'] else None,
            'role': {
                'role_id': user['role_id'],
                'name': user['role_name'],
                'permitted_operations': user['permitted_operations']
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
