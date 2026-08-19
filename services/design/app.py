"""
Design Service
Handles GAN design generation, design file management, and design injection.
"""
from flask import Flask, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
import psycopg2
import psycopg2.extras
import os
import json
import random
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

@app.route('/api/v1/design/generate', methods=['POST'])
@jwt_required()
def generate_design():
    """
    Generates new saree designs using GAN (simulated).
    In production, this would call a GPU server running StyleGAN-XL.
    """
    try:
        data = request.get_json()
        count = data.get('count', 10)
        region = data.get('region', 'ap-south-1')
        motif_style = data.get('motif_style', 'kanchipuram')  # kanchipuram, banarasi, paithani
        sku_ref_id = data.get('sku_ref_id')
        
        if count > 100:
            return jsonify({'error': 'Maximum 100 designs per batch'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Validate SKU if provided
        sku = None
        if sku_ref_id:
            cur.execute("SELECT id, sku_ref_id, jacquard_capacity, weight_category_profile FROM sku_catalog WHERE sku_ref_id = %s AND is_active = TRUE", (sku_ref_id,))
            sku = cur.fetchone()
            if not sku:
                cur.close()
                conn.close()
                return jsonify({'error': f'SKU {sku_ref_id} not found'}), 404
        
        designs = []
        for i in range(count):
            pattern_id = f"DESIGN-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(10000, 99999)}"
            
            # Simulate GAN generation
            viability_score = round(random.uniform(0.6, 0.95), 2)
            cost_margin = round(random.uniform(15, 45), 2)
            
            # Use SKU metrics if provided
            hook_count = 2400
            file_size_gb = round(random.uniform(0.8, 1.2), 2)
            if sku:
                hook_count = int(sku['jacquard_capacity'].split()[0])
                file_size_gb = round(hook_count / 2400 * 1.0, 2)
            
            design = {
                'pattern_id': pattern_id,
                'region': region,
                'motif_style': motif_style,
                'sku_ref_id': sku_ref_id,
                'viability_score': viability_score,
                'cost_margin': cost_margin,
                'hook_count': hook_count,
                'file_size_gb': file_size_gb,
                'segments': {
                    'border_top': {'hooks': '1-400', 'length_m': 5.5},
                    'body': {'hooks': '401-1800', 'repeats': random.randint(8, 20)},
                    'border_bottom': {'hooks': '1801-2200', 'length_m': 5.5},
                    'pallu': {'hooks': '2201-2400', 'density': 'high'}
                }
            }
            designs.append(design)
            
            # Store in database
            cur.execute("""
                INSERT INTO design_generations 
                (pattern_id, factory_node_id, generation_type, viability_score, cost_margin, status, design_json)
                VALUES (%s, %s, 'GAN', %s, %s, 'PENDING', %s)
            """, (
                pattern_id, 'FACT-BLR-01', viability_score, cost_margin,
                json.dumps({
                    'motif_style': motif_style,
                    'sku_ref_id': sku_ref_id,
                    'hook_count': hook_count,
                    'file_size_gb': file_size_gb
                })
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'generated',
            'count': len(designs),
            'designs': designs
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/designs', methods=['GET'])
@jwt_required()
def list_designs():
    """List all designs with filtering"""
    try:
        status_filter = request.args.get('status')
        motif_style = request.args.get('motif_style')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        conn = get_db()
        cur = conn.cursor()
        
        query = """
            SELECT dg.pattern_id, dg.factory_node_id, dg.generation_type,
                   dg.viability_score, dg.cost_margin, dg.sales_velocity_score,
                   dg.status, dg.created_at,
                   df.file_size_gb, df.hook_count, df.segment_map
            FROM design_generations dg
            LEFT JOIN design_files df ON dg.pattern_id = df.pattern_id
            WHERE 1=1
        """
        params = []
        
        if status_filter:
            query += " AND dg.status = %s"
            params.append(status_filter)
        
        if motif_style:
            query += " AND dg.design_json->>'motif_style' = %s"
            params.append(motif_style)
        
        query += " ORDER BY dg.created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        designs = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(designs),
            'designs': [
                {
                    'pattern_id': d['pattern_id'],
                    'factory_node_id': d['factory_node_id'],
                    'generation_type': d['generation_type'],
                    'viability_score': float(d['viability_score']) if d['viability_score'] else None,
                    'cost_margin': float(d['cost_margin']) if d['cost_margin'] else None,
                    'sales_velocity_score': float(d['sales_velocity_score']) if d['sales_velocity_score'] else None,
                    'status': d['status'],
                    'file_size_gb': float(d['file_size_gb']) if d['file_size_gb'] else None,
                    'hook_count': d['hook_count'],
                    'created_at': d['created_at'].isoformat()
                }
                for d in designs
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/designs/<pattern_id>/approve', methods=['POST'])
@jwt_required()
def approve_design(pattern_id):
    """Approve a generated design for production"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Update design generation status
        cur.execute("""
            UPDATE design_generations 
            SET status = 'APPROVED' 
            WHERE pattern_id = %s
        """, (pattern_id,))
        
        # Create design file record
        cur.execute("""
            INSERT INTO design_files (pattern_id, factory_node_id, file_hash, file_size_gb, hook_count, status)
            VALUES (%s, %s, %s, %s, %s, 'APPROVED')
            ON CONFLICT (pattern_id) DO UPDATE SET status = 'APPROVED'
        """, (
            pattern_id, 'FACT-BLR-01',
            f"hash-{random.randint(10000, 99999)}",
            round(random.uniform(0.8, 1.2), 2),
            2400
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'approved',
            'pattern_id': pattern_id,
            'message': 'Design approved for production'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/design/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'design-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5005)
