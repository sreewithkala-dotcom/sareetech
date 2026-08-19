"""
Buy-Back Service
Manages buy-back guarantees, NFC verification, and AI depreciation calculations.
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

@app.route('/api/v1/buyback/valuate', methods=['POST'])
@jwt_required()
def valuate_buyback():
    """
    AI-powered buy-back valuation engine.
    Input: NFC tag ID, spectral scan data
    Output: Dynamic buy-back value with depreciation breakdown
    """
    try:
        data = request.get_json()
        nfc_id = data.get('nfc_id')
        scan_data = data.get('scan_data', {})
        
        if not nfc_id:
            return jsonify({'error': 'Missing nfc_id'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        # Find saree by NFC ID
        cur.execute("""
            SELECT fs.id, fs.lot_id, fs.grade, fs.buyback_value, fs.certificate_hash,
                   pl.lot_number, pl.factory_node_id
            FROM finished_sarees fs
            JOIN production_lots pl ON fs.lot_id = pl.id
            WHERE fs.nfc_id = %s
        """, (nfc_id,))
        
        saree = cur.fetchone()
        if not saree:
            return jsonify({'error': 'Saree not found or invalid NFC tag'}), 404
        
        # AI depreciation calculation
        base_value = float(saree['buyback_value'] or 50000)  # Base value in INR
        
        # Depreciation factors
        fabric_thinning = scan_data.get('fabric_thinning_pct', 0) / 100
        gold_oxidation = scan_data.get('gold_oxidation_pct', 0) / 100
        fiber_pulls = scan_data.get('structural_fiber_pulls', 0)
        chemical_stains = scan_data.get('chemical_stains', 0)
        
        fabric_depreciation = base_value * 0.6 * fabric_thinning
        zari_depreciation = base_value * 0.3 * gold_oxidation
        structural_penalty = min(fiber_pulls * 500, base_value * 0.2)
        stain_penalty = min(chemical_stains * 1000, base_value * 0.15)
        
        total_depreciation = fabric_depreciation + zari_depreciation + structural_penalty + stain_penalty
        buyback_value = max(base_value - total_depreciation, base_value * 0.1)  # Minimum 10%
        
        # Get current gold/silver market rates (simulated)
        gold_rate_per_gram = 6500  # INR per gram
        silver_rate_per_gram = 75   # INR per gram
        
        depreciation_breakdown = {
            'base_value_inr': round(base_value, 2),
            'fabric_thinning_pct': round(fabric_thinning * 100, 2),
            'fabric_depreciation_inr': round(fabric_depreciation, 2),
            'gold_oxidation_pct': round(gold_oxidation * 100, 2),
            'zari_depreciation_inr': round(zari_depreciation, 2),
            'structural_penalty_inr': round(structural_penalty, 2),
            'stain_penalty_inr': round(stain_penalty, 2),
            'total_depreciation_inr': round(total_depreciation, 2),
            'market_rates': {
                'gold_inr_per_gram': gold_rate_per_gram,
                'silver_inr_per_gram': silver_rate_per_gram
            }
        }
        
        # Create buyback guarantee record
        cur.execute("""
            INSERT INTO buyback_guarantees 
            (saree_id, customer_id, scan_data, ai_valuation, payout_amount, depreciation_breakdown)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            saree['id'],
            data.get('customer_id'),
            json.dumps(scan_data),
            json.dumps({'model_version': 'buyback-v2.3.1', 'timestamp': datetime.utcnow().isoformat()}),
            round(buyback_value, 2),
            json.dumps(depreciation_breakdown)
        ))
        
        buyback_id = cur.fetchone()['id']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'buyback_id': str(buyback_id),
            'nfc_id': nfc_id,
            'lot_number': saree['lot_number'],
            'base_value_inr': round(base_value, 2),
            'buyback_value_inr': round(buyback_value, 2),
            'depreciation_breakdown': depreciation_breakdown,
            'payout_status': 'PENDING'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/buyback/<buyback_id>/approve', methods=['POST'])
@jwt_required()
def approve_buyback(buyback_id):
    """Approve and process buy-back payout"""
    try:
        operator_id = get_jwt_identity()
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE buyback_guarantees 
            SET payout_status = 'APPROVED', processed_by = %s, processed_at = %s
            WHERE id = %s AND payout_status = 'PENDING'
            RETURNING *
        """, (operator_id, datetime.utcnow(), buyback_id))
        
        result = cur.fetchone()
        if not result:
            return jsonify({'error': 'Buyback not found or already processed'}), 404
        
        # Update finished saree status
        cur.execute("""
            UPDATE finished_sarees 
            SET status = 'BUYBACK_PAID'
            WHERE id = %s
        """, (result['saree_id'],))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'approved',
            'buyback_id': buyback_id,
            'payout_amount': float(result['payout_amount']),
            'message': 'Buy-back approved and queued for payout'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/buyback/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'buyback-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5006)
