"""
Localization Service
Manages multi-language support, TTS audio generation, and dialect management.
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

@app.route('/api/v1/i18n/translations', methods=['GET'])
@jwt_required()
def get_translations():
    """Get all translations for a specific language"""
    try:
        language_code = request.args.get('language', 'en')
        category = request.args.get('category')
        
        conn = get_db()
        cur = conn.cursor()
        
        query = """
            SELECT ik.key_code, ik.category, ik.description, it.translated_text
            FROM i18n_keys ik
            LEFT JOIN i18n_translations it ON ik.id = it.key_id AND it.language_code = %s
            WHERE 1=1
        """
        params = [language_code]
        
        if category:
            query += " AND ik.category = %s"
            params.append(category)
        
        cur.execute(query, params)
        translations = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'language_code': language_code,
            'total': len(translations),
            'translations': [
                {
                    'key_code': t['key_code'],
                    'category': t['category'],
                    'description': t['description'],
                    'translated_text': t['translated_text']
                }
                for t in translations
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/i18n/tts/generate', methods=['POST'])
@jwt_required()
def generate_tts():
    """
    Generate TTS audio for a specific message key and language.
    In production, this would call a neural TTS service.
    """
    try:
        data = request.get_json()
        key_code = data.get('key_code')
        language_code = data.get('language_code', 'en')
        
        if not key_code:
            return jsonify({'error': 'Missing key_code'}), 400
        
        # Simulate TTS generation
        audio_url = f"/audio/{language_code}/{key_code}.mp3"
        duration_ms = random.randint(2000, 5000)
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get key_id
        cur.execute("SELECT id FROM i18n_keys WHERE key_code = %s", (key_code,))
        key = cur.fetchone()
        
        if not key:
            return jsonify({'error': 'Key not found'}), 404
        
        # Cache audio
        cur.execute("""
            INSERT INTO voice_audio_cache (key_code, language_code, audio_url, duration_ms)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key_code, language_code) 
            DO UPDATE SET audio_url = %s, duration_ms = %s
        """, (key_code, language_code, audio_url, duration_ms, audio_url, duration_ms))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'key_code': key_code,
            'language_code': language_code,
            'audio_url': audio_url,
            'duration_ms': duration_ms,
            'status': 'generated'
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/i18n/keys', methods=['GET'])
@jwt_required()
def list_keys():
    """List all i18n keys"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT ik.key_code, ik.category, ik.description,
                   COUNT(it.id) as translation_count
            FROM i18n_keys ik
            LEFT JOIN i18n_translations it ON ik.id = it.key_id
            GROUP BY ik.id
            ORDER BY ik.category, ik.key_code
        """)
        
        keys = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total': len(keys),
            'keys': [
                {
                    'key_code': k['key_code'],
                    'category': k['category'],
                    'description': k['description'],
                    'translation_count': k['translation_count']
                }
                for k in keys
            ]
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'InternalServerError', 'message': str(e)}), 500

@app.route('/api/v1/i18n/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'localization-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5008)
