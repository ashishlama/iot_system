from flask import Flask, jsonify, request
import sys
import os
from flask_cors import CORS

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from database import init_db, get_data, get_latest_sensor_data, get_latest_processed_data
from config import DB_NAME

app = Flask(__name__)
CORS(app) 

def get_latest_data():
    conn = init_db(DB_NAME)   # Adjust path as needed
    data = get_data(conn)
    return data

@app.route('/api/data', methods=['GET'])
def api_data():
    return jsonify(get_latest_data())

@app.route('/api/sensor_data', methods=['GET'])
def api_sensor():
    sensor_type = request.args.get('sensortype')
    if not sensor_type:
        return jsonify({'error': 'Missing sensortype parameter'}), 400

    conn = init_db(DB_NAME)
    data = get_latest_sensor_data(conn, sensor_type)
    conn.close()

    if not data:
        return jsonify({'error': f'No data found or invalid sensortype: {sensor_type}'}), 404

    return jsonify(data)

@app.route('/api/processed_data', methods=['GET'])
def api_processed():
    conn = init_db(DB_NAME)
    data = get_latest_processed_data(conn)
    conn.close()

    if not data:
        return jsonify({'error': f'No data found'}), 404

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)