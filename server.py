#!usr/bin/env python3

from flask import Flask, send_file, request, jsonify
from flask_cors import CORS
import threading
app = Flask(__name__)
CORS(app)  # Enable CORS for the entire app


metrics_data={}
@app.route('/')
def home():
    global metrics_data
    metrics_str = "\n".join([f"{node}: {metrics}" for node, metrics in metrics_data.items()])
    
    # Display the metrics
    return f"hello 1 2 3 load testing<br><br>Metrics:<br>{metrics_str}"

@app.route('/metrics', methods=['POST'])
def receive_metrics():
    global metrics_data
    data = request.json
    node_id = data.get('node_id')
    test_id = data.get('test_id')
    node_metrics = data.get('metrics', {})
    
    # Store the received metrics
    metrics_data[(node_id, test_id)] = node_metrics
    
    return jsonify({"status": "Metrics received successfully"})

@app.route('/download')
def download_file():
	file_path='XC161971 - Plum-headed Parakeet - Psittacula cyanocephala.mp3'
	return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
