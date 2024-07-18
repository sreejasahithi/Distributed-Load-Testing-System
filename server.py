# from flask import Flask, jsonify, request,render_template
# import requests
# import os
# from kafka import KafkaConsumer, KafkaProducer
# import json
# import threading
# import uuid
# import time

# from orchestrator import OrchestratorNode 
# template_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
# app = Flask(__name__, template_folder=template_folder)
# # Sample data
# global requests_sent, responses_sent

from flask import Flask, jsonify
# from shared_variable import responses_sent
app = Flask(__name__)

# Initialize global variab
# global requests_sent
requests_sent=0
# responses_sent = 0

@app.route('/ping', methods=['GET'])
def ping():
    global requests_sent
    requests_sent += 1
    # Simulate some processing time
    # responses_sent += 1
    
    return jsonify({'message': 'pong'})

@app.route('/metrics', methods=['GET', 'POST'])
def metrics():
    global requests_sent
    
    # requests_sent += 1
    # responses_sent += 1
    return jsonify({'requests_sent': requests_sent})

if __name__ == '__main__':
    
    app.run(debug=True, port=8000)
