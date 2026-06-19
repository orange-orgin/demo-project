from flask import Flask, jsonify
import redis
import os

app = Flask(__name__)

@app.route('/api/ping')
def ping():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)