from flask import Flask, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

@app.route('/api/v1/market/xauusd')
def get_price():
    try:
        # Using a reliable, free public API for Gold
        response = requests.get("https://api.gold-api.com/price/XAU")
        data = response.json()
        live_price = str(round(data['price'], 2))
        return jsonify({"current_price": live_price, "status": "success"})
    except Exception as e:
        return jsonify({"current_price": "4586.11", "status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
