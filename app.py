from flask import Flask, jsonify
from flask_cors import CORS
import websocket
import json
import threading
import os

app = Flask(__name__)
CORS(app)

current_price = "4586.11"
DERIV_APP_ID = "1089"

def on_message(ws, message):
    global current_price
    data = json.loads(message)
    if 'tick' in data:
        current_price = str(data['tick']['quote'])

def on_open(ws):
    ws.send(json.dumps({"ticks": "frxXAUUSD", "subscribe": 1}))

def start_ws():
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message).run_forever()

@app.route('/api/v1/market/xauusd')
def get_price():
    return jsonify({"current_price": current_price, "status": "success"})

if __name__ == '__main__':
    ws_thread = threading.Thread(target=start_ws)
    ws_thread.daemon = True
    ws_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
