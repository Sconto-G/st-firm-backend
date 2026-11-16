from flask import Flask, jsonify
from flask_cors import CORS
import websocket
import json
import threading
import math

app = Flask(__name__)
CORS(app)

# --- GLOBAL STATE ---
market_state = {
    "current_price": "4586.11",
    "timeframes": {
        "M5": {"trend": "WAITING", "bias": "WAITING", "ema_fast": 0, "ema_slow": 0},
        "M30": {"trend": "WAITING", "bias": "WAITING", "ema_fast": 0, "ema_slow": 0},
        "H1": {"trend": "WAITING", "bias": "WAITING", "ema_fast": 0, "ema_slow": 0},
        "H4": {"trend": "WAITING", "bias": "WAITING", "ema_fast": 0, "ema_slow": 0},
        "D1": {"trend": "WAITING", "bias": "WAITING", "ema_fast": 0, "ema_slow": 0}
    },
    "confidence": 0,
    "market_posture": "WAITING",
    "status": "success"
}

DERIV_APP_ID = "1089"
SYMBOL = "frxXAUUSD"

def calculate_ema(prices, period):
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def analyze_candles(timeframe, candles):
    if not candles or len(candles) < 20:
        return
    closes = [float(c['close']) for c in candles]
    
    ema_9 = calculate_ema(closes, 9)
    ema_21 = calculate_ema(closes, 21)
    ema_50 = calculate_ema(closes, 50) if len(closes) >= 50 else ema_21
    
    trend = "UPTREND" if ema_9 > ema_21 else "DOWNTREND"
    bias = "BULLISH" if ema_21 > ema_50 else "BEARISH"
    
    market_state["timeframes"][timeframe] = {
        "trend": trend,
        "bias": bias,
        "ema_fast": round(ema_9, 2),
        "ema_slow": round(ema_21, 2)
    }
    calculate_confluence()

def calculate_confluence():
    bull_count = 0
    bear_count = 0
    for tf in market_state["timeframes"].values():
        if tf["bias"] == "BULLISH": bull_count += 1
        elif tf["bias"] == "BEARISH": bear_count += 1
        
    total = bull_count + bear_count
    if total == 0:
        confidence = 0
    else:
        confidence = int((max(bull_count, bear_count) / 5) * 100)
        
    market_state["confidence"] = confidence
    
    if bull_count > bear_count:
        market_state["market_posture"] = "BUY"
    elif bear_count > bull_count:
        market_state["market_posture"] = "SELL"
    else:
        market_state["market_posture"] = "WAITING"

def on_message(ws, message):
    data = json.loads(message)
    
    # 1. Handle Live Tick
    if 'tick' in data:
        market_state["current_price"] = str(data['tick']['quote'])
        
    # 2. Handle Historical Candles
    if 'candles' in data and 'echo_req' in data:
        granularity = data['echo_req'].get('granularity')
        tf_map = {300: "M5", 1800: "M30", 3600: "H1", 14400: "H4", 86400: "D1"}
        tf_name = tf_map.get(granularity)
        if tf_name:
            analyze_candles(tf_name, data['candles'])

def on_open(ws):
    # Subscribe to live ticks
    ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
    
    # Request historical candles for all timeframes
    granularities = [300, 1800, 3600, 14400, 86400] # M5, M30, H1, H4, D1
    for g in granularities:
        req = {
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": 60,
            "end": "latest",
            "start": 1,
            "style": "candles",
            "granularity": g
        }
        ws.send(json.dumps(req))

def start_ws():
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message).run_forever()

@app.route('/api/v1/market/xauusd')
def get_market_data():
    return jsonify(market_state)

if __name__ == '__main__':
    ws_thread = threading.Thread(target=start_ws)
    ws_thread.daemon = True
    ws_thread.start()
    app.run(host='0.0.0.0', port=10000)
