from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import threading
import time
import os
import json
import websocket
from google import genai
from google.genai import types

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
DERIV_APP_ID = "1089"

if GEMINI_API_KEY:
    client = genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=types.HttpOptions(timeout=30000)
    )
else:
    client = None

MODELS = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"]

market_state = {
    "current_price": "4586.11",
    "timeframes": {
        "M5": {"trend": "WAITING", "bias": "WAITING"},
        "M30": {"trend": "WAITING", "bias": "WAITING"},
        "H1": {"trend": "WAITING", "bias": "WAITING"},
        "H4": {"trend": "WAITING", "bias": "WAITING"},
        "D1": {"trend": "WAITING", "bias": "WAITING"}
    },
    "confidence": 0,
    "market_posture": "WAITING",
    "status": "success"
}

price_history = []

def calculate_trend(prices):
    if len(prices) < 10:
        return "WAITING", "WAITING"
    recent = sum(prices[-3:]) / 3
    older = sum(prices[:3]) / 3
    if recent > older:
        return "UPTREND", "BULLISH"
    elif recent < older:
        return "DOWNTREND", "BEARISH"
    return "WAITING", "WAITING"

def fetch_gold_price():
    while True:
        try:
            response = requests.get("https://api.gold-api.com/price/XAU", timeout=10)
            data = response.json()
            live_price = float(data['price'])
            
            market_state["current_price"] = str(round(live_price, 2))
            price_history.append(live_price)
            
            if len(price_history) > 50:
                price_history.pop(0)
            
            if len(price_history) > 5:
                trend, bias = calculate_trend(price_history)
                for tf in ["M5", "M30", "H1", "H4", "D1"]:
                    market_state["timeframes"][tf] = {"trend": trend, "bias": bias}
                
                if bias == "BULLISH":
                    market_state["market_posture"] = "BUY"
                    market_state["confidence"] = 85 
                else:
                    market_state["market_posture"] = "SELL"
                    market_state["confidence"] = 85
                    
        except Exception as e:
            print(f"Error fetching data: {e}")
        
        time.sleep(5)

@app.route('/api/v1/market/xauusd')
def get_market_data():
    return jsonify(market_state)

@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not client:
        return jsonify({"reply": "Error: AI API key not configured."}), 500

    user_message = request.json.get("message", "")
    sys_prompt = f"""
    You are the ST-FIRM WATCHER AI, an elite, cold-blooded institutional gold trader.
    You are not a teacher. You are a weapon designed to extract liquidity from the market.
    Here is the current live market data:
    - XAUUSD Price: {market_state['current_price']}
    - Overall Posture: {market_state['market_posture']} (Confidence: {market_state['confidence']}%)
    - M30 Trend: {market_state['timeframes']['M30']['trend']}
    Answer Besti with extreme brevity, military precision, and focus on capital preservation.
    """
    
    last_error = None
    for model_name in MODELS:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=sys_prompt + "\nUser: " + user_message,
                config=types.GenerateContentConfig(temperature=0.2)
            )
            return jsonify({"reply": response.text})
        except Exception as e:
            last_error = str(e)
            continue
    return jsonify({"reply": f"AI Error: {last_error}"}), 500

# --- THE NUCLEAR TRADING ENGINE ---
def execute_deriv_trade(direction, lots):
    if not DERIV_TOKEN:
        return {"status": "error", "message": "DERIV_TOKEN missing in Render environment"}

    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    try:
        import websocket
        ws = websocket.create_connection(ws_url, timeout=15)
        
        # 1. Authorize
        ws.send(json.dumps({"authorize": DERIV_TOKEN}))
        auth_resp = json.loads(ws.recv())
        if 'error' in auth_resp:
            return {"status": "error", "message": auth_resp['error']['message']}
            
        # 2. Send Trade (Using CALL/PUT for 5 ticks, $10 stake)
        trade_request = {
            "buy": 1,
            "price": 100, 
            "parameters": {
                "amount": 10, 
                "basis": "stake", 
                "contract_type": "CALL" if direction == "BUY" else "PUT",
                "currency": "USD", 
                "duration": 5, 
                "duration_unit": "t", 
                "symbol": "frxXAUUSD"
            }
        }
        ws.send(json.dumps(trade_request))
        trade_resp = json.loads(ws.recv())
        
        if 'error' in trade_resp:
            return {"status": "error", "message": trade_resp['error']['message']}
            
        if 'buy' in trade_resp:
            ws.close()
            return {"status": "success", "message": f"EXECUTED: {direction} {lots} Lots", "contract_id": trade_resp['buy']['contract_id']}
            
        ws.close()
        return {"status": "error", "message": "Unknown response from Deriv"}
        
    except Exception as e:
        return {"status": "error", "message": f"Connection error: {str(e)}"}

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    order_data = request.json
    direction = order_data.get("direction", "SELL") 
    lots = order_data.get("lots", 0.10)
    
    print(f"⚡ INITIATING NUCLEAR STRIKE: {direction}")
    result = execute_deriv_trade(direction, lots)
    return jsonify(result)

if __name__ == '__main__':
    thread = threading.Thread(target=fetch_gold_price)
    thread.daemon = True
    thread.start()
    app.run(host='0.0.0.0', port=10000)
