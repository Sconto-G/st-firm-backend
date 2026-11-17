from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import threading
import time
import os
import json
import websocket
from datetime import datetime, timedelta
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

# --- V8 TELEMETRY STATE ---
market_state = {
    "current_price": 0.0,
    "bid": 0.0,
    "ask": 0.0,
    "spread": 0.0,
    "market_posture": "WAITING",
    "confidence": 0,
    "sessions": {
        "asia": {"high": 0.0, "low": 0.0, "status": "WAIT", "progress": 0},
        "london": {"high": 0.0, "low": 0.0, "status": "WAIT", "progress": 0},
        "ny": {"high": 0.0, "low": 0.0, "status": "WAIT", "progress": 0}
    },
    "sniper_protocol": {
        "liq_sweep": False,
        "m1_mss": False,
        "spread_guard": False,
        "gate_status": "WAIT // ACCUMULATION ONLY",
        "armed": False
    },
    "dom": {
        "bid_wall": 0.0,
        "ask_wall": 0.0,
        "delta_bias": 0
    }
}

price_history = []
sniper_armed = False

# --- SESSION & TIME LOGIC (EAT UTC+3) ---
def get_current_session():
    # Convert current UTC time to EAT (UTC+3)
    utc_now = datetime.utcnow()
    eat_now = utc_now + timedelta(hours=3)
    hour = eat_now.hour
    
    # Nairobi Time (EAT) Session Blocks
    if 3 <= hour < 8:
        return "asia"
    elif 11 <= hour < 20:
        return "london"
    elif 16 <= hour < 23 or hour >= 0:
        return "ny"
    return "closed"

# --- DERIV WEBSOCKET PRICE FEED ---
def fetch_deriv_prices():
    global market_state, price_history
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    while True:
        try:
            ws = websocket.create_connection(ws_url, timeout=15)
            # Subscribe to XAUUSD ticks
            ws.send(json.dumps({"ticks": "frxXAUUSD", "subscribe": 1}))
            
            while True:
                response = ws.recv()
                data = json.loads(response)
                
                if 'tick' in data:
                    tick = data['tick']
                    quote = float(tick['quote'])
                    bid = float(tick['bid']) if 'bid' in tick else quote - 0.08
                    ask = float(tick['ask']) if 'ask' in tick else quote + 0.08
                    
                    # Update State
                    market_state["current_price"] = round(quote, 2)
                    market_state["bid"] = round(bid, 2)
                    market_state["ask"] = round(ask, 2)
                    market_state["spread"] = round(ask - bid, 2)
                    
                    price_history.append(quote)
                    if len(price_history) > 100:
                        price_history.pop(0)
                    
                    # --- V8 LOGIC UPDATES ---
                    update_sessions(quote)
                    update_sniper_protocol(quote)
                    
        except Exception as e:
            print(f"WebSocket Error: {e}")
            time.sleep(5)

def update_sessions(current_price):
    current_session = get_current_session()
    
    # Live update highs and lows for the active session
    if current_session == "asia":
        if market_state["sessions"]["asia"]["high"] == 0 or current_price > market_state["sessions"]["asia"]["high"]:
            market_state["sessions"]["asia"]["high"] = current_price
        if market_state["sessions"]["asia"]["low"] == 0 or current_price < market_state["sessions"]["asia"]["low"]:
            market_state["sessions"]["asia"]["low"] = current_price
        market_state["sessions"]["asia"]["status"] = "IN PLAY"
    
    elif current_session == "london":
        if market_state["sessions"]["london"]["high"] == 0 or current_price > market_state["sessions"]["london"]["high"]:
            market_state["sessions"]["london"]["high"] = current_price
        if market_state["sessions"]["london"]["low"] == 0 or current_price < market_state["sessions"]["london"]["low"]:
            market_state["sessions"]["london"]["low"] = current_price
        market_state["sessions"]["london"]["status"] = "EXECUTING"

def update_sniper_protocol(current_price):
    global sniper_armed
    
    # 1. Spread Guard (Must be under 0.15)
    market_state["sniper_protocol"]["spread_guard"] = market_state["spread"] <= 0.15
    
    # 2. Liq Sweep (Simple logic: if price drops near Asian Low)
    a_low = market_state["sessions"]["asia"]["low"]
    if a_low > 0 and current_price <= (a_low + 0.50):
        market_state["sniper_protocol"]["liq_sweep"] = True
        
    # 3. M1 MSS (Simulated momentum check)
    if len(price_history) > 5:
        if price_history[-1] > price_history[-3]:
            market_state["sniper_protocol"]["m1_mss"] = True
        else:
            market_state["sniper_protocol"]["m1_mss"] = False

    # Auto-fire if Armed and LEDs are green
    if sniper_armed and market_state["sniper_protocol"]["spread_guard"] and market_state["sniper_protocol"]["m1_mss"]:
        print("🚨 V8 SNIPER TRIGGER FIRED 🚨")
        # execute_deriv_trade("BUY", 10) # Uncomment to execute!
        sniper_armed = False # Reset trigger
        market_state["sniper_protocol"]["armed"] = False

# --- API ROUTES ---
@app.route('/api/v1/market/xauusd')
def get_market_data():
    # This feeds the V8 UI
    return jsonify(market_state)

@app.route('/api/v1/arm-sniper', methods=['POST'])
def arm_sniper():
    global sniper_armed
    sniper_armed = not sniper_armed
    market_state["sniper_protocol"]["armed"] = sniper_armed
    return jsonify({"status": "success", "armed": sniper_armed})

# --- EXECUTION ENGINE ---
def execute_deriv_trade(direction, stake_amount):
    if not DERIV_TOKEN:
        return {"status": "error", "message": "DERIV_TOKEN missing"}
    
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    try:
        ws = websocket.create_connection(ws_url, timeout=15)
        ws.send(json.dumps({"authorize": DERIV_TOKEN}))
        auth_resp = json.loads(ws.recv())
        
        if 'error' in auth_resp:
            return {"status": "error", "message": auth_resp['error']['message']}
        
        trade_request = {
            "buy": 1,
            "price": stake_amount, # Dynamic stake from UI
            "parameters": {
                "amount": stake_amount,
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
        
        ws.close()
        return {"status": "success", "message": f"EXECUTED: {direction} {stake_amount} on XAUUSD"}
        
    except Exception as e:
        return {"status": "error", "message": f"Connection error: {str(e)}"}

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    order_data = request.json
    direction = order_data.get("direction", "BUY")
    stake = order_data.get("stake", 10.0)
    
    result = execute_deriv_trade(direction, stake)
    return jsonify(result)

# --- AI INTEL (DEEP SEEKER) ---
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not client:
        return jsonify({"reply": "Error: AI API key not configured."}), 500
    
    user_message = request.json.get("message", "")
    sys_prompt = f"""
    You are the ST-FIRM WATCHER AI, an elite institutional gold trader.
    Current XAUUSD Price: {market_state['current_price']}
    Session: {get_current_session().upper()}
    Asian High/Low: {market_state['sessions']['asia']['high']} / {market_state['sessions']['asia']['low']}
    Sniper Status: {market_state['sniper_protocol']['gate_status']}
    Answer Besti with extreme brevity, military precision, and focus on capital preservation.
    """
    
    for model_name in MODELS:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=sys_prompt + "\nUser: " + user_message,
                config=types.GenerateContentConfig(temperature=0.2)
            )
            return jsonify({"reply": response.text})
        except Exception as e:
            continue
            
    return jsonify({"reply": "AI System Offline. Focus on the charts."}), 500

if __name__ == '__main__':
    # Start the Deriv price feed in the background
    thread = threading.Thread(target=fetch_deriv_prices)
    thread.daemon = True
    thread.start()
    
    app.run(host='0.0.0.0', port=10000)
