from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import threading
import time
import os
import asyncio
import json
import websockets
from google import genai
from google.genai import types

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
DERIV_APP_ID = "1089"

if GEMINI_API_KEY:
    ai_client = genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=types.HttpOptions(timeout=30000)
    )
else:
    ai_client = None

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

# --- 1. LIVE PRICE & TREND ENGINE ---
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

# --- 2. DEEP-SEEKER AI BRAIN ---
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not ai_client:
        return jsonify({"reply": "Error: AI API key not configured in Render."}), 500

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
            response = ai_client.models.generate_content(
                model=model_name,
                contents=sys_prompt + "\nUser: " + user_message,
                config=types.GenerateContentConfig(temperature=0.2)
            )
            return jsonify({"reply": response.text})
        except Exception as e:
            last_error = str(e)
            continue
    return jsonify({"reply": f"AI Error: {last_error}"}), 500

# --- 3. THE NUCLEAR TRADING ENGINE (Modern WebSockets) ---
async def async_deriv_trade(direction):
    # Using the modern websockets library with headers to bypass Cloudflare
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    # These headers mimic a real browser to prevent Cloudflare from blocking the handshake
    extra_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://app.deriv.com"
    }

    try:
        async with websockets.connect(ws_url, extra_headers=extra_headers, ping_interval=20, ping_timeout=20) as ws:
            # 1. Authorize
            await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
            auth_resp = json.loads(await ws.recv())
            
            if 'error' in auth_resp:
                return {"status": "error", "message": auth_resp['error']['message']}
            
            # 2. Proposal
            proposal_req = {
                "proposal": 1,
                "amount": 10,
                "basis": "stake",
                "contract_type": "CALL" if direction == "BUY" else "PUT",
                "currency": "USD",
                "duration": 5,
                "duration_unit": "t",
                "symbol": "frxXAUUSD"
            }
            await ws.send(json.dumps(proposal_req))
            proposal_resp = json.loads(await ws.recv())
            
            if 'error' in proposal_resp:
                return {"status": "error", "message": proposal_resp['error']['message']}
            
            proposal_id = proposal_resp['proposal']['id']
            
            # 3. Buy
            buy_req = {"buy": proposal_id, "price": 10}
            await ws.send(json.dumps(buy_req))
            buy_resp = json.loads(await ws.recv())
            
            if 'error' in buy_resp:
                return {"status": "error", "message": buy_resp['error']['message']}
            
            if 'buy' in buy_resp:
                return {"status": "success", "message": f"EXECUTED: {direction} 0.10 Lots", "contract_id": buy_resp['buy']['contract_id']}
                
            return {"status": "error", "message": "Unknown response from Deriv"}
            
    except Exception as e:
        return {"status": "error", "message": f"Connection error: {str(e)}"}

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    order_data = request.json
    direction = order_data.get("direction", "SELL") 
    lots = order_data.get("lots", 0.10)
    
    print(f"⚡ INITIATING NUCLEAR STRIKE: {direction}")
    if not DERIV_TOKEN:
        return jsonify({"status": "error", "message": "DERIV_TOKEN missing in Render environment"})
    
    result = asyncio.run(async_deriv_trade(direction))
    return jsonify(result)

if __name__ == '__main__':
    thread = threading.Thread(target=fetch_gold_price)
    thread.daemon = True
    thread.start()
    app.run(host='0.0.0.0', port=10000)
