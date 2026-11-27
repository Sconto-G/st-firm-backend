from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import asyncio
import json
import websockets
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from google import genai
from google.genai import types

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DERIV_APP_ID = "1089"

if GEMINI_API_KEY:
    ai_client = genai.Client(api_key=GEMINI_API_KEY, http_options=types.HttpOptions(timeout=30000))
else:
    ai_client = None

# --- STATE ---
market_state = {
    "current_price": "0.00",
    "macro_state": "HIBERNATION",
    "structure": {"d1_bias": "WAIT", "h4_bias": "WAIT", "h1_bias": "WAIT", "playbook_mode": "NORMAL_BULL", "zone": "EQUILIBRIUM"},
    "liquidity_matrix": {
        "buyside": {"name": "Asian High", "price": 0.0, "distance": 0.0},
        "equilibrium": {"name": "VWAP", "price": 0.0, "distance": 0.0},
        "sellside": {"name": "Asian Low", "price": 0.0, "distance": 0.0}
    },
    "checklist": {"sweep": False, "displacement": False, "fvg": False},
    "execution": {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0, "risk_pct": 1.0, "lots": 0.0},
    "ai_briefing": {"observation": "Initializing...", "inference": "Initializing...", "decision": "WAIT"},
    "session_history": []
}

gold_prices = []
asian_high, asian_low = 0.0, 0.0
daily_risk_used = 0.0
last_reset_date = ""
last_ai_update = 0
latest_tick = None
last_tick_time = 0

def calculate_vwap(prices):
    if not prices: return 0.0
    return sum(prices) / len(prices)

# --- LIVE DERIV WEBSOCKET STREAM ---
async def deriv_stream():
    global latest_tick, last_tick_time
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=10) as ws:
                await ws.send(json.dumps({"ticks": "frxXAUUSD", "subscribe": 1}))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'tick' in data:
                        latest_tick = float(data['tick']['quote'])
                        last_tick_time = time.time()
        except Exception as e:
            print(f"Deriv Stream Error: {e}")
            await asyncio.sleep(3)

def start_stream():
    asyncio.run(deriv_stream())

# --- OVERLORD AI ---
def get_ai_briefing():
    fallback = {
        "observation": f"Price at {market_state['current_price']}. Bias: {market_state['structure']['d1_bias']}. Waiting for setup.",
        "inference": "Analyzing market structure and waiting for trigger conditions.",
        "decision": "WAIT"
    }
    if not ai_client: return fallback
    payload = {
        "clock": (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M"),
        "price": market_state["current_price"],
        "structure": market_state["structure"],
        "triggers": market_state["checklist"]
    }
    sys_prompt = f"""
    You are Overlord AI. Analyze this data: {json.dumps(payload)}
    Respond STRICTLY in JSON format: {{"observation": "text", "inference": "text", "decision": "BUY/SELL/WAIT"}}
    """
    try:
        res = ai_client.models.generate_content(
            model="gemini-1.5-flash", 
            contents=sys_prompt, 
            config=types.GenerateContentConfig(temperature=0.1)
        )
        text = res.text.strip()
        if "{" in text and "}" in text:
            text = text[text.find("{"):text.rfind("}")+1]
        return json.loads(text)
    except Exception as e:
        print(f"AI Error: {e}")
        return fallback

# --- THE CORE ENGINE ---
def main_loop():
    global asian_high, asian_low, daily_risk_used, last_reset_date, last_ai_update, latest_tick
    
    print("🚀 Starting Live Engine...")
    
    while True:
        try:
            # 1. PRICE FETCH: Use Deriv WebSocket if active, else fallback to Gold-API
            if latest_tick and (time.time() - last_tick_time < 5):
                gold_price = latest_tick
            else:
                # Fallback to Gold-API (Correct XAUUSD price, not PAXG)
                res = requests.get("https://api.gold-api.com/price/XAU", timeout=5).json()
                gold_price = float(res['price'])
            
            gold_prices.append(gold_price)
            if len(gold_prices) > 100: gold_prices.pop(0)
            market_state["current_price"] = str(round(gold_price, 2))
            
            # 2. Time & Daily Reset
            now_eat = datetime.now(timezone.utc) + timedelta(hours=3)
            eat_str = now_eat.strftime("%Y-%m-%d")
            eat_hour = now_eat.hour + (now_eat.minute / 60.0)
            
            if eat_str != last_reset_date and eat_hour >= 3.0:
                last_reset_date = eat_str
                daily_risk_used = 0.0
                market_state["session_history"] = []
                market_state["structure"]["d1_bias"] = "BULL" if gold_price > calculate_vwap(gold_prices) else "BEAR"
                market_state["structure"]["playbook_mode"] = "NORMAL_BULL" if market_state["structure"]["d1_bias"] == "BULL" else "MIRROR_FLIP_BEAR"

            # 3. State Machine
            if 4.0 <= eat_hour < 4.75: market_state["macro_state"] = "ASIAN_JUDAS_WINDOW"
            elif 6.0 <= eat_hour < 7.0: market_state["macro_state"] = "DOCK_1_RECOVERY_WINDOW"
            elif 10.0 <= eat_hour < 11.5: market_state["macro_state"] = "LONDON_OPEN_MACRO"
            elif 15.5 <= eat_hour < 17.0: market_state["macro_state"] = "NY_OPEN_MACRO"
            elif 17.0 <= eat_hour < 19.0: market_state["macro_state"] = "PM_DISTRIBUTION_WALL"
            else: market_state["macro_state"] = "HIBERNATION"

            # 4. Asian Range
            if 0.0 <= eat_hour < 9.0:
                if asian_high == 0.0 or gold_price > asian_high: asian_high = gold_price
                if asian_low == 0.0 or gold_price < asian_low: asian_low = gold_price
                
            if asian_low == 0.0: asian_low = gold_price - 10.0
            if asian_high == 0.0: asian_high = gold_price + 10.0

            # 5. Liquidity Matrix
            vwap = calculate_vwap(gold_prices)
            market_state["liquidity_matrix"]["buyside"] = {"name": "Asian High", "price": round(asian_high, 2), "distance": round(abs(asian_high - gold_price), 2)}
            market_state["liquidity_matrix"]["equilibrium"] = {"name": "VWAP", "price": round(vwap, 2), "distance": round(abs(vwap - gold_price), 2)}
            market_state["liquidity_matrix"]["sellside"] = {"name": "Asian Low", "price": round(asian_low, 2), "distance": round(abs(asian_low - gold_price), 2)}

            # 6. Checklist & Execution
            dist_to_low = abs(gold_price - asian_low)
            market_state["checklist"] = {"sweep": False, "displacement": False, "fvg": False}
            
            if market_state["macro_state"] in ["LONDON_OPEN_MACRO", "DOCK_1_RECOVERY_WINDOW"]:
                if dist_to_low <= 1.5:
                    market_state["checklist"]["sweep"] = True
                if market_state["checklist"]["sweep"] and gold_price > asian_low + 2.0:
                    market_state["checklist"]["displacement"] = True
                    market_state["checklist"]["fvg"] = True
                    market_state["execution"] = {
                        "grade": "A+ [3/3 GATES MET]",
                        "entry": round(gold_price, 2),
                        "sl": round(asian_low - 3.5, 2),
                        "tp": round(vwap, 2),
                        "risk_pct": 0.4,
                        "lots": 0.15
                    }
            
            # 7. AI Eval
            if time.time() - last_ai_update > 15:
                market_state["ai_briefing"] = get_ai_briefing()
                last_ai_update = time.time()

        except Exception as e:
            print(f"Engine Error: {e}")
            
        time.sleep(0.2) # SUPER FAST REFRESH

@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

if __name__ == '__main__':
    threading.Thread(target=start_stream, daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
