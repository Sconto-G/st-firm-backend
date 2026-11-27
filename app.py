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
        "buyside": {"name": "PDH", "price": 0.0, "distance": 0.0},
        "equilibrium": {"name": "VWAP", "price": 0.0, "distance": 0.0},
        "sellside": {"name": "PDL", "price": 0.0, "distance": 0.0}
    },
    "checklist": {"sweep": False, "displacement": False, "fvg": False},
    "execution": {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0, "risk_pct": 1.0, "lots": 0.0},
    "ai_briefing": {"observation": "Initializing...", "inference": "Initializing...", "decision": "WAIT"},
    "session_history": []
}

gold_prices = []
silver_prices = []
asian_high, asian_low = 0.0, 0.0
daily_risk_used = 0.0
last_reset_date = ""
last_ai_update = 0
last_trap_lock_time = 0

# --- HELPERS ---
def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except: pass

def calculate_vwap(prices):
    if not prices: return 0.0
    return sum(prices) / len(prices)

def log_event(event):
    # FIXED: Uses timezone-aware datetime
    eat_time = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M")
    market_state["session_history"].append(f"{eat_time} EAT: {event}")
    if len(market_state["session_history"]) > 5:
        market_state["session_history"].pop(0)

# --- THE OVERLORD AI BRAIN ---
def get_ai_briefing():
    if not ai_client: return {"observation": "AI offline", "inference": "N/A", "decision": "WAIT"}
    
    payload = {
        # FIXED: Uses timezone-aware datetime
        "clock": {"eat_time": (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M"), "macro_state": market_state["macro_state"]},
        "structure": market_state["structure"],
        "liquidity": market_state["liquidity_matrix"],
        "triggers": market_state["checklist"],
        "history": market_state["session_history"]
    }
    
    sys_prompt = f"""
    You are Overlord AI, a cold-blooded institutional gold trader.
    Telemetry: {json.dumps(payload)}
    
    LAW 1 (Data Authority): Use ONLY exact numbers. If price is 0 -> output DECISION: WAIT.
    LAW 2 (Gate Supremacy): If Sweep is False OR FVG is False -> FORBIDDEN from BUY/SELL.
    LAW 3 (Divergence Veto): If Volume Divergence is True -> Cut risk 50% or output WAIT.
    LAW 4 (Mirror & Invalidation): If playbook_mode is MIRROR_FLIP_BEAR, invert Dock rules to SELL.
    LAW 5 (Fixed Output): Respond strictly in JSON format with keys: "observation", "inference", "decision".
    """
    
    try:
        res = ai_client.models.generate_content(
            model="gemini-2.5-flash", 
            contents=sys_prompt, 
            config=types.GenerateContentConfig(temperature=0.1, response_mime_type="application/json")
        )
        
        text = res.text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        
        return json.loads(text)
        
    except Exception as e:
        print(f"AI Error: {e}")
        return {"observation": "AI parsing error.", "inference": "Holding.", "decision": "WAIT"}

# --- THE CORE ENGINE ---
def main_loop():
    global asian_high, asian_low, daily_risk_used, last_reset_date, last_ai_update, last_trap_lock_time
    
    print("🚀 Starting Ultimate ST-FIRM Engine...")
    
    while True:
        try:
            # 1. Fetch Gold (Binance PAXG is fast and tracks Gold 1:1)
            try:
                binance_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT", timeout=5).json()
                gold_price = float(binance_res['price'])
            except:
                gold_res = requests.get("https://api.gold-api.com/price/XAU", timeout=5).json()
                gold_price = float(gold_res['price'])

            try:
                silver_res = requests.get("https://api.gold-api.com/price/XAG", timeout=5).json()
                silver_price = float(silver_res['price'])
            except:
                silver_price = gold_price / 80.0

            gold_prices.append(gold_price); silver_prices.append(silver_price)
            if len(gold_prices) > 100: gold_prices.pop(0); silver_prices.pop(0)
            market_state["current_price"] = str(round(gold_price, 2))
            
            # 2. Time & Daily Reset (03:00 EAT)
            # FIXED: Uses timezone-aware datetime
            now_eat = datetime.now(timezone.utc) + timedelta(hours=3)
            eat_str = now_eat.strftime("%Y-%m-%d")
            eat_hour = now_eat.hour + (now_eat.minute / 60.0)
            
            if eat_str != last_reset_date and eat_hour >= 3.0:
                last_reset_date = eat_str
                daily_risk_used = 0.0
                market_state["session_history"] = []
                log_event("Daily Reset Complete. Calculating Bias...")
                market_state["structure"]["d1_bias"] = "BULL" if gold_price > calculate_vwap(gold_prices) else "BEAR"
                market_state["structure"]["playbook_mode"] = "NORMAL_BULL" if market_state["structure"]["d1_bias"] == "BULL" else "MIRROR_FLIP_BEAR"

            # 3. State Machine
            if 4.0 <= eat_hour < 4.75: market_state["macro_state"] = "ASIAN_JUDAS_WINDOW"
            elif 6.0 <= eat_hour < 7.0: market_state["macro_state"] = "DOCK_1_RECOVERY_WINDOW"
            elif 10.0 <= eat_hour < 11.5: market_state["macro_state"] = "LONDON_OPEN_MACRO"
            elif 15.5 <= eat_hour < 17.0: market_state["macro_state"] = "NY_OPEN_MACRO"
            elif 17.0 <= eat_hour < 19.0: market_state["macro_state"] = "PM_DISTRIBUTION_WALL"
            else: market_state["macro_state"] = "HIBERNATION"

            # 4. Asian Range Anchors
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
                    last_trap_lock_time = time.time()
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
            
            # 7. AI Evaluation
            if time.time() - last_ai_update > 300 or market_state["checklist"]["sweep"]:
                market_state["ai_briefing"] = get_ai_briefing()
                last_ai_update = time.time()

        except Exception as e:
            print(f"Engine Error: {e}")
            
        time.sleep(1)

# --- ROUTES ---
@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    return jsonify({"status": "success", "message": "Simulated Trade"})

if __name__ == '__main__':
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
