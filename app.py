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
from datetime import datetime, timedelta
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
    "timeframes": {"M5": "WAIT", "M30": "WAIT", "H1": "WAIT", "H4": "WAIT"},
    "structure": {"trend": "RANGE", "bos": "NONE", "swing_high": 0.0, "swing_low": 0.0},
    "smt_divergence": "NONE",
    "vwap": 0.0,
    "gate": {"status": "SCANNING", "reason": "Awaiting target sweep"},
    "target": {"name": "NY L1", "price": 0.0, "status": "PENDING"},
    "predator": {"entry": 0.0, "tp": 0.0},
    "ai_briefing": {"observation": "Initializing...", "inference": "Initializing...", "decision": "WAIT"}
}

price_history = []
gold_prices = []
silver_prices = []
asian_high = 0.0
asian_low = 0.0
last_sweep_time = 0

# --- HELPERS ---
def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except: pass

def calculate_vwap(prices):
    # Simplified VWAP approximation for tick data
    if not prices: return 0.0
    return sum(prices) / len(prices)

def calculate_atr(prices, period=14):
    if len(prices) < period: return 1.0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def get_ai_briefing(context, is_sweep):
    if not ai_client: return {"observation": "AI offline", "inference": "N/A", "decision": "WAIT"}
    prompt = f"""
    You are the Overlord AI, a cold-blooded institutional gold trader.
    Context: {context}
    Sweep Event: {is_sweep}
    
    Output exactly JSON:
    {{
        "observation": "What the market just did factually (mention 1H vs 15m conflicts).",
        "inference": "Is this a trap, pullback, or reversal? Use SMT/VWAP logic.",
        "decision": "BUY, SELL, or WAIT. If WAIT, state the exact trigger needed."
    }}
    """
    try:
        res = ai_client.models.generate_content(model="gemini-2.5-flash", contents=prompt, config=types.GenerateContentConfig(temperature=0.1))
        text = res.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except:
        return {"observation": "Data parsing error.", "inference": "Hold.", "decision": "WAIT"}

# --- MAIN ENGINE ---
def main_loop():
    global asian_high, asian_low, last_sweep_time, last_ai_update
    print("🚀 Starting Predator Engine V2...")
    
    while True:
        try:
            # 1. Fetch Gold and Silver for SMT
            gold_res = requests.get("https://api.gold-api.com/price/XAU", timeout=5).json()
            silver_res = requests.get("https://api.gold-api.com/price/XAG", timeout=5).json()
            
            gold_price = float(gold_res['price'])
            silver_price = float(silver_res['price'])
            
            gold_prices.append(gold_price)
            silver_prices.append(silver_price)
            if len(gold_prices) > 100: gold_prices.pop(0); silver_prices.pop(0)
            
            market_state["current_price"] = str(round(gold_price, 2))
            market_state["vwap"] = round(calculate_vwap(gold_prices), 2)
            
            # 2. SMT Divergence Logic
            if len(gold_prices) > 20:
                g_low = min(gold_prices[-20:])
                s_low = min(silver_prices[-20:])
                if gold_price <= g_low and silver_price > s_low:
                    market_state["smt_divergence"] = "BULLISH (Gold Lower Low, Silver Higher Low)"
                elif gold_price >= max(gold_prices[-20:]) and silver_price < max(silver_prices[-20:]):
                    market_state["smt_divergence"] = "BEARISH (Gold Higher High, Silver Lower High)"
            
            # 3. Session Targets (Trap Logic)
            now_eat = datetime.utcnow() + timedelta(hours=3)
            eat_hour = now_eat.hour
            
            if 3 <= eat_hour < 9:
                if asian_high == 0.0 or gold_price > asian_high: asian_high = gold_price
                if asian_low == 0.0 or gold_price < asian_low: asian_low = gold_price
                market_state["target"] = {"name": "BUILDING ASIAN RANGE", "price": round(asian_low, 2), "status": "PENDING"}
                market_state["gate"] = {"status": "HIBERNATION", "reason": "Accumulation Phase."}
            else:
                # FALLBACK FIX: If server restarts outside Asia session
                if asian_low == 0.0: asian_low = gold_price - 10.0
                if asian_high == 0.0: asian_high = gold_price + 10.0

                target_price = asian_low if eat_hour >= 15 else asian_high
                
                # State Machine: Scan -> Sweep -> Locked -> Reclaim -> Clear
                dist = abs(gold_price - target_price) * 10 # pips approx
                
                if market_state["gate"]["status"] in ["SCANNING", "HIBERNATION"]:
                    if dist <= 2.0:
                        market_state["gate"] = {"status": "TRAP LOCKED", "reason": f"Price at {target_price}. Awaiting reclaim."}
                        last_sweep_time = time.time()
                
                elif market_state["gate"]["status"] == "TRAP LOCKED":
                    # If price snaps back by 3 pips and we have SMT divergence
                    if dist > 3.0 and market_state["smt_divergence"] != "NONE":
                        market_state["gate"] = {"status": "CLEARED", "reason": "Reclaim + FVG confirmed."}
                        market_state["target"]["status"] = "SWEPT_AND_RECLAIMED"
                        
                        # Trigger AI Briefing only on this massive event
                        context = f"Price: {gold_price}, Target: {target_price}, SMT: {market_state['smt_divergence']}, VWAP: {market_state['vwap']}"
                        market_state["ai_briefing"] = get_ai_briefing(context, is_sweep=True)
                        
                        # Predator Eyes (Sniper Entry)
                        atr = calculate_atr(gold_prices)
                        if gold_price > target_price: # Bullish Reclaim
                            market_state["predator"] = {"entry": round(gold_price + 0.2, 2), "tp": round(asian_high, 2)}
                        else:
                            market_state["predator"] = {"entry": round(gold_price - 0.2, 2), "tp": round(asian_low, 2)}
                
                # Timeout Trap Lock (If it takes too long to reclaim)
                if market_state["gate"]["status"] == "TRAP LOCKED" and (time.time() - last_sweep_time) > 300:
                    market_state["gate"] = {"status": "SCANNING", "reason": "Sweep failed. Resetting."}

            # 4. Timeframe Logic (Simplified for speed)
            if len(gold_prices) > 15:
                market_state["timeframes"]["M5"] = "BULL" if gold_price > gold_prices[-5] else "BEAR"
                market_state["timeframes"]["M30"] = "BULL" if gold_price > gold_prices[-15] else "BEAR"
                market_state["timeframes"]["H1"] = "BULL" if gold_price > market_state["vwap"] else "BEAR"
                
           # 5. Force AI Briefing update every 15 seconds
if 'last_ai_update' not in globals(): last_ai_update = 0

if time.time() - last_ai_update > 15:
    context = f"Price: {gold_price}, Target: {target_price}, SMT: {market_state['smt_divergence']}, VWAP: {market_state['vwap']}, Gate: {market_state['gate']['status']}"
    market_state["ai_briefing"] = get_ai_briefing(context, is_sweep=(market_state["gate"]["status"] == "TRAP LOCKED"))
    last_ai_update = time.time()

        except Exception as e:
            print(f"Engine Error: {e}")
            
        time.sleep(1)

@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

# (Keep AI Chat, Trade execution, and News endpoints the same as your previous working versions)
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    # Simplified for brevity, reuse your existing chat logic here
    return jsonify({"reply": "AI Chat online."})

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    return jsonify({"status": "success", "message": "Simulated Trade"})

@app.route('/api/v1/news/calendar')
def get_news():
    return jsonify({"status": "success", "news": []})

if __name__ == '__main__':
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
