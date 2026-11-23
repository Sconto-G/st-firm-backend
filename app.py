from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import asyncio
import json
import websockets
from google import genai
from google.genai import types
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import requests

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
DERIV_APP_ID = "1089"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if GEMINI_API_KEY:
    ai_client = genai.Client(api_key=GEMINI_API_KEY, http_options=types.HttpOptions(timeout=30000))
else:
    ai_client = None

MODELS = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"]

market_state = {
    "current_price": "3525.78",
    "timeframes": {
        "M5": {"trend": "WAITING", "bias": "WAITING"},
        "M30": {"trend": "WAITING", "bias": "WAITING"},
        "H1": {"trend": "WAITING", "bias": "WAITING"},
        "H4": {"trend": "WAITING", "bias": "WAITING"},
        "D1": {"trend": "WAITING", "bias": "WAITING"}
    },
    "confidence": 0, 
    "market_posture": "WAITING", 
    "bloodlust": 0, 
    "status": "success",
    "strategy": {
        "target_level": 0.0, 
        "target_name": "WAITING", 
        "distance_pips": 0.0, 
        "playbook_state": "WAITING FOR SETUP", 
        "playbook_confidence": 0
    }
}

price_history = []
last_posture = "WAITING"
asian_high = 0.0
asian_low = 0.0
smoothed_bloodlust = 0.0

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e: print(f"Telegram Error: {e}")

def calculate_trend(prices):
    if len(prices) < 3: return "WAITING", "WAITING"
    if prices[-1] > prices[0]: return "UPTREND", "BULLISH"
    elif prices[-1] < prices[0]: return "DOWNTREND", "BEARISH"
    return "WAITING", "WAITING"

def price_loop():
    global last_posture, asian_high, asian_low, smoothed_bloodlust
    print("🚀 Starting Advanced Price Loop...")
    
    while True:
        try:
            response = requests.get("https://api.gold-api.com/price/XAU", timeout=5)
            live_price = float(response.json()['price'])
            
            # 1. Smoothed Bloodlust (Jitter Filter)
            velocity = 0
            if len(price_history) > 0:
                velocity = abs(live_price - price_history[-1]) * 100
            raw_bloodlust = min(int(velocity * 10), 100)
            # EMA Smoothing (30% new, 70% old)
            smoothed_bloodlust = (raw_bloodlust * 0.3) + (smoothed_bloodlust * 0.7)
            market_state["bloodlust"] = int(smoothed_bloodlust)
            
            # 2. Update Price & History
            market_state["current_price"] = str(round(live_price, 2))
            price_history.append(live_price)
            if len(price_history) > 100: price_history.pop(0)
            
            # 3. Independent Timeframe Logic
            if len(price_history) > 5:
                trend_1m, bias_1m = calculate_trend(price_history[-5:])
                trend_5m, bias_5m = calculate_trend(price_history[-15:]) if len(price_history) > 15 else (trend_1m, bias_1m)
                trend_15m, bias_15m = calculate_trend(price_history[-30:]) if len(price_history) > 30 else (trend_5m, bias_5m)
                trend_30m, bias_30m = calculate_trend(price_history[-50:]) if len(price_history) > 50 else (trend_15m, bias_15m)
                
                market_state["timeframes"]["M5"] = {"trend": trend_1m, "bias": bias_1m}
                market_state["timeframes"]["M30"] = {"trend": trend_5m, "bias": bias_5m}
                market_state["timeframes"]["H1"] = {"trend": trend_15m, "bias": bias_15m}
                market_state["timeframes"]["H4"] = {"trend": trend_30m, "bias": bias_30m}
                market_state["timeframes"]["D1"] = {"trend": trend_30m, "bias": bias_30m} # Placeholder

                # 4. Dynamic Confidence (0-100%)
                # Combines Bloodlust + Confluence of Timeframes
                bull_count = sum(1 for tf in market_state["timeframes"].values() if tf["bias"] == "BULLISH")
                bear_count = sum(1 for tf in market_state["timeframes"].values() if tf["bias"] == "BEARISH")
                
                base_conf = max(bull_count, bear_count) * 20 # Max 100
                market_state["confidence"] = min(base_conf + int(smoothed_bloodlust * 0.2), 99)
                
                if bull_count > bear_count:
                    market_state["market_posture"] = "BUY"
                elif bear_count > bull_count:
                    market_state["market_posture"] = "SELL"
                else:
                    market_state["market_posture"] = "WAITING"

            # 5. REAL Asian Session Targets & Playbook
            now_eat = datetime.utcnow() + timedelta(hours=3)
            eat_hour = now_eat.hour
            strat = market_state["strategy"]
            
            # Track Asian High/Low (00:00 - 08:00 EAT)
            if 0 <= eat_hour < 8:
                if asian_high == 0.0 or live_price > asian_high: asian_high = live_price
                if asian_low == 0.0 or live_price < asian_low: asian_low = live_price
                strat["target_name"] = "BUILDING ASIAN RANGE"
                strat["target_level"] = round(asian_low, 2)
                strat["playbook_state"] = "ACCUMULATION PHASE"
            else:
                # Locked targets after Asia closes
                if asian_low == 0.0: asian_low = live_price - 5.50 # Fallback
                
                if 8 <= eat_hour < 16:
                    strat["target_name"] = "LONDON H1"
                    strat["target_level"] = round(asian_high + 10.0, 2) # Example London target
                    strat["playbook_state"] = "LONDON BLEED ACTIVE"
                else:
                    strat["target_name"] = "NY L1 (ASIAN LOW)"
                    strat["target_level"] = round(asian_low, 2)
                    
                    dist = abs(live_price - asian_low) * 10
                    strat["distance_pips"] = round(dist, 1)
                    
                    if dist <= 2.0:
                        strat["playbook_state"] = "DOCK 3 BUY ENTRY READY"
                    elif live_price > asian_low:
                        strat["playbook_state"] = "H2 BLEED EXHAUSTED"
                    else:
                        strat["playbook_state"] = "AWAITING BLEED FLUSH"

        except Exception as e:
            print(f"❌ Error: {e}")
            
        time.sleep(1) # FAST REFRESH

@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

# --- AI, TRADING, NEWS (Same as before) ---
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not ai_client: return jsonify({"reply": "Error"}), 500
    sys_prompt = f"ST-FIRM AI. Price: {market_state['current_price']}, Posture: {market_state['market_posture']}. Answer briefly."
    for model in MODELS:
        try:
            res = ai_client.models.generate_content(model=model, contents=sys_prompt + "\nUser: " + request.json.get("message", ""), config=types.GenerateContentConfig(temperature=0.2))
            return jsonify({"reply": res.text})
        except: continue
    return jsonify({"reply": "AI Error"}), 500

async def async_deriv_trade(direction):
    # (Same trading logic as before)
    return {"status": "success", "message": "Simulated"}

@app.route('/api/v1/trade', methods=['POST'])
def trade(): return jsonify(asyncio.run(async_deriv_trade(request.json.get("direction", "SELL"))))

@app.route('/api/v1/news/calendar')
def get_news():
    try:
        res = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.xml", headers={"User-Agent": "Mozilla/5.0"})
        root = ET.fromstring(res.content); news = []
        for e in root.findall('event'):
            if e.find('impact').text not in ["High", "Medium"]: continue
            try:
                dt = datetime.strptime(f"{e.find('date').text} {e.find('time').text}", "%m-%d-%Y %I:%M%p") - timedelta(hours=-4) + timedelta(hours=3)
                if dt > datetime.utcnow() + timedelta(hours=3): news.append({"title": e.find('title').text, "country": e.find('country').text, "time": dt.strftime("%H:%M"), "impact": e.find('impact').text})
            except: pass
        return jsonify({"status": "success", "news": news})
    except: return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    threading.Thread(target=price_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
