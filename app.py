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

# --- STATE ---
market_state = {
    "current_price": "0.00",
    "timeframes": {
        "M5": {"trend": "WAITING", "candle": "BEAR"},
        "M30": {"trend": "WAITING", "candle": "BEAR"},
        "H1": {"trend": "WAITING", "candle": "BEAR"},
        "H4": {"trend": "WAITING", "candle": "BEAR"},
        "D1": {"trend": "WAITING", "candle": "BEAR"}
    },
    "structure": {"trend": "RANGE", "bos": "NONE", "swing_high": 0.0, "swing_low": 0.0},
    "ignition": {"status": "WAITING", "reason": "Analyzing..."},
    "bloodlust": 0,
    "strategy": {
        "target_name": "WAITING",
        "target_level": 0.0,
        "playbook_state": "WAITING FOR SETUP",
        "is_locked": False
    }
}

price_history = []
asian_high = 0.0
asian_low = 0.0
smoothed_bloodlust = 0.0

# --- HELPER FUNCTIONS ---
def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e: print(f"Telegram Error: {e}")

def calculate_rsi(prices, period=14):
    if len(prices) < period: return 50
    gains, losses = 0, 0
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0: gains += diff
        else: losses -= diff
    if losses == 0: return 100
    rs = (gains/period) / (losses/period)
    return 100 - (100 / (1 + rs))

def calculate_structure(prices):
    if len(prices) < 20: return "RANGE", "NONE", 0.0, 0.0
    recent = prices[-20:]
    swing_high = max(recent)
    swing_low = min(recent)
    
    # BOS Logic
    bos = "NONE"
    if prices[-1] > swing_high * 0.999: bos = "UP"
    elif prices[-1] < swing_low * 1.001: bos = "DOWN"
    
    # Trend Logic (using half windows)
    mid = len(recent) // 2
    if sum(recent[mid:]) / mid > sum(recent[:mid]) / mid: trend = "BULLISH"
    else: trend = "BEARISH"
    
    return trend, bos, swing_high, swing_low

# --- CORE ENGINE LOOP ---
def price_loop():
    global asian_high, asian_low, smoothed_bloodlust
    print("🚀 Starting Institutional Engine...")
    
    while True:
        try:
            response = requests.get("https://api.gold-api.com/price/XAU", timeout=5)
            live_price = float(response.json()['price'])
            
            # 1. Bloodlust Smoothing
            velocity = abs(live_price - price_history[-1]) * 100 if price_history else 0
            raw_bloodlust = min(int(velocity * 10), 100)
            smoothed_bloodlust = (raw_bloodlust * 0.3) + (smoothed_bloodlust * 0.7)
            market_state["bloodlust"] = int(smoothed_bloodlust)
            
            # 2. Price & History
            market_state["current_price"] = str(round(live_price, 2))
            price_history.append(live_price)
            if len(price_history) > 100: price_history.pop(0)
            
            # 3. Independent Timeframes
            if len(price_history) > 15:
                market_state["timeframes"]["M5"] = {"trend": "BULL" if price_history[-1] > price_history[-5] else "BEAR", "candle": "BULL" if price_history[-1] > price_history[-2] else "BEAR"}
                market_state["timeframes"]["M30"] = {"trend": "BULL" if price_history[-1] > price_history[-15] else "BEAR", "candle": "BULL" if price_history[-1] > price_history[-2] else "BEAR"}
                market_state["timeframes"]["H1"] = {"trend": "BULL" if price_history[-1] > price_history[-30] else "BEAR", "candle": "BULL" if price_history[-1] > price_history[-2] else "BEAR"}
                
            # 4. Structure & BOS
            trend, bos, sh, sl = calculate_structure(price_history)
            market_state["structure"] = {"trend": trend, "bos": bos, "swing_high": round(sh, 2), "swing_low": round(sl, 2)}
            
            # 5. Session Targets (Target Lock)
            now_eat = datetime.utcnow() + timedelta(hours=3)
            eat_hour = now_eat.hour
            strat = market_state["strategy"]
            
            # Track Asian Range (00:00 - 08:00 EAT)
            if 0 <= eat_hour < 8:
                if asian_high == 0.0 or live_price > asian_high: asian_high = live_price
                if asian_low == 0.0 or live_price < asian_low: asian_low = live_price
                strat["target_name"] = "BUILDING ASIA"
                strat["target_level"] = round(asian_low, 2)
                strat["playbook_state"] = "ACCUMULATION PHASE"
                strat["is_locked"] = False
            else:
                if asian_low == 0.0: asian_low = live_price - 5.50 # Fallback
                
                # Locking the Target
                if not strat["is_locked"]:
                    if 8 <= eat_hour < 16:
                        strat["target_name"] = "LONDON H1"
                        strat["target_level"] = round(asian_high + 10.0, 2)
                    else:
                        strat["target_name"] = "NY L1 (ASIAN LOW)"
                        strat["target_level"] = round(asian_low, 2)
                    strat["is_locked"] = True # LOCK IT
                
                # Playbook State
                dist = abs(live_price - strat["target_level"]) * 10
                if dist <= 2.0:
                    strat["playbook_state"] = "TARGET HIT"
                elif live_price > strat["target_level"] and eat_hour >= 16:
                    strat["playbook_state"] = "H2 BLEED EXHAUSTED"
                else:
                    strat["playbook_state"] = "AWAITING FLUSH"

            # 6. The Ignition Logic
            rsi = calculate_rsi(price_history)
            ign = market_state["ignition"]
            
            if market_state["structure"]["trend"] == "BULLISH" and bos == "UP" and rsi < 70:
                if dist <= 5.0: # Close to target
                    ign["status"] = "BUY"
                    ign["reason"] = f"BOS UP + RSI {int(rsi)} + Near Target"
                else:
                    ign["status"] = "WAITING"
                    ign["reason"] = "Bullish structure, awaiting pullback."
            elif market_state["structure"]["trend"] == "BEARISH" and bos == "DOWN" and rsi > 30:
                if dist <= 5.0:
                    ign["status"] = "SELL"
                    ign["reason"] = f"BOS DOWN + RSI {int(rsi)} + Near Target"
                else:
                    ign["status"] = "WAITING"
                    ign["reason"] = "Bearish structure, awaiting pullback."
            else:
                ign["status"] = "WAITING"
                ign["reason"] = "Conditions not met."

        except Exception as e:
            print(f"Error: {e}")
            
        time.sleep(1)

@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

# --- AI, NEWS, TRADING (Kept clean) ---
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not ai_client: return jsonify({"reply": "Error"}), 500
    sys_prompt = f"ST-FIRM AI. Price: {market_state['current_price']}, Ignition: {market_state['ignition']['status']}. Answer briefly."
    for model in MODELS:
        try:
            res = ai_client.models.generate_content(model=model, contents=sys_prompt + "\nUser: " + request.json.get("message", ""), config=types.GenerateContentConfig(temperature=0.2))
            return jsonify({"reply": res.text})
        except: continue
    return jsonify({"reply": "AI Error"}), 500

async def async_deriv_trade(direction):
    return {"status": "success", "message": "Simulated Trade"}

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
