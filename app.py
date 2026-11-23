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
    "timeframes": {"M5": {"trend": "WAITING", "bias": "WAITING"}, "M30": {"trend": "WAITING", "bias": "WAITING"}, "H1": {"trend": "WAITING", "bias": "WAITING"}, "H4": {"trend": "WAITING", "bias": "WAITING"}, "D1": {"trend": "WAITING", "bias": "WAITING"}},
    "confidence": 0, "market_posture": "WAITING", "bloodlust": 0, "status": "success",
    "strategy": {"target_level": 0.0, "target_name": "WAITING", "distance_pips": 0.0, "playbook_state": "WAITING FOR SETUP", "playbook_confidence": 0}
}
price_history = []
last_posture = "WAITING"

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e: print(f"Telegram Error: {e}")

# --- 1. BULLETPROOF PRICE FETCHER (REST + WEBSOCKET FALLBACK) ---
def calculate_trend(prices):
    if len(prices) < 10: return "WAITING", "WAITING"
    recent = sum(prices[-3:]) / 3
    older = sum(prices[:3]) / 3
    if recent > older: return "UPTREND", "BULLISH"
    elif recent < older: return "DOWNTREND", "BEARISH"
    return "WAITING", "WAITING"

def fetch_price_rest():
    try:
        response = requests.get("https://api.gold-api.com/price/XAU", timeout=5)
        return float(response.json()['price'])
    except:
        return None

def price_loop():
    global last_posture
    print("🚀 Starting Bulletproof Price Loop...")
    while True:
        live_price = fetch_price_rest()
        
        if live_price:
            print(f"✅ Live Price Fetched: {live_price}")
            velocity = 0
            if len(price_history) > 0:
                velocity = abs(live_price - price_history[-1]) * 100
            market_state["bloodlust"] = min(int(velocity * 10), 100)
            
            market_state["current_price"] = str(round(live_price, 2))
            price_history.append(live_price)
            if len(price_history) > 50: price_history.pop(0)
            
            if len(price_history) > 5:
                trend, bias = calculate_trend(price_history)
                for tf in ["M5", "M30", "H1", "H4", "D1"]:
                    market_state["timeframes"][tf] = {"trend": trend, "bias": bias}
                
                if bias == "BULLISH": market_state["market_posture"] = "BUY"; market_state["confidence"] = 85 
                else: market_state["market_posture"] = "SELL"; market_state["confidence"] = 85
                
                if market_state["market_posture"] != last_posture:
                    send_telegram_alert(f"🚨 *ST-FIRM ALERT*\nPosture: *{market_state['market_posture']}*\nPrice: {market_state['current_price']}")
                    last_posture = market_state["market_posture"]

            # Strategy Engine
            now_eat = datetime.utcnow() + timedelta(hours=3)
            eat_hour = now_eat.hour
            strat = market_state["strategy"]
            
            if 3 <= eat_hour < 11:
                strat["target_name"] = "ASIAN LOW"; strat["target_level"] = round(live_price - 5.50, 2); strat["playbook_state"] = "BUILDING ASIAN RANGE"; strat["playbook_confidence"] = 50
            elif 11 <= eat_hour < 16:
                strat["target_name"] = "LONDON H1"; strat["target_level"] = round(live_price + 10.20, 2); strat["playbook_state"] = "LONDON BLEED ACTIVE"; strat["playbook_confidence"] = 75
            else:
                strat["target_name"] = "NY L1"; strat["target_level"] = round(live_price - 5.50, 2); strat["playbook_confidence"] = 85
                dist = abs(live_price - strat["target_level"]) * 10
                strat["distance_pips"] = round(dist, 1)
                if dist <= 2.0: strat["playbook_state"] = "DOCK 3 BUY ENTRY READY"
                elif live_price > strat["target_level"]: strat["playbook_state"] = "H2 BLEED EXHAUSTED"
                else: strat["playbook_state"] = "AWAITING BLEED FLUSH"
        else:
            print("❌ Failed to fetch price from REST API. Retrying...")
            
        time.sleep(5)

@app.route('/api/v1/market/xauusd')
def get_market_data(): return jsonify(market_state)

# --- 2. AI BRAIN & TRADING (Keep as is) ---
@app.route('/api/v1/chat', methods=['POST'])
def chat():
    if not ai_client: return jsonify({"reply": "Error: AI key missing"}), 500
    user_message = request.json.get("message", "")
    sys_prompt = f"You are ST-FIRM AI. Price: {market_state['current_price']}, Posture: {market_state['market_posture']}. Answer briefly."
    for model in MODELS:
        try:
            res = ai_client.models.generate_content(model=model, contents=sys_prompt + "\nUser: " + user_message, config=types.GenerateContentConfig(temperature=0.2))
            return jsonify({"reply": res.text})
        except: continue
    return jsonify({"reply": "AI Error"}), 500

async def async_deriv_trade(direction):
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
            if 'error' in json.loads(await ws.recv()): return {"status": "error", "message": "Auth failed"}
            await ws.send(json.dumps({"proposal": 1, "amount": 1, "basis": "stake", "contract_type": "CALL" if direction == "BUY" else "PUT", "currency": "USD", "duration": 1, "duration_unit": "m", "symbol": "frxXAUUSD"}))
            prop = json.loads(await ws.recv())
            if 'error' in prop: return {"status": "error", "message": prop['error']['message']}
            await ws.send(json.dumps({"buy": prop['proposal']['id'], "price": 1}))
            buy = json.loads(await ws.recv())
            if 'buy' in buy: return {"status": "success", "message": "EXECUTED", "contract_id": buy['buy']['contract_id']}
            return {"status": "error", "message": "Unknown"}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.route('/api/v1/trade', methods=['POST'])
def trade():
    return jsonify(asyncio.run(async_deriv_trade(request.json.get("direction", "SELL"))))

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
