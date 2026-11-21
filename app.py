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
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
DERIV_APP_ID = "1089"

# TELEGRAM CONFIGURATION
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
    "bloodlust": 0,
    "status": "success"
}

price_history = []
last_posture = "WAITING" 

# --- TELEGRAM ALERT ENGINE ---
def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram keys missing. Skipping alert.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=5)
        print("Telegram alert sent!")
    except Exception as e:
        print(f"Telegram Error: {e}")

# --- 1. LIVE PRICE, BLOODLUST & AUTOPILOT ENGINE ---
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
    global last_posture
    while True:
        try:
            response = requests.get("https://api.gold-api.com/price/XAU", timeout=10)
            data = response.json()
            live_price = float(data['price'])
            
            # Calculate Market Velocity (Bloodlust)
            velocity = 0
            if len(price_history) > 1:
                velocity = abs(live_price - price_history[-1]) * 100 
            
            # Normalize to a 0-100 scale
            market_state["bloodlust"] = min(int(velocity * 10), 100)

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
                
                # --- AUTO-PILOT & TELEGRAM LOGIC ---
                if market_state["market_posture"] != last_posture:
                    alert_msg = f"🚨 *ST-FIRM ALERT*\n\nPosture flipped to: *{market_state['market_posture']}*\nConfidence: {market_state['confidence']}%\nPrice: {market_state['current_price']}"
                    send_telegram_alert(alert_msg)
                    
                    print(f"🤖 AUTOPILOT EXECUTING: {market_state['market_posture']}")
                    try:
                        asyncio.run(async_deriv_trade(market_state["market_posture"]))
                        send_telegram_alert(f"🤖 *AUTOPILOT EXECUTED*\nFired: {market_state['market_posture']}")
                    except Exception as e:
                        print(f"Autopilot failed: {e}")
                        
                    last_posture = market_state["market_posture"]
                        
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

# --- 3. THE NUCLEAR TRADING ENGINE ---
async def async_deriv_trade(direction):
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    
    extra_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://app.deriv.com"
    }

    try:
        async with websockets.connect(ws_url, additional_headers=extra_headers, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
            auth_resp = json.loads(await ws.recv())
            
            if 'error' in auth_resp:
                return {"status": "error", "message": auth_resp['error']['message']}
            
            proposal_req = {
                "proposal": 1, "amount": 10, "basis": "stake",
                "contract_type": "CALL" if direction == "BUY" else "PUT",
                "currency": "USD", "duration": 5, "duration_unit": "t",
                "symbol": "frxXAUUSD"
            }
            await ws.send(json.dumps(proposal_req))
            proposal_resp = json.loads(await ws.recv())
            
            if 'error' in proposal_resp:
                return {"status": "error", "message": proposal_resp['error']['message']}
            
            proposal_id = proposal_resp['proposal']['id']
            
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
    
    print(f"⚡ INITIATING NUCLEAR STRIKE: {direction}")
    if not DERIV_TOKEN:
        return jsonify({"status": "error", "message": "DERIV_TOKEN missing in Render environment"})
    
    result = asyncio.run(async_deriv_trade(direction))
    return jsonify(result)

# --- 4. LIVE FOREX FACTORY NEWS ENGINE ---
@app.route('/api/v1/news/calendar')
def get_economic_calendar():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return jsonify({"status": "error", "message": "Failed to fetch news feed"}), 500

        root = ET.fromstring(response.content)
        news_events = []
        
        now_utc = datetime.utcnow()
        ny_offset = -4 if 3 < now_utc.month < 11 else -5
        
        for event in root.findall('event'):
            try:
                impact = event.find('impact').text
                if impact not in ["High", "Medium"]:
                    continue

                date_str = event.find('date').text
                time_str = event.find('time').text
                
                if not time_str or "day" in time_str.lower() or ":" not in time_str:
                    continue

                event_dt_str = f"{date_str} {time_str}"
                event_ny = datetime.strptime(event_dt_str, "%m-%d-%Y %I:%M%p")
                event_utc = event_ny - timedelta(hours=ny_offset)
                event_eat = event_utc + timedelta(hours=3)

                if event_eat < (now_utc + timedelta(hours=3) - timedelta(minutes=5)):
                    continue

                news_events.append({
                    "title": event.find('title').text,
                    "country": event.find('country').text,
                    "time": event_eat.strftime("%H:%M"),
                    "impact": impact,
                    "timestamp": event_eat.timestamp()
                })
            except Exception as e:
                continue 

        news_events.sort(key=lambda x: x["timestamp"])
        return jsonify({"status": "success", "news": news_events})
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    thread = threading.Thread(target=fetch_gold_price)
    thread.daemon = True
    thread.start()
    app.run(host='0.0.0.0', port=10000)
