from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import threading
import time
import os
from google import genai
from google.genai import types

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# --- CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    client = genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=types.HttpOptions(timeout=30_000) # 30 seconds in milliseconds
    )
else:
    client = None

# Models to try, in order of preference
MODELS = ["gemini-3.8-flash", "gemini-2.5-flash", "gemini-2.0-flash"]

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
    You are the ST-FIRM WATCHER AI, a deadly, analytical, no-nonsense gold trader.
    You do not give generic advice or stories. You look at live data and give sharp, military-grade precision.
    Here is the current live market data:
    - XAUUSD Price: {market_state['current_price']}
    - Overall Posture: {market_state['market_posture']} (Confidence: {market_state['confidence']}%)
    - M30 Trend: {market_state['timeframes']['M30']['trend']}
    
    The user (Besti) will ask you a question. Answer directly, analyzing the data provided. 
    If Besti asks to buy or sell, check the posture and trend first. Protect capital above all else.
    """
    
    last_error = None
    for model_name in MODELS:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=sys_prompt + "\nUser: " + user_message,
                config=types.GenerateContentConfig(temperature=0. 2)
                                                   }
            return jsonify({"reply": response.text})
        except Exception as e:
            last_error = str(e)
            continue
            
    return jsonify({"reply": f"AI Error (all models failed): {last_error}"}), 500

if __name__ == '__main__':
    thread = threading.Thread(target=fetch_gold_price)
    thread.daemon = True
    thread.start()
    app.run(host='0.0.0.0', port=10000)
