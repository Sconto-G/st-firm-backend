from flask import Flask, jsonify
from flask_cors import CORS
import requests
import threading
import time

app = Flask(__name__)
CORS(app)

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
    # Compare recent prices to older prices to determine trend
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
            # Fetch live gold price from a reliable public API
            response = requests.get("https://api.gold-api.com/price/XAU", timeout=10)
            data = response.json()
            live_price = float(data['price'])
            
            market_state["current_price"] = str(round(live_price, 2))
            price_history.append(live_price)
            
            # Keep history manageable
            if len(price_history) > 50:
                price_history.pop(0)
            
            # Calculate trend if we have enough data
            if len(price_history) > 5:
                trend, bias = calculate_trend(price_history)
                
                market_state["timeframes"]["M5"] = {"trend": trend, "bias": bias}
                market_state["timeframes"]["M30"] = {"trend": trend, "bias": bias}
                market_state["timeframes"]["H1"] = {"trend": trend, "bias": bias}
                market_state["timeframes"]["H4"] = {"trend": trend, "bias": bias}
                market_state["timeframes"]["D1"] = {"trend": trend, "bias": bias}
                
                if bias == "BULLISH":
                    market_state["market_posture"] = "BUY"
                    market_state["confidence"] = 85 
                else:
                    market_state["market_posture"] = "SELL"
                    market_state["confidence"] = 85
                    
        except Exception as e:
            print(f"Error fetching data: {e}")
        
        time.sleep(5) # Update every 5 seconds

@app.route('/api/v1/market/xauusd')
def get_market_data():
    return jsonify(market_state)

if __name__ == '__main__':
    # Start the live price fetching thread
    thread = threading.Thread(target=fetch_gold_price)
    thread.daemon = True
    thread.start()
    
    app.run(host='0.0.0.0', port=10000)
