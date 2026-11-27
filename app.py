"""
ST-FIRM WATCHER backend  --  Phase 1
- Price feed that tells you WHY it has no price (see /api/v1/debug)
- Every log line is flushed, so Render shows errors immediately
- Engine, AI and feeds each run in their own thread (AI can no longer freeze prices)
- Asian range + session average reset every EAT day
- Sweep -> displacement -> FVG is now a real sequence with memory (1-minute candles built from ticks)
- /chat works; /trade and /campaign/action are SAFE STUBS (analysis only, no orders)
"""
import os
import json
import time
import asyncio
import threading
from datetime import datetime, timedelta, timezone

import requests
import websockets
from flask import Flask, jsonify, request
from flask_cors import CORS
from google import genai
from google.genai import types

app = Flask(__name__)
CORS(app)  # TODO before execution exists: restrict to your Vercel domain

# ------------------------------------------------------------------ CONFIG
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")  # change in Render env if Google renames it
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")

ASIAN_START, ASIAN_END = 0.0, 9.0   # EAT hours, same as your original
SWEEP_DISPLACEMENT = 2.0            # points above Asian low = displacement
SL_BUFFER = 3.5                     # points under the sweep low
RR = 2.0                            # take-profit = entry + RR * risk
SETUP_LIFETIME = 30 * 60            # seconds an A+ setup stays on screen

ai_client = None
if GEMINI_API_KEY:
    ai_client = genai.Client(api_key=GEMINI_API_KEY, http_options=types.HttpOptions(timeout=30000))


def log(*a):
    print(*a, flush=True)


# ------------------------------------------------------------------ STATE
STARTED = time.time()

feed = {"price": None, "source": None, "ts": 0.0, "ticks": 0,
        "deriv_error": None, "rest_error": None}

market_state = {
    "current_price": "0.00",
    "macro_state": "HIBERNATION",
    "structure": {"d1_bias": "WAIT", "h4_bias": "WAIT", "h1_bias": "WAIT",
                  "playbook_mode": "NORMAL_BULL", "zone": "EQUILIBRIUM"},
    "liquidity_matrix": {
        "buyside": {"name": "Asian High", "price": 0.0, "distance": 0.0},
        "equilibrium": {"name": "SESSION AVG", "price": 0.0, "distance": 0.0},
        "sellside": {"name": "Asian Low", "price": 0.0, "distance": 0.0},
    },
    "checklist": {"sweep": False, "displacement": False, "fvg": False},
    "execution": {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0, "risk_pct": 1.0, "lots": 0.0},
    "ai_briefing": {"observation": "Initializing...", "inference": "Initializing...", "decision": "WAIT"},
    "feed": {"ok": False, "source": None, "age_sec": None},
    "range_ready": False,
    "uptime_sec": 0,
    "session_history": [],
}

day = {"date": "", "asian_high": 0.0, "asian_low": 0.0, "sum": 0.0, "n": 0,
       "swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0}

candles = []  # 1-minute candles built from ticks


def set_price(p, source):
    feed["price"] = p
    feed["source"] = source
    feed["ts"] = time.time()
    feed["ticks"] += 1


def update_candles(price, now_ts):
    minute = int(now_ts // 60)
    if candles and candles[-1]["m"] == minute:
        c = candles[-1]
        c["h"] = max(c["h"], price)
        c["l"] = min(c["l"], price)
        c["c"] = price
    else:
        candles.append({"m": minute, "o": price, "h": price, "l": price, "c": price})
        if len(candles) > 120:
            candles.pop(0)


def bullish_fvg():
    """Last 3 CLOSED candles: gap between candle1 high and candle3 low."""
    if len(candles) < 4:
        return False
    c1, c3 = candles[-4], candles[-2]
    return c3["l"] > c1["h"]


# ------------------------------------------------------------------ PRICE FEEDS
async def deriv_stream():
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, open_timeout=15) as ws:
                await ws.send(json.dumps({"ticks": "frxXAUUSD", "subscribe": 1}))
                log("Deriv: connected, subscribed to frxXAUUSD")
                while True:
                    data = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if "error" in data:
                        raise RuntimeError(data["error"].get("message", "deriv error"))
                    if "tick" in data:
                        set_price(float(data["tick"]["quote"]), "deriv")
                        feed["deriv_error"] = None
        except Exception as e:
            feed["deriv_error"] = f"{type(e).__name__}: {e}"
            log("Deriv stream error:", feed["deriv_error"])
            await asyncio.sleep(5)


def start_deriv():
    asyncio.run(deriv_stream())


def rest_fallback():
    """Only runs when Deriv has been silent for 5s. Polls gently."""
    while True:
        try:
            if time.time() - feed["ts"] > 5:
                r = requests.get("https://api.gold-api.com/price/XAU", timeout=(3, 5))
                r.raise_for_status()
                set_price(float(r.json()["price"]), "gold-api")
                feed["rest_error"] = None
        except Exception as e:
            feed["rest_error"] = f"{type(e).__name__}: {e}"
            log("REST fallback error:", feed["rest_error"])
        time.sleep(3)


# ------------------------------------------------------------------ ENGINE
def reset_day(date_str):
    day.update({"date": date_str, "asian_high": 0.0, "asian_low": 0.0, "sum": 0.0, "n": 0,
                "swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0})
    market_state["execution"] = {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0,
                                 "risk_pct": 1.0, "lots": 0.0}
    market_state["session_history"] = []
    log("New EAT day:", date_str, "- ranges reset")


def engine_step():
    now = datetime.now(timezone.utc) + timedelta(hours=3)
    date_str = now.strftime("%Y-%m-%d")
    h = now.hour + now.minute / 60.0

    age = time.time() - feed["ts"] if feed["ts"] else None
    fresh = age is not None and age < 30
    market_state["feed"] = {"ok": fresh, "source": feed["source"],
                            "age_sec": round(age, 1) if age is not None else None}
    market_state["uptime_sec"] = int(time.time() - STARTED)
    if not fresh:
        return
    price = feed["price"]

    if date_str != day["date"]:
        reset_day(date_str)

    market_state["current_price"] = str(round(price, 2))
    update_candles(price, time.time())
    day["sum"] += price
    day["n"] += 1
    avg = day["sum"] / day["n"]  # session AVERAGE price (no volume in this feed, so not true VWAP)

    # macro state (unchanged windows)
    if 4.0 <= h < 4.75:
        ms = "ASIAN_JUDAS_WINDOW"
    elif 6.0 <= h < 7.0:
        ms = "DOCK_1_RECOVERY_WINDOW"
    elif 10.0 <= h < 11.5:
        ms = "LONDON_OPEN_MACRO"
    elif 15.5 <= h < 17.0:
        ms = "NY_OPEN_MACRO"
    elif 17.0 <= h < 19.0:
        ms = "PM_DISTRIBUTION_WALL"
    else:
        ms = "HIBERNATION"
    market_state["macro_state"] = ms

    # Asian range builds 00:00-09:00 EAT, then freezes
    if ASIAN_START <= h < ASIAN_END:
        if day["asian_high"] == 0.0 or price > day["asian_high"]:
            day["asian_high"] = price
        if day["asian_low"] == 0.0 or price < day["asian_low"]:
            day["asian_low"] = price
    range_ready = h >= ASIAN_END and day["asian_low"] > 0.0
    market_state["range_ready"] = range_ready

    # bias (temporary): price vs session average
    market_state["structure"]["d1_bias"] = "BULL" if price > avg else "BEAR"
    market_state["structure"]["playbook_mode"] = "NORMAL_BULL" if price > avg else "MIRROR_FLIP_BEAR"

    # liquidity matrix
    ah, al = day["asian_high"], day["asian_low"]
    lm = market_state["liquidity_matrix"]
    lm["buyside"] = {"name": "Asian High", "price": round(ah, 2), "distance": round(abs(ah - price), 2) if ah else 0.0}
    lm["equilibrium"] = {"name": "SESSION AVG", "price": round(avg, 2), "distance": round(abs(avg - price), 2)}
    lm["sellside"] = {"name": "Asian Low", "price": round(al, 2), "distance": round(abs(al - price), 2) if al else 0.0}

    # sweep -> displacement -> FVG, with memory
    if range_ready and ms in ("LONDON_OPEN_MACRO", "NY_OPEN_MACRO"):
        if price < al:
            day["swept"] = True
            day["sweep_low"] = price if day["sweep_low"] is None else min(day["sweep_low"], price)
        if day["swept"] and price > al + SWEEP_DISPLACEMENT:
            day["displaced"] = True
        if day["swept"] and bullish_fvg():
            day["fvg"] = True
        if day["swept"] and day["displaced"] and day["fvg"] and day["exec_ts"] == 0.0:
            sl = min(day["sweep_low"], al) - SL_BUFFER
            risk = price - sl
            market_state["execution"] = {
                "grade": "A+ [3/3 GATES MET]",
                "entry": round(price, 2), "sl": round(sl, 2),
                "tp": round(price + RR * risk, 2),
                "risk_pct": 0.4, "lots": 0.0,  # lots: needs account size, set later
            }
            day["exec_ts"] = time.time()
            log("A+ setup latched:", market_state["execution"])

    if day["exec_ts"] and time.time() - day["exec_ts"] > SETUP_LIFETIME:
        day.update({"swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0})
        market_state["execution"] = {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0,
                                     "risk_pct": 1.0, "lots": 0.0}

    market_state["checklist"] = {"sweep": day["swept"], "displacement": day["displaced"], "fvg": day["fvg"]}


def engine_loop():
    last_hb = 0
    log("Engine thread started")
    while True:
        try:
            engine_step()
            if time.time() - last_hb > 60:
                last_hb = time.time()
                log(f"heartbeat price={market_state['current_price']} src={feed['source']} "
                    f"state={market_state['macro_state']} ticks={feed['ticks']}")
        except Exception as e:
            log("Engine error:", type(e).__name__, e)
            time.sleep(2)
        time.sleep(0.5)


# ------------------------------------------------------------------ AI
def ai_context():
    return {"clock_eat": (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M"),
            "price": market_state["current_price"],
            "macro_state": market_state["macro_state"],
            "structure": market_state["structure"],
            "liquidity": market_state["liquidity_matrix"],
            "checklist": market_state["checklist"],
            "execution": market_state["execution"]}


def get_ai_briefing():
    fallback = {"observation": f"Price {market_state['current_price']}. State {market_state['macro_state']}.",
                "inference": "AI offline or waiting for data.", "decision": "WAIT"}
    if not ai_client:
        return fallback
    prompt = ("You are the analysis assistant of a gold dashboard. Use ONLY this data, never invent levels. "
              f"DATA: {json.dumps(ai_context())}\n"
              'Reply ONLY JSON: {"observation": "...", "inference": "...", "decision": "BUY/SELL/WAIT"}')
    try:
        res = ai_client.models.generate_content(model=GEMINI_MODEL, contents=prompt,
                                                config=types.GenerateContentConfig(temperature=0.1))
        text = res.text.strip()
        text = text[text.find("{"):text.rfind("}") + 1]
        return json.loads(text)
    except Exception as e:
        log("AI briefing error:", type(e).__name__, e)
        return fallback


def ai_loop():
    while True:
        if market_state["feed"]["ok"]:
            market_state["ai_briefing"] = get_ai_briefing()
        time.sleep(20)


# ------------------------------------------------------------------ ROUTES
@app.route("/")
def root():
    return "ST-FIRM backend OK", 200


@app.route("/api/v1/market/xauusd")
def get_market_data():
    return jsonify(market_state)


@app.route("/api/v1/debug")
def debug():
    return jsonify({
        "uptime_sec": int(time.time() - STARTED),
        "feed": {**feed, "age_sec": round(time.time() - feed["ts"], 1) if feed["ts"] else None},
        "threads": {t.name: t.is_alive() for t in threading.enumerate()},
        "candles": len(candles),
        "day": {k: day[k] for k in ("date", "asian_high", "asian_low", "n", "swept", "displaced", "fvg")},
        "gemini_key_set": bool(GEMINI_API_KEY),
        "gemini_model": GEMINI_MODEL,
    })


@app.route("/api/v1/chat", methods=["POST"])
def chat():
    msg = str((request.get_json(silent=True) or {}).get("message", "")).strip()[:1000]
    if not msg:
        return jsonify({"reply": "Type a question first."}), 400
    if not ai_client:
        return jsonify({"reply": "AI core is offline: GEMINI_API_KEY is not set on the server."})
    prompt = ("You are the assistant of a gold (XAUUSD) dashboard. Answer using ONLY the live data below; "
              "if it is not in the data, say you don't have it. Be brief. This is analysis, not financial advice, "
              "and never promise outcomes.\n"
              f"DATA: {json.dumps(ai_context())}\nUSER: {msg}")
    try:
        res = ai_client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        return jsonify({"reply": res.text.strip()})
    except Exception as e:
        log("Chat error:", type(e).__name__, e)
        return jsonify({"reply": f"AI core error ({type(e).__name__}). Check model name / key."}), 502


def _disabled():
    return jsonify({"status": "disabled",
                    "message": "Execution is OFF. This build is analysis-only; no orders are sent."})


@app.route("/api/v1/trade", methods=["POST"])
def trade():
    return _disabled()


@app.route("/api/v1/campaign/action", methods=["POST"])
def campaign_action():
    return _disabled()


# ------------------------------------------------------------------ BOOT
if __name__ == "__main__":
    log("Starting ST-FIRM backend (phase 1)")
    for fn, name in ((start_deriv, "deriv"), (rest_fallback, "rest"), (engine_loop, "engine"), (ai_loop, "ai")):
        threading.Thread(target=fn, name=name, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
