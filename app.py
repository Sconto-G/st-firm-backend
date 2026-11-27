"""
ST-FIRM WATCHER backend  v2
Live price now comes from sources Render can actually reach (probe-verified):
  1. Binance PAXGUSDT  (polled every 1s, keep-alive session)   <- main feed
  2. Kraken  PAXGUSD   (only when Binance is silent)
  3. Deriv websocket   (OFF by default: it returns HTTP 520 to Render. Set ENABLE_DERIV=1 to retry)
PAXG is a gold-backed token, so it is a PROXY for XAUUSD. If TWELVEDATA_API_KEY is set, the real
XAU/USD quote is fetched every 150s and the gap (offset) is added to PAXG, so the HUD price tracks spot.
Without that key the HUD marks the price as PROXY.
Analysis only: /trade and /campaign/action are disabled stubs. No orders are ever sent.
"""
import os
import json
import time
import asyncio
import threading
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    from google import genai
    from google.genai import types
except Exception:  # app must still boot if the AI package is missing
    genai = None
    types = None

app = Flask(__name__)
CORS(app)  # TODO before any execution exists: restrict to your Vercel domain

# ------------------------------------------------------------------ CONFIG
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")
ENABLE_DERIV = os.environ.get("ENABLE_DERIV", "0") == "1"
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
PROBE_ON_START = os.environ.get("PROBE_ON_START", "0") == "1"

ASIAN_START, ASIAN_END = 0.0, 9.0   # EAT hours
SWEEP_DISPLACEMENT = 2.0            # points above Asian low
SL_BUFFER = 3.5                     # points below the sweep low
RR = 2.0                            # TP = entry + RR * risk
SETUP_LIFETIME = 30 * 60            # seconds an A+ setup stays on screen
STALE_AFTER = 15                    # seconds without a tick = feed down
CAL_MAX_AGE = 6 * 3600              # a calibration older than this is ignored

ai_client = None
if GEMINI_API_KEY and genai is not None:
    try:
        ai_client = genai.Client(api_key=GEMINI_API_KEY, http_options=types.HttpOptions(timeout=30000))
    except Exception as _e:
        print("WARN AI client could not start:", _e, flush=True)


def log(*a):
    print(*a, flush=True)


_last_warn = {}


def warn(key, msg, every=60):
    """Rate-limited log so a down source cannot flood the logs."""
    now = time.time()
    if now - _last_warn.get(key, 0) >= every:
        _last_warn[key] = now
        log("WARN", key, msg)


# ------------------------------------------------------------------ STATE
STARTED = time.time()

raw = {"price": None, "source": None, "ts": 0.0, "ticks": 0}
cal = {"offset": 0.0, "ts": 0.0, "ref": None, "ref_ts": 0.0}
feed_err = {}

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
    "feed": {"ok": False, "source": None, "age_sec": None, "proxy": True, "calibrated": False, "offset": 0.0},
    "range_ready": False,
    "uptime_sec": 0,
    "session_history": [],
}

day = {"date": "", "asian_high": 0.0, "asian_low": 0.0, "sum": 0.0, "n": 0,
       "swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0}

candles = []  # 1-minute candles built from the price


def set_raw(p, source):
    raw["price"] = p
    raw["source"] = source
    raw["ts"] = time.time()
    raw["ticks"] += 1


def output_price():
    """Returns (price or None, feed-meta). Applies the XAU/PAXG offset when we have a fresh one."""
    now = time.time()
    age = now - raw["ts"] if raw["ts"] else None
    if raw["price"] is None or age is None or age > STALE_AFTER:
        return None, {"ok": False, "source": raw["source"], "age_sec": round(age, 1) if age is not None else None,
                      "proxy": True, "calibrated": False, "offset": 0.0}
    exact = raw["source"] == "deriv"   # Deriv quotes real spot, no offset needed
    use_off = (not exact) and cal["ts"] > 0 and (now - cal["ts"]) < CAL_MAX_AGE
    price = raw["price"] + (cal["offset"] if use_off else 0.0)
    return price, {"ok": True, "source": raw["source"], "age_sec": round(age, 1),
                   "proxy": (not exact) and (not use_off), "calibrated": bool(use_off),
                   "offset": round(cal["offset"], 2) if use_off else 0.0}


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
    """Gap between candle 1 high and candle 3 low, using the last 3 CLOSED candles."""
    if len(candles) < 4:
        return False
    c1, c3 = candles[-4], candles[-2]
    return c3["l"] > c1["h"]


# ------------------------------------------------------------------ PRICE FEEDS
def poll_binance():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    fails = 0
    while True:
        try:
            r = s.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": "PAXGUSDT"}, timeout=(4, 4))
            r.raise_for_status()
            set_raw(float(r.json()["price"]), "binance-paxg")
            if fails:
                log("Binance feed recovered")
            fails = 0
            feed_err.pop("binance", None)
        except Exception as e:
            fails += 1
            feed_err["binance"] = f"{type(e).__name__}: {str(e)[:100]}"
            warn("binance", feed_err["binance"])
        time.sleep(1 if fails == 0 else min(2 + fails, 10))


def poll_kraken():
    """Backup only: runs when Binance has been silent for 5s."""
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    while True:
        try:
            if time.time() - raw["ts"] > 5:
                r = s.get("https://api.kraken.com/0/public/Ticker", params={"pair": "PAXGUSD"}, timeout=(4, 5))
                r.raise_for_status()
                k = next(iter(r.json()["result"].values()))
                set_raw(float(k["c"][0]), "kraken-paxg")
                feed_err.pop("kraken", None)
        except Exception as e:
            feed_err["kraken"] = f"{type(e).__name__}: {str(e)[:100]}"
            warn("kraken", feed_err["kraken"])
        time.sleep(2)


def calibrate_loop():
    """Real XAU/USD every 150s (free plan = 800 calls/day) -> offset between spot and the PAXG proxy."""
    if not TWELVEDATA_API_KEY:
        log("Calibration OFF: set TWELVEDATA_API_KEY to align the PAXG proxy with XAU/USD")
        return
    s = requests.Session()
    while True:
        wait = 150
        try:
            fresh = raw["price"] is not None and time.time() - raw["ts"] < STALE_AFTER and raw["source"] != "deriv"
            if not fresh:
                time.sleep(3)   # no proxy price yet (startup) - wait for it instead of burning a call
                continue
            r = s.get("https://api.twelvedata.com/price",
                      params={"symbol": "XAU/USD", "apikey": TWELVEDATA_API_KEY}, timeout=(4, 6))
            j = r.json()
            if "price" not in j:
                raise RuntimeError(str(j.get("message", j))[:140])
            ref = float(j["price"])
            cal["ref"], cal["ref_ts"] = ref, time.time()
            fresh = raw["price"] is not None and time.time() - raw["ts"] < STALE_AFTER and raw["source"] != "deriv"
            if fresh:
                new = ref - raw["price"]
                if abs(new) <= 0.03 * ref:
                    cal["offset"] = new if cal["ts"] == 0 else 0.6 * new + 0.4 * cal["offset"]
                    cal["ts"] = time.time()
                    log(f"Calibrated: XAU={ref} proxy={raw['price']} offset={cal['offset']:.2f}")
                else:
                    warn("calibrate", f"offset {new:.1f} looks wrong, ignored")
            feed_err.pop("twelvedata", None)
        except Exception as e:
            feed_err["twelvedata"] = f"{type(e).__name__}: {str(e)[:140]}"
            warn("twelvedata", feed_err["twelvedata"], every=300)
            wait = 60   # a bad key / hiccup: try again in a minute, not 2.5
        time.sleep(wait)


async def _deriv_stream():
    import websockets
    url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, open_timeout=15) as ws:
                await ws.send(json.dumps({"ticks": "frxXAUUSD", "subscribe": 1}))
                log("Deriv connected")
                while True:
                    data = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if "error" in data:
                        raise RuntimeError(data["error"].get("message", "deriv"))
                    if "tick" in data:
                        set_raw(float(data["tick"]["quote"]), "deriv")
                        feed_err.pop("deriv", None)
        except Exception as e:
            feed_err["deriv"] = f"{type(e).__name__}: {str(e)[:100]}"
            warn("deriv", feed_err["deriv"], every=300)
            await asyncio.sleep(60)


def start_deriv():
    asyncio.run(_deriv_stream())


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

    price, meta = output_price()
    market_state["feed"] = meta
    market_state["uptime_sec"] = int(time.time() - STARTED)
    if price is None:
        return

    if date_str != day["date"]:
        reset_day(date_str)

    market_state["current_price"] = str(round(price, 2))
    update_candles(price, time.time())
    day["sum"] += price
    day["n"] += 1
    avg = day["sum"] / day["n"]  # session AVERAGE price (no volume data, so not a true VWAP)

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

    if ASIAN_START <= h < ASIAN_END:
        if day["asian_high"] == 0.0 or price > day["asian_high"]:
            day["asian_high"] = price
        if day["asian_low"] == 0.0 or price < day["asian_low"]:
            day["asian_low"] = price
    range_ready = h >= ASIAN_END and day["asian_low"] > 0.0
    market_state["range_ready"] = range_ready

    market_state["structure"]["d1_bias"] = "BULL" if price > avg else "BEAR"   # temporary rule
    market_state["structure"]["playbook_mode"] = "NORMAL_BULL" if price > avg else "MIRROR_FLIP_BEAR"

    ah, al = day["asian_high"], day["asian_low"]
    lm = market_state["liquidity_matrix"]
    lm["buyside"] = {"name": "Asian High", "price": round(ah, 2), "distance": round(abs(ah - price), 2) if ah else 0.0}
    lm["equilibrium"] = {"name": "SESSION AVG", "price": round(avg, 2), "distance": round(abs(avg - price), 2)}
    lm["sellside"] = {"name": "Asian Low", "price": round(al, 2), "distance": round(abs(al - price), 2) if al else 0.0}

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
                "risk_pct": 0.4, "lots": 0.0,
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
                f = market_state["feed"]
                log(f"heartbeat price={market_state['current_price']} src={f.get('source')} "
                    f"proxy={f.get('proxy')} offset={f.get('offset')} state={market_state['macro_state']} "
                    f"ticks={raw['ticks']}")
        except Exception as e:
            warn("engine", f"{type(e).__name__}: {e}", every=10)
            time.sleep(2)
        time.sleep(0.5)


# ------------------------------------------------------------------ AI
def ai_context():
    return {"clock_eat": (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M"),
            "price": market_state["current_price"],
            "price_is_proxy": market_state["feed"].get("proxy"),
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
        out = json.loads(text[text.find("{"):text.rfind("}") + 1])
        return {"observation": str(out.get("observation", ""))[:400],
                "inference": str(out.get("inference", ""))[:400],
                "decision": str(out.get("decision", "WAIT")).upper()[:10]}
    except Exception as e:
        warn("ai", f"{type(e).__name__}: {str(e)[:100]}", every=120)
        return fallback


def ai_loop():
    while True:
        if market_state["feed"].get("ok"):
            market_state["ai_briefing"] = get_ai_briefing()
        time.sleep(20)


# ------------------------------------------------------------------ OUTBOUND PROBE (diagnostics)
PROBE_TARGETS = {
    "google": "https://www.google.com/generate_204",
    "binance-paxg": "https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT",
    "kraken-paxg": "https://api.kraken.com/0/public/Ticker?pair=PAXGUSD",
    "twelvedata": "https://api.twelvedata.com/price?symbol=XAU/USD",
}
probe_results = {}


def _probe_one(name, url):
    t0 = time.time()
    try:
        r = requests.get(url, timeout=(5, 8), headers={"User-Agent": "Mozilla/5.0"})
        probe_results[name] = {"ok": True, "status": r.status_code, "ms": int((time.time() - t0) * 1000),
                               "sample": r.text[:80].replace("\n", " ")}
    except Exception as e:
        probe_results[name] = {"ok": False, "ms": int((time.time() - t0) * 1000),
                               "problem": f"{type(e).__name__}: {str(e)[:90]}"}


def run_probe():
    ts = [threading.Thread(target=_probe_one, args=(n, u), daemon=True) for n, u in PROBE_TARGETS.items()]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=15)


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
        "raw_feed": {**raw, "age_sec": round(time.time() - raw["ts"], 1) if raw["ts"] else None},
        "output": market_state["feed"],
        "calibration": {**cal, "age_sec": round(time.time() - cal["ts"], 1) if cal["ts"] else None},
        "source_problems": feed_err,
        "threads": {t.name: t.is_alive() for t in threading.enumerate()},
        "candles": len(candles),
        "day": {k: day[k] for k in ("date", "asian_high", "asian_low", "n", "swept", "displaced", "fvg")},
        "keys": {"gemini": bool(GEMINI_API_KEY), "twelvedata": bool(TWELVEDATA_API_KEY)},
        "gemini_model": GEMINI_MODEL,
        "deriv_enabled": ENABLE_DERIV,
    })


@app.route("/api/v1/probe")
def probe():
    run_probe()
    return jsonify(probe_results)


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
        warn("chat", f"{type(e).__name__}: {str(e)[:100]}", every=30)
        return jsonify({"reply": f"AI core problem ({type(e).__name__}). Check GEMINI_MODEL / key."}), 502


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
def start_threads():
    specs = [(poll_binance, "binance"), (poll_kraken, "kraken"), (calibrate_loop, "calibrate"),
             (engine_loop, "engine"), (ai_loop, "ai")]
    if ENABLE_DERIV:
        specs.append((start_deriv, "deriv"))
    if PROBE_ON_START:
        specs.append((run_probe, "probe"))
    for fn, name in specs:
        threading.Thread(target=fn, name=name, daemon=True).start()


if __name__ == "__main__":
    log("Starting ST-FIRM backend v2")
    start_threads()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
