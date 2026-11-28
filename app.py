"""
ST-FIRM WATCHER backend  v3

SPEED   Binance websocket (bid pushes, ~100ms) is the main feed; REST polling is only a backup.
START   On every boot (and each new EAT day) the server back-fills Asian range, PDH/PDL and recent candles
        from Binance klines, so a Render restart no longer wipes your levels.
ACCURACY  Prices are PAXG (gold token) + an OFFSET that aligns them with your broker:
          - POST /api/v1/sync  (X-Admin-Token) lets you align to the price on YOUR MT5 screen, or
          - TWELVEDATA_API_KEY aligns to spot XAU/USD automatically every 150s.
          The engine works in raw PAXG space and adds the offset only when publishing, so a new
          calibration shifts every level consistently.
Analysis only: /trade and /campaign/action are disabled stubs. No orders are ever sent.
"""
import os
import json
import hmac
import math
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
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")            # enables /api/v1/sync (broker alignment)
ENABLE_DERIV = os.environ.get("ENABLE_DERIV", "0") == "1"
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
PROBE_ON_START = os.environ.get("PROBE_ON_START", "0") == "1"

ASIAN_START, ASIAN_END = 0.0, 9.0   # EAT hours
SWEEP_DISPLACEMENT = 2.0            # points above Asian low
SL_BUFFER = 3.5                     # points below the sweep low
RR = 2.0                            # TP = entry + RR * risk
SETUP_LIFETIME = 30 * 60            # seconds an A+ setup stays on screen
STALE_AFTER = 15                    # seconds without a tick = feed down
CAL_MAX_AGE = 6 * 3600              # automatic (Twelve Data) calibration expires after this
EAT = timedelta(hours=3)

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
state_lock = threading.RLock()

raw = {"price": None, "source": None, "ts": 0.0, "ticks": 0}
cal = {"offset": 0.0, "ts": 0.0, "ref": None, "ref_ts": 0.0, "manual": False}
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
    "levels": {"pdh": 0.0, "pdl": 0.0},
    "checklist": {"sweep": False, "displacement": False, "fvg": False},
    "execution": {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0, "risk_pct": 1.0, "lots": 0.0},
    "ai_briefing": {"observation": "Initializing...", "inference": "Initializing...", "decision": "WAIT"},
    "feed": {"ok": False, "source": None, "age_sec": None, "proxy": True, "calibrated": False,
             "manual": False, "offset": 0.0, "raw_price": None},
    "range_ready": False,
    "uptime_sec": 0,
    "session_history": [],
}

# everything in `day` is stored in RAW (PAXG) price space
day = {"date": "", "asian_high": 0.0, "asian_low": 0.0, "sum": 0.0, "n": 0, "hi": 0.0, "lo": 0.0,
       "pdh": 0.0, "pdl": 0.0,
       "swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0}

candles = []  # 1-minute candles, raw space
backfill_event = threading.Event()
backfill_info = {"last_ok": None, "last_problem": None, "runs": 0}


def set_raw(p, source):
    raw["price"] = p
    raw["source"] = source
    raw["ts"] = time.time()
    raw["ticks"] += 1


def current_offset():
    """(offset, calibrated, manual). Deriv quotes real spot, so it never needs an offset."""
    if raw["source"] == "deriv":
        return 0.0, False, False
    if cal["manual"]:
        return cal["offset"], True, True
    if cal["ts"] > 0 and (time.time() - cal["ts"]) < CAL_MAX_AGE:
        return cal["offset"], True, False
    return 0.0, False, False


def feed_meta():
    """(raw price or None, meta dict for the HUD)."""
    now = time.time()
    age = now - raw["ts"] if raw["ts"] else None
    off, calibrated, manual = current_offset()
    fresh = raw["price"] is not None and age is not None and age <= STALE_AFTER
    meta = {"ok": bool(fresh), "source": raw["source"],
            "age_sec": round(age, 1) if age is not None else None,
            "proxy": (raw["source"] != "deriv") and (not calibrated),
            "calibrated": calibrated, "manual": manual, "offset": round(off, 2),
            "raw_price": round(raw["price"], 2) if raw["price"] is not None else None}
    return (raw["price"] if fresh else None), meta


def update_candles(price, now_ts):
    minute = int(now_ts // 60)
    if candles and candles[-1]["m"] == minute:
        c = candles[-1]
        c["h"] = max(c["h"], price)
        c["l"] = min(c["l"], price)
        c["c"] = price
    else:
        candles.append({"m": minute, "o": price, "h": price, "l": price, "c": price})
        if len(candles) > 300:
            candles.pop(0)


def bullish_fvg():
    """Gap between candle 1 high and candle 3 low, using the last 3 CLOSED candles."""
    if len(candles) < 4:
        return False
    c1, c3 = candles[-4], candles[-2]
    return c3["l"] > c1["h"]


# ------------------------------------------------------------------ PRICE FEEDS
async def _binance_ws():
    import websockets
    urls = ["wss://stream.binance.com:9443/ws/paxgusdt@bookTicker",
            "wss://stream.binance.com:443/ws/paxgusdt@bookTicker"]
    i = 0
    while True:
        url = urls[i % len(urls)]
        try:
            async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
                log("Binance WS connected:", url)
                feed_err.pop("binance_ws", None)
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    d = json.loads(msg)
                    if "b" in d:                       # best BID, same side an MT5 chart draws
                        set_raw(float(d["b"]), "binance-ws")
        except Exception as e:
            feed_err["binance_ws"] = f"{type(e).__name__}: {str(e)[:100]}"
            warn("binance_ws", feed_err["binance_ws"], every=120)
            i += 1
            await asyncio.sleep(3)


def start_binance_ws():
    asyncio.run(_binance_ws())


def poll_binance():
    """REST backup. Idle while the websocket is healthy."""
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    fails = 0
    while True:
        try:
            if time.time() - raw["ts"] > 3:
                r = s.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": "PAXGUSDT"}, timeout=(4, 4))
                r.raise_for_status()
                set_raw(float(r.json()["price"]), "binance-rest")
                if fails:
                    log("Binance REST recovered")
                fails = 0
                feed_err.pop("binance", None)
        except Exception as e:
            fails += 1
            feed_err["binance"] = f"{type(e).__name__}: {str(e)[:100]}"
            warn("binance", feed_err["binance"])
        time.sleep(1 if fails == 0 else min(2 + fails, 10))


def poll_kraken():
    """Last-resort backup: runs when nothing has ticked for 5s."""
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
    """Spot XAU/USD every 150s (free plan = 800 calls/day). Skipped while a manual MT5 sync is active."""
    if not TWELVEDATA_API_KEY:
        log("Auto-calibration OFF (no TWELVEDATA_API_KEY). Use /api/v1/sync or the SYNC tap in the HUD.")
        return
    s = requests.Session()
    while True:
        wait = 150
        try:
            fresh = raw["price"] is not None and time.time() - raw["ts"] < STALE_AFTER and raw["source"] != "deriv"
            if not fresh:
                time.sleep(3)
                continue
            if cal["manual"]:
                time.sleep(60)
                continue
            r = s.get("https://api.twelvedata.com/price",
                      params={"symbol": "XAU/USD", "apikey": TWELVEDATA_API_KEY}, timeout=(4, 6))
            j = r.json()
            if "price" not in j:
                raise RuntimeError(str(j.get("message", j))[:140])
            ref = float(j["price"])
            cal["ref"], cal["ref_ts"] = ref, time.time()
            if raw["price"] is not None and time.time() - raw["ts"] < STALE_AFTER:
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
            wait = 60
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


# ------------------------------------------------------------------ BACKFILL (Binance klines)
def eat_day_bounds(now_utc=None):
    """UTC instants of today's EAT midnight / Asian end / previous EAT midnight, plus today's date string."""
    now_utc = now_utc or datetime.now(timezone.utc)
    now_eat = now_utc + EAT
    today0 = datetime(now_eat.year, now_eat.month, now_eat.day, tzinfo=timezone.utc) - EAT
    return today0, today0 + timedelta(hours=ASIAN_END), today0 - timedelta(hours=24), now_eat.strftime("%Y-%m-%d")


def apply_backfill(k5, k1, now_utc=None):
    """Merge historical klines into today's state. k5/k1 are Binance kline arrays (raw PAXG space)."""
    now_utc = now_utc or datetime.now(timezone.utc)
    today0, asian_end, prev0, date_str = eat_day_bounds(now_utc)
    t0, ta, tp = (int(x.timestamp() * 1000) for x in (today0, asian_end, prev0))
    tnow = int(now_utc.timestamp() * 1000)
    pdh = pdl = ah = al = None
    s = n = 0
    hi = lo = None
    for k in k5:
        t, h, l, c = int(k[0]), float(k[2]), float(k[3]), float(k[4])
        if tp <= t < t0:
            pdh = h if pdh is None else max(pdh, h)
            pdl = l if pdl is None else min(pdl, l)
        if t0 <= t < min(ta, tnow):
            ah = h if ah is None else max(ah, h)
            al = l if al is None else min(al, l)
        if t >= t0:
            s += c
            n += 1
            hi = h if hi is None else max(hi, h)
            lo = l if lo is None else min(lo, l)
    with state_lock:
        if day["date"] != date_str:
            reset_day(date_str)
        if pdh is not None:
            day["pdh"], day["pdl"] = pdh, pdl
        if ah is not None:
            day["asian_high"] = ah if day["asian_high"] == 0.0 else max(day["asian_high"], ah)
            day["asian_low"] = al if day["asian_low"] == 0.0 else min(day["asian_low"], al)
        if hi is not None:
            day["hi"] = hi if day["hi"] == 0.0 else max(day["hi"], hi)
            day["lo"] = lo if day["lo"] == 0.0 else min(day["lo"], lo)
        if n and day["n"] < 20:               # fresh boot: seed the session average
            day["sum"], day["n"] = s, n
        if k1 and len(candles) < 5:           # fresh boot: seed 1-minute candles
            candles[:] = [{"m": int(k[0]) // 60000, "o": float(k[1]), "h": float(k[2]),
                           "l": float(k[3]), "c": float(k[4])} for k in k1][-300:]
    return {"pdh": pdh, "pdl": pdl, "asian_high": ah, "asian_low": al, "bars5": len(k5), "bars1": len(k1)}


def backfill_loop():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    base = "https://api.binance.com/api/v3/klines"
    while True:
        backfill_event.wait(timeout=1800)       # new day or every 30 min
        backfill_event.clear()
        try:
            r5 = s.get(base, params={"symbol": "PAXGUSDT", "interval": "5m", "limit": 1000}, timeout=(5, 10))
            r5.raise_for_status()
            r1 = s.get(base, params={"symbol": "PAXGUSDT", "interval": "1m", "limit": 120}, timeout=(5, 10))
            r1.raise_for_status()
            info = apply_backfill(r5.json(), r1.json())
            backfill_info.update(last_ok=time.time(), last_problem=None, runs=backfill_info["runs"] + 1)
            log("Backfill OK:", info)
        except Exception as e:
            backfill_info["last_problem"] = f"{type(e).__name__}: {str(e)[:120]}"
            warn("backfill", backfill_info["last_problem"], every=30)
            time.sleep(20)
            backfill_event.set()                # retry soon


# ------------------------------------------------------------------ ENGINE
def reset_day(date_str):
    prev_hi, prev_lo = day["hi"], day["lo"]
    day.update({"date": date_str, "asian_high": 0.0, "asian_low": 0.0, "sum": 0.0, "n": 0, "hi": 0.0, "lo": 0.0,
                "swept": False, "sweep_low": None, "displaced": False, "fvg": False, "exec_ts": 0.0})
    if prev_hi:                                  # yesterday's range becomes PDH/PDL until backfill refines it
        day["pdh"], day["pdl"] = prev_hi, prev_lo
    market_state["execution"] = {"grade": "WAITING", "entry": 0.0, "sl": 0.0, "tp": 0.0,
                                 "risk_pct": 1.0, "lots": 0.0}
    market_state["session_history"] = []
    backfill_event.set()
    log("New EAT day:", date_str, "- ranges reset")


def engine_step():
    with state_lock:
        _engine_step()


def _engine_step():
    now = datetime.now(timezone.utc) + EAT
    date_str = now.strftime("%Y-%m-%d")
    h = now.hour + now.minute / 60.0

    p, meta = feed_meta()
    market_state["feed"] = meta
    market_state["uptime_sec"] = int(time.time() - STARTED)
    if p is None:
        return
    off = meta["offset"]

    def px(v):
        return round(v + off, 2)

    if date_str != day["date"]:
        reset_day(date_str)

    market_state["current_price"] = str(px(p))
    update_candles(p, time.time())
    day["sum"] += p
    day["n"] += 1
    day["hi"] = p if day["hi"] == 0.0 else max(day["hi"], p)
    day["lo"] = p if day["lo"] == 0.0 else min(day["lo"], p)
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
        if day["asian_high"] == 0.0 or p > day["asian_high"]:
            day["asian_high"] = p
        if day["asian_low"] == 0.0 or p < day["asian_low"]:
            day["asian_low"] = p
    range_ready = h >= ASIAN_END and day["asian_low"] > 0.0
    market_state["range_ready"] = range_ready

    market_state["structure"]["d1_bias"] = "BULL" if p > avg else "BEAR"   # temporary rule
    market_state["structure"]["playbook_mode"] = "NORMAL_BULL" if p > avg else "MIRROR_FLIP_BEAR"

    ah, al = day["asian_high"], day["asian_low"]
    lm = market_state["liquidity_matrix"]
    lm["buyside"] = {"name": "Asian High", "price": px(ah) if ah else 0.0, "distance": round(abs(ah - p), 2) if ah else 0.0}
    lm["equilibrium"] = {"name": "SESSION AVG", "price": px(avg), "distance": round(abs(avg - p), 2)}
    lm["sellside"] = {"name": "Asian Low", "price": px(al) if al else 0.0, "distance": round(abs(al - p), 2) if al else 0.0}
    market_state["levels"] = {"pdh": px(day["pdh"]) if day["pdh"] else 0.0,
                              "pdl": px(day["pdl"]) if day["pdl"] else 0.0}

    if range_ready and ms in ("LONDON_OPEN_MACRO", "NY_OPEN_MACRO"):
        if p < al:
            day["swept"] = True
            day["sweep_low"] = p if day["sweep_low"] is None else min(day["sweep_low"], p)
        if day["swept"] and p > al + SWEEP_DISPLACEMENT:
            day["displaced"] = True
        if day["swept"] and bullish_fvg():
            day["fvg"] = True
        if day["swept"] and day["displaced"] and day["fvg"] and day["exec_ts"] == 0.0:
            sl = min(day["sweep_low"], al) - SL_BUFFER
            risk = p - sl
            market_state["execution"] = {
                "grade": "A+ [3/3 GATES MET]",
                "entry": px(p), "sl": px(sl), "tp": px(p + RR * risk),
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
                    f"proxy={f.get('proxy')} offset={f.get('offset')} manual={f.get('manual')} "
                    f"state={market_state['macro_state']} ticks={raw['ticks']}")
        except Exception as e:
            warn("engine", f"{type(e).__name__}: {e}", every=10)
            time.sleep(2)
        time.sleep(0.25)


# ------------------------------------------------------------------ AI
def ai_context():
    return {"clock_eat": (datetime.now(timezone.utc) + EAT).strftime("%H:%M"),
            "price": market_state["current_price"],
            "price_is_proxy": market_state["feed"].get("proxy"),
            "macro_state": market_state["macro_state"],
            "structure": market_state["structure"],
            "liquidity": market_state["liquidity_matrix"],
            "levels": market_state["levels"],
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
    "binance-rest": "https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT",
    "binance-klines": "https://api.binance.com/api/v3/klines?symbol=PAXGUSDT&interval=1m&limit=2",
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
        "backfill": backfill_info,
        "threads": {t.name: t.is_alive() for t in threading.enumerate()},
        "candles": len(candles),
        "day": {k: day[k] for k in ("date", "asian_high", "asian_low", "pdh", "pdl", "n", "swept", "displaced", "fvg")},
        "keys": {"gemini": bool(GEMINI_API_KEY), "twelvedata": bool(TWELVEDATA_API_KEY), "admin_token": bool(ADMIN_TOKEN)},
        "gemini_model": GEMINI_MODEL,
        "deriv_enabled": ENABLE_DERIV,
    })


@app.route("/api/v1/probe")
def probe():
    run_probe()
    return jsonify(probe_results)


@app.route("/api/v1/sync", methods=["POST"])
def sync():
    """Align the HUD to YOUR broker. Body: {"price": 3650.83} (price on your MT5 now) or {"offset": 10.6} or {"clear": true}."""
    if not ADMIN_TOKEN:
        return jsonify({"status": "disabled", "message": "Set ADMIN_TOKEN in Render to enable broker sync."}), 403
    tok = request.headers.get("X-Admin-Token", "")
    if not hmac.compare_digest(tok.encode(), ADMIN_TOKEN.encode()):
        return jsonify({"status": "denied", "message": "Wrong token."}), 401
    body = request.get_json(silent=True) or {}
    if body.get("clear"):
        cal.update(manual=False, ts=0.0, offset=0.0)
        return jsonify({"status": "ok", "message": "Manual sync cleared."})
    try:
        if "offset" in body:
            off = float(body["offset"])
        elif "price" in body:
            if raw["price"] is None or time.time() - raw["ts"] > STALE_AFTER:
                return jsonify({"status": "error", "message": "No fresh feed yet, try again in a few seconds."}), 409
            off = float(body["price"]) - raw["price"]
        else:
            return jsonify({"status": "error", "message": "Send price, offset or clear."}), 400
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Not a number."}), 400
    limit = 0.05 * (raw["price"] or 4000.0)
    if not math.isfinite(off) or abs(off) > limit:
        return jsonify({"status": "error", "message": f"Offset {off:.2f} is outside +/-{limit:.0f}; check the number."}), 400
    cal.update(offset=off, ts=time.time(), manual=True)
    log(f"Manual sync: offset={off:.2f}")
    return jsonify({"status": "ok", "offset": round(off, 2), "message": "HUD aligned to your broker."})


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
    specs = [(start_binance_ws, "binance-ws"), (poll_binance, "binance-rest"), (poll_kraken, "kraken"),
             (calibrate_loop, "calibrate"), (backfill_loop, "backfill"), (engine_loop, "engine"), (ai_loop, "ai")]
    if ENABLE_DERIV:
        specs.append((start_deriv, "deriv"))
    if PROBE_ON_START:
        specs.append((run_probe, "probe"))
    for fn, name in specs:
        threading.Thread(target=fn, name=name, daemon=True).start()
    backfill_event.set()


if __name__ == "__main__":
    log("Starting ST-FIRM backend v3")
    start_threads()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
