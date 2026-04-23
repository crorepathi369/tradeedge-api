"""
TradeEdge Cloud API — Yahoo Finance with robust rate limit handling
Key design decisions:
  1. Use Ticker.history() NOT yf.download() — download() swallows rate limit errors
     internally ("1 Failed download:"), returns empty DataFrame, so retry logic never runs.
     Ticker.history() raises YFRateLimitError properly.
  2. Sequential fetch only — bulk download triggers mass rate limits on shared IPs
  3. MAX_BATCH_RL_HITS=3 abort — when Yahoo is clearly blocking the IP, stop early
     rather than burning through retries for every remaining symbol in the batch
  4. MAX_BATCH_SECS=130 safety cutoff — returns partial results before 150s client timeout
  5. YFRateLimitError, YFPricesMissingError, YFTzMissingError all handled explicitly
  6. actions=False — skips dividend/split data, faster fetches
"""
from __future__ import annotations
import os, time, random, warnings

# yfinance uses pd.Timestamp.utcnow() which is deprecated in pandas 2.x — suppress the noise
warnings.filterwarnings('ignore', message='.*utcnow.*', category=FutureWarning)
warnings.filterwarnings('ignore', message='.*Timestamp.utcnow.*', category=FutureWarning)
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Run: pip install yfinance pandas flask flask-cors")

# Grab YFRateLimitError if available (yfinance >= 0.2.38), else fall back to Exception
try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:
    YFRateLimitError = None

try:
    from yfinance.exceptions import YFPricesMissingError
except ImportError:
    YFPricesMissingError = None

try:
    from yfinance.exceptions import YFTzMissingError
except ImportError:
    YFTzMissingError = None

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

def cors_response(data, status=200):
    resp = make_response(jsonify(data), status)
    resp.headers['Access-Control-Allow-Origin']  = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return resp

@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin']  = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return response

@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def options_handler(path=''):
    return cors_response({'ok': True})

# ── Symbol maps ───────────────────────────────────────────────────────────────

YAHOO_TICKER_MAP = {
    "NIFTY50":      "^NSEI",
    "BANKNIFTY":    "^NSEBANK",
    "FINNIFTY":     "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":   "^NSEMDCP50",
    "CNXIT":        "^CNXIT",
    "CNXAUTO":      "^CNXAUTO",
    "CNXPHARMA":    "^CNXPHARMA",
    "CNXENERGY":    "^CNXENERGY",
    "CNXMETAL":     "^CNXMETAL",
    "CNXFMCG":      "^CNXFMCG",
    "CNXINFRA":     "^CNXINFRA",
    "CNXCONSUM":    "^CNXCONSUM",
    "M&M":          "M&M.NS",
    "BAJAJ-AUTO":   "BAJAJ-AUTO.NS",
    "BIRLASOFT":    "BSOFT.NS",
    "DEEPAKNITR":   "DEEPAKNTR.NS",
    "ICICIPRULIFE": "ICICIPRULI.NS",
    "MCDOWELL-N":   "UNITDSPR.NS",
    "TATAMOTORS":   "TMPV.NS",
    "ZOMATO":       "ETERNAL.NS",
}

ALL_SYMBOLS = [
    "NIFTY50","BANKNIFTY","FINNIFTY","MIDCPNIFTY",
    "CNXIT","CNXAUTO","CNXPHARMA","CNXENERGY","CNXMETAL","CNXFMCG","CNXINFRA","CNXCONSUM",
    "AARTIIND","ABB","ABCAPITAL","ABFRL","ACC","ADANIENT","ADANIGREEN",
    "ADANIPORTS","ALKEM","AMBUJACEM","AMBER","APOLLOHOSP",
    "APOLLOTYRE","ASHOKLEY","ASIANPAINT","AUBANK","AUROPHARMA",
    "BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK",
    "BANKBARODA","BEL","BERGEPAINT","BHARTIARTL","BHEL","BIOCON",
    "BIRLASOFT","BOSCHLTD","BPCL","BRITANNIA","BSE",
    "CAMS","CANBK","CESC","CHAMBLFERT","CHOLAFIN","CIPLA","COALINDIA",
    "COFORGE","COLPAL","CONCOR","COROMANDEL","CUMMINSIND",
    "DABUR","DEEPAKNITR","DELHIVERY","DMART","DIVISLAB","DIXON","DLF","DRREDDY",
    "EICHERMOT","EMAMILTD","EXIDEIND","FEDERALBNK",
    "GAIL","GLAND","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO",
    "HINDALCO","HINDUNILVR","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULIFE","IDEA","IDFCFIRSTB","IGL",
    "IIFL","INDHOTEL","INDIAMART","INDIGO","INDUSINDBK","IOC",
    "IPCALAB","IRB","IRFC","ITC",
    "JINDALSTEL","JUBLFOOD","JSWSTEEL",
    "KALYANKJIL","KOTAKBANK","KPITTECH",
    "LALPATHLAB","LAURUSLABS","LICHSGFIN","LT","LTIM","LTTS","LUPIN",
    "M&M","M&MFIN","MANAPPURAM","MARICO","MARUTI","MCX","MCDOWELL-N",
    "MGL","MOTHERSON","MPHASIS","MRF","MUTHOOTFIN",
    "NATIONALUM","NAUKRI","NBCC","NESTLEIND","NHPC",
    "NMDC","NTPC","NYKAA","OBEROIRLTY","OFSS","ONGC",
    "PAYTM","PFC","PIDILITIND","PIIND","PNBHOUSING","POLICYBZR",
    "POWERGRID","PRESTIGE","PERSISTENT","PNB","PVRINOX",
    "RADICO","RBLBANK","RECLTD","RELIANCE","RPOWER",
    "SAIL","SBICARD","SBILIFE","SBIN","SHREECEM","SIEMENS","SJVN",
    "SRF","STAR","SUNPHARMA","SUZLON",
    "TATACHEM","TATACOMM","TATACONSUM","TATAELXSI","TATAMOTORS",
    "TATAPOWER","TATASTEEL","TCS","TECHM","TIINDIA","TITAN",
    "TORNTPHARM","TORNTPOWER","TRENT",
    "UBL","ULTRACEMCO","UNIONBANK","UPL",
    "VBL","VEDL","VOLTAS","WHIRLPOOL","WIPRO","ZOMATO",
]

def get_yf_ticker(s):
    return YAHOO_TICKER_MAP.get(s, s + ".NS")

# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_df(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(c[0]).lower().replace(" ", "_") for c in df.columns]
    else:
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    if not all(c in df.columns for c in ["open","high","low","close"]):
        return None
    df = df[df["open"] > 0].round(2)
    has_adj = "adj_close" in df.columns
    has_vol = "volume" in df.columns
    rows = []
    for i, r in df.iterrows():
        try:
            rows.append({
                "date":     pd.Timestamp(i).strftime("%Y-%m-%d"),
                "open":     round(float(r["open"]),  2),
                "high":     round(float(r["high"]),  2),
                "low":      round(float(r["low"]),   2),
                "close":    round(float(r["close"]), 2),
                "adjClose": round(float(r["adj_close"]), 2)
                            if has_adj and pd.notna(r.get("adj_close"))
                            else round(float(r["close"]), 2),
                "volume":   int(r["volume"]) if has_vol and pd.notna(r.get("volume")) else 0,
            })
        except Exception:
            continue
    return rows if rows else None

# ── Rate limit detection ──────────────────────────────────────────────────────

def is_rate_limit(e):
    """Detect rate limit errors by class name AND message — covers all yfinance versions."""
    cls_name = type(e).__name__
    if "RateLimit" in cls_name or "ratelimit" in cls_name.lower():
        return True
    if YFRateLimitError and isinstance(e, YFRateLimitError):
        return True
    msg = str(e).lower()
    return any(k in msg for k in ["rate limit", "too many requests", "429", "try after"])

# ── Fetch constants ───────────────────────────────────────────────────────────

INTER_SYMBOL      = 1.5   # seconds between every symbol fetch — steady pace
RATE_LIMIT_WAIT   = 25.0  # seconds to pause on rate limit per symbol
MAX_RETRIES       = 2     # retries per symbol after rate limit
MAX_BATCH_SECS    = 130   # safety cutoff: return partial results rather than blow HTML timeout
MAX_BATCH_RL_HITS = 3     # abort batch early if this many symbols hit RL (Yahoo IP is blocked)

# NOTE: Use Ticker.history() NOT yf.download().
# yf.download() silently catches rate limit errors internally, prints them as
# "1 Failed download:", and returns an empty DataFrame — so our except block
# never runs and there is zero retry or backoff. Ticker.history() raises properly.

def fetch_single(sym, start, end):
    """
    Fetch one symbol using Ticker.history() which raises exceptions properly.
    On rate limit: wait RATE_LIMIT_WAIT seconds and retry up to MAX_RETRIES times.
    On known no-data errors (delisted, tz missing): skip immediately, no retry.
    Returns (rows_or_None, rate_limited: bool)
    """
    tk = get_yf_ticker(sym)
    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(tk)
            df = ticker.history(start=start, end=end, interval="1d",
                                auto_adjust=False, actions=False)
            rows = parse_df(df)
            return rows, False   # success (rows may be None if empty)
        except Exception as e:
            # No-data errors — skip immediately, no retry
            if YFPricesMissingError and isinstance(e, YFPricesMissingError):
                print(f"[{sym}] No price data (delisted or ticker change)")
                return None, False
            if YFTzMissingError and isinstance(e, YFTzMissingError):
                print(f"[{sym}] Timezone data missing")
                return None, False
            if is_rate_limit(e):
                wait = RATE_LIMIT_WAIT + random.uniform(0, 5)
                print(f"[{sym}] Rate limit (attempt {attempt+1}/{MAX_RETRIES}), "
                      f"pausing {wait:.0f}s …")
                time.sleep(wait)
            else:
                print(f"[{sym}] Error: {type(e).__name__}: {e}")
                return None, False
    print(f"[{sym}] Giving up after {MAX_RETRIES} rate-limit retries")
    return None, True   # exhausted retries — signal RL to caller

def fetch_symbols(symbols, start, end):
    """
    Pure sequential fetch — one symbol at a time with INTER_SYMBOL gap.
    Ticker.history() is used so rate limits raise exceptions (yf.download swallows them).
    Two safety valves:
      - MAX_BATCH_RL_HITS: abort batch when Yahoo is clearly blocking this IP
      - MAX_BATCH_SECS: return partial results before HTML client timeout fires
    """
    result, failed = {}, []
    batch_t0  = time.time()
    rl_hits   = 0
    for i, sym in enumerate(symbols):
        # Safety cutoff — return partial results before HTML client times out
        if time.time() - batch_t0 > MAX_BATCH_SECS:
            remaining = symbols[i:]
            failed.extend(remaining)
            print(f"[batch] Safety cutoff after {MAX_BATCH_SECS}s — skipping {len(remaining)} remaining")
            break
        # Rate-limit abort — Yahoo is blocking this IP; remaining symbols will all fail
        if rl_hits >= MAX_BATCH_RL_HITS:
            remaining = symbols[i:]
            failed.extend(remaining)
            print(f"[batch] RL abort after {rl_hits} rate-limit hits — skipping {len(remaining)} remaining")
            break
        rows, was_rl = fetch_single(sym, start, end)
        if was_rl:
            rl_hits += 1
        if rows:
            result[sym] = rows
        else:
            failed.append(sym)
        # Always wait between symbols — even on success
        if i < len(symbols) - 1:
            time.sleep(INTER_SYMBOL)
    return result, failed

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    return cors_response({
        "status":  "ok",
        "service": "TradeEdge API",
        "time":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbols": len(ALL_SYMBOLS),
    })

@app.route("/sync-today")
def sync_today():
    offset = int(request.args.get("offset", 0))
    limit  = int(request.args.get("limit",  20))
    days   = int(request.args.get("days",   10))
    syms   = ALL_SYMBOLS[offset:offset + limit]

    if not syms:
        return cors_response({
            "status": "ok", "fetched": 0, "failed": 0,
            "failedSymbols": [], "elapsed": 0, "data": {},
            "asOf": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "done": True,
        })

    end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    t0    = time.time()

    result, failed = fetch_symbols(syms, start, end)

    return cors_response({
        "status":        "ok",
        "fetched":       len(result),
        "failed":        len(failed),
        "failedSymbols": failed,
        "elapsed":       round(time.time() - t0, 1),
        "asOf":          datetime.now().strftime("%Y-%m-%d %H:%M"),
        "data":          result,
        "offset":        offset,
        "limit":         limit,
        "grandTotal":    len(ALL_SYMBOLS),
        "done":          (offset + limit) >= len(ALL_SYMBOLS),
    })

# ── Futures endpoint ──────────────────────────────────────────────────────────
#
# GET /futures?symbols=LTIM,INFY,TCS
#
# For each symbol fetches:
#   - Spot price (last close from Ticker.history 5d)
#   - Near month futures  (Yahoo ticker: SYMBOL-I.NS)
#   - Far  month futures  (Yahoo ticker: SYMBOL-II.NS)
#   - 50 days of close prices for EMA20/EMA50 trend
#
# Returns:
# {
#   "status": "ok",
#   "data": {
#     "LTIM": { "spot": 4531.5, "nearFut": 4495.0, "farFut": 4360.5,
#               "closes": [...50 daily closes...], "error": null },
#     "INFY": { ... }
#   },
#   "failed": ["XYZ"],
#   "elapsed": 4.2
# }
#
# Design notes:
#   - Uses same Ticker.history() pattern as /sync-today (raises rate limits properly)
#   - Futures tickers tried first via Yahoo (-I.NS / -II.NS suffix)
#   - If futures not found on Yahoo, nearFut/farFut returned as null
#     (frontend falls back to NSE API or shows N/A)
#   - INTER_SYMBOL gap applied between spot fetches to avoid rate limits
#   - Futures fetched with short 5d window — only need latest price

def fetch_latest_price(ticker_str):
    """Fetch the most recent closing price for a ticker. Returns float or None."""
    for attempt in range(2):
        try:
            tk = yf.Ticker(ticker_str)
            df = tk.history(period="5d", interval="1d",
                            auto_adjust=False, actions=False)
            if df is not None and not df.empty:
                cols = [str(c).lower().replace(" ", "_") for c in df.columns]
                df.columns = cols
                if "close" in df.columns:
                    closes = df["close"].dropna()
                    if len(closes) > 0:
                        return round(float(closes.iloc[-1]), 2)
            return None
        except Exception as e:
            if is_rate_limit(e):
                time.sleep(RATE_LIMIT_WAIT)
            else:
                return None
    return None

def fetch_close_series(ticker_str, days=60):
    """Fetch last N days of daily closes for EMA calc. Returns list[float] or []."""
    try:
        tk     = yf.Ticker(ticker_str)
        end    = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        start  = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        df     = tk.history(start=start, end=end, interval="1d",
                            auto_adjust=False, actions=False)
        if df is None or df.empty:
            return []
        cols = [str(c).lower().replace(" ", "_") for c in df.columns]
        df.columns = cols
        if "close" not in df.columns:
            return []
        return [round(float(v), 2) for v in df["close"].dropna().tolist()]
    except Exception:
        return []

@app.route("/futures")
def futures_endpoint():
    raw     = request.args.get("symbols", "").strip()
    if not raw:
        return cors_response({"error": "symbols param required"}, 400)

    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if len(symbols) > 60:
        return cors_response({"error": "max 60 symbols per request"}, 400)

    t0     = time.time()
    result = {}
    failed = []

    for i, sym in enumerate(symbols):
        # Safety cutoff — same pattern as /sync-today
        if time.time() - t0 > MAX_BATCH_SECS:
            failed.extend(symbols[i:])
            print(f"[futures] Safety cutoff — skipping {len(symbols[i:])} remaining")
            break

        yf_sym  = get_yf_ticker(sym)           # e.g. LTIM.NS
        # Yahoo NSE futures tickers follow pattern SYMBOL-I.NS / SYMBOL-II.NS
        # These work for liquid F&O names; returns None if not listed on Yahoo
        base    = sym if sym.endswith(".NS") else sym
        near_tk = f"{base}-I.NS"
        far_tk  = f"{base}-II.NS"

        try:
            # 1. Spot + close series (60d for EMA50)
            closes  = fetch_close_series(yf_sym, days=60)
            spot    = closes[-1] if closes else fetch_latest_price(yf_sym)

            if not spot:
                failed.append(sym)
                result[sym] = {"spot": None, "nearFut": None, "farFut": None,
                               "closes": [], "error": "No spot data"}
                continue

            # 2. Near month futures
            time.sleep(0.4)
            near_fut = fetch_latest_price(near_tk)

            # 3. Far month futures
            time.sleep(0.4)
            far_fut  = fetch_latest_price(far_tk)

            result[sym] = {
                "spot":    spot,
                "nearFut": near_fut,
                "farFut":  far_fut,
                "closes":  closes,
                "error":   None,
            }
            print(f"[futures] {sym}: spot={spot} near={near_fut} far={far_fut}")

        except Exception as e:
            failed.append(sym)
            result[sym] = {"spot": None, "nearFut": None, "farFut": None,
                           "closes": [], "error": str(e)}
            print(f"[futures] {sym} error: {e}")

        # Inter-symbol gap between spot fetches
        if i < len(symbols) - 1:
            time.sleep(INTER_SYMBOL)

    return cors_response({
        "status":  "ok",
        "data":    result,
        "failed":  failed,
        "elapsed": round(time.time() - t0, 1),
        "asOf":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count":   len(symbols),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
