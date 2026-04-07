"""
TradeEdge Cloud API — Yahoo Finance with sequential fetching + exponential backoff
Fixes rate-limiting inconsistency by:
  1. Sequential per-symbol fetch (no concurrent threads → no rate limit pile-up)
  2. Exponential backoff on 429 / RateLimitError (2s → 4s → 8s → 16s, up to 4 retries)
  3. Small inter-symbol delay (0.35s) to stay under Yahoo's per-IP rate limit
  4. Batch yf.download() attempted first for speed; falls back to per-symbol on failure
  5. Indices always fetched individually (^ symbols hit rate limits faster)
"""
from __future__ import annotations
import os, time, random
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Run: pip install yfinance pandas flask flask-cors")

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

# Indices — always fetched individually (^ symbols rate-limited more aggressively)
INDEX_SYMBOLS = {
    "NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    "CNXIT", "CNXAUTO", "CNXPHARMA", "CNXENERGY",
    "CNXMETAL", "CNXFMCG", "CNXINFRA", "CNXCONSUM",
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

# ── Data parsing ──────────────────────────────────────────────────────────────

def parse_df(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(c[0]).lower().replace(" ", "_") for c in df.columns]
    else:
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    required = ["open", "high", "low", "close"]
    if not all(c in df.columns for c in required):
        return None
    df = df[df["open"] > 0].round(2)
    has_adj = "adj_close" in df.columns
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
            })
        except Exception:
            continue
    return rows if rows else None

# ── Fetch logic ───────────────────────────────────────────────────────────────

MAX_RETRIES  = 4      # attempts per symbol
BASE_DELAY   = 2.0    # seconds, doubles each retry
INTER_SYMBOL = 0.35   # seconds between sequential fetches
BATCH_DELAY  = 1.5    # seconds before switching to individual fallback

def _is_rate_limit(e):
    msg = str(e).lower()
    return any(k in msg for k in ["rate", "429", "too many", "limit", "yfratelimit"])

def fetch_single(sym, start, end):
    """Fetch one symbol with exponential backoff. Returns rows or None."""
    tk = get_yf_ticker(sym)
    delay = BASE_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(tk, start=start, end=end, interval="1d",
                             auto_adjust=False, progress=False, timeout=20)
            rows = parse_df(df)
            if rows:
                return rows
            return None  # empty = wrong ticker, don't retry
        except Exception as e:
            if _is_rate_limit(e):
                jitter = random.uniform(0, delay * 0.3)
                wait   = delay + jitter
                print(f"[{sym}] Rate limit attempt {attempt+1}, wait {wait:.1f}s")
                time.sleep(wait)
                delay *= 2
            else:
                print(f"[{sym}] Error: {e}")
                return None
    print(f"[{sym}] All {MAX_RETRIES} retries exhausted")
    return None

def fetch_bulk(symbols, start, end):
    """One-shot bulk fetch. Returns (result, failed)."""
    if not symbols:
        return {}, []
    tickers = [get_yf_ticker(s) for s in symbols]
    t2s     = {get_yf_ticker(s): s for s in symbols}
    result, failed = {}, []
    try:
        raw = yf.download(tickers, start=start, end=end, interval="1d",
                          auto_adjust=False, progress=False,
                          group_by="ticker", timeout=30)
        if raw.empty:
            return {}, symbols[:]
        for tk, sym in t2s.items():
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if tk not in raw.columns.get_level_values(0):
                        failed.append(sym); continue
                    df = raw[tk].copy()
                    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
                else:
                    df = raw.copy()
                    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
                rows = parse_df(df)
                if rows: result[sym] = rows
                else:    failed.append(sym)
            except Exception:
                failed.append(sym)
    except Exception as e:
        print(f"Bulk error: {e}")
        failed = symbols[:]
    return result, failed

def fetch_symbols(symbols, start, end):
    """
    Strategy:
    - Equities → bulk attempt first, then individual retry for failures
    - Indices  → always individual (^ symbols rate-limited faster)
    """
    equities = [s for s in symbols if s not in INDEX_SYMBOLS]
    indices  = [s for s in symbols if s in INDEX_SYMBOLS]
    result, all_failed = {}, []

    # Equities: bulk → individual fallback
    if equities:
        bulk_res, bulk_failed = fetch_bulk(equities, start, end)
        result.update(bulk_res)
        print(f"Bulk: {len(bulk_res)} ok / {len(bulk_failed)} failed")
        if bulk_failed:
            time.sleep(BATCH_DELAY)
            for sym in bulk_failed:
                rows = fetch_single(sym, start, end)
                if rows: result[sym] = rows
                else:    all_failed.append(sym)
                time.sleep(INTER_SYMBOL)

    # Indices: always individual
    if indices:
        time.sleep(BATCH_DELAY)
        for sym in indices:
            rows = fetch_single(sym, start, end)
            if rows: result[sym] = rows
            else:    all_failed.append(sym)
            time.sleep(INTER_SYMBOL)

    return result, all_failed

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
