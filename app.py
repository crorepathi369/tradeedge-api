"""
TradeEdge Cloud API — Yahoo Finance with robust rate limit handling
Key fixes vs previous version:
  1. Catch YFRateLimitError by class name (not just string) — works across yfinance versions
  2. Much longer inter-symbol delay (1.5s) — Yahoo's current rate limit is strict
  3. On rate limit: wait 30s before retrying entire batch (not per-symbol backoff)
  4. Global rate-limit state: if one symbol hits limit, pause ALL fetching for 30s
  5. Skip bulk download entirely — it's the main trigger for rate limits
  6. Sequential only, one symbol at a time with steady pacing
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

# Grab YFRateLimitError if available (yfinance >= 0.2.38), else fall back to Exception
try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:
    YFRateLimitError = None

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

INTER_SYMBOL    = 0.8   # seconds between every symbol fetch — steady pace
RATE_LIMIT_WAIT = 45.0  # seconds to pause when rate limit hit
MAX_RETRIES     = 3     # retries per symbol after rate limit

def fetch_single(sym, start, end):
    """
    Fetch one symbol sequentially.
    On rate limit: wait RATE_LIMIT_WAIT seconds and retry up to MAX_RETRIES times.
    On other errors: skip immediately.
    """
    tk = get_yf_ticker(sym)
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(tk, start=start, end=end, interval="1d",
                             auto_adjust=False, progress=False, timeout=25)
            rows = parse_df(df)
            return rows  # None if empty, that's fine
        except Exception as e:
            if is_rate_limit(e):
                wait = RATE_LIMIT_WAIT + random.uniform(0, 5)
                print(f"[{sym}] Rate limit (attempt {attempt+1}/{MAX_RETRIES}), "
                      f"pausing {wait:.0f}s …")
                time.sleep(wait)
            else:
                print(f"[{sym}] Error: {type(e).__name__}: {e}")
                return None
    print(f"[{sym}] Giving up after {MAX_RETRIES} attempts")
    return None

def fetch_symbols(symbols, start, end):
    """
    Pure sequential fetch — one symbol at a time with INTER_SYMBOL gap.
    No bulk download (bulk is what triggers mass rate limits on Render's shared IP).
    """
    result, failed = {}, []
    for i, sym in enumerate(symbols):
        rows = fetch_single(sym, start, end)
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





# ── Breeze API Proxy ──────────────────────────────────────────────────────────
# Uses the official breeze-connect SDK which handles SHA256 checksum correctly.
# Install: pip install breeze-connect
# The SDK's generate_session() initialises auth; get_historical_data_v2() fetches OHLC.

import hashlib as _hashlib
import json as _json

BREEZE_API_BASE = 'https://api.icicidirect.com/breezeapi/api/v1'

def _breeze_checksum(timestamp, payload_str, secret_key):
    raw = timestamp + payload_str + secret_key
    return 'token ' + _hashlib.sha256(raw.encode('utf-8')).hexdigest()

def _breeze_fetch_ohlc(api_key, secret_key, session_token, stock_code, exchange_code, from_date, to_date):
    """
    Fetch daily OHLC from Breeze using the official SDK.
    Falls back to raw REST if SDK not installed.
    """
    try:
        from breeze_connect import BreezeConnect
        breeze = BreezeConnect(api_key=api_key)
        breeze.generate_session(api_secret=secret_key, session_token=session_token)
        resp = breeze.get_historical_data_v2(
            interval='1day',
            from_date=from_date,   # e.g. "2025-04-10T07:00:00.000Z"
            to_date=to_date,
            stock_code=stock_code,
            exchange_code=exchange_code,
            product_type='cash',
        )
        if resp.get('Status') != 200:
            return None, resp.get('Error') or 'Breeze error'
        candles = resp.get('Success') or []
        return candles, None

    except ImportError:
        # SDK not available — use raw REST with manual checksum
        import requests as _req
        payload = _json.dumps({
            'interval':      '1day',
            'from_date':     from_date,
            'to_date':       to_date,
            'stock_code':    stock_code,
            'exchange_code': exchange_code,
            'product_type':  'cash',
        }, separators=(',', ':'))
        ts = datetime.now().__class__.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        headers = {
            'Content-Type':   'application/json',
            'X-AppKey':       api_key,
            'X-SessionToken': session_token,
            'X-Timestamp':    ts,
            'X-Checksum':     _breeze_checksum(ts, payload, secret_key),
        }
        url = f'{BREEZE_API_BASE}/historicalcharts'
        r = _req.get(url, headers=headers, data=payload, timeout=20)
        data = r.json()
        if data.get('Status') != 200:
            return None, data.get('Error') or f'HTTP {r.status_code}'
        return data.get('Success') or [], None

def _parse_breeze_candles(candles):
    rows = []
    for c in candles:
        d = (c.get('datetime') or c.get('date') or '')[:10]
        try:
            o  = float(c.get('open',  0))
            h  = float(c.get('high',  0))
            l  = float(c.get('low',   0))
            cl = float(c.get('close', 0))
            if d and o > 0:
                rows.append({'date': d, 'open': round(o,2), 'high': round(h,2),
                             'low': round(l,2), 'close': round(cl,2), 'adjClose': round(cl,2)})
        except Exception:
            continue
    return rows


@app.route('/breeze-historical')
def breeze_historical():
    """
    Proxy: fetch one symbol's OHLC from Breeze and return clean rows.
    Query params: api_key, secret_key, session_token, stock_code, exchange_code, from_date, to_date
    """
    api_key       = request.args.get('api_key',       '').strip()
    secret_key    = request.args.get('secret_key',    '').strip()
    session_token = request.args.get('session_token', '').strip()
    stock_code    = request.args.get('stock_code',    '').strip()
    exchange_code = request.args.get('exchange_code', 'NSE').strip()
    from_date     = request.args.get('from_date',     '').strip()
    to_date       = request.args.get('to_date',       '').strip()

    if not all([api_key, secret_key, session_token, stock_code, from_date, to_date]):
        return cors_response({'status': 'error', 'message': 'Missing required params'}, 400)

    # Normalise dates to ISO format Breeze expects
    def iso(d):
        return d if 'T' in d else d + 'T07:00:00.000Z'

    try:
        candles, err = _breeze_fetch_ohlc(
            api_key, secret_key, session_token,
            stock_code, exchange_code, iso(from_date), iso(to_date)
        )
        if err:
            return cors_response({'status': 'error', 'message': err}, 400)
        rows = _parse_breeze_candles(candles or [])
        return cors_response({'status': 'ok', 'rows': rows, 'count': len(rows)})
    except Exception as e:
        print(f'[Breeze] {stock_code} error: {e}')
        return cors_response({'status': 'error', 'message': str(e)}, 500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
