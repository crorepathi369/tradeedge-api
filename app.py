"""
TradeEdge Cloud API — batched fetching (40 symbols per call, ~10s each)
Stays within Render free tier 30s response limit.
"""
from __future__ import annotations
import os, time, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Run: pip install yfinance pandas flask flask-cors")

app = Flask(__name__)
# Most permissive CORS config — allows file:// (null origin) and everything else
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

# Semaphore — only one Yahoo fetch at a time to avoid rate limiting
_fetch_lock = threading.Semaphore(1)

def cors_response(data, status=200):
    """Wrap jsonify with explicit CORS headers to handle null origin from file://"""
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

# Handle preflight OPTIONS for all routes
@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def options_handler(path=''):
    return cors_response({'ok': True})

YAHOO_TICKER_MAP = {
    # Indices
    "NIFTY50":    "^NSEI",
    "BANKNIFTY":  "^NSEBANK",
    "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY": "^NSEMDCP50",
    # Stocks with non-standard Yahoo tickers
    "M&M":        "M&M.NS",
    "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
    "ETERNAL":    "ETERNAL.NS",       # formerly ZOMATO
    "UNITDSPR":   "UNITDSPR.NS",      # formerly MCDOWELL-N
    "ICICIPRULI": "ICICIPRULI.NS",    # ICICI Prudential Life
    "BLUESTARCO": "BLUESTARCO.NS",  # Blue Star
    "MAZDOCK":    "MAZDOCK.NS",       # Mazagon Dock
}


ALL_SYMBOLS = [
    "NIFTY50","BANKNIFTY","FINNIFTY","MIDCPNIFTY","360ONE","ABB","ABCAPITAL","ADANIENSOL",
    "ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APLAPOLLO",
    "APOLLOHOSP","ASHOKLEY","ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO",
    "BAJAJFINSV","BAJAJHLDNG","BAJFINANCE","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHARATFORG","BHARTIARTL","BHEL","BIOCON","BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA",
    "BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA",
    "COFORGE","COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY",
    "DIVISLAB","DIXON","DLF","DMART","DRREDDY","EICHERMOT","ETERNAL","EXIDEIND",
    "FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM","HAL",
    "HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO",
    "HINDUNILVR","HINDZINC","HUDCO","ICICIBANK","ICICIGI","ICICIPRULI","IDEA","IDFCFIRSTB",
    "IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND",
    "IOC","IREDA","IRFC","ITC","JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL",
    "JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KOTAKBANK","KPITTECH","LAURUSLABS",
    "LICHSGFIN","LICI","LODHA","LT","LTF","LTIM","LUPIN","M&M",
    "MANAPPURAM","MANKIND","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MOTHERSON","MPHASIS","MUTHOOTFIN","NATIONALUM","NAUKRI","NBCC","NESTLEIND","NHPC",
    "NMDC","NTPC","NUVAMA","NYKAA","OBEROIRLTY","OFSS","OIL","ONGC",
    "PAGEIND","PATANJALI","PAYTM","PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","PREMIERENE",
    "PRESTIGE","RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP","SBICARD",
    "SBILIFE","SBIN","SHREECEM","SHRIRAMFIN","SIEMENS","SOLARINDS","SONACOMS","SRF",
    "SUNPHARMA","SUPREMEIND","SUZLON","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER",
    "TATASTEEL","TATATECH","TCS","TECHM","TIINDIA","TITAN","TORNTPHARM","TORNTPOWER",
    "TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK","UNITDSPR","UNOMINDA","UPL","VBL",
    "VEDL","VOLTAS","WAAREEENER","WIPRO","YESBANK","ZYDUSLIFE",
]

def get_yf_ticker(s):
    return YAHOO_TICKER_MAP.get(s, s + ".NS")

def parse_df(df):
    """
    Parse a yfinance DataFrame (auto_adjust=False) into row dicts.
    - close     = unadjusted close (real market price, used for entry/exit)
    - adj_close = adjusted close   (div/split corrected, used for signal detection)
    This matches how the browser fetchOHLC stores both fields separately.
    """
    if df is None or df.empty: return None
    df = df.copy()
    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    if not all(c in df.columns for c in ["open","high","low","close"]): return None
    df = df[df["open"] > 0].round(2)
    # Use adj_close if present (from auto_adjust=False download); else fallback to close
    has_adj = "adj_close" in df.columns
    rows = []
    for i, r in df.iterrows():
        row = {
            "date":      pd.Timestamp(i).strftime("%Y-%m-%d"),
            "open":      round(float(r["open"]),  2),
            "high":      round(float(r["high"]),  2),
            "low":       round(float(r["low"]),   2),
            "close":     round(float(r["close"]), 2),
            "adjClose":  round(float(r["adj_close"]), 2) if has_adj and pd.notna(r["adj_close"]) else round(float(r["close"]), 2),
        }
        rows.append(row)
    return rows if rows else None

def fetch_batch(symbols, start, end):
    tickers = [get_yf_ticker(s) for s in symbols]
    t2s = {get_yf_ticker(s): s for s in symbols}
    result, failed = {}, []
    try:
        raw = yf.download(tickers, start=start, end=end, interval="1d",
                          auto_adjust=False, progress=False,
                          group_by="ticker", timeout=25)

        if raw is None or raw.empty:
            print(f"Batch returned empty DataFrame for {len(symbols)} symbols")
            failed.extend(symbols)
            return result, failed

        print(f"Batch raw shape: {raw.shape}, cols type: {type(raw.columns)}")

        for tk, sym in t2s.items():
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    # New yfinance: MultiIndex is (field, ticker) or (ticker, field)
                    # Detect orientation by checking level values
                    lvl0 = [str(v) for v in raw.columns.get_level_values(0)]
                    lvl1 = [str(v) for v in raw.columns.get_level_values(1)]
                    if tk in lvl1:
                        # Format: (field, ticker) — standard group_by='ticker' output
                        df = raw.xs(tk, axis=1, level=1).copy()
                    elif tk in lvl0:
                        # Format: (ticker, field)
                        df = raw.xs(tk, axis=1, level=0).copy()
                    else:
                        print(f"Ticker {tk} not found in MultiIndex")
                        failed.append(sym)
                        continue
                    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
                else:
                    df = raw.copy()
                    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]

                rows = parse_df(df)
                if rows:
                    result[sym] = rows
                else:
                    print(f"parse_df returned None for {sym} ({tk})")
                    failed.append(sym)
            except Exception as e:
                print(f"Error parsing {sym} ({tk}): {e}")
                failed.append(sym)

    except Exception as e:
        print(f"Batch download error: {e}")
        failed.extend(symbols)

    print(f"Batch done: {len(result)} ok, {len(failed)} failed")
    return result, failed


@app.route("/")
def health():
    return cors_response({"status":"ok","service":"TradeEdge API",
                    "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "symbols":len(ALL_SYMBOLS)})


@app.route("/sync-today")
def sync_today():
    offset = int(request.args.get("offset", 0))
    limit  = int(request.args.get("limit",  40))
    days   = int(request.args.get("days",   5))   # default 5; TradeEdge app passes &days=10 explicitly
    syms   = ALL_SYMBOLS[offset:offset + limit]

    if not syms:
        return cors_response({"status":"ok","fetched":0,"failed":0,
                        "failedSymbols":[],"elapsed":0,"data":{},
                        "asOf":datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "done":True})

    end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    t0    = time.time()

    # Small pause between batches — reduces Yahoo Finance rate limit risk
    # First batch (offset=0) gets no delay; subsequent batches get 2s
    if offset > 0:
        time.sleep(2)

    # Acquire lock — only one Yahoo fetch runs at a time across all clients
    acquired = _fetch_lock.acquire(timeout=25)
    if not acquired:
        return cors_response({"status":"error","error":"Server busy, retry shortly"}, 503)

    try:
        result, failed = fetch_batch(syms, start, end)
    finally:
        _fetch_lock.release()

    return cors_response({
        "status":       "ok",
        "fetched":      len(result),
        "failed":       len(failed),
        "failedSymbols": failed,
        "elapsed":      round(time.time()-t0, 1),
        "asOf":         datetime.now().strftime("%Y-%m-%d %H:%M"),
        "data":         result,
        "offset":       offset,
        "limit":        limit,
        "grandTotal":   len(ALL_SYMBOLS),
        "done":         (offset + limit) >= len(ALL_SYMBOLS),
    })
