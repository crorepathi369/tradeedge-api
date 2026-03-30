"""
TradeEdge Cloud API — batched fetching (40 symbols per call, ~10s each)
Stays within Render free tier 30s response limit.
"""
from __future__ import annotations
import os, time
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
    "NIFTY50":"^NSEI","BANKNIFTY":"^NSEBANK","FINNIFTY":"NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":"^NSEMDCP50","CNXIT":"^CNXIT","CNXAUTO":"^CNXAUTO",
    "CNXPHARMA":"^CNXPHARMA","CNXENERGY":"^CNXENERGY","CNXMETAL":"^CNXMETAL",
    "CNXFMCG":"^CNXFMCG","CNXINFRA":"^CNXINFRA","CNXCONSUM":"^CNXCONSUM",
    "M&M":"M&M.NS","BAJAJ-AUTO":"BAJAJ-AUTO.NS",
    "BIRLASOFT":"BSOFT.NS","DEEPAKNITR":"DEEPAKNTR.NS",
    "ICICIPRULIFE":"ICICIPRULI.NS","MCDOWELL-N":"UNITDSPR.NS",
    "TATAMOTORS":"TMPV.NS",
    "ZOMATO":"ETERNAL.NS",
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
        for tk, sym in t2s.items():
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    df = raw[tk].copy()
                    # Flatten MultiIndex: ("Adj Close", "RELIANCE.NS") -> "adj_close"
                    df.columns = [str(c[0]).lower().replace(" ", "_") for c in df.columns]
                else:
                    df = raw.copy()
                    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
                rows = parse_df(df)
                if rows: result[sym] = rows
                else: failed.append(sym)
            except: failed.append(sym)
    except Exception as e:
        print(f"Batch error: {e}")
        failed.extend(symbols)
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
    days   = int(request.args.get("days",   7))   # default 7 = existing behaviour
    syms   = ALL_SYMBOLS[offset:offset + limit]

    if not syms:
        return cors_response({"status":"ok","fetched":0,"failed":0,
                        "failedSymbols":[],"elapsed":0,"data":{},
                        "asOf":datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "done":True})

    end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    t0    = time.time()

    result, failed = fetch_batch(syms, start, end)

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
