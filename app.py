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
    "AMARAJABAT":"AMARARAJA.NS","BIRLASOFT":"BSOFT.NS","DEEPAKNITR":"DEEPAKNTR.NS",
    "ICICIPRULIFE":"ICICIPRULI.NS","MCDOWELL-N":"UNITDSPR.NS",
    "NAVINFLOUR":"NAVNFLUOR.NS",
    "TATAMOTORS":"TMPV.NS",
    "ZOMATO":"ETERNAL.NS",
}

ALL_SYMBOLS = [
    "NIFTY50","BANKNIFTY","FINNIFTY","MIDCPNIFTY",
    "CNXIT","CNXAUTO","CNXPHARMA","CNXENERGY","CNXMETAL","CNXFMCG","CNXINFRA","CNXCONSUM",
    "AARTIIND","ABB","ABCAPITAL","ABFRL","ACC","ADANIENT","ADANIGREEN",
    "ADANIPORTS","ALKEM","AMARAJABAT","AMBUJACEM","AMBER","APOLLOHOSP",
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
    "NATIONALUM","NAUKRI","NAVINFLOUR","NBCC","NESTLEIND","NHPC",
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
    if df is None or df.empty: return None
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if not all(c in df.columns for c in ["open","high","low","close"]): return None
    df = df[["open","high","low","close"]].dropna()
    df = df[df["open"] > 0].round(2)
    if df.empty: return None
    return [{"date": pd.Timestamp(i).strftime("%Y-%m-%d"),
             "open": round(float(r["open"]),2), "high": round(float(r["high"]),2),
             "low":  round(float(r["low"]),2),  "close": round(float(r["close"]),2)}
            for i, r in df.iterrows()]

def fetch_batch(symbols, start, end):
    tickers = [get_yf_ticker(s) for s in symbols]
    t2s = {get_yf_ticker(s): s for s in symbols}
    result, failed = {}, []
    try:
        raw = yf.download(tickers, start=start, end=end, interval="1d",
                          auto_adjust=True, progress=False,
                          group_by="ticker", timeout=25)
        for tk, sym in t2s.items():
            try:
                df = raw[tk] if isinstance(raw.columns, pd.MultiIndex) else raw
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
    syms   = ALL_SYMBOLS[offset:offset + limit]

    if not syms:
        return cors_response({"status":"ok","fetched":0,"failed":0,
                        "failedSymbols":[],"elapsed":0,"data":{},
                        "asOf":datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "done":True})

    end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
