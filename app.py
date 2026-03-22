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
    # Indices
    "NIFTY50":     "^NSEI",
    "BANKNIFTY":   "^NSEBANK",
    "FINNIFTY":    "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":  "^NSEMDCP50",
    # Stocks with non-standard Yahoo tickers
    "M&M":         "M&M.NS",
    "BAJAJ-AUTO":  "BAJAJ-AUTO.NS",
    "ETERNAL":     "ETERNAL.NS",      # Formerly Zomato
    "ICICIPRULI":  "ICICIPRULI.NS",
    "JIOFIN":      "JIOFIN.NS",
    "LODHA":       "LODHA.NS",
    "INDUSTOWER":  "INDUSTOWER.NS",
    "SAMMAANCAP":  "SAMMAANCAP.NS",
    "RVNL":        "RVNL.NS",
    "PGEL":        "PGEL.NS",
    "WAAREEENER":  "WAAREEENER.NS",
    "INOXWIND":    "INOXWIND.NS",
    "IREDA":       "IREDA.NS",
    "SWIGGY":      "SWIGGY.NS",
    "SYNGENE":     "SYNGENE.NS",
    "NUVAMA":      "NUVAMA.NS",
    "KFINTECH":    "KFINTECH.NS",
    "KAYNES":      "KAYNES.NS",
    "TATATECH":    "TATATECH.NS",
    "PREMIERENE":  "PREMIERENE.NS",
}

ALL_SYMBOLS = [
    # ── Indices ───────────────────────────────────────────────────────────────
    "NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    # ── A ─────────────────────────────────────────────────────────────────────
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN",
    "ADANIPORTS", "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APLAPOLLO",
    "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUBANK", "AUROPHARMA",
    "AXISBANK",
    # ── B ─────────────────────────────────────────────────────────────────────
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHLDNG", "BAJFINANCE", "BANDHANBNK",
    "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL",
    "BHEL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE",
    # ── C ─────────────────────────────────────────────────────────────────────
    "CAMS", "CANBK", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA",
    "COFORGE", "COLPAL", "CONCOR", "CROMPTON", "CUMMINSIND",
    # ── D ─────────────────────────────────────────────────────────────────────
    "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DMART",
    "DRREDDY",
    # ── E ─────────────────────────────────────────────────────────────────────
    "EICHERMOT", "ETERNAL", "EXIDEIND",
    # ── F ─────────────────────────────────────────────────────────────────────
    "FEDERALBNK", "FORTIS",
    # ── G ─────────────────────────────────────────────────────────────────────
    "GAIL", "GLENMARK", "GODREJCP", "GODREJPROP", "GRASIM",
    # ── H ─────────────────────────────────────────────────────────────────────
    "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HUDCO",
    # ── I ─────────────────────────────────────────────────────────────────────
    "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDEA", "IDFCFIRSTB", "IEX",
    "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY",
    "INOXWIND", "IOC", "IREDA", "IRFC", "ITC",
    # ── J ─────────────────────────────────────────────────────────────────────
    "JINDALSTEL", "JIOFIN", "JSWENERGY", "JSWSTEEL", "JUBLFOOD",
    # ── K ─────────────────────────────────────────────────────────────────────
    "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH",
    # ── L ─────────────────────────────────────────────────────────────────────
    "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTIM", "LUPIN",
    # ── M ─────────────────────────────────────────────────────────────────────
    "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK",
    "MCX", "MFSL", "MOTHERSON", "MPHASIS", "MUTHOOTFIN",
    # ── N ─────────────────────────────────────────────────────────────────────
    "NATIONALUM", "NAUKRI", "NBCC", "NESTLEIND", "NHPC", "NMDC", "NTPC",
    "NUVAMA", "NYKAA",
    # ── O ─────────────────────────────────────────────────────────────────────
    "OBEROIRLTY", "OFSS", "OIL", "ONGC",
    # ── P ─────────────────────────────────────────────────────────────────────
    "PAGEIND", "PATANJALI", "PAYTM", "PERSISTENT", "PETRONET", "PFC", "PGEL",
    "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING", "POLICYBZR",
    "POLYCAB", "POWERGRID", "PREMIERENE", "PRESTIGE",
    # ── R ─────────────────────────────────────────────────────────────────────
    "RBLBANK", "RECLTD", "RELIANCE", "RVNL",
    # ── S ─────────────────────────────────────────────────────────────────────
    "SAIL", "SAMMAANCAP", "SBICARD", "SBILIFE", "SBIN", "SHREECEM",
    "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUNPHARMA",
    "SUPREMEIND", "SUZLON", "SWIGGY", "SYNGENE",
    # ── T ─────────────────────────────────────────────────────────────────────
    "TATACONSUM", "TATAELXSI", "TATAPOWER", "TATASTEEL", "TATATECH", "TCS",
    "TECHM", "TIINDIA", "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TVSMOTOR",
    # ── U ─────────────────────────────────────────────────────────────────────
    "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL",
    # ── V ─────────────────────────────────────────────────────────────────────
    "VBL", "VEDL", "VOLTAS",
    # ── W ─────────────────────────────────────────────────────────────────────
    "WAAREEENER", "WIPRO",
    # ── Y ─────────────────────────────────────────────────────────────────────
    "YESBANK",
    # ── Z ─────────────────────────────────────────────────────────────────────
    "ZYDUSLIFE",
]

def get_yf_ticker(s):
    return YAHOO_TICKER_MAP.get(s, s + ".NS")

def parse_df(df):
    if df is None or df.empty: return None
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if not all(c in df.columns for c in ["open","high","low","close"]): return None
    df = df[["open","high","low","close","volume"] if "volume" in df.columns else ["open","high","low","close"]].dropna(subset=["open","high","low","close"])
    df = df[df["open"] > 0].round(2)
    if df.empty: return None
    return [{"date": pd.Timestamp(i).strftime("%Y-%m-%d"),
             "open": round(float(r["open"]),2), "high": round(float(r["high"]),2),
             "low":  round(float(r["low"]),2),  "close": round(float(r["close"]),2),
             "volume": int(r["volume"]) if "volume" in r.index and pd.notna(r["volume"]) else 0}
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
