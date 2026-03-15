"""
TradeEdge Cloud API
===================
Fetches today's OHLC for all NSE F&O symbols from Yahoo Finance.
Deploy to Railway / Render — called from the TradeEdge app button.

Endpoints:
  GET  /              → health check
  GET  /sync-today    → returns JSON with today's OHLC for all symbols
  GET  /sync-today?symbols=TCS,RELIANCE  → specific symbols only
"""
from __future__ import annotations
import os, time, json
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Run: pip install yfinance pandas flask flask-cors")

app = Flask(__name__)
# Allow requests from local file:// (origin = 'null') and any web origin
CORS(app, origins="*", supports_credentials=False)

# Extra: handle preflight and null origin explicitly
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin']  = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return response

# ── Ticker map ─────────────────────────────────────────────────────────────────
YAHOO_TICKER_MAP = {
    "NIFTY50":     "^NSEI",
    "BANKNIFTY":   "^NSEBANK",
    "FINNIFTY":    "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":  "^NSEMDCP50",
    "CNXIT":       "^CNXIT",
    "CNXAUTO":     "^CNXAUTO",
    "CNXPHARMA":   "^CNXPHARMA",
    "CNXENERGY":   "^CNXENERGY",
    "CNXMETAL":    "^CNXMETAL",
    "CNXFMCG":     "^CNXFMCG",
    "CNXINFRA":    "^CNXINFRA",
    "CNXCONSUM":   "^CNXCONSUM",
    "M&M":         "M&M.NS",
    "BAJAJ-AUTO":  "BAJAJ-AUTO.NS",
    "AMARAJABAT":  "AMARARAJA.NS",
    "BIRLASOFT":   "BSOFT.NS",
    "DEEPAKNITR":  "DEEPAKNTR.NS",
    "ICICIPRULIFE":"ICICIPRULI.NS",
    "MCDOWELL-N":  "UNITDSPR.NS",
    "NAVINFLOUR":  "NAVNFLUOR.NS",
    "TATAMOTORS":  "TATAMOTORS.NS",
    "ZOMATO":      "ETERNAL.NS",
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
    "EICHERMOT","EMAMILTD","EXIDEIND",
    "FEDERALBNK",
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
    "NMDC","NTPC","NYKAA",
    "OBEROIRLTY","OFSS","ONGC",
    "PAYTM","PFC","PIDILITIND","PIIND","PNBHOUSING","POLICYBZR",
    "POWERGRID","PRESTIGE","PERSISTENT","PNB","PVRINOX",
    "RADICO","RBLBANK","RECLTD","RELIANCE","RPOWER",
    "SAIL","SBICARD","SBILIFE","SBIN","SHREECEM","SIEMENS","SJVN",
    "SRF","STAR","SUNPHARMA","SUZLON",
    "TATACHEM","TATACOMM","TATACONSUM","TATAELXSI","TATAMOTORS",
    "TATAPOWER","TATASTEEL","TCS","TECHM","TIINDIA","TITAN",
    "TORNTPHARM","TORNTPOWER","TRENT",
    "UBL","ULTRACEMCO","UNIONBANK","UPL",
    "VBL","VEDL","VOLTAS",
    "WHIRLPOOL","WIPRO",
    "ZOMATO",
]

def get_yf_ticker(symbol):
    return YAHOO_TICKER_MAP.get(symbol, symbol + ".NS")

def fetch_today_batch(symbols: list[str]) -> dict:
    """Fetch last 5 days for all symbols in one yfinance batch call, return latest candle per symbol."""
    tickers = [get_yf_ticker(s) for s in symbols]
    ticker_to_sym = {get_yf_ticker(s): s for s in symbols}

    end   = datetime.today() + timedelta(days=1)
    start = datetime.today() - timedelta(days=7)  # buffer for weekends/holidays

    result = {}
    failed = []

    try:
        raw = yf.download(
            tickers,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            timeout=30,
        )
    except Exception as e:
        print(f"Batch download error: {e}")
        return {}, symbols

    for ticker_str, sym in ticker_to_sym.items():
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                df = raw[ticker_str].dropna(how="all")
            else:
                # Single symbol fallback
                df = raw.dropna(how="all")

            if df is None or df.empty:
                failed.append(sym)
                continue

            df.columns = [c.lower() for c in df.columns]
            df = df[["open", "high", "low", "close"]].dropna()
            df = df[df["open"] > 0].round(2)

            if df.empty:
                failed.append(sym)
                continue

            # Return ALL rows so app can merge/update properly
            rows = []
            for date_idx, row in df.iterrows():
                date_str = pd.Timestamp(date_idx).strftime("%Y-%m-%d")
                rows.append({
                    "date":  date_str,
                    "open":  round(float(row["open"]),  2),
                    "high":  round(float(row["high"]),  2),
                    "low":   round(float(row["low"]),   2),
                    "close": round(float(row["close"]), 2),
                })
            result[sym] = rows

        except Exception as e:
            print(f"  {sym} ({ticker_str}): {e}")
            failed.append(sym)

    return result, failed


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
    return jsonify({
        "status": "ok",
        "service": "TradeEdge API",
        "time": now,
        "symbols": len(ALL_SYMBOLS),
    })

@app.route("/sync-today")
def sync_today():
    # Optional: limit to specific symbols via query param
    syms_param = request.args.get("symbols", "")
    if syms_param:
        symbols = [s.strip().upper() for s in syms_param.split(",") if s.strip()]
        # Validate against known list
        symbols = [s for s in symbols if s in ALL_SYMBOLS]
    else:
        symbols = ALL_SYMBOLS

    if not symbols:
        return jsonify({"error": "No valid symbols specified"}), 400

    t0 = time.time()
    data, failed = fetch_today_batch(symbols)
    elapsed = round(time.time() - t0, 1)

    return jsonify({
        "status":   "ok",
        "fetched":  len(data),
        "failed":   len(failed),
        "failedSymbols": failed[:20],  # cap list for readability
        "elapsed":  elapsed,
        "asOf":     datetime.now().strftime("%Y-%m-%d %H:%M"),
        "data":     data,   # { SYM: [{date,open,high,low,close}, ...] }
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
