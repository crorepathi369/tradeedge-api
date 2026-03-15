"""
TradeEdge Cloud API — with SSE progress streaming
"""
from __future__ import annotations
import os, time, json
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, Response, stream_with_context
from flask_cors import CORS

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Run: pip install yfinance pandas flask flask-cors")

app = Flask(__name__)
CORS(app, origins="*", supports_credentials=False)

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin']  = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return response

YAHOO_TICKER_MAP = {
    "NIFTY50":"^NSEI","BANKNIFTY":"^NSEBANK","FINNIFTY":"NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":"^NSEMDCP50","CNXIT":"^CNXIT","CNXAUTO":"^CNXAUTO",
    "CNXPHARMA":"^CNXPHARMA","CNXENERGY":"^CNXENERGY","CNXMETAL":"^CNXMETAL",
    "CNXFMCG":"^CNXFMCG","CNXINFRA":"^CNXINFRA","CNXCONSUM":"^CNXCONSUM",
    "M&M":"M&M.NS","BAJAJ-AUTO":"BAJAJ-AUTO.NS","AMARAJABAT":"AMARARAJA.NS",
    "BIRLASOFT":"BSOFT.NS","DEEPAKNITR":"DEEPAKNTR.NS","ICICIPRULIFE":"ICICIPRULI.NS",
    "MCDOWELL-N":"UNITDSPR.NS","NAVINFLOUR":"NAVNFLUOR.NS",
    "TATAMOTORS":"TATAMOTORS.NS","ZOMATO":"ETERNAL.NS",
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

def sse(data):
    return f"data: {json.dumps(data)}\n\n"

@app.route("/")
def health():
    return jsonify({"status":"ok","service":"TradeEdge API",
                    "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "symbols":len(ALL_SYMBOLS)})

@app.route("/sync-today")
def sync_today():
    p = request.args.get("symbols","")
    syms = [s.strip().upper() for s in p.split(",") if s.strip()] if p else ALL_SYMBOLS
    syms = [s for s in syms if s in set(ALL_SYMBOLS)]
    if not syms: return jsonify({"error":"No valid symbols"}), 400
    end   = (datetime.today()+timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today()-timedelta(days=7)).strftime("%Y-%m-%d")
    t0 = time.time()
    tickers = [get_yf_ticker(s) for s in syms]
    t2s = {get_yf_ticker(s):s for s in syms}
    result, failed = {}, []
    try:
        raw = yf.download(tickers,start=start,end=end,interval="1d",
                          auto_adjust=True,progress=False,group_by="ticker",timeout=60)
        for tk,sym in t2s.items():
            try:
                df = raw[tk] if isinstance(raw.columns,pd.MultiIndex) else raw
                rows = parse_df(df)
                if rows: result[sym]=rows
                else: failed.append(sym)
            except: failed.append(sym)
    except Exception as e:
        return jsonify({"error":str(e)}), 500
    return jsonify({"status":"ok","fetched":len(result),"failed":len(failed),
                    "failedSymbols":failed[:20],"elapsed":round(time.time()-t0,1),
                    "asOf":datetime.now().strftime("%Y-%m-%d %H:%M"),"data":result})

@app.route("/sync-today-stream")
def sync_today_stream():
    """SSE: streams progress per batch, sends final JSON at end."""
    p = request.args.get("symbols","")
    syms = [s.strip().upper() for s in p.split(",") if s.strip()] if p else ALL_SYMBOLS
    syms = [s for s in syms if s in set(ALL_SYMBOLS)]

    def generate():
        total   = len(syms)
        result  = {}
        failed  = []
        t0      = time.time()
        end     = (datetime.today()+timedelta(days=1)).strftime("%Y-%m-%d")
        start   = (datetime.today()-timedelta(days=7)).strftime("%Y-%m-%d")
        done    = 0
        BATCH   = 20

        for bi in range(0, total, BATCH):
            batch = syms[bi:bi+BATCH]
            tickers = [get_yf_ticker(s) for s in batch]
            t2s = {get_yf_ticker(s):s for s in batch}
            try:
                raw = yf.download(tickers, start=start, end=end, interval="1d",
                                  auto_adjust=True, progress=False,
                                  group_by="ticker", timeout=30)
                sym_list = list(t2s.values())
                for tk, sym in t2s.items():
                    done += 1
                    try:
                        df = raw[tk] if isinstance(raw.columns, pd.MultiIndex) else raw
                        rows = parse_df(df)
                        ok = rows is not None
                        if ok: result[sym] = rows
                        else: failed.append(sym)
                    except:
                        ok = False
                        failed.append(sym)
                    yield sse({"type":"progress","done":done,"total":total,
                               "sym":sym,"ok":ok,"pct":round(done/total*100)})
            except Exception as e:
                for sym in batch:
                    done += 1
                    failed.append(sym)
                    yield sse({"type":"progress","done":done,"total":total,
                               "sym":sym,"ok":False,"pct":round(done/total*100)})

        yield sse({"type":"done","fetched":len(result),"failed":len(failed),
                   "failedSymbols":failed[:20],"elapsed":round(time.time()-t0,1),
                   "asOf":datetime.now().strftime("%Y-%m-%d %H:%M"),"data":result})

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
