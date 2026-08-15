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
import os, time, random, warnings, threading
from pathlib import Path

# yfinance uses pd.Timestamp.utcnow() which is deprecated in pandas 2.x — suppress the noise
warnings.filterwarnings('ignore', message='.*utcnow.*', category=FutureWarning)
warnings.filterwarnings('ignore', message='.*Timestamp.utcnow.*', category=FutureWarning)
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, make_response, send_from_directory
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

try:
    from kiteconnect import KiteConnect
except ImportError:
    KiteConnect = None

import gap_scan
import kite_orders
import telegram_notify

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

# ── Data directory — CSVs written here by /run-fetch, served by /data/ ────────
DATA_DIR = Path(os.environ.get("TRADEEDGE_DATA_DIR", "./tradeedge_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── GitHub restore — pulls CSVs from 'data' branch on every startup ───────────
def restore_data_from_github():
    """
    On startup, download CSVs + SECTOR_MAP.json from the GitHub data branch
    into DATA_DIR. Smart restore — only downloads files that:
      1. Don't exist on disk yet, OR
      2. Are stale (local file older than 20 hours)

    This minimises outbound bandwidth on Render restarts/redeploys.
    A full restore (~186 files x 22 KB = ~4 MB) only happens on the first
    cold start of the day. Subsequent restarts skip fresh files entirely.
    """
    import urllib.request, urllib.error, json as _json

    token  = os.environ.get("GITHUB_TOKEN", "")
    repo   = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch = os.environ.get("GITHUB_DATA_BRANCH", "data")

    if not token:
        print("[restore] GITHUB_TOKEN not set — skipping")
        return

    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
    }

    print(f"[restore] Smart restore from github:{repo}@{branch} -> {DATA_DIR}")

    # Step 1: fetch full file tree (single API call)
    tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
    try:
        tree = _json.loads(urllib.request.urlopen(
            urllib.request.Request(tree_url, headers=headers), timeout=30).read())
    except Exception as e:
        print(f"[restore] tree fetch failed: {e}")
        return

    files = [
        f["path"] for f in tree.get("tree", [])
        if f["type"] == "blob" and (
            f["path"].endswith(".csv") or f["path"] in ("SECTOR_MAP.json", "gap_positions.json", "gap_presets.json", "gap_settings.json", "gap_automation_config.json", "kite_token.json")
        )
    ]
    print(f"[restore] {len(files)} files in data branch")

    # Step 2: smart download — skip files fresher than 20 hours
    # Data pipeline runs at 5 PM IST daily, so 20h covers overnight gap safely
    FRESH_SECS = 20 * 3600
    now_ts     = datetime.utcnow().timestamp()
    downloaded = skipped = failed = 0

    for filename in files:
        dest = DATA_DIR / filename
        if dest.exists():
            age = now_ts - dest.stat().st_mtime
            if age < FRESH_SECS:
                skipped += 1
                continue   # fresh enough — no download needed

        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{filename}"
        try:
            data = urllib.request.urlopen(
                urllib.request.Request(raw_url, headers={
                    "Authorization": f"token {token}",
                    "User-Agent":    "TradeEdge-App",
                }), timeout=30).read()
            dest.write_bytes(data)
            downloaded += 1
        except Exception as e:
            print(f"[restore] x {filename}: {e}")
            failed += 1

    size_kb = downloaded * 22
    print(f"[restore] Done — downloaded={downloaded} (~{size_kb} KB)  "
          f"skipped(fresh)={skipped}  failed={failed}")

# Run restore in a background thread so Flask starts up immediately
# (Render health check hits '/' within 30s — we can't block startup)
threading.Thread(target=restore_data_from_github, daemon=True).start()

# ── Fetch job state — prevents overlapping runs ────────────────────────────────
_fetch_lock   = threading.Lock()
_fetch_status = {
    "running":    False,
    "startedAt":  None,
    "finishedAt": None,
    "status":     "idle",
    "done":       False,
    "error":      None,
    "pct":        "",
    "ok":         0,
    "failed":     0,
    "total":      0,
    "failedSyms": [],
    "lastError":  None,
    "log":        [],
    "cancelled":  False,   # set True by /breeze/cancel to stop the fetch loop
}

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
    "TATACHEM","TATACOMM","TATACONSUM","TATAELXSI","TMPV",
    "TATAPOWER","TATASTEEL","TCS","TECHM","TIINDIA","TITAN",
    "TORNTPHARM","TORNTPOWER","TRENT",
    "UBL","ULTRACEMCO","UNIONBANK","UPL",
    "VBL","VEDL","VOLTAS","WHIRLPOOL","WIPRO","ZOMATO",
]

def get_yf_ticker(s):
    return YAHOO_TICKER_MAP.get(s, s + ".NS")

# ── Sector map — written to SECTOR_MAP.json by /run-fetch ─────────────────────
STOCK_SECTOR_MAP = {
    "TCS":"SECTOR_IT","INFY":"SECTOR_IT","HCLTECH":"SECTOR_IT","WIPRO":"SECTOR_IT",
    "TECHM":"SECTOR_IT","LTIM":"SECTOR_IT","MPHASIS":"SECTOR_IT","COFORGE":"SECTOR_IT",
    "KPITTECH":"SECTOR_IT","PERSISTENT":"SECTOR_IT","TATAELXSI":"SECTOR_IT",
    "OFSS":"SECTOR_IT","NAUKRI":"SECTOR_IT","BIRLASOFT":"SECTOR_IT","LTTS":"SECTOR_IT",
    "HDFCBANK":"SECTOR_BANK","ICICIBANK":"SECTOR_BANK","SBIN":"SECTOR_BANK",
    "KOTAKBANK":"SECTOR_BANK","AXISBANK":"SECTOR_BANK","INDUSINDBK":"SECTOR_BANK",
    "BANKBARODA":"SECTOR_BANK","PNB":"SECTOR_BANK","CANBK":"SECTOR_BANK",
    "FEDERALBNK":"SECTOR_BANK","IDFCFIRSTB":"SECTOR_BANK","AUBANK":"SECTOR_BANK",
    "RBLBANK":"SECTOR_BANK","BANDHANBNK":"SECTOR_BANK","YESBANK":"SECTOR_BANK",
    "UNIONBANK":"SECTOR_BANK","IDBI":"SECTOR_BANK",
    "BAJFINANCE":"SECTOR_FINANCE","BAJAJFINSV":"SECTOR_FINANCE","SHRIRAMFIN":"SECTOR_FINANCE",
    "CHOLAFIN":"SECTOR_FINANCE","MUTHOOTFIN":"SECTOR_FINANCE","SBICARD":"SECTOR_FINANCE",
    "SBILIFE":"SECTOR_FINANCE","HDFCLIFE":"SECTOR_FINANCE","HDFCAMC":"SECTOR_FINANCE",
    "JIOFIN":"SECTOR_FINANCE","ABCAPITAL":"SECTOR_FINANCE","LTF":"SECTOR_FINANCE",
    "POLICYBZR":"SECTOR_FINANCE","CDSL":"SECTOR_FINANCE","MCX":"SECTOR_FINANCE",
    "360ONE":"SECTOR_FINANCE","ICICIGI":"SECTOR_FINANCE","MANAPPURAM":"SECTOR_FINANCE",
    "LICHSGFIN":"SECTOR_FINANCE","M&MFIN":"SECTOR_FINANCE","PNBHOUSING":"SECTOR_FINANCE",
    "SUNPHARMA":"SECTOR_PHARMA","DRREDDY":"SECTOR_PHARMA","CIPLA":"SECTOR_PHARMA",
    "DIVISLAB":"SECTOR_PHARMA","AUROPHARMA":"SECTOR_PHARMA","LUPIN":"SECTOR_PHARMA",
    "BIOCON":"SECTOR_PHARMA","ALKEM":"SECTOR_PHARMA","TORNTPHARM":"SECTOR_PHARMA",
    "GLENMARK":"SECTOR_PHARMA","MANKIND":"SECTOR_PHARMA","MAXHEALTH":"SECTOR_PHARMA",
    "APOLLOHOSP":"SECTOR_PHARMA","IPCALAB":"SECTOR_PHARMA","LALPATHLAB":"SECTOR_PHARMA",
    "LAURUSLABS":"SECTOR_PHARMA","ZYDUSLIFE":"SECTOR_PHARMA","GLAND":"SECTOR_PHARMA",
    "SRF":"SECTOR_PHARMA","PIDILITIND":"SECTOR_PHARMA","UPL":"SECTOR_PHARMA",
    "MARUTI":"SECTOR_AUTO","BAJAJ-AUTO":"SECTOR_AUTO","M&M":"SECTOR_AUTO",
    "EICHERMOT":"SECTOR_AUTO","TVSMOTOR":"SECTOR_AUTO","HEROMOTOCO":"SECTOR_AUTO",
    "BOSCHLTD":"SECTOR_AUTO","BHARATFORG":"SECTOR_AUTO","MOTHERSON":"SECTOR_AUTO",
    "APOLLOTYRE":"SECTOR_AUTO","ASHOKLEY":"SECTOR_AUTO","BALKRISIND":"SECTOR_AUTO",
    "EXIDEIND":"SECTOR_AUTO","TIINDIA":"SECTOR_AUTO","CUMMINSIND":"SECTOR_AUTO",
    "HINDUNILVR":"SECTOR_FMCG","ITC":"SECTOR_FMCG","NESTLEIND":"SECTOR_FMCG",
    "BRITANNIA":"SECTOR_FMCG","DABUR":"SECTOR_FMCG","MARICO":"SECTOR_FMCG",
    "COLPAL":"SECTOR_FMCG","GODREJCP":"SECTOR_FMCG","TATACONSUM":"SECTOR_FMCG",
    "VBL":"SECTOR_FMCG","KALYANKJIL":"SECTOR_FMCG","TITAN":"SECTOR_FMCG",
    "TRENT":"SECTOR_FMCG","PAGEIND":"SECTOR_FMCG","DMART":"SECTOR_FMCG",
    "JUBLFOOD":"SECTOR_FMCG","NYKAA":"SECTOR_FMCG","ZOMATO":"SECTOR_FMCG",
    "ASIANPAINT":"SECTOR_FMCG","BERGEPAINT":"SECTOR_FMCG","EMAMILTD":"SECTOR_FMCG",
    "UBL":"SECTOR_FMCG","MCDOWELL-N":"SECTOR_FMCG","RADICO":"SECTOR_FMCG",
    "INDHOTEL":"SECTOR_FMCG","PVRINOX":"SECTOR_FMCG","STAR":"SECTOR_FMCG",
    "TATASTEEL":"SECTOR_METAL","JSWSTEEL":"SECTOR_METAL","HINDALCO":"SECTOR_METAL",
    "VEDL":"SECTOR_METAL","SAIL":"SECTOR_METAL","NMDC":"SECTOR_METAL",
    "HINDZINC":"SECTOR_METAL","JINDALSTEL":"SECTOR_METAL","NATIONALUM":"SECTOR_METAL",
    "DLF":"SECTOR_REALTY","GODREJPROP":"SECTOR_REALTY","OBEROIRLTY":"SECTOR_REALTY",
    "LODHA":"SECTOR_REALTY","PRESTIGE":"SECTOR_REALTY","PHOENIXLTD":"SECTOR_REALTY",
    "NBCC":"SECTOR_REALTY","HUDCO":"SECTOR_REALTY","IRB":"SECTOR_REALTY",
    "RELIANCE":"SECTOR_ENERGY","ONGC":"SECTOR_ENERGY","NTPC":"SECTOR_ENERGY",
    "POWERGRID":"SECTOR_ENERGY","BPCL":"SECTOR_ENERGY","IOC":"SECTOR_ENERGY",
    "HINDPETRO":"SECTOR_ENERGY","GAIL":"SECTOR_ENERGY","PETRONET":"SECTOR_ENERGY",
    "TATAPOWER":"SECTOR_ENERGY","JSWENERGY":"SECTOR_ENERGY","ADANIGREEN":"SECTOR_ENERGY",
    "ADANIENSOL":"SECTOR_ENERGY","NHPC":"SECTOR_ENERGY","SUZLON":"SECTOR_ENERGY",
    "TORNTPOWER":"SECTOR_ENERGY","SJVN":"SECTOR_ENERGY","RPOWER":"SECTOR_ENERGY",
    "CESC":"SECTOR_ENERGY","IGL":"SECTOR_ENERGY","MGL":"SECTOR_ENERGY",
    "LT":"SECTOR_INFRA","ABB":"SECTOR_INFRA","SIEMENS":"SECTOR_INFRA",
    "BEL":"SECTOR_INFRA","HAL":"SECTOR_INFRA","BHEL":"SECTOR_INFRA",
    "CGPOWER":"SECTOR_INFRA","POLYCAB":"SECTOR_INFRA","HAVELLS":"SECTOR_INFRA",
    "KEI":"SECTOR_INFRA","DIXON":"SECTOR_INFRA","ADANIPORTS":"SECTOR_INFRA",
    "ADANIENT":"SECTOR_INFRA","INDIGO":"SECTOR_INFRA","DELHIVERY":"SECTOR_INFRA",
    "ULTRACEMCO":"SECTOR_INFRA","AMBUJACEM":"SECTOR_INFRA","SHREECEM":"SECTOR_INFRA",
    "GRASIM":"SECTOR_INFRA","ACC":"SECTOR_INFRA","CONCOR":"SECTOR_INFRA",
    "AMBER":"SECTOR_INFRA","VOLTAS":"SECTOR_INFRA","WHIRLPOOL":"SECTOR_INFRA",
    "COALINDIA":"SECTOR_PSU","RECLTD":"SECTOR_PSU","PFC":"SECTOR_PSU",
    "IRFC":"SECTOR_PSU","LICI":"SECTOR_PSU","TATACHEM":"SECTOR_PHARMA",
    "COROMANDEL":"SECTOR_PHARMA","CHAMBLFERT":"SECTOR_PHARMA",
    "PAYTM":"SECTOR_FINANCE","INDIAMART":"SECTOR_IT","BSE":"SECTOR_FINANCE",
    "CAMS":"SECTOR_FINANCE","IIFL":"SECTOR_FINANCE","TMPV":"SECTOR_AUTO",
    "TATACOMM":"SECTOR_IT",
}

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

INTER_SYMBOL      = 1.0   # seconds between symbols in sequential fallback
RATE_LIMIT_WAIT   = 20.0  # seconds to pause on rate limit (sequential fallback only)
MAX_RETRIES       = 2     # retries per symbol in sequential fallback
MAX_BATCH_SECS    = 130   # safety cutoff: return partial results before HTML client timeout
MAX_BATCH_RL_HITS = 3     # sequential fallback: abort if this many RL hits

def fetch_symbols_bulk(symbols, start, end):
    """
    Primary fetch path: one yf.download() call for the whole batch (N symbols → 1 HTTP request).
    Far less likely to trigger Yahoo rate limits than N sequential Ticker.history() calls.
    Falls back to sequential per-symbol fetch if the bulk call fails or returns nothing.
    """
    yf_tickers = [get_yf_ticker(s) for s in symbols]
    sym_by_yf  = {yf_tickers[i]: symbols[i] for i in range(len(symbols))}

    for attempt in range(2):
        try:
            if attempt > 0:
                time.sleep(5)
            df = yf.download(
                tickers=yf_tickers,
                start=start, end=end,
                interval="1d",
                auto_adjust=False,
                actions=False,
                group_by="ticker",
                progress=False,
                threads=False,
            )
            if df is None or df.empty:
                print(f"[bulk] Empty result on attempt {attempt+1}, falling back")
                return fetch_symbols_sequential(symbols, start, end)

            result, failed = {}, []

            for yf_tk, our_sym in sym_by_yf.items():
                try:
                    if isinstance(df.columns, pd.MultiIndex):
                        # Multi-ticker: columns = MultiIndex[(price_type, ticker), ...]
                        # Slice to get this ticker's sub-DataFrame (flat columns)
                        level1 = df.columns.get_level_values(1)
                        if yf_tk in level1:
                            sym_df = df.xs(yf_tk, axis=1, level=1)
                        else:
                            # ticker not found in result (delisted / bad ticker)
                            failed.append(our_sym)
                            continue
                    else:
                        # Single-ticker download returns a flat DataFrame
                        sym_df = df

                    rows = parse_df(sym_df)
                    if rows:
                        result[our_sym] = rows
                    else:
                        failed.append(our_sym)
                except Exception as e:
                    print(f"[{our_sym}] Parse error: {type(e).__name__}: {e}")
                    failed.append(our_sym)

            print(f"[bulk] fetched={len(result)} failed={len(failed)} of {len(symbols)}")
            return result, failed

        except Exception as e:
            print(f"[bulk] attempt {attempt+1} error: {type(e).__name__}: {e}")

    # Both bulk attempts failed — fall back to sequential
    print("[bulk] both attempts failed, falling back to sequential fetch")
    return fetch_symbols_sequential(symbols, start, end)


def fetch_single(sym, start, end):
    """Sequential fallback: fetch one symbol via Ticker.history()."""
    tk = get_yf_ticker(sym)
    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(tk)
            df = ticker.history(start=start, end=end, interval="1d",
                                auto_adjust=False, actions=False)
            rows = parse_df(df)
            return rows, False
        except Exception as e:
            if YFPricesMissingError and isinstance(e, YFPricesMissingError):
                return None, False
            if YFTzMissingError and isinstance(e, YFTzMissingError):
                return None, False
            if is_rate_limit(e):
                wait = RATE_LIMIT_WAIT + random.uniform(0, 5)
                print(f"[{sym}] Rate limit (attempt {attempt+1}/{MAX_RETRIES}), pausing {wait:.0f}s")
                time.sleep(wait)
            else:
                print(f"[{sym}] Error: {type(e).__name__}: {e}")
                return None, False
    print(f"[{sym}] Giving up after {MAX_RETRIES} rate-limit retries")
    return None, True


def fetch_symbols_sequential(symbols, start, end):
    """Sequential fallback — used when bulk download fails."""
    result, failed = {}, []
    batch_t0, rl_hits = time.time(), 0
    for i, sym in enumerate(symbols):
        if time.time() - batch_t0 > MAX_BATCH_SECS:
            failed.extend(symbols[i:])
            print(f"[seq] Safety cutoff — skipping {len(symbols) - i} remaining")
            break
        if rl_hits >= MAX_BATCH_RL_HITS:
            failed.extend(symbols[i:])
            print(f"[seq] RL abort after {rl_hits} hits — skipping {len(symbols) - i} remaining")
            break
        rows, was_rl = fetch_single(sym, start, end)
        if was_rl:
            rl_hits += 1
        if rows:
            result[sym] = rows
        else:
            failed.append(sym)
        if i < len(symbols) - 1:
            time.sleep(INTER_SYMBOL)
    return result, failed


def fetch_symbols(symbols, start, end):
    """Entry point: bulk first, sequential fallback."""
    return fetch_symbols_bulk(symbols, start, end)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    csv_count = len(list(DATA_DIR.glob("*.csv"))) if DATA_DIR.exists() else 0
    return cors_response({
        "status":   "ok",
        "service":  "TradeEdge API",
        "time":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbols":  len(ALL_SYMBOLS),
        "csvFiles": csv_count,
        "dataDir":  str(DATA_DIR),
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

@app.route("/sync-range")
def sync_range():
    """
    Incremental sync for an explicit date range — used by quickSync() in the HTML.
    Accepts from/to as YYYY-MM-DD strings so the client controls exactly which dates to fetch.
    Same response shape as /sync-today for easy client reuse.
    """
    offset   = int(request.args.get("offset", 0))
    limit    = int(request.args.get("limit",  10))
    from_str = request.args.get("from", "")
    to_str   = request.args.get("to",   "")

    if not from_str:
        return cors_response({"status": "error", "error": "missing 'from' param"}, 400)

    # Default to tomorrow if no end date given (yfinance end is exclusive)
    end = to_str if to_str else (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    # yfinance end is exclusive — add 1 day to include to_str's data
    try:
        end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        pass

    syms = ALL_SYMBOLS[offset:offset + limit]
    if not syms:
        return cors_response({
            "status": "ok", "fetched": 0, "failed": 0,
            "failedSymbols": [], "elapsed": 0, "data": {},
            "asOf": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "done": True,
        })

    t0 = time.time()
    result, failed = fetch_symbols(syms, from_str, end)

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

def _do_fetch_job():
    """
    Background worker: incremental fetch for all F&O symbols, saves CSVs to DATA_DIR.
    Runs in a daemon thread so it never blocks Flask request handlers.
    """
    import json as _json
    global _fetch_status

    end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")

    _fetch_status.update({
        "running": True, "startedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finishedAt": None, "ok": 0, "failed": 0,
        "failedSyms": [], "lastError": None, "log": [],
    })

    def _log(msg):
        print(msg)
        _fetch_status["log"].append(msg)
        if len(_fetch_status["log"]) > 200:
            _fetch_status["log"] = _fetch_status["log"][-200:]

    _log(f"[fetch-job] Started — {start} → {end}  symbols={len(ALL_SYMBOLS)}")

    # Fetch all symbols in batches of 10 (reuse existing bulk fetch logic)
    BATCH = 10
    ok_count, fail_syms = 0, []

    for i in range(0, len(ALL_SYMBOLS), BATCH):
        batch = ALL_SYMBOLS[i:i + BATCH]
        try:
            result, failed = fetch_symbols(batch, start, end)
        except Exception as e:
            _log(f"[fetch-job] batch {i}–{i+BATCH} exception: {e}")
            fail_syms.extend(batch)
            continue

        # Write / merge each symbol's CSV
        for sym, rows in result.items():
            csv_path = DATA_DIR / f"{sym}.csv"
            try:
                new_df = pd.DataFrame(rows)
                new_df.rename(columns={"adjClose": "adj_close"}, inplace=True)
                col_order = ["date","open","high","low","close","adj_close","volume"]
                for c in col_order:
                    if c not in new_df.columns:
                        new_df[c] = 0
                new_df = new_df[col_order]

                if csv_path.exists():
                    old_df  = pd.read_csv(csv_path, dtype=str)
                    merged  = pd.concat([old_df, new_df.astype(str)], ignore_index=True)
                    merged.drop_duplicates(subset="date", keep="last", inplace=True)
                    merged.sort_values("date", inplace=True)
                    merged.to_csv(csv_path, index=False)
                else:
                    new_df.to_csv(csv_path, index=False)

                ok_count += 1
            except Exception as e:
                _log(f"[fetch-job] CSV write error {sym}: {e}")
                fail_syms.append(sym)

        fail_syms.extend(failed)
        _fetch_status["ok"]      = ok_count
        _fetch_status["failed"]  = len(fail_syms)
        _fetch_status["failedSyms"] = fail_syms
        _log(f"[fetch-job] batch {i//BATCH + 1}/{-(-len(ALL_SYMBOLS)//BATCH)}  ok={ok_count}  fail={len(fail_syms)}")

    # Write SECTOR_MAP.json
    try:
        import json as _json2
        sector_map_path = DATA_DIR / "SECTOR_MAP.json"
        _json2.dump(STOCK_SECTOR_MAP, open(sector_map_path, "w"), indent=2)
        _log(f"[fetch-job] SECTOR_MAP.json saved")
    except Exception as e:
        _log(f"[fetch-job] SECTOR_MAP.json write failed: {e}")

    _fetch_status.update({
        "running":    False,
        "finishedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ok":         ok_count,
        "failed":     len(fail_syms),
        "failedSyms": fail_syms,
    })
    _log(f"[fetch-job] Done — ok={ok_count}  failed={len(fail_syms)}")


# ── Breeze helpers ────────────────────────────────────────────────────────────

import time as _time

_BREEZE_TOKEN_FILE    = "/tmp/breeze_session.txt"
_BREEZE_TOKEN_TS_FILE = "/tmp/breeze_session_ts.txt"
_BREEZE_TOKEN_MAX_AGE_SEC = 20 * 60 * 60  # 20h — mirrors TradeEdge's client-side freshness window

# ── Kite Connect config ─────────────────────────────────────────────────────
# Access token is re-generated each morning via manual /kite/login tap (no
# credentials stored). Persisted to DATA_DIR/kite_token.json and pushed to the
# GitHub data branch (push_kite_token_to_github(), same pattern as
# gap_settings.json) so a Render restart mid-day doesn't force a re-login —
# restore_data_from_github() pulls it back at startup. The token itself still
# only lasts one trading day either way (Kite expires it nightly), so a fresh
# /kite/login is still needed once per day regardless of restarts.
KITE_API_KEY    = os.environ.get("KITE_API_KEY", "")
KITE_API_SECRET = os.environ.get("KITE_API_SECRET", "")
_KITE_TOKEN_FILE = DATA_DIR / "kite_token.json"

_kite = KiteConnect(api_key=KITE_API_KEY) if (KiteConnect and KITE_API_KEY) else None

# ── Gap Settings sync ────────────────────────────────────────────────────────
# TradeEdge.html POSTs here whenever "Save" is hit on Gap (Overnight) settings
# in the browser (localStorage is the source of truth on the frontend; this
# is a mirror so the headless automation job always reads the same values
# without any hardcoding or manual sync step). Persisted in DATA_DIR so it
# rides along with the existing GitHub data-branch backup/restore.
GAP_SETTINGS_FILE = DATA_DIR / "gap_settings.json"

# Named Overnight Gap presets ({name: paramsObject}), same DATA_DIR pattern as
# GAP_SETTINGS_FILE above. The server is the source of truth here (not just a
# mirror) — the frontend's localStorage copy is a cache, refreshed from here
# on load, so presets saved on desktop show up on mobile and vice versa.
GAP_PRESETS_FILE = DATA_DIR / "gap_presets.json"

# Which presets actually run automated entry/exit ({"enabledPresets": [name,...]}).
# Deliberately a separate file, not a flag baked into each preset's params
# object in gap_presets.json — the frontend's "Save As" on an existing preset
# name fully REPLACES that preset's params object (not a merge), so a flag
# living inside it would get silently wiped the next time someone tweaks and
# re-saves a preset's backtest params. A separate file sidesteps that bug
# class entirely. Opt-in, empty by default — a preset saved just for
# backtesting never starts placing paper trades on its own.
GAP_AUTOMATION_CONFIG_FILE = DATA_DIR / "gap_automation_config.json"

try:
    from breeze_connect import BreezeConnect
    _BREEZE_AVAILABLE = True
except ImportError:
    _BREEZE_AVAILABLE = False

BREEZE_API_KEY    = os.environ.get("BREEZE_API_KEY", "")
BREEZE_API_SECRET = os.environ.get("BREEZE_API_SECRET", "")

_CORP_ACTION_THRESHOLD = 0.30


def _compute_adj_close(closes: list) -> list:
    """Back-adjust close series for corporate actions (splits/bonuses)."""
    if not closes:
        return []
    adj        = closes[:]
    cumulative = 1.0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] == 0:
            continue
        ratio = closes[i - 1] / closes[i]
        if abs(ratio - 1.0) > _CORP_ACTION_THRESHOLD:
            cumulative *= ratio
        adj[i - 1] = closes[i - 1] / cumulative if cumulative else closes[i - 1]
    return adj


def _breeze_iso(dt_str: str, end_of_day: bool = False) -> str:
    """Convert YYYY-MM-DD to Breeze ISO format in UTC."""
    from datetime import datetime, timedelta
    IST_OFFSET = timedelta(hours=5, minutes=30)
    dt = datetime.strptime(dt_str, "%Y-%m-%d")
    dt = dt.replace(hour=23, minute=59, second=59) if end_of_day \
         else dt.replace(hour=0, minute=0, second=0)
    utc = dt - IST_OFFSET
    return utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _do_breeze_fetch_job():
    """
    Background worker: fetch today's OHLC from Breeze for all F&O symbols,
    merge into existing CSVs, push to GitHub data branch.

    Mirrors breeze_fetch.py delta mode — sequential with 0.35s pause,
    uses get_quotes() for today's live candle.
    """
    import json as _json
    global _fetch_status

    _fetch_status["running"]    = True
    _fetch_status["status"]     = "starting"
    _fetch_status["done"]       = False
    _fetch_status["error"]      = None
    _fetch_status["ok"]         = 0
    _fetch_status["failed"]     = 0
    _fetch_status["failedSyms"] = []
    _fetch_status["total"]      = 0
    _fetch_status["cancelled"]  = False

    try:
        # Read stored token
        with open(_BREEZE_TOKEN_FILE) as f:
            token = f.read().strip()
        if not token:
            raise ValueError("Breeze token file is empty")

        if not _BREEZE_AVAILABLE:
            raise ImportError("breeze-connect not installed")
        if not BREEZE_API_KEY or not BREEZE_API_SECRET:
            raise ValueError("BREEZE_API_KEY / BREEZE_API_SECRET not set")

        # Connect to Breeze
        _fetch_status["status"] = "connecting to Breeze"
        breeze = BreezeConnect(api_key=BREEZE_API_KEY)
        breeze.generate_session(api_secret=BREEZE_API_SECRET, session_token=token)

        from datetime import datetime, timedelta
        IST_OFFSET = timedelta(hours=5, minutes=30)
        now_ist    = datetime.utcnow() + IST_OFFSET
        today_str  = now_ist.strftime("%Y-%m-%d")

        # Fetch last 7 calendar days to ensure we get all recent trading days
        from_dt   = now_ist - timedelta(days=7)
        from_str  = from_dt.strftime("%Y-%m-%d")
        from_iso  = _breeze_iso(from_str, end_of_day=False)
        to_iso    = _breeze_iso(today_str, end_of_day=True)

        # Breeze ShortName mapping — verified against Breeze_Symbols.xlsx (ExchangeCode → ShortName)
        # 25 symbols have same name in Breeze (no override needed):
        # ABB, BHEL, BIOCON, BSE, CDSL, CIPLA, COLPAL, CONCOR, GAIL, GRASIM,
        # ITC, JIOFIN, LUPIN, MARUTI, MCX, NBCC, NHPC, NTPC, ONGC, PIIND,
        # SAIL, SRF, TCS, TRENT, WIPRO
        # 6 symbols not in Breeze file (will fail gracefully):
        # HUDCO, ICICIPRULIFE, LTIM, TATATECH, TORNTPOWER, ZOMATO
        BREEZE_OVERRIDES = {
            'ABCAPITAL':  ('ADICAP', 'NSE'),
            'ADANIENSOL': ('ADATRA', 'NSE'),
            'ADANIENT':   ('ADAENT', 'NSE'),
            'ADANIGREEN': ('ADAGRE', 'NSE'),
            'ADANIPORTS': ('ADAPOR', 'NSE'),
            'ADANIPOWER': ('ADAPOW', 'NSE'),
            'ALKEM':      ('ALKLAB', 'NSE'),
            'AMBER':      ('AMBEN',  'NSE'),
            'AMBUJACEM':  ('AMBCE',  'NSE'),
            'ANGELONE':   ('ANGBRO', 'NSE'),
            'APLAPOLLO':  ('APLAPO', 'NSE'),
            'APOLLOHOSP': ('APOHOS', 'NSE'),
            'ASHOKLEY':   ('ASHLEY', 'NSE'),
            'ASIANPAINT': ('ASIPAI', 'NSE'),
            'ASTRAL':     ('ASTPOL', 'NSE'),
            'AUBANK':     ('AUSMA',  'NSE'),
            'AUROPHARMA': ('AURPHA', 'NSE'),
            'AXISBANK':   ('AXIBAN', 'NSE'),
            'BAJAJ-AUTO': ('BAAUTO', 'NSE'),
            'BAJAJFINSV': ('BAFINS', 'NSE'),
            'BAJFINANCE': ('BAJFI',  'NSE'),
            'BANDHANBNK': ('BANBAN', 'NSE'),
            'BANKBARODA': ('BANBAR', 'NSE'),
            'BDL':        ('BHADYN', 'NSE'),
            'BEL':        ('BHAELE', 'NSE'),
            'BHARATFORG': ('BHAFOR', 'NSE'),
            'BHARTIARTL': ('BHAAIR', 'NSE'),
            'BLUESTARCO': ('BLUSTA', 'NSE'),
            'BOSCHLTD':   ('BOSLIM', 'NSE'),
            'BPCL':       ('BHAPET', 'NSE'),
            'BRITANNIA':  ('BRIIND', 'NSE'),
            'CAMS':       ('COMAGE', 'NSE'),
            'CANBK':      ('CANBAN', 'NSE'),
            'CGPOWER':    ('CROGRE', 'NSE'),
            'CHOLAFIN':   ('CHOINV', 'NSE'),
            'COALINDIA':  ('COALIN', 'NSE'),
            'COCHINSHIP': ('COCSHI', 'NSE'),
            'COFORGE':    ('NIITEC', 'NSE'),
            'CROMPTON':   ('CROGR',  'NSE'),
            'CUMMINSIND': ('CUMIND', 'NSE'),
            'DABUR':      ('DABIND', 'NSE'),
            'DALBHARAT':  ('ODICEM', 'NSE'),
            'DELHIVERY':  ('DELLIM', 'NSE'),
            'DIVISLAB':   ('DIVLAB', 'NSE'),
            'DIXON':      ('DIXTEC', 'NSE'),
            'DLF':        ('DLFLIM', 'NSE'),
            'DMART':      ('AVESUP', 'NSE'),
            'DRREDDY':    ('DRREDD', 'NSE'),
            'GLENMARK':   ('GLEPHA', 'NSE'),
            'GMRAIRPORT': ('GMRINF', 'NSE'),
            'GODREJCP':   ('GODCON', 'NSE'),
            'GODREJPROP': ('GODPRO', 'NSE'),
            'HAL':        ('HINAER', 'NSE'),
            'HAVELLS':    ('HAVIND', 'NSE'),
            'HCLTECH':    ('HCLTEC', 'NSE'),
            'HDFCAMC':    ('HDFAMC', 'NSE'),
            'HDFCBANK':   ('HDFBAN', 'NSE'),
            'HDFCLIFE':   ('HDFSTA', 'NSE'),
            'HEROMOTOCO': ('HERHON', 'NSE'),
            'HINDALCO':   ('HINDAL', 'NSE'),
            'HINDPETRO':  ('HINPET', 'NSE'),
            'HINDUNILVR': ('HINLEV', 'NSE'),
            'HINDZINC':   ('HINZIN', 'NSE'),
            'HYUNDAI':    ('HYUMOT', 'NSE'),
            'ICICIBANK':  ('ICIBAN', 'NSE'),
            'ICICIGI':    ('ICILOM', 'NSE'),
            'IDEA':       ('IDECEL', 'NSE'),
            'IDFCFIRSTB': ('IDFBAN', 'NSE'),
            'IEX':        ('INDEN',  'NSE'),
            'INDHOTEL':   ('INDHOT', 'NSE'),
            'INDIANB':    ('INDIBA', 'NSE'),
            'INDIGO':     ('INTAVI', 'NSE'),
            'INDUSINDBK': ('INDBA',  'NSE'),
            'INDUSTOWER': ('BHAINF', 'NSE'),
            'INFY':       ('INFTEC', 'NSE'),
            'INOXWIND':   ('INOWIN', 'NSE'),
            'IOC':        ('INDOIL', 'NSE'),
            'IREDA':      ('INDREN', 'NSE'),
            'IRFC':       ('INDR',   'NSE'),
            'JINDALSTEL': ('JINSP',  'NSE'),
            'JSWENERGY':  ('JSWENE', 'NSE'),
            'JSWSTEEL':   ('JSWSTE', 'NSE'),
            'JUBLFOOD':   ('JUBFOO', 'NSE'),
            'KALYANKJIL': ('KALJEW', 'NSE'),
            'KAYNES':     ('KAYTEC', 'NSE'),
            'KEI':        ('KEIIND', 'NSE'),
            'KFINTECH':   ('KFITEC', 'NSE'),
            'KOTAKBANK':  ('KOTMAH', 'NSE'),
            'KPITTECH':   ('KPITE',  'NSE'),
            'LAURUSLABS': ('LAULAB', 'NSE'),
            'LICHSGFIN':  ('LICHF',  'NSE'),
            'LICI':       ('LIC',    'NSE'),
            'LODHA':      ('MACDEV', 'NSE'),
            'LT':         ('LARTOU', 'NSE'),
            'LTF':        ('LTFINA', 'NSE'),
            'M&M':        ('MAHMAH', 'NSE'),
            'MANAPPURAM': ('MANAFI', 'NSE'),
            'MANKIND':    ('MAPHA',  'NSE'),
            'MARICO':     ('MARLIM', 'NSE'),
            'MAXHEALTH':  ('MAXHEA', 'NSE'),
            'MAZDOCK':    ('MAZDOC', 'NSE'),
            'MFSL':       ('MAXFIN', 'NSE'),
            'MOTHERSON':  ('MOTSUM', 'NSE'),
            'MOTILALOFS': ('MOTOSW', 'NSE'),
            'MPHASIS':    ('MPHLIM', 'NSE'),
            'MUTHOOTFIN': ('MUTFIN', 'NSE'),
            'NAM-INDIA':  ('RELNIP', 'NSE'),
            'NATIONALUM': ('NATALU', 'NSE'),
            'NAUKRI':     ('INFEDG', 'NSE'),
            'NESTLEIND':  ('NESIND', 'NSE'),
            'NMDC':       ('NATMIN', 'NSE'),
            'NUVAMA':     ('NUVWEA', 'NSE'),
            'NYKAA':      ('FSNECO', 'NSE'),
            'OBEROIRLTY': ('OBEREA', 'NSE'),
            'OFSS':       ('ORAFIN', 'NSE'),
            'OIL':        ('OILIND', 'NSE'),
            'PAGEIND':    ('PAGIND', 'NSE'),
            'PATANJALI':  ('RUCSOY', 'NSE'),
            'PAYTM':      ('ONE97',  'NSE'),
            'PERSISTENT': ('PERSYS', 'NSE'),
            'PFC':        ('POWFIN', 'NSE'),
            'PGEL':       ('PGELEC', 'NSE'),
            'PHOENIXLTD': ('PHOMIL', 'NSE'),
            'PIDILITIND': ('PIDIND', 'NSE'),
            'PNB':        ('PUNBAN', 'NSE'),
            'PNBHOUSING': ('PNBHOU', 'NSE'),
            'POLICYBZR':  ('PBFINT', 'NSE'),
            'POLYCAB':    ('POLI',   'NSE'),
            'POWERGRID':  ('POWGRI', 'NSE'),
            'POWERINDIA': ('ABBPOW', 'NSE'),
            'PREMIERENE': ('PREENR', 'NSE'),
            'PRESTIGE':   ('PREEST', 'NSE'),
            'RBLBANK':    ('RBLBAN', 'NSE'),
            'RECLTD':     ('RURELE', 'NSE'),
            'RELIANCE':   ('RELIND', 'NSE'),
            'RVNL':       ('RAIVIK', 'NSE'),
            'SAMMAANCAP': ('INDHO',  'NSE'),
            'SBICARD':    ('SBICAR', 'NSE'),
            'SBILIFE':    ('SBILIF', 'NSE'),
            'SBIN':       ('STABAN', 'NSE'),
            'SHREECEM':   ('SHRCEM', 'NSE'),
            'SHRIRAMFIN': ('SHRTRA', 'NSE'),
            'SIEMENS':    ('SIEMEN', 'NSE'),
            'SOLARINDS':  ('SOLIN',  'NSE'),
            'SONACOMS':   ('SONBLW', 'NSE'),
            'SUNPHARMA':  ('SUNPHA', 'NSE'),
            'SUPREMEIND': ('SUPIND', 'NSE'),
            'SUZLON':     ('SUZENE', 'NSE'),
            'SWIGGY':     ('SWILIM', 'NSE'),
            'TATACONSUM': ('TATGLO', 'NSE'),
            'TATAELXSI':  ('TATELX', 'NSE'),
            'TATAPOWER':  ('TATPOW', 'NSE'),
            'TATASTEEL':  ('TATSTE', 'NSE'),
            'TECHM':      ('TECMAH', 'NSE'),
            'TIINDIA':    ('TUBIN',  'NSE'),
            'TITAN':      ('TITIND', 'NSE'),
            'TMPV':       ('TATMOT', 'NSE'),
            'TORNTPHARM': ('TORPHA', 'NSE'),
            'TVSMOTOR':   ('TVSMOT', 'NSE'),
            'ULTRACEMCO': ('ULTCEM', 'NSE'),
            'UNIONBANK':  ('UNIBAN', 'NSE'),
            'UNITDSPR':   ('UNISPI', 'NSE'),
            'UNOMINDA':   ('MININD', 'NSE'),
            'UPL':        ('UNIP',   'NSE'),
            'VBL':        ('VARBEV', 'NSE'),
            'VEDL':       ('VEDLIM', 'NSE'),
            'VMM':        ('VISMEG', 'NSE'),
            'ZYDUSLIFE':  ('CADHEA', 'NSE'),
        }
        # Skip indices — Breeze daily cash data not available
        INDEX_PREFIXES = ('NIFTY50', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY',
                          'CNXIT', 'CNXAUTO', 'CNXPHARMA', 'CNXENERGY',
                          'CNXMETAL', 'CNXFMCG', 'CNXINFRA', 'CNXCONSUM')

        # Skip equities verified as not in Breeze universe
        NOT_IN_BREEZE = {
            'AARTIIND','ABFRL','ACC','APOLLOTYRE','BALKRISIND','BERGEPAINT',
            'BIRLASOFT','CESC','CHAMBLFERT','COROMANDEL','DEEPAKNITR','EMAMILTD',
            'GLAND','HUDCO','ICICIPRULIFE','IGL','IIFL','INDIAMART','IPCALAB',
            'IRB','LALPATHLAB','LTIM','LTTS','M&MFIN','MCDOWELL-N','MGL','MRF',
            'PVRINOX','RPOWER','SJVN','STAR','TATACHEM','TATACOMM','TATAMOTORS',
            'TORNTPOWER','UBL','WHIRLPOOL','ZOMATO',
        }

        symbols = [s for s in ALL_SYMBOLS
                   if not any(s == idx for idx in INDEX_PREFIXES)
                   and s not in NOT_IN_BREEZE]
        total      = len(symbols)
        fetched      = 0
        fetched_syms = []   # track successfully fetched symbol names for GitHub push
        failed       = []
        _PAUSE     = 0.35   # same as breeze_fetch.py

        _fetch_status["total"]  = total
        _fetch_status["status"] = f"fetching Breeze data for {total} symbols"

        for i, sym in enumerate(symbols):
            # Check if cancelled by user
            if _fetch_status.get("cancelled"):
                print(f"[breeze/fetch] Cancelled at {i}/{total}")
                _fetch_status["status"] = f"cancelled — {fetched} symbols fetched"
                break

            _fetch_status["pct"]    = f"{i}/{total}"
            _fetch_status["status"] = f"Breeze: {sym} ({i+1}/{total})"

            stock_code, exchange_code = BREEZE_OVERRIDES.get(sym, (sym, 'NSE'))
            product_type = 'cash'  # always cash for historical daily data

            try:
                resp = breeze.get_historical_data_v2(
                    interval="1day",
                    from_date=from_iso,
                    to_date=to_iso,
                    stock_code=stock_code,
                    exchange_code=exchange_code,
                    product_type=product_type,
                )

                rows = []
                if resp and resp.get("Status") == 200:
                    rows = resp.get("Success") or []

                # For equities, also try get_quotes() for today's live candle
                if exchange_code != 'NFO':
                    try:
                        quote_resp = breeze.get_quotes(
                            stock_code=stock_code,
                            exchange_code=exchange_code,
                            product_type="cash",
                        )
                        if isinstance(quote_resp, dict) and not quote_resp.get("Error"):
                            success = quote_resp.get("Success") or []
                            if success:
                                qrow = next(
                                    (r for r in success
                                     if isinstance(r, dict)
                                     and str(r.get("exchange_code","")).upper() == exchange_code),
                                    success[0] if success else None
                                )
                                if qrow:
                                    open_v  = float(qrow.get("open") or 0)
                                    ltp     = float(qrow.get("ltp") or qrow.get("close") or 0)
                                    high_v  = float(qrow.get("high") or 0)
                                    low_v   = float(qrow.get("low")  or 0)
                                    vol_v   = float(qrow.get("total_quantity_traded") or 0)
                                    if open_v > 0 and ltp > 0:
                                        # Build today's candle in Breeze row format
                                        today_row = {
                                            "datetime":     today_str + " 00:00:00",
                                            "open":         open_v,
                                            "high":         high_v,
                                            "low":          low_v,
                                            "close":        ltp,
                                            "volume":       vol_v,
                                            "stock_code":   stock_code,
                                            "exchange_code": exchange_code,
                                        }
                                        # Remove any existing today row, add live one
                                        rows = [r for r in rows
                                                if str(r.get("datetime",""))[:10] != today_str]
                                        rows.append(today_row)
                    except Exception:
                        pass  # quotes optional — historical data is enough

                if not rows:
                    failed.append(sym)
                    _fetch_status["failed"]     = len(failed)
                    _fetch_status["failedSyms"] = failed[-20:]
                    _time.sleep(_PAUSE)
                    continue

                # Parse to TradeEdge CSV format: date,open,high,low,close,adj_close
                parsed = []
                for row in rows:
                    try:
                        raw_dt   = row.get("datetime") or row.get("date") or ""
                        date_str = str(raw_dt)[:10]
                        if not date_str or date_str == "None":
                            continue
                        o = float(row.get("open")  or 0)
                        h = float(row.get("high")  or 0)
                        l = float(row.get("low")   or 0)
                        c = float(row.get("close") or 0)
                        v = float(row.get("volume") or 0)
                        if o and h and l and c:
                            parsed.append({"date": date_str, "open": o, "high": h,
                                           "low": l, "close": c, "volume": v})
                    except (ValueError, TypeError):
                        continue

                if not parsed:
                    failed.append(sym)
                    _fetch_status["failed"]     = len(failed)
                    _fetch_status["failedSyms"] = failed[-20:]
                    _time.sleep(_PAUSE)
                    continue

                parsed.sort(key=lambda r: r["date"])

                # Back-adjust for corporate actions
                closes     = [r["close"] for r in parsed]
                adj_closes = _compute_adj_close(closes)
                for j, row in enumerate(parsed):
                    row["adj_close"] = round(adj_closes[j], 4)

                # Merge with existing CSV — skip flat candles (bad Breeze data)
                # A flat candle has open==high==low==close and volume==0
                # This happens on market holidays when Breeze returns previous close
                csv_path = DATA_DIR / f"{sym}.csv"
                if csv_path.exists():
                    import csv
                    with open(csv_path, newline='') as cf:
                        reader = csv.DictReader(cf)
                        existing = list(reader)

                    # Remove any existing flat candles (holiday artifacts) from CSV
                    def _is_flat_row(r):
                        try:
                            o,h,l,c = float(r.get("open",0)), float(r.get("high",0)), \
                                      float(r.get("low",0)),  float(r.get("close",0))
                            v = float(r.get("volume",1) or 1)
                            return o == h == l == c and v == 0
                        except: return False
                    existing = [r for r in existing if not _is_flat_row(r)]

                    existing_dates = {r.get("date","") for r in existing}
                    for row in parsed:
                        # Skip flat candles from Breeze — market holidays
                        is_flat = (row["open"] == row["high"] == row["low"] == row["close"]
                                   and row.get("volume", 1) == 0)
                        if is_flat:
                            print(f"[breeze/fetch] {sym} {row['date']} flat candle skipped")
                            continue
                        # Only write today's candle or genuinely new dates
                        # Do NOT overwrite existing historical dates — Yahoo data is authoritative
                        if row["date"] == today_str or row["date"] not in existing_dates:
                            existing = [r for r in existing if r.get("date") != row["date"]]
                            existing.append({
                                "date":      row["date"],
                                "open":      row["open"],
                                "high":      row["high"],
                                "low":       row["low"],
                                "close":     row["close"],
                                "adj_close": row["adj_close"],
                            })
                    existing.sort(key=lambda r: r.get("date",""))
                    _FIELDS = ["date","open","high","low","close","adj_close"]
                    with open(csv_path, "w", newline='') as cf:
                        writer = csv.DictWriter(cf, fieldnames=_FIELDS, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(existing)
                else:
                    # No existing CSV — write fresh
                    import csv
                    _FIELDS = ["date","open","high","low","close","adj_close"]
                    with open(csv_path, "w", newline='') as cf:
                        writer = csv.DictWriter(cf, fieldnames=_FIELDS, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows([{
                            "date": r["date"], "open": r["open"], "high": r["high"],
                            "low": r["low"], "close": r["close"], "adj_close": r["adj_close"],
                        } for r in parsed])

                fetched += 1
                fetched_syms.append(sym)
                _fetch_status["ok"] = fetched

            except Exception as e:
                print(f"[breeze/fetch] {sym} error: {e}")
                failed.append(sym)
                _fetch_status["failed"]     = len(failed)
                _fetch_status["failedSyms"] = failed[-20:]  # keep last 20 failed syms

            _time.sleep(_PAUSE)

        print(f"[breeze/fetch] Done: {fetched}/{total} · {len(failed)} failed")
        _fetch_status["status"] = "pushing CSVs to GitHub data branch…"

        # Push updated CSVs directly to GitHub data branch via API
        try:
            import urllib.request, urllib.error, json as _json2, base64 as _b64

            gh_token = os.environ.get("GITHUB_TOKEN", "")
            repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
            branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")

            if not gh_token:
                print("[breeze/fetch] GITHUB_TOKEN not set — skipping push")
            else:
                pushed  = 0
                headers = {
                    "Authorization": f"token {gh_token}",
                    "Accept":        "application/vnd.github.v3+json",
                    "User-Agent":    "TradeEdge-App",
                    "Content-Type":  "application/json",
                }

                # Push all successfully fetched symbols
                for sym in fetched_syms:
                    csv_path = DATA_DIR / f"{sym}.csv"
                    if not csv_path.exists():
                        continue
                    try:
                        file_bytes  = csv_path.read_bytes()
                        content_b64 = _b64.b64encode(file_bytes).decode()

                        # Get current file SHA (required for update)
                        file_url = f"https://api.github.com/repos/{repo}/contents/{sym}.csv"
                        sha = None
                        try:
                            get_req = urllib.request.Request(
                                file_url + f"?ref={branch}", headers=headers)
                            meta = _json2.loads(urllib.request.urlopen(get_req, timeout=10).read())
                            sha  = meta.get("sha")
                        except urllib.error.HTTPError as e:
                            if e.code != 404:
                                raise

                        body = {
                            "message": f"Breeze update {today_str}: {sym}",
                            "content": content_b64,
                            "branch":  branch,
                        }
                        if sha:
                            body["sha"] = sha

                        put_req = urllib.request.Request(
                            file_url,
                            data=_json2.dumps(body).encode(),
                            headers=headers,
                            method="PUT",
                        )
                        urllib.request.urlopen(put_req, timeout=15)
                        pushed += 1
                        _fetch_status["status"] = f"GitHub push: {pushed}/{len(fetched_syms)}"

                    except Exception as e:
                        print(f"[breeze/fetch] GitHub push failed for {sym}: {e}")

                print(f"[breeze/fetch] GitHub push: {pushed}/{len(fetched_syms)} files pushed")

        except Exception as e:
            print(f"[breeze/fetch] GitHub push error: {e}")


        _fetch_status["status"]  = f"done — {fetched}/{total} symbols updated"
        _fetch_status["done"]    = True

    except Exception as e:
        print(f"[breeze/fetch] Fatal error: {e}")
        _fetch_status["error"]  = str(e)
        _fetch_status["status"] = f"error: {e}"
    finally:
        _fetch_status["running"] = False


@app.route("/run-fetch", methods=["GET", "POST"])
def run_fetch():
    """
    Trigger a fetch job.
    mode=yahoo  (default) — daily incremental Yahoo Finance fetch
    mode=breeze           — today's OHLC from Breeze API using stored token

    Call from cron-job.org at 15:20 IST (09:50 UTC) Mon–Fri for Yahoo.
    Call from TradeEdge 📡 button for Breeze (on-demand, any time).
    Protected by FETCH_SECRET env var — pass as ?secret=xxx.
    """
    secret = os.environ.get("FETCH_SECRET", "")
    if secret and request.args.get("secret", "") != secret:
        return cors_response({"error": "Unauthorized"}, 401)

    mode = (request.args.get("mode") or "yahoo").strip().lower()

    if _fetch_status["running"]:
        return cors_response({
            "job_started": False,
            "reason":      "already running",
            "status":      _fetch_status,
        })

    if mode == "breeze":
        # Check token is stored
        try:
            with open(_BREEZE_TOKEN_FILE) as f:
                token = f.read().strip()
            if not token:
                return cors_response({"error": "No Breeze token stored. Tap 🔑 in TradeEdge first."}, 400)
        except FileNotFoundError:
            return cors_response({"error": "No Breeze token stored. Tap 🔑 in TradeEdge first."}, 400)

        # Check token freshness — a cron-fired fetch should fail fast and clearly
        # rather than silently erroring per-symbol against Breeze with a stale token.
        try:
            with open(_BREEZE_TOKEN_TS_FILE) as f:
                token_ts = float(f.read().strip())
            age_sec = time.time() - token_ts
        except (FileNotFoundError, ValueError):
            age_sec = None  # token predates this change / no timestamp — allow, don't block

        if age_sec is not None and age_sec > _BREEZE_TOKEN_MAX_AGE_SEC:
            age_hr = round(age_sec / 3600, 1)
            return cors_response({
                "error": f"Breeze token is stale ({age_hr}h old, max {_BREEZE_TOKEN_MAX_AGE_SEC // 3600}h). "
                         f"Tap 🔑 in TradeEdge to refresh today's token."
            }, 400)

        t = threading.Thread(target=_do_breeze_fetch_job, daemon=True)
        t.start()
        return cors_response({
            "ok":      True,
            "status":  "started",
            "message": "Breeze fetch started. Poll /fetch-status for progress.",
        })
    else:
        t = threading.Thread(target=_do_fetch_job, daemon=True)
        t.start()
        return cors_response({
            "job_started": True,
            "symbols":     len(ALL_SYMBOLS),
            "dataDir":     str(DATA_DIR),
            "message":     "Yahoo fetch started in background. Poll /fetch-status for progress.",
        })


@app.route("/fetch-status")
def fetch_status():
    """Poll this to monitor the background fetch job started by /run-fetch."""
    return cors_response(_fetch_status)


@app.route("/restore-status")
def restore_status():
    """Check how many CSVs are currently on disk — proxy for restore progress."""
    csvs = list(DATA_DIR.glob("*.csv"))
    has_sector_map = (DATA_DIR / "SECTOR_MAP.json").exists()
    return cors_response({
        "csvCount":     len(csvs),
        "hasSectorMap": has_sector_map,
        "dataDir":      str(DATA_DIR),
        "symbols":      sorted(f.stem for f in csvs),
    })


# ── Pull job state — tracks background pull from GitHub ───────────────────────
_pull_status = {
    "running":    False,
    "startedAt":  None,
    "finishedAt": None,
    "downloaded": 0,
    "skipped":    0,
    "failed":     0,
    "log":        [],
}

def _do_pull_from_github():
    """
    Background worker: pulls ALL files from GitHub data branch into DATA_DIR.
    Unlike restore_data_from_github() which skips existing files,
    this OVERWRITES existing files so Render always gets the latest pushed CSVs.
    """
    import urllib.request, urllib.error, json as _json
    global _pull_status

    token  = os.environ.get("GITHUB_TOKEN", "")
    repo   = os.environ.get("GITHUB_REPO",        "crorepathi369/tradeedge-api")
    branch = os.environ.get("GITHUB_DATA_BRANCH", "data")

    _pull_status.update({
        "running": True, "startedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finishedAt": None, "downloaded": 0, "skipped": 0, "failed": 0, "log": [],
    })

    def _log(msg):
        print(msg)
        _pull_status["log"].append(msg)
        if len(_pull_status["log"]) > 100:
            _pull_status["log"] = _pull_status["log"][-100:]

    if not token:
        _log("[pull] GITHUB_TOKEN not set — cannot pull")
        _pull_status["running"] = False
        return

    _log(f"[pull] Fetching file tree from {repo}@{branch}")

    # Step 1: get file tree
    tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
    req = urllib.request.Request(tree_url, headers={
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
    })
    try:
        tree = _json.loads(urllib.request.urlopen(req, timeout=30).read())
    except Exception as e:
        _log(f"[pull] Tree fetch failed: {e}")
        _pull_status["running"] = False
        return

    files = [
        f["path"] for f in tree.get("tree", [])
        if f["type"] == "blob" and (
            f["path"].endswith(".csv") or f["path"] in ("SECTOR_MAP.json", "gap_positions.json", "gap_presets.json", "gap_settings.json", "gap_automation_config.json", "kite_token.json")
        )
    ]
    _log(f"[pull] {len(files)} files found — downloading all (overwrite mode)")

    downloaded = skipped = failed = 0

    for filename in files:
        dest    = DATA_DIR / filename
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{filename}"
        raw_req = urllib.request.Request(raw_url, headers={
            "Authorization": f"token {token}",
            "User-Agent":    "TradeEdge-App",
        })
        try:
            data = urllib.request.urlopen(raw_req, timeout=30).read()
            dest.write_bytes(data)
            downloaded += 1
        except Exception as e:
            _log(f"[pull] ✗ {filename}: {e}")
            failed += 1

        # Update counts live so /pull-status shows progress
        _pull_status["downloaded"] = downloaded
        _pull_status["failed"]     = failed

    _pull_status.update({
        "running":    False,
        "finishedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "downloaded": downloaded,
        "skipped":    skipped,
        "failed":     failed,
    })
    _log(f"[pull] Done — downloaded={downloaded}  failed={failed}")


@app.route("/pull-from-github")
def pull_from_github():
    """
    Trigger an immediate pull of all CSVs from GitHub data branch → Render disk.
    Call this after running push_incr_data.py locally to make Render serve
    the fresh data right away without waiting for a restart.

    Protected by FETCH_SECRET env var (same secret as /run-fetch).
    Poll /pull-status for progress.
    """
    secret = os.environ.get("FETCH_SECRET", "")
    if secret and request.args.get("secret", "") != secret:
        return cors_response({"error": "Unauthorized"}, 401)

    if _pull_status["running"]:
        return cors_response({
            "job_started": False,
            "reason":      "pull already running",
            "status":      _pull_status,
        })

    threading.Thread(target=_do_pull_from_github, daemon=True).start()
    return cors_response({
        "job_started": True,
        "message":     "Pull started in background. Poll /pull-status for progress.",
        "dataDir":     str(DATA_DIR),
    })


@app.route("/pull-status")
def pull_status():
    """Poll this to monitor the background pull started by /pull-from-github."""
    return cors_response(_pull_status)


@app.route("/data/manifest")
def data_manifest():
    """
    Return list of available CSV symbols + last-modified timestamps.
    Used by TradeEdge.html loadFromCloud() to discover available symbols.
    """
    files = {}
    for f in sorted(DATA_DIR.glob("*.csv")):
        st = f.stat()
        files[f.stem.upper()] = {"mtime": round(st.st_mtime), "size": st.st_size}
    sector_map = DATA_DIR / "SECTOR_MAP.json"
    return cors_response({
        "status":       "ok",
        "count":        len(files),
        "symbols":      files,
        "hasSectorMap": sector_map.exists(),
        "asOf":         datetime.now().strftime("%Y-%m-%d %H:%M"),
    })


@app.route("/data/<path:filename>")
def serve_data_file(filename):
    """
    Serve a single CSV or SECTOR_MAP.json from DATA_DIR.
    Cache-Control: no-cache so TradeEdge.html always gets the latest data.
    """
    lower = filename.lower()
    if not (lower.endswith(".csv") or lower == "sector_map.json"):
        return cors_response({"error": "Only .csv and SECTOR_MAP.json files are served"}, 403)

    file_path = DATA_DIR / filename
    if not file_path.exists():
        return cors_response({"error": f"{filename} not found — run /run-fetch first"}, 404)

    try:
        file_path.resolve().relative_to(DATA_DIR.resolve())
    except ValueError:
        return cors_response({"error": "Invalid path"}, 400)

    mime = "application/json" if lower.endswith(".json") else "text/csv"
    resp = send_from_directory(DATA_DIR.resolve(), filename, mimetype=mime)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/debug/csv", methods=["GET"])
def debug_csv():
    """
    Returns latest row + file stats for any symbol.
    Usage: /debug/csv?sym=RELIANCE
    Returns last 3 rows so you can verify today's date is present.
    """
    sym      = request.args.get("sym", "RELIANCE").upper().strip()
    csv_path = DATA_DIR / f"{sym}.csv"
    if not csv_path.exists():
        return cors_response({"error": f"{sym}.csv not found in DATA_DIR"}, 404)
    import csv as _csv, os as _os
    stat     = _os.stat(csv_path)
    modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    with open(csv_path, newline="") as f:
        rows = list(_csv.DictReader(f))
    last3 = rows[-5:] if len(rows) >= 5 else rows
    return cors_response({
        "symbol":   sym,
        "rows":     len(rows),
        "modified": modified,
        "last5":    last3,
        "dataDir":  str(DATA_DIR),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# ── /futures endpoint ─────────────────────────────────────────────────────────
#
# GET /futures?symbols=LTIM,INFY,TCS
#
# Returns per symbol: spot, nearFut, farFut, closes[], error
# Uses same Ticker.history() + rate-limit pattern as /sync-today

def fetch_latest_price(ticker_str):
    """Return most recent close for a ticker, or None on any error."""
    for attempt in range(2):
        try:
            tk  = yf.Ticker(ticker_str)
            df  = tk.history(period="5d", interval="1d",
                             auto_adjust=False, actions=False)
            if df is not None and not df.empty:
                cols = [str(c).lower().replace(" ", "_") for c in df.columns]
                df.columns = cols
                if "close" in df.columns:
                    closes = df["close"].dropna()
                    if len(closes):
                        return round(float(closes.iloc[-1]), 2)
            return None
        except Exception as e:
            if is_rate_limit(e):
                time.sleep(RATE_LIMIT_WAIT)
            else:
                return None
    return None

def fetch_close_series(ticker_str, days=60):
    """Return list of daily closes for last N days, or [] on error."""
    try:
        end   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        tk    = yf.Ticker(ticker_str)
        df    = tk.history(start=start, end=end, interval="1d",
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

@app.route("/futures", methods=["GET", "OPTIONS"])
def futures_endpoint():
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    raw = request.args.get("symbols", "").strip()
    if not raw:
        return cors_response({"error": "symbols param required"}, 400)

    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if len(symbols) > 60:
        return cors_response({"error": "max 60 symbols per request"}, 400)

    t0, result, failed = time.time(), {}, []

    for i, sym in enumerate(symbols):
        if time.time() - t0 > MAX_BATCH_SECS:
            failed.extend(symbols[i:])
            print(f"[futures] Safety cutoff — skipping {len(symbols[i:])} remaining")
            break

        yf_sym  = get_yf_ticker(sym)
        near_tk = f"{sym}-I.NS"
        far_tk  = f"{sym}-II.NS"

        try:
            closes   = fetch_close_series(yf_sym, days=60)
            spot     = closes[-1] if closes else fetch_latest_price(yf_sym)
            if not spot:
                failed.append(sym)
                result[sym] = {"spot": None, "nearFut": None, "farFut": None,
                               "closes": [], "error": "No spot data"}
                continue

            time.sleep(0.4)
            near_fut = fetch_latest_price(near_tk)
            time.sleep(0.4)
            far_fut  = fetch_latest_price(far_tk)

            result[sym] = {"spot": spot, "nearFut": near_fut,
                           "farFut": far_fut, "closes": closes, "error": None}
            print(f"[futures] {sym}: spot={spot} near={near_fut} far={far_fut}")

        except Exception as e:
            failed.append(sym)
            result[sym] = {"spot": None, "nearFut": None, "farFut": None,
                           "closes": [], "error": str(e)}
            print(f"[futures] {sym} error: {type(e).__name__}: {e}")

        if i < len(symbols) - 1:
            time.sleep(INTER_SYMBOL)

    return cors_response({
        "status": "ok", "data": result, "failed": failed,
        "elapsed": round(time.time() - t0, 1),
        "asOf": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(symbols),
    })


# ── /breeze/set-token ────────────────────────────────────────────────────────
# Receives the daily Breeze session token from the TradeEdge 📡 button.
# Stores it server-side so _do_breeze_fetch_job can use it.


@app.route("/breeze/set-token", methods=["POST", "OPTIONS"])
def breeze_set_token():
    """
    Store the daily Breeze session token server-side.
    Called from TradeEdge 🔑 modal on Save.
    Token is then used by /run-fetch?mode=breeze.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    body  = request.get_json(force=True) or {}
    token = (body.get("token") or "").strip()

    if not token:
        return cors_response({"error": "missing_token"}, 400)

    try:
        with open(_BREEZE_TOKEN_FILE, "w") as f:
            f.write(token)
        with open(_BREEZE_TOKEN_TS_FILE, "w") as f:
            f.write(str(time.time()))
        print(f"[breeze/set-token] Token stored ({len(token)} chars)")
        return cors_response({"ok": True, "msg": "Token stored"})
    except Exception as e:
        return cors_response({"error": str(e)}, 500)


@app.route("/breeze/token-status", methods=["GET", "OPTIONS"])
def breeze_token_status():
    """Check if a Breeze token is currently stored server-side."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    try:
        with open(_BREEZE_TOKEN_FILE) as f:
            token = f.read().strip()
        age_sec = None
        try:
            with open(_BREEZE_TOKEN_TS_FILE) as f:
                age_sec = time.time() - float(f.read().strip())
        except (FileNotFoundError, ValueError):
            pass
        return cors_response({
            "hasToken": bool(token),
            "length":   len(token),
            "ageHours": round(age_sec / 3600, 2) if age_sec is not None else None,
            "stale":    (age_sec is not None and age_sec > _BREEZE_TOKEN_MAX_AGE_SEC),
        })
    except FileNotFoundError:
        return cors_response({"hasToken": False})


@app.route("/breeze/cancel", methods=["POST", "OPTIONS"])
def breeze_cancel():
    """Signal the running Breeze fetch job to stop after current symbol."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    if _fetch_status.get("running"):
        _fetch_status["cancelled"] = True
        return cors_response({"ok": True, "msg": "Cancel signal sent"})
    return cors_response({"ok": False, "msg": "No fetch running"})


# ── /kite/* — Gap strategy order placement auth ────────────────────────────
# Daily flow: Raja visits /kite/login each morning -> Kite login page ->
# redirected to /kite/callback with a request_token -> exchanged here for
# an access_token, stored in /tmp for the rest of the day.

from flask import redirect as _redirect


@app.route("/kite/login", methods=["GET"])
def kite_login():
    if _kite is None:
        return cors_response(
            {"error": "KITE_API_KEY not configured on server"}, 500
        )
    return _redirect(_kite.login_url())


@app.route("/kite/callback", methods=["GET"])
def kite_callback():
    if _kite is None:
        return cors_response(
            {"error": "KITE_API_KEY not configured on server"}, 500
        )

    request_token = request.args.get("request_token", "")
    if not request_token:
        return cors_response({"error": "missing_request_token"}, 400)

    try:
        session_data = _kite.generate_session(
            request_token, api_secret=KITE_API_SECRET
        )
        access_token = session_data["access_token"]
        import json as _json
        _KITE_TOKEN_FILE.write_text(_json.dumps({
            "access_token": access_token,
            "timestamp": _time.time(),
        }))
        push_kite_token_to_github()
        print("[kite/callback] Access token stored for today")
        return "Kite login successful — token stored for today \u2705"
    except Exception as e:
        print(f"[kite/callback] auth failed: {e}")
        return cors_response({"error": str(e)}, 500)


def _read_kite_token():
    """Returns (token, token_date_str) from disk, or (None, None) if absent/unreadable."""
    import json as _json
    try:
        data = _json.loads(_KITE_TOKEN_FILE.read_text())
        token = (data.get("access_token") or "").strip()
        token_date = datetime.fromtimestamp(float(data["timestamp"])).strftime("%Y-%m-%d")
        return token, token_date
    except (FileNotFoundError, ValueError, KeyError):
        return None, None


@app.route("/kite/token-status", methods=["GET", "OPTIONS"])
def kite_token_status():
    """Check if a Kite access token is stored and still same-day fresh."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    token, token_date = _read_kite_token()
    today = datetime.now().strftime("%Y-%m-%d")
    return cors_response({
        "hasToken": bool(token),
        "valid":    bool(token) and token_date == today,
        "date":     token_date,
    })


def get_kite_client():
    """
    Returns a KiteConnect instance with today's access_token set, or None
    if not configured / not logged in yet today. Use this from the Gap
    scan-to-order bridge once it's built.
    """
    if _kite is None:
        return None
    token, token_date = _read_kite_token()
    if not token or token_date != datetime.now().strftime("%Y-%m-%d"):
        return None
    _kite.set_access_token(token)
    return _kite


def push_kite_token_to_github():
    """
    Pushes kite_token.json to the GitHub data branch — same PUT pattern as
    push_gap_settings_to_github(). Without this, a Render restart between a
    morning /kite/login and the 3:15 PM /gap-orders/enter cron wipes the
    token (DATA_DIR is ephemeral like /tmp), silently turning today's
    automation into a 401 "not_logged_in" until someone notices and logs in
    again. Best-effort — a push failure here must never surface as a failure
    of the login that already succeeded locally.

    The repo this pushes to (crorepathi369/tradeedge-api, data branch) MUST
    stay private — this file holds a live same-day trading credential, not
    just config, unlike the other files this pattern is used for.
    """
    import urllib.request, urllib.error, json as _json, base64 as _b64

    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")
    if not gh_token:
        print("[kite-token] GITHUB_TOKEN not set — skipping kite_token.json push")
        return

    if not _KITE_TOKEN_FILE.exists():
        return

    headers = {
        "Authorization": f"token {gh_token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
        "Content-Type":  "application/json",
    }
    file_url = f"https://api.github.com/repos/{repo}/contents/kite_token.json"
    try:
        content_b64 = _b64.b64encode(_KITE_TOKEN_FILE.read_bytes()).decode()
        sha = None
        try:
            get_req = urllib.request.Request(file_url + f"?ref={branch}", headers=headers)
            meta = _json.loads(urllib.request.urlopen(get_req, timeout=10).read())
            sha = meta.get("sha")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        body = {
            "message": f"kite_token.json update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        put_req = urllib.request.Request(
            file_url, data=_json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(put_req, timeout=15)
        print("[kite-token] kite_token.json pushed to GitHub")
    except Exception as e:
        print(f"[kite-token] GitHub push failed for kite_token.json: {e}")


@app.route("/gap-settings", methods=["POST", "OPTIONS"])
def save_gap_settings():
    """
    Called from TradeEdge.html's saveStrategyDefaults() / saveMobDefaults()
    on Save, for the Overnight Gap strategy only. Mirrors TE_OVERNIGHT_DEFAULTS
    from localStorage server-side so the headless automation job stays in
    sync with whatever's configured in the app — no manual step, no
    hardcoded values.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    body = request.get_json(force=True) or {}
    if not body:
        return cors_response({"error": "empty_body"}, 400)

    try:
        body["_syncedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(GAP_SETTINGS_FILE, "w") as f:
            import json as _json
            _json.dump(body, f, indent=2)
        print(f"[gap-settings] Synced: {body}")
        push_gap_settings_to_github()
        return cors_response({"ok": True, "msg": "Gap settings stored"})
    except Exception as e:
        return cors_response({"error": str(e)}, 500)


@app.route("/gap-settings", methods=["GET", "OPTIONS"])
def get_gap_settings_endpoint():
    """Returns the last-synced Gap settings. Used by the automation job and,
    optionally, the frontend to confirm what's currently live server-side."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    settings = get_gap_settings()
    if settings is None:
        return cors_response({"error": "no_settings_synced_yet"}, 404)
    return cors_response(settings)


def get_gap_settings():
    """
    Returns the current Gap Settings dict as last synced from the frontend,
    or None if nothing's been synced yet. The automation job (once built)
    calls this instead of hardcoding minGap/maxGap/slPct/etc, so any
    backtest-driven setting change in the app is picked up automatically.
    """
    try:
        import json as _json
        with open(GAP_SETTINGS_FILE) as f:
            return _json.load(f)
    except (FileNotFoundError, ValueError):
        return None


def push_gap_settings_to_github():
    """
    Pushes gap_settings.json to the GitHub data branch — same PUT pattern as
    push_positions_to_github()/push_gap_presets_to_github(). This is the live
    Overnight Gap automation config (minGap/maxGap/slPct/tradingMode/etc, i.e.
    whether entries are Paper or Live); without a backup, a Render redeploy
    wipes it and get_gap_settings() returns None, silently breaking
    /gap-orders/enter and /gap-scan ("no_gap_settings_synced") until someone
    notices and hits Save again in the app. Called after every settings save
    so GitHub is never more than one save behind. Best-effort — a push
    failure here must never surface as a failure of the save that already
    succeeded locally.
    """
    import urllib.request, urllib.error, json as _json, base64 as _b64

    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")
    if not gh_token:
        print("[gap-settings] GITHUB_TOKEN not set — skipping gap_settings.json push")
        return

    path = GAP_SETTINGS_FILE
    if not path.exists():
        return

    headers = {
        "Authorization": f"token {gh_token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
        "Content-Type":  "application/json",
    }
    file_url = f"https://api.github.com/repos/{repo}/contents/gap_settings.json"
    try:
        content_b64 = _b64.b64encode(path.read_bytes()).decode()
        sha = None
        try:
            get_req = urllib.request.Request(file_url + f"?ref={branch}", headers=headers)
            meta = _json.loads(urllib.request.urlopen(get_req, timeout=10).read())
            sha = meta.get("sha")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        body = {
            "message": f"gap_settings.json update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        put_req = urllib.request.Request(
            file_url, data=_json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(put_req, timeout=15)
        print("[gap-settings] gap_settings.json pushed to GitHub")
    except Exception as e:
        print(f"[gap-settings] GitHub push failed for gap_settings.json: {e}")


# ── Named Overnight Gap presets ──────────────────────────────────────────────
# Separate from GAP_SETTINGS_FILE above: gap-settings is the single "live"
# config the automation reads; gap-presets is a named library of parameter
# combinations the frontend lets the user save/switch between for backtesting.
# DELETE isn't used anywhere else in this app and add_cors()/cors_response()
# both hardcode Access-Control-Allow-Methods to "GET, OPTIONS", so deletion
# goes through POST /gap-presets/delete rather than an actual DELETE verb —
# consistent with every other mutating route here.

def get_gap_presets():
    """Returns the full {name: paramsObject} preset dict, or {} if none saved yet."""
    try:
        import json as _json
        with open(GAP_PRESETS_FILE) as f:
            return _json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def _save_gap_presets(presets):
    import json as _json
    with open(GAP_PRESETS_FILE, "w") as f:
        _json.dump(presets, f, indent=2)
    push_gap_presets_to_github()


def push_gap_presets_to_github():
    """
    Pushes gap_presets.json to the GitHub data branch — same PUT pattern as
    push_positions_to_github() below. Without this, gap_presets.json only
    ever lived in Render's local disk, which is wiped on every redeploy/dyno
    restart (this is exactly what happened the first time: presets saved via
    /gap-presets disappeared after the next deploy). Called after every
    save/delete so GitHub is never more than one edit behind. Best-effort —
    a push failure here must never surface as a failure of the save/delete
    that already succeeded locally.
    """
    import urllib.request, urllib.error, json as _json, base64 as _b64

    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")
    if not gh_token:
        print("[gap-presets] GITHUB_TOKEN not set — skipping gap_presets.json push")
        return

    path = GAP_PRESETS_FILE
    if not path.exists():
        return

    headers = {
        "Authorization": f"token {gh_token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
        "Content-Type":  "application/json",
    }
    file_url = f"https://api.github.com/repos/{repo}/contents/gap_presets.json"
    try:
        content_b64 = _b64.b64encode(path.read_bytes()).decode()
        sha = None
        try:
            get_req = urllib.request.Request(file_url + f"?ref={branch}", headers=headers)
            meta = _json.loads(urllib.request.urlopen(get_req, timeout=10).read())
            sha = meta.get("sha")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        body = {
            "message": f"gap_presets.json update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        put_req = urllib.request.Request(
            file_url, data=_json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(put_req, timeout=15)
        print("[gap-presets] gap_presets.json pushed to GitHub")
    except Exception as e:
        print(f"[gap-presets] GitHub push failed for gap_presets.json: {e}")


@app.route("/gap-presets", methods=["GET", "OPTIONS"])
def get_gap_presets_endpoint():
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    return cors_response(get_gap_presets())


@app.route("/gap-presets", methods=["POST", "OPTIONS"])
def save_gap_preset():
    """Upserts one named preset. Body: {name, params}."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    body = request.get_json(force=True) or {}
    name = (body.get("name") or "").strip()
    params = body.get("params")
    if not name or not isinstance(params, dict):
        return cors_response({"error": "name_and_params_required"}, 400)

    try:
        presets = get_gap_presets()
        presets[name] = params
        _save_gap_presets(presets)
        return cors_response({"ok": True, "msg": f"Preset '{name}' saved", "presets": presets})
    except Exception as e:
        return cors_response({"error": str(e)}, 500)


@app.route("/gap-presets/delete", methods=["POST", "OPTIONS"])
def delete_gap_preset():
    """Removes one named preset. Body: {name}."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    body = request.get_json(force=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return cors_response({"error": "name_required"}, 400)

    try:
        presets = get_gap_presets()
        presets.pop(name, None)
        _save_gap_presets(presets)
        return cors_response({"ok": True, "msg": f"Preset '{name}' deleted", "presets": presets})
    except Exception as e:
        return cors_response({"error": str(e)}, 500)


# ── Gap automation config — which presets actually trade ────────────────────
# See GAP_AUTOMATION_CONFIG_FILE's comment above for why this is a separate
# file from gap_presets.json rather than a flag inside each preset.

def get_enabled_preset_names() -> list:
    try:
        import json as _json
        with open(GAP_AUTOMATION_CONFIG_FILE) as f:
            return _json.load(f).get("enabledPresets", [])
    except (FileNotFoundError, ValueError):
        return []


def _save_enabled_preset_names(names: list) -> None:
    import json as _json
    with open(GAP_AUTOMATION_CONFIG_FILE, "w") as f:
        _json.dump({"enabledPresets": names}, f, indent=2)
    push_gap_automation_config_to_github()


def push_gap_automation_config_to_github():
    """Pushes gap_automation_config.json to the GitHub data branch — same PUT
    pattern as push_gap_presets_to_github()/push_gap_settings_to_github(). Same
    ephemeral-Render-disk rationale: without this, the "which presets are
    automated" list would be wiped on every redeploy just like the other two
    were before they got this treatment."""
    import urllib.request, urllib.error, json as _json, base64 as _b64

    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")
    if not gh_token:
        print("[gap-automation-config] GITHUB_TOKEN not set — skipping push")
        return

    path = GAP_AUTOMATION_CONFIG_FILE
    if not path.exists():
        return

    headers = {
        "Authorization": f"token {gh_token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
        "Content-Type":  "application/json",
    }
    file_url = f"https://api.github.com/repos/{repo}/contents/gap_automation_config.json"
    try:
        content_b64 = _b64.b64encode(path.read_bytes()).decode()
        sha = None
        try:
            get_req = urllib.request.Request(file_url + f"?ref={branch}", headers=headers)
            meta = _json.loads(urllib.request.urlopen(get_req, timeout=10).read())
            sha = meta.get("sha")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        body = {
            "message": f"gap_automation_config.json update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        put_req = urllib.request.Request(
            file_url, data=_json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(put_req, timeout=15)
        print("[gap-automation-config] gap_automation_config.json pushed to GitHub")
    except Exception as e:
        print(f"[gap-automation-config] GitHub push failed: {e}")


@app.route("/gap-automation-config", methods=["GET", "OPTIONS"])
def get_gap_automation_config_endpoint():
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    return cors_response({"enabledPresets": get_enabled_preset_names()})


@app.route("/gap-automation-config", methods=["POST", "OPTIONS"])
def save_gap_automation_config():
    """Sets which presets participate in automated entry/exit. Body:
    {"enabledPresets": [name, ...]} — full replacement, not a toggle-one-name
    call, matching how the frontend caches and re-syncs this list."""
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    body = request.get_json(force=True) or {}
    names = body.get("enabledPresets")
    if not isinstance(names, list):
        return cors_response({"error": "enabledPresets_list_required"}, 400)

    try:
        _save_enabled_preset_names(names)
        return cors_response({"ok": True, "enabledPresets": names})
    except Exception as e:
        return cors_response({"error": str(e)}, 500)


def get_automated_presets() -> list:
    """Returns [(name, params), ...] for every preset flagged 'Include in
    Automated Trades' — the sole driver of /gap-orders/enter and
    /gap-orders/backfill-paper now, replacing the old single-gap-settings
    flow."""
    presets = get_gap_presets()
    enabled = set(get_enabled_preset_names())
    return [(name, params) for name, params in presets.items() if name in enabled]


# ── Gap automation — scan, entry, exit ──────────────────────────────────────
# Wiring order: cron-job.org hits /gap-orders/enter at 3:15 PM (signal day)
# and /gap-orders/exit at 3:15 PM the next trading day. Both are safe to
# call manually too — e.g. for a dry run via /gap-scan first.
#
# Paper vs Live: settings['tradingMode'] (synced from the Overnight Gap
# config panel, defaults to 'paper' if missing) decides whether
# place_entry_order() places a real order or simulates one against real
# market data — see kite_orders.py's module docstring. Still requires
# today's Kite login either way, since paper mode reads real LTP/OHLC.

def get_scan_symbols():
    """
    Returns every symbol with a CSV currently on disk in DATA_DIR, rather
    than the hardcoded ALL_SYMBOLS list — restore_data_from_github() pulls
    down every CSV present in the GitHub data branch regardless of
    ALL_SYMBOLS, so ALL_SYMBOLS can silently drift out of sync (e.g.
    CGPOWER was missing from it despite having live data). This keeps the
    Gap automation scanning the same universe the frontend actually uses.
    """
    return sorted(p.stem for p in DATA_DIR.glob("*.csv"))


def push_positions_to_github():
    """
    Pushes gap_positions.json to the GitHub data branch — same PUT pattern
    already used for CSVs in /breeze/fetch. Trade history is real money now
    (entry/exit fill prices, realized P&L), and unlike the CSVs it had no
    backup at all until this: restore_data_from_github() only ever pulled
    .csv/SECTOR_MAP.json back down, so a Render disk wipe (redeploy, dyno
    rebuild) would have erased it permanently. Called after every entry/exit
    so GitHub is never more than one trade behind. Best-effort — a push
    failure here must never surface as a failure of the order that already
    went through.
    """
    import urllib.request, urllib.error, json as _json, base64 as _b64

    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo     = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch   = os.environ.get("GITHUB_DATA_BRANCH", "data")
    if not gh_token:
        print("[gap-orders] GITHUB_TOKEN not set — skipping gap_positions.json push")
        return

    path = DATA_DIR / "gap_positions.json"
    if not path.exists():
        return

    headers = {
        "Authorization": f"token {gh_token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
        "Content-Type":  "application/json",
    }
    file_url = f"https://api.github.com/repos/{repo}/contents/gap_positions.json"
    try:
        content_b64 = _b64.b64encode(path.read_bytes()).decode()
        sha = None
        try:
            get_req = urllib.request.Request(file_url + f"?ref={branch}", headers=headers)
            meta = _json.loads(urllib.request.urlopen(get_req, timeout=10).read())
            sha = meta.get("sha")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        body = {
            "message": f"gap_positions.json update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        put_req = urllib.request.Request(
            file_url, data=_json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(put_req, timeout=15)
        print("[gap-orders] gap_positions.json pushed to GitHub")
    except Exception as e:
        print(f"[gap-orders] GitHub push failed for gap_positions.json: {e}")


@app.route("/gap-scan", methods=["GET", "OPTIONS"])
def gap_scan_endpoint():
    """
    Dry-run — runs the headless scan for a given date (default: today) and
    returns the ranked/filtered signal(s) WITHOUT placing any order. Use
    this to sanity-check against the app's own Scanner tab before trusting
    /gap-orders/enter.
    Usage: /gap-scan?date=2026-08-08
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    settings = get_gap_settings()
    if settings is None:
        return cors_response({"error": "no_gap_settings_synced"}, 400)

    scan_date = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
    verbose = request.args.get("verbose") == "1"
    try:
        result = gap_scan.scan_gap_signals(DATA_DIR, get_scan_symbols(), scan_date, settings)
    except Exception as e:
        return cors_response({"error": str(e)}, 500)

    resp = {
        "scanDate": scan_date,
        "settingsUsed": settings,
        "longsCount": len(result["longs"]), "shortsCount": len(result["shorts"]),
        "selected": result["selected"],
    }
    if verbose:
        resp["longs"] = result["longs"]
        resp["shorts"] = result["shorts"]
    return cors_response(resp)


@app.route("/gap-orders/enter", methods=["POST", "OPTIONS"])
def gap_orders_enter():
    """
    Called by cron-job.org at 3:15 PM IST on scan days. Loops every preset
    flagged "Include in Automated Trades" (get_automated_presets()) and runs
    today's scan + entry independently for each, tagging every trade with
    its own preset name — this REPLACES the old single-get_gap_settings()
    flow entirely (gap_settings.json/the plain "Save" button no longer
    drive automation; presets are now the sole source). Each preset's own
    Cap Max/day applies (see gap_scan.py), so different presets can enter a
    different number of signals on the same day. Two presets signaling the
    same symbol the same day each place their own independent position —
    intentional, so setups stay comparable apples-to-apples.

    mode is hardcoded to "paper" for every preset here, regardless of
    whatever tradingMode a preset's saved params happen to contain — an
    explicit safety override, not incidental, since Live hasn't been
    extended to multi-preset yet (see kite_orders.close_open_positions()'s
    live-mode aggregation caveat).
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    kite = get_kite_client()
    if kite is None:
        return cors_response({"error": "not_logged_in — visit /kite/login first"}, 401)

    automated = get_automated_presets()
    if not automated:
        telegram_notify.notify_error("enter", "No presets flagged 'Include in Automated Trades' — nothing to do")
        return cors_response({"error": "no_automated_presets"}, 400)

    today = datetime.now().date()
    scan_date = today.isoformat()
    symbols = get_scan_symbols()
    all_results = []

    for preset_name, params in automated:
        try:
            scan_result = gap_scan.scan_gap_signals(DATA_DIR, symbols, scan_date, params)
        except Exception as e:
            telegram_notify.notify_error("enter/scan", f"[{preset_name}] Scan failed for {scan_date}: {e}")
            all_results.append({"preset": preset_name, "ok": False, "error": f"scan failed: {e}"})
            continue

        for signal in scan_result["selected"]:
            res = kite_orders.place_entry_order(
                kite, DATA_DIR, signal, today,
                sl_pct=params["slPct"], sl_type=params.get("slType", "pct"),
                mode="paper", preset=preset_name,
                tp_type=params.get("tpType", "d2_close"), tp_pct=params.get("tpPct", 1.0),
                hold_days=params.get("holdDays", 1),
            )
            res["sym"] = signal["sym"]
            res["preset"] = preset_name
            all_results.append(res)
            print(f"[gap-orders/enter] ({preset_name}, paper) {signal['sym']}: {res}")

    telegram_notify.notify_entry_results(scan_date, "paper", all_results)
    push_positions_to_github()
    return cors_response({"scanDate": scan_date, "results": all_results})


@app.route("/gap-orders/exit", methods=["POST", "OPTIONS"])
def gap_orders_exit():
    """
    Called by cron-job.org at 3:15 PM IST the day AFTER entry. Closes any
    position still open (i.e. not already stopped out by its GTT).
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    kite = get_kite_client()
    if kite is None:
        telegram_notify.notify_error("exit", "Not logged in — visit /kite/login before market close")
        return cors_response({"error": "not_logged_in — visit /kite/login first"}, 401)

    results = kite_orders.close_open_positions(kite, DATA_DIR)
    print(f"[gap-orders/exit] {results}")
    telegram_notify.notify_exit_results(results)
    push_positions_to_github()
    return cors_response({"results": results})


@app.route("/gap-orders/daily-digest", methods=["POST", "OPTIONS"])
def gap_orders_daily_digest():
    """
    Sends the once-a-day Telegram summary — meant for a separate
    cron-job.org trigger timed AFTER both /gap-orders/enter and
    /gap-orders/exit have run (e.g. 3:45 PM IST), so it reflects today's
    completed activity rather than a snapshot mid-run.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    positions = kite_orders.load_positions(DATA_DIR)
    digest = telegram_notify.build_daily_digest(positions)
    sent = telegram_notify.send_telegram(digest)
    return cors_response({"sent": sent, "digest": digest})


@app.route("/gap-orders/backfill-paper", methods=["POST", "OPTIONS"])
def gap_orders_backfill_paper():
    """
    Retroactively populates paper trades for a date window, so Paper mode
    can be sanity-checked against real market history immediately instead
    of waiting for it to accumulate forward day by day. Always paper —
    backfilling a 'live' trade for a past date makes no sense, so this
    ignores settings['tradingMode'] entirely.

    Window: pass explicit `from`/`to` (YYYY-MM-DD) for a specific slice —
    this is what the frontend uses to backfill in monthly chunks, so it can
    show real progress and keep each request small enough to not time out.
    Falls back to `days` calendar days back from today (default 365) if
    `from`/`to` aren't given, for direct/manual calls.

    For each trading day in the window (oldest to newest, using whatever
    dates actually have CSV data — same universe as the live scan), runs
    the exact same gap_scan.scan_gap_signals() the live automation would
    have, then backfills each selected signal via
    kite_orders.backfill_paper_trade(), passing up to `holdDays` future
    trading dates (not naive +N calendar days, and NOT the entry day's own
    range, which would wrongly compare the SL/TP to price action from
    before the position existed) for it to walk day-by-day — a single date
    for d2_close/d2_open presets (hold is always 1 day), several for a
    %-TP preset with holdDays>1, mirroring exactly how the live automation
    now resolves the same preset's multi-day hold. Critically, those future
    dates are looked up against the FULL date history on disk, not just the
    dates inside this one request's window — otherwise the last day(s) of
    every chunk would wrongly look like "no more data yet" and get left
    open, even though the real next day's data already exists in an
    adjacent chunk. Only dates genuinely at/near the end of the whole
    backfillable history run out of lookahead, and those are correctly
    left open (partway through, if some days were already resolved) for
    the regular /gap-orders/exit job to finish naturally.

    kite_orders.backfill_paper_trade() saves after every individual trade,
    not just at the end, and skips (symbol, date, preset) triples already
    done (idempotent) — so a timed-out or retried request never duplicates
    or loses work, whether retried whole or chunk-by-chunk.

    Requires a `preset` query param — since presets now drive automation
    (see /gap-orders/enter), an untagged backfill would create more
    untagged trades and undo the one-time /gap-orders/migrate-preset-tag
    migration. The frontend passes whichever preset is currently selected
    in the Presets dropdown; loop this once per preset to backfill more
    than one (that's what the existing chunked-backfill UI button does).

    Usage: POST /gap-orders/backfill-paper?preset=Oneday-Setup&from=2025-08-01&to=2025-08-31
       or: POST /gap-orders/backfill-paper?preset=Oneday-Setup&days=365
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})

    kite = get_kite_client()
    if kite is None:
        return cors_response({"error": "not_logged_in — visit /kite/login first"}, 401)

    preset_name = request.args.get("preset")
    if not preset_name:
        return cors_response({"error": "preset_param_required"}, 400)
    params = get_gap_presets().get(preset_name)
    if params is None:
        return cors_response({"error": f"unknown_preset: {preset_name}"}, 404)

    from_param = request.args.get("from")
    to_param = request.args.get("to")
    if from_param and to_param:
        window_start, window_end = from_param, to_param
    else:
        try:
            days_back = int(request.args.get("days", 365))
        except ValueError:
            return cors_response({"error": "invalid 'days' param"}, 400)
        days_back = max(1, min(days_back, 400))  # sane bounds — this is a backfill, not a full re-scan
        window_start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        window_end = datetime.now().strftime("%Y-%m-%d")

    symbols = get_scan_symbols()

    # Full history, not window-scoped — needed so "next trading day" resolves
    # correctly even for the last date of a chunked (sub-window) request.
    all_dates = set()
    for sym in symbols:
        for c in gap_scan.load_ohlc(DATA_DIR, sym):
            all_dates.add(c["date"])
    all_dates = sorted(all_dates)

    dates = [d for d in all_dates if window_start <= d < window_end]  # exclude window_end itself — today's own date is the live enter job's, and chunk boundaries shouldn't double-count
    if not dates:
        return cors_response({"error": "no trading days with data in that window"}, 400)

    results = []
    for scan_date in dates:
        try:
            scan_result = gap_scan.scan_gap_signals(DATA_DIR, symbols, scan_date, params)
        except Exception as e:
            results.append({"date": scan_date, "preset": preset_name, "ok": False, "error": f"scan failed: {e}"})
            continue

        idx = all_dates.index(scan_date)
        # Up to hold_days future trading dates (naturally truncated near the
        # end of the available history) — d2_close/d2_open presets only ever
        # need one, since their hold is always 1 day; a %-TP preset with
        # holdDays=3 gets up to 3, so backfill_paper_trade() can walk the
        # same multi-day SL/TP resolution the live automation now does.
        hold_days_for_backfill = max(1, params.get("holdDays", 1) or 1)
        exit_check_date_strs = all_dates[idx + 1: idx + 1 + hold_days_for_backfill]
        exit_check_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in exit_check_date_strs]
        entry_date = datetime.strptime(scan_date, "%Y-%m-%d").date()

        for signal in scan_result["selected"]:
            res = kite_orders.backfill_paper_trade(
                kite, DATA_DIR, signal, entry_date,
                sl_pct=params["slPct"], sl_type=params.get("slType", "pct"),
                exit_check_dates=exit_check_dates, preset=preset_name,
                tp_type=params.get("tpType", "d2_close"), tp_pct=params.get("tpPct", 1.0),
                hold_days=hold_days_for_backfill,
            )
            res["date"] = scan_date
            res["sym"] = signal["sym"]
            res["preset"] = preset_name
            results.append(res)
            print(f"[gap-orders/backfill] ({preset_name}) {scan_date} {signal['sym']}: {res}")

    push_positions_to_github()
    backfilled = sum(1 for r in results if r.get("ok"))
    return cors_response({
        "datesScanned": len(dates), "window": f"{window_start}..{window_end}",
        "preset": preset_name, "backfilled": backfilled, "results": results,
    })


@app.route("/gap-orders/clear-backfill", methods=["POST", "OPTIONS"])
def gap_orders_clear_backfill():
    """
    Removes backfilled=True trades, leaving real paper/live trades
    untouched. Needed after any fix to the backfill/paper SL logic —
    otherwise re-running /gap-orders/backfill-paper would just skip every
    (symbol, date, preset) the buggy run already covered, permanently
    stranding the bad records under the idempotency check.

    Optional `preset` query param scopes the clear to one preset only —
    with multiple presets automated in parallel, an unscoped clear would
    wipe every other preset's backfill history too when you only meant to
    fix one. Omit it for the old unscoped "clear everything" behavior
    (an intentional full reset); the frontend button always passes the
    currently-selected preset.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    preset_name = request.args.get("preset")
    removed = kite_orders.clear_backfilled_trades(DATA_DIR, preset=preset_name)
    push_positions_to_github()
    return cors_response({"removed": removed, "preset": preset_name})


@app.route("/gap-orders/clear-all", methods=["POST", "OPTIONS"])
def gap_orders_clear_all():
    """
    Removes EVERY trade for a preset — backfilled AND real (paper/live) alike.
    Unlike /gap-orders/clear-backfill, this also erases genuinely-executed
    automated trades — for a deliberate full reset when a strategy's params
    have changed enough that the existing log no longer reflects how the
    preset actually behaves. Requires an explicit `preset` query param (no
    unscoped "clear everything" default here — the blast radius is real trade
    history, not just re-creatable backfill data, so an accidental omission
    must not wipe every preset at once).
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    preset_name = request.args.get("preset")
    if not preset_name:
        return cors_response({"error": "preset_required"}, 400)
    removed = kite_orders.clear_all_trades(DATA_DIR, preset=preset_name)
    push_positions_to_github()
    return cors_response({"removed": removed, "preset": preset_name})


@app.route("/gap-orders/migrate-preset-tag", methods=["POST", "OPTIONS"])
def gap_orders_migrate_preset_tag():
    """
    One-time (idempotent — safe to call more than once) migration: tags
    every trade in gap_positions.json currently missing a 'preset' field
    with the given preset name (normally 'Oneday-Setup', since that
    preset's params are what drove gap_settings.json/the plain "Save"
    button before presets existed and drove automation). Admin-triggered
    manually, not cron-wired.

    MUST be run before switching /gap-orders/enter's automation over to
    the new (symbol, preset)-scoped duplicate-entry guard in production —
    an untagged genuinely-open position is invisible to that guard and
    could otherwise be silently re-entered. Body: {"preset": "Oneday-Setup"}
    (defaults to "Oneday-Setup" if omitted).
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    body = request.get_json(force=True) or {}
    preset_name = (body.get("preset") or "Oneday-Setup").strip()
    tagged = kite_orders.tag_untagged_trades(DATA_DIR, preset_name)
    push_positions_to_github()
    return cors_response({"ok": True, "tagged": tagged, "preset": preset_name})


@app.route("/gap-orders/status", methods=["GET", "OPTIONS"])
def gap_orders_status():
    """
    Returns every trade ever recorded — a flat array, each item carrying
    its own 'sym' — for the Automated Trades UI to render directly.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    positions = kite_orders.load_positions(DATA_DIR)
    flat = [{"sym": sym, **trade} for sym, trades in positions.items() for trade in trades]
    return cors_response(flat)


@app.route("/gap-orders/diagnose", methods=["GET", "OPTIONS"])
def gap_orders_diagnose():
    """
    One-time diagnostic (read-only) — scans gap_positions.json for the same
    out-of-order-append pattern that stranded BRITANNIA (fixed in
    kite_orders._last_trade()/close_open_positions() to sort by entry_date
    rather than trust array position). Reports any symbol with a
    non-chronological trade list, and specifically flags any symbol whose
    genuinely-open trade the OLD logic would have missed — i.e. any other
    BRITANNIA-like cases sitting stuck right now.
    """
    if request.method == "OPTIONS":
        return cors_response({"ok": True})
    findings = kite_orders.diagnose_order_issues(DATA_DIR)
    return cors_response({
        "issuesFound": len(findings),
        "findings": findings,
    })
