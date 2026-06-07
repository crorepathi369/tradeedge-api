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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

# ── Data directory — CSVs written here by /run-fetch, served by /data/ ────────
DATA_DIR = Path(os.environ.get("TRADEEDGE_DATA_DIR", "./tradeedge_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── GitHub restore — pulls CSVs from 'data' branch on every startup ───────────
def restore_data_from_github():
    """
    On startup, download all CSVs + SECTOR_MAP.json from the GitHub 'data' branch
    into DATA_DIR. Only downloads files that don't exist yet — incremental updates
    (written by /run-fetch) are kept as-is.

    Required env vars:
        GITHUB_TOKEN       — personal access token with 'repo' scope
        GITHUB_REPO        — e.g. crorepathi369/tradeedge-api
        GITHUB_DATA_BRANCH — branch name, default 'data'
    """
    import urllib.request, urllib.error, json as _json

    token  = os.environ.get("GITHUB_TOKEN", "")
    repo   = os.environ.get("GITHUB_REPO", "crorepathi369/tradeedge-api")
    branch = os.environ.get("GITHUB_DATA_BRANCH", "data")

    if not token:
        print("[restore] GITHUB_TOKEN not set — skipping GitHub restore")
        return

    print(f"[restore] Starting restore from github:{repo}@{branch} → {DATA_DIR}")

    # Step 1: fetch the full file tree of the data branch
    tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
    req = urllib.request.Request(tree_url, headers={
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "TradeEdge-App",
    })
    try:
        tree = _json.loads(urllib.request.urlopen(req, timeout=30).read())
    except Exception as e:
        print(f"[restore] GitHub tree fetch failed: {e}")
        return

    files = [
        f["path"] for f in tree.get("tree", [])
        if f["type"] == "blob" and (
            f["path"].endswith(".csv") or f["path"] == "SECTOR_MAP.json"
        )
    ]
    print(f"[restore] {len(files)} files found in data branch")

    # Step 2: download each file that isn't already on disk
    downloaded = 0
    skipped    = 0
    failed     = 0

    for filename in files:
        dest = DATA_DIR / filename          # filename is just e.g. "RELIANCE.csv"
        if dest.exists():
            skipped += 1
            continue                        # already present — keep local version

        raw_url = (
            f"https://raw.githubusercontent.com/{repo}/{branch}/{filename}"
        )
        raw_req = urllib.request.Request(raw_url, headers={
            "Authorization": f"token {token}",
            "User-Agent":    "TradeEdge-App",
        })
        try:
            data = urllib.request.urlopen(raw_req, timeout=30).read()
            dest.write_bytes(data)
            downloaded += 1
        except Exception as e:
            print(f"[restore] ✗ {filename}: {e}")
            failed += 1

    print(
        f"[restore] Done — downloaded={downloaded}  "
        f"skipped(already present)={skipped}  failed={failed}"
    )

# Run restore in a background thread so Flask starts up immediately
# (Render health check hits '/' within 30s — we can't block startup)
threading.Thread(target=restore_data_from_github, daemon=True).start()

# ── Fetch job state — prevents overlapping runs ────────────────────────────────
_fetch_lock   = threading.Lock()
_fetch_status = {
    "running":    False,
    "startedAt":  None,
    "finishedAt": None,
    "ok":         0,
    "failed":     0,
    "failedSyms": [],
    "lastError":  None,
    "log":        [],        # last N log lines
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
    "JUBLFOOD":"SECTOR_FMCG","NYKAA":"SECTOR_FMCG","ETERNAL":"SECTOR_FMCG",
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
    "CAMS":"SECTOR_FINANCE","IIFL":"SECTOR_FINANCE","TATAMOTORS":"SECTOR_AUTO",
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


@app.route("/run-fetch")
def run_fetch():
    """
    Trigger the daily fetch job.
    Call this from cron-job.org at 15:20 IST (09:50 UTC) Mon–Fri.
    Protected by FETCH_SECRET env var — pass as ?secret=xxx.

    Returns immediately with job_started=true; poll /fetch-status for progress.
    If a job is already running, returns job_started=false.
    """
    secret = os.environ.get("FETCH_SECRET", "")
    if secret and request.args.get("secret", "") != secret:
        return cors_response({"error": "Unauthorized"}, 401)

    if _fetch_status["running"]:
        return cors_response({
            "job_started": False,
            "reason":      "already running",
            "status":      _fetch_status,
        })

    t = threading.Thread(target=_do_fetch_job, daemon=True)
    t.start()
    return cors_response({
        "job_started": True,
        "symbols":     len(ALL_SYMBOLS),
        "dataDir":     str(DATA_DIR),
        "message":     "Fetch started in background. Poll /fetch-status for progress.",
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
