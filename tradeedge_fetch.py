"""
TradeEdge — Python Data Fetcher
=================================
Downloads historical daily OHLC data from Yahoo Finance and saves as CSV files
compatible with TradeEdge.html. Symbol names and Yahoo ticker mappings are kept
in exact sync with app.py (ALL_SYMBOLS + YAHOO_TICKER_MAP).

USAGE:
    python tradeedge_fetch.py                        # Nifty 50, last 2 years
    python tradeedge_fetch.py --fo                   # Full F&O universe
    python tradeedge_fetch.py --fo --merge           # Incremental update
    python tradeedge_fetch.py --from 2023-01-01      # Custom start date
    python tradeedge_fetch.py --symbols RELIANCE TCS # Specific symbols
    python tradeedge_fetch.py --summary              # Show saved files

    # Futures backwardation scanner (requires Kotak Neo API or manual CSV)
    python tradeedge_fetch.py --futures                        # Nifty 50 scan
    python tradeedge_fetch.py --futures --fo                   # All F&O stocks
    python tradeedge_fetch.py --futures --fo --futures-save    # + save FUTURES_SNAPSHOT.csv
    python tradeedge_fetch.py --futures --symbols LTIM INFY    # Specific symbols

    # Manual futures CSV import (when broker API not configured)
    # Export futures prices from Kite/Kotak watchlist as CSV with columns:
    #   symbol, near_fut, far_fut
    # Then run:
    python tradeedge_fetch.py --futures --futures-csv /path/to/watchlist.csv

FUTURES DATA SOURCE:
    Priority 1: Kotak Neo API (set KOTAK_CONSUMER_KEY + KOTAK_ACCESS_TOKEN env vars)
    Priority 2: Manual CSV import (--futures-csv flag)
    Priority 3: Spot-only from Yahoo (near/far will show N/A)

    Note: NSE direct API and Yahoo Finance do NOT provide NSE stock futures prices
    programmatically — both are blocked/unavailable for scripted access.
    Broker APIs (Kotak Neo, Kite Connect, Upstox) are the only reliable source.

OUTPUT: ./tradeedge_data/RELIANCE.csv, TCS.csv ... (one file per symbol)

CSV FORMAT (TradeEasy/TradeEdge compatible):
    date,open,high,low,close,adj_close,volume
    2024-01-02,2450.10,2465.50,2430.00,2458.30,2440.50,5123456
"""
from __future__ import annotations
import sys, time, argparse, json, warnings, os, contextlib
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)

@contextlib.contextmanager
def _suppress_yf_noise():
    """Suppress yfinance's internal print() warnings to stderr."""
    with open(os.devnull, 'w') as devnull:
        old_stderr = sys.stderr
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stderr = old_stderr
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
    import requests
except ImportError:
    print("Missing deps. Run:  pip install yfinance pandas requests")
    sys.exit(1)


# ── Yahoo ticker overrides — must match app.py YAHOO_TICKER_MAP exactly ───────
YAHOO_TICKER_MAP = {
    # Major indices
    "NIFTY50":      "^NSEI",
    "BANKNIFTY":    "^NSEBANK",
    "FINNIFTY":     "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY":   "^NSEMDCP50",
    # Sector indices
    "CNXIT":        "^CNXIT",
    "CNXAUTO":      "^CNXAUTO",
    "CNXPHARMA":    "^CNXPHARMA",
    "CNXENERGY":    "^CNXENERGY",
    "CNXMETAL":     "^CNXMETAL",
    "CNXFMCG":      "^CNXFMCG",
    "CNXINFRA":     "^CNXINFRA",
    "CNXCONSUM":    "^CNXCONSUM",
    # Stocks with non-standard Yahoo tickers
    "M&M":          "M&M.NS",
    "BAJAJ-AUTO":   "BAJAJ-AUTO.NS",
    "BIRLASOFT":    "BSOFT.NS",
    "DEEPAKNITR":   "DEEPAKNTR.NS",
    "ICICIPRULIFE": "ICICIPRULI.NS",
    "MCDOWELL-N":   "UNITDSPR.NS",
    "TATAMOTORS":   "TMPV.NS",
    "ZOMATO":       "ETERNAL.NS",
}

# ── Stock → Sector mapping (for sector filter in TradeEdgeAI) ─────────────────
STOCK_SECTOR_MAP = {
    # IT
    "TCS": "SECTOR_IT", "INFY": "SECTOR_IT", "HCLTECH": "SECTOR_IT",
    "WIPRO": "SECTOR_IT", "TECHM": "SECTOR_IT", "LTIM": "SECTOR_IT",
    "MPHASIS": "SECTOR_IT", "COFORGE": "SECTOR_IT", "KPITTECH": "SECTOR_IT",
    "PERSISTENT": "SECTOR_IT", "TATAELXSI": "SECTOR_IT", "OFSS": "SECTOR_IT",
    "NAUKRI": "SECTOR_IT",
    # Banking
    "HDFCBANK": "SECTOR_BANK", "ICICIBANK": "SECTOR_BANK", "SBIN": "SECTOR_BANK",
    "KOTAKBANK": "SECTOR_BANK", "AXISBANK": "SECTOR_BANK", "INDUSINDBK": "SECTOR_BANK",
    "BANKBARODA": "SECTOR_BANK", "PNB": "SECTOR_BANK", "CANBK": "SECTOR_BANK",
    "FEDERALBNK": "SECTOR_BANK", "IDFCFIRSTB": "SECTOR_BANK", "AUBANK": "SECTOR_BANK",
    "RBLBANK": "SECTOR_BANK", "BANDHANBNK": "SECTOR_BANK", "YESBANK": "SECTOR_BANK",
    # Finance / NBFC
    "BAJFINANCE": "SECTOR_FINANCE", "BAJAJFINSV": "SECTOR_FINANCE",
    "SHRIRAMFIN": "SECTOR_FINANCE", "CHOLAFIN": "SECTOR_FINANCE",
    "MUTHOOTFIN": "SECTOR_FINANCE", "SBICARD": "SECTOR_FINANCE",
    "SBILIFE": "SECTOR_FINANCE", "HDFCLIFE": "SECTOR_FINANCE",
    "HDFCAMC": "SECTOR_FINANCE", "JIOFIN": "SECTOR_FINANCE",
    "ABCAPITAL": "SECTOR_FINANCE", "LTF": "SECTOR_FINANCE",
    "POLICYBZR": "SECTOR_FINANCE", "CDSL": "SECTOR_FINANCE",
    "MCX": "SECTOR_FINANCE", "360ONE": "SECTOR_FINANCE",
    # Pharma
    "SUNPHARMA": "SECTOR_PHARMA", "DRREDDY": "SECTOR_PHARMA",
    "CIPLA": "SECTOR_PHARMA", "DIVISLAB": "SECTOR_PHARMA",
    "AUROPHARMA": "SECTOR_PHARMA", "LUPIN": "SECTOR_PHARMA",
    "BIOCON": "SECTOR_PHARMA", "ALKEM": "SECTOR_PHARMA",
    "TORNTPHARM": "SECTOR_PHARMA", "GLENMARK": "SECTOR_PHARMA",
    "MANKIND": "SECTOR_PHARMA", "MAXHEALTH": "SECTOR_PHARMA",
    "APOLLOHOSP": "SECTOR_PHARMA",
    # Auto
    "MARUTI": "SECTOR_AUTO", "BAJAJ-AUTO": "SECTOR_AUTO",
    "M&M": "SECTOR_AUTO", "EICHERMOT": "SECTOR_AUTO",
    "TVSMOTOR": "SECTOR_AUTO", "HEROMOTOCO": "SECTOR_AUTO",
    "BOSCHLTD": "SECTOR_AUTO", "BHARATFORG": "SECTOR_AUTO",
    "MOTHERSON": "SECTOR_AUTO",
    # FMCG
    "HINDUNILVR": "SECTOR_FMCG", "ITC": "SECTOR_FMCG",
    "NESTLEIND": "SECTOR_FMCG", "BRITANNIA": "SECTOR_FMCG",
    "DABUR": "SECTOR_FMCG", "MARICO": "SECTOR_FMCG",
    "COLPAL": "SECTOR_FMCG", "GODREJCP": "SECTOR_FMCG",
    "TATACONSUM": "SECTOR_FMCG", "VBL": "SECTOR_FMCG",
    "KALYANKJIL": "SECTOR_FMCG",
    # Metal
    "TATASTEEL": "SECTOR_METAL", "JSWSTEEL": "SECTOR_METAL",
    "HINDALCO": "SECTOR_METAL", "VEDL": "SECTOR_METAL",
    "SAIL": "SECTOR_METAL", "NMDC": "SECTOR_METAL",
    "HINDZINC": "SECTOR_METAL", "JINDALSTEL": "SECTOR_METAL",
    # Realty
    "DLF": "SECTOR_REALTY", "GODREJPROP": "SECTOR_REALTY",
    "OBEROIRLTY": "SECTOR_REALTY", "LODHA": "SECTOR_REALTY",
    "PRESTIGE": "SECTOR_REALTY", "PHOENIXLTD": "SECTOR_REALTY",
    # Energy / Power
    "RELIANCE": "SECTOR_ENERGY", "ONGC": "SECTOR_ENERGY",
    "NTPC": "SECTOR_ENERGY", "POWERGRID": "SECTOR_ENERGY",
    "BPCL": "SECTOR_ENERGY", "IOC": "SECTOR_ENERGY",
    "HINDPETRO": "SECTOR_ENERGY", "GAIL": "SECTOR_ENERGY",
    "PETRONET": "SECTOR_ENERGY", "TATAPOWER": "SECTOR_ENERGY",
    "JSWENERGY": "SECTOR_ENERGY", "ADANIGREEN": "SECTOR_ENERGY",
    "ADANIENSOL": "SECTOR_ENERGY", "NHPC": "SECTOR_ENERGY",
    "SUZLON": "SECTOR_ENERGY",
    # Infra / Engineering
    "LT": "SECTOR_INFRA", "ABB": "SECTOR_INFRA",
    "SIEMENS": "SECTOR_INFRA", "BEL": "SECTOR_INFRA",
    "HAL": "SECTOR_INFRA", "BHEL": "SECTOR_INFRA",
    "CGPOWER": "SECTOR_INFRA", "POLYCAB": "SECTOR_INFRA",
    "HAVELLS": "SECTOR_INFRA", "KEI": "SECTOR_INFRA",
    "DIXON": "SECTOR_INFRA", "ADANIPORTS": "SECTOR_INFRA",
    # PSU / Misc
    "COALINDIA": "SECTOR_PSU", "RECLTD": "SECTOR_PSU",
    "PFC": "SECTOR_PSU", "IRFC": "SECTOR_PSU",
    "LICI": "SECTOR_PSU", "UNIONBANK": "SECTOR_PSU",
    # Consumer / Misc
    "TITAN": "SECTOR_FMCG", "TRENT": "SECTOR_FMCG",
    "PAGEIND": "SECTOR_FMCG", "DMART": "SECTOR_FMCG",
    "JUBLFOOD": "SECTOR_FMCG", "NYKAA": "SECTOR_FMCG",
    "INDIGO": "SECTOR_INFRA", "DELHIVERY": "SECTOR_INFRA",
    "ADANIENT": "SECTOR_INFRA",
    "ETERNAL": "SECTOR_FMCG", "PAYTM": "SECTOR_FINANCE",
    "SRF": "SECTOR_PHARMA", "PIDILITIND": "SECTOR_PHARMA",
    "ULTRACEMCO": "SECTOR_INFRA", "AMBUJACEM": "SECTOR_INFRA",
    "SHREECEM": "SECTOR_INFRA", "GRASIM": "SECTOR_INFRA",
    "TORNTPOWER": "SECTOR_ENERGY", "UPL": "SECTOR_PHARMA",
    "ZYDUSLIFE": "SECTOR_PHARMA", "ASIANPAINT": "SECTOR_FMCG",
    "BERGEPAINT": "SECTOR_FMCG", "INDHOTEL": "SECTOR_FMCG",
    "MUTHOOTFIN": "SECTOR_FINANCE", "CHOLAFIN": "SECTOR_FINANCE",
}

def get_yf_ticker(symbol: str) -> str:
    return YAHOO_TICKER_MAP.get(symbol, symbol + ".NS")


# ── NSE Session + Futures Fetch ───────────────────────────────────────────────
#
# NSE's public API requires a valid browser session cookie.
# Strategy:
#   1. Hit nseindia.com homepage to get cookies
#   2. Then call /api/quote-derivative?symbol=RELIANCE
#   3. Parse FUT contracts from response, sorted by expiry date
#
# This is the same approach used by NSE-Python, nsepy, and similar libraries.

# ── NSE F&O Bhavcopy Downloader ──────────────────────────────────────────────
#
# NSE publishes a complete EOD F&O bhavcopy CSV every trading day.
# This contains ALL futures + options contracts with OHLC, settle price, OI etc.
#
# URL patterns:
#   NEW (2023+): https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
#   OLD (pre-2023): https://archives.nseindia.com/content/historical/DERIVATIVES/YYYY/MON/foDDMONYYYYbhav.csv.zip
#
# Key columns in bhavcopy:
#   TckrSymb  — stock symbol (LTIM, RELIANCE etc.)
#   FinInstrmTp / Instrm — instrument type (STF = Stock Futures, IDF = Index Futures)
#   XpryDt    — expiry date (YYYY-MM-DD)
#   ClsPric / CLOSE_PRICE — closing price
#   SttlmPric / SETTLE_PR — settlement price (use this as last price)

BHAV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en-US;q=0.9",
    "Referer":    "https://www.nseindia.com/",
}


def _bhav_url_new(dt: "date") -> str:
    return (f"https://nsearchives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip")

def _bhav_url_old(dt: "date") -> str:
    mon = dt.strftime("%b").upper()
    return (f"https://archives.nseindia.com/content/historical/DERIVATIVES/"
            f"{dt.year}/{mon}/fo{dt.strftime('%d')}{mon}{dt.year}bhav.csv.zip")


def download_fo_bhavcopy(target_date: "date | None" = None,
                          lookback_days: int = 10,
                          cache_dir: "Path | None" = None) -> "pd.DataFrame | None":
    """
    Download NSE F&O bhavcopy for the most recent available trading day.

    If cache_dir is provided:
      - Checks for an existing BhavCopy_FO_YYYYMMDD.csv in that folder first
      - Saves the downloaded file there for future reuse and offline reference
    Skips weekends automatically. Tries last `lookback_days` calendar days.

    Returns a normalised DataFrame or None if download fails.
    """
    from datetime import date, timedelta
    import zipfile, io

    start = target_date or date.today()

    # ── Check local cache first ───────────────────────────────────────────────
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        for delta in range(lookback_days):
            dt = start - timedelta(days=delta)
            if dt.weekday() >= 5:
                continue
            cached_csv = cache_dir / f"BhavCopy_FO_{dt.strftime('%Y%m%d')}.csv"
            if cached_csv.exists():
                print(f"  ✓ Bhavcopy loaded from cache: {cached_csv.name}")
                df = pd.read_csv(cached_csv, low_memory=False)
                df.columns = [c.strip().upper() for c in df.columns]
                return _normalise_bhavcopy(df, dt)

    # ── Download fresh ────────────────────────────────────────────────────────
    session = requests.Session()
    session.headers.update(BHAV_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1.5)
    except Exception:
        pass

    for delta in range(lookback_days):
        dt = start - timedelta(days=delta)
        if dt.weekday() >= 5:
            continue

        for url_fn in [_bhav_url_new, _bhav_url_old]:
            url = url_fn(dt)
            try:
                r = session.get(url, timeout=30)
                if r.status_code != 200:
                    continue

                print(f"  ✓ Bhavcopy downloaded: {dt.strftime('%d-%b-%Y')} ({len(r.content):,} bytes)")

                # Unzip in memory
                zf       = zipfile.ZipFile(io.BytesIO(r.content))
                csv_name = zf.namelist()[0]
                raw_df   = pd.read_csv(zf.open(csv_name), low_memory=False)

                # ── Save raw CSV to cache_dir for reference & reuse ───────────
                if cache_dir:
                    save_path = cache_dir / f"BhavCopy_FO_{dt.strftime('%Y%m%d')}.csv"
                    raw_df.to_csv(save_path, index=False)
                    print(f"  ✓ Bhavcopy saved → {save_path}")

                raw_df.columns = [c.strip().upper() for c in raw_df.columns]
                return _normalise_bhavcopy(raw_df, dt)

            except Exception:
                continue

    print("  ✗ Could not download bhavcopy for any recent trading day.")
    print("    Bhavcopy is published after 6 PM IST on trading days.")
    return None


def _normalise_bhavcopy(df: "pd.DataFrame", bhav_date: "date") -> "pd.DataFrame":
    """
    Normalise old and new bhavcopy column names into a consistent schema:
        symbol | instrument | expiry | settle | close
    """
    col = df.columns.tolist()

    # ── Symbol column ─────────────────────────────────────────────────
    sym_col = next((c for c in col if c in ("TCKRSYMB","SYMBOL","FINSYMBOL","SCRIP")), None)
    if not sym_col:
        raise ValueError(f"Cannot find symbol column. Available: {col}")

    # ── Instrument type ───────────────────────────────────────────────
    inst_col = next((c for c in col if c in ("FININSTRMTP","INSTRUMENT","INSTRM")), None)

    # ── Expiry date ───────────────────────────────────────────────────
    exp_col = next((c for c in col if c in ("XPRYDT","EXPIRY_DT","EXPIRYDATE")), None)

    # ── Settle price ──────────────────────────────────────────────────
    # Prefer settlement price (official EOD price used for M2M)
    settle_col = next((c for c in col if c in ("STTLMPRIC","SETTLE_PR","SETTLPRICE")), None)
    close_col  = next((c for c in col if c in ("CLSPRIC","CLOSE_PRICE","CLOSE")), None)
    price_col  = settle_col or close_col

    if not price_col:
        raise ValueError(f"Cannot find price column. Available: {col}")

    out = pd.DataFrame()
    out["symbol"]     = df[sym_col].astype(str).str.strip()
    out["instrument"] = df[inst_col].astype(str).str.strip() if inst_col else "UNKNOWN"
    out["expiry"]     = pd.to_datetime(df[exp_col], errors="coerce") if exp_col else pd.NaT
    out["settle"]     = pd.to_numeric(df[price_col].astype(str).str.replace(",",""), errors="coerce")
    out["close"]      = pd.to_numeric(df[close_col].astype(str).str.replace(",",""), errors="coerce") if close_col else out["settle"]
    out["bhav_date"]  = bhav_date

    return out


def extract_futures_from_bhavcopy(bhav_df: "pd.DataFrame",
                                   symbols: "list[str]",
                                   debug: bool = False) -> "dict[str, dict]":
    """
    Extract near-month and far-month futures settle prices for given symbols
    from a normalised bhavcopy DataFrame.

    Returns: { "LTIM": {"near_fut": 4495.0, "far_fut": 4360.5, "near_expiry": date, "far_expiry": date}, ... }
    """
    # ── Step 1: Identify futures rows ─────────────────────────────────────────
    # NSE bhavcopy instrument type codes (varies across format versions):
    #   New format: "FUTSTK" = stock futures, "FUTIDX" = index futures
    #   Old format: "STF"    = stock futures, "IDF"    = index futures
    # Always filter for rows where instrument contains "FUT" to be safe.
    inst_vals = bhav_df["instrument"].str.upper().str.strip()

    # Prefer explicit known codes
    STK_FUT_CODES = {"FUTSTK", "STF", "FUTSTK ", "STF "}
    futs_df = bhav_df[inst_vals.isin(STK_FUT_CODES)].copy()

    # Broader fallback — any instrument containing FUT but not IDX/OPT
    if futs_df.empty:
        mask = inst_vals.str.contains("FUT", na=False) & \
               ~inst_vals.str.contains("IDX|OPT|CE|PE", na=False)
        futs_df = bhav_df[mask].copy()

    # Build symbol set early — needed by both debug block and find_sym_rows
    bhav_syms = set(futs_df["symbol"].str.strip().str.upper().unique())

    if debug:
        print(f"\n{'─'*60}")
        print(f"[DEBUG] Instrument codes in bhavcopy:")
        for code, cnt in bhav_df["instrument"].value_counts().items():
            print(f"  {str(code):<20} {cnt:>6} rows")
        print(f"\n[DEBUG] Futures rows matched: {len(futs_df)}")
        print(f"\n[DEBUG] All unique stock symbols in futures ({len(bhav_syms)}):")
        for i, s in enumerate(sorted(bhav_syms)):
            print(f"  {s:<20}", end="" if (i+1)%4 else "\n")
        print(f"\n{'─'*60}\n")

    # ── Step 2: Build reverse alias map ──────────────────────────────────────
    # NSE bhavcopy uses exact NSE symbols. Our FO_SYMBOLS list uses the same
    # names, BUT some stocks have different bhavcopy names.
    # Known mismatches (NSE ticker → bhavcopy name):
    BHAV_ALIAS = {
        # ── Confirmed bhavcopy name → our FO_SYMBOLS name ──────────────────────
        # LTIM: NSE uses 'LTM' in bhavcopy (confirmed via debug)
        "LTM":          "LTIM",
        # M&MFIN: appears as M&MFIN in bhavcopy (same, should match directly)
        # ICICIPRULIFE: bhavcopy uses ICICIPLUI
        "ICICIPLUI":    "ICICIPRULIFE",
        # WIPRO: confirmed present in bhavcopy as WIPRO — direct match should work
        # ZOMATO: listed as ETERNAL in bhavcopy (company renamed)
        "ETERNAL":      "ZOMATO",
        # PERSISTENT: present in bhavcopy as PERSISTENT — check direct match
        # TATAMOTORS: in bhavcopy? — cross-ref shows missing, may be TATAMOT
        # APOLLOHOSP: missing from bhavcopy — may have been removed from F&O
        # Stocks confirmed GENUINELY not in F&O bhavcopy (removed/expired):
        # AARTIIND, ABFRL, ACC, APOLLOTYRE, BALKRISIND, BERGEPAINT, BIRLASOFT,
        # CESC, CHAMBLFERT, DEEPAKNITR, EMAMILTD, GLAND, IGL, IIFL, INDIAMART,
        # IPCALAB, IRB, LALPATHLAB, LTTS, M&MFIN, MCDOWELL-N, MGL, MRF,
        # PVRINOX, RADICO, RPOWER, SJVN, STAR, TATACHEM, TATACOMM, UBL,
        # VOLTAS, WHIRLPOOL — these will correctly show N/A
    }
    # Also build reverse: our_symbol → possible bhavcopy names to try
    REVERSE_ALIAS: dict[str, list[str]] = {}
    for bhav_name, our_name in BHAV_ALIAS.items():
        REVERSE_ALIAS.setdefault(our_name, []).append(bhav_name)

    def find_sym_rows(sym: str) -> "pd.DataFrame":
        """Try multiple name variants to find matching rows."""
        # 1. Exact match
        df = futs_df[futs_df["symbol"].str.strip() == sym]
        if not df.empty: return df

        # 2. Case-insensitive
        df = futs_df[futs_df["symbol"].str.strip().str.upper() == sym.upper()]
        if not df.empty: return df

        # 3. Known aliases
        for alias in REVERSE_ALIAS.get(sym, []):
            df = futs_df[futs_df["symbol"].str.strip().str.upper() == alias.upper()]
            if not df.empty:
                if debug: print(f"[debug] {sym} → matched via alias '{alias}'")
                return df

        # 4. Prefix match (e.g. AARTIIND → AARTIINDS)
        prefix_matches = [s for s in bhav_syms if s.startswith(sym[:6])]
        for pm in prefix_matches:
            df = futs_df[futs_df["symbol"].str.strip().str.upper() == pm]
            if not df.empty:
                if debug: print(f"[debug] {sym} → prefix match '{pm}'")
                return df

        return pd.DataFrame()

    # ── Step 3: Extract near/far for each symbol ──────────────────────────────
    results = {}
    missing = []

    for sym in symbols:
        sym_df = find_sym_rows(sym)

        if sym_df.empty:
            results[sym] = {"near_fut": None, "far_fut": None,
                            "near_expiry": None, "far_expiry": None}
            missing.append(sym)
            continue

        # Sort by expiry ascending — [0]=near month, [1]=far month
        sym_df = sym_df.dropna(subset=["expiry"]).sort_values("expiry")
        rows   = sym_df.reset_index(drop=True)

        def safe_price(row_idx):
            if row_idx >= len(rows): return None
            v = rows.loc[row_idx, "settle"]
            return round(float(v), 2) if pd.notna(v) else None

        def safe_expiry(row_idx):
            if row_idx >= len(rows): return None
            v = rows.loc[row_idx, "expiry"]
            return v.date() if pd.notna(v) else None

        results[sym] = {
            "near_fut":    safe_price(0),
            "far_fut":     safe_price(1),
            "near_expiry": safe_expiry(0),
            "far_expiry":  safe_expiry(1),
        }

    if missing and debug:
        print(f"\n[debug] {len(missing)} symbols not found in bhavcopy futures:")
        print(f"  {', '.join(missing)}")

    # ── Step 4: Print missing summary (always, not just debug) ───────────────
    if missing:
        print(f"\n  ⚠  {len(missing)} symbols had no futures in bhavcopy "
              f"(expired/not in F&O/name mismatch):")
        # Group into rows of 8 for readability
        for i in range(0, len(missing), 8):
            print(f"     {', '.join(missing[i:i+8])}")
        print(f"\n  Tip: Run with --futures-debug to see all bhavcopy instrument codes")
        print(f"       and which names are present — helps identify mismatches.\n")

    return results


# ── Nifty 50 universe (50 stocks, Mar 2026) ───────────────────────────────────
# Matches TradeEasy's SYMBOLS.stocks — use this for Momentum strategy backtesting
NIFTY50_SYMBOLS = [
    "ADANIENT",   "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL",        "BHARTIARTL",
    "CIPLA",      "COALINDIA",  "DIVISLAB",   "DRREDDY",    "EICHERMOT",
    "ETERNAL",    "GRASIM",     "HCLTECH",    "HDFCBANK",   "HDFCLIFE",
    "HINDALCO",   "HINDUNILVR", "ICICIBANK",  "INDIGO",     "INFY",
    "ITC",        "JIOFIN",     "JSWSTEEL",   "KOTAKBANK",  "LT",
    "M&M",        "MARUTI",     "MAXHEALTH",  "NESTLEIND",  "NTPC",
    "ONGC",       "POWERGRID",  "RELIANCE",   "SBILIFE",    "SBIN",
    "SHRIRAMFIN", "SUNPHARMA",  "TATACONSUM", "TATASTEEL",  "TCS",
    "TECHM",      "TITAN",      "TRENT",      "ULTRACEMCO", "WIPRO",
]

# ── Full F&O universe — 186 symbols, exact match with app.py ALL_SYMBOLS ──────
# 12 indices (NIFTY50 + BANKNIFTY + FINNIFTY + MIDCPNIFTY + 8 sector) + 174 F&O stocks
# Symbol names here = CSV filenames = symbol IDs used in TradeEdge.html
# Yahoo tickers resolved via YAHOO_TICKER_MAP above (e.g. ZOMATO → ETERNAL.NS)
FO_SYMBOLS = [
    "NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    "CNXIT", "CNXAUTO", "CNXPHARMA", "CNXENERGY", "CNXMETAL", "CNXFMCG", "CNXINFRA", "CNXCONSUM",
    # ── F&O Stocks (active as of Apr 2026 bhavcopy) ────────────────────────────
    "ABB", "ABCAPITAL", "ADANIENT", "ADANIENSOL", "ADANIGREEN",
    "ADANIPORTS", "ADANIPOWER", "ALKEM", "AMBUJACEM", "AMBER", "ANGELONE", "APLAPOLLO",
    "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUBANK", "AUROPHARMA",
    "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BANDHANBNK",
    "BANKBARODA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL", "BHEL", "BIOCON",
    "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE",
    "CAMS", "CANBK", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA",
    "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR", "CROMPTON", "CUMMINSIND",
    "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DMART", "DRREDDY",
    "GAIL", "GLENMARK", "GMRAIRPORT", "GODREJCP", "GODREJPROP", "GRASIM",
    "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HUDCO", "HYUNDAI",
    "ICICIBANK", "ICICIGI", "ICICIPRULIFE", "IDEA", "IDFCFIRSTB", "IEX",
    "INDHOTEL", "INDIGO", "INDIANB", "INDUSINDBK", "INDUSTOWER", "INFY", "INOXWIND", "IOC",
    "IREDA", "IRFC", "ITC",
    "JINDALSTEL", "JIOFIN", "JUBLFOOD", "JSWENERGY", "JSWSTEEL",
    "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH",
    "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTIM", "LUPIN",
    "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK",
    "MCX", "MFSL", "MOTHERSON", "MOTILALOFS", "MPHASIS", "MUTHOOTFIN",
    "NAM-INDIA", "NATIONALUM", "NAUKRI", "NBCC", "NESTLEIND", "NHPC",
    "NMDC", "NTPC", "NUVAMA", "NYKAA", "OBEROIRLTY", "OFSS", "OIL", "ONGC",
    "PAGEIND", "PATANJALI", "PAYTM", "PFC", "PGEL", "PHOENIXLTD",
    "PIDILITIND", "PIIND", "PNB", "PNBHOUSING", "POLICYBZR", "POLYCAB",
    "POWERINDIA", "POWERGRID", "PREMIERENE", "PRESTIGE", "PERSISTENT",
    "RBLBANK", "RECLTD", "RELIANCE", "RVNL",
    "SAIL", "SAMMAANCAP", "SBICARD", "SBILIFE", "SBIN", "SHREECEM", "SHRIRAMFIN",
    "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUPREMEIND", "SUNPHARMA", "SUZLON", "SWIGGY",
    "TATACONSUM", "TATAELXSI", "TATAPOWER", "TATASTEEL", "TATATECH", "TCS",
    "TECHM", "TIINDIA", "TITAN", "TMPV", "TORNTPHARM", "TORNTPOWER", "TRENT", "TVSMOTOR",
    "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL",
    "VBL", "VEDL", "VMM", "WIPRO", "ZOMATO", "ZYDUSLIFE",
]


# ── Fetch one symbol ──────────────────────────────────────────────────────────
def _normalise_yf_df(df: "pd.DataFrame", ticker_str: str) -> "pd.DataFrame":
    """
    Normalise a yfinance DataFrame (from either .download() or .history()) into
    the standard 6-column format: open, high, low, close, adj_close, volume.

    Handles:
      - MultiIndex columns like ('Adj Close', '^NSEI')  [yfinance 0.2.x download]
      - Flat columns like 'Adj Close', 'Open' ...       [yfinance history()]
      - Both auto_adjust=True (no Adj Close col) and False
    """
    # ── Flatten MultiIndex ────────────────────────────────────────────────────
    if isinstance(df.columns, pd.MultiIndex):
        # Join levels, e.g. ('Adj Close', '^NSEI') → 'adj close ^nsei'
        df.columns = [" ".join(str(c) for c in col).strip().lower() for col in df.columns]
        rename = {}
        for col in df.columns:
            cl = col.lower()
            if cl.startswith("adj close"):   rename[col] = "adj_close"
            elif cl.startswith("open"):      rename[col] = "open"
            elif cl.startswith("high"):      rename[col] = "high"
            elif cl.startswith("low"):       rename[col] = "low"
            elif cl.startswith("close"):     rename[col] = "close"
            elif cl.startswith("volume"):    rename[col] = "volume"
        df = df.rename(columns=rename)
    else:
        # Flat columns — lowercase + underscore
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # ── Ensure adj_close exists ───────────────────────────────────────────────
    # history(auto_adjust=True) has no 'adj close' — the Close IS already adjusted
    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"].copy()

    # ── Standardise index → plain date strings (IST timezone) ────────────────
    # Yahoo returns timestamps in UTC. Convert to IST (UTC+5:30) before
    # extracting date — otherwise a 10-Jun IST candle stored at midnight UTC
    # would be labelled 2026-06-11 instead of 2026-06-10.
    df.index.name = "date"
    idx = pd.to_datetime(df.index)
    if idx.tz is not None:
        # Already timezone-aware — convert to IST
        idx = idx.tz_convert("Asia/Kolkata")
    else:
        # Timezone-naive UTC timestamps from yfinance — localize then convert
        idx = idx.tz_localize("UTC").tz_convert("Asia/Kolkata")
    df.index = idx.strftime("%Y-%m-%d")

    # ── Select & clean ────────────────────────────────────────────────────────
    df = df[["open", "high", "low", "close", "adj_close", "volume"]].copy()
    df = df.dropna(subset=["open", "close"])
    df = df[df["open"] > 0].round(2)
    return df


def fetch_symbol(symbol: str, start: str, end: str, retries: int = 3):
    """
    Download adjusted OHLC for one symbol using yf.Ticker.history().

    Uses Ticker.history() (more reliable than yf.download() for single symbols
    in yfinance 0.2.x) with auto_adjust=True so Close = adjusted close.
    adj_close column is set equal to close (already adjusted).

    Falls back to .BO exchange if .NS fails.
    Returns DataFrame or None on failure.
    """
    primary = get_yf_ticker(symbol)
    candidates = [primary]
    if primary.endswith(".NS"):
        candidates.append(primary.replace(".NS", ".BO"))

    for ticker_str in candidates:
        for attempt in range(retries):
            try:
                with _suppress_yf_noise():
                    tk = yf.Ticker(ticker_str)
                    df = tk.history(
                        start=start,
                        end=end,
                        interval="1d",
                        auto_adjust=True,   # Close = adjusted; no separate Adj Close col
                        actions=False,
                        timeout=20,
                    )

                if df is None or df.empty:
                    raise ValueError("empty")

                df = _normalise_yf_df(df, ticker_str)

                if df.empty:
                    raise ValueError("empty after normalise")

                if ticker_str != primary:
                    print(f" (fallback: {ticker_str})", end="")
                return df

            except Exception as e:
                err_str = str(e).lower()
                is_rl = any(k in err_str for k in
                            ['rate limit', 'too many', '429', 'yfratelimit', 'rate_limit'])
                if attempt < retries - 1:
                    wait = 60 if is_rl else 2 ** attempt
                    label = 'rate limited — waiting 60s' if is_rl else f'retry {attempt+2}/{retries}'
                    print(f"\n    {label} ({type(e).__name__}: {str(e)[:60]})...",
                          end="", flush=True)
                    time.sleep(wait)
                else:
                    print(f"\n    gave up ({type(e).__name__}: {str(e)[:80]})",
                          end="", flush=True)
    return None


# ── Futures scanner ───────────────────────────────────────────────────────────

def fetch_spot_price(symbol: str) -> float | None:
    """Fetch latest spot close from Yahoo Finance."""
    tk = get_yf_ticker(symbol)
    for attempt in range(2):
        try:
            raw = yf.Ticker(tk).history(period="5d", interval="1d",
                                        auto_adjust=True, actions=False)
            if raw is None or raw.empty:
                return None
            df = _normalise_yf_df(raw, tk)
            closes = df["close"].dropna()
            return round(float(closes.iloc[-1]), 2) if len(closes) else None
        except Exception:
            if attempt == 0:
                time.sleep(1)
    return None


def fetch_futures_batch(symbols: list[str], delay: float = 0.4,
                        outdir: "Path | None" = None,
                        debug: bool = False,
                        cache_dir: "Path | None" = None) -> list[dict]:
    """
    Scan F&O symbols for backwardation using NSE EOD bhavcopy.

    Flow:
      1. Download today's (or most recent) F&O bhavcopy ZIP in one shot
      2. Extract near/far futures settle prices for all symbols at once
      3. Fetch spot prices from Yahoo Finance per symbol
      4. Compute discount%, spread%, backwardation flag
      5. Print live table + optionally save FUTURES_SNAPSHOT.csv
    """
    from datetime import date

    # ── Step 1: Download bhavcopy ──────────────────────────────────────────────
    print("  Downloading NSE F&O bhavcopy...", flush=True)
    bhav_df = download_fo_bhavcopy(cache_dir=cache_dir)

    if bhav_df is None:
        print("\n  ✗ Bhavcopy unavailable. Possible reasons:")
        print("    - Market holiday / weekend (no bhavcopy published)")
        print("    - NSE server issue")
        print("    - Run during market hours — bhavcopy is published after 6 PM IST")
        print("    Use --futures-csv to load a manual CSV instead.\n")
        return []

    # ── Step 2: Extract futures prices for all symbols at once ────────────────
    # Filter indices out — bhavcopy has them as index futures (NIFTY, BANKNIFTY)
    # For stock backwardation scan we only want STF (stock futures)
    print(f"  Extracting futures from bhavcopy ({len(bhav_df):,} rows)...", flush=True)
    fut_map = extract_futures_from_bhavcopy(bhav_df, symbols, debug=debug)

    bhav_date = bhav_df["bhav_date"].iloc[0]
    print(f"  Bhavcopy date: {bhav_date.strftime('%d-%b-%Y')}\n")

    # ── Step 3: Fetch spot prices + compute metrics ────────────────────────────
    results  = []
    ok = partial = failed = 0

    print(f"  {'SYM':<16} {'SPOT':>10}  {'NEAR FUT':>10}  {'FAR FUT':>10}  "
          f"{'NEAR%':>7}  {'FAR%':>7}  {'SPREAD%':>8}  STATUS")
    print(f"  {'─'*16} {'─'*10}  {'─'*10}  {'─'*10}  "
          f"{'─'*7}  {'─'*7}  {'─'*8}  {'─'*20}")

    for i, sym in enumerate(symbols, 1):
        spot     = fetch_spot_price(sym)
        fut_data = fut_map.get(sym, {})
        near_fut = fut_data.get("near_fut")
        far_fut  = fut_data.get("far_fut")

        near_disc = round((near_fut - spot) / spot * 100, 3) if (spot and near_fut) else None
        far_disc  = round((far_fut  - spot) / spot * 100, 3) if (spot and far_fut)  else None
        spread    = round((far_fut - near_fut) / near_fut * 100, 3) if (near_fut and far_fut) else None
        backward  = bool(spot and near_fut and far_fut and spot > near_fut > far_fut)

        r = {
            "symbol":        sym,
            "spot":          spot,
            "near_fut":      near_fut,
            "far_fut":       far_fut,
            "near_disc_pct": near_disc,
            "far_disc_pct":  far_disc,
            "spread_pct":    spread,
            "backwardation": backward,
            "near_expiry":   str(fut_data.get("near_expiry") or ""),
            "far_expiry":    str(fut_data.get("far_expiry")  or ""),
            "bhav_date":     str(bhav_date),
            "error":         None if spot else "No spot (Yahoo)",
        }
        results.append(r)

        if not spot:
            status = "✗ no spot"; failed += 1
        elif near_fut and far_fut:
            status = "◀ BACKWARDATION" if backward else "  contango"; ok += 1
        else:
            status = "~ no fut in bhav"; partial += 1

        s_s  = f"{spot:>10.2f}"     if spot     else f"{'—':>10}"
        nf_s = f"{near_fut:>10.2f}" if near_fut else f"{'N/A':>10}"
        ff_s = f"{far_fut:>10.2f}"  if far_fut  else f"{'N/A':>10}"
        nd_s = f"{near_disc:>+7.2f}%" if near_disc is not None else f"{'—':>7} "
        fd_s = f"{far_disc:>+7.2f}%"  if far_disc  is not None else f"{'—':>7} "
        sp_s = f"{spread:>+8.2f}%"    if spread    is not None else f"{'—':>8} "

        print(f"  [{i:>3}/{len(symbols)}] {sym:<12} {s_s}  {nf_s}  {ff_s}  "
              f"{nd_s}  {fd_s}  {sp_s}  {status}")

        if i < len(symbols):
            time.sleep(delay)

    # ── Save FUTURES_SNAPSHOT.csv ──────────────────────────────────────────────
    if outdir:
        snap_path = outdir / "FUTURES_SNAPSHOT.csv"
        import csv as csvmod
        fieldnames = ["symbol", "bhav_date", "spot", "near_fut", "far_fut",
                      "near_disc_pct", "far_disc_pct", "spread_pct",
                      "backwardation", "near_expiry", "far_expiry", "error"]
        with open(snap_path, "w", newline="") as f:
            w = csvmod.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(results)
        print(f"\n  Futures snapshot saved → {snap_path}")

    # ── Summary ────────────────────────────────────────────────────────────────
    back_list = [r for r in results if r["backwardation"]]
    print(f"\n{'='*72}")
    print(f"  ✓ {ok} full  |  ~ {partial} partial  |  ✗ {failed} failed")
    print(f"  Bhavcopy date : {bhav_date.strftime('%d-%b-%Y')}")
    print(f"  Backwardation : {len(back_list)} / {len(symbols)} stocks")
    if back_list:
        print(f"\n  ◀ BACKWARDATION CANDIDATES:")
        for r in back_list:
            print(f"    {r['symbol']:<16}  spot={r['spot']:>10.2f}  "
                  f"near={r['near_fut']:>10.2f} ({r['near_disc_pct']:>+.2f}%)  "
                  f"far={r['far_fut']:>10.2f} ({r['far_disc_pct']:>+.2f}%)  "
                  f"spread={r['spread_pct']:>+.2f}%  "
                  f"[near exp: {r['near_expiry']}]")
    print(f"{'='*72}\n")

    return results


# ── CSV helpers ───────────────────────────────────────────────────────────────
def load_csv(path: Path):
    try:
        df = pd.read_csv(path, index_col=0)
        df.index = df.index.astype(str)
        return df
    except Exception:
        return None

def last_csv_date(path: Path):
    df = load_csv(path)
    return str(df.index[-1]) if (df is not None and not df.empty) else None

def merge_into_csv(path: Path, new_df) -> tuple:
    existing = load_csv(path)
    if existing is None or existing.empty:
        new_df.to_csv(path)
        return len(new_df), len(new_df)
    prev_len = len(existing)
    combined = pd.concat([existing, new_df])
    combined = combined[~combined.index.duplicated(keep='last')].sort_index()
    combined.to_csv(path)
    return len(combined) - prev_len, len(combined)

def is_up_to_date(path: Path) -> bool:
    if not path.exists():
        return False
    return datetime.fromtimestamp(path.stat().st_mtime).date() >= datetime.today().date()

def print_summary(outdir: Path) -> None:
    files = sorted(outdir.glob("*.csv"))
    if not files:
        print("  No CSV files found in", outdir.resolve())
        return
    print(f"\n  {'SYMBOL':<18} {'ROWS':>6}  {'FROM':<12} {'TO'}")
    print(f"  {'─'*18} {'─'*6}  {'─'*12} {'─'*12}")
    for f in files:
        try:
            df = pd.read_csv(f, index_col=0)
            print(f"  {f.stem:<18} {len(df):>6}  {df.index[0]:<12} {df.index[-1]}")
        except Exception:
            print(f"  {f.stem:<18}  (unreadable)")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="TradeEasy — fetch adjusted OHLC from Yahoo Finance"
    )
    parser.add_argument("--fo",      action="store_true",
                        help="Full F&O universe instead of Nifty 50")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Specific symbols to fetch (overrides --fo)")
    parser.add_argument("--from",    dest="start", default=None,
                        help="Start date YYYY-MM-DD (default: 2 years back)")
    parser.add_argument("--to",      dest="end",   default=None)
    parser.add_argument("--outdir",  default="./tradeedge_data")
    parser.add_argument("--merge",   action="store_true",
                        help="Incremental: append new rows only")
    parser.add_argument("--days",    type=int, default=None,
                        help="With --merge: fetch last N calendar days")
    parser.add_argument("--update",  action="store_true",
                        help="Skip symbols already fetched today")
    parser.add_argument("--delay",   type=float, default=1.5,
                        help="Seconds between requests (default 0.5)")
    parser.add_argument("--sectors", action="store_true",
                        help="(deprecated — sector indices now included in --fo)")
    parser.add_argument("--summary", action="store_true",
                        help="Show CSV file table and exit")
    parser.add_argument("--futures", action="store_true",
                        help="Scan F&O stocks for futures backwardation via NSE API")
    parser.add_argument("--futures-save", action="store_true",
                        help="With --futures: save FUTURES_SNAPSHOT.csv to outdir")
    parser.add_argument("--futures-debug", action="store_true",
                        help="With --futures: print all bhavcopy instrument codes and symbol matches")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.summary:
        print_summary(outdir)
        return

    # ── FUTURES MODE ──────────────────────────────────────────────────────────
    if args.futures:
        if args.symbols:
            fut_syms = args.symbols
            label    = f"custom ({len(fut_syms)} symbols)"
        elif args.fo:
            # Exclude indices — only stocks have futures on NSE
            indices  = {"NIFTY50","BANKNIFTY","FINNIFTY","MIDCPNIFTY",
                        "CNXIT","CNXAUTO","CNXPHARMA","CNXENERGY",
                        "CNXMETAL","CNXFMCG","CNXINFRA","CNXCONSUM"}
            fut_syms = [s for s in FO_SYMBOLS if s not in indices]
            label    = f"F&O stocks ({len(fut_syms)} symbols)"
        else:
            fut_syms = NIFTY50_SYMBOLS
            label    = f"Nifty 50 ({len(NIFTY50_SYMBOLS)} symbols)"

        print(f"\n{'='*72}")
        print(f"  TradeEdge Futures Scanner  —  {label}")
        print(f"  Spot   : Yahoo Finance")
        print(f"  Futures: NSE F&O Bhavcopy (EOD settle prices)")
        print(f"  Signal : Backwardation — Spot > Near Fut > Far Fut")
        save_str = str(outdir.resolve()) if args.futures_save else "console only (use --futures-save to export)"
        print(f"  Output : {save_str}")
        print(f"{'='*72}")

        save_dir = outdir if args.futures_save else None
        # Bhavcopy cache: save in tradeedge_data parent (same folder as the script)
        bhav_cache = outdir.parent / "tradeedge_bhav"
        fetch_futures_batch(fut_syms, delay=args.delay, outdir=save_dir,
                            debug=args.futures_debug, cache_dir=bhav_cache)
        return

    # Resolve symbol list — flags can be combined
    if args.symbols:
        symbols = args.symbols
        label   = f"custom ({len(symbols)} symbols)"
    else:
        symbols = []
        parts   = []
        if args.fo:
            symbols += FO_SYMBOLS
            parts.append(f"F&O + indices ({len(FO_SYMBOLS)} symbols — matches TradeEdge.html)")
        if args.sectors:
            print("Note: --sectors is no longer needed; sector indices (CNXIT, CNXAUTO, etc.) are included in --fo")
        if not symbols:
            symbols = NIFTY50_SYMBOLS
            parts   = [f"Nifty 50 ({len(NIFTY50_SYMBOLS)} symbols)  ← default"]
        label = " + ".join(parts)

    today      = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date   = args.end or today
    start_2yr  = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")
    fetch_mode = "MERGE (incremental)" if args.merge else "FULL FETCH"

    print(f"\n{'='*62}")
    print(f"  TradeEasy Fetcher  —  {label}")
    print(f"  Mode    : {fetch_mode}")
    print(f"  Prices  : Yahoo Finance ADJUSTED CLOSE (auto_adjust=True)")
    if not args.merge:
        sd = args.start or start_2yr
        print(f"  Range   : {sd}  →  {end_date}")
    else:
        if args.days:
            print(f"  Merge   : last {args.days} days  →  {end_date}")
        else:
            print(f"  Merge   : since last CSV date  →  {end_date}")
    print(f"  Output  : {outdir.resolve()}")
    print(f"{'='*62}\n")

    ok = skipped = 0
    failed: list[str] = []
    total_rows = 0

    for i, sym in enumerate(symbols, 1):
        csv_path = outdir / f"{sym}.csv"
        prefix   = f"  [{i:>3}/{len(symbols)}] {sym:<16}"

        # ── MERGE ─────────────────────────────────────────────────────────────
        if args.merge:
            if args.days:
                fetch_start = (datetime.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")
            else:
                last = last_csv_date(csv_path)
                if last:
                    nxt = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    if nxt > end_date:
                        print(f"{prefix} up to date  ({last})")
                        skipped += 1
                        continue
                    fetch_start = nxt
                else:
                    fetch_start = start_2yr

            print(f"{prefix} merging {fetch_start} → {end_date} ...", end="", flush=True)
            df = fetch_symbol(sym, fetch_start, end_date)
            if df is not None and not df.empty:
                added, total = merge_into_csv(csv_path, df)
                total_rows += added
                print(f"  +{added} rows  (total {total})")
                ok += 1
            elif df is not None:
                print("  already up to date")
                skipped += 1
            else:
                print("  ✗ FAILED")
                failed.append(sym)

        # ── FULL FETCH ────────────────────────────────────────────────────────
        else:
            if args.update and is_up_to_date(csv_path):
                print(f"{prefix} skipped (already today)")
                skipped += 1
                continue

            sd = args.start or start_2yr
            print(f"{prefix} fetching ...", end="", flush=True)
            df = fetch_symbol(sym, sd, end_date)
            if df is not None and not df.empty:
                df.to_csv(csv_path)
                total_rows += len(df)
                print(f"  ✓  {len(df)} rows  [{df.index[0]} → {df.index[-1]}]")
                ok += 1
            else:
                print("  ✗ FAILED")
                failed.append(sym)

        if i < len(symbols):
            time.sleep(args.delay)

    # ── Export sector map JSON (always) ──────────────────────────────────────
    import json
    sector_map_path = outdir / "SECTOR_MAP.json"
    with open(sector_map_path, "w") as f:
        json.dump(STOCK_SECTOR_MAP, f, indent=2)
    print(f"\n  Sector map saved → {sector_map_path}")

    # ── Result ────────────────────────────────────────────────────────────────
    print(f"\n{'='*62}")
    print(f"  ✓ {ok} {'merged' if args.merge else 'fetched'}  |  "
          f"{skipped} skipped  |  {len(failed)} failed")
    print(f"  Rows written: {total_rows:,}")
    if failed:
        print(f"\n  ✗ Failed: {', '.join(failed)}")
        print(f"  Retry:  python tradeedge_fetch.py --symbols {' '.join(failed)}")
    print(f"\n  Load in TradeEasy.html via 📂 Load Data Folder")
    print(f"  Select:  {outdir.resolve()}")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
