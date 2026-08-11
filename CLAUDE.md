# TradeEdge — Claude Code Context

## Project Overview
Single-file HTML trading backtester and live scanner for NSE F&O stocks (Indian equities).

**Files:**
- `TradeEdge.html` — entire frontend: backtest engine, live scanner, UI (ALWAYS this exact filename, never any variant)
- `app.py` — Flask API deployed on Render free tier; fetches OHLC data from Yahoo Finance via `yfinance`, and hosts the Gap automation routes (`/gap-orders/*`, `/kite/*`, `/gap-settings`, `/gap-scan`)
- `tradeedge_fetch.py` — standalone Python data pipeline for local CSV exports
- `kite_orders.py` — Zerodha Kite order lifecycle for the Overnight Gap automation: entry/exit, Paper vs Live mode, backfill, contract resolution. See "Live Trading Automation" below.
- `gap_scan.py` — headless Python port of the Overnight Gap signal logic (mirrors `backtest()` in TradeEdge.html), used by both live scanning and backfill so entry/exit decisions match the backtest exactly
- `telegram_notify.py` — Telegram alerts for the Gap automation (per-event + daily digest); needs `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` env vars
- `cron_fetch.py` — Render Cron Job entry point (15:15 IST) that runs `tradeedge_fetch.py --fo --merge`
- `push_incr_data.py` — pushes updated CSVs to GitHub's `data` branch after a fetch; confirms the live symbol universe is **211** total (`--all` flag pushes "all 211 CSVs") — the "186 total" figure elsewhere in this doc and in `ALL_SYMBOLS`/`SYMBOLS` is stale and needs a recount before being trusted

**Note:** the Watchlist / Trigger Tracker feature referenced in older docs was intentionally removed — do not re-add it.

---

## Architecture

### Data Flow
```
TradeEdge.html
  → warmup ping → TRADEEDGE_API_URL/
  → batched fetch → TRADEEDGE_API_URL/sync-today?offset=X&limit=40&days=10
      → app.py (Render free tier, Flask)
          → yfinance (Yahoo Finance)
              → returns OHLC JSON per symbol
```

### Key Rules
- `app.py` `ALL_SYMBOLS` list (186 total) must **exactly match** `TradeEdge.html` `SYMBOLS` list — same symbols, same order
- Batch size = 10 symbols per request
- Fetch timeout = 150s per batch (HTML) / 130s safety cutoff (app.py)
- HTML does a warmup ping before batching (Render cold start is ~30s)
- Render free tier: server sleeps after 15min of inactivity → always expect cold start

### Symbol List (186 total = 12 indices + 174 F&O stocks)

**Indices (12):**
`NIFTY50, BANKNIFTY, FINNIFTY, MIDCPNIFTY, CNXIT, CNXAUTO, CNXPHARMA, CNXENERGY, CNXMETAL, CNXFMCG, CNXINFRA, CNXCONSUM`

**F&O Stocks (174):**
`AARTIIND, ABB, ABCAPITAL, ABFRL, ACC, ADANIENT, ADANIGREEN, ADANIPORTS, ALKEM, AMBUJACEM, AMBER, APOLLOHOSP, APOLLOTYRE, ASHOKLEY, ASIANPAINT, AUBANK, AUROPHARMA, BAJAJ-AUTO, BAJAJFINSV, BAJFINANCE, BALKRISIND, BANDHANBNK, BANKBARODA, BEL, BERGEPAINT, BHARTIARTL, BHEL, BIOCON, BIRLASOFT, BOSCHLTD, BPCL, BRITANNIA, BSE, CAMS, CANBK, CESC, CHAMBLFERT, CHOLAFIN, CIPLA, COALINDIA, COFORGE, COLPAL, CONCOR, COROMANDEL, CUMMINSIND, DABUR, DEEPAKNITR, DELHIVERY, DMART, DIVISLAB, DIXON, DLF, DRREDDY, EICHERMOT, EMAMILTD, EXIDEIND, FEDERALBNK, GAIL, GLAND, GODREJCP, GODREJPROP, GRASIM, HAL, HAVELLS, HCLTECH, HDFCBANK, HDFCLIFE, HEROMOTOCO, HINDALCO, HINDUNILVR, HUDCO, ICICIBANK, ICICIGI, ICICIPRULIFE, IDEA, IDFCFIRSTB, IGL, IIFL, INDHOTEL, INDIAMART, INDIGO, INDUSINDBK, IOC, IPCALAB, IRB, IRFC, ITC, JINDALSTEL, JUBLFOOD, JSWSTEEL, KALYANKJIL, KOTAKBANK, KPITTECH, LALPATHLAB, LAURUSLABS, LICHSGFIN, LT, LTIM, LTTS, LUPIN, M&M, M&MFIN, MANAPPURAM, MARICO, MARUTI, MCX, MCDOWELL-N, MGL, MOTHERSON, MPHASIS, MRF, MUTHOOTFIN, NATIONALUM, NAUKRI, NBCC, NESTLEIND, NHPC, NMDC, NTPC, NYKAA, OBEROIRLTY, OFSS, ONGC, PAYTM, PFC, PIDILITIND, PIIND, PNBHOUSING, POLICYBZR, POWERGRID, PRESTIGE, PERSISTENT, PNB, PVRINOX, RADICO, RBLBANK, RECLTD, RELIANCE, RPOWER, SAIL, SBICARD, SBILIFE, SBIN, SHREECEM, SIEMENS, SJVN, SRF, STAR, SUNPHARMA, SUZLON, TATACHEM, TATACOMM, TATACONSUM, TATAELXSI, TATAMOTORS, TATAPOWER, TATASTEEL, TCS, TECHM, TIINDIA, TITAN, TORNTPHARM, TORNTPOWER, TRENT, UBL, ULTRACEMCO, UNIONBANK, UPL, VBL, VEDL, VOLTAS, WHIRLPOOL, WIPRO, ZOMATO`

**Yahoo Finance ticker map (special cases — applies to both app.py and TradeEdge.html):**
```python
# Sector indices
"CNXIT"→"^CNXIT", "CNXAUTO"→"^CNXAUTO", "CNXPHARMA"→"^CNXPHARMA",
"CNXENERGY"→"^CNXENERGY", "CNXMETAL"→"^CNXMETAL", "CNXFMCG"→"^CNXFMCG",
"CNXINFRA"→"^CNXINFRA", "CNXCONSUM"→"^CNXCONSUM"
# Stocks with non-standard tickers
"M&M"→"M&M.NS", "BAJAJ-AUTO"→"BAJAJ-AUTO.NS",
"BIRLASOFT"→"BSOFT.NS", "DEEPAKNITR"→"DEEPAKNTR.NS",
"ICICIPRULIFE"→"ICICIPRULI.NS", "MCDOWELL-N"→"UNITDSPR.NS",
"TATAMOTORS"→"TMPV.NS", "ZOMATO"→"ETERNAL.NS"
# All others: append .NS  (e.g. RELIANCE → RELIANCE.NS)
# Major indices: NIFTY50→^NSEI, BANKNIFTY→^NSEBANK, FINNIFTY→NIFTY_FIN_SERVICE.NS, MIDCPNIFTY→^NSEMDCP50
```

---

## Backtest Strategies (3 active)

Strategy dispatch uses `STRATEGY_ENGINES`, `STRATEGY_DATE_IDS`, and `STRATEGY_PARAM_READERS` tables.

### 1. Overnight Gap (`overnight`)
**Engine:** `backtest()` (~line 5080)
- Signal: gap on D1 open vs prior close, within [minGap%, maxGap%]
- Entry: D1 close (trade with the gap, confirmed by close — requires close in gap's direction via strict `closeFilter`)
- Side: gap UP + closes green → LONG, gap DOWN + closes red → SHORT
- Exit options: D2 open (TP if moved in favour) or D2 close; SL via gap-fill or fixed %
- Multi-day hold supported (`tpType: 'pct'`)

### 2. Intraday Momentum (`intraday`)
**Engine:** `backtestIntraday()` (~line 5440)
- Signal: D1 close move vs prev close within [minMove%, maxMove%]
- Entry type toggle: D2 Open (default) or D1 Close (overnight hold into D2)
- SL/TP checked intraday via D2 High/Low, else exit at D2 close
- Overnight gap SL check on day 3+ for multi-day holds
- Direction locked to SHORT in defaults

### 3. Pullback (`pullback`)
**Engine:** `backtestPullback()` (~line 5296)
- Signal: weekly trend (HH/HL swing detection) + daily pullback ≥ dropPct% from rolling peak/trough
- Confirmation: waits N days for price to turn back in trend direction before entry
- Entry: next session open after confirmation candle
- SL: structural (trigger day's low for LONG, high for SHORT)
- TP: fixed % from entry; hold days timeout as fallback
- One position per symbol at a time (busyUntilIdx guard)
- OPEN outcome tracked for still-live positions (not counted in win rate)

**Note:** S&R (Support & Resistance) strategy was previously implemented and has since been removed. Do not re-add it without explicit instruction.

---

## Strategy Parameters & Defaults

Each strategy has its own param block in the config panel and its own `localStorage` key:
- `TE_OVERNIGHT_DEFAULTS` — overnight params
- `TE_INTRADAY_D2_DEFAULTS` / `TE_INTRADAY_D1_DEFAULTS` — intraday params (keyed by entry type)
- `TE_PULLBACK_DEFAULTS` — pullback params

Shared keys: `TE_CAPITAL`, `TE_WR_THRESHOLD`, `TE_PNL_THRESHOLD`

---

## UI Structure

### Desktop
- Config panel — collapsible (collapsed by default on load)
- Signal Filters panel — collapsible (collapsed by default)
- Strategy tabs: Overnight Gap | Intraday Momentum | Pullback (direction buttons + params per strategy)
- Results sections: Summary Cards → Equity Curve → Trade Analysis (Day-wise / Calendar / Monthly / All Trades / Symbol / Insights / Positions / Sweep)
- Live Scanner — EOD signal cards with WR badge, SL/TP levels, MTF alignment badge
- Automated Trades (`#msTradesTaken`, Gap strategy only) — real Kite paper/live trades, 4 sub-tabs: All Trades / Day-wise Trades / Calendar View (2 months/row) / Stockwise. See "Live Trading Automation" below.

### Mobile
- Full app-shell (`#mobileShell`), desktop layout hidden at ≤768px
- Header: logo + data info block (sym count + last date) + icon buttons (Load / Incr. Sync)
- Scan bar: strategy toggle pills → date + WR filter row → filter chips + Scan button
- Results area: scanner signal cards → mobile overlay for sync progress
- Settings slide-in panel: per-strategy params, capital, WR/PnL thresholds, ranking mode
- Trades tab: sub-tabs — All Trades / Day-wise / Monthly / Stockwise / Insights

### Calendar Views
- Day view: `renderCalendarYear()` — 7-col grid, month-by-month, trade badges + PnL per day
- Monthly view: `renderMonthlyView()` — table with W/L/SL counts and PnL% per month
- Both driven by `_tradeStats.byTradeDay`
- `buildCalendar()` called from `buildResults()` with `_calYear = null` to reset to latest year

---

## Key Functions Reference

| Function | Location | Purpose |
|---|---|---|
| `backtest()` | ~5080 | Overnight Gap engine |
| `backtestIntraday()` | ~5440 | Intraday Momentum engine |
| `backtestPullback()` | ~5296 | Pullback engine |
| `_simulatePullbackExit()` | ~5360 | Day-by-day exit simulation for Pullback |
| `computeTradeStats()` | ~10294 | Builds `_tradeStats` (byTradeDay, bySymbol, etc.) |
| `buildResults()` | ~8035 | Renders all result sections after backtest |
| `buildCalendar()` | ~9861 | Initialises calendar year and renders both views |
| `renderCalendarYear()` | ~9876 | Day-grid calendar render |
| `renderMonthlyView()` | ~9656 | Monthly summary table render |
| `_pnlColor/_pnlBg/_pnlSign` | ~9851 | PnL colour helpers (threshold 0.005 for flat) |
| `fetchOHLC()` | ~4170 | Per-symbol CORS proxy fetch (10 proxy attempts) |
| `quickSync()` | ~5835 | Incremental sync — CORS proxy loop, 5 retry rounds |
| `syncToday()` | ~5700 | Today Sync — batched Render API calls |
| `runScanner()` | ~6741 | Desktop EOD scanner |
| `runMobileScanner()` | ~11558 | Mobile scanner (auto-backtests if WR data missing) |
| `getYFTicker()` | ~4311 | Maps internal symbol ID → Yahoo Finance ticker |
| `computeMTFAlignment()` | ~4653 | Weekly+Daily trend alignment score |
| `loadTradesTaken()` / `renderTradesTaken()` | ~3430 / ~3803 | Fetch + render Automated Trades (desktop) |
| `_ttDayCardsHTML()` / `_ttCalendarBuild()` / `_ttStockwiseRows()` | ~3870 / ~3960 / ~4090 | Pure builders shared by desktop AND mobile Automated Trades sub-tabs |
| `renderMobLiveTrades()` | ~4181 | Mobile Auto Trades tab (strategy pills + sub-tab bar + stats + list) |

---

## Live Trading Automation (Kite Integration)

Overnight Gap only (no automation for Intraday Momentum or Pullback). Places real/simulated NSE F&O futures trades via Zerodha Kite Connect, driven by cron-job.org hitting Flask routes on `app.py`. Max 1 signal/day is hardcoded in `gap_scan.py` regardless of any UI setting.

### Cron wiring (all times IST)
- **`POST /gap-orders/enter`** — 3:15 PM on the signal day. Scans today via `gap_scan.scan_gap_signals()`, places entry + GTT-SL.
- **`POST /gap-orders/exit`** — 3:15 PM the next trading day. Closes whatever is still `status: "open"` via `kite_orders.close_open_positions()`.
- **`POST /gap-orders/daily-digest`** — ~3:45 PM, after both of the above. Sends the Telegram daily summary.
- `app.py` has the exact wiring order and timing documented in a comment above these three routes (~line 1866).

### Paper vs Live mode
- Per-position `mode` field (`"paper"` or `"live"`), read from `settings["tradingMode"]` at entry time — **not** a global switch, so toggling mid-flight never changes how an already-open position gets closed.
- Fail-safe default: an unset/missing `tradingMode` always trades paper, never live.
- Paper mode never places real orders — uses `kite.ltp()`/`kite.ohlc()` (free, read-only) to simulate fills and SL checks against real market data.

### Data model — `gap_positions.json`
- Shape: `{symbol: [trade_record, ...]}` — a **list** per symbol (holds full history), not a single record. Load via `kite_orders.load_positions()` (migrates old single-record files automatically).
- A trade record's key fields: `mode`, `backfilled`, `direction`, `entry_date`, `entry_price` (futures fill), `signal_entry` (cash-market reference — differs from `entry_price` by the futures basis, by design), `sl_price`, `sl_price_source`, `status` (`open` / `closed_eod` / `closed_by_sl`), and once closed: `exit_price`, `exit_date`, `pnl_pct`, `pnl_amount` (real lot-size ₹, not capital-normalized like the backtest).
- **Invariant that does NOT always hold:** `close_open_positions()` used to assume only the chronologically-last trade per symbol could be `status: "open"` (true for the live entry flow, since `place_entry_order()` refuses a new entry while the last is open). Backfill breaks this — it processes date chunks newest-first and can leave an *older* entry_date stuck `open` (from a historical-data gap on its exit-check date) while a *newer* entry_date for the same symbol is already closed. Fixed by sweeping **every** trade with `status == "open"` across all symbols, not just `_last_trade()`. If a symbol shows a stale open position with an oddly old/expired contract month in its `tradingsymbol`, this is the bug class to check first.

### Backfill
- `kite_orders.backfill_paper_trade()` retroactively populates paper trades from historical data — a one-time activity, run via the "⏪ Backfill 1yr" button (chunked, ~30-day chunks, newest-first, with incremental progress).
- Expired NFO contracts are flushed from `kite.instruments("NFO")` by Kite the moment they expire, so old contracts can't be resolved directly. Backfill works around this via `continuous=True` historical data mode: resolves the *current live* contract purely to get an `instrument_token`, but builds the *historically-correct* display `tradingsymbol` from the entry date's own real expiry month (e.g. `RELIANCE26JUNFUT` for a June signal, even though the token used is this month's contract).
- Idempotent via `(symbol, entry_date, backfilled=True)` matching — re-running doesn't duplicate.
- `/gap-orders/clear-backfill` removes all `backfilled: True` entries (real paper/live trades untouched) — use after a backfill logic fix, before re-running.

### Verifying signal parity (backtest vs automation)
- Comparing the Automated Trades "Day-wise" view against the backtest's own Day-wise view for the same symbol/date requires using the **same params** (`minGap`, `maxGap`, `slPct`, etc.) the live automation actually used — those live in the real browser's `TE_OVERNIGHT_DEFAULTS` localStorage key, not any hardcoded default. A fresh/incognito browser profile falls back to generic form defaults and will show false "no backtest signal on this date" mismatches for any real signal whose gap % sits just outside that fallback window — always diff the actual gap % against the window before concluding it's a real bug.
- `gap_scan.py`'s `scan_gap_signals()` is the source of truth for what the automation would have signaled on a given day — mirrors `backtest()` exactly, but reads params from whatever `settings` dict the caller passes in (from `/gap-settings`, synced from the frontend).

### Automated Trades UI (`TradeEdge.html`)
- Desktop section `#msTradesTaken` — visible only when Gap strategy is selected (hidden in `setStrategy()` for other strategies). 8-card stat row (`renderTradesTakenStatsExtended()`) mirrors the backtest's Summary Statistics card-for-card. 4 tabs: All Trades (sortable/filterable table) / Day-wise Trades (day-cards, reuses `.day-card` CSS) / Calendar View (month-grid, 2 months per row) / Stockwise (per-symbol aggregate table).
- Mobile `#mobLiveTradesTab` — strategy pills (Gap/Momentum/Pullback, automation-only-for-Gap message shown otherwise) → filters/backfill controls → scrollable trade list → sub-tab pill bar **pinned at the bottom** of the tab (same pattern as the Stats tab's `#mobTradesSubTabBar`), same 4 tabs as desktop.
- All three non-trivial sub-tab views (day-cards, calendar, stockwise) are built from shared pure functions (`_ttDayCardsHTML`, `_ttCalendarBuild`, `_ttStockwiseRows`) so desktop and mobile render from one source instead of duplicating the logic.

---

## Data Sync Details

### Incremental Sync (`quickSync()`)
- Fetches from max loaded date to today for all 186 symbols
- Uses CORS proxy chain — null-origin-safe proxies only:
  - `api.codetabs.com` (primary)
  - `thingproxy.freeboard.io`
  - `corsproxy.org` (not corsproxy.io — that one blocks null origin)
  - Yahoo Finance v7 API variants as extra fallbacks
- Removed: `allorigins.win` (blocks null/file:// origin), `corsproxy.io` (403 for Yahoo)
- 5 retry rounds with exponential backoff: 4s, 8s, 13s, 18s, 24s (+random jitter)

### Today Sync (`syncToday()`)
- Calls Render `/sync-today?offset=X&limit=10&days=3` in batches
- **Known issue:** Yahoo Finance actively rate-limits Render's free-tier shared IP (`YFRateLimitError`). Not fixable without upgrading to a paid Render tier (dedicated IP) or switching data source.
- Render `/sync-range` endpoint also available (added to app.py) for date-range fetches

### app.py Fetch Strategy
- `fetch_symbols_bulk()` — primary: `yf.download()` bulk call for all batch symbols in one HTTP request, MultiIndex DataFrame parsing
- `fetch_symbols_sequential()` — fallback: sequential `Ticker.history()` per symbol
- `fetch_symbols()` now calls `fetch_symbols_bulk()` with sequential fallback

---

## app.py Key Details

- **Framework:** Flask with CORS enabled
- **Deployed on:** Render free tier (`https://tradeedge-api.onrender.com` or similar)
- **Route:** `GET /sync-today?offset=<int>&limit=<int>&days=<int>`
- **Route:** `GET /sync-range?offset=<int>&limit=<int>&from=<date>&to=<date>`
- **Route:** `GET /` — health check + warmup endpoint
- **Batching:** Equities fetched in bulk via `yf.download()` MultiIndex; fallback to sequential
- **Retry logic:** 2 attempts per batch, backoff between retries
- **Constants:** `INTER_SYMBOL = 1.0`, `RATE_LIMIT_WAIT = 20.0`

---

## tradeedge_fetch.py Key Details

- Local data pipeline, outputs CSV files
- Flags: `--fo` (full F&O universe), `--sectors` (sector indices), `--merge` (incremental), `--days N`, `--symbols SYM1 SYM2`, `--summary`
- `--fo` and `--sectors` can be combined (symbols are merged, not overridden)
- Default (no flags): Nifty 50 stocks, last 2 years

---

## Coding Conventions

- All JS is vanilla (no frameworks) inside the single HTML file
- CSS variables used for theming (`--accent`, `--bull`, `--bear`, `--gold`, `--mono`, etc.)
- Mobile-responsive: separate mobile shell (`#mobileShell`) replaces desktop layout at ≤768px
- Strategy dispatch via `STRATEGY_ENGINES` / `STRATEGY_DATE_IDS` / `STRATEGY_PARAM_READERS` maps
- `computeTradeStats()` builds `_tradeStats` — single source of truth for all result rendering
- Trade log uses day-of-week and gap analysis in the Insights tab
- `_pnlColor/_pnlBg/_pnlSign` helpers use 0.005 threshold to distinguish flat from positive

---

## What NOT to do

- Never rename `TradeEdge.html` to any other name
- Never re-add the S&R (Support & Resistance) strategy without explicit instruction — it was intentionally removed
- Never change ALL_SYMBOLS order in app.py without updating HTML SYMBOLS to match
- Never remove the Render warmup ping logic from the HTML fetch flow
- Never add `allorigins.win` or `corsproxy.io` back to the CORS proxy list — both block null/file:// origin
