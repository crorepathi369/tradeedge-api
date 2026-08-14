"""
kite_orders.py — Order lifecycle for the Gap strategy automation.

Three responsibilities:
  1. Contract resolution — pick current-month NFO future, rolling to
     next-month if the signal date is <4 days from expiry (last Tuesday
     of the month), per Raja's rule.
  2. Entry — place market order + a GTT stop-loss order right after
     (paper mode: simulate the fill via kite.ltp() instead, no real order).
  3. Exit  — next trading day, close any position not already stopped
     out by its GTT (paper mode: simulate against real market data via
     kite.historical_data()/kite.ohlc() instead of a real GTT).

Every position carries its own "mode" ('paper' or 'live'), set at entry
time from whatever the Gap Settings toggle said that day — not a global
flag — so close_open_positions() branches per-position. That's what makes
toggling Paper → Live safe mid-flight: a paper position already open when
you flip the toggle still gets closed the paper way; only new entries
after the flip go live.

State is a single JSON file (gap_positions.json) in DATA_DIR, one entry
per open/recently-closed position, keyed by symbol. This lets the exit
job run independently of the entry job and survive a Render restart
mid-cycle (positions.json rides along with the GitHub data-branch
backup, same as the CSVs, via push_positions_to_github() in app.py).
"""
from __future__ import annotations
import json
import threading
import time
from pathlib import Path
from datetime import date, datetime, timedelta

# Guards the load->mutate->save critical section on gap_positions.json.
# The app runs under gunicorn --workers 1 --worker-class gevent (see
# Procfile), which cooperatively multitasks on I/O — a cron retry against a
# cold Render dyno, or a manual "run now" overlapping the scheduled cron
# fire, can genuinely interleave two read-modify-write cycles on this file
# without a lock. Plain threading.Lock() works correctly here because
# gevent's monkey-patching makes it cooperate with greenlets the same way
# a real lock cooperates with OS threads.
_positions_lock = threading.Lock()


# ── Expiry / contract resolution ────────────────────────────────────────────

def _last_tuesday(year: int, month: int) -> date:
    """Last Tuesday of the given month — NSE F&O monthly expiry day."""
    if month == 12:
        next_month_first = date(year + 1, 1, 1)
    else:
        next_month_first = date(year, month + 1, 1)
    d = next_month_first - timedelta(days=1)
    while d.weekday() != 1:  # Monday=0, Tuesday=1
        d -= timedelta(days=1)
    return d


def resolve_expiry(signal_date: date, days_threshold: int = 4) -> date:
    """
    Returns the expiry date to trade: current-month expiry, unless
    signal_date is within `days_threshold` days of it, in which case
    rolls to next-month's expiry.
    """
    current_expiry = _last_tuesday(signal_date.year, signal_date.month)
    if (current_expiry - signal_date).days < days_threshold:
        if signal_date.month == 12:
            ny, nm = signal_date.year + 1, 1
        else:
            ny, nm = signal_date.year, signal_date.month + 1
        return _last_tuesday(ny, nm)
    return current_expiry


def find_future_instrument(kite, symbol: str, expiry: date) -> dict | None:
    """
    Looks up the NFO future instrument for `symbol` expiring on `expiry`.
    Caller should pass a kite client with instruments already cached via
    get_nfo_instruments() below — this just filters that list.
    """
    instruments = get_nfo_instruments(kite)
    for inst in instruments:
        if (inst["name"] == symbol and inst["instrument_type"] == "FUT"
                and inst["expiry"] == expiry):
            return inst
    return None


_nfo_cache = {"date": None, "instruments": None}


def get_nfo_instruments(kite) -> list[dict]:
    """Cached per-day — Kite's instrument dump is large and static intraday."""
    today = datetime.now().strftime("%Y-%m-%d")
    if _nfo_cache["date"] == today and _nfo_cache["instruments"] is not None:
        return _nfo_cache["instruments"]
    raw = kite.instruments("NFO")
    # Normalise expiry to a date object for comparison
    for inst in raw:
        exp = inst.get("expiry")
        if exp and not isinstance(exp, date):
            inst["expiry"] = datetime.strptime(str(exp), "%Y-%m-%d").date()
    _nfo_cache["date"] = today
    _nfo_cache["instruments"] = raw
    return raw


# ── Position state (gap_positions.json) ─────────────────────────────────────
# Format: {symbol: [trade_record, trade_record, ...]} — a LIST per symbol,
# chronological, not a single record. A symbol can trade more than once
# (e.g. within a backfilled month, or simply a second real trade weeks
# later); a plain {symbol: record} shape would silently overwrite the
# earlier one the moment that symbol traded again, losing its history.

def _positions_file(data_dir: Path) -> Path:
    return data_dir / "gap_positions.json"


def load_positions(data_dir: Path) -> dict:
    path = _positions_file(data_dir)
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            raw = json.load(f)
    except (ValueError, FileNotFoundError):
        return {}
    # Defensive migration from the old {symbol: single_record} shape —
    # harmless no-op once everything's already list-shaped.
    return {sym: (rec if isinstance(rec, list) else [rec]) for sym, rec in raw.items()}


def save_positions(data_dir: Path, positions: dict) -> None:
    with open(_positions_file(data_dir), "w") as f:
        json.dump(positions, f, indent=2, default=str)


def _last_trade(positions: dict, symbol: str, preset: str | None = None) -> dict | None:
    """
    Returns the chronologically most recent trade for `symbol`, sorted by
    entry_date — NOT simply the last array element. Backfilled trades can
    end up appended out of chronological order (observed directly: a
    symbol's list with entry_date descending instead of ascending), which
    would otherwise make pl[-1] silently pick an old, already-closed trade
    instead of a genuinely open one.

    preset, if given, scopes the lookup to trades tagged with that preset
    only — lets multiple presets each hold their own independent position
    in the same symbol without one preset's guard seeing another's trade.
    """
    trades = positions.get(symbol)
    if not trades:
        return None
    if preset is not None:
        trades = [t for t in trades if t.get("preset") == preset]
        if not trades:
            return None
    return sorted(trades, key=lambda t: t.get("entry_date", ""))[-1]


def diagnose_order_issues(data_dir: Path) -> list[dict]:
    """
    One-time diagnostic — scans every symbol's trade list for the same
    out-of-order-append pattern that stranded BRITANNIA (a trade appended
    out of entry_date order, which made the old raw pl[-1] logic silently
    look at the wrong trade). Flags two kinds of finding per symbol:

      - "list_not_chronological": entry_dates in the stored list aren't in
        ascending order at all — worth knowing even if it didn't cause a
        visible problem yet, since it's the same latent risk.
      - "was_stuck_open": the OLD raw-pl[-1] logic would have picked a
        different trade than the correct (entry_date-sorted) most-recent
        one, AND that correct most-recent trade is 'open' — this is the
        exact BRITANNIA scenario: an open trade that close_open_positions()
        would have silently never seen before this fix.

    Purely read-only — flags issues, doesn't fix data. The code fix
    (sorting by entry_date in _last_trade()/close_open_positions()) already
    makes any "was_stuck_open" case resolve itself on the next exit run;
    this is just visibility into whether BRITANNIA was a one-off or one of
    several.
    """
    positions = load_positions(data_dir)
    findings = []
    for symbol, trades in positions.items():
        if not trades or len(trades) < 2:
            continue

        dates = [t.get("entry_date", "") for t in trades]
        is_chronological = dates == sorted(dates)

        raw_last = trades[-1]
        correct_last = sorted(trades, key=lambda t: t.get("entry_date", ""))[-1]
        mismatch = raw_last is not correct_last

        if not is_chronological:
            findings.append({
                "sym": symbol, "issue": "list_not_chronological",
                "entry_dates_in_order": dates,
            })
        if mismatch and correct_last.get("status") == "open" and raw_last.get("status") != "open":
            findings.append({
                "sym": symbol, "issue": "was_stuck_open",
                "correct_last_entry_date": correct_last.get("entry_date"),
                "old_logic_would_have_picked": raw_last.get("entry_date"),
                "old_logic_status": raw_last.get("status"),
            })
    return findings


def clear_backfilled_trades(data_dir: Path, preset: str | None = None) -> int:
    """
    Removes every trade tagged backfilled=True, leaving real paper/live
    trades untouched. For re-running a backfill after a logic fix — the
    (symbol, entry_date, preset) idempotency check in backfill_paper_trade()
    would otherwise skip every date already covered by the buggy run,
    permanently stranding the bad records. Returns the count removed.

    preset, if given, only clears that preset's backfilled trades — with
    multiple presets automated in parallel, an unscoped clear would wipe
    every other preset's backfill history too when you only meant to fix
    one. preset=None keeps the old unscoped "clear everything" behavior
    available for an intentional full reset.
    """
    with _positions_lock:
        positions = load_positions(data_dir)
        removed = 0
        for symbol in list(positions.keys()):
            kept = [
                t for t in positions[symbol]
                if not (t.get("backfilled") and (preset is None or t.get("preset") == preset))
            ]
            removed += len(positions[symbol]) - len(kept)
            if kept:
                positions[symbol] = kept
            else:
                del positions[symbol]
        save_positions(data_dir, positions)
    return removed


def clear_all_trades(data_dir: Path, preset: str | None = None) -> int:
    """
    Removes EVERY trade for a preset — backfilled and real (paper/live) alike.
    For a deliberate full reset when a strategy's own logic/params have changed
    enough that the existing log (including genuinely-executed automated trades)
    no longer reflects how the preset actually behaves — not for routine backfill
    cleanup, which is what clear_backfilled_trades() (real trades untouched) is for.
    preset=None clears every preset. Returns the count removed.
    """
    with _positions_lock:
        positions = load_positions(data_dir)
        removed = 0
        for symbol in list(positions.keys()):
            kept = [
                t for t in positions[symbol]
                if preset is not None and t.get("preset") != preset
            ]
            removed += len(positions[symbol]) - len(kept)
            if kept:
                positions[symbol] = kept
            else:
                del positions[symbol]
        save_positions(data_dir, positions)
    return removed


def tag_untagged_trades(data_dir: Path, preset_name: str) -> int:
    """
    One-time migration: tags every trade currently missing a 'preset' field
    with preset_name (normally 'Oneday-Setup', since that preset's params
    are what drove gap_settings.json historically, before presets existed).
    Idempotent — only touches trades that don't already have a preset, so
    it's safe to call more than once. Must be run BEFORE the (symbol,
    preset)-scoped duplicate-entry guard in place_entry_order() goes live in
    production: an untagged genuinely-open position is invisible to that
    guard and could otherwise be re-entered. Returns the count tagged.
    """
    with _positions_lock:
        positions = load_positions(data_dir)
        tagged = 0
        for trades in positions.values():
            for t in trades:
                if not t.get("preset"):
                    t["preset"] = preset_name
                    tagged += 1
        if tagged:
            save_positions(data_dir, positions)
    return tagged


# ── Entry ────────────────────────────────────────────────────────────────────

def _wait_for_fill(kite, order_id: str, attempts: int = 6, delay_sec: float = 1.0) -> float | None:
    """
    Polls the order book for order_id's average fill price. NFO futures
    market orders during market hours normally fill within a second or two;
    a few short retries covers that without blocking the request for long.
    Returns None if it never shows COMPLETE within the retry budget —
    caller falls back to the signal's slLevel rather than blocking the
    entry entirely on a slow order-book update.
    """
    for _ in range(attempts):
        try:
            for o in kite.orders():
                if o.get("order_id") == order_id and o.get("status") == "COMPLETE":
                    return o.get("average_price")
        except Exception:
            pass
        time.sleep(delay_sec)
    return None


def _find_last_fill(kite, tradingsymbol: str, txn_type: str, attempts: int = 6,
                     delay_sec: float = 1.0) -> float | None:
    """
    Like _wait_for_fill(), but for a position closed by the GTT firing on
    its own rather than an order this code placed — there's no order_id to
    poll, so this finds the most recent COMPLETE order on tradingsymbol/
    txn_type instead (the GTT's triggered order shows up in the regular
    order book same as any other). Retries for the same reason: the GTT
    may have only just fired and the order book can lag a second or two.
    """
    for _ in range(attempts):
        try:
            matches = [
                o for o in kite.orders()
                if o.get("tradingsymbol") == tradingsymbol
                and o.get("transaction_type") == txn_type
                and o.get("status") == "COMPLETE"
            ]
            if matches:
                # order_timestamp sorts naturally as ISO-ish strings; last one wins
                matches.sort(key=lambda o: o.get("order_timestamp") or "")
                return matches[-1].get("average_price")
        except Exception:
            pass
        time.sleep(delay_sec)
    return None


def _compute_pnl(direction: str, entry_price: float | None, exit_price: float | None,
                  lot_size: int) -> tuple[float | None, float | None]:
    """Returns (pnl_pct, pnl_amount) — both None if either price is unknown."""
    if entry_price is None or exit_price is None:
        return None, None
    diff = (exit_price - entry_price) if direction == "LONG" else (entry_price - exit_price)
    pnl_pct = round((diff / entry_price) * 100, 2)
    pnl_amount = round(diff * lot_size, 2)
    return pnl_pct, pnl_amount


def _paper_entry_price(kite, tradingsymbol: str) -> float | None:
    """Simulated fill for a paper entry — current LTP, same free read-only
    call the live path already makes for the GTT's last_price param."""
    try:
        ltp_data = kite.ltp(f"NFO:{tradingsymbol}")
        return ltp_data[f"NFO:{tradingsymbol}"]["last_price"]
    except Exception as e:
        print(f"[gap-orders/paper] Could not read LTP for {tradingsymbol}: {e}")
        return None


def _paper_check_exit(kite, tradingsymbol: str, direction: str, entry_price: float | None,
                       sl_price: float, tp_type: str, tp_price: float | None,
                       days_held: int, hold_days: int) -> tuple[float, str, bool, bool, str]:
    """
    Simulates one day's exit check for a paper position, dispatching on
    tp_type EXACTLY like backtest_overnight() does — so live/paper trading
    always matches whatever the preset that placed this trade backtests as,
    whether that's a 1-day D2 Close/Open exit or a multi-day %-TP hold with
    its own Hold Days budget. Nothing here is hardcoded per preset name;
    behaviour is driven purely by tp_type/tp_price/hold_days as stored on
    the trade record at entry time (see place_entry_order()) — change a
    preset's TP settings later and this automatically follows.

    Called once per day by close_open_positions() (which runs daily via
    /gap-orders/exit), for as long as the position stays open. Only ever
    checks TODAY's range so far (kite.ohlc(), no Historical API needed) —
    NEVER the entry day's own range, for the same reason as before: the
    entry happens near the close that sl_price/tp_price are computed from,
    so checking that same day's range would compare against price action
    from before the position existed.

    Returns (exit_price, exit_date, closed, sl_hit, note).
    closed=False means: neither SL nor TP hit today, and there's still
    Hold Days budget left — caller should leave the position open and bump
    its days_held counter by 1 (this is the ONLY state this function
    doesn't mutate itself — it's a pure day-check, the caller owns saving).
    Never raises — any data-fetch failure resolves the position at the
    current LTP (closed=True) rather than leaving it stuck open forever.
    """
    today_str = date.today().isoformat()
    hold = max(1, hold_days or 1)
    is_last_day = (days_held + 1) >= hold
    day_label = f"day {days_held + 1}"

    def _sl_hit_range(low, high):
        return (low <= sl_price) if direction == "LONG" else (high >= sl_price)

    try:
        ohlc = kite.ohlc(f"NFO:{tradingsymbol}")[f"NFO:{tradingsymbol}"]["ohlc"]
    except Exception as e:
        print(f"[gap-orders/paper] ohlc failed for {tradingsymbol}: {e}")
        ltp = _paper_entry_price(kite, tradingsymbol)
        return (ltp, today_str, True, False, f"paper closed at LTP ({day_label}, ohlc unavailable)")

    low, high, o = ohlc["low"], ohlc["high"], ohlc["open"]

    if tp_type == "pct" and tp_price is not None:
        # Gap-through SL at today's open, checked first — mirrors
        # backtest_overnight()'s day-open SL check taking priority over TP.
        open_past_sl = (o <= sl_price) if direction == "LONG" else (o >= sl_price)
        if open_past_sl:
            return (o, today_str, True, True, f"paper SL hit at open ({day_label})")
        if _sl_hit_range(low, high):
            return (sl_price, today_str, True, True, f"paper SL hit ({day_label})")
        tp_hit = (high >= tp_price) if direction == "LONG" else (low <= tp_price)
        if tp_hit:
            return (tp_price, today_str, True, False, f"paper TP hit ({day_label})")
        if is_last_day:
            ltp = _paper_entry_price(kite, tradingsymbol)
            return (ltp, today_str, True, False, f"paper hold expired, closed at LTP ({day_label})")
        return (None, today_str, False, False, f"paper still open, no SL/TP hit ({day_label})")

    if tp_type == "d2_open" and entry_price is not None:
        moved_in_favour = (o > entry_price) if direction == "LONG" else (o < entry_price)
        if moved_in_favour:
            return (o, today_str, True, False, "paper TP hit at open (D2 Open)")
        if _sl_hit_range(low, high):
            return (sl_price, today_str, True, True, "paper SL hit (D2)")
        ltp = _paper_entry_price(kite, tradingsymbol)
        return (ltp, today_str, True, False, "paper closed at LTP (D2 Open miss)")

    # Default: 'd2_close' — unchanged from the original single-day-only
    # behaviour (hold is always effectively 1 for this tp_type).
    if _sl_hit_range(low, high):
        return (sl_price, today_str, True, True, "paper SL hit (exit-day range so far)")
    ltp = _paper_entry_price(kite, tradingsymbol)
    return (ltp, today_str, True, False, "paper closed at LTP (no SL cross detected)")


def place_entry_order(kite, data_dir: Path, signal: dict, today: date,
                       sl_pct: float, sl_type: str = "pct", mode: str = "paper",
                       *, preset: str, tp_type: str = "d2_close", tp_pct: float = 1.0,
                       hold_days: int = 1) -> dict:
    """
    Enters `signal` (from gap_scan.scan_gap_signals' "selected" list) in
    the correct current/next-month future contract, tagged with `preset`
    (required — every entry now belongs to a named preset, see
    gap_presets.json / "Include in Automated Trades").

    tp_type/tp_pct/hold_days come from the preset's own params (same as
    sl_pct/sl_type) and are stored on the trade record so
    close_open_positions() knows how to exit it later — d2_close (default)
    and d2_open both resolve on the very next trading day regardless of
    hold_days; tp_type='pct' holds for up to hold_days days, checking both
    SL and a %-TP target each day, exactly mirroring how
    backtest_overnight() simulates the same preset. See _paper_check_exit().

    mode='live' places a real market order + a real GTT stop-loss, exactly
    as before. mode='paper' (the default — fail-safe, never assume live)
    skips both real calls entirely and simulates the fill via kite.ltp(),
    the same free read-only call the live path already makes for the
    GTT's last_price param. Everything downstream — SL computation,
    position record shape, the Automated Trades UI — is identical between
    the two; only the "mode" field and the absence of order_id/gtt_id
    distinguish a paper trade from a real one.

    The signal's slLevel is computed off the STOCK's cash-market close
    (that's what the scan/backtest uses to decide WR/expectancy), but we
    trade the FUTURE — its price differs from spot by the futures basis,
    and a market order itself may fill a bit away from the last-seen
    price. Per Raja: SL must be sl_pct% from the future's own actual (or,
    in paper mode, simulated) entry price, not the cash-market slLevel.
    If a live fill can't be read back in time, falls back to slLevel
    (better than no SL) and flags it in the result.

    sl_pct/sl_type come from the caller's preset params at call time (not
    baked into signal), since they can change independently.

    The (symbol, preset) duplicate-entry guard and the final save both
    happen under _positions_lock, held for the ENTIRE function body (not
    just the file I/O) — with multiple presets automating in parallel and
    the possibility of a cron retry or manual "run now" overlapping a
    scheduled fire, the guard-check-then-later-save gap is exactly where a
    genuine duplicate order could slip through if two calls interleaved.
    Serializing entry placement is a non-issue at this app's request volume
    and is the only way to actually close that race, not just protect the
    JSON file's bytes.

    Returns a result dict — either {"ok": True, "position": {...}} or
    {"ok": False, "error": "..."}. Never raises — callers (the cron
    endpoint) should just report whatever comes back.
    """
    symbol = signal["sym"]
    side = signal["side"]  # LONG / SHORT

    with _positions_lock:
        existing = _last_trade(load_positions(data_dir), symbol, preset=preset)
        if existing and existing.get("status") == "open":
            return {"ok": False, "error": f"[{preset}] {symbol} already has an open position from {existing.get('entry_date')} — skipping"}
        return _place_entry_order_locked(kite, data_dir, signal, today, sl_pct, sl_type, mode, preset,
                                          symbol, side, tp_type, tp_pct, hold_days)


def _place_entry_order_locked(kite, data_dir: Path, signal: dict, today: date,
                               sl_pct: float, sl_type: str, mode: str, preset: str,
                               symbol: str, side: str, tp_type: str, tp_pct: float,
                               hold_days: int) -> dict:
    """Body of place_entry_order() that runs under _positions_lock — split out
    only so the guard check above reads cleanly; not meant to be called directly."""
    expiry = resolve_expiry(today)
    inst = find_future_instrument(kite, symbol, expiry)
    if inst is None:
        return {"ok": False, "error": f"No NFO future found for {symbol} exp {expiry}"}

    tradingsymbol = inst["tradingsymbol"]
    lot_size = inst["lot_size"]
    txn_type = kite.TRANSACTION_TYPE_BUY if side == "LONG" else kite.TRANSACTION_TYPE_SELL
    exit_txn_type = kite.TRANSACTION_TYPE_SELL if side == "LONG" else kite.TRANSACTION_TYPE_BUY

    order_id = None
    if mode == "live":
        try:
            order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR, exchange="NFO",
                tradingsymbol=tradingsymbol, transaction_type=txn_type,
                quantity=lot_size, order_type=kite.ORDER_TYPE_MARKET,
                product=kite.PRODUCT_NRML,
            )
        except Exception as e:
            return {"ok": False, "error": f"Entry order failed: {e}"}
        fill_price = _wait_for_fill(kite, order_id)
    else:
        fill_price = _paper_entry_price(kite, tradingsymbol)

    # Read back the future's actual (or simulated) fill price — this, not
    # the signal's cash-market slLevel, is what the SL% must be measured from.
    sl_price_source = "actual_fill" if mode == "live" else "paper_simulated_fill"
    if fill_price:
        if sl_type == "gap_fill":
            sl_price = fill_price
        else:
            sl_price = (fill_price * (1 - sl_pct / 100) if side == "LONG"
                        else fill_price * (1 + sl_pct / 100))
        sl_price = round(sl_price, 2)
    else:
        # Couldn't confirm a price in time — fall back to the signal's
        # cash-market slLevel rather than leaving the position unprotected.
        fill_price = None
        sl_price = signal["slLevel"]
        sl_price_source = "fallback_signal_slLevel"
        print(f"[gap-orders] {symbol}: could not read back {'fill' if mode == 'live' else 'LTP'} price "
              f"— SL falls back to signal slLevel {sl_price}")

    # TP target for the multi-day %-TP hold path — None for d2_close/d2_open
    # (those resolve on the next trading day regardless of a price target) or
    # when fill_price couldn't be confirmed (same fallback reasoning as SL).
    tp_price = None
    if tp_type == "pct" and fill_price:
        tp_price = (fill_price * (1 + tp_pct / 100) if side == "LONG"
                    else fill_price * (1 - tp_pct / 100))
        tp_price = round(tp_price, 2)

    # GTT stop-loss — only for real trades. A paper position's "GTT" is
    # just the sl_price value checked day-by-day in close_open_positions().
    gtt_id = None
    if mode == "live":
        try:
            ltp_data = kite.ltp(f"NFO:{tradingsymbol}")
            last_price = ltp_data[f"NFO:{tradingsymbol}"]["last_price"]
            gtt = kite.place_gtt(
                trigger_type=kite.GTT_TYPE_SINGLE,
                tradingsymbol=tradingsymbol, exchange="NFO",
                trigger_values=[sl_price], last_price=last_price,
                orders=[{
                    "transaction_type": exit_txn_type, "quantity": lot_size,
                    "order_type": kite.ORDER_TYPE_MARKET, "product": kite.PRODUCT_NRML,
                }],
            )
            gtt_id = gtt.get("trigger_id")
        except Exception as e:
            # Entry already went through — don't lose the position, just flag
            # that SL protection failed so the morning check can surface it.
            print(f"[gap-orders] GTT placement failed for {symbol}: {e}")

    positions = load_positions(data_dir)
    new_trade = {
        "preset": preset,  # which named preset produced this trade — see gap_presets.json
        "mode": mode,  # 'paper' or 'live' — drives how close_open_positions() handles this one
        "direction": side, "entry_date": today.isoformat(),
        "entry_order_id": order_id, "tradingsymbol": tradingsymbol,
        "lot_size": lot_size,
        "entry_price": fill_price,           # actual (live) or simulated (paper) futures fill
        "signal_entry": signal["entry"],      # cash-market close at signal time — for slippage vs actual fill
        "signal_slLevel": signal["slLevel"],  # cash-market reference, for comparison
        "sl_price": sl_price,
        "sl_price_source": sl_price_source,
        "tp_type": tp_type, "tp_pct": tp_pct, "tp_price": tp_price,
        "hold_days": max(1, hold_days or 1), "days_held": 0,
        "gtt_id": gtt_id, "status": "open",
    }
    positions.setdefault(symbol, []).append(new_trade)
    save_positions(data_dir, positions)

    return {"ok": True, "position": new_trade}


# ── Exit ─────────────────────────────────────────────────────────────────────

def _reconcile_missing_exit_prices(kite, data_dir: Path) -> list[dict]:
    """
    Sweeps every already-closed trade whose exit_price never got confirmed —
    the rare case where _wait_for_fill()'s ~6s polling window closed before
    Kite's order book caught up, even though the real exit already happened
    (real order, real fill, just not confirmed in time to log). Re-checks
    for the real average_price and backfills it retroactively, so a blank
    exit_price/pnl doesn't stay blank forever. Runs automatically at the
    start of every close_open_positions() call — i.e. every daily exit-cron
    run — so this self-heals within a day or two without needing its own
    schedule. One quick check per sweep (not the full 6-attempt retry used
    right after placing an order) — if still not found, it just tries again
    next time this runs.
    """
    positions = load_positions(data_dir)
    results = []
    changed = False
    for symbol, trades in positions.items():
        for pos in trades:
            if pos.get("status") not in ("closed_eod", "closed_by_sl") or pos.get("exit_price") is not None:
                continue
            if pos.get("mode") == "paper":
                continue  # paper exits are simulated, never unconfirmed — nothing to reconcile

            exit_price = None
            if pos.get("exit_order_id"):
                try:
                    for o in kite.orders():
                        if o.get("order_id") == pos["exit_order_id"] and o.get("status") == "COMPLETE":
                            exit_price = o.get("average_price")
                            break
                except Exception as e:
                    print(f"[gap-orders/reconcile] orders() lookup failed for {symbol}: {e}")
            if exit_price is None:
                exit_txn_type = kite.TRANSACTION_TYPE_SELL if pos["direction"] == "LONG" else kite.TRANSACTION_TYPE_BUY
                exit_price = _find_last_fill(kite, pos["tradingsymbol"], exit_txn_type, attempts=1, delay_sec=0)

            if exit_price is not None:
                pnl_pct, pnl_amount = _compute_pnl(pos["direction"], pos.get("entry_price"), exit_price, pos["lot_size"])
                pos["exit_price"] = exit_price
                pos["pnl_pct"] = pnl_pct
                pos["pnl_amount"] = pnl_amount
                changed = True
                results.append({"sym": symbol, "preset": pos.get("preset"), "ok": True, "action": "reconciled_exit_price", "exit_price": exit_price})
                print(f"[gap-orders/reconcile] {symbol}: backfilled exit_price={exit_price}")

    if changed:
        save_positions(data_dir, positions)
    return results


def close_open_positions(kite, data_dir: Path) -> list[dict]:
    """
    For every position with status 'open', closes it the way appropriate
    to its own stored mode — 'live' positions check Kite's real position
    book and place a real exit order if the GTT hasn't already closed it;
    'paper' positions get checked against real market data via
    _paper_check_exit() instead, no real orders anywhere — including
    multi-day Hold Days + %-TP positions, which stay open (days_held
    bumped) across as many daily runs of this function as their preset's
    Hold Days budget allows. Mode is read per-position, not passed in
    globally, so a Paper→Live toggle mid-flight never changes how an
    already-open paper position gets closed. Also sweeps for any
    already-closed trade still missing its
    exit_price (see _reconcile_missing_exit_prices()) before processing
    today's closes. Returns a list of per-symbol result dicts (reconciled
    ones first). Called by the next-day 3:15 PM job.

    No preset-scoping needed here (unlike entry/backfill) -- exit is fully
    self-contained per trade record (tradingsymbol/direction/sl_price all
    stored at entry time), so every open trade across every preset closes
    correctly regardless of which preset placed it. The whole body runs
    under _positions_lock for the same reason as place_entry_order() --
    holding it across the Kite API calls, not just the file I/O, is what
    actually closes the race between an overlapping request's guard check
    and this function's own read-modify-write cycle.

    KNOWN FOLLOW-UP, not reachable while automation is Paper-only: the
    live-mode branch below keys live_positions by tradingsymbol alone --
    a broker-level aggregate across ALL open quantity in that contract.
    If Live is ever extended to multiple presets simultaneously holding
    the same symbol, one preset's GTT firing would leave the aggregate
    quantity still non-zero (the other preset's leg), so this code would
    misread that preset's own position as "still open" and place an
    incorrect second exit order against the other preset's shares. Must
    be fixed (e.g. per-preset live position tracking) before Live is ever
    enabled for more than one preset at once.
    """
    with _positions_lock:
        reconciled = _reconcile_missing_exit_prices(kite, data_dir)

        positions = load_positions(data_dir)
        if not positions:
            return reconciled

        return _close_open_positions_locked(kite, data_dir, positions, reconciled)


def _close_open_positions_locked(kite, data_dir: Path, positions: dict, reconciled: list[dict]) -> list[dict]:
    """Body of close_open_positions() that runs under _positions_lock; not
    meant to be called directly."""
    # For the live entry flow, only the chronologically-last trade per
    # symbol can ever be 'open' — place_entry_order() refuses a new entry
    # while the last one is still open. But backfill_paper_trade() bypasses
    # that guard and processes chunks newest-first, so a data gap on one
    # historical exit-check date can leave an OLDER entry_date 'open' while
    # a NEWER entry_date for the same symbol is already closed (confirmed:
    # NATIONALUM26JUNFUT sat 'open' from 2026-06-25 while NATIONALUM26AUGFUT,
    # entered 2026-08-03, was already closed_eod — _last_trade() alone would
    # never surface the June one again). So sweep every trade with
    # status=='open' across every symbol, not just the last one — a symbol
    # can appear more than once here only via that backfill edge case; the
    # live flow's one-open-position-per-symbol invariant still holds.
    open_items = [(s, t) for s, pl in positions.items() for t in pl if t.get("status") == "open"]
    if not open_items:
        return reconciled

    # Only live positions need Kite's real position book — skip the call
    # entirely (and don't let its failure block paper closes) if there
    # are none open right now.
    live_positions = {}
    if any(p.get("mode", "live") == "live" for _, p in open_items):
        try:
            live_positions = {p["tradingsymbol"]: p for p in kite.positions()["net"]}
        except Exception as e:
            print(f"[gap-orders] Could not fetch live positions: {e}")

    today_str = date.today().isoformat()
    results = []
    for symbol, pos in open_items:
        mode = pos.get("mode", "live")  # positions entered before mode existed default to live
        preset = pos.get("preset")
        exit_txn_type = kite.TRANSACTION_TYPE_SELL if pos["direction"] == "LONG" else kite.TRANSACTION_TYPE_BUY

        if mode == "paper":
            # Fields default for trade records placed before this multi-day-hold
            # support existed — those are all effectively 'd2_close', hold=1,
            # so defaulting reproduces their exact original single-day behaviour.
            tp_type   = pos.get("tp_type", "d2_close")
            tp_price  = pos.get("tp_price")
            hold_days = pos.get("hold_days", 1)
            days_held = pos.get("days_held", 0)
            exit_price, exit_date, closed, sl_hit, note = _paper_check_exit(
                kite, pos["tradingsymbol"], pos["direction"], pos.get("entry_price"),
                pos["sl_price"], tp_type, tp_price, days_held, hold_days)
            if not closed:
                # Still within its Hold Days budget, neither SL nor TP hit
                # today — leave it open, just remember another day passed.
                # save_positions() at the end of this function persists it.
                pos["days_held"] = days_held + 1
                results.append({"sym": symbol, "preset": preset, "ok": True, "action": note, "exit_price": None})
                continue
            pnl_pct, pnl_amount = _compute_pnl(pos["direction"], pos.get("entry_price"), exit_price, pos["lot_size"])
            pos["status"] = "closed_by_sl" if sl_hit else "closed_eod"
            pos["exit_price"] = exit_price
            pos["exit_date"] = exit_date
            pos["pnl_pct"] = pnl_pct
            pos["pnl_amount"] = pnl_amount
            results.append({"sym": symbol, "preset": preset, "ok": True, "action": note, "exit_price": exit_price})
            continue

        live = live_positions.get(pos["tradingsymbol"])
        still_open = live is not None and live.get("quantity", 0) != 0

        if not still_open:
            # GTT already closed it (or it was closed manually) — no order
            # to place, but still read back what it actually exited at.
            exit_price = _find_last_fill(kite, pos["tradingsymbol"], exit_txn_type)
            pnl_pct, pnl_amount = _compute_pnl(pos["direction"], pos.get("entry_price"), exit_price, pos["lot_size"])
            pos["status"] = "closed_by_sl"
            pos["exit_price"] = exit_price
            pos["exit_date"] = today_str
            pos["pnl_pct"] = pnl_pct
            pos["pnl_amount"] = pnl_amount
            results.append({"sym": symbol, "preset": preset, "ok": True, "action": "already_closed", "exit_price": exit_price})
            continue

        try:
            order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR, exchange="NFO",
                tradingsymbol=pos["tradingsymbol"], transaction_type=exit_txn_type,
                quantity=pos["lot_size"], order_type=kite.ORDER_TYPE_MARKET,
                product=kite.PRODUCT_NRML,
            )
            if pos.get("gtt_id"):
                try:
                    kite.delete_gtt(pos["gtt_id"])
                except Exception:
                    pass  # GTT may have already expired/fired — non-fatal

            exit_price = _wait_for_fill(kite, order_id)
            pnl_pct, pnl_amount = _compute_pnl(pos["direction"], pos.get("entry_price"), exit_price, pos["lot_size"])
            pos["status"] = "closed_eod"
            pos["exit_order_id"] = order_id
            pos["exit_price"] = exit_price
            pos["exit_date"] = today_str
            pos["pnl_pct"] = pnl_pct
            pos["pnl_amount"] = pnl_amount
            results.append({"sym": symbol, "preset": preset, "ok": True, "action": "exited", "order_id": order_id, "exit_price": exit_price})
        except Exception as e:
            results.append({"sym": symbol, "preset": preset, "ok": False, "error": str(e)})

    save_positions(data_dir, positions)
    return reconciled + results


# ── Backfill ─────────────────────────────────────────────────────────────────
# Retroactively populates paper trades for past signal days, so Paper mode's
# mechanics can be validated against a month of real market history without
# waiting for it to run forward day by day. Uses the same day-granularity SL
# check as the live paper path (kite.historical_data(), 'day' interval) —
# just for a specific past date instead of "today".

def _already_backfilled(positions: dict, symbol: str, entry_date: str, preset: str) -> bool:
    """
    True if ANY trade already exists for this (symbol, entry_date, preset) —
    backfilled OR real. A real live entry for that exact date already
    correctly represents that day's activity; backfill must never create a
    second record alongside it.

    Bug found 2026-08-12: this used to only check other backfilled==True
    records. If the live cron had already placed a real entry for a symbol
    today, then someone cleared backfilled trades and re-ran backfill (a
    normal workflow — see clear_backfilled_trades()), the old check never
    saw the real entry (backfilled is unset on it) and happily created a
    second, duplicate "backfilled" record for the same (symbol, date,
    preset) — observed live: two OIL/Oneday-Setup entries for the same day,
    one real, one backfilled. There is never a legitimate reason for two
    trades to share (symbol, entry_date, preset) within a single preset's
    automation — a symbol either signals on a given day or it doesn't.
    """
    return any(t.get("entry_date") == entry_date and t.get("preset") == preset
               for t in positions.get(symbol, []))


def _resolve_backfill_exit(kite, token: int, tradingsymbol: str, side: str,
                            entry_price: float, sl_price: float, tp_type: str,
                            tp_price: float | None, exit_check_dates: list[date]) -> tuple[str, float | None, str | None, int]:
    """
    Walks exit_check_dates (ascending, already trimmed by the caller to at
    most Hold Days entries) one historical daily candle at a time — this is
    the backfill equivalent of _paper_check_exit(), mirroring
    backtest_overnight()'s own day-by-day resolution exactly, since here
    (unlike the live daily check) the full future date range is already
    known up front. d2_close/d2_open only ever get ONE date in
    exit_check_dates (their hold is always 1 day), so this naturally
    reduces to the original single-day check for those — no behaviour
    change for Oneday-Setup/High-Conviction-Only-style presets.

    Returns (status, exit_price, exit_date_str, days_checked):
      status: "open" (ran out of dates, or the list was empty — the regular
        /gap-orders/exit → close_open_positions() path finishes it later,
        continuing from days_checked), "closed_by_sl", or "closed_eod"
        (used for a TP win too — there's no separate "closed_by_tp" status
        anywhere in this app, so a TP close is just a closed_eod win).
      days_checked: how many dates were actually resolved (candle fetched,
        neither SL nor TP hit) before stopping — the caller stores this as
        the new trade's days_held so close_open_positions() picks up
        counting from the right place instead of restarting at 0.
    """
    def _sl_hit_range(low, high):
        return (low <= sl_price) if side == "LONG" else (high >= sl_price)

    days_checked = 0
    for h, check_date in enumerate(exit_check_dates, start=1):
        date_str = check_date.isoformat()
        try:
            candles = kite.historical_data(token, date_str, date_str, "day", continuous=True)
        except Exception as e:
            print(f"[gap-orders/backfill] historical_data failed for {tradingsymbol} on {date_str}: {e}")
            candles = []
        if not candles:
            break  # missing data for this date — stop here, leave open at days_checked
        c = candles[0]
        is_last = (h == len(exit_check_dates))

        if tp_type == "pct" and tp_price is not None:
            open_past_sl = (c["open"] <= sl_price) if side == "LONG" else (c["open"] >= sl_price)
            if open_past_sl:
                return "closed_by_sl", c["open"], date_str, days_checked
            if _sl_hit_range(c["low"], c["high"]):
                return "closed_by_sl", sl_price, date_str, days_checked
            tp_hit = (c["high"] >= tp_price) if side == "LONG" else (c["low"] <= tp_price)
            if tp_hit:
                return "closed_eod", tp_price, date_str, days_checked
            if is_last:
                return "closed_eod", c["close"], date_str, days_checked
            days_checked = h
            continue

        if tp_type == "d2_open":
            moved_in_favour = (c["open"] > entry_price) if side == "LONG" else (c["open"] < entry_price)
            if moved_in_favour:
                return "closed_eod", c["open"], date_str, days_checked
            if _sl_hit_range(c["low"], c["high"]):
                return "closed_by_sl", sl_price, date_str, days_checked
            return "closed_eod", c["close"], date_str, days_checked

        # Default: 'd2_close' — original single-day behaviour, unchanged.
        if _sl_hit_range(c["low"], c["high"]):
            return "closed_by_sl", sl_price, date_str, days_checked
        return "closed_eod", c["close"], date_str, days_checked

    return "open", None, None, days_checked


def backfill_paper_trade(kite, data_dir: Path, signal: dict, entry_date: date,
                          sl_pct: float, sl_type: str, exit_check_dates: list[date],
                          *, preset: str, tp_type: str = "d2_close", tp_pct: float = 1.0,
                          hold_days: int = 1) -> dict:
    """
    Creates one backfilled paper trade for a past `signal` (from
    gap_scan.scan_gap_signals' "selected" list for that historical date).

    entry_date: the historical signal day being backfilled.
    exit_check_dates: the next dates (ascending) in the caller's own
    backfill date sequence — not naive +1-calendar-day, so they always line
    up with actual trading days the caller already knows have data. Length
    should be at most hold_days (the caller trims it); d2_close/d2_open
    presets only ever need one date since their hold is always 1 day. If
    empty (this is at/near the most recent date in the backfill window,
    with no "next day" data available yet), the trade is left status='open'
    and un-exited — the regular /gap-orders/exit → close_open_positions()
    paper path finishes it correctly the next time it runs, exactly as it
    would for a same-day live paper entry (kite.ohlc() covers "today"
    either way). See _resolve_backfill_exit() for the day-by-day walk.

    Expired contracts are flushed from kite.instruments("NFO") the moment
    they expire — the exchange doesn't keep old instrument_tokens resolvable
    at all, confirmed against Kite's own forum docs, not just observed
    behaviour. So for any entry_date older than the *current* contract
    cycle, find_future_instrument(entry_date's own expiry) would always
    return None. Instead: resolve the CURRENT live contract (whatever
    month that is today) purely to get an instrument_token, then call
    historical_data(..., continuous=True) — Kite's continuous-futures mode
    stitches daily candles across expired contracts using a live contract's
    token as the anchor, so the actual historical date range works
    regardless of which specific contract was live back then. The DISPLAY
    tradingsymbol is still the historically-correct one (built from the
    live contract's own name + entry_date's real expiry month), so it
    reads as "RELIANCE26JUNFUT" for a June signal, not this month's
    contract. lot_size is taken from the current live contract as an
    approximation — exact historical lot sizes aren't recoverable this
    way, but they rarely change.

    Idempotent — a (symbol, entry_date, preset) triple that already has ANY
    trade (backfilled or real) is skipped, so re-running a backfill request
    never duplicates trades, including against a real entry the live cron
    already placed for that exact date (see _already_backfilled()). Two
    different presets backfilling the same historical (symbol, date) still
    don't collide with each other, since the check is scoped per-preset.
    Never raises; returns {"ok": False, "error": ...} on any failure so a
    bad symbol/date doesn't
    abort the whole batch. Runs under _positions_lock end-to-end, same
    reasoning as place_entry_order().
    """
    symbol = signal["sym"]
    side = signal["side"]
    entry_date_str = entry_date.isoformat()

    with _positions_lock:
        positions = load_positions(data_dir)
        if _already_backfilled(positions, symbol, entry_date_str, preset):
            return {"ok": False, "error": f"[{preset}] {symbol} {entry_date_str} already backfilled — skipping"}
        return _backfill_paper_trade_locked(
            kite, data_dir, signal, entry_date, sl_pct, sl_type, exit_check_dates,
            preset, symbol, side, entry_date_str, positions, tp_type, tp_pct, hold_days)


def _backfill_paper_trade_locked(kite, data_dir: Path, signal: dict, entry_date: date,
                                  sl_pct: float, sl_type: str, exit_check_dates: list[date],
                                  preset: str, symbol: str, side: str, entry_date_str: str,
                                  positions: dict, tp_type: str, tp_pct: float, hold_days: int) -> dict:
    """Body of backfill_paper_trade() that runs under _positions_lock; not
    meant to be called directly."""
    live_inst = find_future_instrument(kite, symbol, resolve_expiry(date.today()))
    if live_inst is None:
        return {"ok": False, "error": f"No currently-live NFO future found for {symbol} "
                                       f"(needed as the continuous-mode anchor token)"}

    token = live_inst["instrument_token"]
    lot_size = live_inst["lot_size"]
    hist_expiry = resolve_expiry(entry_date)
    tradingsymbol = f"{live_inst['name']}{hist_expiry.strftime('%y%b').upper()}FUT"

    try:
        entry_candles = kite.historical_data(token, entry_date_str, entry_date_str, "day", continuous=True)
    except Exception as e:
        return {"ok": False, "error": f"historical_data failed for {tradingsymbol} on {entry_date_str}: {e}"}
    if not entry_candles:
        return {"ok": False, "error": f"No historical data for {tradingsymbol} on {entry_date_str} "
                                       f"(contract may not have been listed yet)"}

    # Entry fill = entry day's close, same close sl_price is measured from —
    # only used here to get that price, NOT to check for an SL cross. See
    # _paper_check_exit()'s docstring: checking the entry day's own
    # range against an SL computed from that same day's close compares the
    # SL to price action that happened mostly before the position existed
    # (entry is ~3:15 PM), and would flag false hits on any normal-range day.
    # SL exposure only starts the day AFTER entry — exactly what
    # backtest_overnight() itself checks ("tomorrow", never "today").
    entry_candle = entry_candles[0]
    fill_price = entry_candle["close"]

    if sl_type == "gap_fill":
        sl_price = fill_price
    else:
        sl_price = (fill_price * (1 - sl_pct / 100) if side == "LONG"
                    else fill_price * (1 + sl_pct / 100))
    sl_price = round(sl_price, 2)

    tp_price = None
    if tp_type == "pct" and fill_price:
        tp_price = (fill_price * (1 + tp_pct / 100) if side == "LONG"
                    else fill_price * (1 - tp_pct / 100))
        tp_price = round(tp_price, 2)

    status, exit_price, exit_date_str, days_checked = _resolve_backfill_exit(
        kite, token, tradingsymbol, side, fill_price, sl_price, tp_type, tp_price, exit_check_dates)

    pnl_pct, pnl_amount = (None, None) if status == "open" else _compute_pnl(side, fill_price, exit_price, lot_size)

    new_trade = {
        "preset": preset,  # which named preset produced this backfilled trade
        "mode": "paper", "backfilled": True,
        "direction": side, "entry_date": entry_date_str,
        "entry_order_id": None, "tradingsymbol": tradingsymbol, "lot_size": lot_size,
        "entry_price": fill_price,
        "signal_entry": signal["entry"], "signal_slLevel": signal["slLevel"],
        "sl_price": sl_price, "sl_price_source": "paper_simulated_fill",
        "tp_type": tp_type, "tp_pct": tp_pct, "tp_price": tp_price,
        "hold_days": max(1, hold_days or 1),
        # days_checked from _resolve_backfill_exit — for a still-open trade,
        # this is how many days were already resolved during backfill, so
        # close_open_positions() continues counting from here rather than
        # restarting at 0 (which would let it hold longer than hold_days).
        "days_held": days_checked,
        "gtt_id": None, "status": status,
    }
    if status != "open":
        new_trade.update(exit_price=exit_price, exit_date=exit_date_str, pnl_pct=pnl_pct, pnl_amount=pnl_amount)

    positions.setdefault(symbol, []).append(new_trade)
    save_positions(data_dir, positions)
    return {"ok": True, "position": new_trade}
