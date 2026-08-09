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
import time
from pathlib import Path
from datetime import date, datetime, timedelta


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


def _last_trade(positions: dict, symbol: str) -> dict | None:
    trades = positions.get(symbol)
    return trades[-1] if trades else None


def clear_backfilled_trades(data_dir: Path) -> int:
    """
    Removes every trade tagged backfilled=True, leaving real paper/live
    trades untouched. For re-running a backfill after a logic fix — the
    (symbol, entry_date) idempotency check in backfill_paper_trade()
    would otherwise skip every date already covered by the buggy run,
    permanently stranding the bad records. Returns the count removed.
    """
    positions = load_positions(data_dir)
    removed = 0
    for symbol in list(positions.keys()):
        kept = [t for t in positions[symbol] if not t.get("backfilled")]
        removed += len(positions[symbol]) - len(kept)
        if kept:
            positions[symbol] = kept
        else:
            del positions[symbol]
    save_positions(data_dir, positions)
    return removed


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


def _paper_check_sl_and_exit(kite, tradingsymbol: str, direction: str, sl_price: float) -> tuple[float, str, bool, str]:
    """
    Simulates what a real GTT would have done, using real market data
    instead of a live order. Only ever checks the exit-check day (today) —
    NOT the entry day's own range. The entry itself happens at ~3:15 PM,
    right near the close that sl_price is computed FROM, so checking that
    same day's full daily low/high against it is comparing the SL to price
    action that happened mostly BEFORE the position existed — it isn't an
    approximation of real exposure, it's simply wrong, and would flag SL
    hits constantly on any day with normal intraday range. The real
    backtest engine (backtest_overnight()) never checks the entry day's
    own range either — only "tomorrow" — and this now matches that:

      1. Exit-check day (today): did the LIVE so-far high/low (kite.ohlc()
         — a free real-time call, no Historical API needed) cross sl_price?
      2. Not hit: exits at today's current LTP, same as a real EOD close
         would.

    Returns (exit_price, exit_date, sl_hit, note). Never raises — on any
    data-fetch failure, falls through to "exit at current LTP" rather than
    leaving a paper position stuck open forever.
    """
    today_str = date.today().isoformat()

    def _hit(low, high):
        return (low <= sl_price) if direction == "LONG" else (high >= sl_price)

    try:
        ohlc = kite.ohlc(f"NFO:{tradingsymbol}")[f"NFO:{tradingsymbol}"]["ohlc"]
        if _hit(ohlc["low"], ohlc["high"]):
            return sl_price, today_str, True, "paper SL hit (exit-day range so far)"
    except Exception as e:
        print(f"[gap-orders/paper] ohlc failed for {tradingsymbol}: {e}")

    ltp = _paper_entry_price(kite, tradingsymbol)  # same LTP read, different purpose here
    return (ltp, today_str, False, "paper closed at LTP (no SL cross detected)")


def place_entry_order(kite, data_dir: Path, signal: dict, today: date,
                       sl_pct: float, sl_type: str = "pct", mode: str = "paper") -> dict:
    """
    Enters `signal` (from gap_scan.scan_gap_signals' "selected" list) in
    the correct current/next-month future contract.

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

    sl_pct/sl_type come from the live Gap Settings at call time (not
    baked into signal), since they can change independently.

    Returns a result dict — either {"ok": True, "position": {...}} or
    {"ok": False, "error": "..."}. Never raises — callers (the cron
    endpoint) should just report whatever comes back.
    """
    symbol = signal["sym"]
    side = signal["side"]  # LONG / SHORT

    existing = _last_trade(load_positions(data_dir), symbol)
    if existing and existing.get("status") == "open":
        return {"ok": False, "error": f"{symbol} already has an open position from {existing.get('entry_date')} — skipping"}

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
        "mode": mode,  # 'paper' or 'live' — drives how close_open_positions() handles this one
        "direction": side, "entry_date": today.isoformat(),
        "entry_order_id": order_id, "tradingsymbol": tradingsymbol,
        "lot_size": lot_size,
        "entry_price": fill_price,           # actual (live) or simulated (paper) futures fill
        "signal_entry": signal["entry"],      # cash-market close at signal time — for slippage vs actual fill
        "signal_slLevel": signal["slLevel"],  # cash-market reference, for comparison
        "sl_price": sl_price,
        "sl_price_source": sl_price_source,
        "gtt_id": gtt_id, "status": "open",
    }
    positions.setdefault(symbol, []).append(new_trade)
    save_positions(data_dir, positions)

    return {"ok": True, "position": new_trade}


# ── Exit ─────────────────────────────────────────────────────────────────────

def close_open_positions(kite, data_dir: Path) -> list[dict]:
    """
    For every position with status 'open', closes it the way appropriate
    to its own stored mode — 'live' positions check Kite's real position
    book and place a real exit order if the GTT hasn't already closed it;
    'paper' positions get checked against real market data via
    _paper_check_sl_and_exit() instead, no real orders anywhere. Mode is
    read per-position, not passed in globally, so a Paper→Live toggle
    mid-flight never changes how an already-open paper position gets
    closed. Returns a list of per-symbol result dicts. Called by the
    next-day 3:15 PM job.
    """
    positions = load_positions(data_dir)
    if not positions:
        return []

    # Only the LAST trade per symbol can ever be 'open' — place_entry_order()
    # refuses a new entry while the last one is still open, so there's no
    # scenario where an earlier, non-last entry is open underneath a closed
    # or newer one.
    open_items = {s: pl[-1] for s, pl in positions.items() if pl and pl[-1].get("status") == "open"}
    if not open_items:
        return []

    # Only live positions need Kite's real position book — skip the call
    # entirely (and don't let its failure block paper closes) if there
    # are none open right now.
    live_positions = {}
    if any(p.get("mode", "live") == "live" for p in open_items.values()):
        try:
            live_positions = {p["tradingsymbol"]: p for p in kite.positions()["net"]}
        except Exception as e:
            print(f"[gap-orders] Could not fetch live positions: {e}")

    today_str = date.today().isoformat()
    results = []
    for symbol, pos in open_items.items():
        mode = pos.get("mode", "live")  # positions entered before mode existed default to live
        exit_txn_type = kite.TRANSACTION_TYPE_SELL if pos["direction"] == "LONG" else kite.TRANSACTION_TYPE_BUY

        if mode == "paper":
            exit_price, exit_date, sl_hit, note = _paper_check_sl_and_exit(
                kite, pos["tradingsymbol"], pos["direction"], pos["sl_price"])
            pnl_pct, pnl_amount = _compute_pnl(pos["direction"], pos.get("entry_price"), exit_price, pos["lot_size"])
            pos["status"] = "closed_by_sl" if sl_hit else "closed_eod"
            pos["exit_price"] = exit_price
            pos["exit_date"] = exit_date
            pos["pnl_pct"] = pnl_pct
            pos["pnl_amount"] = pnl_amount
            results.append({"sym": symbol, "ok": True, "action": note, "exit_price": exit_price})
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
            results.append({"sym": symbol, "ok": True, "action": "already_closed", "exit_price": exit_price})
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
            results.append({"sym": symbol, "ok": True, "action": "exited", "order_id": order_id, "exit_price": exit_price})
        except Exception as e:
            results.append({"sym": symbol, "ok": False, "error": str(e)})

    save_positions(data_dir, positions)
    return results


# ── Backfill ─────────────────────────────────────────────────────────────────
# Retroactively populates paper trades for past signal days, so Paper mode's
# mechanics can be validated against a month of real market history without
# waiting for it to run forward day by day. Uses the same day-granularity SL
# check as the live paper path (kite.historical_data(), 'day' interval) —
# just for a specific past date instead of "today".

def _already_backfilled(positions: dict, symbol: str, entry_date: str) -> bool:
    return any(t.get("backfilled") and t.get("entry_date") == entry_date
               for t in positions.get(symbol, []))


def backfill_paper_trade(kite, data_dir: Path, signal: dict, entry_date: date,
                          sl_pct: float, sl_type: str, exit_check_date: date | None) -> dict:
    """
    Creates one backfilled paper trade for a past `signal` (from
    gap_scan.scan_gap_signals' "selected" list for that historical date).

    entry_date: the historical signal day being backfilled.
    exit_check_date: the next date in the caller's own backfill date
    sequence — not a naive +1 calendar day, so it always lines up with an
    actual trading day the caller already knows has data. If None (this is
    the most recent date in the backfill window, with no "next day" data
    available yet), the trade is left status='open' and un-exited — the
    regular /gap-orders/exit → close_open_positions() paper path finishes
    it correctly the next time it runs, exactly as it would for a same-day
    live paper entry (kite.ohlc() covers "today" either way).

    Idempotent — a (symbol, entry_date) pair already backfilled is skipped,
    so re-running a backfill request doesn't duplicate trades. Never
    raises; returns {"ok": False, "error": ...} on any failure so a bad
    symbol/date doesn't abort the whole batch.
    """
    symbol = signal["sym"]
    side = signal["side"]
    entry_date_str = entry_date.isoformat()

    positions = load_positions(data_dir)
    if _already_backfilled(positions, symbol, entry_date_str):
        return {"ok": False, "error": f"{symbol} {entry_date_str} already backfilled — skipping"}

    expiry = resolve_expiry(entry_date)
    inst = find_future_instrument(kite, symbol, expiry)
    if inst is None:
        return {"ok": False, "error": f"No NFO future found for {symbol} exp {expiry}"}

    tradingsymbol = inst["tradingsymbol"]
    lot_size = inst["lot_size"]
    token = inst["instrument_token"]

    try:
        entry_candles = kite.historical_data(token, entry_date_str, entry_date_str, "day")
    except Exception as e:
        return {"ok": False, "error": f"historical_data failed for {tradingsymbol} on {entry_date_str}: {e}"}
    if not entry_candles:
        return {"ok": False, "error": f"No historical data for {tradingsymbol} on {entry_date_str} "
                                       f"(contract may not have been listed yet)"}

    # Entry fill = entry day's close, same close sl_price is measured from —
    # only used here to get that price, NOT to check for an SL cross. See
    # _paper_check_sl_and_exit()'s docstring: checking the entry day's own
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

    def _hit(low, high):
        return (low <= sl_price) if side == "LONG" else (high >= sl_price)

    status, exit_price, exit_date_str = "open", None, None
    if exit_check_date is not None:
        exit_date_str = exit_check_date.isoformat()
        try:
            exit_candles = kite.historical_data(token, exit_date_str, exit_date_str, "day")
        except Exception as e:
            exit_candles = []
            print(f"[gap-orders/backfill] historical_data failed for {tradingsymbol} on {exit_date_str}: {e}")
        if exit_candles:
            ec = exit_candles[0]
            status, exit_price = ("closed_by_sl", sl_price) if _hit(ec["low"], ec["high"]) else ("closed_eod", ec["close"])
        # else: no data for the next date either — leave open rather than guess

    pnl_pct, pnl_amount = (None, None) if status == "open" else _compute_pnl(side, fill_price, exit_price, lot_size)

    new_trade = {
        "mode": "paper", "backfilled": True,
        "direction": side, "entry_date": entry_date_str,
        "entry_order_id": None, "tradingsymbol": tradingsymbol, "lot_size": lot_size,
        "entry_price": fill_price,
        "signal_entry": signal["entry"], "signal_slLevel": signal["slLevel"],
        "sl_price": sl_price, "sl_price_source": "paper_simulated_fill",
        "gtt_id": None, "status": status,
    }
    if status != "open":
        new_trade.update(exit_price=exit_price, exit_date=exit_date_str, pnl_pct=pnl_pct, pnl_amount=pnl_amount)

    positions.setdefault(symbol, []).append(new_trade)
    save_positions(data_dir, positions)
    return {"ok": True, "position": new_trade}
