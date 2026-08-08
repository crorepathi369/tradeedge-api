"""
kite_orders.py — Order lifecycle for the Gap strategy automation.

Three responsibilities:
  1. Contract resolution — pick current-month NFO future, rolling to
     next-month if the signal date is <4 days from expiry (last Tuesday
     of the month), per Raja's rule.
  2. Entry — place market order + a GTT stop-loss order right after.
  3. Exit  — next trading day, close any position not already stopped
     out by its GTT.

State is a single JSON file (gap_positions.json) in DATA_DIR, one entry
per open/recently-closed position, keyed by symbol. This lets the exit
job run independently of the entry job and survive a Render restart
mid-cycle (positions.json rides along with the GitHub data-branch
backup, same as the CSVs).
"""
from __future__ import annotations
import json
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

def _positions_file(data_dir: Path) -> Path:
    return data_dir / "gap_positions.json"


def load_positions(data_dir: Path) -> dict:
    path = _positions_file(data_dir)
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (ValueError, FileNotFoundError):
        return {}


def save_positions(data_dir: Path, positions: dict) -> None:
    with open(_positions_file(data_dir), "w") as f:
        json.dump(positions, f, indent=2, default=str)


# ── Entry ────────────────────────────────────────────────────────────────────

def place_entry_order(kite, data_dir: Path, signal: dict, today: date) -> dict:
    """
    Places a market order for `signal` (from gap_scan.scan_gap_signals'
    "selected" list) in the correct current/next-month future contract,
    then a GTT stop-loss order, then records the position.

    Returns a result dict — either {"ok": True, "position": {...}} or
    {"ok": False, "error": "..."}. Never raises — callers (the cron
    endpoint) should just report whatever comes back.
    """
    symbol = signal["sym"]
    side = signal["side"]  # LONG / SHORT
    sl_price = signal["slLevel"]

    existing = load_positions(data_dir).get(symbol)
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

    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR, exchange="NFO",
            tradingsymbol=tradingsymbol, transaction_type=txn_type,
            quantity=lot_size, order_type=kite.ORDER_TYPE_MARKET,
            product=kite.PRODUCT_NRML,
        )
    except Exception as e:
        return {"ok": False, "error": f"Entry order failed: {e}"}

    # GTT stop-loss — single-leg trigger that fires a market order on the
    # opposite side if price crosses sl_price, independent of anything
    # running server-side after this.
    gtt_id = None
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
    positions[symbol] = {
        "direction": side, "entry_date": today.isoformat(),
        "entry_order_id": order_id, "tradingsymbol": tradingsymbol,
        "lot_size": lot_size, "sl_price": sl_price,
        "gtt_id": gtt_id, "status": "open",
    }
    save_positions(data_dir, positions)

    return {"ok": True, "position": positions[symbol]}


# ── Exit ─────────────────────────────────────────────────────────────────────

def close_open_positions(kite, data_dir: Path) -> list[dict]:
    """
    For every position with status 'open', checks whether its GTT already
    fired (position no longer in Kite's live positions) and, if not,
    places a market exit order and deletes the GTT. Returns a list of
    per-symbol result dicts. Called by the next-day 3:15 PM job.
    """
    positions = load_positions(data_dir)
    if not positions:
        return []

    try:
        live_positions = {p["tradingsymbol"]: p for p in kite.positions()["net"]}
    except Exception as e:
        return [{"ok": False, "error": f"Could not fetch live positions: {e}"}]

    results = []
    for symbol, pos in positions.items():
        if pos.get("status") != "open":
            continue

        live = live_positions.get(pos["tradingsymbol"])
        still_open = live is not None and live.get("quantity", 0) != 0

        if not still_open:
            # GTT already closed it (or it was closed manually) — just
            # reconcile state, no order needed.
            pos["status"] = "closed_by_sl"
            results.append({"sym": symbol, "ok": True, "action": "already_closed"})
            continue

        exit_txn_type = kite.TRANSACTION_TYPE_SELL if pos["direction"] == "LONG" else kite.TRANSACTION_TYPE_BUY
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
            pos["status"] = "closed_eod"
            pos["exit_order_id"] = order_id
            results.append({"sym": symbol, "ok": True, "action": "exited", "order_id": order_id})
        except Exception as e:
            results.append({"sym": symbol, "ok": False, "error": str(e)})

    save_positions(data_dir, positions)
    return results
