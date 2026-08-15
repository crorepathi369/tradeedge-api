"""
gap_scan.py — Headless port of TradeEdge's Overnight Gap live scanner.

This mirrors, function-for-function, the JS logic in TradeEdge.html:
  - backtest()          -> backtest_overnight()
  - runScanner()         -> scan_gap_signals()   (Overnight branch only)
  - _scoreFromTrades()   -> score_from_trades()
  - _signalSortFn()      -> sorted(..., key=_signal_sort_key)

Kept as a separate module (not inline in app.py) so it can be unit-tested
in isolation and so the porting stays traceable against the JS source.

Reads OHLC from the same CSVs app.py already maintains in DATA_DIR
(date,open,high,low,close,adj_close) — no separate data source needed.
"""
from __future__ import annotations
import csv
import math
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from datetime import datetime


def js_round(x: float, ndigits: int = 0) -> float:
    """
    Matches JS rounding — NOT Python's built-in round(), which is
    round-half-to-even (banker's rounding) and silently disagreed with the
    frontend on any value landing on a .5 tie (e.g. bare round(62.5) gives
    62 in Python vs 63 in JS).

    ndigits=0 mirrors Math.round() (used for wilsonPct/histWR): always
    rounds .5 up, i.e. floor(x + 0.5).

    ndigits>0 mirrors .toFixed() (used for entry/slLevel/prices etc.):
    NOT the same tie-breaking as Math.round — toFixed rounds the *exact*
    binary value of the float, and floor(x*100+0.5)/100 introduces its own
    scaling error that can disagree right at a tie (e.g. 4923.5*1.03 is
    stored as 5071.204999999999927... — toFixed(2) correctly gives
    "5071.20", but floor(x*100+0.5)/100 overshoots to 5071.21). Decimal(x)
    (not Decimal(str(x))) captures that exact binary value, so quantizing
    it with ROUND_HALF_UP reproduces toFixed()'s result exactly.
    """
    if ndigits == 0:
        return int(math.floor(x + 0.5))
    quantum = Decimal(1).scaleb(-ndigits)
    return float(Decimal(x).quantize(quantum, rounding=ROUND_HALF_UP))


# ── OHLC loading ─────────────────────────────────────────────────────────────

def load_ohlc(data_dir: Path, symbol: str) -> list[dict]:
    """
    Load one symbol's CSV into a list of dicts sorted by date ascending.
    Mirrors the shape of _loadedOHLC[sym] in the frontend:
      {date, open, high, low, close, adjClose}
    """
    path = data_dir / f"{symbol}.csv"
    if not path.exists():
        return []
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            try:
                rows.append({
                    "date":  r["date"],
                    "open":  float(r["open"]),
                    "high":  float(r["high"]),
                    "low":   float(r["low"]),
                    "close": float(r["close"]),
                    "adjClose": float(r["adj_close"]) if r.get("adj_close") not in (None, "") else None,
                })
            except (ValueError, KeyError):
                continue
    rows.sort(key=lambda c: c["date"])
    return rows


# ── backtest() port ──────────────────────────────────────────────────────────

def backtest_overnight(ohlc: list[dict], sym_id: str, params: dict) -> list[dict]:
    """
    Faithful port of backtest(ohlc, symId, params) for the Overnight Gap
    strategy — TP types 'd2_close' (default), 'd2_open', 'pct'; SL types
    'pct' (fixed %) and 'gap_fill'.
    """
    trades = []
    n = len(ohlc)

    for i in range(n - 1):
        today = ohlc[i]
        yesterday = ohlc[i - 1] if i > 0 else None
        tomorrow = ohlc[i + 1]
        if yesterday is None:
            continue

        prev_ref = yesterday["adjClose"] or yesterday["close"]
        if not prev_ref:
            continue
        gap_pct = ((today["open"] - prev_ref) / prev_ref) * 100
        abs_gap = abs(gap_pct)
        if abs_gap < params["minGap"] or abs_gap > params["maxGap"]:
            continue

        gap_dir = "UP" if gap_pct > 0 else "DOWN"

        # Mandatory: D1 close vs prior close must ALSO be beyond minGap%, same
        # direction as the gap — confirms the move held through the close, not
        # just at the open. Mirrors backtest() in TradeEdge.html.
        change_pct = ((today["close"] - prev_ref) / prev_ref) * 100
        if gap_dir == "UP" and change_pct < params["minGap"]:
            continue
        if gap_dir == "DOWN" and change_pct > -params["minGap"]:
            continue

        # 0 = off. Reject if |D1 close vs prior close| exceeds this — caps
        # overextended gap-closes rather than requiring a minimum (that's the
        # mandatory check above).
        max_change_pct = params.get("maxChangePct", 0) or 0
        if max_change_pct > 0 and abs(change_pct) > max_change_pct:
            continue

        side = "LONG" if gap_dir == "UP" else "SHORT"
        entry = today["close"]
        next_open = tomorrow["open"]
        next_close = tomorrow["close"]

        sl_type = params.get("slType", "pct")
        sl_hit = False
        if sl_type == "gap_fill":
            sl = entry
            if side == "LONG" and next_open < entry:
                sl_hit = True
            if side == "SHORT" and next_open > entry:
                sl_hit = True
        else:
            sl_pct = params["slPct"] / 100
            sl = entry * (1 - sl_pct) if side == "LONG" else entry * (1 + sl_pct)

        def intraday_sl_check():
            hit = (tomorrow["low"] <= sl) if side == "LONG" else (tomorrow["high"] >= sl)
            if hit:
                return sl, "D2 SL (intraday)", True
            return next_close, "D2 Close", False

        tp_type = params.get("tpType", "d2_close")
        exit_price = exit_note = outcome = None

        if sl_hit:
            exit_price, outcome, exit_note = next_open, "SL", "D2 Open (SL)"

        elif tp_type == "d2_open":
            moved_in_favour = (next_open > entry) if side == "LONG" else (next_open < entry)
            if moved_in_favour:
                exit_price, exit_note, outcome = next_open, "D2 Open (TP)", "WIN"
            else:
                exit_price, exit_note, sl_hit = intraday_sl_check()
                if sl_hit:
                    outcome = "SL"
                else:
                    diff = (exit_price - entry) / entry
                    outcome = "FLAT" if abs(diff) < 0.00001 else (
                        "WIN" if (side == "LONG") == (exit_price > entry) else "LOSS"
                    )

        elif tp_type == "pct":
            tp_pct = params["tpPct"] / 100
            tp_level = entry * (1 + tp_pct) if side == "LONG" else entry * (1 - tp_pct)
            hold = max(1, params.get("holdDays", 1))
            resolved = False

            for h in range(1, hold + 1):
                if i + h >= n:
                    break
                day = ohlc[i + h]
                is_last_day = (h == hold) or (i + h + 1 >= n)

                sl_hit_open = (day["open"] < entry) if (sl_type == "gap_fill" and side == "LONG") else \
                              (day["open"] > entry) if (sl_type == "gap_fill" and side == "SHORT") else \
                              (day["open"] <= sl) if side == "LONG" else (day["open"] >= sl)
                if sl_hit_open:
                    exit_price, exit_note = day["open"], f"D{h+1} Open (SL gap)"
                    sl_hit, outcome, resolved = True, "SL", True
                    break

                tp_hit = (day["high"] >= tp_level) if side == "LONG" else (day["low"] <= tp_level)
                sl_hit_intra = (day["low"] <= sl) if side == "LONG" else (day["high"] >= sl)

                if sl_hit_intra:
                    exit_price, exit_note = sl, f"D{h+1} SL"
                    sl_hit, outcome, resolved = True, "SL", True
                    break
                elif tp_hit:
                    exit_price, exit_note = tp_level, f"TP hit D{h+1} ({params['tpPct']}%)"
                    outcome, resolved = "WIN", True
                    break

                if is_last_day:
                    exit_price = day["close"]
                    exit_note = "D2 Close (TP miss)" if h == 1 else f"D{h+1} Close (hold expired)"
                    diff = (exit_price - entry) / entry
                    outcome = "FLAT" if abs(diff) < 0.00001 else (
                        "WIN" if (side == "LONG") == (exit_price > entry) else "LOSS"
                    )
                    resolved = True

            if not resolved:
                last = ohlc[min(i + hold, n - 1)]
                exit_price, exit_note = last["close"], "D Close (data end)"
                diff = (exit_price - entry) / entry
                outcome = "FLAT" if abs(diff) < 0.00001 else (
                    "WIN" if (side == "LONG") == (exit_price > entry) else "LOSS"
                )

        else:  # tp_type == 'd2_close' (default)
            exit_price, exit_note, sl_hit = intraday_sl_check()
            if sl_hit:
                outcome = "SL"
            else:
                diff = (exit_price - entry) / entry
                outcome = "FLAT" if abs(diff) < 0.00001 else (
                    "WIN" if (side == "LONG") == (exit_price > entry) else "LOSS"
                )

        pnl_pct = ((exit_price - entry) / entry) * 100 if side == "LONG" else \
                  ((entry - exit_price) / entry) * 100

        trades.append({
            "sym": sym_id, "signalDate": today["date"], "tradeDate": tomorrow["date"],
            "side": side, "gapPct": js_round(gap_pct, 2), "entry": js_round(entry, 2),
            "sl": js_round(sl, 2), "exit": js_round(exit_price, 2), "exitNote": exit_note,
            "slHit": sl_hit, "outcome": outcome, "pnlPct": js_round(pnl_pct, 2),
        })

    return trades


# ── _scoreFromTrades() port ─────────────────────────────────────────────────

def score_from_trades(hist_trades: list[dict]) -> dict:
    scored = [t for t in hist_trades if t["outcome"] != "SKIP"]
    total = len(scored)
    wins = [t for t in scored if t["outcome"] == "WIN"]
    losses = [t for t in scored if t["outcome"] != "WIN"]
    avg_win = sum(t["pnlPct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnlPct"] for t in losses) / len(losses) if losses else 0
    expectancy = (len(wins) / total * avg_win) + (len(losses) / total * avg_loss) if total else 0
    return {"expectancy": expectancy, "avgWin": avg_win, "avgLoss": avg_loss, "total": total}


def _signal_sort_key(s: dict, ranking_mode: str) -> tuple:
    if ranking_mode == "expectancy":
        score = s["histExpectancy"]
    else:
        score = (s["histWins"] + 1) / (s["histCount"] + 2) if s["histCount"] > 0 else 0.5
    # Negative for descending sort on (score, |gapPct|)
    return (-score, -abs(s["gapPct"]))


# ── runScanner() port — Overnight Gap branch only ───────────────────────────

def scan_gap_signals(data_dir: Path, all_symbols: list[str], scan_date: str,
                      settings: dict) -> dict:
    """
    Faithful port of the Overnight branch of runScanner(). Returns:
      {"longs": [...], "shorts": [...], "selected": [...]}
    "selected" is longs+shorts after WR/PnL filtering, sorted, and capped
    to maxSignalsDay.

    Historical WR/expectancy is computed over settings['fromDate']..
    settings['toDate'] (or ..scan_date if toDate is blank/absent) — the
    exact same backtest window the app itself is configured with. No
    lookback-days guessing here: every parameter, including maxSignalsDay,
    comes straight from whatever preset's params the caller passes in.

    maxSignalsDay used to be hardcoded to 1 for all automation regardless
    of what the UI had configured ("per Raja") — a blanket safety cap from
    when there was only one active config. Now that automation runs
    per-preset (see /gap-orders/enter's loop over "Include in Automated
    Trades" presets), each preset's OWN configured cap applies instead —
    e.g. a deliberately curated "High-Conviction-Only" preset can take up
    to 5 entries/day while a tighter preset stays at 1, because that's
    what its own Cap Max/day field says. Defaults to 1 if a preset somehow
    omits it, same fail-safe as before.
    """
    params = {
        "minGap":      settings["minGap"],
        "maxGap":      settings["maxGap"],
        "slType":      settings.get("slType", "pct"),
        "slPct":       settings["slPct"],
        "tpType":      settings.get("tpType", "d2_close"),
        "tpPct":       settings.get("tpPct", 1.0),
        "holdDays":    settings.get("holdDays", 1),
        # 0 = off. Maximum |D1 close vs prior close| move allowed — caps
        # overextended gap-closes. The minimum-side check is now mandatory
        # (always minGap%, see backtest_overnight()/scan loop below).
        "maxChangePct": settings.get("maxChangePct", 0) or 0,
    }
    direction = settings.get("direction", "both")
    wr_threshold = settings.get("wrThreshold", 0) or 0
    pnl_threshold = settings.get("pnlThreshold", 0) or 0
    ranking_mode = settings.get("rankingMode", "expectancy")
    # Per-preset cap — see the docstring above for why this is no longer
    # hardcoded to 1 for every preset.
    max_per_day = settings.get("maxSignalsDay", 1) or 1

    from_date = settings.get("fromDate") or None
    to_date = settings.get("toDate") or scan_date

    longs, shorts = [], []

    for sym in all_symbols:
        ohlc = load_ohlc(data_dir, sym)
        if len(ohlc) < 2:
            continue

        idx = next((i for i, c in enumerate(ohlc) if c["date"] == scan_date), -1)
        if idx < 1:
            continue

        today, yesterday = ohlc[idx], ohlc[idx - 1]
        prev_ref = yesterday["adjClose"] or yesterday["close"]
        if not prev_ref:
            continue
        gap_pct = ((today["open"] - prev_ref) / prev_ref) * 100
        abs_gap = abs(gap_pct)
        if abs_gap < params["minGap"] or abs_gap > params["maxGap"]:
            continue

        gap_dir = "UP" if gap_pct > 0 else "DOWN"
        if direction == "long" and gap_dir != "UP":
            continue
        if direction == "short" and gap_dir != "DOWN":
            continue

        change_pct = ((today["close"] - prev_ref) / prev_ref) * 100
        if gap_dir == "UP" and change_pct < params["minGap"]:
            continue
        if gap_dir == "DOWN" and change_pct > -params["minGap"]:
            continue

        if params["maxChangePct"] > 0:
            if abs(change_pct) > params["maxChangePct"]:
                continue

        side = "LONG" if gap_dir == "UP" else "SHORT"
        if params["slType"] == "gap_fill":
            sl_level = today["close"]
        else:
            sl_pct = params["slPct"] / 100
            sl_level = today["close"] * (1 - sl_pct) if gap_dir == "UP" else today["close"] * (1 + sl_pct)

        tp_level = None
        if params["tpType"] == "pct":
            tp_pct = params["tpPct"] / 100
            tp_level = today["close"] * (1 + tp_pct) if gap_dir == "UP" else today["close"] * (1 - tp_pct)

        # Historical WR/expectancy over the app's own configured backtest
        # window (settings['fromDate']..settings['toDate']), synced from
        # the frontend — not a guessed lookback period.
        hist_ohlc = [c for c in ohlc if c["date"] <= to_date]
        if from_date:
            hist_ohlc = [c for c in hist_ohlc if c["date"] >= from_date]
        all_hist_trades = backtest_overnight(hist_ohlc, sym, params)
        hist_trades = [t for t in all_hist_trades if t["side"] == side and t["outcome"] != "SKIP"]
        hist_total = len(hist_trades)
        hist_wins = len([t for t in hist_trades if t["outcome"] == "WIN"])
        score = score_from_trades(hist_trades)

        signal = {
            "sym": sym, "gapPct": js_round(gap_pct, 2), "entry": js_round(today["close"], 2),
            "slLevel": js_round(sl_level, 2), "tpLevel": js_round(tp_level, 2) if tp_level else None,
            "gapDir": gap_dir, "side": side,
            "histWR": js_round(hist_wins / hist_total * 100) if hist_total >= 5 else None,
            "histWins": hist_wins, "histCount": hist_total,
            "histExpectancy": score["expectancy"],
            "histAvgPnl": (sum(t["pnlPct"] for t in hist_trades) / hist_total) if hist_total else 0,
            "d1Open": js_round(today["open"], 2), "d1High": js_round(today["high"], 2),
            "d1Low": js_round(today["low"], 2), "d1Close": js_round(today["close"], 2),
            "prevClose": js_round(prev_ref, 2),
        }

        has_backtest = hist_total > 0
        wilson_pct = js_round((hist_wins + 1) / (hist_total + 2) * 100) if hist_total > 0 else 50
        signal["wilsonPct"] = wilson_pct
        passes = not (has_backtest and (
            (wr_threshold > 0 and wilson_pct < wr_threshold) or
            (pnl_threshold != 0 and signal["histAvgPnl"] < pnl_threshold)
        ))
        signal["_passesFilter"] = passes

        (longs if gap_dir == "UP" else shorts).append(signal)

    longs.sort(key=lambda s: _signal_sort_key(s, ranking_mode))
    shorts.sort(key=lambda s: _signal_sort_key(s, ranking_mode))

    # Min Signals gate — a day-level "broad market momentum" filter, mirrors the JS
    # scanner exactly: skip the whole day (no entries at all) unless MORE than this
    # many raw candidates qualified, checked BEFORE the WR/PnL _passesFilter below.
    min_signals_gate = settings.get("minSignalsGate", 0) or 0
    raw_candidate_count = len(longs) + len(shorts)
    if min_signals_gate > 0 and raw_candidate_count <= min_signals_gate:
        return {"longs": longs, "shorts": shorts, "selected": [], "gatedOut": True,
                "rawCandidateCount": raw_candidate_count, "minSignalsGate": min_signals_gate}

    if max_per_day > 0:
        all_sigs = longs + shorts
        all_sigs.sort(key=lambda s: _signal_sort_key(s, ranking_mode))
        filtered = [s for s in all_sigs if s["_passesFilter"]]
        selected = filtered[:max_per_day]
    else:
        selected = [s for s in (longs + shorts) if s["_passesFilter"]]

    return {"longs": longs, "shorts": shorts, "selected": selected}
