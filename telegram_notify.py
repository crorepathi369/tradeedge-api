"""
telegram_notify.py — Telegram alerts for the Gap automation.

Two kinds of message:
  1. Per-event — sent right after /gap-orders/enter and /gap-orders/exit,
     one message per result (entry placed, exit closed, no signal, or an
     error).
  2. Daily digest — sent once a day by /gap-orders/daily-digest (a
     separate cron trigger, meant to fire after both enter/exit have run),
     summarizing today's activity plus current open-position count.

Setup: set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID as Render env vars.
Get a token from @BotFather on Telegram; get chat_id by messaging your
bot once, then visiting https://api.telegram.org/bot<TOKEN>/getUpdates.

Every send is best-effort — never raises. A Telegram outage or missing
config must never block or fail the actual trading logic that triggered
the notification.
"""
from __future__ import annotations
import os
import urllib.request
import urllib.parse
from datetime import date

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(text: str) -> bool:
    """Sends a message via the Telegram Bot API. Returns False (never
    raises) on missing config or any send failure — logged, not fatal."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — skipping")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[telegram] send failed: {e}")
        return False


# ── Per-event messages ───────────────────────────────────────────────────────

def notify_entry_results(scan_date: str, mode: str, results: list[dict]) -> None:
    if not results:
        send_telegram(f"📭 <b>Gap Entry — {scan_date}</b>\nNo qualifying signal today.")
        return
    lines = [f"📥 <b>Gap Entry — {scan_date}</b> ({mode})"]
    for r in results:
        sym = r.get("sym", "?")
        if r.get("ok"):
            pos = r.get("position", {})
            lines.append(
                f"✅ {sym} {pos.get('direction','')} @ {pos.get('entry_price','?')} "
                f"| SL {pos.get('sl_price','?')} | {pos.get('tradingsymbol','')}"
            )
        else:
            lines.append(f"⚠️ {sym} — {r.get('error', 'unknown error')}")
    send_telegram("\n".join(lines))


def notify_exit_results(results: list[dict]) -> None:
    if not results:
        return  # nothing was open — no message needed for a no-op exit run
    lines = ["📤 <b>Gap Exit</b>"]
    for r in results:
        sym = r.get("sym", "?")
        if r.get("ok"):
            exit_price = r.get("exit_price")
            lines.append(f"✅ {sym} closed @ {exit_price if exit_price is not None else '?'} — {r.get('action','')}")
        else:
            lines.append(f"⚠️ {sym} — {r.get('error', 'unknown error')}")
    send_telegram("\n".join(lines))


def notify_error(context: str, message: str) -> None:
    """For hard failures that never made it to results — e.g. no Kite
    login, settings missing, scan crashed."""
    send_telegram(f"🚨 <b>Gap Automation Error</b> ({context})\n{message}")


# ── Daily digest ─────────────────────────────────────────────────────────────

def build_daily_digest(positions: dict, today: date | None = None) -> str:
    """
    Summarizes today's activity from gap_positions.json — entries opened
    today, exits closed today, and the current open-position count.
    `positions` is the {symbol: [trades]} dict from kite_orders.load_positions().
    """
    today_str = (today or date.today()).isoformat()

    entered_today, exited_today, still_open = [], [], []
    for sym, trades in positions.items():
        for t in trades:
            if t.get("entry_date") == today_str:
                entered_today.append((sym, t))
            if t.get("exit_date") == today_str:
                exited_today.append((sym, t))
        last = sorted(trades, key=lambda t: t.get("entry_date", ""))[-1] if trades else None
        if last and last.get("status") == "open":
            still_open.append((sym, last))

    lines = [f"📊 <b>Gap Automation Daily Digest — {today_str}</b>"]

    if entered_today:
        lines.append("\n<b>Entered today:</b>")
        for sym, t in entered_today:
            lines.append(f"• {sym} {t.get('direction','')} @ {t.get('entry_price','?')} (SL {t.get('sl_price','?')})")
    else:
        lines.append("\nNo entry today.")

    if exited_today:
        lines.append("\n<b>Exited today:</b>")
        for sym, t in exited_today:
            pnl_pct = t.get("pnl_pct")
            pnl_amt = t.get("pnl_amount")
            sign = "🟢" if (pnl_pct or 0) >= 0 else "🔴"
            lines.append(f"{sign} {sym} @ {t.get('exit_price','?')} | {pnl_pct}% (₹{pnl_amt})")
    else:
        lines.append("\nNo exit today.")

    lines.append(f"\n<b>Currently open:</b> {len(still_open)}")
    for sym, t in still_open:
        lines.append(f"• {sym} {t.get('direction','')} since {t.get('entry_date','?')}")

    return "\n".join(lines)


def notify_daily_digest(positions: dict, today: date | None = None) -> None:
    send_telegram(build_daily_digest(positions, today))
