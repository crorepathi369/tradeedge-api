"""
push_incr_data.py — Push updated CSVs from tradeedge_data/ to GitHub 'data' branch.

Run this AFTER tradeedge_fetch.py --fo --merge --days 5

Usage:
    python push_incr_data.py                        # push all CSVs modified today
    python push_incr_data.py --all                  # push all 211 CSVs (full sync)
    python push_incr_data.py --symbols RELIANCE TCS # push specific symbols only
    python push_incr_data.py --dry-run              # show what would be pushed, don't push

Setup (one time):
    Set GITHUB_TOKEN env var — Personal Access Token with 'repo' scope
    OR create a .env file in same folder:
        GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx

    Optionally override defaults:
        GITHUB_REPO=crorepathi369/tradeedge-api
        GITHUB_DATA_BRANCH=data
        TRADEEDGE_DATA_DIR=./tradeedge_data
"""
from __future__ import annotations
import os, sys, base64, json, time, argparse
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime, date

# ── Load .env if present (simple parser, no dotenv dependency) ─────────────────
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Config ─────────────────────────────────────────────────────────────────────
TOKEN           = os.environ.get("GITHUB_TOKEN", "")
REPO            = os.environ.get("GITHUB_REPO",        "crorepathi369/tradeedge-api")
BRANCH          = os.environ.get("GITHUB_DATA_BRANCH", "data")
RENDER_API_URL  = os.environ.get("RENDER_API_URL",     "https://tradeedge-api.onrender.com")
FETCH_SECRET    = os.environ.get("FETCH_SECRET",       "")

def _find_data_dir() -> Path:
    """
    Auto-detect the tradeedge_data folder by searching common locations.
    Priority:
      1. TRADEEDGE_DATA_DIR env var (explicit override)
      2. ./tradeedge_data          (script's own folder)
      3. ~/tradeedge_data          (home directory)
      4. ~/TradeEdge/tradeedge_data
      5. ~/Downloads/tradeedge_data
      6. Any folder named tradeedge_data found 2 levels up from script
      7. Any folder on Desktop containing *.csv files named like stock symbols
    """
    # 1. Explicit env var
    env_val = os.environ.get("TRADEEDGE_DATA_DIR", "")
    if env_val:
        p = Path(env_val)
        if p.exists():
            return p

    script_dir = Path(__file__).parent.resolve()

    # 2-5. Fixed candidate paths
    candidates = [
        script_dir / "tradeedge_data",
        script_dir / "TradeEdge_data",
        Path.home() / "tradeedge_data",
        Path.home() / "TradeEdge" / "tradeedge_data",
        Path.home() / "Downloads" / "tradeedge_data",
        Path.home() / "Desktop" / "tradeedge_data",
        Path.home() / "Documents" / "tradeedge_data",
        # also try the script dir itself (some setups put CSVs alongside the script)
        script_dir,
    ]
    for p in candidates:
        if p.exists() and list(p.glob("*.csv")):
            return p

    # 6. Walk up 3 levels from script and search for tradeedge_data folder
    for parent in [script_dir, script_dir.parent, script_dir.parent.parent]:
        for child in parent.iterdir() if parent.exists() else []:
            if child.is_dir() and "tradeedge" in child.name.lower() and list(child.glob("*.csv")):
                return child

    # 7. Check Desktop for any folder with NSE-style CSVs
    desktop = Path.home() / "Desktop"
    if desktop.exists():
        for folder in desktop.iterdir():
            if folder.is_dir() and len(list(folder.glob("*.csv"))) > 10:
                return folder

    return Path("./tradeedge_data")   # fallback — will fail gracefully in main()

DATA_DIR = _find_data_dir()

# How recently a file must have been modified to count as "updated today"
MODIFIED_WITHIN_HOURS = 6   # covers 3 PM fetch even if script runs at 4 PM

# GitHub API rate limit: 5000 req/hour authenticated.
# With 211 files × 2 calls (GET sha + PUT) = 422 calls — well within limits.
# We add a small delay anyway to be polite.
API_DELAY = 0.05   # seconds between API calls

# ── GitHub API helpers ─────────────────────────────────────────────────────────

def _gh_request(path: str, method: str = "GET", body: dict | None = None) -> dict:
    """Make an authenticated GitHub API call. Raises on HTTP errors."""
    url = f"https://api.github.com/repos/{REPO}/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"token {TOKEN}",
            "Accept":        "application/vnd.github.v3+json",
            "Content-Type":  "application/json",
            "User-Agent":    "TradeEdge-push-incr",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        raise RuntimeError(f"GitHub API {method} {path} → HTTP {e.code}: {body_text}") from e


def _get_file_sha(filename: str) -> str | None:
    """Return the blob SHA of an existing file in the data branch, or None if not found."""
    try:
        info = _gh_request(f"contents/{filename}?ref={BRANCH}")
        return info.get("sha")
    except RuntimeError as e:
        if "HTTP 404" in str(e):
            return None
        raise


def _push_file(filepath: Path, dry_run: bool = False) -> str:
    """
    Create or update a single file in the GitHub data branch.
    Returns 'created', 'updated', or 'skipped'.
    """
    filename = filepath.name
    content  = base64.b64encode(filepath.read_bytes()).decode()

    # Get current SHA (needed for updates — GitHub rejects PUT without SHA on existing files)
    sha = _get_file_sha(filename)
    time.sleep(API_DELAY)

    if dry_run:
        action = "would update" if sha else "would create"
        return action

    commit_msg = f"Auto-update {filename} {date.today().isoformat()}"
    body: dict = {
        "message": commit_msg,
        "content": content,
        "branch":  BRANCH,
    }
    if sha:
        body["sha"] = sha

    _gh_request(f"contents/{filename}", method="PUT", body=body)
    time.sleep(API_DELAY)
    return "updated" if sha else "created"


# ── File selection ─────────────────────────────────────────────────────────────

def _files_modified_today() -> list[Path]:
    """Return CSV files + SECTOR_MAP.json modified within MODIFIED_WITHIN_HOURS."""
    cutoff = datetime.now().timestamp() - MODIFIED_WITHIN_HOURS * 3600
    result = []
    for f in DATA_DIR.glob("*.csv"):
        if f.stat().st_mtime >= cutoff:
            result.append(f)
    sm = DATA_DIR / "SECTOR_MAP.json"
    if sm.exists() and sm.stat().st_mtime >= cutoff:
        result.append(sm)
    return sorted(result, key=lambda f: f.name)


def _all_files() -> list[Path]:
    """Return all CSVs + SECTOR_MAP.json in DATA_DIR."""
    files = sorted(DATA_DIR.glob("*.csv"), key=lambda f: f.name)
    sm = DATA_DIR / "SECTOR_MAP.json"
    if sm.exists():
        files.append(sm)
    return files


def _symbol_files(symbols: list[str]) -> list[Path]:
    """Return CSV files for specific symbols."""
    result = []
    for sym in symbols:
        p = DATA_DIR / f"{sym.upper()}.csv"
        if p.exists():
            result.append(p)
        else:
            print(f"  ⚠  {sym}.csv not found in {DATA_DIR} — skipping")
    return result


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Push incremental CSV updates to GitHub data branch"
    )
    parser.add_argument("--all",     action="store_true",
                        help="Push all CSVs (not just today's updates)")
    parser.add_argument("--symbols", nargs="+", metavar="SYM",
                        help="Push specific symbols only, e.g. --symbols RELIANCE TCS")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be pushed without actually pushing")
    parser.add_argument("--data-dir", default=None,
                        help="Override DATA_DIR path")
    args = parser.parse_args()

    global DATA_DIR
    if args.data_dir:
        DATA_DIR = Path(args.data_dir)

    # ── Validate ───────────────────────────────────────────────────────────────
    # Re-read token at runtime (GitHub Actions sets it as env var after .env load)
    global TOKEN, REPO, BRANCH, RENDER_API_URL, FETCH_SECRET
    TOKEN          = os.environ.get("GITHUB_TOKEN", TOKEN)
    REPO           = os.environ.get("GITHUB_REPO",        REPO)
    BRANCH         = os.environ.get("GITHUB_DATA_BRANCH", BRANCH)
    RENDER_API_URL = os.environ.get("RENDER_API_URL",     RENDER_API_URL)
    FETCH_SECRET   = os.environ.get("FETCH_SECRET",       FETCH_SECRET)

    # In GitHub Actions, GITHUB_REPOSITORY is set automatically
    if not REPO or REPO == "crorepathi369/tradeedge-api":
        gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
        if gh_repo:
            REPO = gh_repo

    if not TOKEN:
        print("✗ GITHUB_TOKEN not set.")
        print("  Set it as an environment variable or add to a .env file:")
        print("      GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx")
        sys.exit(1)

    if not DATA_DIR.exists():
        print(f"✗ Could not find tradeedge_data folder.")
        print(f"  Searched common locations — none contained CSV files.")
        print(f"  Fix: set TRADEEDGE_DATA_DIR env var to the full path, e.g.:")
        print(f"      export TRADEEDGE_DATA_DIR=/Users/yourname/TradeEdge/tradeedge_data")
        print(f"  Or add to your .env file:")
        print(f"      TRADEEDGE_DATA_DIR=/Users/yourname/TradeEdge/tradeedge_data")
        sys.exit(1)

    # ── Select files ──────────────────────────────────────────────────────────
    if args.symbols:
        files = _symbol_files(args.symbols)
        mode  = f"specific symbols ({len(files)} files)"
    elif args.all:
        files = _all_files()
        mode  = f"ALL files ({len(files)} files)"
    else:
        files = _files_modified_today()
        mode  = f"files modified in last {MODIFIED_WITHIN_HOURS}h ({len(files)} files)"

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  TradeEdge — push_incr_data")
    print(f"  Repo   : {REPO}")
    print(f"  Branch : {BRANCH}")
    print(f"  DataDir: {DATA_DIR}")
    print(f"  Mode   : {mode}")
    print(f"  DryRun : {args.dry_run}")
    print(f"{'='*60}\n")

    if not files:
        print("  Nothing to push — no files modified recently.")
        print(f"  Run with --all to force push all CSVs,")
        print(f"  or check that tradeedge_fetch.py wrote to: {DATA_DIR}")
        return

    # ── Push files ────────────────────────────────────────────────────────────
    t0 = time.time()
    ok = created = updated = failed = 0

    for i, filepath in enumerate(files, 1):
        prefix = f"  [{i:>3}/{len(files)}] {filepath.name:<22}"
        try:
            result = _push_file(filepath, dry_run=args.dry_run)
            print(f"{prefix} ✓ {result}")
            ok += 1
            if "creat" in result: created += 1
            else:                 updated += 1
        except Exception as e:
            print(f"{prefix} ✗ FAILED — {e}")
            failed += 1

    elapsed = round(time.time() - t0, 1)

    print(f"\n{'='*60}")
    if args.dry_run:
        print(f"  DRY RUN — nothing was pushed")
        print(f"  Would push: {ok} files  ({created} new, {updated} updates)")
    else:
        print(f"  ✓ {ok} pushed  ({created} new, {updated} updated)  |  ✗ {failed} failed")
        print(f"  Time: {elapsed}s")
        if ok:
            # ── Auto-trigger Render to pull fresh CSVs immediately ────────────
            _trigger_render_pull()
    if failed:
        print(f"\n  ✗ {failed} file(s) failed — check GITHUB_TOKEN permissions (needs 'repo' scope)")
    print(f"{'='*60}\n")

    sys.exit(1 if failed and not ok else 0)


def _trigger_render_pull():
    """
    After a successful push to GitHub, tell Render to pull the fresh CSVs
    immediately via /pull-from-github — no restart needed.
    """
    if not RENDER_API_URL:
        return

    url = RENDER_API_URL.rstrip("/") + "/pull-from-github"
    if FETCH_SECRET:
        url += f"?secret={FETCH_SECRET}"

    print(f"\n  Triggering Render pull → {RENDER_API_URL}/pull-from-github")
    req = urllib.request.Request(url, headers={"User-Agent": "TradeEdge-push-incr"})
    try:
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
        if resp.get("job_started"):
            print(f"  ✓ Render pull started — fresh CSVs will be live in ~2 min")
            print(f"  ✓ Monitor: {RENDER_API_URL}/pull-status")
        else:
            print(f"  ⚠ Render pull not started: {resp.get('reason', 'unknown')}")
    except Exception as e:
        print(f"  ⚠ Could not trigger Render pull: {e}")
        print(f"  Manual trigger: {url}")


if __name__ == "__main__":
    main()
