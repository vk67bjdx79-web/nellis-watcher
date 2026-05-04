#!/usr/bin/env python3
"""
Nellis Auction Houston Watcher
==============================
Polls the Algolia search index used by nellisauction.com every N seconds.
Finds Houston items with:
  - 0-2 total bids
  - Current bid <= $5
  - Estimated retail > $100
  - Opportunity score >= 7/10
  - Less than 5 minutes remaining

Features:
  - Item photo attached to Pushover notifications
  - Smart scheduling: 15s (5pm-10pm CST), 3min otherwise
  - Opportunity score 1-10 (ROI + urgency + bid scarcity)
  - eBay sold price lookup + profit estimate
  - Google Sheets logging (optional via env vars)
  - Flask health check on PORT (default 8080)
"""

import io
import os
import re
import time
import json
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from flask import Flask

try:
    import gspread
    from google.oauth2.service_account import Credentials as SACredentials
    _GSPREAD_OK = True
except ImportError:
    _GSPREAD_OK = False

# ── Pushover ──────────────────────────────────────────────────────────────────
PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY  = os.environ.get("PUSHOVER_USER_KEY", "")
PUSHOVER_URL       = "https://api.pushover.net/1/messages.json"

# ── Algolia (public credentials embedded in nellisauction.com) ───────────────
ALGOLIA_APP_ID  = "GL1QVP8R29"
ALGOLIA_API_KEY = "d22f83c614aa8eda28fa9eadda0d07b9"
ALGOLIA_INDEX   = "nellisauction-prd_bids_asc"
ALGOLIA_URL     = (
    f"https://{ALGOLIA_APP_ID}-dsn.algolia.net"
    f"/1/indexes/{ALGOLIA_INDEX}/query"
)
ALGOLIA_HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key":        ALGOLIA_API_KEY,
    "Content-Type":             "application/json",
}

NELLIS_HOUSTON_URL = "https://www.nellisauction.com/browse?location=houston"

# ── Tuning knobs ──────────────────────────────────────────────────────────────
MAX_BIDS        = 5
MIN_RETAIL      = 100.0
MAX_CURRENT_BID = 5.0
ALERT_WINDOW    = 300    # notify when < this many seconds remain (5 min)
LOOK_AHEAD      = 3600   # only fetch items closing within 1 hour

# ── Smart scheduling ──────────────────────────────────────────────────────────
HOUSTON_TZ         = ZoneInfo("America/Chicago")
PEAK_START         = 17    # 5 PM CST
PEAK_END           = 22    # 10 PM CST
PEAK_POLL_INTERVAL = 15    # seconds during peak hours
OFF_POLL_INTERVAL  = 180   # 3 minutes outside peak hours

# ── Google Sheets (optional) ──────────────────────────────────────────────────
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.environ.get("GOOGLE_SHEET_ID", "")
_SHEETS_SCOPES          = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEET_HEADERS = [
    "Timestamp", "Title", "Current Price (est.)", "Retail Value",
    "eBay Sold Price", "eBay Sold Date", "Est. Profit",
    "Score", "Time Left", "URL",
]

# ── State ─────────────────────────────────────────────────────────────────────
notified: set[str] = set()
_stats: dict = {"polls": 0, "notified": 0, "last_poll": "never", "started": ""}
HTTP_PORT = int(os.environ.get("PORT", 8080))


# ── Flask health server ────────────────────────────────────────────────────────

_app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)


@_app.route("/", defaults={"path": ""})
@_app.route("/<path:path>")
def health(path: str) -> tuple[str, int]:
    return "OK", 200


def _start_health_server() -> None:
    _app.run(host="0.0.0.0", port=HTTP_PORT, use_reloader=False)


# ── Google Sheets ─────────────────────────────────────────────────────────────

_sheet = None


def _init_sheets() -> None:
    global _sheet
    if not _GSPREAD_OK:
        print("  Google Sheets: gspread not installed — skipping")
        return
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        print("  Google Sheets: GOOGLE_CREDENTIALS_JSON / GOOGLE_SHEET_ID not set — skipping")
        return
    try:
        creds_dict   = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds        = SACredentials.from_service_account_info(creds_dict, scopes=_SHEETS_SCOPES)
        client       = gspread.authorize(creds)
        spreadsheet  = client.open_by_key(GOOGLE_SHEET_ID)
        _sheet       = spreadsheet.sheet1
        if not _sheet.row_values(1):
            _sheet.append_row(_SHEET_HEADERS)
        print("  Google Sheets: connected ✓")
    except Exception as exc:
        print(f"  Google Sheets: setup failed — {exc}")
        _sheet = None


def _log_to_sheets(row: list) -> None:
    if _sheet is None:
        return
    try:
        _sheet.append_row(row)
    except Exception as exc:
        print(f"  [Sheets error] {exc}")


# ── Opportunity scorer ────────────────────────────────────────────────────────

def opportunity_score(retail: float, secs_left: int) -> int:
    """
    Score 1-10:
      Bid scarcity  3 pts  (fixed — all items have 0-2 bids)
      ROI           1-4 pts (retail / $5 max bid)
      Urgency       0-3 pts (time remaining)

    Threshold: MIN_SCORE (default 7) triggers a Pushover alert.
    """
    score = 3  # bid scarcity always max given the <=2 bids filter

    roi = retail / MAX_CURRENT_BID
    if roi >= 60:        score += 4   # retail >= $300
    elif roi >= 40:      score += 3   # retail >= $200
    elif roi >= 20:      score += 2   # retail >= $100
    else:                score += 1

    if secs_left < 120:              score += 3   # < 2 min
    elif secs_left < 240:            score += 2   # < 4 min
    elif secs_left < ALERT_WINDOW:   score += 1   # < 5 min

    return min(10, max(1, score))


# ── eBay sold price lookup ────────────────────────────────────────────────────

_EBAY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _clean_title_for_search(title: str) -> str:
    """Strip auction noise and return first ~50 chars for eBay search."""
    cleaned = re.sub(r"\*\*.*?\*\*", "", title)   # remove **NOTE** blocks
    cleaned = re.sub(r"\(.*?\)", "", cleaned)       # remove parentheticals
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > 50:
        cleaned = cleaned[:50].rsplit(" ", 1)[0]
    return cleaned


def fetch_ebay_sold(title: str) -> tuple[float | None, str | None]:
    """
    Scrape eBay completed listings sorted by most recent.
    Returns (sold_price, sold_date_str) or (None, None) on failure.
    """
    query = _clean_title_for_search(title)
    if not query:
        return None, None
    url = (
        "https://www.ebay.com/sch/i.html?"
        f"_nkw={quote_plus(query)}&LH_Sold=1&LH_Complete=1&_sop=13&_ipg=5"
    )
    try:
        resp = requests.get(url, headers=_EBAY_HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception:
        return None, None

    soup  = BeautifulSoup(resp.text, "lxml")
    items = soup.select(".s-item")
    for item in items:
        title_el = item.select_one(".s-item__title")
        if not title_el or "Shop on eBay" in title_el.text:
            continue
        price_el = item.select_one(".s-item__price")
        date_el  = item.select_one(".s-item__end-time, .SECONDARY_INFO")
        if not price_el:
            continue

        price_text  = price_el.text.strip().split(" to ")[0]   # handle ranges
        price_match = re.search(r"[\d,]+\.?\d*", price_text.replace(",", ""))
        if not price_match:
            continue
        try:
            price = float(price_match.group().replace(",", ""))
        except ValueError:
            continue

        date_str = date_el.text.strip() if date_el else "unknown"
        date_str = re.sub(r"(?i)^sold\s*", "", date_str).strip()
        return price, date_str

    return None, None


def calc_profit(ebay_price: float) -> float:
    """eBay price × 0.85 (15% fees) − $12 shipping − $5 max bid cost."""
    return ebay_price * 0.85 - 12.0 - MAX_CURRENT_BID


# ── Pushover helper ───────────────────────────────────────────────────────────

def send_pushover(
    title: str,
    message: str,
    url: str = "",
    photo_url: str = "",
) -> bool:
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        print("  [WARN] Pushover credentials missing — skipping notification.")
        return False

    payload: dict = {
        "token":    PUSHOVER_API_TOKEN,
        "user":     PUSHOVER_USER_KEY,
        "title":    title,
        "message":  message,
        "priority": 1,
        "sound":    "siren",
    }
    if url:
        payload["url"]       = url
        payload["url_title"] = "View on Nellis Auction"

    files = None
    if photo_url:
        try:
            img_resp = requests.get(photo_url, timeout=8)
            img_resp.raise_for_status()
            img_bytes = img_resp.content
            if len(img_bytes) <= 2_500_000:
                files = {"attachment": ("photo.jpg", io.BytesIO(img_bytes), "image/jpeg")}
        except Exception:
            pass   # send without photo rather than fail

    try:
        if files:
            resp = requests.post(PUSHOVER_URL, data=payload, files=files, timeout=15)
        else:
            resp = requests.post(PUSHOVER_URL, data=payload, timeout=10)
        resp.raise_for_status()
        print(f"  [NOTIFY sent] {title}")
        return True
    except Exception as exc:
        print(f"  [NOTIFY error] {exc}")
        return False


# ── Algolia query ─────────────────────────────────────────────────────────────

def fetch_qualifying_items(now: int) -> list[dict]:
    payload = {
        "query": "",
        "facetFilters": [["Shopping Location:Houston, TX"]],
        "numericFilters": [
            f"Total Bids<={MAX_BIDS}",
            f"Current Bid<={MAX_CURRENT_BID}",
            f"Time Remaining>{now}",
            f"Time Remaining<{now + LOOK_AHEAD}",
            f"Suggested Retail>{MIN_RETAIL}",
        ],
        "hitsPerPage": 1000,
        "attributesToRetrieve": [
            "objectID",
            "Lead Description",
            "Suggested Retail",
            "Time Remaining",
            "Location Name",
            "ClerkId",
            "Photo",
        ],
    }
    try:
        resp = requests.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS, json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json().get("hits", [])
    except Exception as exc:
        print(f"  [ERROR] Algolia query failed: {exc}")
        return []


# ── Alert logic ───────────────────────────────────────────────────────────────

def check_and_alert(hits: list[dict], now: int) -> int:
    alerted = 0
    for item in hits:
        obj_id    = item.get("objectID", "")
        secs_left = int(item.get("Time Remaining", 0)) - now

        if secs_left >= ALERT_WINDOW:
            continue
        if obj_id in notified:
            continue

        retail = item.get("Suggested Retail", 0)
        score  = opportunity_score(retail, secs_left)

        notified.add(obj_id)
        alerted += 1

        title_text = item.get("Lead Description", "Unknown Item")
        location   = item.get("Location Name", "Houston")
        clerk_id   = item.get("ClerkId", "")
        photo      = item.get("Photo", "")

        if isinstance(photo, list):
            photo = photo[0] if photo else ""

        mins     = secs_left // 60
        secs     = secs_left % 60
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        item_url = (
            f"{NELLIS_HOUSTON_URL}&clerk={clerk_id}"
            if clerk_id else NELLIS_HOUSTON_URL
        )

        # eBay lookup
        ebay_price, ebay_date = fetch_ebay_sold(title_text)
        profit = calc_profit(ebay_price) if ebay_price is not None else None

        # Build Pushover message
        pushover_title = f"[{score}/10] Nellis — {time_str} left!"
        msg_lines = [
            title_text[:80],
            f"Retail: ${retail:,.2f}  |  Score: {score}/10",
            f"Location: {location}  |  {time_str} remaining",
        ]
        if ebay_price is not None:
            msg_lines.append(f"eBay sold: ${ebay_price:,.2f} ({ebay_date})")
            msg_lines.append(f"Est. profit: ${profit:+,.2f} after fees + shipping")
        else:
            msg_lines.append("eBay: no recent sold data")
        pushover_msg = "\n".join(msg_lines)

        # Console
        print(f"\n{'='*64}")
        print(f"MATCH  {title_text[:70]}")
        print(f"  Retail: ${retail:,.2f}  |  Score: {score}/10  |  {time_str} left")
        if ebay_price is not None:
            print(f"  eBay: ${ebay_price:,.2f} ({ebay_date}) | Profit: ${profit:+,.2f}")
        print(f"  Link: {item_url}")
        print(f"{'='*64}\n")

        send_pushover(pushover_title, pushover_msg, url=item_url, photo_url=photo)

        # Sheets log
        _log_to_sheets([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            title_text[:120],
            f"<=${MAX_CURRENT_BID:.2f}",
            f"${retail:,.2f}",
            f"${ebay_price:,.2f}" if ebay_price is not None else "",
            ebay_date or "",
            f"${profit:+,.2f}" if profit is not None else "",
            f"{score}/10",
            time_str,
            item_url,
        ])

    return alerted


# ── Poll interval ─────────────────────────────────────────────────────────────

def current_poll_interval() -> int:
    hour = datetime.now(HOUSTON_TZ).hour
    return PEAK_POLL_INTERVAL if PEAK_START <= hour < PEAK_END else OFF_POLL_INTERVAL


# ── Main loop ─────────────────────────────────────────────────────────────────

def run() -> None:
    _stats["started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    threading.Thread(target=_start_health_server, daemon=True).start()
    _init_sheets()

    print(
        f"Nellis Houston Watcher — started {_stats['started']}\n"
        f"  Filter : ≤{MAX_BIDS} bids | bid ≤ ${MAX_CURRENT_BID:.2f} | "
        f"retail > ${MIN_RETAIL} | alert < {ALERT_WINDOW//60}m\n"
        f"  Schedule: {PEAK_POLL_INTERVAL}s (5–10 PM CST)  |  {OFF_POLL_INTERVAL}s otherwise\n"
        f"  Health  : http://0.0.0.0:{HTTP_PORT}/\n"
        f"  Sheets  : {'configured ✓' if _sheet else 'not configured'}\n"
        f"  Pushover: {'configured ✓' if PUSHOVER_API_TOKEN else 'NOT configured'}\n"
    )

    while True:
        now      = int(time.time())
        ts       = datetime.now().strftime("%H:%M:%S")
        interval = current_poll_interval()

        hits = fetch_qualifying_items(now)
        _stats["polls"]     += 1
        _stats["last_poll"]  = ts

        alert_ready = [
            h for h in hits
            if int(h.get("Time Remaining", 0)) - now < ALERT_WINDOW
        ]

        print(
            f"[{ts}] {len(hits)} item(s) in window | "
            f"{len(alert_ready)} in alert zone | "
            f"next in {interval}s"
        )

        if hits:
            before = _stats["notified"]
            check_and_alert(hits, now)
            _stats["notified"] += len(notified) - before

        time.sleep(interval)


if __name__ == "__main__":
    run()
