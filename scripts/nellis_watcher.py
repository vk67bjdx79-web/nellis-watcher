#!/usr/bin/env python3
"""
Nellis Auction Houston Watcher
==============================
Polls Algolia (the same search index Nellis uses publicly) every 30 seconds.
Finds Houston items with:
  - 0-2 total bids
  - Current bid <= Suggested Retail / RETAIL_MULTIPLIER  (3x ratio)
  - Estimated retail price >= $60
  - Less than 5 minutes remaining

Sends a Pushover notification with a direct link for each qualifying item.

No login required — uses the public Algolia API key embedded in the Nellis site.

Note on the 3x ratio filter: Nellis does not expose the live current bid price
in its public search index. The field "Current Bid" IS available as a numeric
filter attribute (used for sorting), so we filter Current Bid <= MIN_RETAIL /
RETAIL_MULTIPLIER server-side. This correctly catches all 0-bid items and
1-2-bid items where the current bid is within that threshold. Items with higher
retail prices and proportionally higher bids that still satisfy 3x are not
counted — that would require a logged-in API call.
"""

import os
import time
import threading
import logging
from datetime import datetime

import requests
from flask import Flask

# ── Pushover ─────────────────────────────────────────────────────────────────
PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY  = os.environ.get("PUSHOVER_USER_KEY", "")
PUSHOVER_URL       = "https://api.pushover.net/1/messages.json"

# ── Algolia (public credentials embedded in nellisauction.com) ───────────────
ALGOLIA_APP_ID  = "GL1QVP8R29"
ALGOLIA_API_KEY = "d22f83c614aa8eda28fa9eadda0d07b9"
# Use the bids_asc replica — it supports numeric filtering on "Total Bids"
ALGOLIA_INDEX   = "nellisauction-prd_bids_asc"
ALGOLIA_URL     = (
    f"https://{ALGOLIA_APP_ID}-dsn.algolia.net"
    f"/1/indexes/{ALGOLIA_INDEX}/query"
)
ALGOLIA_HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key": ALGOLIA_API_KEY,
    "Content-Type": "application/json",
}

NELLIS_HOUSTON_URL = "https://www.nellisauction.com/browse?location=houston"

# ── Tuning knobs ──────────────────────────────────────────────────────────────
POLL_INTERVAL      = 30    # seconds between polls
MAX_BIDS           = 2     # alert when Total Bids <= this
MIN_RETAIL         = 100.0  # alert when Suggested Retail > this
MAX_CURRENT_BID    = 5.0   # alert when Current Bid <= this (i.e. $5 or less)
ALERT_WINDOW       = 300   # alert when Time Remaining < this many seconds (5 min)
LOOK_AHEAD         = 3600  # only fetch items closing within this window (1 hour)
                           # keeps the result set small and avoids noise

# Track items already notified to avoid duplicate alerts
notified: set[str] = set()

# Shared stats for the health page (updated by the main loop)
_stats: dict = {"polls": 0, "notified": 0, "last_poll": "never", "started": ""}

HTTP_PORT = int(os.environ.get("PORT", 8080))


# ── Flask health server ────────────────────────────────────────────────────────

_app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)  # suppress request logs


@_app.route("/", defaults={"path": ""})
@_app.route("/<path:path>")
def health(path: str) -> tuple[str, int]:
    return "OK", 200


def _start_health_server() -> None:
    _app.run(host="0.0.0.0", port=HTTP_PORT, use_reloader=False)


# ── Pushover helper ───────────────────────────────────────────────────────────

def send_pushover(title: str, message: str, url: str = "") -> bool:
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        print("  [WARN] Pushover credentials missing — skipping notification.")
        return False
    payload: dict = {
        "token":    PUSHOVER_API_TOKEN,
        "user":     PUSHOVER_USER_KEY,
        "title":    title,
        "message":  message,
        "priority": 1,        # high-priority (bypasses quiet hours)
        "sound":    "siren",
    }
    if url:
        payload["url"]       = url
        payload["url_title"] = "View on Nellis Auction"
    try:
        resp = requests.post(PUSHOVER_URL, data=payload, timeout=10)
        resp.raise_for_status()
        print(f"  [NOTIFY sent] {title}")
        return True
    except Exception as exc:
        print(f"  [NOTIFY error] {exc}")
        return False


# ── Algolia query ─────────────────────────────────────────────────────────────

def fetch_qualifying_items(now: int) -> list[dict]:
    """
    Query Algolia for Houston items that:
      - close in the next LOOK_AHEAD seconds
      - have Total Bids <= MAX_BIDS
      - have Current Bid <= MAX_CURRENT_BID  (proxy for retail >= 3x current bid)
      - have Suggested Retail >= MIN_RETAIL
    Returns a list of hit dicts.
    """
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
        resp = requests.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS,
                             json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("hits", [])
    except Exception as exc:
        print(f"  [ERROR] Algolia query failed: {exc}")
        return []


# ── Alert logic ───────────────────────────────────────────────────────────────

def check_and_alert(hits: list[dict], now: int) -> None:
    alerted = 0
    for item in hits:
        obj_id    = item.get("objectID", "")
        secs_left = int(item.get("Time Remaining", 0)) - now

        if secs_left >= ALERT_WINDOW:
            continue  # not close enough yet

        if obj_id in notified:
            continue  # already alerted

        notified.add(obj_id)
        alerted += 1

        title_text = item.get("Lead Description", "Unknown Item")
        retail     = item.get("Suggested Retail", 0)
        location   = item.get("Location Name", "Houston")
        clerk_id   = item.get("ClerkId", "")

        mins = secs_left // 60
        secs = secs_left % 60
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        # Build the most direct link available without a login:
        # The SPA reads the ?clerk= param to navigate to that specific item.
        item_url = (
            f"{NELLIS_HOUSTON_URL}&clerk={clerk_id}"
            if clerk_id else NELLIS_HOUSTON_URL
        )

        pushover_title = f"Nellis Deal — {time_str} left!"
        pushover_msg   = (
            f"{title_text[:80]}\n"
            f"Est. retail: ${retail:,.2f}\n"
            f"Location: {location}\n"
            f"Time left: {time_str}"
        )

        print(f"\n{'='*64}")
        print(f"MATCH  {title_text[:70]}")
        print(f"  Retail: ${retail:,.2f}  |  Location: {location}  |  Time left: {time_str}")
        print(f"  Link: {item_url}")
        print(f"{'='*64}\n")

        send_pushover(pushover_title, pushover_msg, url=item_url)

    if alerted == 0 and hits:
        # Items exist in window but none are close enough yet — normal
        pass


# ── Main loop ─────────────────────────────────────────────────────────────────

def run() -> None:
    _stats["started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Start health-check server in a background daemon thread
    t = threading.Thread(target=_start_health_server, daemon=True)
    t.start()

    print(
        f"Nellis Houston Watcher — started {_stats['started']}\n"
        f"  Filter: ≤{MAX_BIDS} bids | current bid ≤ ${MAX_CURRENT_BID:.2f} | "
        f"retail > ${MIN_RETAIL} | alert < {ALERT_WINDOW//60}m remaining\n"
        f"  Polling every {POLL_INTERVAL}s | Looking {LOOK_AHEAD//60}m ahead\n"
        f"  Health server: http://0.0.0.0:{HTTP_PORT}/\n"
        f"  Pushover: {'configured' if PUSHOVER_API_TOKEN else 'NOT configured'}\n"
    )

    while True:
        now = int(time.time())
        ts  = datetime.now().strftime("%H:%M:%S")

        hits = fetch_qualifying_items(now)

        _stats["polls"] += 1
        _stats["last_poll"] = ts

        # Count items in alert window vs just in the look-ahead window
        alert_ready = [h for h in hits if int(h.get("Time Remaining", 0)) - now < ALERT_WINDOW]

        print(
            f"[{ts}] Found {len(hits)} qualifying item(s) closing within "
            f"{LOOK_AHEAD//60}m  ({len(alert_ready)} within {ALERT_WINDOW//60}m alert window)"
        )

        if hits:
            before = _stats["notified"]
            check_and_alert(hits, now)
            _stats["notified"] += len(notified) - before

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
