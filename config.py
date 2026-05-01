"""
Central configuration for the minimal TicketSwap pipeline.

This repo intentionally keeps configuration simple: edit this file or pass CLI flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# --- Storage ---
DB_PATH = Path("ticketswap.db")
DEBUG_DIR = Path("data/debug")


# --- Browser (Selenium + undetected-chromedriver) ---
# Dedicated Chrome user-data dir (not your personal browser). Keeps TicketSwap login across runs.
USE_PERSISTENT_BROWSER_PROFILE = True
BROWSER_PROFILE_DIR = Path(".ticketswap_browser_profile")
BROWSER_PROFILE_NAME = "Default"

# Pin to your installed Chrome major when known; avoids UC/Selenium driver mismatch errors.
# Override per-run in smoke tests if needed.
CHROME_VERSION_MAIN: Optional[int] = 147
HEADLESS_DEFAULT = False
PAGE_LOAD_SLEEP_SECONDS = 4.0
PAGE_READY_TIMEOUT_SECONDS = 28.0
PAGE_POLL_INTERVAL_SECONDS = 0.55
DISCOVERY_MAX_EVENT_PAGES_PER_HUB = 45
# Max "Show more" clicks when expanding /festival-tickets overview listings (Amsterdam, etc.).
DISCOVERY_OVERVIEW_MAX_SHOW_MORE = 50

# Entry pages for automatic festival hub discovery (see discover_festival_hubs.py).
FESTIVAL_HUB_ENTRY_PAGES: list[str] = [
    "https://www.ticketswap.com/",
    "https://www.ticketswap.com/festival-tickets",
    # Amsterdam area (TicketSwap uses location=3 for this filter in the festivals UI).
    "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
]


# --- Discovery ---
# Seed URLs can include:
# - festivals overview (optional query, e.g. Amsterdam): https://www.ticketswap.com/festival-tickets?slug=...&location=3
# - festival pages: https://www.ticketswap.com/festival-tickets/a/<slug>
# - event pages:    https://www.ticketswap.com/festival-tickets/<event-slug>
# - ticket URLs:    https://www.ticketswap.com/festival-tickets/<event-slug>/<ticket-type>/<id>
#
# Example (Music On Festival) — same shapes the site uses in production:
#   All festivals list: https://www.ticketswap.com/festival-tickets
#   Series hub:         https://www.ticketswap.com/festival-tickets/a/music-on-festival
#   Ticket category:   https://www.ticketswap.com/festival-tickets/music-on-festival-2026-amsterdam-meerpark-2026-05-09-CUfJVG9ggm76WkYpo1Fqe/weekend-tickets/5314233
SEED_URLS: list[str] = [
    "https://www.ticketswap.com/festival-tickets/a/music-on-festival",
    "https://www.ticketswap.com/festival-tickets/music-on-festival-2026-amsterdam-meerpark-2026-05-09-CUfJVG9ggm76WkYpo1Fqe/weekend-tickets/5314233",
]

# When TicketSwap shows a verification page, keep the visible browser open for this many seconds.
MANUAL_VERIFY_WAIT_SECONDS = 90

# Mark URLs inactive only after missing N discovery runs.
MISSING_RUNS_THRESHOLD = 3


# --- Scheduler ---
DEFAULT_JOB_LIMIT = 25

# Temporary backoff after repeated failures.
FAILURE_BACKOFF_BASE_MINUTES = 10
FAILURE_BACKOFF_CAP_MINUTES = 6 * 60

# --- New pipeline modes / scopes ---
LOCAL_TIMEZONE = "Europe/Amsterdam"
MONITOR_AFTER_EVENT = False

SCOPES: dict[str, dict] = {
    "amsterdam_festivals": {
        "category": "festival",
        "listing_urls": [
            "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
        ],
    },
    # TODO: Add NL location-specific festival listing URLs/discovery heuristics.
    "netherlands_festivals": {
        "category": "festival",
        "listing_urls": [],
    },
    # TODO: Add Western Europe scope URLs/filters once location strategy is stable.
    "western_europe_festivals": {
        "category": "festival",
        "listing_urls": [],
    },
}


def persistent_browser_user_data_dir() -> Optional[str]:
    """
    Absolute path for Chrome's user-data-dir when persistent profile is on.
    Pass this as uc.Chrome(user_data_dir=...) — undetected-chromedriver ignores ad-hoc temp
    profiles unless user_data_dir is set on the constructor.
    """
    if not USE_PERSISTENT_BROWSER_PROFILE:
        return None
    root = BROWSER_PROFILE_DIR.resolve()
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def apply_persistent_chrome_profile(options: object) -> None:
    """Adds --profile-directory only; user-data-dir is set via uc.Chrome(user_data_dir=...)."""
    if not USE_PERSISTENT_BROWSER_PROFILE:
        return
    add_arg = getattr(options, "add_argument", None)
    if add_arg is None:
        return
    add_arg(f"--profile-directory={BROWSER_PROFILE_NAME}")
