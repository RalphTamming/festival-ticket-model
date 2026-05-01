"""
Simple Python configuration for the ticket monitor.

Version 1 intentionally avoids config frameworks and YAML to stay simple.
You can edit the TARGETS list directly to add/remove monitored pages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Target:
    """One page to monitor."""

    site_name: str
    label: str
    url: str
    scraper_type: str


# --- Scheduling ---
SCRAPE_INTERVAL_MINUTES: int = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "10"))

# --- Playwright (visible-browser friendly) ---
# Default to visible mode for real-site testing.
HEADLESS: bool = os.getenv("HEADLESS", "false").strip().lower() in {"1", "true", "yes", "y"}
SLOW_MO_MS: int = int(os.getenv("SLOW_MO_MS", "250"))
TIMEOUT_MS: int = int(os.getenv("TIMEOUT_MS", "60000"))

# Use a real installed browser channel where possible (more "normal" than bundled Chromium).
# On Windows, "chrome" usually works if Chrome is installed. Fallback is Playwright's Chromium.
BROWSER_CHANNEL: Optional[str] = os.getenv("BROWSER_CHANNEL", "chrome").strip() or None

# Persistent browser profile directory (recommended for "trusted session" stability).
# This keeps cookies/localStorage/other profile data between runs, similar to a normal browser profile.
USE_PERSISTENT_CONTEXT: bool = os.getenv("USE_PERSISTENT_CONTEXT", "true").strip().lower() in {"1", "true", "yes", "y"}
USER_DATA_DIR: str = os.getenv("USER_DATA_DIR", os.path.join(os.path.dirname(__file__), "pw_user_data"))

USE_STORAGE_STATE: bool = os.getenv("USE_STORAGE_STATE", "true").strip().lower() in {"1", "true", "yes", "y"}
STORAGE_STATE_PATH: str = os.getenv("STORAGE_STATE_PATH", os.path.join(os.path.dirname(__file__), "playwright_state.json"))

SAVE_DEBUG_HTML: bool = os.getenv("SAVE_DEBUG_HTML", "true").strip().lower() in {"1", "true", "yes", "y"}
SAVE_DEBUG_SCREENSHOT: bool = os.getenv("SAVE_DEBUG_SCREENSHOT", "true").strip().lower() in {"1", "true", "yes", "y"}

# --- Storage ---
DATABASE_PATH: str = os.getenv(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), "data", "ticket_monitor.sqlite3"),
)


# --- Targets to monitor ---
# NOTE: The example scraper uses placeholder selectors; you must customize it per site.
TARGETS: List[Target] = [
    Target(
        site_name="TicketSwap",
        label="Awakenings Festival (NL) - category page",
        url="https://www.ticketswap.com/festival-tickets/a/awakenings-festival",
        scraper_type="real_site",
    ),
]


def validate_config() -> Optional[str]:
    """Return an error string if config is invalid, else None."""
    if SCRAPE_INTERVAL_MINUTES <= 0:
        return "SCRAPE_INTERVAL_MINUTES must be > 0"
    if TIMEOUT_MS <= 0:
        return "TIMEOUT_MS must be > 0"
    if SLOW_MO_MS < 0:
        return "SLOW_MO_MS must be >= 0"
    if not TARGETS:
        return "TARGETS must contain at least one target"
    for t in TARGETS:
        if not (t.site_name and t.label and t.url and t.scraper_type):
            return f"Invalid target entry: {t!r}"
    return None

