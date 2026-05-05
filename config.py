"""
Central configuration for the minimal TicketSwap pipeline.

This repo intentionally keeps configuration simple: edit this file or pass CLI flags.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional at runtime before deps install
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


# --- Storage ---
DB_PATH = Path(os.getenv("DB_PATH", "ticketswap.db"))
DEBUG_DIR = Path("data/debug")


# --- Browser (Selenium + undetected-chromedriver) ---
# TICKETSWAP_DRIVER_IMPL: "uc" (default, undetected_chromedriver) or "selenium" (stock ChromeDriver).
# On VPS where UC is unstable, use TICKETSWAP_DRIVER_IMPL=selenium (see discovery_urls.new_driver).
# Dedicated Chrome user-data dir (not your personal browser). Keeps TicketSwap login across runs.
USE_PERSISTENT_BROWSER_PROFILE = True
BROWSER_PROFILE_DIR = Path(os.getenv("TICKETSWAP_PROFILE_DIR", ".ticketswap_browser_profile"))
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

# --- STEP2 / TicketSwap browser (mutable per-run; env defaults) ---
# TICKETSWAP_HEADLESS=1 forces headless when CLI does not pass --headed/--headless (see discover_urls.resolve_discovery_headed).
STEP2_SLOW_PAGE_READY_SECONDS = float(os.getenv("STEP2_SLOW_PAGE_READY", "38"))
STEP2_SLOW_PAGE_LOAD_SECONDS = float(os.getenv("STEP2_SLOW_PAGE_LOAD", "6"))
STEP2_INTERACTION_WAIT_SECONDS = float(os.getenv("STEP2_INTERACTION_WAIT", "2.5"))
STEP2_INTERACT_ROUNDS = int(os.getenv("STEP2_INTERACT_ROUNDS", "3"))
# mode_runner sets these before STEP2 Selenium work:
STEP2_INTERACT_ENABLED: bool = False
STEP2_DRIVER_USER_DATA_DIR: Optional[str] = None
STEP2_USE_ANONYMOUS_PROFILE: bool = False
STEP2_SLOW_MODE: bool = False
STEP2_MANUAL_VERIFICATION_PRESS_ENTER: bool = False
# headed_vps mode sets this True so failures persist rich debug bundles under tmp/ticketswap_debug/.
STEP2_DEBUG_DUMP_ON_FAILURE: bool = False

_STEP2_TIMING_BACKUP: dict[str, Any] = {}


def ticketswap_profile_directory() -> Path:
    """Resolved Chrome user-data dir for TicketSwap (env TICKETSWAP_PROFILE_DIR or BROWSER_PROFILE_DIR)."""
    return Path(os.getenv("TICKETSWAP_PROFILE_DIR", str(BROWSER_PROFILE_DIR))).expanduser().resolve()


def warn_if_step2_profile_missing(logger: Optional[logging.Logger] = None) -> None:
    log = logger or logging.getLogger("ticketswap.config")
    if not USE_PERSISTENT_BROWSER_PROFILE or STEP2_USE_ANONYMOUS_PROFILE:
        return
    p = ticketswap_profile_directory()
    if not p.exists():
        log.warning(
            "No persistent TicketSwap profile directory at %s. "
            "Login/trust state may be missing and TicketSwap verification is likely. "
            "Log in once with headed Chrome using this path, or set TICKETSWAP_PROFILE_DIR.",
            p,
        )


def warn_step2_headless_without_trusted_profile(logger: Optional[logging.Logger] = None) -> None:
    """Headless VPS-style runs without a usable profile often hit verification-only HTML."""
    log = logger or logging.getLogger("ticketswap.config")
    if STEP2_USE_ANONYMOUS_PROFILE:
        log.warning(
            "STEP2 anonymous Chrome profile in use — no persisted TicketSwap session; verification may block extraction."
        )
        return
    if not USE_PERSISTENT_BROWSER_PROFILE:
        return
    p = ticketswap_profile_directory()
    if not p.exists():
        log.warning(
            "STEP2 headless with no profile at %s — set TICKETSWAP_PROFILE_DIR and TICKETSWAP_HEADLESS=0 "
            "for trusted-session runs, or use headed Chrome/xvfb.",
            p,
        )

# Mark URLs inactive only after missing N discovery runs.
MISSING_RUNS_THRESHOLD = 3


# --- Scheduler ---
DEFAULT_JOB_LIMIT = 25

# Temporary backoff after repeated failures.
FAILURE_BACKOFF_BASE_MINUTES = 10
FAILURE_BACKOFF_CAP_MINUTES = 6 * 60

# --- New pipeline modes / scopes ---
LOCAL_TIMEZONE = os.getenv("LOCAL_TIMEZONE", "Europe/Amsterdam")
DEFAULT_SCOPE = os.getenv("DEFAULT_SCOPE", "amsterdam_festivals")
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "8"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))
MONITOR_AFTER_EVENT = str(os.getenv("MONITOR_AFTER_EVENT", "false")).lower() in ("1", "true", "yes", "on")
DAILY_REPORT_TIME = os.getenv("DAILY_REPORT_TIME", "21:00")
ENABLE_WEEKLY_EXPORT = str(os.getenv("ENABLE_WEEKLY_EXPORT", "true")).lower() in ("1", "true", "yes", "on")
TELEGRAM_ERROR_ONLY_MODE = str(os.getenv("TELEGRAM_ERROR_ONLY_MODE", "false")).lower() in ("1", "true", "yes", "on")
STEP2_DISCOVERY_STRATEGY = os.getenv("STEP2_DISCOVERY_STRATEGY", "hybrid_fast").strip().lower()

LOCATION_CACHE_PATH = Path(os.getenv("LOCATION_CACHE_PATH", "data/location_cache.json"))
LOCATION_CACHE_MAX_AGE_DAYS = int(os.getenv("LOCATION_CACHE_MAX_AGE_DAYS", "30"))


def _load_location_cache() -> dict[str, dict]:
    if not LOCATION_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(LOCATION_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, dict):
            out[k] = v
    return out


def _cache_entry_fresh(entry: dict) -> bool:
    ts = str(entry.get("last_verified_at", "") or "").strip()
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return False
    return dt >= (datetime.now(timezone.utc) - timedelta(days=max(1, int(LOCATION_CACHE_MAX_AGE_DAYS))))


def _scope_urls_for_country(cache: dict[str, dict], country_name: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for key, entry in cache.items():
        parts = [p.strip() for p in str(key).split(",", 1)]
        if len(parts) != 2:
            continue
        _city, country = parts
        if country.lower() != country_name.lower():
            continue
        if not _cache_entry_fresh(entry):
            continue
        url = str(entry.get("resulting_url", "") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _scope_urls_for_locations(
    cache: dict[str, dict],
    locations: list[tuple[str, str]],
    *,
    fresh_only: bool = False,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for city, country in locations:
        key = f"{city},{country}"
        entry = cache.get(key)
        if not isinstance(entry, dict):
            continue
        if fresh_only and (not _cache_entry_fresh(entry)):
            continue
        url = str(entry.get("resulting_url", "") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


VERIFIED_WESTERN_EUROPE_LOCATIONS: list[tuple[str, str]] = [
    ("Amsterdam", "Netherlands"),
    ("Rotterdam", "Netherlands"),
    ("Utrecht", "Netherlands"),
    ("Eindhoven", "Netherlands"),
    ("Groningen", "Netherlands"),
    ("Brussels", "Belgium"),
    ("Antwerp", "Belgium"),
    ("Ghent", "Belgium"),
    ("Berlin", "Germany"),
    ("Hamburg", "Germany"),
    ("Cologne", "Germany"),
    ("Munich", "Germany"),
    ("Paris", "France"),
    ("Lyon", "France"),
    ("Marseille", "France"),
]


_LOC_CACHE = _load_location_cache()
_NL_URLS = _scope_urls_for_country(_LOC_CACHE, "Netherlands")
_BE_URLS = _scope_urls_for_country(_LOC_CACHE, "Belgium")
_DE_URLS = _scope_urls_for_country(_LOC_CACHE, "Germany")
_FR_URLS = _scope_urls_for_country(_LOC_CACHE, "France")
_LU_URLS = _scope_urls_for_country(_LOC_CACHE, "Luxembourg")
_WESTERN_URLS = list(dict.fromkeys(_NL_URLS + _BE_URLS + _DE_URLS + _FR_URLS + _LU_URLS))
_WESTERN_VERIFIED_URLS = _scope_urls_for_locations(
    _LOC_CACHE,
    VERIFIED_WESTERN_EUROPE_LOCATIONS,
    fresh_only=False,
)

SCOPES: dict[str, dict] = {
    "amsterdam_festivals": {
        "category": "festival",
        "listing_urls": [
            "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
        ],
        "enabled": True,
    },
    "nl_festivals": {
        "category": "festival",
        "listing_urls": _NL_URLS,
        "enabled": False,
    },
    "belgium_festivals": {
        "category": "festival",
        "listing_urls": _BE_URLS,
        "enabled": False,
    },
    "germany_festivals": {
        "category": "festival",
        "listing_urls": _DE_URLS,
        "enabled": False,
    },
    "france_festivals": {
        "category": "festival",
        "listing_urls": _FR_URLS,
        "enabled": False,
    },
    "luxembourg_festivals": {
        "category": "festival",
        "listing_urls": _LU_URLS,
        "enabled": False,
    },
    "western_europe_festivals": {
        "category": "festival",
        "listing_urls": _WESTERN_URLS,
        "enabled": False,
    },
    "western_europe_festivals_verified": {
        "category": "festival",
        "listing_urls": _WESTERN_VERIFIED_URLS,
        "enabled": False,
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
