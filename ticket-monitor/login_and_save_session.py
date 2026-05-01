"""
Manual login / verification flow to save Playwright storage state.

This script intentionally does NOT bypass anything automatically.
You watch the visible browser, complete any normal login/verification yourself,
then press Enter in the terminal to save a storage-state JSON file.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

from playwright.sync_api import sync_playwright

import config


def setup_logging() -> None:
    os.makedirs(os.path.join(os.path.dirname(__file__), "logs"), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "login.log"), encoding="utf-8"),
        ],
    )


def main(url: Optional[str] = None) -> int:
    setup_logging()
    log = logging.getLogger("login")

    target_url = url or (config.TARGETS[0].url if config.TARGETS else None)
    if not target_url:
        log.error("No URL provided and no TARGETS configured.")
        return 2

    state_path = config.STORAGE_STATE_PATH
    log.info("Opening visible browser for manual login.")
    log.info("URL: %s", target_url)
    log.info("Storage state will be saved to: %s", state_path)

    with sync_playwright() as p:
        # Prefer a persistent context so the session behaves like a normal browser profile.
        # This tends to be more stable than storage-state alone for sites with stricter checks.
        os.makedirs(config.USER_DATA_DIR, exist_ok=True)
        log.info("Using persistent profile dir: %s", config.USER_DATA_DIR)

        context = p.chromium.launch_persistent_context(
            user_data_dir=config.USER_DATA_DIR,
            headless=False,
            slow_mo=config.SLOW_MO_MS,
            channel=config.BROWSER_CHANNEL,
        )
        page = context.new_page()
        page.set_default_timeout(config.TIMEOUT_MS)
        page.set_default_navigation_timeout(config.TIMEOUT_MS)

        page.goto(target_url, wait_until="domcontentloaded")
        log.info("Browser opened. Complete login/verification in the window.")
        input("When you're fully logged in / verified, press Enter to save session...")

        os.makedirs(os.path.dirname(os.path.abspath(state_path)) or ".", exist_ok=True)
        context.storage_state(path=state_path)
        log.info("Saved storage state to %s", state_path)
        log.info("Persistent profile remains in %s (reused automatically on next runs).", config.USER_DATA_DIR)

        context.close()

    return 0


if __name__ == "__main__":
    # Optional usage: python login_and_save_session.py "https://example.com/page"
    arg_url = sys.argv[1] if len(sys.argv) > 1 else None
    raise SystemExit(main(arg_url))

