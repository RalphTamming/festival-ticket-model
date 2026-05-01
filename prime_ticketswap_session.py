"""
Open the SAME Chrome profile the scraper uses and keep it open until you finish logging in.

Where to log in:
  Not in Cursor, not in your normal Chrome taskbar icon.
  Only in the Chrome window that appears when you run this script.

Usage (from project folder, in a terminal):
  python prime_ticketswap_session.py

Then:
  1. In that new Chrome window, go to TicketSwap (already opened for you).
  2. Click Log in and sign in (email or social — complete any check in that same window).
  3. When you see your account / logged-in state, come back to the terminal and press Enter.

After that, discovery and scraping runs can reuse the saved session.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
import time

import config
from discovery import discover_urls as du


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prime TicketSwap persistent browser profile for VPS verification.")
    p.add_argument(
        "--wait-seconds",
        type=int,
        default=int(getattr(config, "MANUAL_VERIFY_WAIT_SECONDS", 90)),
        help="Automatic wait window for manual verification before asking for Enter.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(list(argv or []))
    if not config.USE_PERSISTENT_BROWSER_PROFILE:
        print("Set USE_PERSISTENT_BROWSER_PROFILE = True in config.py first.", file=sys.stderr)
        return 1

    profile = config.BROWSER_PROFILE_DIR.resolve()
    print("")
    print("Opening Chrome with scraper profile at:")
    print(f"  {profile}")
    print("")
    print("Log in ONLY in the Chrome window that just opened (not your everyday Chrome).")
    print("If you see a verification / captcha page, complete it in that same window.")
    print("If verification appears, complete it once, then close browser or press Enter.")
    print("")
    print("This script will visit:")
    print("  1) https://www.ticketswap.com/")
    print("  2) https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3")
    print("")

    driver = du.new_driver(headless=False)
    try:
        driver.set_page_load_timeout(120)
        # Xvfb/remote sessions may fail on maximize; keep priming resilient.
        with contextlib.suppress(Exception):
            driver.maximize_window()
        with contextlib.suppress(Exception):
            driver.set_window_size(1366, 900)
        driver.get("https://www.ticketswap.com/")
        time.sleep(3)
        driver.get("https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3")
        print(f"Waiting {args.wait_seconds}s for manual verification/login in the opened browser...")
        time.sleep(max(0, int(args.wait_seconds)))
        input("When verification/login is complete, press Enter here to save and close browser... ")
    finally:
        driver.quit()

    print("")
    print(f"Done. Session data is stored under: {profile}")
    print("You can run discovery/monitoring with run_pipeline.py next.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
