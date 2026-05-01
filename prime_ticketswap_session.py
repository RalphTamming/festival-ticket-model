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

import sys

import config
import discover_urls as du


def main() -> int:
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
    print("")

    driver = du.new_driver(headless=False)
    try:
        driver.set_page_load_timeout(120)
        driver.maximize_window()
        driver.get("https://www.ticketswap.com/")
        input("When you are logged in on TicketSwap in that browser window, press Enter here to save and close… ")
    finally:
        driver.quit()

    print("")
    print(f"Done. Session data is stored under: {profile}")
    print("You can run discover_urls, scrape_market, or run_full_test.py next.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
