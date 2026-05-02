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
import json
import sys
import time
from pathlib import Path

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
    p.add_argument(
        "--manual-timeout",
        type=int,
        default=300,
        help="Maximum seconds to keep browser open for manual verification.",
    )
    p.add_argument(
        "--remote-debugging-port",
        type=int,
        default=9222,
        help="Expose Chrome remote debugging port for SSH tunnel workflows.",
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
    print("Complete TicketSwap verification manually if shown. Do not close until the real page loads.")
    print("")
    print(f"Remote debugging enabled on port {args.remote_debugging_port}.")
    print("If needed, tunnel from local machine:")
    print(f"  ssh -L {args.remote_debugging_port}:localhost:{args.remote_debugging_port} root@178.104.249.252")
    print(f"Then open http://localhost:{args.remote_debugging_port}")
    print("")
    print("This script will visit:")
    print("  1) https://www.ticketswap.com/")
    print("  2) https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3")
    print("  3) https://www.ticketswap.com/festival-tickets/bondgenoten-festival-2026-amsterdam-lofi-2026-05-29-CZrBG4iowfg4JsxTeEx7j")
    print("")

    driver = du.new_driver(
        headless=False,
        extra_args=[f"--remote-debugging-port={int(args.remote_debugging_port)}"],
    )
    html = ""
    status = "still_blocked"
    final_url = ""
    try:
        driver.set_page_load_timeout(120)
        # Xvfb/remote sessions may fail on maximize; keep priming resilient.
        with contextlib.suppress(Exception):
            driver.maximize_window()
        with contextlib.suppress(Exception):
            driver.set_window_size(1366, 900)
        urls = [
            "https://www.ticketswap.com/",
            "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
            "https://www.ticketswap.com/festival-tickets/bondgenoten-festival-2026-amsterdam-lofi-2026-05-29-CZrBG4iowfg4JsxTeEx7j",
        ]
        for u in urls:
            with contextlib.suppress(Exception):
                driver.get(u)
            time.sleep(3)

        print(f"Waiting {args.wait_seconds}s first-pass verification window...")
        time.sleep(max(0, int(args.wait_seconds)))
        print(
            f"Press Enter when real TicketSwap content is visible; "
            f"auto-timeout in {int(args.manual_timeout)} seconds."
        )
        with contextlib.suppress(Exception):
            import select

            r, _, _ = select.select([sys.stdin], [], [], float(max(0, int(args.manual_timeout))))
            if r:
                sys.stdin.readline()

        html = driver.page_source or ""
        final_url = str(getattr(driver, "current_url", "") or "")
        if not du.is_blocked_for_discovery(html) and not du.looks_like_verification(html):
            status = "ok"
    finally:
        out = Path(config.DEBUG_DIR) / "priming"
        out.mkdir(parents=True, exist_ok=True)
        ts = str(int(time.time()))
        with contextlib.suppress(Exception):
            (out / f"{ts}_priming_page.html").write_text(html or "", encoding="utf-8")
        with contextlib.suppress(Exception):
            driver.save_screenshot(str(out / f"{ts}_priming_screenshot.png"))
        driver.quit()

    print("")
    print(f"Done. Session data is stored under: {profile}")
    print(f"final_url: {final_url}")
    print(f"priming_status: {status}")
    print("priming_debug_dir: data/debug/priming")
    print("You can run discovery/monitoring with run_pipeline.py next.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
