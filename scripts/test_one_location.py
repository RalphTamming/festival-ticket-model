#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from discovery import discover_urls as du


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Debug one TicketSwap location selection.")
    p.add_argument("--city", required=True)
    p.add_argument("--country", required=True)
    p.add_argument("--expected-suggestion", required=True)
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    debug_dir = None
    if args.debug:
        debug_dir = Path("data/debug/location_one") / f"{args.city}_{args.country}".replace(" ", "_").lower()
    driver = du.new_driver(headless=not bool(args.headed))
    try:
        driver.set_page_load_timeout(120)
        driver.get("https://www.ticketswap.com/festival-tickets")
        du.wait_for_page_content(driver, headless=not bool(args.headed))
        res = du.select_location(
            driver,
            args.city,
            country_hint=args.country,
            expected_suggestion=args.expected_suggestion,
            debug_dir=debug_dir,
        )
    finally:
        driver.quit()
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if bool(res.get("success")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
