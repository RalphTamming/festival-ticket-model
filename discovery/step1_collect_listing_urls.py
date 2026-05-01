"""
STEP 1 helper: collect many festival-related URLs from the /festival-tickets overview page.

This script is intentionally scope-limited for testing:
- expands "Show more" / "Toon meer" repeatedly
- prefers Amsterdam if possible (location=3 URL)
- extracts hub URLs (/festival-tickets/a/...) AND event URLs (/festival-tickets/<event-slug>)

Usage:
  python step1_collect_listing_urls.py --min-events 100
  python step1_collect_listing_urls.py --url "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3" --min-events 100
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from typing import Iterable

import config
from discovery import discover_urls as du


def _jitter(base: float = 0.35) -> None:
    time.sleep(base + random.random() * 0.55)


def _dom_event_link_count(driver) -> int:
    try:
        n = driver.execute_script(
            r"""
            const root = document.querySelector('main') || document.body;
            const links = Array.from(root.querySelectorAll('a[href*="/festival-tickets/"]'));
            let c = 0;
            for (const a of links) {
              const href = a.getAttribute('href') || '';
              if (!href.includes('/festival-tickets/')) continue;
              if (href.includes('/festival-tickets/a/')) continue;
              // ignore deep ticket-category pages
              if (/\/festival-tickets\/[^\/]+\/[^\/]+\/\d+/.test(href)) continue;
              c += 1;
            }
            return c;
            """
        )
        return int(n or 0)
    except Exception:
        return 0


def _extract_event_urls(cands: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for u in cands:
        n = du.normalize_url(u)
        if not n or n in seen:
            continue
        if du.is_ticket_url(n) or du.is_festival_page(n):
            continue
        if du.is_event_page(n):
            seen.add(n)
            out.append(n)
    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect festival hub and event URLs from the festival overview listing.")
    p.add_argument(
        "--url",
        default="https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
        help="Overview URL (default: Amsterdam-pinned location=3 listing).",
    )
    p.add_argument("--headless", action="store_true", default=False)
    p.add_argument("--min-events", type=int, default=100, help="Stop once this many unique event URLs are found.")
    p.add_argument("--max-show-more", type=int, default=int(getattr(config, "DISCOVERY_OVERVIEW_MAX_SHOW_MORE", 50)))
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    url = du.normalize_url(args.url) or args.url

    driver = du.new_driver(headless=bool(args.headless))
    try:
        driver.get(url)
        html0 = du.wait_for_page_content(driver, headless=bool(args.headless))
        if du.is_blocked_for_discovery(html0):
            print("verification_encountered: True")
            print("total_unique_hubs: 0")
            print("total_unique_events: 0")
            return 2

        # attempt Amsterdam selection if user didn't pin location=3
        ams_clicked = du.try_select_amsterdam_location_filter(driver, url)
        if ams_clicked:
            _jitter(0.8)

        # Expand until we have enough event URLs or run out of show-more clicks.
        hubs: set[str] = set()
        events: list[str] = []
        show_more_clicks = 0
        stagnant = 0
        prev_count = _dom_event_link_count(driver)

        for _ in range(int(args.max_show_more)):
            du.scroll_for_lazy_content(driver)
            _jitter(0.35)

            html = driver.page_source or ""
            cands = du.merge_link_candidates(html, driver, base_url=url)
            hubs |= {u for u in cands if du.is_festival_page(u)}
            events = _extract_event_urls(cands)

            if len(events) >= int(args.min_events):
                break

            # try click "show more" using the hardened helper
            before = du._overview_hub_signal_count(driver)  # type: ignore[attr-defined]
            clicked = du.expand_festival_overview_show_more(driver, max_clicks=1)
            if clicked:
                show_more_clicks += 1
                _jitter(0.8)
            after = du._overview_hub_signal_count(driver)  # type: ignore[attr-defined]

            cur_count = _dom_event_link_count(driver)
            if cur_count <= prev_count and after <= before:
                stagnant += 1
            else:
                stagnant = 0
                prev_count = max(prev_count, cur_count)

            if stagnant >= 3:
                break

        print(f"url: {url}")
        print(f"amsterdam_clicked: {bool(ams_clicked)}")
        print(f"show_more_clicks: {int(show_more_clicks)}")
        print(f"total_unique_hubs: {len(hubs)}")
        print(f"total_unique_events: {len(events)}")
        print("")
        print("HUB_URLS")
        for u in sorted(hubs):
            print(u)
        print("")
        print("EVENT_URLS")
        for u in events:
            print(u)
        return 0
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

