"""
Stress test matrix for TicketSwap pipeline across categories and cities.

Uses Selenium + undetected-chromedriver (same as discover_urls / scrape_market), not Playwright.

Usage:
  python stress_test_pipeline.py --headed --debug

Writes:
  debug/stress_test/stress_test_results.jsonl
  debug/stress_test/<test_name>/<listing_or_event_slug>/{html,txt,screenshot,...}
"""

from __future__ import annotations

import argparse
import contextlib
import json
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from selenium.webdriver.chrome.webdriver import WebDriver

import config
import discover_urls as du
import scrape_market as sm


def _slugify(s: str) -> str:
    import re

    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_-]+", "_", s)[:80]
    return s or "x"


def _jitter(a: float = 0.35, b: float = 0.85) -> None:
    time.sleep(a + random.random() * max(0.0, b - a))


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _debug_write(dirp: Path, *, name: str, content: str) -> None:
    try:
        (dirp / name).write_text(content or "", encoding="utf-8")
    except Exception:
        pass


def _category_prefix(url: str) -> Optional[str]:
    return du.detect_category_prefix(url)


@dataclass
class TestCase:
    name: str
    listing_url: str
    city: Optional[str] = None


@dataclass
class CaseResult:
    name: str
    listing_url: str
    category: Optional[str]
    status: str
    verification: bool = False
    city_selected: bool = False
    show_more_clicks: int = 0
    target_kind: str = ""  # "hubs" | "events" | ""
    events_found: int = 0
    events_tested: int = 0
    ticket_urls_found: int = 0
    snapshots_ok: int = 0
    failures: list[str] = field(default_factory=list)
    debug_dir: Optional[str] = None


def run_case_uc(
    driver: WebDriver,
    case: TestCase,
    *,
    headed: bool,
    debug: bool,
    max_events: int,
    max_tickets_per_event: int,
) -> CaseResult:
    base_debug = Path(config.DEBUG_DIR) / "stress_test" / _slugify(case.name)
    if debug:
        _ensure_dir(base_debug)

    category = _category_prefix(case.listing_url)
    if not category:
        return CaseResult(
            case.name,
            case.listing_url,
            None,
            "unsupported_category",
            failures=["unsupported_category"],
            debug_dir=str(base_debug) if debug else None,
        )

    listing_n = du.normalize_url(case.listing_url) or case.listing_url
    cap = max(1, int(getattr(config, "DISCOVERY_OVERVIEW_MAX_SHOW_MORE", 50)))
    fest_overview = category == "festival-tickets" and du.is_festival_overview_page(listing_n)

    def _load_and_settle() -> str:
        driver.get(listing_n)
        return du.wait_for_page_content(driver, headless=not headed)

    html = _load_and_settle()
    verification = du.is_blocked_for_discovery(html)
    if verification:
        with contextlib.suppress(Exception):
            driver.refresh()
        _jitter(1.0, 2.0)
        html = du.wait_for_page_content(driver, headless=not headed)
        verification = du.is_blocked_for_discovery(html)

    if verification and headed and int(config.MANUAL_VERIFY_WAIT_SECONDS) > 0:
        time.sleep(min(2.0, float(config.PAGE_POLL_INTERVAL_SECONDS)))
        html = driver.page_source or html
        verification = du.is_blocked_for_discovery(html)

    if verification:
        body_text = ""
        try:
            body_text = driver.execute_script(
                "return document.body ? document.body.innerText : ''",
            ) or ""
        except Exception:
            body_text = ""
        if debug:
            _debug_write(base_debug, name="listing.html", content=driver.page_source or "")
            _debug_write(base_debug, name="listing.txt", content=body_text)
            with contextlib.suppress(Exception):
                driver.save_screenshot(str(base_debug / "listing.png"))
        return CaseResult(
            case.name,
            case.listing_url,
            category,
            "verification_blocked",
            verification=True,
            failures=["verification_blocked"],
            debug_dir=str(base_debug) if debug else None,
        )

    if headed:
        time.sleep(1.0)

    city_selected = False
    if case.city:
        city_selected = du.try_select_city_location_filter(driver, case.city)
        _jitter(0.65, 1.05)
        du.scroll_for_lazy_content(driver)
        html = du.wait_for_page_content(driver, headless=not headed)

    if fest_overview and not case.city:
        if du.try_select_amsterdam_location_filter(driver, listing_n):
            _jitter(0.55, 0.95)
            du.scroll_for_lazy_content(driver)
            html = du.wait_for_page_content(driver, headless=not headed)

    if fest_overview:
        show_more_clicks = du.expand_festival_overview_show_more(driver, max_clicks=cap)
    else:
        show_more_clicks = du.expand_category_listing_show_more(
            driver, listing_n, category, max_clicks=cap
        )
    du.scroll_for_lazy_content(driver)
    html_final = driver.page_source or ""

    targets_full, target_kind = du.list_stress_targets_from_listing(
        driver, html_final, listing_n, category_prefix=category
    )
    targets_to_test = targets_full[:max_events]

    fails: list[str] = []
    if case.city and not city_selected:
        fails.append("city_filter_not_found")

    if not targets_full:
        status = "listing_no_targets"
        fails.append("listing_no_targets")
        events_tested = 0
        ticket_total = 0
        snapshots_ok = 0
    else:
        status = "ok"
        events_tested = 0
        ticket_total = 0
        snapshots_ok = 0
        for target in targets_to_test:
            events_tested += 1
            tus_raw, step2_status = du.discover_ticket_urls_for_listing_target_uc(
                driver, target, headless=not headed
            )
            if step2_status != "ok":
                fails.append(f"step2_{step2_status}:{target}")
            tus = tus_raw[:max_tickets_per_event]
            ticket_total += len(tus)
            for tu in tus:
                snap = sm.scrape_market_url(
                    tu,
                    headless=not headed,
                    debug_dir=config.DEBUG_DIR,
                    manual_wait_seconds=int(config.MANUAL_VERIFY_WAIT_SECONDS) if headed else 0,
                    driver=driver,
                )
                if snap.status == "ok":
                    snapshots_ok += 1
            _jitter(0.4, 0.9)

    if debug:
        body_text_dbg = ""
        try:
            body_text_dbg = (
                driver.execute_script("return document.body ? document.body.innerText : ''") or ""
            )
        except Exception:
            body_text_dbg = ""
        _debug_write(base_debug, name="listing.html", content=html_final)
        _debug_write(base_debug, name="listing.txt", content=body_text_dbg)
        _debug_write(
            base_debug,
            name="network_urls.txt",
            content="# UC runner: HTTP request dump not wired (same stack as scrape_market).\n",
        )
        with contextlib.suppress(Exception):
            driver.save_screenshot(str(base_debug / "listing.png"))

    return CaseResult(
        name=case.name,
        listing_url=case.listing_url,
        category=category,
        status=status,
        verification=False,
        city_selected=city_selected,
        show_more_clicks=show_more_clicks,
        target_kind=target_kind,
        events_found=len(targets_to_test),
        events_tested=events_tested,
        ticket_urls_found=ticket_total,
        snapshots_ok=snapshots_ok,
        failures=fails,
        debug_dir=str(base_debug) if debug else None,
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stress test TicketSwap scraper across categories/cities (UC).")
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument("--max-events", type=int, default=10)
    p.add_argument("--max-tickets-per-event", type=int, default=3)
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    out_dir = Path(config.DEBUG_DIR) / "stress_test"
    _ensure_dir(out_dir)
    out_jsonl = out_dir / "stress_test_results.jsonl"
    out_jsonl.write_text("", encoding="utf-8")

    cases = [
        TestCase("festivals_amsterdam", "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3"),
        TestCase("festivals_berlin", "https://www.ticketswap.com/festival-tickets", city="Berlin"),
        TestCase("festivals_sao_paulo", "https://www.ticketswap.com/festival-tickets", city="Sao Paulo"),
        TestCase("concerts_general", "https://www.ticketswap.com/concert-tickets"),
        TestCase("clubs_general", "https://www.ticketswap.com/club-tickets"),
        TestCase("sports_general", "https://www.ticketswap.com/sports-tickets"),
    ]

    driver = du.new_driver(headless=not bool(args.headed))
    results: list[CaseResult] = []
    try:
        for c in cases:
            r = run_case_uc(
                driver,
                c,
                headed=bool(args.headed),
                debug=bool(args.debug),
                max_events=int(args.max_events),
                max_tickets_per_event=int(args.max_tickets_per_event),
            )
            results.append(r)
            with out_jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(r), ensure_ascii=False, default=str) + "\n")
                f.flush()
            _jitter(0.5, 1.1)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()

    print("")
    print("=== Stress test summary ===")
    print("name\tcategory\tkind\tstatus\tevents_found\tevents_tested\ttickets\tsnapshots_ok\tfailures")
    for r in results:
        print(
            f"{r.name}\t{r.category or ''}\t{r.target_kind}\t{r.status}\t{r.events_found}\t{r.events_tested}\t{r.ticket_urls_found}\t{r.snapshots_ok}\t{','.join(r.failures)}"
        )
    print("")
    print(f"results_jsonl: {out_jsonl.resolve()}")
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))
