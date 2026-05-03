#!/usr/bin/env python
from __future__ import annotations

import contextlib
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from discovery import discover_urls as du
from discovery.step2_discover_ticket_urls import extract_ticket_urls_from_loaded_selenium_page

try:
    from selenium.webdriver.common.by import By
except Exception:  # pragma: no cover
    By = None  # type: ignore

BASE_LISTING_URL = "https://www.ticketswap.com/festival-tickets"
CITY = "Amsterdam"
COUNTRY = "Netherlands"


def _pause(label: str) -> None:
    if sys.stdin.isatty():
        input(f"{label} Press ENTER to continue...")
    else:
        print(f"{label} Non-interactive run; skipping input pause.")


def _sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def _visible_text(driver: Any) -> str:
    with contextlib.suppress(Exception):
        return str(driver.execute_script("return document.body && document.body.innerText") or "")
    return ""


def _first_event_url_from_page(driver: Any, base_url: str) -> str:
    html = driver.page_source or ""
    hrefs = sorted(du.merge_link_candidates(html, driver, base_url=base_url))
    for h in hrefs:
        n = du.normalize_url(h)
        if not n:
            continue
        if not du.is_event_page(n):
            continue
        if du.is_festival_page(n) or du.is_ticket_url(n):
            continue
        return n
    return ""


def _click_event_link(driver: Any, event_url: str) -> bool:
    target = du.normalize_url(event_url) or event_url
    if not target:
        return False
    if By is not None:
        with contextlib.suppress(Exception):
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href]"):
                h = du.normalize_url(a.get_attribute("href") or "") or ""
                if h != target:
                    continue
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)
                _sleep(0.3)
                driver.execute_script("arguments[0].click();", a)
                return True
    with contextlib.suppress(Exception):
        return bool(
            driver.execute_script(
                """
                const target = arguments[0];
                const as = Array.from(document.querySelectorAll('a[href]'));
                for (const a of as) {
                  const href = a.href || a.getAttribute('href') || '';
                  if (href !== target) continue;
                  try { a.scrollIntoView({block:'center'}); a.click(); return true; } catch (e) {}
                }
                return false;
                """,
                target,
            )
        )
    return False


def _detect_status(*, current_url: str, html: str, visible_text: str) -> str:
    cur = (current_url or "").lower()
    txt = f"{html}\n{visible_text}".lower()
    if "/401" in cur or 'iframe src="/401"' in txt:
        return "401"
    if any(k in txt for k in ("captcha", "verify you are human", "unable to verify", "cloudflare", "<title>verifying")):
        return "verification_page"
    if any(k in txt for k in ("ticket", "tickets", "price", "prices", "€", "eur", "wanted")):
        return "real_event_page"
    return "unknown"


def _save_debug_artifacts(test_name: str, *, current_url: str, html: str, visible_text: str, driver: Any) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(config.DEBUG_DIR) / "local_visual" / f"{test_name}_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "current_url.txt").write_text(current_url or "", encoding="utf-8")
    (out_dir / "visible_text.txt").write_text(visible_text or "", encoding="utf-8")
    (out_dir / "page.html").write_text(html or "", encoding="utf-8")
    with contextlib.suppress(Exception):
        driver.save_screenshot(str(out_dir / "screenshot.png"))
    return str(out_dir)


def _common_listing_setup(driver: Any) -> tuple[str, str]:
    print(f"Opening listing: {BASE_LISTING_URL}")
    driver.get(BASE_LISTING_URL)
    du.wait_for_page_content(driver, headless=False)
    print("Pausing 10s for visual inspection of listing...")
    _sleep(10)
    sel = du.select_location_exact(
        driver,
        city=CITY,
        country=COUNTRY,
        expected_suggestion=f"{CITY}, {COUNTRY}",
    )
    if not bool(sel.get("success")):
        raise RuntimeError(f"location_select_failed: {sel.get('error_message')}")
    _sleep(random.uniform(5.0, 10.0))
    listing_url = str(sel.get("resulting_url") or getattr(driver, "current_url", "") or BASE_LISTING_URL)
    event_url = _first_event_url_from_page(driver, listing_url)
    if not event_url:
        raise RuntimeError("no_event_url_found_from_listing")
    print(f"Collected first event URL from page: {event_url}")
    return listing_url, event_url


def _finalize_test(test_name: str, event_url: str, driver: Any) -> dict[str, Any]:
    html = driver.page_source or ""
    current_url = str(getattr(driver, "current_url", "") or "")
    visible = _visible_text(driver)
    title = ""
    with contextlib.suppress(Exception):
        title = str(driver.title or "")
    print(f"[{test_name}] current_url: {current_url}")
    print(f"[{test_name}] title: {title}")
    print(f"[{test_name}] visible_text_sample: {(visible or '')[:500]}")
    status = _detect_status(current_url=current_url, html=html, visible_text=visible)
    debug_dir = _save_debug_artifacts(test_name, current_url=current_url, html=html, visible_text=visible, driver=driver)
    ticket_urls: list[str] = []
    strategy_used = "selenium_loaded_page_extract"
    extraction_debug: dict[str, Any] = {}
    if status == "real_event_page":
        ticket_urls, counts = extract_ticket_urls_from_loaded_selenium_page(driver, event_url=event_url)
        extraction_debug = {
            "status": status,
            "strategy_used": strategy_used,
            "ticket_urls_found": len(ticket_urls),
            "ticket_urls": ticket_urls,
            "counts": counts,
            "event_url": event_url,
            "current_url": current_url,
        }
        print(f"[{test_name}] strategy_used: {strategy_used}")
        print(f"[{test_name}] ticket_urls_found: {len(ticket_urls)}")
        print(f"[{test_name}] ticket_urls: {ticket_urls}")
        with contextlib.suppress(Exception):
            Path(debug_dir, "extraction_debug.json").write_text(
                json.dumps(extraction_debug, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
    return {
        "status": status,
        "event_url": event_url,
        "current_url": current_url,
        "visible_text_sample": (visible or "")[:500],
        "debug_dir": debug_dir,
        "strategy_used": strategy_used if status == "real_event_page" else None,
        "ticket_urls_found": len(ticket_urls),
        "ticket_urls": ticket_urls,
        "extraction_debug": extraction_debug,
    }


def _run_test_direct_url() -> dict[str, Any]:
    driver = du.new_driver(headless=False)
    try:
        _, event_url = _common_listing_setup(driver)
        driver.get(event_url)
        _sleep(random.uniform(10.0, 20.0))
        _pause("[Direct URL] Event opened.")
        du.scroll_for_lazy_content(driver)
        _pause("[Direct URL] After scroll.")
        return _finalize_test("test_a_direct_url", event_url, driver)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def _run_test_click_navigation() -> dict[str, Any]:
    driver = du.new_driver(headless=False)
    try:
        listing_url, event_url = _common_listing_setup(driver)
        clicked = _click_event_link(driver, event_url)
        if not clicked:
            raise RuntimeError("click_navigation_failed")
        _sleep(random.uniform(10.0, 20.0))
        _pause("[Click Nav] Event opened by click.")
        du.scroll_for_lazy_content(driver)
        _pause("[Click Nav] After scroll.")
        return _finalize_test("test_b_click_navigation", event_url, driver)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def _run_test_slow_navigation() -> dict[str, Any]:
    driver = du.new_driver(headless=False)
    try:
        listing_url, event_url = _common_listing_setup(driver)
        _sleep(random.uniform(3.0, 6.0))
        du.scroll_for_lazy_content(driver)
        _sleep(random.uniform(3.0, 8.0))
        clicked = _click_event_link(driver, event_url)
        if not clicked:
            raise RuntimeError("slow_click_navigation_failed")
        _sleep(random.uniform(12.0, 20.0))
        du.scroll_for_lazy_content(driver)
        _sleep(random.uniform(4.0, 8.0))
        _pause("[Slow Nav] Event opened by slow human-like path.")
        du.scroll_for_lazy_content(driver)
        _pause("[Slow Nav] After second scroll.")
        return _finalize_test("test_c_slow_navigation", event_url, driver)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def main() -> int:
    print("Running local visual STEP2 debugger (visible browser, single event per test).")
    direct = _run_test_direct_url()
    click = _run_test_click_navigation()
    slow = _run_test_slow_navigation()

    comparison = {
        "direct_url_status": direct.get("status"),
        "click_navigation_status": click.get("status"),
        "slow_navigation_status": slow.get("status"),
        "direct": direct,
        "click": click,
        "slow": slow,
    }
    print("\nComparison:")
    print(json.dumps(comparison, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
