#!/usr/bin/env python
from __future__ import annotations

import argparse
import contextlib
import json
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from discovery import discover_urls as du

try:
    from selenium.webdriver.common.by import By
except Exception:  # pragma: no cover
    By = None  # type: ignore


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Debug one live STEP2 event page and classify failure mode.")
    p.add_argument("--event-url", required=True)
    p.add_argument(
        "--strategy",
        default="selenium",
        choices=["selenium", "s1", "s2", "s3", "s4", "s5", "s6", "playwright"],
    )
    p.add_argument("--city", default="Amsterdam")
    p.add_argument("--country", default="Netherlands")
    p.add_argument("--listing-url", default="")
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument("--wait-seconds", type=int, default=30)
    return p.parse_args()


def _safe_slug(value: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip())
    return out[:100] or "item"


def _event_slug(event_url: str) -> str:
    n = du.normalize_url(event_url) or event_url
    path = (n.split("/festival-tickets/", 1)[-1] if "/festival-tickets/" in n else n).strip("/")
    return _safe_slug(path or "event")


def _debug_dir(strategy: str, event_url: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    d = Path(config.DEBUG_DIR) / "step2_live_debug" / strategy / f"{_event_slug(event_url)}_{ts}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _collect_cache_listing(city: str, country: str) -> str:
    p = Path("data/location_cache.json")
    if not p.exists():
        return ""
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return ""
    key = f"{city},{country}"
    entry = raw.get(key) if isinstance(raw, dict) else None
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("resulting_url") or "").strip()


def _extract_ticket_urls_for_event(html: str, event_url: str) -> list[str]:
    ev = du.normalize_url(event_url) or event_url
    out: set[str] = set()
    for u in du.extract_ticket_urls_from_page_text(html, base_url=ev):
        n = du.normalize_url(u)
        if n and du.is_ticket_url(n) and du.normalize_url(du.event_url_from_ticket_url(n) or "") == ev:
            out.add(n)
    for u in du.extract_ticket_urls_from_eventtype_cache(html, base_url=ev):
        n = du.normalize_url(u)
        if n and du.is_ticket_url(n) and du.normalize_url(du.event_url_from_ticket_url(n) or "") == ev:
            out.add(n)
    for u in du.extract_next_data_link_candidates(html, base_url=ev):
        n = du.normalize_url(u)
        if n and du.is_ticket_url(n) and du.normalize_url(du.event_url_from_ticket_url(n) or "") == ev:
            out.add(n)
    return sorted(out)


def _classify_page(*, current_url: str, html: str, visible_text: str, ticket_urls_found: int) -> str:
    text = f"{html}\n{visible_text}".lower()
    cur = (current_url or "").lower()
    if "/401" in cur or 'iframe src="/401"' in text or "forbidden" in text or "unauthorized" in text:
        return "401_or_forbidden"
    if "/login" in cur or "log in" in text or "sign in" in text:
        return "login_required"
    if any(k in text for k in ("captcha", "unable to verify", "verify you are human", "cloudflare", "<title>verifying")):
        return "verification_page"
    if any(k in text for k in ("hmm, 404", "we're a bit lost", "we’re a bit lost", "couldn't find that page", "couldnt find that page")):
        return "404"
    if ticket_urls_found > 0:
        return "real_event_page"
    if len((visible_text or "").strip()) < 20:
        return "empty_hydration"
    if "/festival-tickets/" in cur:
        return "real_event_page"
    return "unknown"


def _save_state(
    *,
    out_dir: Path,
    current_url: str,
    html: str,
    visible_text: str,
    cookies: list[dict[str, Any]],
    local_storage: dict[str, Any],
    hrefs: list[str],
    network_urls: list[str],
    extracted_json_snippets: list[dict[str, Any]],
    screenshot_fn: Optional[Any],
) -> None:
    (out_dir / "current_url.txt").write_text(current_url or "", encoding="utf-8")
    (out_dir / "page.html").write_text(html or "", encoding="utf-8")
    (out_dir / "visible_text.txt").write_text(visible_text or "", encoding="utf-8")
    (out_dir / "cookies.json").write_text(json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "local_storage.json").write_text(json.dumps(local_storage, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "hrefs.txt").write_text("\n".join(hrefs), encoding="utf-8")
    (out_dir / "network_urls.txt").write_text("\n".join(network_urls), encoding="utf-8")
    (out_dir / "extracted_json_snippets.json").write_text(
        json.dumps(extracted_json_snippets, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if screenshot_fn is not None:
        try:
            screenshot_fn(str(out_dir / "screenshot.png"))
        except Exception:
            pass


def _first_event_from_listing(driver: Any, listing_url: str, *, target_event: str = "") -> tuple[str, list[str]]:
    driver.get(listing_url)
    du.wait_for_page_content(driver, headless=False)
    du.scroll_for_lazy_content(driver)
    hrefs = sorted(du.merge_link_candidates(driver.page_source or "", driver, base_url=listing_url))
    evs = [u for u in hrefs if du.normalize_url(u) and du.is_event_page(u) and (not du.is_festival_page(u)) and (not du.is_ticket_url(u))]
    if target_event:
        t = du.normalize_url(target_event) or target_event
        for ev in evs:
            if (du.normalize_url(ev) or ev) == t:
                return ev, evs
    return (evs[0] if evs else ""), evs


def _click_event_from_listing(
    driver: Any,
    listing_url: str,
    *,
    target_event: str = "",
    pre_click_slow: bool = False,
    max_show_more: int = 2,
) -> tuple[str, list[str], bool]:
    driver.get(listing_url)
    du.wait_for_page_content(driver, headless=False)
    for _ in range(max(0, int(max_show_more))):
        du.scroll_for_lazy_content(driver)
        with contextlib.suppress(Exception):
            driver.execute_script(
                """
                const root = document.querySelector('main') || document.body;
                const needles = ['show more', 'load more', 'toon meer', 'meer tonen'];
                const els = Array.from(root.querySelectorAll('button, a, [role="button"]'));
                for (const el of els) {
                  const t = String(el.textContent || '').toLowerCase().replace(/\\s+/g,' ').trim();
                  if (!needles.some(n => t.includes(n))) continue;
                  try { el.scrollIntoView({block:'center'}); el.click(); } catch(e) {}
                  break;
                }
                """
            )
        time.sleep(1.0)

    if pre_click_slow:
        du.scroll_for_lazy_content(driver)
        time.sleep(random.uniform(3.0, 8.0))

    html = driver.page_source or ""
    hrefs = sorted(du.merge_link_candidates(html, driver, base_url=listing_url))
    events = [
        u
        for u in hrefs
        if du.normalize_url(u) and du.is_event_page(u) and (not du.is_festival_page(u)) and (not du.is_ticket_url(u))
    ]
    picked = ""
    if target_event:
        t = du.normalize_url(target_event) or target_event
        for ev in events:
            if (du.normalize_url(ev) or ev) == t:
                picked = ev
                break
        if not picked:
            # Keep target for referer-preserving injected click fallback.
            picked = t
    if not picked and events:
        picked = events[0]
    if not picked:
        return "", events, False

    clicked = False
    if By is not None:
        with contextlib.suppress(Exception):
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href]"):
                h = du.normalize_url(a.get_attribute("href") or "") or ""
                if h != (du.normalize_url(picked) or picked):
                    continue
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", a)
                clicked = True
                break
    if not clicked:
        with contextlib.suppress(Exception):
            clicked = bool(
                driver.execute_script(
                    """
                    const target = arguments[0];
                    const as = Array.from(document.querySelectorAll('a[href]'));
                    for (const a of as) {
                      const href = a.href || a.getAttribute('href') || '';
                      if (!href || !href.includes('/festival-tickets/')) continue;
                      if (href !== target) continue;
                      try { a.scrollIntoView({block:'center'}); a.click(); return true; } catch(e) {}
                    }
                    return false;
                    """,
                    du.normalize_url(picked) or picked,
                )
            )
    if not clicked:
        # Fallback: inject an anchor into current listing page and click it.
        with contextlib.suppress(Exception):
            clicked = bool(
                driver.execute_script(
                    """
                    const target = arguments[0];
                    try {
                      const a = document.createElement('a');
                      a.href = target;
                      a.id = '__step2_debug_injected_event_link';
                      a.style.display = 'none';
                      document.body.appendChild(a);
                      a.click();
                      return true;
                    } catch (e) {
                      return false;
                    }
                    """,
                    du.normalize_url(picked) or picked,
                )
            )
    if not clicked:
        # last resort: still move forward so strategy can be compared
        driver.get(picked)
    return picked, events, clicked


def run_selenium_strategy(args: argparse.Namespace) -> dict[str, Any]:
    strategy = str(args.strategy).lower()
    event_url = du.normalize_url(args.event_url) or args.event_url
    listing_url = args.listing_url.strip() or _collect_cache_listing(args.city, args.country) or "https://www.ticketswap.com/festival-tickets?slug=festival-tickets"
    out_dir = _debug_dir(f"strategy_{strategy}", event_url)
    try:
        driver = du.new_driver(headless=not bool(args.headed))
    except Exception as exc:
        (out_dir / "startup_error.txt").write_text(f"{type(exc).__name__}: {exc}", encoding="utf-8")
        return {
            "strategy": strategy,
            "status": "error",
            "classification": "unknown",
            "fresh_ticket_urls_found": 0,
            "ticket_urls_sample": [],
            "current_url": event_url,
            "debug_dir": str(out_dir),
            "duration_seconds": 0.0,
            "error_message": f"driver_start_failed: {type(exc).__name__}: {exc}",
        }
    start = time.perf_counter()
    network_urls: list[str] = []
    snippets: list[dict[str, Any]] = []
    try:
        target_url = event_url
        if strategy in ("s2",):
            driver.get("https://www.ticketswap.com/")
            time.sleep(min(12, max(3, int(args.wait_seconds // 3))))
            driver.get(listing_url)
            time.sleep(min(12, max(3, int(args.wait_seconds // 3))))
        elif strategy in ("s3", "s4", "s6"):
            chosen, evs, clicked = _click_event_from_listing(
                driver,
                listing_url,
                target_event=event_url,
                pre_click_slow=(strategy == "s4"),
                max_show_more=2,
            )
            snippets.append({"listing_event_count": len(evs), "clicked_from_listing": bool(clicked)})
            if not chosen:
                target_url = ""
            else:
                target_url = chosen

        if not target_url:
            html = driver.page_source or ""
            vis = ""
            with contextlib.suppress(Exception):
                vis = str(driver.execute_script("return document.body && document.body.innerText") or "")
            _save_state(
                out_dir=out_dir,
                current_url=str(getattr(driver, "current_url", "") or listing_url),
                html=html,
                visible_text=vis,
                cookies=[],
                local_storage={},
                hrefs=[],
                network_urls=[],
                extracted_json_snippets=snippets,
                screenshot_fn=getattr(driver, "save_screenshot", None),
            )
            return {
                "strategy": strategy,
                "status": "error",
                "classification": "unknown",
                "fresh_ticket_urls_found": 0,
                "current_url": str(getattr(driver, "current_url", "") or ""),
                "debug_dir": str(out_dir),
                "duration_seconds": round(time.perf_counter() - start, 3),
                "error_message": "no_event_link_from_listing",
            }

        if strategy not in ("s3", "s4", "s6"):
            driver.get(target_url)
        if strategy == "s4":
            time.sleep(random.uniform(10.0, 20.0))
            du.scroll_for_lazy_content(driver)
        else:
            time.sleep(max(5, int(args.wait_seconds)))
        html = driver.page_source or ""
        visible = ""
        with contextlib.suppress(Exception):
            visible = str(driver.execute_script("return document.body && document.body.innerText") or "")
        cur = str(getattr(driver, "current_url", "") or target_url)

        if strategy == "s6":
            cls0 = _classify_page(current_url=cur, html=html, visible_text=visible, ticket_urls_found=0)
            if cls0 in ("verification_page", "401_or_forbidden", "login_required", "empty_hydration"):
                driver.get("https://www.ticketswap.com/")
                time.sleep(random.uniform(60.0, 120.0))
                _, _, _ = _click_event_from_listing(
                    driver,
                    listing_url,
                    target_event=target_url,
                    pre_click_slow=True,
                    max_show_more=2,
                )
                time.sleep(max(10, int(args.wait_seconds)))
                html = driver.page_source or ""
                with contextlib.suppress(Exception):
                    visible = str(driver.execute_script("return document.body && document.body.innerText") or "")
                cur = str(getattr(driver, "current_url", "") or "")

        ticket_urls = _extract_ticket_urls_for_event(html, event_url=target_url)
        cls = _classify_page(current_url=cur, html=html, visible_text=visible, ticket_urls_found=len(ticket_urls))
        cookies = []
        with contextlib.suppress(Exception):
            cookies = list(driver.get_cookies() or [])
        local_storage = {}
        with contextlib.suppress(Exception):
            local_storage = dict(driver.execute_script("var o={}; for (var i=0;i<localStorage.length;i++){var k=localStorage.key(i);o[k]=localStorage.getItem(k);} return o;") or {})
        hrefs = sorted(du.merge_link_candidates(html, driver, base_url=cur))
        _save_state(
            out_dir=out_dir,
            current_url=cur,
            html=html,
            visible_text=visible,
            cookies=cookies,
            local_storage=local_storage,
            hrefs=hrefs,
            network_urls=network_urls,
            extracted_json_snippets=[{"ticket_urls_sample": ticket_urls[:10], "classification": cls}] + snippets,
            screenshot_fn=getattr(driver, "save_screenshot", None),
        )
        status = "ok" if ticket_urls else "no_ticket_urls"
        return {
            "strategy": strategy,
            "status": status,
            "classification": cls,
            "fresh_ticket_urls_found": len(ticket_urls),
            "ticket_urls_sample": ticket_urls[:10],
            "current_url": cur,
            "debug_dir": str(out_dir),
            "duration_seconds": round(time.perf_counter() - start, 3),
            "error_message": None,
        }
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def run_playwright_strategy(args: argparse.Namespace) -> dict[str, Any]:
    # S5 minimal implementation via existing playwright Step2 helper path.
    from discovery.step2_discover_ticket_urls import discover_ticket_urls_from_event_playwright

    strategy = str(args.strategy).lower()
    event_url = du.normalize_url(args.event_url) or args.event_url
    start = time.perf_counter()
    out_dir = _debug_dir(f"strategy_{strategy}", event_url)
    res = discover_ticket_urls_from_event_playwright(
        event_url,
        headed=bool(args.headed),
        debug=True,
        db_fallback=False,
        page_timeout_ms=60_000,
        pre_network_wait_ms=1500,
        post_network_wait_ms=5000,
        debug_root=f"step2_live_debug/strategy_{strategy}",
        wait_for_manual_verification=False,
        manual_verification_timeout=0,
    )
    debug_dir = Path(res.debug_dir) if res.debug_dir else out_dir
    page_html = _read_text(debug_dir / "page.html")
    vis = _read_text(debug_dir / "visible_text.txt")
    cur = _read_text(debug_dir / "current_url.txt").strip() or res.event_url
    cls = _classify_page(current_url=cur, html=page_html, visible_text=vis, ticket_urls_found=len(res.ticket_urls))
    return {
        "strategy": strategy,
        "status": "ok" if res.ticket_urls else ("blocked" if res.status == "blocked" else "no_ticket_urls"),
        "classification": cls,
        "fresh_ticket_urls_found": len(res.ticket_urls),
        "ticket_urls_sample": list(res.ticket_urls[:10]),
        "current_url": cur,
        "debug_dir": str(debug_dir),
        "duration_seconds": round(time.perf_counter() - start, 3),
        "error_message": None,
    }


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def main() -> int:
    args = _parse_args()
    strategy = str(args.strategy).lower()
    if strategy in ("playwright", "s5"):
        result = run_playwright_strategy(args)
    else:
        result = run_selenium_strategy(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if int(result.get("fresh_ticket_urls_found") or 0) > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
