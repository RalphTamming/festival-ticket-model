#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from discovery import discover_urls as du
from discovery.step2_discover_ticket_urls import (
    Step2Result,
    discover_ticket_urls_from_event_playwright,
    discover_ticket_urls_from_event_selenium,
)

SCOPE_NAME = "western_europe_festivals_verified"
LISTING_STRATEGIES = (
    "l1_cached_resulting_url",
    "l2_fresh_select_location",
    "l3_cached_slow_show_more",
    "l4_fresh_select_slow_show_more",
)
STEP2_STRATEGIES = (
    "strategy_a_selenium_embedded_only",
    "strategy_b_selenium_slow_hydrate",
    "strategy_c_playwright_network",
    "strategy_d_hybrid_fast",
    "strategy_e_hybrid_safe",
)
BASE_FESTIVAL_URL = "https://www.ticketswap.com/festival-tickets?slug=festival-tickets"


@dataclass
class ListingBenchRow:
    city: str
    country: str
    listing_strategy: str
    listing_status: str
    event_urls_found: int
    duration_seconds: float
    resulting_url: str
    selected_location: str
    verification_detected: bool
    error_message: Optional[str]
    debug_dir: str
    event_urls_sample: list[str]


@dataclass
class Step2BenchRow:
    city: str
    country: str
    listing_strategy: str
    event_url: str
    strategy: str
    status: str
    zero_reason: Optional[str]
    fresh_ticket_urls_found: int
    duration_seconds: float
    verification_detected: bool
    current_url: str
    debug_dir: str
    ticket_urls_sample: list[str]
    error_message: Optional[str]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark listing + fresh STEP2 discovery strategies (live-only).")
    p.add_argument("--max-cities", type=int, default=15)
    p.add_argument("--events-per-city", type=int, default=3)
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument("--live-only", dest="live_only", action="store_true", default=True)
    p.add_argument("--no-live-only", dest="live_only", action="store_false")
    p.add_argument("--output", default="data/outputs/step2_strategy_benchmark_<timestamp>.json")
    p.add_argument("--csv", default="data/outputs/step2_strategy_benchmark_<timestamp>.csv")
    p.add_argument(
        "--listing-strategies",
        default=",".join(LISTING_STRATEGIES),
        help="Comma-separated listing strategies to run.",
    )
    p.add_argument(
        "--step2-strategies",
        default=",".join(STEP2_STRATEGIES),
        help="Comma-separated STEP2 strategies to run.",
    )
    p.add_argument("--max-events-total", type=int, default=45, help="Cap total unique events tested across all cities.")
    p.add_argument("--listing-only", action="store_true", default=False, help="Only benchmark listing collection, skip STEP2.")
    p.add_argument(
        "--isolated-profile",
        action="store_true",
        default=True,
        help="Use dedicated Chrome profile for benchmark to avoid conflicts with running production process.",
    )
    p.add_argument("--no-isolated-profile", dest="isolated_profile", action="store_false")
    p.add_argument(
        "--clone-base-profile",
        action="store_true",
        default=True,
        help="Clone the main profile into isolated benchmark profile to reuse logged-in session safely.",
    )
    p.add_argument("--no-clone-base-profile", dest="clone_base_profile", action="store_false")
    p.add_argument(
        "--quick",
        action="store_true",
        default=False,
        help="Tiny smoke test: 2 cities, 1 event/city, listing L1+L2, STEP2 hybrid-fast only.",
    )
    return p.parse_args()


def _stamp_paths(path_template: str, stamp: str) -> Path:
    return Path(path_template.replace("<timestamp>", stamp))


def _load_cache() -> dict[str, Any]:
    p = Path("data/location_cache.json")
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _safe_slug(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in value.strip())
    return out[:90] or "item"


def _event_slug(event_url: str) -> str:
    n = du.normalize_url(event_url) or event_url
    path = (n.split("/festival-tickets/", 1)[-1] if "/festival-tickets/" in n else n).strip("/")
    return _safe_slug(path or "event")


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _collect_scope_cities(max_cities: int) -> list[dict[str, str]]:
    cache = _load_cache()
    out: list[dict[str, str]] = []
    for city, country in list(getattr(config, "VERIFIED_WESTERN_EUROPE_LOCATIONS", []))[: max(1, int(max_cities))]:
        key = f"{city},{country}"
        entry = cache.get(key) if isinstance(cache.get(key), dict) else {}
        cached_url = str((entry or {}).get("resulting_url") or "").strip()
        out.append({"city": city, "country": country, "cached_url": cached_url})
    return out


def _collect_event_urls(driver: Any, listing_url: str, *, max_events: int, slow: bool, max_show_more: int) -> tuple[list[str], set[str], str]:
    events: list[str] = []
    hrefs_acc: set[str] = set()
    iterations = max(1, int(max_show_more) + 1)
    for i in range(iterations):
        html = driver.page_source or ""
        hrefs = du.merge_link_candidates(html, driver, base_url=listing_url)
        hrefs_acc |= set(hrefs)
        for u in sorted(hrefs):
            n = du.normalize_url(u)
            if not n or not du.is_event_page(n) or du.is_festival_page(n) or du.is_ticket_url(n):
                continue
            if n not in events:
                events.append(n)
            if len(events) >= max(1, int(max_events)):
                cur = str(getattr(driver, "current_url", "") or listing_url)
                return events[: max(1, int(max_events))], hrefs_acc, cur
        if i >= iterations - 1:
            break
        if slow:
            du.scroll_for_lazy_content(driver)
            time.sleep(1.8)
        clicked = du.expand_festival_overview_show_more(driver, max_clicks=1)
        if clicked <= 0:
            break
        time.sleep(1.2 if slow else 0.6)
    cur = str(getattr(driver, "current_url", "") or listing_url)
    return events[: max(1, int(max_events))], hrefs_acc, cur


def _verification_flags(html: str, text: str) -> dict[str, Any]:
    combined = f"{html}\n{text}".lower()
    return {
        "blocked_for_discovery": bool(du.is_blocked_for_discovery(html)),
        "looks_like_verification": bool(du.looks_like_verification(html)),
        "contains_captcha": ("captcha" in combined),
        "contains_cloudflare": ("cloudflare" in combined),
    }


def _save_listing_debug(
    *,
    city: str,
    country: str,
    strategy: str,
    html: str,
    visible_text: str,
    current_url: str,
    hrefs: set[str],
    selected_location: str,
    flags: dict[str, Any],
    screenshot: Optional[Callable[[str], Any]],
) -> str:
    root = Path(config.DEBUG_DIR) / "listing_benchmark" / f"{_safe_slug(city)}_{_safe_slug(country)}" / strategy
    root.mkdir(parents=True, exist_ok=True)
    (root / "page.html").write_text(html or "", encoding="utf-8")
    (root / "visible_text.txt").write_text(visible_text or "", encoding="utf-8")
    (root / "current_url.txt").write_text(current_url or "", encoding="utf-8")
    (root / "hrefs.txt").write_text("\n".join(sorted(hrefs)), encoding="utf-8")
    (root / "selected_location.txt").write_text(selected_location or "", encoding="utf-8")
    (root / "verification_signals.json").write_text(json.dumps(flags, ensure_ascii=False, indent=2), encoding="utf-8")
    if screenshot is not None:
        try:
            screenshot(str(root / "screenshot.png"))
        except Exception:
            pass
    return str(root)


def _run_listing_strategy(
    *,
    city: str,
    country: str,
    cached_url: str,
    listing_strategy: str,
    headed: bool,
    events_per_city: int,
    debug: bool,
    live_only: bool,
) -> ListingBenchRow:
    start = time.perf_counter()
    status = "error"
    error_message: Optional[str] = None
    events: list[str] = []
    resulting_url = ""
    selected_location = ""
    debug_dir = ""
    verification_detected = False
    hrefs: set[str] = set()
    html = ""
    visible = ""

    if (not live_only) is True:
        # Placeholder to keep explicit intent: benchmark is designed for live-only fresh collection.
        pass

    driver = None
    try:
        driver = du.new_driver(headless=not headed)
        driver.set_page_load_timeout(90)
        slow = listing_strategy in ("l3_cached_slow_show_more", "l4_fresh_select_slow_show_more")
        show_more = 6 if slow else 1

        if listing_strategy in ("l1_cached_resulting_url", "l3_cached_slow_show_more"):
            if not cached_url:
                status = "error"
                error_message = "missing_cached_resulting_url"
                resulting_url = ""
            else:
                driver.get(cached_url)
                html = du.wait_for_page_content(driver, headless=not headed)
                resulting_url = str(getattr(driver, "current_url", "") or cached_url)
        else:
            driver.get(BASE_FESTIVAL_URL)
            html = du.wait_for_page_content(driver, headless=not headed)
            sel = du.select_location_exact(
                driver,
                city=city,
                country=country,
                expected_suggestion=f"{city}, {country}",
                debug_dir=(Path(config.DEBUG_DIR) / "listing_benchmark" / f"{_safe_slug(city)}_{_safe_slug(country)}" / listing_strategy)
                if debug
                else None,
            )
            selected_location = str(sel.get("selected_text") or sel.get("selected_suggestion") or "")
            resulting_url = str(sel.get("resulting_url") or getattr(driver, "current_url", "") or BASE_FESTIVAL_URL)
            if not bool(sel.get("success")):
                status = "selector_failed"
                error_message = str(sel.get("error_message") or "select_location_failed")

        if status != "selector_failed" and resulting_url:
            html = driver.page_source or html
            try:
                visible = str(driver.execute_script("return document.body && document.body.innerText") or "")
            except Exception:
                visible = ""
            flags = _verification_flags(html, visible)
            verification_detected = bool(flags.get("blocked_for_discovery") or flags.get("looks_like_verification"))
            if verification_detected:
                status = "verification_blocked"
            else:
                if not selected_location:
                    selected_location = str(du.selected_location_text(driver) or "")
                events, hrefs, resulting_url = _collect_event_urls(
                    driver,
                    resulting_url,
                    max_events=events_per_city,
                    slow=slow,
                    max_show_more=show_more,
                )
                status = "ok" if events else "no_events_found"

            if debug and status != "ok":
                debug_dir = _save_listing_debug(
                    city=city,
                    country=country,
                    strategy=listing_strategy,
                    html=html,
                    visible_text=visible,
                    current_url=resulting_url,
                    hrefs=hrefs,
                    selected_location=selected_location,
                    flags=flags,
                    screenshot=getattr(driver, "save_screenshot", None),
                )
        elif status != "selector_failed":
            status = "error"
            if not error_message:
                error_message = "missing_resulting_url"

    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        if "timeout" in msg.lower():
            status = "timeout"
        else:
            status = "error"
        error_message = msg
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    return ListingBenchRow(
        city=city,
        country=country,
        listing_strategy=listing_strategy,
        listing_status=status,
        event_urls_found=len(events),
        duration_seconds=round(time.perf_counter() - start, 3),
        resulting_url=resulting_url,
        selected_location=selected_location,
        verification_detected=verification_detected,
        error_message=error_message,
        debug_dir=debug_dir,
        event_urls_sample=events,
    )


def _run_step2_call(fn: Callable[[], Step2Result], event_url: str) -> tuple[Step2Result, Optional[str], float]:
    start = time.perf_counter()
    try:
        res = fn()
        return res, None, round(time.perf_counter() - start, 3)
    except Exception as exc:
        ev = du.normalize_url(event_url) or event_url
        return Step2Result(ev, "error", False, "none", [], debug_dir=None), f"{type(exc).__name__}: {exc}", round(time.perf_counter() - start, 3)


def _strategy_a_embedded(event_url: str, headed: bool, debug: bool) -> tuple[Step2Result, Optional[str], float]:
    ev = du.normalize_url(event_url) or event_url

    def call() -> Step2Result:
        driver = du.new_driver(headless=not headed)
        dbg = Path(config.DEBUG_DIR) / "step2_benchmark" / "strategy_a_selenium_embedded_only" / _event_slug(ev)
        if debug:
            dbg.mkdir(parents=True, exist_ok=True)
        try:
            driver.get(ev)
            html = du.wait_for_page_content(driver, headless=not headed)
            visible = str(driver.execute_script("return document.body && document.body.innerText") or "")
            current = str(getattr(driver, "current_url", "") or ev)
            if du.is_blocked_for_discovery(html) or du.looks_like_verification(html):
                return Step2Result(ev, "blocked", True, "selenium_embedded_only", [], debug_dir=str(dbg) if debug else None)
            tickets: set[str] = set()
            for tu in du.extract_ticket_urls_from_eventtype_cache(html, base_url=ev):
                n = du.normalize_url(tu)
                if n and du.is_ticket_url(n) and du.normalize_url(du.event_url_from_ticket_url(n) or "") == ev:
                    tickets.add(n)
            if debug:
                (dbg / "page.html").write_text(html or "", encoding="utf-8")
                (dbg / "visible_text.txt").write_text(visible or "", encoding="utf-8")
                (dbg / "current_url.txt").write_text(current or "", encoding="utf-8")
                (dbg / "extracted_json_snippets.json").write_text(
                    json.dumps([{"embedded_ticket_count": len(tickets)}], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                try:
                    driver.save_screenshot(str(dbg / "screenshot.png"))
                except Exception:
                    pass
            return Step2Result(ev, "ok" if tickets else "no_data", False, "selenium_embedded_only", sorted(tickets), debug_dir=str(dbg) if debug else None)
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    return _run_step2_call(call, ev)


def _strategy_b(event_url: str, headed: bool, debug: bool) -> tuple[Step2Result, Optional[str], float]:
    return _run_step2_call(
        lambda: discover_ticket_urls_from_event_selenium(
            event_url,
            headed=headed,
            debug=debug,
            verification_wait_seconds=75,
            debug_root="step2_benchmark/strategy_b_selenium_slow_hydrate",
            wait_for_manual_verification=False,
            manual_verification_timeout=0,
        ),
        event_url,
    )


def _strategy_c(event_url: str, headed: bool, debug: bool) -> tuple[Step2Result, Optional[str], float]:
    return _run_step2_call(
        lambda: discover_ticket_urls_from_event_playwright(
            event_url,
            headed=headed,
            debug=debug,
            db_fallback=False,
            page_timeout_ms=60_000,
            pre_network_wait_ms=1500,
            post_network_wait_ms=5000,
            debug_root="step2_benchmark/strategy_c_playwright_network",
            wait_for_manual_verification=False,
            manual_verification_timeout=0,
        ),
        event_url,
    )


def _strategy_d(event_url: str, headed: bool, debug: bool) -> tuple[Step2Result, Optional[str], float]:
    start = time.perf_counter()
    r1, e1, _ = _strategy_a_embedded(event_url, headed, debug)
    if r1.status == "ok" and r1.ticket_urls:
        return r1, e1, round(time.perf_counter() - start, 3)
    r2, e2, _ = _strategy_c(event_url, headed, debug)
    return r2, (e2 or e1), round(time.perf_counter() - start, 3)


def _strategy_e(event_url: str, headed: bool, debug: bool) -> tuple[Step2Result, Optional[str], float]:
    start = time.perf_counter()
    r1, e1, _ = _strategy_a_embedded(event_url, headed, debug)
    if r1.status == "ok" and r1.ticket_urls:
        return r1, e1, round(time.perf_counter() - start, 3)
    r2, e2, _ = _strategy_b(event_url, headed, debug)
    if r2.status == "ok" and r2.ticket_urls:
        return r2, (e2 or e1), round(time.perf_counter() - start, 3)
    r3, e3, _ = _run_step2_call(
        lambda: discover_ticket_urls_from_event_playwright(
            event_url,
            headed=headed,
            debug=debug,
            db_fallback=False,
            page_timeout_ms=75_000,
            pre_network_wait_ms=3000,
            post_network_wait_ms=6000,
            debug_root="step2_benchmark/strategy_e_hybrid_safe",
            wait_for_manual_verification=False,
            manual_verification_timeout=0,
        ),
        event_url,
    )
    return r3, (e3 or e2 or e1), round(time.perf_counter() - start, 3)


def _step2_zero_reason(res: Step2Result, err: Optional[str]) -> Optional[str]:
    if res.ticket_urls:
        return None
    if err and "timeout" in err.lower():
        return "timeout"
    d = Path(res.debug_dir) if res.debug_dir else None
    html = _read_text(d / "page.html") if d else ""
    vis = _read_text(d / "visible_text.txt") if d else ""
    merged = f"{html}\n{vis}".lower()
    if "/401" in merged or 'iframe src="/401"' in merged:
        return "verification_page"
    if any(k in merged for k in ("verify", "captcha", "cloudflare", "unable to verify")):
        return "verification_page"
    if any(k in merged for k in ("hmm, 404", "couldn't find that page", "couldnt find that page", "we're a bit lost", "we’re a bit lost")):
        return "404"
    if res.status == "error":
        return "parsing_failure"
    return "real_event_page_but_no_ticket_data"


def _run_step2_for_event(
    *,
    city: str,
    country: str,
    listing_strategy: str,
    event_url: str,
    headed: bool,
    debug: bool,
    step2_strategies: tuple[str, ...],
) -> list[Step2BenchRow]:
    run_map: dict[str, Callable[[str, bool, bool], tuple[Step2Result, Optional[str], float]]] = {
        "strategy_a_selenium_embedded_only": _strategy_a_embedded,
        "strategy_b_selenium_slow_hydrate": _strategy_b,
        "strategy_c_playwright_network": _strategy_c,
        "strategy_d_hybrid_fast": _strategy_d,
        "strategy_e_hybrid_safe": _strategy_e,
    }
    out: list[Step2BenchRow] = []
    for s in step2_strategies:
        res, err, dur = run_map[s](event_url, headed, debug)
        if err and "timeout" in err.lower():
            status = "timeout"
        elif res.status == "blocked":
            status = "blocked"
        elif res.status == "error":
            status = "error"
        elif res.ticket_urls:
            status = "ok"
        else:
            status = "no_ticket_urls"
        cur = _read_text(Path(res.debug_dir) / "current_url.txt").strip() if res.debug_dir else ""
        out.append(
            Step2BenchRow(
                city=city,
                country=country,
                listing_strategy=listing_strategy,
                event_url=res.event_url,
                strategy=s,
                status=status,
                zero_reason=_step2_zero_reason(res, err),
                fresh_ticket_urls_found=len(res.ticket_urls),
                duration_seconds=dur,
                verification_detected=bool(res.verification),
                current_url=cur or res.event_url,
                debug_dir=str(res.debug_dir or ""),
                ticket_urls_sample=res.ticket_urls[:10],
                error_message=err,
            )
        )
    return out


def _summarize(
    listing_rows: list[ListingBenchRow],
    step2_rows: list[Step2BenchRow],
    total_events_sampled: int,
    *,
    listing_strategies: tuple[str, ...],
    step2_strategies: tuple[str, ...],
) -> dict[str, Any]:
    listing_summary: dict[str, Any] = {}
    for ls in listing_strategies:
        rows = [r for r in listing_rows if r.listing_strategy == ls]
        if not rows:
            continue
        listing_summary[ls] = {
            "runs": len(rows),
            "ok": sum(1 for r in rows if r.listing_status == "ok"),
            "verification_blocked": sum(1 for r in rows if r.listing_status == "verification_blocked"),
            "no_events_found": sum(1 for r in rows if r.listing_status == "no_events_found"),
            "selector_failed": sum(1 for r in rows if r.listing_status == "selector_failed"),
            "timeout": sum(1 for r in rows if r.listing_status == "timeout"),
            "error": sum(1 for r in rows if r.listing_status == "error"),
            "avg_duration_seconds": round(sum(r.duration_seconds for r in rows) / max(1, len(rows)), 3),
            "events_collected_total": sum(r.event_urls_found for r in rows),
        }

    step2_summary: dict[str, Any] = {}
    best: Optional[tuple[str, float, float]] = None
    for ss in step2_strategies:
        rows = [r for r in step2_rows if r.strategy == ss]
        if not rows:
            continue
        ok = sum(1 for r in rows if r.status == "ok")
        success_rate = ok / max(1, len(rows))
        avg = sum(r.duration_seconds for r in rows) / max(1, len(rows))
        step2_summary[ss] = {
            "runs": len(rows),
            "ok": ok,
            "success_rate": round(success_rate, 4),
            "avg_duration_seconds": round(avg, 3),
            "blocked": sum(1 for r in rows if r.status == "blocked"),
            "no_ticket_urls": sum(1 for r in rows if r.status == "no_ticket_urls"),
            "timeout": sum(1 for r in rows if r.status == "timeout"),
            "error": sum(1 for r in rows if r.status == "error"),
        }
        if best is None or success_rate > best[1] or (success_rate == best[1] and avg < best[2]):
            best = (ss, success_rate, avg)

    return {
        "total_listing_runs": len(listing_rows),
        "total_step2_runs": len(step2_rows),
        "total_unique_events_sampled": total_events_sampled,
        "listing_by_strategy": listing_summary,
        "step2_by_strategy": step2_summary,
        "recommended_strategy": best[0] if best else None,
    }


def main() -> int:
    args = _parse_args()
    listing_strategies = tuple(s.strip() for s in str(args.listing_strategies or "").split(",") if s.strip())
    step2_strategies = tuple(s.strip() for s in str(args.step2_strategies or "").split(",") if s.strip())
    if args.quick:
        args.max_cities = min(int(args.max_cities), 2)
        args.events_per_city = 1
        args.max_events_total = min(int(args.max_events_total), 2)
        listing_strategies = ("l1_cached_resulting_url", "l2_fresh_select_location")
        step2_strategies = ("strategy_d_hybrid_fast",)
    listing_strategies = tuple(s for s in listing_strategies if s in LISTING_STRATEGIES) or LISTING_STRATEGIES
    step2_strategies = tuple(s for s in step2_strategies if s in STEP2_STRATEGIES) or STEP2_STRATEGIES
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if bool(args.isolated_profile):
        original_profile_dir = Path(getattr(config, "BROWSER_PROFILE_DIR", ".ticketswap_browser_profile"))
        profile_dir = Path(config.DEBUG_DIR) / "benchmark_profiles" / stamp
        profile_dir.mkdir(parents=True, exist_ok=True)
        if bool(args.clone_base_profile) and original_profile_dir.exists():
            try:
                shutil.copytree(original_profile_dir, profile_dir, dirs_exist_ok=True)
            except Exception:
                # Best-effort copy; even partial cookie/session state can help.
                pass
        config.USE_PERSISTENT_BROWSER_PROFILE = True
        config.BROWSER_PROFILE_DIR = profile_dir
        config.BROWSER_PROFILE_NAME = "Default"
    out_json = _stamp_paths(args.output, stamp)
    out_csv = _stamp_paths(args.csv, stamp)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    cities = _collect_scope_cities(args.max_cities)
    listing_rows: list[ListingBenchRow] = []
    step2_rows: list[Step2BenchRow] = []
    sampled_events: set[str] = set()

    for c in cities:
        city = c["city"]
        country = c["country"]
        cached_url = c["cached_url"]
        for ls in listing_strategies:
            lrow = _run_listing_strategy(
                city=city,
                country=country,
                cached_url=cached_url,
                listing_strategy=ls,
                headed=bool(args.headed),
                events_per_city=max(1, int(args.events_per_city)),
                debug=bool(args.debug),
                live_only=bool(args.live_only),
            )
            listing_rows.append(lrow)
            if lrow.event_urls_found <= 0 or bool(args.listing_only):
                continue
            for ev in lrow.event_urls_sample:
                n = du.normalize_url(ev)
                if not n:
                    continue
                if n in sampled_events:
                    continue
                if len(sampled_events) >= max(1, int(args.max_events_total)):
                    continue
                sampled_events.add(n)
                step2_rows.extend(
                    _run_step2_for_event(
                        city=city,
                        country=country,
                        listing_strategy=ls,
                        event_url=n,
                        headed=bool(args.headed),
                        debug=bool(args.debug),
                        step2_strategies=step2_strategies,
                    )
                )

    summary = _summarize(
        listing_rows,
        step2_rows,
        total_events_sampled=len(sampled_events),
        listing_strategies=listing_strategies,
        step2_strategies=step2_strategies,
    )
    payload = {
        "scope": SCOPE_NAME,
        "live_only": bool(args.live_only),
        "cities_tested": cities,
        "listing_results": [asdict(r) for r in listing_rows],
        "step2_results": [asdict(r) for r in step2_rows],
        "summary": summary,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "row_type",
                "city",
                "country",
                "listing_strategy",
                "listing_status",
                "event_urls_found",
                "event_url",
                "step2_strategy",
                "step2_status",
                "zero_reason",
                "fresh_ticket_urls_found",
                "duration_seconds",
                "verification_detected",
                "current_or_resulting_url",
                "selected_location",
                "debug_dir",
                "error_message",
            ]
        )
        for r in listing_rows:
            w.writerow(
                [
                    "listing",
                    r.city,
                    r.country,
                    r.listing_strategy,
                    r.listing_status,
                    r.event_urls_found,
                    "",
                    "",
                    "",
                    "",
                    "",
                    r.duration_seconds,
                    r.verification_detected,
                    r.resulting_url,
                    r.selected_location,
                    r.debug_dir,
                    r.error_message or "",
                ]
            )
        for r in step2_rows:
            w.writerow(
                [
                    "step2",
                    r.city,
                    r.country,
                    r.listing_strategy,
                    "",
                    "",
                    r.event_url,
                    r.strategy,
                    r.status,
                    r.zero_reason or "",
                    r.fresh_ticket_urls_found,
                    r.duration_seconds,
                    r.verification_detected,
                    r.current_url,
                    "",
                    r.debug_dir,
                    r.error_message or "",
                ]
            )

    print(
        json.dumps(
            {
                "scope": SCOPE_NAME,
                "live_only": bool(args.live_only),
                "listing_runs": len(listing_rows),
                "step2_runs": len(step2_rows),
                "events_sampled": len(sampled_events),
                "recommended_strategy": summary.get("recommended_strategy"),
                "quick": bool(args.quick),
                "listing_only": bool(args.listing_only),
                "listing_strategies": list(listing_strategies),
                "step2_strategies": list(step2_strategies),
                "json": str(out_json),
                "csv": str(out_csv),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
