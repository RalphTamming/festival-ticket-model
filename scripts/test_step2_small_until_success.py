#!/usr/bin/env python
from __future__ import annotations

import argparse
import contextlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from discovery import discover_urls as du

BASE_FESTIVAL_URL = "https://www.ticketswap.com/festival-tickets?slug=festival-tickets"
DEFAULT_STRATEGIES = ("s1", "s2", "s3", "s4", "s5", "s6")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tiny live STEP2 strategy loop until first fresh success.")
    p.add_argument("--city", default="Amsterdam")
    p.add_argument("--country", default="Netherlands")
    p.add_argument("--max-events", type=int, default=1)
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument("--wait-seconds", type=int, default=30)
    p.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES), help="Comma-separated strategies (e.g. s1,s3,s4)")
    return p.parse_args()


def _cache_listing(city: str, country: str) -> str:
    p = Path("data/location_cache.json")
    if not p.exists():
        return ""
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return ""
    key = f"{city},{country}"
    e = raw.get(key) if isinstance(raw, dict) else None
    if not isinstance(e, dict):
        return ""
    return str(e.get("resulting_url") or "").strip()


def _collect_fresh_events(city: str, country: str, max_events: int) -> tuple[str, list[str]]:
    listing = _cache_listing(city, country) or BASE_FESTIVAL_URL
    cmd = [
        sys.executable,
        "-m",
        "discovery.step1_collect_listing_urls",
        "--url",
        listing,
        "--min-events",
        str(max(1, int(max_events))),
        "--max-show-more",
        "2",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if p.returncode != 0:
        return listing, []
    out: list[str] = []
    mode = ""
    for ln in (p.stdout or "").splitlines():
        if ln.strip() == "EVENT_URLS":
            mode = "events"
            continue
        if mode != "events":
            continue
        n = du.normalize_url(ln.strip())
        if not n or not du.is_event_page(n) or du.is_festival_page(n) or du.is_ticket_url(n):
            continue
        if n not in out:
            out.append(n)
        if len(out) >= max(1, int(max_events)):
            break
    if out:
        return listing, out

    # Fallback A: fresh location selection in-browser (still live-only, no DB/cached events).
    driver = None
    try:
        driver = du.new_driver(headless=False)
        driver.set_page_load_timeout(90)
        driver.get(BASE_FESTIVAL_URL)
        du.wait_for_page_content(driver, headless=False)
        sel = du.select_location_exact(
            driver,
            city=city,
            country=country,
            expected_suggestion=f"{city}, {country}",
        )
        listing2 = str(sel.get("resulting_url") or getattr(driver, "current_url", "") or BASE_FESTIVAL_URL)
        if sel.get("success"):
            with contextlib.suppress(Exception):
                du.expand_category_listing_show_more(
                    driver,
                    listing2,
                    "festival-tickets",
                    max_clicks=2,
                )
            html = driver.page_source or ""
            hrefs = sorted(du.merge_link_candidates(html, driver, base_url=listing2))
            for h in hrefs:
                n = du.normalize_url(h)
                if not n or not du.is_event_page(n) or du.is_festival_page(n) or du.is_ticket_url(n):
                    continue
                if n not in out:
                    out.append(n)
                if len(out) >= max(1, int(max_events)):
                    break
            listing = listing2
    except Exception:
        pass
    finally:
        if driver is not None:
            with contextlib.suppress(Exception):
                driver.quit()
    if out:
        return listing, out

    # Fallback B: reuse benchmark listing strategy logic for one-city fresh sample.
    with contextlib.suppress(Exception):
        from scripts.benchmark_step2_strategies import _run_listing_strategy

        for st in ("l1_cached_resulting_url", "l2_fresh_select_location"):
            row = _run_listing_strategy(
                city=city,
                country=country,
                cached_url=_cache_listing(city, country),
                listing_strategy=st,
                headed=True,
                events_per_city=max(1, int(max_events)),
                debug=True,
                live_only=True,
            )
            events = list(getattr(row, "event_urls_sample", []) or [])
            resulting = str(getattr(row, "resulting_url", "") or listing)
            if events:
                return resulting, [du.normalize_url(e) or e for e in events[: max(1, int(max_events))]]
    return listing, out


def _run_one(strategy: str, *, event_url: str, city: str, country: str, listing_url: str, headed: bool, debug: bool, wait_seconds: int) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "scripts/debug_one_step2_live.py",
        "--event-url",
        event_url,
        "--strategy",
        strategy,
        "--city",
        city,
        "--country",
        country,
        "--listing-url",
        listing_url,
        "--wait-seconds",
        str(int(wait_seconds)),
    ]
    if headed:
        cmd.append("--headed")
    if debug:
        cmd.append("--debug")
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    payload: dict[str, Any] = {
        "strategy": strategy,
        "status": "error",
        "fresh_ticket_urls_found": 0,
        "classification": "unknown",
        "duration_seconds": None,
        "debug_dir": "",
        "error_message": f"debug_script_exit={p.returncode}",
    }
    txt = (p.stdout or "").strip()
    if txt:
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                payload.update(obj)
        except Exception:
            payload["raw_stdout"] = txt[-600:]
    if p.stderr:
        payload["stderr_tail"] = p.stderr[-600:]
    return payload


def main() -> int:
    args = _parse_args()
    strategies = tuple(s.strip().lower() for s in str(args.strategies or "").split(",") if s.strip())
    if not strategies:
        strategies = DEFAULT_STRATEGIES
    listing_url, events = _collect_fresh_events(args.city, args.country, args.max_events)
    result: dict[str, Any] = {
        "city": args.city,
        "country": args.country,
        "listing_url": listing_url,
        "fresh_events_collected": len(events),
        "events": events,
        "attempts": [],
        "winner": None,
        "strategies": list(strategies),
        "tested_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if not events:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    event_url = events[0]
    for s in strategies:
        r = _run_one(
            s,
            event_url=event_url,
            city=args.city,
            country=args.country,
            listing_url=listing_url,
            headed=bool(args.headed),
            debug=bool(args.debug),
            wait_seconds=int(args.wait_seconds),
        )
        result["attempts"].append(r)
        if int(r.get("fresh_ticket_urls_found") or 0) > 0:
            result["winner"] = s
            break

    out_dir = Path("data/outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"step2_small_until_success_{args.city.lower()}_{stamp}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    result["output"] = str(out_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("winner") else 2


if __name__ == "__main__":
    raise SystemExit(main())
