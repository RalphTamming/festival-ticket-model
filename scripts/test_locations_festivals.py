#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from discovery import discover_urls as du


VERIFIED_WESTERN_EUROPE_LOCATIONS: list[tuple[str, str]] = [
    ("Amsterdam", "Netherlands"),
    ("Rotterdam", "Netherlands"),
    ("Utrecht", "Netherlands"),
    ("Eindhoven", "Netherlands"),
    ("Groningen", "Netherlands"),
    ("Brussels", "Belgium"),
    ("Antwerp", "Belgium"),
    ("Ghent", "Belgium"),
    ("Berlin", "Germany"),
    ("Hamburg", "Germany"),
    ("Cologne", "Germany"),
    ("Munich", "Germany"),
    ("Paris", "France"),
    ("Lyon", "France"),
    ("Marseille", "France"),
]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Test festival listing event collection per verified location.")
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--max-show-more", type=int, default=3)
    p.add_argument("--max-events-per-location", type=int, default=10)
    p.add_argument("--debug", action="store_true", default=False)
    return p.parse_args()


def _load_cache(cache_path: Path) -> dict[str, Any]:
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _fresh(v: dict[str, Any], *, days: int = 30) -> bool:
    ts = str(v.get("last_verified_at", ""))
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(days=days)


def _city_country_from_key(key: str) -> tuple[str, str]:
    city, country = (key.split(",", 1) + [""])[:2]
    return city.strip(), country.strip()


def _verified_entries(cache: dict[str, Any]) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    for city, country in VERIFIED_WESTERN_EUROPE_LOCATIONS:
        key = f"{city},{country}"
        out.append((key, cache.get(key)))
    return out


def _looks_like_verification_block(page_source: str) -> bool:
    txt = (page_source or "").lower()
    return ("verify you are human" in txt) or ("captcha" in txt) or ("cloudflare" in txt)


def _collect_event_urls(driver: Any, base_url: str, *, max_show_more: int, max_events: int) -> list[str]:
    try:
        du._dismiss_page_overlays(driver)  # best-effort; non-critical if unavailable
    except Exception:
        pass
    du.wait_for_page_content(driver, headless=False)
    du.expand_festival_overview_show_more(driver, max_clicks=max(1, int(max_show_more)))
    html = driver.page_source or ""
    merged = du.merge_link_candidates(html, driver, base_url=base_url)
    out: list[str] = []
    seen: set[str] = set()
    for u in sorted(merged):
        n = du.normalize_url(u)
        if not n or not du.is_event_page(n):
            continue
        if du.is_ticket_url(n) or du.is_festival_page(n):
            continue
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
        if len(out) >= int(max_events):
            break
    return out


def main() -> int:
    args = _parse_args()
    cache = _load_cache(Path("data/location_cache.json"))
    entries = _verified_entries(cache)
    out_dir = Path("data/outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = Path(config.DEBUG_DIR) / "location_festivals"
    if args.debug:
        debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"location_festival_test_{stamp}.json"
    out_csv = out_dir / f"location_festival_test_{stamp}.csv"
    rows: list[dict[str, Any]] = []
    if not entries:
        out_json.write_text("[]", encoding="utf-8")
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "location",
                    "country_hint",
                    "selected_text",
                    "resulting_url",
                    "location_param",
                    "events_found",
                    "sample_event_urls",
                    "success",
                    "error_reason",
                ]
            )
        print("No fresh cached locations in data/location_cache.json.")
        return 2

    driver = du.new_driver(headless=not bool(args.headed))
    try:
        for key, entry in entries:
            city, country = _city_country_from_key(key)
            if not isinstance(entry, dict):
                rows.append(
                    {
                        "location": city,
                        "country_hint": country,
                        "selected_text": None,
                        "resulting_url": None,
                        "location_param": None,
                        "events_found": 0,
                        "sample_event_urls": [],
                        "success": False,
                        "error_reason": "missing_cache_entry",
                    }
                )
                continue
            if not _fresh(entry):
                rows.append(
                    {
                        "location": city,
                        "country_hint": country,
                        "selected_text": entry.get("selected_text"),
                        "resulting_url": entry.get("resulting_url"),
                        "location_param": entry.get("location_param"),
                        "events_found": 0,
                        "sample_event_urls": [],
                        "success": False,
                        "error_reason": "stale_cache_entry",
                    }
                )
                continue
            result_url = str(entry.get("resulting_url") or "").strip()
            row = {
                "location": city,
                "country_hint": country,
                "selected_text": entry.get("selected_text"),
                "resulting_url": result_url,
                "location_param": entry.get("location_param"),
                "events_found": 0,
                "sample_event_urls": [],
                "success": False,
                "error_reason": None,
            }
            try:
                if not result_url:
                    raise RuntimeError("missing_resulting_url_in_cache")
                driver.get(result_url)
                du.wait_for_page_content(driver, headless=not bool(args.headed))
                if _looks_like_verification_block(driver.page_source or ""):
                    row["error_reason"] = "verification_blocked"
                    rows.append(row)
                    continue
                selected = du.selected_location_text(driver)
                if selected:
                    row["selected_text"] = selected
                events = _collect_event_urls(
                    driver,
                    result_url,
                    max_show_more=int(args.max_show_more),
                    max_events=int(args.max_events_per_location),
                )
                row["events_found"] = len(events)
                row["sample_event_urls"] = events
                row["success"] = len(events) > 0
                if not row["success"]:
                    row["error_reason"] = "no_event_urls_found"
                    if args.debug:
                        safe = key.replace(",", "_").replace(" ", "_").lower()
                        try:
                            driver.save_screenshot(str(debug_dir / f"{stamp}_{safe}_no_events.png"))
                        except Exception:
                            pass
            except Exception as exc:
                row["error_reason"] = f"{type(exc).__name__}: {exc}"
                if args.debug:
                    safe = key.replace(",", "_").replace(" ", "_").lower()
                    try:
                        driver.save_screenshot(str(debug_dir / f"{stamp}_{safe}_error.png"))
                    except Exception:
                        pass
            rows.append(row)
    finally:
        driver.quit()

    out_json.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        cols = [
            "location",
            "country_hint",
            "selected_text",
            "resulting_url",
            "location_param",
            "events_found",
            "sample_event_urls",
            "success",
            "error_reason",
        ]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            rr = dict(r)
            rr["sample_event_urls"] = json.dumps(rr["sample_event_urls"], ensure_ascii=False)
            w.writerow(rr)
    ok = sum(1 for r in rows if r["success"])
    verification_blocked = sum(1 for r in rows if str(r.get("error_reason") or "") == "verification_blocked")
    amsterdam_row = next((r for r in rows if str(r.get("location") or "").strip().lower() == "amsterdam"), None)
    amsterdam_pass = bool(amsterdam_row and amsterdam_row.get("success"))
    print(
        json.dumps(
            {
                "ok": ok,
                "total": len(rows),
                "amsterdam_pass": amsterdam_pass,
                "verification_blocked": verification_blocked,
                "json": str(out_json),
                "csv": str(out_csv),
            },
            indent=2,
        )
    )
    return 0 if ok > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
