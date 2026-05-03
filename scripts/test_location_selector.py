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


TEST_LOCATIONS: list[dict[str, str]] = [
    {"city": "Amsterdam", "country": "Netherlands", "expected_suggestion": "Amsterdam, Netherlands"},
    {"city": "Rotterdam", "country": "Netherlands", "expected_suggestion": "Rotterdam, Netherlands"},
    {"city": "Utrecht", "country": "Netherlands", "expected_suggestion": "Utrecht, Netherlands"},
    {"city": "Eindhoven", "country": "Netherlands", "expected_suggestion": "Eindhoven, Netherlands"},
    {"city": "Groningen", "country": "Netherlands", "expected_suggestion": "Groningen, Netherlands"},
    {"city": "Brussels", "country": "Belgium", "expected_suggestion": "Brussels, Belgium"},
    {"city": "Antwerp", "country": "Belgium", "expected_suggestion": "Antwerp, Belgium"},
    {"city": "Ghent", "country": "Belgium", "expected_suggestion": "Ghent, Belgium"},
    {"city": "Berlin", "country": "Germany", "expected_suggestion": "Berlin, Germany"},
    {"city": "Hamburg", "country": "Germany", "expected_suggestion": "Hamburg, Germany"},
    {"city": "Cologne", "country": "Germany", "expected_suggestion": "Cologne, Germany"},
    {"city": "Munich", "country": "Germany", "expected_suggestion": "Munich, Germany"},
    {"city": "Paris", "country": "France", "expected_suggestion": "Paris, France"},
    {"city": "Lyon", "country": "France", "expected_suggestion": "Lyon, France"},
    {"city": "Marseille", "country": "France", "expected_suggestion": "Marseille, France"},
    {"city": "Luxembourg", "country": "Luxembourg", "expected_suggestion": "Luxembourg, Luxembourg"},
]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Test TicketSwap typed location selector only.")
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    return p.parse_args()


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _cache_recent(ts_iso: str, *, days: int = 30) -> bool:
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    except Exception:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(days=days)


def main() -> int:
    args = _parse_args()
    out_dir = Path("data/outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = Path(config.DEBUG_DIR) / "location_selector"
    if args.debug:
        debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"location_selector_test_{stamp}.json"
    out_csv = out_dir / f"location_selector_test_{stamp}.csv"
    cache_path = Path("data/location_cache.json")
    cache = _load_cache(cache_path)
    rows: list[dict[str, Any]] = []
    driver = du.new_driver(headless=not bool(args.headed))
    try:
        driver.set_page_load_timeout(120)
        for item in TEST_LOCATIONS:
            city = item["city"]
            country = item["country"]
            expected_suggestion = item["expected_suggestion"]
            driver.get("https://www.ticketswap.com/festival-tickets")
            du.wait_for_page_content(driver, headless=not bool(args.headed))
            city_debug_dir = (debug_dir / f"{city}_{country}".replace(" ", "_").lower()) if args.debug else None
            res = du.select_location(
                driver,
                city,
                country_hint=country,
                expected_suggestion=expected_suggestion,
                debug_dir=city_debug_dir,
            )
            row = {
                "city": city,
                "country": country,
                "expected_suggestion": expected_suggestion,
                "selected_suggestion": res.get("selected_suggestion"),
                "selected_dropdown_text": res.get("selected_dropdown_text"),
                "selected_text": res.get("selected_text"),
                "resulting_url": res.get("resulting_url"),
                "location_param": res.get("location_param"),
                "strategy_used": res.get("strategy_used"),
                "success": bool(res.get("success")),
                "error_message": res.get("error_message"),
                "suggestions_available": res.get("suggestions_available", []),
            }
            if not row["success"] and args.debug:
                safe = f"{city}_{country}".replace(" ", "_").lower()
                try:
                    driver.save_screenshot(str(debug_dir / f"{stamp}_{safe}_failed.png"))
                except Exception:
                    pass
            if row["success"]:
                key = f"{city},{country}"
                cache[key] = {
                    "city": city,
                    "country": country,
                    "expected_suggestion": expected_suggestion,
                    "selected_suggestion": row["selected_suggestion"],
                    "selected_dropdown_text": row["selected_dropdown_text"],
                    "selected_text": row["selected_text"],
                    "resulting_url": row["resulting_url"],
                    "location_param": row["location_param"],
                    "strategy_used": row["strategy_used"],
                    "last_verified_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                }
            else:
                key = f"{city},{country}"
                cache.pop(key, None)
            rows.append(row)
    finally:
        driver.quit()

    # keep only fresh cache entries
    fresh_cache: dict[str, Any] = {}
    for k, v in cache.items():
        ts = str(v.get("last_verified_at", ""))
        if _cache_recent(ts, days=30):
            fresh_cache[k] = v
    _save_cache(cache_path, fresh_cache)
    out_json.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        cols = [
            "city",
            "country",
            "expected_suggestion",
            "selected_suggestion",
            "selected_dropdown_text",
            "selected_text",
            "resulting_url",
            "location_param",
            "strategy_used",
            "success",
            "error_message",
            "suggestions_available",
        ]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            rr = dict(r)
            rr["suggestions_available"] = json.dumps(rr.get("suggestions_available", []), ensure_ascii=False)
            w.writerow(rr)
    ok = sum(1 for r in rows if r["success"])
    print(json.dumps({"ok": ok, "total": len(rows), "json": str(out_json), "csv": str(out_csv)}, indent=2))
    return 0 if ok > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
