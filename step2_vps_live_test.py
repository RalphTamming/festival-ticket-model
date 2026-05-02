from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from typing import Optional

import config
import db
from discovery import discover_urls as du
from discovery.step2_discover_ticket_urls import (
    Step2Result,
    discover_ticket_urls_from_event_playwright,
    discover_ticket_urls_from_event_selenium,
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="VPS STEP2 live discovery test (fresh URLs first, DB fallback optional).")
    p.add_argument("--event-url", required=True)
    p.add_argument("--browser", choices=["selenium", "playwright", "auto"], default="selenium")
    p.add_argument("--headed", action="store_true", default=False)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--verification-wait", type=int, default=60)
    p.add_argument("--wait-for-manual-verification", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument("--allow-db-fallback", action="store_true", default=False)
    p.add_argument("--blocked-sleep-min", type=int, default=30)
    p.add_argument("--blocked-sleep-max", type=int, default=90)
    return p.parse_args(argv)


def _discover_once(
    event_url: str,
    *,
    browser: str,
    headed: bool,
    debug: bool,
    verification_wait: int,
    wait_for_manual_verification: bool,
) -> Step2Result:
    if browser == "selenium":
        return discover_ticket_urls_from_event_selenium(
            event_url,
            headed=headed,
            debug=debug,
            verification_wait_seconds=verification_wait,
            debug_root="step2_vps_live",
            wait_for_manual_verification=wait_for_manual_verification,
            manual_verification_timeout=300,
        )
    return discover_ticket_urls_from_event_playwright(
        event_url,
        headed=headed,
        debug=debug,
        db_fallback=False,
        page_timeout_ms=75_000,
        pre_network_wait_ms=3000,
        post_network_wait_ms=5000,
        debug_root="step2_vps_live",
        wait_for_manual_verification=wait_for_manual_verification,
        manual_verification_timeout=300,
    )


def _discover_with_retries(args: argparse.Namespace) -> tuple[Step2Result, int]:
    order: list[str]
    if args.browser == "auto":
        order = ["selenium", "playwright"]
    elif args.browser == "selenium":
        order = ["selenium", "playwright"]
    else:
        order = ["playwright", "selenium"]

    last = Step2Result(args.event_url, "no_data", False, "none", [], debug_dir=None)
    attempts = 0
    for attempt in range(max(1, int(args.retries))):
        attempts += 1
        for browser in order:
            last = _discover_once(
                args.event_url,
                browser=browser,
                headed=bool(args.headed),
                debug=bool(args.debug),
                verification_wait=int(args.verification_wait),
                wait_for_manual_verification=bool(args.wait_for_manual_verification),
            )
            if last.status == "ok" and last.ticket_urls:
                return last, attempts
        if attempt < int(args.retries) - 1:
            a = float(args.blocked_sleep_min)
            b = float(max(args.blocked_sleep_min, args.blocked_sleep_max))
            time.sleep(a + random.random() * (b - a))
    return last, attempts


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    res, attempts = _discover_with_retries(args)
    used_fallback = False
    ticket_urls = list(res.ticket_urls)

    if not ticket_urls and args.allow_db_fallback and config.DB_PATH.exists():
        conn = db.connect(config.DB_PATH)
        try:
            event_url = du.normalize_url(args.event_url) or args.event_url
            known = db.list_ticket_urls_for_event(conn, event_url=event_url)
        finally:
            conn.close()
        if known:
            ticket_urls = known
            used_fallback = True

    final_url = ""
    if res.debug_dir:
        cu = du.normalize_url((Path(res.debug_dir) / "current_url.txt").read_text(encoding="utf-8").strip()) if (Path(res.debug_dir) / "current_url.txt").exists() else ""
        final_url = cu or ""
    payload = {
        "status": "ok" if ticket_urls else ("blocked" if res.status == "blocked" else res.status),
        "browser_used": args.browser if args.browser != "auto" else "selenium_then_playwright",
        "fresh_ticket_urls_found": 0 if used_fallback else len(ticket_urls),
        "ticket_urls": ticket_urls,
        "verification_detected": bool(res.verification),
        "final_url": final_url,
        "strategy_used": "db_fallback_due_to_block" if used_fallback else res.strategy,
        "debug_artifact_path": res.debug_dir,
        "attempts": attempts,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["fresh_ticket_urls_found"] > 0 else (2 if payload["status"] == "blocked" else 1)


if __name__ == "__main__":
    raise SystemExit(main())
