"""
Run the scheduler once: pick due TicketSwap URLs, scrape them, and store snapshots.

Manual workflow:
  python discover_urls.py
  python run_scheduler.py

This scheduler is intentionally simple:
- Computes an interval tier from time-to-event (or URL date fallback).
- Uses temporary backoff on repeated failures.
- Calls the real scraper in `scrape_market.py` (no placeholder).
"""

from __future__ import annotations

import argparse
import logging
import math
import re
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

import config
import db as dbmod
from scrape_market import market_scrape_session, scrape_market_with_driver


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


LOGGER = logging.getLogger("ticketswap.scheduler")
DATE_IN_URL_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def infer_event_dt_utc(start_datetime_utc: Optional[str], ticket_url: str) -> Optional[datetime]:
    if start_datetime_utc:
        dt = parse_iso_dt(start_datetime_utc)
        if dt:
            return dt
    m = DATE_IN_URL_RE.search(ticket_url)
    if not m:
        return None
    try:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(y, mo, d, tzinfo=timezone.utc)
    except Exception:
        return None


def tier_for_event_dt(event_dt: Optional[datetime]) -> tuple[int, int, bool]:
    """
    Returns (interval_minutes, priority, active_for_scraping).
    """
    if event_dt is None:
        return 24 * 60, 10, True

    now = utc_now()
    if now >= event_dt + timedelta(days=1):
        return 0, 0, False

    if now.date() == event_dt.date():
        return 15, 100, True

    days_until = (event_dt - now).total_seconds() / 86400.0
    if days_until > 60:
        return 24 * 60, 20, True
    if 14 <= days_until <= 60:
        return 12 * 60, 40, True
    if 7 <= days_until < 14:
        return 4 * 60, 60, True
    if 2 <= days_until < 7:
        return 2 * 60, 75, True
    if 0 <= days_until < 2:
        return 30, 90, True
    return 0, 0, False


def compute_next_scrape(now: datetime, interval_minutes: int, jitter_seconds: int = 30) -> datetime:
    return now + timedelta(minutes=max(1, int(interval_minutes))) + timedelta(seconds=int(jitter_seconds * (0.25 + 0.75 * (now.microsecond % 1000) / 1000.0)))


def backoff_minutes_for_failures(failures: int) -> int:
    if failures <= 1:
        return config.FAILURE_BACKOFF_BASE_MINUTES
    minutes = int((2 ** min(failures, 8)) * config.FAILURE_BACKOFF_BASE_MINUTES)
    return min(minutes, config.FAILURE_BACKOFF_CAP_MINUTES)


def ensure_schedule_rows(conn, *, force_due_once: bool, override_interval_minutes: Optional[int]) -> None:
    rows = conn.execute(
        """
        SELECT tu.ticket_url_id, tu.ticket_url, e.start_datetime_utc
        FROM ticket_urls tu
        JOIN events e ON e.event_id = tu.event_id
        WHERE tu.is_active=1
        """
    ).fetchall()

    now = utc_now()
    for r in rows:
        ticket_url_id = int(r["ticket_url_id"])
        ticket_url = str(r["ticket_url"])
        event_dt = infer_event_dt_utc(r["start_datetime_utc"], ticket_url)
        interval, priority, active = tier_for_event_dt(event_dt)
        if override_interval_minutes is not None:
            interval = int(override_interval_minutes)
        next_at = now if force_due_once else compute_next_scrape(now, interval)
        dbmod.upsert_schedule_row(
            conn,
            ticket_url_id=ticket_url_id,
            active_for_scraping=active,
            scrape_interval_minutes=max(1, int(interval or 1)),
            scrape_priority=int(priority),
            next_scrape_at_utc=next_at,
            update_next=force_due_once,
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one scheduler cycle (select due URLs, scrape, store snapshots).")
    p.add_argument("--limit", type=int, default=config.DEFAULT_JOB_LIMIT)
    p.add_argument("--headless", action="store_true", default=False)
    p.add_argument("--force-due-once", action="store_true", help="For testing: force next_scrape_at to now for all active URLs.")
    p.add_argument("--override-interval-minutes", type=int, default=None, help="For testing: override tier interval.")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--print-only", action="store_true", help="Only print due URLs, do not scrape.")
    return p.parse_args(list(argv))


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    conn = dbmod.connect(config.DB_PATH)
    try:
        dbmod.init_db(conn)
        ensure_schedule_rows(conn, force_due_once=bool(args.force_due_once), override_interval_minutes=args.override_interval_minutes)

        due = list(dbmod.get_ticket_urls_due(conn, limit=int(args.limit)))
        if not due:
            print("No due URLs.")
            return 0

        print(f"Due URLs: {len(due)}")
        for r in due:
            print(f"- id={r['ticket_url_id']} every={r['scrape_interval_minutes']}m url={r['ticket_url']}")

        if args.print_only:
            return 0

        ok = 0
        failed = 0
        headless = bool(args.headless)
        manual_wait = int(config.MANUAL_VERIFY_WAIT_SECONDS) if not headless else 0
        with market_scrape_session(headless=headless) as scrape_driver:
            for r in due:
                ticket_url_id = int(r["ticket_url_id"])
                url = str(r["ticket_url"])
                failures = int(r["consecutive_failures"] or 0)
                base_interval = int(r["scrape_interval_minutes"] or 1440)

                snap = scrape_market_with_driver(
                    scrape_driver,
                    url,
                    debug_dir=config.DEBUG_DIR,
                    headless=headless,
                    manual_wait_seconds=manual_wait,
                )

                snapshot_id = dbmod.insert_market_snapshot(conn, ticket_url_id=ticket_url_id, snap=snap)

                now = utc_now()
                if snap.status in {"ok", "no_data"}:
                    ok += 1
                    next_at = compute_next_scrape(now, base_interval)
                    dbmod.mark_scrape_success(conn, ticket_url_id=ticket_url_id, next_scrape_at_utc=next_at)
                    LOGGER.info("Saved snapshot_id=%s status=%s url=%s", snapshot_id, snap.status, url)
                else:
                    failed += 1
                    failures2 = failures + 1
                    backoff_m = backoff_minutes_for_failures(failures2)
                    backoff_until = now + timedelta(minutes=backoff_m)
                    next_at = max(backoff_until, now + timedelta(minutes=max(5, base_interval // 2)))
                    dbmod.mark_scrape_failure(
                        conn,
                        ticket_url_id=ticket_url_id,
                        consecutive_failures=failures2,
                        backoff_until_utc=backoff_until,
                        next_scrape_at_utc=next_at,
                    )
                    LOGGER.warning("Scrape failed status=%s url=%s err=%s", snap.status, url, snap.error_message)

        print("")
        print("=== Scheduler run summary ===")
        print(f"ok: {ok}")
        print(f"failed: {failed}")
        return 0 if failed == 0 else 2
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))

