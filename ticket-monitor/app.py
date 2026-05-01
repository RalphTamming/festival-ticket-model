"""
Ticket market monitor runner (visible-browser friendly).

Runs one scrape immediately on startup, then repeats every N minutes using APScheduler.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Type
import logging
import os
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config
from config import Target
from db import connect, init_db, save_snapshot
from scrapers.base import BaseScraper
from scrapers.example_playwright_scraper import ExamplePlaywrightScraper
from scrapers.real_site_scraper import RealSiteScraper


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def setup_logging() -> None:
    os.makedirs(os.path.join(os.path.dirname(__file__), "logs"), exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File
    log_path = os.path.join(os.path.dirname(__file__), "logs", "ticket_monitor.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


SCRAPER_REGISTRY: Dict[str, Type[BaseScraper]] = {
    "example_playwright": ExamplePlaywrightScraper,
    "real_site": RealSiteScraper,
}


def build_scraper(target: Target) -> BaseScraper:
    scraper_cls = SCRAPER_REGISTRY.get(target.scraper_type)
    if scraper_cls is None:
        raise ValueError(f"Unknown scraper_type={target.scraper_type!r} for target {target.label!r}")

    return scraper_cls(
        site_name=target.site_name,
        label=target.label,
        url=target.url,
        headless=config.HEADLESS,
        navigation_timeout_ms=config.TIMEOUT_MS,
        selector_timeout_ms=config.TIMEOUT_MS,
    )


def run_scrape_job(db_path: str) -> None:
    log = logging.getLogger("job")
    started = _utc_now()
    log.info(
        "Scrape job started. targets=%d headless=%s slow_mo_ms=%s use_storage_state=%s",
        len(config.TARGETS),
        config.HEADLESS,
        config.SLOW_MO_MS,
        config.USE_STORAGE_STATE,
    )

    conn = connect(db_path)
    try:
        init_db(conn)

        ok = 0
        failed = 0

        for target in config.TARGETS:
            tlog = logging.getLogger(f"target.{target.site_name}.{target.label}")
            t0 = time.time()
            try:
                scraper = build_scraper(target)
                snapshot = scraper.scrape()
                status = "ok"
                error_message = None

                blockers = []
                try:
                    blockers = list(snapshot.raw_payload.get("blockers") or [])
                except Exception:
                    blockers = []
                if "verification_page_detected" in blockers:
                    status = "blocked"
                    error_message = "verification_page_detected"
                elif "login_maybe_required" in blockers or "logged_in_indicator_missing" in blockers:
                    status = "auth_required"
                    error_message = "login_required_or_session_expired"

                # If no prices found, warn but still count as success (snapshot stored).
                if snapshot.min_price is None and snapshot.max_price is None and snapshot.avg_price is None:
                    tlog.warning(
                        "Saved snapshot but no prices parsed. listing_count=%s wanted_count=%s",
                        snapshot.listing_count,
                        snapshot.wanted_count,
                    )
                    if status == "ok":
                        status = "no_data"
                else:
                    tlog.info(
                        "Saved snapshot. min=%s max=%s avg=%s listings=%s wanted=%s",
                        snapshot.min_price,
                        snapshot.max_price,
                        snapshot.avg_price,
                        snapshot.listing_count,
                        snapshot.wanted_count,
                    )

                save_snapshot(conn, snapshot, status=status, error_message=error_message)
                ok += 1

            except Exception as e:
                failed += 1
                tlog.exception("Target scrape failed (continuing). url=%s", target.url)
                try:
                    from scrapers.base import MarketSnapshot

                    snap = MarketSnapshot(
                        site_name=target.site_name,
                        label=target.label,
                        url=target.url,
                        scraped_at=MarketSnapshot.now_utc(),
                        min_price=None,
                        max_price=None,
                        avg_price=None,
                        listing_count=None,
                        wanted_count=None,
                        raw_payload={"error": str(e)},
                    )
                    save_snapshot(conn, snap, status="error", error_message=str(e)[:500])
                except Exception:
                    tlog.exception("Failed to store error snapshot.")
            finally:
                tlog.info("Target done in %.2fs", time.time() - t0)

        elapsed = (_utc_now() - started).total_seconds()
        log.info("Scrape job finished. ok=%d failed=%d elapsed_s=%.2f", ok, failed, elapsed)

    finally:
        conn.close()


def main() -> int:
    setup_logging()
    log = logging.getLogger("main")

    cfg_err = config.validate_config()
    if cfg_err:
        log.error("Config invalid: %s", cfg_err)
        return 2

    log.info("DB path: %s", config.DATABASE_PATH)

    # Run once immediately.
    run_scrape_job(config.DATABASE_PATH)

    # Then schedule every N minutes.
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=config.SCRAPE_INTERVAL_MINUTES),
        args=[config.DATABASE_PATH],
        id="scrape_job",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started. interval_minutes=%d", config.SCRAPE_INTERVAL_MINUTES)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        scheduler.shutdown(wait=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

