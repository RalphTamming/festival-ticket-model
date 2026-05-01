from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import config
import db
from discovery import discover_urls as du
from discovery.step2_discover_ticket_urls import Step2Result, discover_ticket_urls_from_event_playwright
from scraping import scrape_market as sm


def _jitter(a: float = 0.4, b: float = 1.2) -> None:
    time.sleep(a + random.random() * max(0.0, b - a))


def _event_slug(event_url: str) -> str:
    n = du.normalize_url(event_url) or event_url
    path = (urlparse(n).path or "").strip("/")
    if path.startswith("festival-tickets/"):
        path = path[len("festival-tickets/") :]
    seg = path.split("/")[0] if path else ""
    return seg or "unknown"


def _slug_to_name(slug: str) -> str:
    s = re.sub(r"-\d{4}-\d{2}-\d{2}$", "", slug.strip("-"))
    s = re.sub(r"-\d{4}-\d{2}-\d{2}-.*$", "", s)
    return re.sub(r"\s+", " ", s.replace("-", " ")).strip().title() if s else slug


def _extract_event_date_local_from_slug(event_slug: str) -> Optional[str]:
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", event_slug)
    return m.group(1) if m else None


def _extract_location_from_slug(event_slug: str) -> Optional[str]:
    parts = event_slug.split("-")
    if len(parts) >= 4 and re.match(r"^\d{4}$", parts[-3]):
        return parts[-4].replace("-", " ").title()
    return None


def _send_telegram(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False
    body = json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8")
    req = Request(
        url=f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10):
            return True
    except (URLError, TimeoutError):
        return False


def _run_step1_events(listing_url: str, *, limit_events: int, headless: bool) -> list[str]:
    step1_cmd = [
        sys.executable,
        "-m",
        "discovery.step1_collect_listing_urls",
        "--url",
        listing_url,
        "--min-events",
        str(int(limit_events)),
        "--max-show-more",
        str(int(getattr(config, "DISCOVERY_OVERVIEW_MAX_SHOW_MORE", 50))),
    ]
    if headless:
        step1_cmd.append("--headless")
    p = subprocess.run(step1_cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if p.returncode != 0:
        raise RuntimeError(
            f"STEP1 failed for {listing_url} (code={p.returncode}): {(p.stderr or p.stdout or '')[-600:]}"
        )
    events: list[str] = []
    mode = ""
    for ln in (p.stdout or "").splitlines():
        if ln.strip() == "EVENT_URLS":
            mode = "events"
            continue
        if mode == "events" and ln.strip().startswith("https://www.ticketswap.com/festival-tickets/") and "/festival-tickets/a/" not in ln:
            events.append(ln.strip())
    return list(dict.fromkeys(events))[: int(limit_events)]


def _discover_with_retry(event_url: str, *, headed: bool, debug: bool) -> tuple[Step2Result, int, Optional[str]]:
    return _discover_with_retry_tuned(
        event_url,
        headed=headed,
        debug=debug,
        retries=1,
        blocked_sleep_min=2,
        blocked_sleep_max=5,
        page_timeout_ms=45_000,
        pre_network_wait_ms=1500,
        post_network_wait_ms=2500,
    )


def _discover_with_retry_tuned(
    event_url: str,
    *,
    headed: bool,
    debug: bool,
    retries: int,
    blocked_sleep_min: int,
    blocked_sleep_max: int,
    page_timeout_ms: int,
    pre_network_wait_ms: int,
    post_network_wait_ms: int,
) -> tuple[Step2Result, int, Optional[str]]:
    last: Optional[Step2Result] = None
    err_detail: Optional[str] = None
    total_attempts = max(1, int(retries) + 1)
    for attempt in range(total_attempts):
        if attempt:
            _jitter(float(blocked_sleep_min), float(max(blocked_sleep_min, blocked_sleep_max)))
        try:
            res = discover_ticket_urls_from_event_playwright(
                event_url,
                headed=bool(headed),
                debug=bool(debug),
                db_fallback=True,
                page_timeout_ms=int(page_timeout_ms),
                pre_network_wait_ms=int(pre_network_wait_ms),
                post_network_wait_ms=int(post_network_wait_ms),
            )
        except Exception as e:
            err_detail = f"{type(e).__name__}: {e}"
            if attempt < total_attempts - 1:
                continue
            ev = du.normalize_url(event_url) or event_url
            return Step2Result(ev, "error", False, "none", [], debug_dir=None), attempt + 1, err_detail
        last = res
        if res.status == "ok" and res.ticket_urls:
            return res, attempt + 1, None
        if res.status == "blocked" and attempt < total_attempts - 1:
            continue
        if res.status == "no_data" and attempt < total_attempts - 1 and headed:
            continue
        return res, attempt + 1, None
    assert last is not None
    return last, total_attempts, err_detail


def _scrape_with_retry(
    driver: Any,
    ticket_url: str,
    *,
    headless: bool,
    debug_dir: Path,
    manual_wait: int,
) -> tuple[sm.MarketSnapshot, int]:
    last: Optional[sm.MarketSnapshot] = None
    for attempt in range(2):
        if attempt:
            _jitter(1.5, 4.0)
            if manual_wait > 0:
                time.sleep(min(manual_wait, 45))
        snap = sm.scrape_market_url(
            ticket_url,
            headless=headless,
            debug_dir=debug_dir,
            manual_wait_seconds=int(manual_wait) if attempt == 0 else min(manual_wait, 30),
            driver=driver,
        )
        last = snap
        if snap.status == "ok":
            return snap, attempt + 1
        if snap.status == "blocked" and attempt == 0 and not headless:
            continue
        return snap, attempt + 1
    assert last is not None
    return last, 2


def _export_mode_csvs(conn: Any, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "events": output_dir / "events.csv",
        "ticket_types": output_dir / "ticket_types.csv",
        "latest_market_snapshots": output_dir / "latest_market_snapshots.csv",
        "market_snapshots_recent": output_dir / "market_snapshots_recent.csv",
    }
    events_rows = conn.execute(
        """
        SELECT event_id, event_url, event_slug, event_name, event_date_local, category,
               location, country, region, first_seen_at_utc, last_seen_at_utc, status
        FROM events
        ORDER BY COALESCE(event_date_local, '9999-12-31'), event_slug
        """
    ).fetchall()
    with paths["events"].open("w", newline="", encoding="utf-8") as f:
        cols = [
            "event_id",
            "event_url",
            "event_slug",
            "event_name",
            "event_date_local",
            "category",
            "location",
            "country",
            "region",
            "first_seen_at_utc",
            "last_seen_at_utc",
            "status",
        ]
        w = csv.writer(f)
        w.writerow(cols)
        for r in events_rows:
            w.writerow([r[c] for c in cols])

    ticket_rows = conn.execute(
        """
        SELECT ticket_type_id, ticket_url, event_id, event_url, ticket_type_label,
               ticket_type_slug, first_seen_at_utc, last_seen_at_utc, status
        FROM ticket_types
        ORDER BY ticket_type_id ASC
        """
    ).fetchall()
    with paths["ticket_types"].open("w", newline="", encoding="utf-8") as f:
        cols = [
            "ticket_type_id",
            "ticket_url",
            "event_id",
            "event_url",
            "ticket_type_label",
            "ticket_type_slug",
            "first_seen_at_utc",
            "last_seen_at_utc",
            "status",
        ]
        w = csv.writer(f)
        w.writerow(cols)
        for r in ticket_rows:
            w.writerow([r[c] for c in cols])

    latest_rows = conn.execute(
        """
        WITH ranked AS (
          SELECT ms.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY ms.ticket_type_id
                   ORDER BY ms.scraped_at_utc DESC, ms.snapshot_id DESC
                 ) AS rn
          FROM market_snapshots ms
          WHERE ms.ticket_type_id IS NOT NULL
        )
        SELECT snapshot_id, ticket_type_id, ticket_url, scraped_at_utc, status, currency,
               listing_count, wanted_count, lowest_ask, highest_ask, median_ask, average_ask,
               error_message, run_id
        FROM ranked
        WHERE rn=1
        ORDER BY scraped_at_utc DESC
        """
    ).fetchall()
    with paths["latest_market_snapshots"].open("w", newline="", encoding="utf-8") as f:
        cols = [
            "snapshot_id",
            "ticket_type_id",
            "ticket_url",
            "scraped_at_utc",
            "status",
            "currency",
            "listing_count",
            "wanted_count",
            "lowest_ask",
            "highest_ask",
            "median_ask",
            "average_ask",
            "error_message",
            "run_id",
        ]
        w = csv.writer(f)
        w.writerow(cols)
        for r in latest_rows:
            w.writerow([r[c] for c in cols])

    recent_rows = conn.execute(
        """
        SELECT snapshot_id, ticket_type_id, ticket_url, scraped_at_utc, status, currency,
               listing_count, wanted_count, lowest_ask, highest_ask, median_ask, average_ask,
               error_message, run_id
        FROM market_snapshots
        WHERE scraped_at_utc >= datetime('now', '-7 days')
        ORDER BY scraped_at_utc DESC
        """
    ).fetchall()
    with paths["market_snapshots_recent"].open("w", newline="", encoding="utf-8") as f:
        cols = [
            "snapshot_id",
            "ticket_type_id",
            "ticket_url",
            "scraped_at_utc",
            "status",
            "currency",
            "listing_count",
            "wanted_count",
            "lowest_ask",
            "highest_ask",
            "median_ask",
            "average_ask",
            "error_message",
            "run_id",
        ]
        w = csv.writer(f)
        w.writerow(cols)
        for r in recent_rows:
            w.writerow([r[c] for c in cols])
    return paths


def run_discovery_mode(args: Any) -> int:
    logging.info("Starting discovery mode")
    scope_name = str(args.scope)
    listing_urls = [str(args.listing_url)] if args.listing_url else list(config.SCOPES.get(scope_name, {}).get("listing_urls", []))
    if not listing_urls:
        raise SystemExit(f"No listing URLs configured for scope '{scope_name}'.")
    headless = not bool(args.headed)
    conn = db.connect(config.DB_PATH)
    db.init_db(conn)
    run_id = db.create_pipeline_run(conn, mode="discovery", scope=scope_name)
    blocked_count = 0
    blocked_consecutive = 0
    stopped_early_blocked = False
    stop_threshold = int(getattr(args, "step2_blocked_stop_threshold", 3))
    retries = int(getattr(args, "step2_retries", 1))
    blocked_sleep_min = int(getattr(args, "step2_blocked_sleep_min", 30))
    blocked_sleep_max = int(getattr(args, "step2_blocked_sleep_max", 90))
    safe_mode = bool(getattr(args, "vps_safe_mode", False))
    page_timeout_ms = 75_000 if safe_mode else 45_000
    pre_network_wait_ms = 3000 if safe_mode else 1500
    post_network_wait_ms = 5000 if safe_mode else 2500
    inter_event_min = 2.0 if safe_mode else 0.2
    inter_event_max = 6.0 if safe_mode else 1.0
    counts: dict[str, int] = {
        "listing_urls": len(listing_urls),
        "events_collected": 0,
        "events_upserted": 0,
        "ticket_types_seen": 0,
        "ticket_types_upserted": 0,
        "step2_blocked": 0,
        "step2_errors": 0,
        "step2_db_fallback_due_to_block": 0,
        "stopped_early_blocked": 0,
    }
    try:
        for listing_url in listing_urls:
            events = _run_step1_events(listing_url, limit_events=args.limit_events, headless=headless)
            counts["events_collected"] += len(events)
            for ev in events:
                slug = _event_slug(ev)
                _jitter(inter_event_min, inter_event_max)
                step2, _, _ = _discover_with_retry_tuned(
                    ev,
                    headed=bool(args.headed),
                    debug=bool(args.debug),
                    retries=retries,
                    blocked_sleep_min=blocked_sleep_min,
                    blocked_sleep_max=blocked_sleep_max,
                    page_timeout_ms=page_timeout_ms,
                    pre_network_wait_ms=pre_network_wait_ms,
                    post_network_wait_ms=post_network_wait_ms,
                )
                if step2.status == "blocked":
                    blocked_count += 1
                    counts["step2_blocked"] += 1
                    blocked_consecutive += 1
                elif step2.status == "error":
                    counts["step2_errors"] += 1
                    blocked_consecutive = 0
                else:
                    blocked_consecutive = 0
                event_id = db.upsert_event_record(
                    conn,
                    event_url=ev,
                    event_slug=slug,
                    event_name=_slug_to_name(slug),
                    event_date_local=_extract_event_date_local_from_slug(slug),
                    category=config.SCOPES.get(scope_name, {}).get("category", "festival"),
                    location=_extract_location_from_slug(slug) or "Amsterdam",
                    country="Netherlands",
                    region="Western Europe",
                    status="active",
                )
                counts["events_upserted"] += 1
                ticket_urls_for_event = list(step2.ticket_urls)
                strategy = step2.strategy
                if step2.status == "blocked" and not ticket_urls_for_event:
                    known = db.list_ticket_urls_for_event(conn, event_url=ev)
                    if known:
                        ticket_urls_for_event = known
                        strategy = "db_fallback_due_to_block"
                        counts["step2_db_fallback_due_to_block"] += 1
                        logging.warning("STEP2 blocked for %s; reusing %d DB ticket URLs", ev, len(ticket_urls_for_event))

                for tu in ticket_urls_for_event:
                    t_slug, t_label = du.ticket_type_from_ticket_url(tu)
                    db.upsert_ticket_type_record(
                        conn,
                        ticket_url=tu,
                        event_id=event_id,
                        event_url=ev,
                        ticket_type_slug=t_slug,
                        ticket_type_label=t_label,
                        status="active",
                    )
                    counts["ticket_types_seen"] += 1
                    counts["ticket_types_upserted"] += 1
                if blocked_consecutive > stop_threshold:
                    stopped_early_blocked = True
                    counts["stopped_early_blocked"] = 1
                    logging.warning(
                        "Stopping discovery early due to consecutive verification blocks (%d > %d)",
                        blocked_consecutive,
                        stop_threshold,
                    )
                    break
            if stopped_early_blocked:
                break

        paths = _export_mode_csvs(conn, Path("data/outputs"))
        final_status = "ok"
        if stopped_early_blocked or (counts["step2_blocked"] > 0 and counts["ticket_types_upserted"] == 0):
            final_status = "verification_blocked_partial"
        db.finish_pipeline_run(conn, run_id=run_id, status=final_status, counts=counts, error_summary=None)
        _send_telegram(
            f"TicketSwap discovery finished ({scope_name}) events={counts['events_upserted']} "
            f"ticket_types={counts['ticket_types_upserted']} blocked={counts['step2_blocked']} status={final_status}"
        )
        if blocked_count >= 5:
            _send_telegram(f"TicketSwap warning: verification_blocked occurred {blocked_count} times in discovery run.")
        if stopped_early_blocked:
            _send_telegram(
                f"TicketSwap discovery stopped early on VPS due to consecutive step2 blocks ({blocked_consecutive})."
            )
        elif final_status == "verification_blocked_partial":
            _send_telegram(
                "TicketSwap discovery ended with verification_blocked_partial (no new ticket types discovered)."
            )
        print(json.dumps({"run_id": run_id, "mode": "discovery", "status": final_status, "counts": counts}, indent=2))
        print(f"CSV exports: {', '.join(str(p) for p in paths.values())}")
        return 3 if stopped_early_blocked else 0
    except Exception as exc:
        db.finish_pipeline_run(conn, run_id=run_id, status="failed", counts=counts, error_summary=str(exc))
        _send_telegram(f"TicketSwap discovery failed ({scope_name}): {exc}")
        logging.exception("Discovery mode failed")
        return 2
    finally:
        conn.close()


def _parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _monitoring_interval_hours(days_until_event: int) -> float:
    if days_until_event > 30:
        return 72.0
    if 7 <= days_until_event <= 30:
        return 24.0
    if days_until_event == 6:
        return 6.0
    if days_until_event == 5:
        return 24.0 / 5.0
    if days_until_event == 4:
        return 4.0
    return 1.0


def _ticket_due_now(
    *,
    event_date_local: Optional[str],
    last_scraped_utc: Optional[str],
    now_local: datetime,
    tz: ZoneInfo,
    monitor_after_event: bool,
    monitor_start_hour: int,
    monitor_end_hour: int,
) -> tuple[bool, str]:
    # inclusive bounds: MONITOR_START_HOUR..MONITOR_END_HOUR
    if now_local.hour < monitor_start_hour or now_local.hour > monitor_end_hour:
        return False, "outside_window"
    if not event_date_local:
        return True, "missing_event_date"
    try:
        event_dt_local = datetime.fromisoformat(f"{event_date_local}T23:59:59").replace(tzinfo=tz)
    except ValueError:
        return True, "bad_event_date"
    days_until_event = (event_dt_local.date() - now_local.date()).days
    if days_until_event < 0 and not monitor_after_event:
        return False, "past_event"
    interval = _monitoring_interval_hours(days_until_event)
    if not last_scraped_utc:
        return True, "never_scraped"
    last_dt_utc = _parse_iso_dt(last_scraped_utc)
    if last_dt_utc is None:
        return True, "bad_last_scraped"
    last_local = last_dt_utc.astimezone(tz)
    due = (now_local - last_local) >= timedelta(hours=interval)
    return due, f"interval_{interval:.2f}h"


def run_monitoring_mode(args: Any) -> int:
    logging.info("Starting monitoring mode")
    headless = not bool(args.headed)
    conn = db.connect(config.DB_PATH)
    db.init_db(conn)
    run_id = db.create_pipeline_run(conn, mode="monitoring", scope=None)
    tz = ZoneInfo(getattr(config, "LOCAL_TIMEZONE", "Europe/Amsterdam"))
    now_local = datetime.now(tz)
    counts: dict[str, int] = {
        "ticket_types_loaded": 0,
        "due_ticket_types": 0,
        "scraped": 0,
        "scrape_ok": 0,
        "scrape_blocked": 0,
        "scrape_no_data": 0,
        "scrape_error": 0,
        "skipped_not_due": 0,
        "skipped_past_event": 0,
        "outside_window": 0,
    }
    blocked_count = 0
    monitor_after_event = bool(args.monitor_after_event) or bool(getattr(config, "MONITOR_AFTER_EVENT", False))
    monitor_start_hour = int(getattr(config, "MONITOR_START_HOUR", 8))
    monitor_end_hour = int(getattr(config, "MONITOR_END_HOUR", 23))
    try:
        rows = db.list_ticket_types_for_monitoring(conn, limit=None)
        counts["ticket_types_loaded"] = len(rows)
        due_rows: list[Any] = []
        for r in rows:
            last_row = db.latest_snapshot_for_ticket_type(conn, int(r["ticket_type_id"]))
            last_scraped = str(last_row["scraped_at_utc"]) if last_row else None
            due, reason = _ticket_due_now(
                event_date_local=r["event_date_local"],
                last_scraped_utc=last_scraped,
                now_local=now_local,
                tz=tz,
                monitor_after_event=monitor_after_event,
                monitor_start_hour=monitor_start_hour,
                monitor_end_hour=monitor_end_hour,
            )
            if due:
                due_rows.append(r)
            elif reason == "past_event":
                counts["skipped_past_event"] += 1
            elif reason == "outside_window":
                counts["outside_window"] += 1
            else:
                counts["skipped_not_due"] += 1
        if int(args.limit_tickets) > 0:
            due_rows = due_rows[: int(args.limit_tickets)]
        counts["due_ticket_types"] = len(due_rows)

        if due_rows:
            with sm.market_scrape_session(headless=headless) as m_driver:
                for r in due_rows:
                    snap, _ = _scrape_with_retry(
                        m_driver,
                        str(r["ticket_url"]),
                        headless=headless,
                        debug_dir=Path(config.DEBUG_DIR),
                        manual_wait=int(getattr(config, "MANUAL_VERIFY_WAIT_SECONDS", 90)) if args.headed else 0,
                    )
                    db.insert_market_snapshot_for_ticket_type(
                        conn,
                        ticket_type_id=int(r["ticket_type_id"]),
                        run_id=run_id,
                        snap=snap,
                    )
                    counts["scraped"] += 1
                    if snap.status == "ok":
                        counts["scrape_ok"] += 1
                    elif snap.status == "blocked":
                        blocked_count += 1
                        counts["scrape_blocked"] += 1
                    elif snap.status == "no_data":
                        counts["scrape_no_data"] += 1
                    else:
                        counts["scrape_error"] += 1

        paths = _export_mode_csvs(conn, Path("data/outputs"))
        db.finish_pipeline_run(conn, run_id=run_id, status="ok", counts=counts, error_summary=None)
        _send_telegram(
            f"TicketSwap monitoring finished due={counts['due_ticket_types']} "
            f"ok={counts['scrape_ok']} blocked={counts['scrape_blocked']}"
        )
        if blocked_count >= 5:
            _send_telegram(f"TicketSwap warning: verification_blocked occurred {blocked_count} times in monitoring run.")
        print(json.dumps({"run_id": run_id, "mode": "monitoring", "status": "ok", "counts": counts}, indent=2))
        print(f"CSV exports: {', '.join(str(p) for p in paths.values())}")
        return 0
    except Exception as exc:
        db.finish_pipeline_run(conn, run_id=run_id, status="failed", counts=counts, error_summary=str(exc))
        _send_telegram(f"TicketSwap monitoring failed: {exc}")
        logging.exception("Monitoring mode failed")
        return 2
    finally:
        conn.close()
