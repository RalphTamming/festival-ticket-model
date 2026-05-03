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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import config
import db
from discovery import discover_urls as du
from discovery.step2_discover_ticket_urls import (
    Step2Result,
    discover_ticket_urls_from_event_playwright,
    discover_ticket_urls_from_event_selenium,
)
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


def _send_telegram_document(file_path: Path, *, caption: Optional[str] = None) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id or not file_path.exists():
        return False
    boundary = f"----TicketSwapBoundary{int(time.time())}"
    data = file_path.read_bytes()
    parts = [
        f"--{boundary}\r\n".encode("utf-8"),
        b'Content-Disposition: form-data; name="chat_id"\r\n\r\n',
        f"{chat_id}\r\n".encode("utf-8"),
    ]
    if caption:
        parts.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                b'Content-Disposition: form-data; name="caption"\r\n\r\n',
                f"{caption}\r\n".encode("utf-8"),
            ]
        )
    parts.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            f'Content-Disposition: form-data; name="document"; filename="{file_path.name}"\r\n'.encode("utf-8"),
            b"Content-Type: text/csv\r\n\r\n",
            data,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    body = b"".join(parts)
    req = Request(
        url=f"https://api.telegram.org/bot{token}/sendDocument",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=20):
            return True
    except (URLError, TimeoutError):
        return False


def _send_error_alert(
    *,
    error_type: str,
    event_url: Optional[str] = None,
    debug_path: Optional[str] = None,
    details: Optional[str] = None,
) -> None:
    if not os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or not os.getenv("TELEGRAM_CHAT_ID", "").strip():
        return
    parts = [f"TicketSwap ERROR: {error_type}"]
    if event_url:
        parts.append(f"event: {event_url}")
    if debug_path:
        parts.append(f"debug: {debug_path}")
    if details:
        parts.append(f"details: {details}")
    _send_telegram("\n".join(parts))


def _parse_daily_report_hour_minute(value: str) -> tuple[int, int]:
    raw = (value or "21:00").strip()
    try:
        hh_s, mm_s = raw.split(":", 1)
        hh = max(0, min(23, int(hh_s)))
        mm = max(0, min(59, int(mm_s)))
        return hh, mm
    except Exception:
        return 21, 0


def _should_send_daily_report(now_local: datetime, conn: Any) -> bool:
    if bool(getattr(config, "TELEGRAM_ERROR_ONLY_MODE", False)):
        return False
    hh, mm = _parse_daily_report_hour_minute(str(getattr(config, "DAILY_REPORT_TIME", "21:00")))
    due_now = (now_local.hour > hh) or (now_local.hour == hh and now_local.minute >= mm)
    if not due_now:
        return False
    key = "daily_report_last_sent_local_date"
    sent = db.kv_get(conn, key)
    today = now_local.date().isoformat()
    return sent != today


def _send_daily_report(conn: Any, now_local: datetime) -> None:
    day_start_local = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=now_local.tzinfo)
    day_start_utc = day_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    day_start_utc_s = day_start_utc.isoformat(timespec="seconds") + "Z"
    runs = conn.execute(
        """
        SELECT mode, status, counts_json, error_summary
        FROM pipeline_runs
        WHERE started_at_utc >= ?
        """,
        (day_start_utc_s,),
    ).fetchall()
    discovery_runs = sum(1 for r in runs if str(r["mode"]) == "discovery")
    monitoring_runs = sum(1 for r in runs if str(r["mode"]) == "monitoring")
    errors = 0
    blocks = 0
    for r in runs:
        if r["status"] == "failed" or (r["error_summary"] or "").strip():
            errors += 1
        try:
            c = json.loads(r["counts_json"] or "{}")
        except Exception:
            c = {}
        blocks += int(c.get("step2_blocked", 0) or 0) + int(c.get("scrape_blocked", 0) or 0)
        errors += int(c.get("scrape_error", 0) or 0) + int(c.get("step2_errors", 0) or 0)
    snapshots_collected = int(
        conn.execute("SELECT COUNT(*) AS c FROM market_snapshots WHERE scraped_at_utc >= ?", (day_start_utc_s,)).fetchone()["c"]
    )
    events_tracked = int(conn.execute("SELECT COUNT(*) AS c FROM events WHERE COALESCE(status, 'active')='active'").fetchone()["c"])
    ticket_types_tracked = int(conn.execute("SELECT COUNT(*) AS c FROM ticket_types WHERE status='active'").fetchone()["c"])
    status_summary = "healthy" if errors == 0 and blocks == 0 else "attention_needed"
    _send_telegram(
        "\n".join(
            [
                f"TicketSwap daily report ({now_local.date().isoformat()})",
                f"monitoring_runs: {monitoring_runs}",
                f"discovery_runs: {discovery_runs}",
                f"snapshots_collected: {snapshots_collected}",
                f"events_tracked: {events_tracked}",
                f"ticket_types_tracked: {ticket_types_tracked}",
                f"errors: {errors}",
                f"blocks: {blocks}",
                f"status: {status_summary}",
            ]
        )
    )
    db.kv_set(conn, "daily_report_last_sent_local_date", now_local.date().isoformat())


def _maybe_export_weekly_report(conn: Any, now_local: datetime) -> Optional[Path]:
    if not bool(getattr(config, "ENABLE_WEEKLY_EXPORT", True)):
        return None
    if bool(getattr(config, "TELEGRAM_ERROR_ONLY_MODE", False)):
        return None
    iso_year, iso_week, _ = now_local.isocalendar()
    week_key = f"{iso_year}-W{iso_week:02d}"
    if db.kv_get(conn, "weekly_report_last_iso_week") == week_key:
        return None
    if now_local.weekday() != 0:  # Monday
        return None
    exports_dir = Path("data/exports")
    exports_dir.mkdir(parents=True, exist_ok=True)
    out = exports_dir / f"weekly_report_{now_local.date().isoformat()}.csv"
    rows = conn.execute(
        """
        SELECT
          COALESCE(e.event_name, e.event_slug, ms.event_url) AS event,
          COALESCE(tt.ticket_type_label, ms.ticket_type_label, tt.ticket_type_slug) AS ticket_type,
          ms.lowest_ask,
          ms.highest_ask,
          ms.median_ask,
          ms.average_ask,
          ms.listing_count,
          ms.wanted_count,
          ms.scraped_at_utc
        FROM market_snapshots ms
        LEFT JOIN ticket_types tt ON tt.ticket_type_id = ms.ticket_type_id
        LEFT JOIN events e ON e.event_id = tt.event_id
        WHERE ms.scraped_at_utc >= datetime('now', '-7 days')
        ORDER BY ms.scraped_at_utc DESC
        """
    ).fetchall()
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "event",
                "ticket_type",
                "lowest_ask",
                "highest_ask",
                "median_ask",
                "average_ask",
                "listing_count",
                "wanted_count",
                "scraped_at_utc",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r["event"],
                    r["ticket_type"],
                    r["lowest_ask"],
                    r["highest_ask"],
                    r["median_ask"],
                    r["average_ask"],
                    r["listing_count"],
                    r["wanted_count"],
                    r["scraped_at_utc"],
                ]
            )
    db.kv_set(conn, "weekly_report_last_iso_week", week_key)
    if out.stat().st_size <= (45 * 1024 * 1024):
        _send_telegram_document(out, caption=f"TicketSwap weekly report {now_local.date().isoformat()}")
    return out


def _maybe_send_daily_outputs(conn: Any, now_local: datetime) -> None:
    if _should_send_daily_report(now_local, conn):
        _send_daily_report(conn, now_local)
        _maybe_export_weekly_report(conn, now_local)


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


def _discover_live_with_retry(
    event_url: str,
    *,
    headed: bool,
    debug: bool,
    browser: str,
    verification_wait_seconds: int,
    wait_for_manual_verification: bool,
    manual_verification_timeout: int,
    retries: int,
    blocked_sleep_min: int,
    blocked_sleep_max: int,
    page_timeout_ms: int,
    pre_network_wait_ms: int,
    post_network_wait_ms: int,
) -> tuple[Step2Result, int, Optional[str], bool]:
    last: Optional[Step2Result] = None
    err_detail: Optional[str] = None
    total_attempts = max(1, int(retries) + 1)
    verification_detected = False
    for attempt in range(total_attempts):
        if attempt:
            _jitter(float(blocked_sleep_min), float(max(blocked_sleep_min, blocked_sleep_max)))
        try:
            order: list[str]
            b = str(browser or "auto")
            if b == "selenium":
                order = ["selenium", "playwright"]
            elif b == "playwright":
                order = ["playwright", "selenium"]
            else:
                order = ["selenium", "playwright"]

            res = Step2Result(du.normalize_url(event_url) or event_url, "no_data", False, "none", [], debug_dir=None)
            for strategy in order:
                if strategy == "selenium":
                    cand = discover_ticket_urls_from_event_selenium(
                        event_url,
                        headed=bool(headed),
                        debug=bool(debug),
                        verification_wait_seconds=int(verification_wait_seconds),
                        debug_root="step2_vps_live",
                        wait_for_manual_verification=bool(wait_for_manual_verification),
                        manual_verification_timeout=int(manual_verification_timeout),
                    )
                else:
                    cand = discover_ticket_urls_from_event_playwright(
                        event_url,
                        headed=bool(headed),
                        debug=bool(debug),
                        db_fallback=False,
                        page_timeout_ms=int(page_timeout_ms),
                        pre_network_wait_ms=int(pre_network_wait_ms),
                        post_network_wait_ms=int(post_network_wait_ms),
                        debug_root="step2_vps_live",
                        wait_for_manual_verification=bool(wait_for_manual_verification),
                        manual_verification_timeout=int(manual_verification_timeout),
                    )
                if cand.verification:
                    verification_detected = True
                if cand.status == "ok" and cand.ticket_urls:
                    res = cand
                    break
                # keep the most informative failure as last
                res = cand
                if cand.status == "blocked":
                    # Give strategy a chance to recover with manual wait inside it; try next strategy now.
                    continue
        except Exception as e:
            err_detail = f"{type(e).__name__}: {e}"
            if attempt < total_attempts - 1:
                continue
            ev = du.normalize_url(event_url) or event_url
            return Step2Result(ev, "error", False, "none", [], debug_dir=None), attempt + 1, err_detail, verification_detected
        last = res
        if res.status == "ok" and res.ticket_urls:
            return res, attempt + 1, None, verification_detected
        if res.status == "blocked" and attempt < total_attempts - 1:
            continue
        if res.status == "no_data" and attempt < total_attempts - 1 and headed:
            continue
        return res, attempt + 1, None, verification_detected
    assert last is not None
    return last, total_attempts, err_detail, verification_detected


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
               error_message, run_id, days_until_event, hours_until_event, event_weekday, event_month,
               total_available_quantity, is_sold_out
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
            "days_until_event",
            "hours_until_event",
            "event_weekday",
            "event_month",
            "total_available_quantity",
            "is_sold_out",
        ]
        w = csv.writer(f)
        w.writerow(cols)
        for r in latest_rows:
            w.writerow([r[c] for c in cols])

    recent_rows = conn.execute(
        """
        SELECT snapshot_id, ticket_type_id, ticket_url, scraped_at_utc, status, currency,
               listing_count, wanted_count, lowest_ask, highest_ask, median_ask, average_ask,
               error_message, run_id, days_until_event, hours_until_event, event_weekday, event_month,
               total_available_quantity, is_sold_out
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
            "days_until_event",
            "hours_until_event",
            "event_weekday",
            "event_month",
            "total_available_quantity",
            "is_sold_out",
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
    verification_wait = int(getattr(args, "step2_verification_wait", 60))
    wait_for_manual_verification = bool(getattr(args, "wait_for_manual_verification", False))
    require_fresh_step2 = bool(getattr(args, "require_fresh_step2", False))
    browser_pref = str(getattr(args, "step2_browser", "auto"))
    if safe_mode and browser_pref == "auto":
        browser_pref = "selenium"
    page_timeout_ms = 75_000 if safe_mode else 45_000
    pre_network_wait_ms = 3000 if safe_mode else 1500
    post_network_wait_ms = 5000 if safe_mode else 2500
    inter_event_min = 2.0 if safe_mode else 0.2
    inter_event_max = 6.0 if safe_mode else 1.0
    counts: dict[str, int] = {
        "listing_urls": len(listing_urls),
        "events_collected": 0,
        "events_upserted": 0,
        "step2_fresh_ok": 0,
        "step2_blocked": 0,
        "step2_db_fallback_used": 0,
        "step2_errors": 0,
        "ticket_types_fresh_upserted": 0,
        "ticket_types_fallback_upserted": 0,
        "stopped_early_blocked": 0,
    }
    try:
        for listing_url in listing_urls:
            events = _run_step1_events(listing_url, limit_events=args.limit_events, headless=headless)
            counts["events_collected"] += len(events)
            for ev in events:
                slug = _event_slug(ev)
                _jitter(inter_event_min, inter_event_max)
                step2, _, _, _verification_seen = _discover_live_with_retry(
                    ev,
                    headed=bool(args.headed),
                    debug=bool(args.debug),
                    browser=browser_pref,
                    verification_wait_seconds=verification_wait,
                    wait_for_manual_verification=wait_for_manual_verification,
                    manual_verification_timeout=300,
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
                    if require_fresh_step2:
                        _send_error_alert(
                            error_type="step2_fresh_failure",
                            event_url=ev,
                            debug_path=step2.debug_dir,
                            details="verification_blocked during require-fresh discovery",
                        )
                elif step2.status == "ok" and step2.ticket_urls:
                    counts["step2_fresh_ok"] += 1
                    blocked_consecutive = 0
                elif step2.status == "error":
                    counts["step2_errors"] += 1
                    blocked_consecutive = 0
                    _send_error_alert(
                        error_type="step2_error",
                        event_url=ev,
                        debug_path=step2.debug_dir,
                        details=step2.status,
                    )
                else:
                    blocked_consecutive = 0
                    if require_fresh_step2 and not step2.ticket_urls:
                        _send_error_alert(
                            error_type="step2_fresh_failure",
                            event_url=ev,
                            debug_path=step2.debug_dir,
                            details=f"status={step2.status}",
                        )
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
                is_fallback = False
                if (not require_fresh_step2) and step2.status == "blocked" and not ticket_urls_for_event:
                    known = db.list_ticket_urls_for_event(conn, event_url=ev)
                    if known:
                        ticket_urls_for_event = known
                        is_fallback = True
                        counts["step2_db_fallback_used"] += 1
                        logging.warning("STEP2 blocked for %s; reusing %d DB ticket URLs", ev, len(ticket_urls_for_event))
                elif (not require_fresh_step2) and step2.status in ("no_data", "error") and not ticket_urls_for_event:
                    known = db.list_ticket_urls_for_event(conn, event_url=ev)
                    if known:
                        ticket_urls_for_event = known
                        is_fallback = True
                        counts["step2_db_fallback_used"] += 1
                        logging.warning(
                            "STEP2 %s for %s; reusing %d DB ticket URLs",
                            step2.status,
                            ev,
                            len(ticket_urls_for_event),
                        )

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
                    if is_fallback:
                        counts["ticket_types_fallback_upserted"] += 1
                    else:
                        counts["ticket_types_fresh_upserted"] += 1
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
        if require_fresh_step2 and (counts["ticket_types_fresh_upserted"] == 0):
            final_status = "verification_blocked"
        elif stopped_early_blocked or (counts["step2_blocked"] > 0 and counts["ticket_types_fresh_upserted"] == 0):
            final_status = "verification_blocked_partial"
        db.finish_pipeline_run(conn, run_id=run_id, status=final_status, counts=counts, error_summary=None)
        if blocked_count >= 5:
            _send_error_alert(
                error_type="verification_blocked_threshold_exceeded",
                details=f"blocked_count={blocked_count} scope={scope_name}",
            )
        if require_fresh_step2 and final_status == "verification_blocked":
            _send_error_alert(
                error_type="step2_fresh_failure",
                details="require-fresh discovery ended with zero fresh ticket URLs",
            )
        if stopped_early_blocked:
            _send_error_alert(
                error_type="verification_blocked_threshold_exceeded",
                details=f"stopped_early consecutive_blocked={blocked_consecutive}",
            )
        if counts["ticket_types_fresh_upserted"] == 0:
            logging.warning("0 fresh ticket URLs discovered in this discovery run.")
        _maybe_send_daily_outputs(conn, datetime.now(ZoneInfo(getattr(config, "LOCAL_TIMEZONE", "Europe/Amsterdam"))))
        print(json.dumps({"run_id": run_id, "mode": "discovery", "status": final_status, "counts": counts}, indent=2))
        print(f"CSV exports: {', '.join(str(p) for p in paths.values())}")
        return 3 if stopped_early_blocked else 0
    except Exception as exc:
        db.finish_pipeline_run(conn, run_id=run_id, status="failed", counts=counts, error_summary=str(exc))
        _send_error_alert(error_type="pipeline_crash", details=f"discovery scope={scope_name} error={exc}")
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
        return 24.0
    if 8 <= days_until_event <= 30:
        return 12.0
    if days_until_event == 7:
        return 8.0
    if days_until_event == 6:
        return 6.0
    if days_until_event == 5:
        return 24.0 / 5.0
    if days_until_event == 4:
        return 4.0
    if days_until_event == 3:
        return 3.0
    if days_until_event in (1, 2):
        return 24.0 / 10.0
    if days_until_event == 0:
        return 1.0
    return 0.0


def _event_start_hour_local(event_date_local: Optional[str], event_start_utc: Optional[str], tz: ZoneInfo) -> Optional[int]:
    dt_utc = _parse_iso_dt(event_start_utc)
    if dt_utc is not None:
        return int(dt_utc.astimezone(tz).hour)
    if event_date_local:
        # If exact start is unknown, keep monitoring window open.
        return int(getattr(config, "MONITOR_END_HOUR", 23))
    return None


def _ticket_due_now(
    *,
    event_date_local: Optional[str],
    event_start_utc: Optional[str],
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
        event_date = date.fromisoformat(event_date_local)
    except ValueError:
        return True, "bad_event_date"

    days_until_event = (event_date - now_local.date()).days
    if days_until_event < 0 and not monitor_after_event:
        return False, "past_event"

    if days_until_event == 0:
        event_start_hour = _event_start_hour_local(event_date_local, event_start_utc, tz)
        if event_start_hour is not None and now_local.hour > event_start_hour and not monitor_after_event:
            return False, "past_event"

    if days_until_event < 0:
        return True, "monitor_after_event"

    interval = _monitoring_interval_hours(days_until_event)
    if not last_scraped_utc:
        return True, "never_scraped"
    last_dt_utc = _parse_iso_dt(last_scraped_utc)
    if last_dt_utc is None:
        return True, "bad_last_scraped"
    last_local = last_dt_utc.astimezone(tz)
    due = (now_local - last_local) >= timedelta(hours=interval)
    return due, f"interval_{interval:.2f}h"


def _total_available_quantity(snap: sm.MarketSnapshot) -> int:
    qty = 0
    for item in (snap.listings or []):
        q = getattr(item, "quantity", None)
        if isinstance(q, int):
            qty += q
    return qty


def _ensure_snapshot_listings_payload(snap: sm.MarketSnapshot) -> None:
    normalized: list[dict[str, Any]] = []
    for item in (snap.listings or []):
        href = getattr(item, "listing_href", None)
        listing_id = None
        if href:
            tail = str(href).rstrip("/").split("/")[-1]
            listing_id = "".join(ch for ch in tail if ch.isdigit()) or None
        normalized.append(
            {
                "price": getattr(item, "price_per_ticket", None),
                "quantity": getattr(item, "quantity", None),
                "listing_url": href,
                "listing_id": listing_id,
            }
        )
    raw = dict(getattr(snap, "raw_debug", {}) or {})
    raw["normalized_listings"] = normalized
    raw["total_available_quantity"] = _total_available_quantity(snap)
    raw["is_sold_out"] = bool(int(getattr(snap, "listing_count", 0) or 0) == 0)
    object.__setattr__(snap, "raw_debug", raw)


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
                event_start_utc=r["start_datetime_utc"],
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
                    _ensure_snapshot_listings_payload(snap)
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
                        _send_error_alert(
                            error_type="scrape_error",
                            event_url=str(r["event_url"]),
                            debug_path=str(Path(config.DEBUG_DIR)),
                            details=snap.error_message,
                        )

        paths = _export_mode_csvs(conn, Path("data/outputs"))
        db.finish_pipeline_run(conn, run_id=run_id, status="ok", counts=counts, error_summary=None)
        if blocked_count >= 5:
            _send_error_alert(
                error_type="verification_blocked_threshold_exceeded",
                details=f"monitoring blocked_count={blocked_count}",
            )
        _maybe_send_daily_outputs(conn, now_local)
        print(json.dumps({"run_id": run_id, "mode": "monitoring", "status": "ok", "counts": counts}, indent=2))
        print(f"CSV exports: {', '.join(str(p) for p in paths.values())}")
        return 0
    except Exception as exc:
        db.finish_pipeline_run(conn, run_id=run_id, status="failed", counts=counts, error_summary=str(exc))
        _send_error_alert(error_type="pipeline_crash", details=f"monitoring error={exc}")
        logging.exception("Monitoring mode failed")
        return 2
    finally:
        conn.close()
