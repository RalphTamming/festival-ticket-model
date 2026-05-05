from __future__ import annotations

import contextlib
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
from discovery import vps_chrome_bootstrap as vcb
from discovery.step2_discover_ticket_urls import (
    Step2Result,
    classify_loaded_selenium_page,
    discover_ticket_urls_from_event_selenium_embedded_only,
    discover_ticket_urls_from_event_playwright,
    discover_ticket_urls_from_event_selenium,
    extract_ticket_urls_from_loaded_selenium_page,
)
from scraping import scrape_market as sm


def _jitter(a: float = 0.4, b: float = 1.2) -> None:
    time.sleep(a + random.random() * max(0.0, b - a))


MAX_VPS18_DRIVER_RECOVERY_RESTARTS = 3
RESTART_SHARED_DRIVER_EVERY_N_HUBS = 2


def _is_selenium_driver_dead(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(
        needle in msg
        for needle in (
            "connection refused",
            "max retries exceeded with url",
            "invalid session id",
            "disconnected",
            "chrome not reachable",
            "no such window",
            "target window already closed",
            "session deleted",
        )
    )


def _driver_healthcheck(driver: Any) -> bool:
    if driver is None:
        return False
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def _safe_driver_quit(driver: Any) -> None:
    if driver is None:
        return
    with contextlib.suppress(Exception):
        driver.quit()


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
    # STEP1 emits two named blocks: HUB_URLS (festival hub pages) and
    # EVENT_URLS (direct event pages). On the live TicketSwap listing the
    # tiles overwhelmingly link to hubs (verified manually 2026-05-03 against
    # the Amsterdam location=3 listing: 11 hubs vs 1 direct event), and
    # STEP2's loaded-page extractor reads ticket-type links from both shapes.
    # Treat both as valid STEP2 inputs, preferring direct event URLs first.
    events: list[str] = []
    hubs: list[str] = []
    mode = ""
    for ln in (p.stdout or "").splitlines():
        s = ln.strip()
        if s == "EVENT_URLS":
            mode = "events"
            continue
        if s == "HUB_URLS":
            mode = "hubs"
            continue
        if not s.startswith("https://www.ticketswap.com/festival-tickets/"):
            continue
        if mode == "events" and "/festival-tickets/a/" not in s:
            events.append(s)
        elif mode == "hubs" and "/festival-tickets/a/" in s:
            hubs.append(s)
    combined = list(dict.fromkeys(events + hubs))
    return combined[: int(limit_events)]


def _save_shared_step2_debug(
    *,
    event_url: str,
    status: str,
    classification: str,
    current_url: str,
    html: str,
    visible_text: str,
    ticket_urls: list[str],
    counts: dict[str, int],
    screenshot_writer: Optional[Any],
) -> str:
    slug = _event_slug(event_url)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = Path(config.DEBUG_DIR) / "step2_shared_listing_click" / f"{slug}_{ts}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "event_url.txt").write_text(event_url or "", encoding="utf-8")
    (out / "current_url.txt").write_text(current_url or "", encoding="utf-8")
    (out / "status.txt").write_text(status, encoding="utf-8")
    (out / "classification.txt").write_text(classification, encoding="utf-8")
    (out / "page.html").write_text(html or "", encoding="utf-8")
    (out / "visible_text.txt").write_text(visible_text or "", encoding="utf-8")
    (out / "ticket_urls.txt").write_text("\n".join(ticket_urls), encoding="utf-8")
    (out / "extraction_debug.json").write_text(
        json.dumps({"counts": counts, "ticket_urls_found": len(ticket_urls), "ticket_urls": ticket_urls}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if screenshot_writer is not None:
        with contextlib.suppress(Exception):
            screenshot_writer(str(out / "screenshot.png"))
    return str(out)


def _collect_listing_event_urls_live(driver: Any, listing_url: str, *, limit_events: int) -> list[str]:
    # The Amsterdam-pinned listing tiles often link to hub URLs
    # (/festival-tickets/a/<slug>) instead of to direct event pages.
    # STEP2's loaded-page extractor reads ticket-type links from both shapes,
    # so we treat both as valid click targets here. Direct event URLs are
    # preferred over hubs for click order to keep listings deterministic.
    html = driver.page_source or ""
    hrefs = sorted(du.merge_link_candidates(html, driver, base_url=listing_url))
    events: list[str] = []
    hubs: list[str] = []
    for h in hrefs:
        n = du.normalize_url(h)
        if not n or du.is_ticket_url(n):
            continue
        if du.is_event_page(n):
            if n not in events:
                events.append(n)
        elif du.is_festival_page(n):
            if n not in hubs:
                hubs.append(n)
    combined: list[str] = []
    for u in events + hubs:
        if u in combined:
            continue
        combined.append(u)
        if len(combined) >= int(limit_events):
            break
    return combined


def _click_event_link_on_listing(driver: Any, event_url: str) -> bool:
    target = du.normalize_url(event_url) or event_url
    if not target:
        return False
    try:
        from selenium.webdriver.common.by import By
    except Exception:
        By = None  # type: ignore
    if By is not None:
        with contextlib.suppress(Exception):
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            for a in anchors:
                href = du.normalize_url(a.get_attribute("href") or "") or ""
                if href != target:
                    continue
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)
                time.sleep(0.25)
                driver.execute_script("arguments[0].click();", a)
                return True
    with contextlib.suppress(Exception):
        return bool(
            driver.execute_script(
                """
                const target = arguments[0];
                const as = Array.from(document.querySelectorAll('a[href]'));
                for (const a of as) {
                  const href = a.href || a.getAttribute('href') || '';
                  if (href !== target) continue;
                  try { a.scrollIntoView({block:'center'}); a.click(); return true; } catch (e) {}
                }
                return false;
                """,
                target,
            )
        )
    return False


def _discover_shared_listing_click(
    *,
    listing_url: str,
    limit_events: int,
    headed: bool,
    debug: bool,
    max_consecutive_blocked: int = 2,
) -> list[tuple[str, Step2Result]]:
    """
    Shared-session STEP2 strategy: one browser for listing -> click event -> parse -> back.
    """
    results: list[tuple[str, Step2Result]] = []
    driver = du.new_driver(headless=not headed)
    blocked_streak = 0
    try:
        driver.set_page_load_timeout(90)
        driver.get("https://www.ticketswap.com/")
        time.sleep(random.uniform(8.0, 12.0))
        driver.get(listing_url)
        du.wait_for_page_content(driver, headless=not headed)
        time.sleep(random.uniform(8.0, 15.0))
        du.scroll_for_lazy_content(driver)
        time.sleep(random.uniform(2.0, 5.0))
        event_urls: list[str] = []
        for _ in range(4):
            for ev in _collect_listing_event_urls_live(driver, listing_url, limit_events=limit_events):
                if ev not in event_urls:
                    event_urls.append(ev)
            if len(event_urls) >= int(limit_events):
                break
            with contextlib.suppress(Exception):
                du.expand_category_listing_show_more(driver, listing_url, "festival-tickets", max_clicks=2)
            du.scroll_for_lazy_content(driver)
            time.sleep(random.uniform(2.0, 5.0))
        event_urls = event_urls[: int(limit_events)]
        if not event_urls:
            with contextlib.suppress(Exception):
                event_urls = _run_step1_events(listing_url, limit_events=limit_events, headless=not headed)
        listing_anchor = str(getattr(driver, "current_url", "") or listing_url)
        for ev in event_urls:
            clicked = _click_event_link_on_listing(driver, ev)
            if not clicked:
                driver.get(ev)
            time.sleep(random.uniform(10.0, 20.0))
            du.scroll_for_lazy_content(driver)
            html = driver.page_source or ""
            visible = ""
            with contextlib.suppress(Exception):
                visible = str(driver.execute_script("return document.body && document.body.innerText") or "")
            current_url = str(getattr(driver, "current_url", "") or ev)
            page_title = ""
            with contextlib.suppress(Exception):
                page_title = str(getattr(driver, "title", "") or "")
            classification = classify_loaded_selenium_page(
                current_url=current_url, html=html, visible_text=visible, title=page_title
            )
            if classification in ("401_or_forbidden", "verification_page"):
                step2 = Step2Result(
                    du.normalize_url(ev) or ev,
                    "blocked",
                    True,
                    "shared_listing_click",
                    [],
                    debug_dir=None,
                    failure_reason="verification_blocked",
                )
                blocked_streak += 1
                if debug:
                    dbg = _save_shared_step2_debug(
                        event_url=ev,
                        status=step2.status,
                        classification=classification,
                        current_url=current_url,
                        html=html,
                        visible_text=visible,
                        ticket_urls=[],
                        counts={},
                        screenshot_writer=getattr(driver, "save_screenshot", None),
                    )
                    step2 = Step2Result(
                        step2.event_url,
                        step2.status,
                        step2.verification,
                        step2.strategy,
                        step2.ticket_urls,
                        debug_dir=dbg,
                        failure_reason=step2.failure_reason,
                    )
            elif classification == "404":
                step2 = Step2Result(
                    du.normalize_url(ev) or ev,
                    "no_data",
                    False,
                    "shared_listing_click",
                    [],
                    debug_dir=None,
                    failure_reason="no_ticket_urls_after_real_page",
                )
                blocked_streak = 0
                if debug:
                    dbg = _save_shared_step2_debug(
                        event_url=ev,
                        status=step2.status,
                        classification=classification,
                        current_url=current_url,
                        html=html,
                        visible_text=visible,
                        ticket_urls=[],
                        counts={},
                        screenshot_writer=getattr(driver, "save_screenshot", None),
                    )
                    step2 = Step2Result(
                        step2.event_url,
                        step2.status,
                        step2.verification,
                        step2.strategy,
                        step2.ticket_urls,
                        debug_dir=dbg,
                        failure_reason=step2.failure_reason,
                    )
            else:
                ticket_urls, counts = extract_ticket_urls_from_loaded_selenium_page(driver, event_url=ev)
                status = "ok" if ticket_urls else "no_data"
                fr = None if status == "ok" else "no_ticket_urls_after_real_page"
                step2 = Step2Result(
                    du.normalize_url(ev) or ev, status, False, "shared_listing_click", ticket_urls, debug_dir=None, failure_reason=fr
                )
                blocked_streak = 0
                if debug and status != "ok":
                    dbg = _save_shared_step2_debug(
                        event_url=ev,
                        status=step2.status,
                        classification=classification,
                        current_url=current_url,
                        html=html,
                        visible_text=visible,
                        ticket_urls=ticket_urls,
                        counts=counts,
                        screenshot_writer=getattr(driver, "save_screenshot", None),
                    )
                    step2 = Step2Result(
                        step2.event_url,
                        step2.status,
                        step2.verification,
                        step2.strategy,
                        step2.ticket_urls,
                        debug_dir=dbg,
                        failure_reason=step2.failure_reason,
                    )
            results.append((ev, step2))
            if blocked_streak >= int(max_consecutive_blocked):
                break
            with contextlib.suppress(Exception):
                driver.back()
                time.sleep(random.uniform(2.0, 4.0))
                if not str(getattr(driver, "current_url", "") or "").startswith(listing_anchor):
                    driver.get(listing_anchor)
                    time.sleep(random.uniform(2.0, 4.0))
            time.sleep(random.uniform(2.0, 6.0))
    finally:
        with contextlib.suppress(Exception):
            driver.quit()
    return results


def _discover_live_with_retry(
    event_url: str,
    *,
    headed: bool,
    debug: bool,
    browser: str,
    verification_wait_seconds: int,
    wait_for_manual_verification: bool,
    manual_verification_timeout: int,
    manual_verification_press_enter: bool = False,
    debug_dump: bool = False,
    retries: int,
    strategy: str,
    blocked_sleep_min: int,
    blocked_sleep_max: int,
    page_timeout_ms: int,
    pre_network_wait_ms: int,
    post_network_wait_ms: int,
    existing_driver: Any | None = None,
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
            strategy_norm = str(strategy or "").strip().lower()
            if strategy_norm == "selenium_embedded_only":
                order = ["selenium_embedded_only"]
            elif strategy_norm == "selenium_slow_hydrate":
                order = ["selenium"]
            elif strategy_norm == "playwright_network":
                order = ["playwright"]
            elif strategy_norm == "hybrid_fast":
                order = ["selenium_embedded_only", "playwright"]
            elif strategy_norm == "hybrid_safe":
                order = ["selenium_embedded_only", "selenium", "playwright"]
            elif strategy_norm == "shared_listing_click":
                # handled at listing-session level in run_discovery_mode
                order = ["selenium"]
            else:
                b = str(browser or "auto")
                if b == "selenium":
                    order = ["selenium", "playwright"]
                elif b == "playwright":
                    order = ["playwright", "selenium"]
                else:
                    order = ["selenium", "playwright"]

            res = Step2Result(du.normalize_url(event_url) or event_url, "no_data", False, "none", [], debug_dir=None)
            for strategy in order:
                if strategy == "selenium_embedded_only":
                    cand = discover_ticket_urls_from_event_selenium_embedded_only(
                        event_url,
                        headed=bool(headed),
                        debug=bool(debug),
                        verification_wait_seconds=int(verification_wait_seconds),
                        debug_root="step2_vps_live",
                        wait_for_manual_verification=bool(wait_for_manual_verification),
                        manual_verification_timeout=int(manual_verification_timeout),
                        manual_verification_press_enter=bool(manual_verification_press_enter),
                    )
                elif strategy == "selenium":
                    cand = discover_ticket_urls_from_event_selenium(
                        event_url,
                        headed=bool(headed),
                        debug=bool(debug),
                        verification_wait_seconds=int(verification_wait_seconds),
                        debug_root="step2_vps_live",
                        wait_for_manual_verification=bool(wait_for_manual_verification),
                        manual_verification_timeout=int(manual_verification_timeout),
                        manual_verification_press_enter=bool(manual_verification_press_enter),
                        debug_dump=bool(debug_dump),
                        existing_driver=existing_driver,
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
            if _is_selenium_driver_dead(e):
                fr = "driver_session_lost"
            elif "timeout" in str(e).lower() or "timeout" in type(e).__name__.lower():
                fr = "timeout"
            else:
                fr = "extraction_error"
            return (
                Step2Result(ev, "error", False, "none", [], debug_dir=None, failure_reason=fr),
                attempt + 1,
                err_detail,
                verification_detected,
            )
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
    from discovery import ticketswap_vps_mode as tvm

    logging.info("Starting discovery mode")
    scope_name = str(args.scope)
    pd = str(getattr(args, "profile_dir", "") or "").strip()
    if pd:
        os.environ["TICKETSWAP_PROFILE_DIR"] = str(Path(pd).expanduser().resolve())

    use_vps_eighteen = bool(getattr(args, "vps_eighteen_hubs", False)) or str(
        os.getenv("TICKETSWAP_VPS_EIGHTEEN", "")
    ).strip().lower() in ("1", "true", "yes", "on")

    hub_event_urls: list[str] = []
    if use_vps_eighteen:
        from discovery.vps_eighteen_targets import EIGHTEEN_FESTIVAL_URLS

        hub_event_urls = list(EIGHTEEN_FESTIVAL_URLS)
        listing_urls: list[str] = []
    else:
        listing_urls = [str(args.listing_url)] if args.listing_url else list(
            config.SCOPES.get(scope_name, {}).get("listing_urls", [])
        )
        if not listing_urls:
            raise SystemExit(f"No listing URLs configured for scope '{scope_name}'.")

    headed_vps = tvm.is_headed_vps_browser_mode(args)
    if headed_vps and bool(getattr(args, "headless", False)):
        raise SystemExit(
            "headed_vps (TICKETSWAP_BROWSER_MODE=headed_vps or --headed-vps) cannot be combined with --headless."
        )

    args.headed = bool(du.resolve_discovery_headed(args))
    headless = not bool(args.headed)

    if use_vps_eighteen and headless:
        raise SystemExit("--vps-eighteen-hubs (or TICKETSWAP_VPS_EIGHTEEN=1) requires headed Chrome (not --headless).")

    if bool(getattr(args, "anonymous_profile", False)):
        config.STEP2_USE_ANONYMOUS_PROFILE = True
    if bool(getattr(args, "no_interact", False)):
        config.STEP2_INTERACT_ENABLED = False
    elif bool(getattr(args, "interact", False)):
        config.STEP2_INTERACT_ENABLED = True
    if bool(getattr(args, "manual_verification", False)):
        config.STEP2_MANUAL_VERIFICATION_PRESS_ENTER = True
    if bool(getattr(args, "debug_dump", False)):
        setattr(config, "STEP2_DEBUG_DUMP_ON_FAILURE", True)

    setattr(config, "STEP2_LAST_PROFILE_HEALTH", "unknown")
    vps_lock_acquired = False
    if headed_vps:
        vcb.run_clean_slate_if_enabled(logger=logging.getLogger("ticketswap.pipeline"))
        vcb.run_ensure_xvfb_if_enabled(logger=logging.getLogger("ticketswap.pipeline"))
        tvm.validate_headed_vps_prerequisites(profile_dir=config.ticketswap_profile_directory(), allow_anonymous=False)
        tvm.apply_headed_vps_runtime_defaults()
        try:
            tvm.acquire_step2_profile_lock(
                config.ticketswap_profile_directory(),
                logger=logging.getLogger("ticketswap.pipeline"),
            )
            vps_lock_acquired = True
        except tvm.ProfileLockError as exc:
            logging.error("%s", exc)
            print(str(exc), flush=True)
            return 4
        hr = tvm.run_profile_health_probe(headed=True, logger=logging.getLogger("ticketswap.pipeline"))
        setattr(config, "STEP2_LAST_PROFILE_HEALTH", hr.status.value)
        if hr.status != tvm.ProfileHealthStatus.trusted:
            if tvm.is_non_interactive_vps():
                tvm.release_step2_profile_lock()
                vps_lock_acquired = False
                raise SystemExit(
                    f"PROFILE_HEALTH status={hr.status.value} — non-interactive environment cannot clear "
                    "verification/login. Fix the profile using a display/VNC (see scripts/README_step2_vps.md)."
                )
            logging.warning(
                "PROFILE_HEALTH status=%s; solve verification/login in the browser, then press Enter.",
                hr.status.value,
            )
            du.pause_manual_verification_enter(logger=logging.getLogger("ticketswap.pipeline"))
            hr = tvm.run_profile_health_probe(headed=True, logger=logging.getLogger("ticketswap.pipeline"))
            setattr(config, "STEP2_LAST_PROFILE_HEALTH", hr.status.value)
            if hr.status != tvm.ProfileHealthStatus.trusted:
                tvm.release_step2_profile_lock()
                vps_lock_acquired = False
                raise SystemExit(f"PROFILE_HEALTH still not trusted: status={hr.status.value}")
    elif bool(getattr(args, "slow", False)):
        du.apply_step2_slow_timings()

    if headless:
        config.warn_step2_headless_without_trusted_profile(logger=logging.getLogger("ticketswap.pipeline"))
    else:
        config.warn_if_step2_profile_missing(logger=logging.getLogger("ticketswap.pipeline"))

    logging.info(
        "Discovery headed=%s headless=%s headed_vps=%s profile_dir=%s interact=%s slow=%s",
        bool(args.headed),
        headless,
        headed_vps,
        str(config.ticketswap_profile_directory()),
        bool(getattr(config, "STEP2_INTERACT_ENABLED", False)),
        bool(getattr(config, "STEP2_SLOW_MODE", False)),
    )

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
    suppress_per_event_step2_alerts = bool(getattr(args, "suppress_per_event_step2_alerts", False))
    configured_strategy = str(getattr(args, "step2_discovery_strategy", "") or "").strip().lower()
    if not configured_strategy:
        configured_strategy = str(getattr(config, "STEP2_DISCOVERY_STRATEGY", "hybrid_fast") or "hybrid_fast").strip().lower()
    browser_pref = str(getattr(args, "step2_browser", "auto"))
    if use_vps_eighteen:
        configured_strategy = "selenium_slow_hydrate"
        browser_pref = "selenium"
        # Do not churn retries per hub; we do a targeted rerun pass at the end for the 0-fresh hubs.
        retries = 0
    if safe_mode and browser_pref == "auto":
        browser_pref = "selenium"
    page_timeout_ms = 75_000 if safe_mode else 45_000
    pre_network_wait_ms = 3000 if safe_mode else 1500
    post_network_wait_ms = 5000 if safe_mode else 2500
    inter_event_min = 2.0 if safe_mode else 0.2
    inter_event_max = 6.0 if safe_mode else 1.0
    counts: dict[str, int] = {
        "listing_urls": len(hub_event_urls) if use_vps_eighteen else len(listing_urls),
        "events_collected": 0,
        "events_upserted": 0,
        "step2_fresh_ok": 0,
        "step2_blocked": 0,
        "step2_db_fallback_used": 0,
        "step2_errors": 0,
        "ticket_types_fresh_upserted": 0,
        "ticket_types_fallback_upserted": 0,
        "stopped_early_blocked": 0,
        "step2_no_fresh": 0,
        "shared_driver_restarts": 0,
        "step2_driver_session_lost": 0,
        "step2_hub_retry_after_driver_restart_ok": 0,
        "vps18_driver_recovery_restarts": 0,
    }
    step2_fresh_fail_event_urls: list[str] = []
    step2_fresh_fail_debug_dirs: set[str] = set()
    vps18_zero_fresh: list[str] = []

    def _handle_event_step2(ev: str, step2: Step2Result) -> bool:
        nonlocal blocked_count, blocked_consecutive, stopped_early_blocked
        slug = _event_slug(ev)
        if step2.status == "blocked":
            blocked_count += 1
            counts["step2_blocked"] += 1
            counts["step2_no_fresh"] += 1
            blocked_consecutive += 1
            step2_fresh_fail_event_urls.append(ev)
            if step2.debug_dir:
                step2_fresh_fail_debug_dirs.add(step2.debug_dir)
            if require_fresh_step2 and (not suppress_per_event_step2_alerts):
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
            if step2.failure_reason == "driver_session_lost":
                counts["step2_driver_session_lost"] += 1
            else:
                counts["step2_no_fresh"] += 1
            blocked_consecutive = 0
            step2_fresh_fail_event_urls.append(ev)
            if step2.debug_dir:
                step2_fresh_fail_debug_dirs.add(step2.debug_dir)
            _send_error_alert(
                error_type="step2_error",
                event_url=ev,
                debug_path=step2.debug_dir,
                details=f"{step2.status} failure_reason={step2.failure_reason}",
            )
        else:
            blocked_consecutive = 0
            counts["step2_no_fresh"] += 1
            step2_fresh_fail_event_urls.append(ev)
            if step2.debug_dir:
                step2_fresh_fail_debug_dirs.add(step2.debug_dir)
            if use_vps_eighteen and (step2.status != "blocked") and (not step2.ticket_urls):
                vps18_zero_fresh.append(ev)
            if require_fresh_step2 and (not suppress_per_event_step2_alerts):
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
            return True
        return False

    vps18_discovery_fatal: Optional[str] = None
    try:
        if use_vps_eighteen:
            shared_driver: Any | None = None
            recovery_restarts = 0
            try:
                shared_driver = du.new_driver(headless=headless)
                counts["events_collected"] = len(hub_event_urls)
                manual_press = bool(getattr(config, "STEP2_MANUAL_VERIFICATION_PRESS_ENTER", False))
                dbg_dump_default = bool(getattr(config, "STEP2_DEBUG_DUMP_ON_FAILURE", False))

                def _vps18_log_restart(total: int, reason: str, hub: str) -> None:
                    logging.warning(
                        "[STEP2][VPS18] shared driver restart count=%s reason=%s hub=%s",
                        total,
                        reason,
                        hub,
                    )

                def _vps18_recovery_restart(reason: str, hub: str) -> bool:
                    nonlocal shared_driver, recovery_restarts
                    if recovery_restarts >= MAX_VPS18_DRIVER_RECOVERY_RESTARTS:
                        return False
                    recovery_restarts += 1
                    counts["vps18_driver_recovery_restarts"] = recovery_restarts
                    counts["shared_driver_restarts"] += 1
                    _vps18_log_restart(counts["shared_driver_restarts"], reason, hub)
                    _safe_driver_quit(shared_driver)
                    vcb.run_clean_slate_if_enabled(logger=logging.getLogger("ticketswap.pipeline"))
                    time.sleep(2.0)
                    shared_driver = du.new_driver(headless=headless)
                    return True

                def _vps18_preventive_restart(hub: str) -> None:
                    nonlocal shared_driver
                    counts["shared_driver_restarts"] += 1
                    _vps18_log_restart(
                        counts["shared_driver_restarts"],
                        f"preventive_every_{RESTART_SHARED_DRIVER_EVERY_N_HUBS}_hubs",
                        hub,
                    )
                    _safe_driver_quit(shared_driver)
                    vcb.run_clean_slate_if_enabled(logger=logging.getLogger("ticketswap.pipeline"))
                    time.sleep(1.5)
                    shared_driver = du.new_driver(headless=headless)

                def _vps18_discover(
                    ev: str, *, dbg: bool, dbg_dump: bool
                ) -> tuple[Step2Result, int, Optional[str], bool]:
                    return _discover_live_with_retry(
                        ev,
                        headed=bool(args.headed),
                        debug=dbg,
                        browser=browser_pref,
                        verification_wait_seconds=verification_wait,
                        wait_for_manual_verification=wait_for_manual_verification,
                        manual_verification_timeout=300,
                        manual_verification_press_enter=manual_press,
                        debug_dump=dbg_dump,
                        retries=retries,
                        strategy=configured_strategy,
                        blocked_sleep_min=blocked_sleep_min,
                        blocked_sleep_max=blocked_sleep_max,
                        page_timeout_ms=page_timeout_ms,
                        pre_network_wait_ms=pre_network_wait_ms,
                        post_network_wait_ms=post_network_wait_ms,
                        existing_driver=shared_driver,
                    )

                def _fatal_driver_step2(ev: str) -> Step2Result:
                    evn = du.normalize_url(ev) or ev
                    return Step2Result(
                        evn,
                        "error",
                        False,
                        "none",
                        [],
                        debug_dir=None,
                        failure_reason="driver_session_lost",
                    )

                def _vps18_run_one_hub(
                    hub_ix: int,
                    ev: str,
                    *,
                    dbg: bool,
                    dbg_dump: bool,
                    skip_preventive: bool = False,
                ) -> Step2Result:
                    nonlocal shared_driver, vps18_discovery_fatal
                    assert shared_driver is not None
                    if (
                        (not skip_preventive)
                        and hub_ix > 1
                        and (hub_ix - 1) % RESTART_SHARED_DRIVER_EVERY_N_HUBS == 0
                    ):
                        _vps18_preventive_restart(ev)
                    if not _driver_healthcheck(shared_driver):
                        if not _vps18_recovery_restart("healthcheck_preflight", ev):
                            vps18_discovery_fatal = (
                                f"Exceeded max VPS18 driver recovery restarts ({MAX_VPS18_DRIVER_RECOVERY_RESTARTS})"
                            )
                            return _fatal_driver_step2(ev)
                    step2, _a, err_detail, _v = _vps18_discover(ev, dbg=dbg, dbg_dump=dbg_dump)
                    needs_retry = (
                        (not _driver_healthcheck(shared_driver))
                        or (step2.status == "error" and step2.failure_reason == "driver_session_lost")
                        or (bool(err_detail) and _is_selenium_driver_dead(RuntimeError(err_detail)))
                    )
                    if needs_retry:
                        if not _vps18_recovery_restart("recover_after_step2_dead_session", ev):
                            vps18_discovery_fatal = (
                                f"Exceeded max VPS18 driver recovery restarts ({MAX_VPS18_DRIVER_RECOVERY_RESTARTS})"
                            )
                            return _fatal_driver_step2(ev)
                        step2, _a2, err_detail2, _v2 = _vps18_discover(ev, dbg=dbg, dbg_dump=dbg_dump)
                        if step2.status == "ok" and step2.ticket_urls:
                            counts["step2_hub_retry_after_driver_restart_ok"] += 1
                        elif (
                            (not _driver_healthcheck(shared_driver))
                            or (step2.status == "error" and step2.failure_reason == "driver_session_lost")
                            or (bool(err_detail2) and _is_selenium_driver_dead(RuntimeError(err_detail2)))
                        ):
                            evn = du.normalize_url(ev) or ev
                            step2 = Step2Result(
                                evn,
                                "error",
                                False,
                                "none",
                                [],
                                debug_dir=None,
                                failure_reason="driver_session_lost",
                            )
                    return step2

                for hub_ix, ev in enumerate(hub_event_urls, start=1):
                    if vps18_discovery_fatal:
                        break
                    _jitter(inter_event_min, inter_event_max)
                    try:
                        step2 = _vps18_run_one_hub(
                            hub_ix, ev, dbg=bool(args.debug), dbg_dump=dbg_dump_default
                        )
                    except Exception as e:
                        if _is_selenium_driver_dead(e):
                            if _vps18_recovery_restart("exception_during_step2", ev):
                                try:
                                    step2, _, _, _ = _vps18_discover(
                                        ev, dbg=bool(args.debug), dbg_dump=dbg_dump_default
                                    )
                                    if step2.status == "ok" and step2.ticket_urls:
                                        counts["step2_hub_retry_after_driver_restart_ok"] += 1
                                except Exception as e2:
                                    evn = du.normalize_url(ev) or ev
                                    fr = (
                                        "driver_session_lost"
                                        if _is_selenium_driver_dead(e2)
                                        else "extraction_error"
                                    )
                                    step2 = Step2Result(
                                        evn,
                                        "error",
                                        False,
                                        "none",
                                        [],
                                        debug_dir=None,
                                        failure_reason=fr,
                                    )
                            else:
                                vps18_discovery_fatal = (
                                    f"Exceeded max VPS18 driver recovery restarts ({MAX_VPS18_DRIVER_RECOVERY_RESTARTS})"
                                )
                                step2 = _fatal_driver_step2(ev)
                        else:
                            raise
                    if _handle_event_step2(ev, step2):
                        break
                    if vps18_discovery_fatal:
                        break

                # Targeted rerun ONLY for hubs that produced zero fresh URLs.
                # This avoids retesting successful hubs while still giving "empty" hubs a second shot.
                if vps18_zero_fresh and (not vps18_discovery_fatal):
                    logging.warning("VPS18 rerun pass: %d hubs had zero fresh URLs", len(vps18_zero_fresh))
                    for ev in list(dict.fromkeys(vps18_zero_fresh)):
                        if vps18_discovery_fatal:
                            break
                        _jitter(inter_event_min, inter_event_max)
                        try:
                            step2 = _vps18_run_one_hub(
                                1,
                                ev,
                                dbg=True,
                                dbg_dump=True,
                                skip_preventive=True,
                            )
                        except Exception as e:
                            if _is_selenium_driver_dead(e):
                                if _vps18_recovery_restart("exception_during_step2_rerun", ev):
                                    try:
                                        step2, _, _, _ = _vps18_discover(ev, dbg=True, dbg_dump=True)
                                        if step2.status == "ok" and step2.ticket_urls:
                                            counts["step2_hub_retry_after_driver_restart_ok"] += 1
                                    except Exception as e2:
                                        evn = du.normalize_url(ev) or ev
                                        fr = (
                                            "driver_session_lost"
                                            if _is_selenium_driver_dead(e2)
                                            else "extraction_error"
                                        )
                                        step2 = Step2Result(
                                            evn,
                                            "error",
                                            False,
                                            "none",
                                            [],
                                            debug_dir=None,
                                            failure_reason=fr,
                                        )
                                else:
                                    vps18_discovery_fatal = (
                                        f"Exceeded max VPS18 driver recovery restarts ({MAX_VPS18_DRIVER_RECOVERY_RESTARTS})"
                                    )
                                    step2 = _fatal_driver_step2(ev)
                            else:
                                raise
                        _handle_event_step2(ev, step2)
                        if vps18_discovery_fatal:
                            break
            finally:
                if shared_driver is not None:
                    with contextlib.suppress(Exception):
                        shared_driver.quit()
        else:
            for listing_url in listing_urls:
                if configured_strategy == "shared_listing_click":
                    shared_results = _discover_shared_listing_click(
                        listing_url=listing_url,
                        limit_events=int(args.limit_events),
                        headed=bool(args.headed),
                        debug=bool(args.debug),
                        max_consecutive_blocked=2,
                    )
                    counts["events_collected"] += len(shared_results)
                    for ev, step2 in shared_results:
                        _jitter(inter_event_min, inter_event_max)
                        if _handle_event_step2(ev, step2):
                            break
                else:
                    events = _run_step1_events(listing_url, limit_events=args.limit_events, headless=headless)
                    counts["events_collected"] += len(events)
                    for ev in events:
                        _jitter(inter_event_min, inter_event_max)
                        step2, _, _, _verification_seen = _discover_live_with_retry(
                            ev,
                            headed=bool(args.headed),
                            debug=bool(args.debug),
                            browser=browser_pref,
                            verification_wait_seconds=verification_wait,
                            wait_for_manual_verification=wait_for_manual_verification,
                            manual_verification_timeout=300,
                            manual_verification_press_enter=bool(
                                getattr(config, "STEP2_MANUAL_VERIFICATION_PRESS_ENTER", False)
                            ),
                            debug_dump=bool(getattr(config, "STEP2_DEBUG_DUMP_ON_FAILURE", False)),
                            retries=retries,
                            strategy=configured_strategy,
                            blocked_sleep_min=blocked_sleep_min,
                            blocked_sleep_max=blocked_sleep_max,
                            page_timeout_ms=page_timeout_ms,
                            pre_network_wait_ms=pre_network_wait_ms,
                            post_network_wait_ms=post_network_wait_ms,
                            existing_driver=None,
                        )
                        if _handle_event_step2(ev, step2):
                            break
                if stopped_early_blocked:
                    break

        paths = _export_mode_csvs(conn, Path("data/outputs"))
        final_status = "ok"
        if vps18_discovery_fatal:
            final_status = "failed"
        elif require_fresh_step2 and (counts["ticket_types_fresh_upserted"] == 0):
            final_status = "verification_blocked"
        elif stopped_early_blocked or (counts["step2_blocked"] > 0 and counts["ticket_types_fresh_upserted"] == 0):
            final_status = "verification_blocked_partial"
        db.finish_pipeline_run(
            conn,
            run_id=run_id,
            status=final_status,
            counts=counts,
            error_summary=vps18_discovery_fatal if vps18_discovery_fatal else None,
        )
        if blocked_count >= 5 and (not suppress_per_event_step2_alerts):
            _send_error_alert(
                error_type="verification_blocked_threshold_exceeded",
                details=f"blocked_count={blocked_count} scope={scope_name}",
            )
        if require_fresh_step2 and final_status == "verification_blocked" and (not suppress_per_event_step2_alerts):
            _send_error_alert(
                error_type="step2_fresh_failure",
                details="require-fresh discovery ended with zero fresh ticket URLs",
            )
        if stopped_early_blocked and (not suppress_per_event_step2_alerts):
            _send_error_alert(
                error_type="verification_blocked_threshold_exceeded",
                details=f"stopped_early consecutive_blocked={blocked_consecutive}",
            )
        if suppress_per_event_step2_alerts and require_fresh_step2 and step2_fresh_fail_event_urls:
            top_failed = step2_fresh_fail_event_urls[:10]
            debug_root = ", ".join(sorted(step2_fresh_fail_debug_dirs)[:3]) if step2_fresh_fail_debug_dirs else "data/debug/step2_vps_live"
            summary_lines = [
                f"events_tested={counts.get('events_upserted', 0)}",
                f"fresh_success={counts.get('step2_fresh_ok', 0)}",
                f"blocked={counts.get('step2_blocked', 0)}",
                f"no_fresh_ticket={counts.get('step2_no_fresh', 0)}",
                f"driver_session_lost={counts.get('step2_driver_session_lost', 0)}",
                f"shared_driver_restarts={counts.get('shared_driver_restarts', 0)}",
                f"failed_events_top10={top_failed}",
                f"debug_dir={debug_root}",
            ]
            _send_error_alert(
                error_type="step2_fresh_failure_aggregated",
                details="; ".join(summary_lines),
            )
        if counts["ticket_types_fresh_upserted"] == 0:
            logging.warning("0 fresh ticket URLs discovered in this discovery run.")
        _maybe_send_daily_outputs(conn, datetime.now(ZoneInfo(getattr(config, "LOCAL_TIMEZONE", "Europe/Amsterdam"))))
        print(json.dumps({"run_id": run_id, "mode": "discovery", "status": final_status, "counts": counts}, indent=2))
        print(f"CSV exports: {', '.join(str(p) for p in paths.values())}")
        if vps18_discovery_fatal:
            return 7
        return 3 if stopped_early_blocked else 0
    except Exception as exc:
        db.finish_pipeline_run(conn, run_id=run_id, status="failed", counts=counts, error_summary=str(exc))
        _send_error_alert(error_type="pipeline_crash", details=f"discovery scope={scope_name} error={exc}")
        logging.exception("Discovery mode failed")
        return 2
    finally:
        if headed_vps and vps_lock_acquired:
            tvm.release_step2_profile_lock()
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
