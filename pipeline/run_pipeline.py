"""
Full TicketSwap pipeline orchestrator (STEP 1 → STEP 2 → STEP 3).

Delegates to existing scripts/modules only — no changes to step1, step2, or scrape_market core logic.

Usage:
  python run_pipeline.py \\
    --listing-url "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3" \\
    --limit-events 20 \\
    --headed \\
    --debug \\
    --out debug/pipeline_runs/pipeline_20_events.jsonl
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import random
import shutil
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import config
from discovery import discover_urls as du
from pipeline import mode_runner
from discovery.step2_discover_ticket_urls import Step2Result, discover_ticket_urls_from_event_playwright
from discovery.step2_discover_ticket_urls import _safe_key_from_event_url as _event_slug_for_path
from scraping import scrape_market as sm


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run TicketSwap pipeline: listing -> events -> tickets -> market snapshots.")
    p.add_argument(
        "--mode",
        default="legacy",
        choices=["legacy", "discovery", "monitoring"],
        help="legacy = old full pipeline, discovery = collect events/ticket types, monitoring = scrape due ticket types",
    )
    p.add_argument(
        "--scope",
        default=getattr(config, "DEFAULT_SCOPE", "amsterdam_festivals"),
        help="Scope key in config.SCOPES (discovery mode).",
    )
    p.add_argument(
        "--listing-url",
        default="https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3",
        help="Festivals listing URL (Amsterdam location=3 recommended).",
    )
    p.add_argument("--limit-events", type=int, default=20)
    p.add_argument("--limit-tickets", type=int, default=25)
    p.add_argument("--headed", action="store_true", default=False, help="Run non-headless (recommended for verification).")
    p.add_argument("--debug", action="store_true", default=False)
    p.add_argument(
        "--scrape-market-in-discovery",
        action="store_true",
        default=False,
        help="Discovery mode only: also run market scrapes for discovered ticket types.",
    )
    p.add_argument(
        "--monitor-after-event",
        action="store_true",
        default=False,
        help="Monitoring mode only: continue scraping after event date.",
    )
    p.add_argument("--step2-retries", type=int, default=1, help="Discovery mode: retries after first STEP2 attempt.")
    p.add_argument(
        "--step2-blocked-sleep-min",
        type=int,
        default=30,
        help="Discovery mode: min seconds to sleep before retrying blocked STEP2.",
    )
    p.add_argument(
        "--step2-blocked-sleep-max",
        type=int,
        default=90,
        help="Discovery mode: max seconds to sleep before retrying blocked STEP2.",
    )
    p.add_argument(
        "--step2-blocked-stop-threshold",
        type=int,
        default=3,
        help="Discovery mode: stop early after more than this many consecutive blocked STEP2 events.",
    )
    p.add_argument(
        "--vps-safe-mode",
        action="store_true",
        default=False,
        help="Discovery mode: use slower timings and stronger anti-block behavior for VPS runs.",
    )
    p.add_argument(
        "--step2-browser",
        choices=["selenium", "playwright", "auto"],
        default="auto",
        help="Discovery mode: STEP2 browser strategy preference.",
    )
    p.add_argument(
        "--step2-verification-wait",
        type=int,
        default=60,
        help="Discovery mode: seconds to wait when verification is detected before retry/reload.",
    )
    p.add_argument(
        "--wait-for-manual-verification",
        action="store_true",
        default=False,
        help="Discovery mode: keep STEP2 browser open up to 300s for manual verification recovery.",
    )
    p.add_argument(
        "--require-fresh-step2",
        action="store_true",
        default=False,
        help="Discovery mode: disable DB fallback; fail discovery status if fresh STEP2 is blocked.",
    )
    p.add_argument(
        "--suppress-per-event-step2-alerts",
        action="store_true",
        default=False,
        help="Discovery mode: suppress per-event fresh STEP2 Telegram alerts and send one aggregated alert at end.",
    )
    p.add_argument(
        "--step2-discovery-strategy",
        choices=["selenium_embedded_only", "selenium_slow_hydrate", "playwright_network", "hybrid_fast", "hybrid_safe", "shared_listing_click"],
        default=None,
        help="Discovery mode: strategy profile for fresh STEP2 discovery.",
    )
    p.add_argument(
        "--out",
        default="data/outputs/pipeline_run.jsonl",
        help="Output JSONL path (parent dirs are created).",
    )
    return p.parse_args(argv)


def _jitter(a: float = 0.4, b: float = 1.2) -> None:
    time.sleep(a + random.random() * max(0.0, b - a))


def _event_slug(event_url: str) -> str:
    n = du.normalize_url(event_url) or event_url
    path = (urlparse(n).path or "").strip("/")
    if path.startswith("festival-tickets/"):
        path = path[len("festival-tickets/") :]
    seg = path.split("/")[0] if path else ""
    return seg or "unknown"


def _map_discovery_strategy(step2_strategy: str) -> str:
    if step2_strategy == "embedded_json":
        return "embedded_json"
    if step2_strategy == "network":
        return "network"
    return "fallback"


def _append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
        f.flush()


def _copy_step2_artifacts_to_run_dir(src: Optional[str], dest: Path) -> None:
    if not src:
        return
    sp = Path(src)
    if not sp.is_dir():
        return
    dest.mkdir(parents=True, exist_ok=True)
    for item in sp.iterdir():
        if item.is_file():
            with contextlib.suppress(Exception):
                shutil.copy2(item, dest / item.name)


def _discover_with_retry(
    event_url: str,
    *,
    headed: bool,
    debug: bool,
) -> tuple[Step2Result, int, Optional[str]]:
    """Up to two attempts; second after a delay for verification / transient failures / exceptions."""
    last: Optional[Step2Result] = None
    err_detail: Optional[str] = None
    for attempt in range(2):
        if attempt:
            _jitter(2.0, 5.0)
        try:
            res = discover_ticket_urls_from_event_playwright(
                event_url,
                headed=bool(headed),
                debug=bool(debug),
                db_fallback=True,
            )
        except Exception as e:
            err_detail = f"{type(e).__name__}: {e}"
            if attempt == 0:
                continue
            ev = du.normalize_url(event_url) or event_url
            return Step2Result(ev, "error", False, "none", [], debug_dir=None), attempt + 1, err_detail
        last = res
        if res.status == "ok" and res.ticket_urls:
            return res, attempt + 1, None
        if res.status == "blocked" and attempt == 0:
            continue
        if res.status == "no_data" and attempt == 0 and headed:
            continue
        return res, attempt + 1, None
    assert last is not None
    return last, 2, err_detail


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


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.mode == "discovery":
        return mode_runner.run_discovery_mode(args)
    if args.mode == "monitoring":
        return mode_runner.run_monitoring_mode(args)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    run_base = out_path.parent

    listing_url = args.listing_url
    headed = bool(args.headed)
    headless = not headed
    manual_wait = int(getattr(config, "MANUAL_VERIFY_WAIT_SECONDS", 90)) if headed else 0

    out_path.write_text("", encoding="utf-8")

    breakdown: dict[str, int] = {
        "step2_ok": 0,
        "step2_blocked": 0,
        "step2_no_data": 0,
        "step2_error": 0,
        "scrape_ok": 0,
        "scrape_blocked": 0,
        "scrape_no_data": 0,
        "scrape_error": 0,
    }

    stats: dict[str, Any] = {
        "total_events_processed": 0,
        "events_with_ticket_urls": 0,
        "total_ticket_urls": 0,
        "total_successful_market_scrapes": 0,
        "total_failures": 0,
        "failure_breakdown": breakdown,
    }

    # STEP 1 (subprocess; unchanged script)
    step1_cmd = [
        sys.executable,
        "-m",
        "discovery.step1_collect_listing_urls",
        "--url",
        listing_url,
        "--min-events",
        str(int(args.limit_events)),
        "--max-show-more",
        str(int(getattr(config, "DISCOVERY_OVERVIEW_MAX_SHOW_MORE", 50))),
    ]
    if headless:
        step1_cmd.append("--headless")

    p = subprocess.run(step1_cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if p.returncode != 0:
        _append_jsonl(
            out_path,
            {
                "type": "listing_step",
                "status": "failed",
                "listing_url": listing_url,
                "returncode": p.returncode,
                "stdout_tail": (p.stdout or "")[-4000:],
                "stderr_tail": (p.stderr or "")[-4000:],
            },
        )
        print(p.stdout)
        print(p.stderr)
        print("STEP1 failed (often verification). Re-run with --headed and persistent Chrome profile.")
        summary = {**stats, "type": "pipeline_summary", "step1": "failed"}
        _append_jsonl(out_path, summary)
        (out_path.with_suffix(".summary.json")).write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return 2

    events: list[str] = []
    mode = ""
    for ln in (p.stdout or "").splitlines():
        if ln.strip() == "EVENT_URLS":
            mode = "events"
            continue
        if ln.strip() == "HUB_URLS":
            mode = "hubs"
            continue
        if mode == "events" and ln.strip().startswith("https://www.ticketswap.com/festival-tickets/") and "/festival-tickets/a/" not in ln:
            events.append(ln.strip())
    events = list(dict.fromkeys(events))[: int(args.limit_events)]

    _append_jsonl(
        out_path,
        {
            "type": "listing_step",
            "status": "ok",
            "listing_url": listing_url,
            "events_collected": len(events),
        },
    )

    print(f"events_collected: {len(events)}")

    for ev in events:
        stats["total_events_processed"] += 1
        _jitter(0.35, 1.0)

        slug = _event_slug(ev)
        path_key = _event_slug_for_path(ev)
        event_debug_dir = run_base / path_key
        if bool(args.debug):
            event_debug_dir.mkdir(parents=True, exist_ok=True)

        scrape_debug_dir = event_debug_dir if bool(args.debug) else Path(config.DEBUG_DIR)

        step2, attempts, discover_err = _discover_with_retry(ev, headed=headed, debug=bool(args.debug))

        event_record = {
            "type": "event_discovery",
            "event_url": step2.event_url,
            "event_slug": slug,
            "category": "festival-tickets",
            "status": step2.status,
            "verification": step2.verification,
            "step2_strategy": step2.strategy,
            "ticket_count": len(step2.ticket_urls),
            "attempts": attempts,
            "error": discover_err,
        }
        _append_jsonl(out_path, event_record)

        if step2.status == "ok":
            breakdown["step2_ok"] += 1
        elif step2.status == "blocked":
            breakdown["step2_blocked"] += 1
            stats["total_failures"] += 1
        elif step2.status == "error":
            breakdown["step2_error"] += 1
            stats["total_failures"] += 1
        else:
            breakdown["step2_no_data"] += 1
            if not step2.ticket_urls:
                stats["total_failures"] += 1

        if bool(args.debug) and step2.status in ("blocked", "error", "no_data") and step2.debug_dir:
            _copy_step2_artifacts_to_run_dir(step2.debug_dir, event_debug_dir)

        if not step2.ticket_urls:
            continue

        stats["events_with_ticket_urls"] += 1
        stats["total_ticket_urls"] += len(step2.ticket_urls)

        disc = _map_discovery_strategy(step2.strategy)
        for tu in step2.ticket_urls:
            slug_t, label_t = du.ticket_type_from_ticket_url(tu)
            _append_jsonl(
                out_path,
                {
                    "type": "ticket_discovery",
                    "event_url": step2.event_url,
                    "event_slug": slug,
                    "ticket_url": tu,
                    "ticket_type_slug": slug_t,
                    "ticket_type_label": label_t,
                    "discovery_strategy": disc,
                    "step2_strategy_raw": step2.strategy,
                },
            )

        # Brief pause so Playwright releases the profile before UC attaches (Windows).
        _jitter(1.5, 3.5)

        with sm.market_scrape_session(headless=headless) as m_driver:
            for tu in step2.ticket_urls:
                _jitter(0.25, 0.85)
                snap, s_attempts = _scrape_with_retry(
                    m_driver,
                    tu,
                    headless=headless,
                    debug_dir=scrape_debug_dir,
                    manual_wait=manual_wait,
                )
                srec = {
                    "type": "market_snapshot",
                    "scrape_attempts": s_attempts,
                    **asdict(snap),
                }
                _append_jsonl(out_path, srec)

                if snap.status == "ok":
                    stats["total_successful_market_scrapes"] += 1
                    breakdown["scrape_ok"] += 1
                elif snap.status == "blocked":
                    breakdown["scrape_blocked"] += 1
                    stats["total_failures"] += 1
                elif snap.status == "no_data":
                    breakdown["scrape_no_data"] += 1
                    stats["total_failures"] += 1
                else:
                    breakdown["scrape_error"] += 1
                    stats["total_failures"] += 1

    stats["failure_breakdown"] = breakdown
    summary = {"type": "pipeline_summary", **stats}
    _append_jsonl(out_path, summary)
    (out_path.with_suffix(".summary.json")).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    print("")
    print("=== Pipeline summary ===")
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
    print(f"jsonl: {out_path.resolve()}")
    print(f"summary: {out_path.with_suffix('.summary.json').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
