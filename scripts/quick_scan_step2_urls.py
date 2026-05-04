#!/usr/bin/env python3
"""
Quick Selenium diagnostics for TicketSwap STEP2 (Selenium only — no Playwright, no DB).

Examples:
  python scripts/quick_scan_step2_urls.py --headed --slow --debug-dump
  python scripts/quick_scan_step2_urls.py --single-url "https://www.ticketswap.com/festival-tickets/a/909-festival" --headed --interact --debug-dump
  python scripts/quick_scan_step2_urls.py --profile-dir "C:/path/to/.ticketswap_browser_profile" --headed
  python scripts/quick_scan_step2_urls.py --fresh-profile --headed   # ephemeral Chrome (explicit)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config as _cfg

from discovery import discover_urls as du  # noqa: E402
from discovery import ticketswap_candidate_harvest as tch  # noqa: E402
from discovery.step2_discover_ticket_urls import extract_ticket_urls_from_loaded_selenium_page  # noqa: E402
from pipeline import mode_runner as mr  # noqa: E402

EIGHTEEN_URLS = [
    "https://www.ticketswap.com/festival-tickets/a/awakenings-upclose",
    "https://www.ticketswap.com/festival-tickets/a/dekmantel-festival",
    "https://www.ticketswap.com/festival-tickets/a/festifest",
    "https://www.ticketswap.com/festival-tickets/a/het-landjuweel",
    "https://www.ticketswap.com/festival-tickets/a/lente-kabinet",
    "https://www.ticketswap.com/festival-tickets/a/music-on-festival",
    "https://www.ticketswap.com/festival-tickets/a/springbreak-festival",
    "https://www.ticketswap.com/festival-tickets/hemmeland-live-festival-monnickendam-hemmeland-2026-05-14-CXAX2jkuni3hLnv7zQUU4",
    "https://www.ticketswap.com/festival-tickets/a/de-amsterdamse-zomer",
    "https://www.ticketswap.com/festival-tickets/a/loveland-festival",
    "https://www.ticketswap.com/festival-tickets/a/909-festival",
    "https://www.ticketswap.com/festival-tickets/a/de-zon-festival",
    "https://www.ticketswap.com/festival-tickets/a/joy-flow-festival",
    "https://www.ticketswap.com/festival-tickets/a/liberte-liberte",
    "https://www.ticketswap.com/festival-tickets/a/no-art-festival",
    "https://www.ticketswap.com/festival-tickets/a/festival-macumba",
    "https://www.ticketswap.com/festival-tickets/a/het-amsterdams-verbond",
    "https://www.ticketswap.com/festival-tickets/a/komm-schon-alter-das-festival",
]

PARIS_LISTING_FALLBACK = "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=510"
DEBUG_ROOT = ROOT / "tmp" / "ticketswap_debug"


def _load_paris_listing_from_cache() -> str:
    p = ROOT / "data" / "location_cache.json"
    if not p.exists():
        return PARIS_LISTING_FALLBACK
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        e = raw.get("Paris,France")
        if isinstance(e, dict):
            u = str(e.get("resulting_url") or "").strip()
            if u:
                return u
    except Exception:
        pass
    return PARIS_LISTING_FALLBACK


def _safe_slug(url: str) -> str:
    u = du.normalize_url(url) or url
    path = urlparse(u).path or "root"
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", path.strip("/"))[:120] or "root"
    return s[:100]


def _read_title_visible(driver: Any) -> tuple[str, str]:
    title = ""
    vis = ""
    with contextlib.suppress(Exception):
        title = str(getattr(driver, "title", "") or "")
    with contextlib.suppress(Exception):
        vis = str(driver.execute_script("return document.body && document.body.innerText") or "")
    return title, vis


def _verification_report(driver: Any, html: str) -> tuple[bool, str]:
    cur = str(getattr(driver, "current_url", "") or "")
    title, vis = _read_title_visible(driver)
    blocked = du.is_blocked_for_discovery(
        html,
        title=title,
        visible_text=vis[:8000],
        current_url=cur,
    )
    vhtml = du.looks_like_verification_html(
        html,
        current_url=cur,
        title=title,
        visible_text=vis[:8000],
    )
    if blocked:
        reason = "blocked_for_discovery (verification + no discovery signals)"
    elif vhtml:
        reason = "verification_html_heuristic (title/body/WAF markers)"
    elif not du.has_next_data_script(html) and len(html or "") > 15_000:
        reason = "large_html_but_no___NEXT_DATA__ (unexpected shell)"
    else:
        reason = "ok"
    return blocked or vhtml, reason


def _collect_anchor_hrefs(driver: Any) -> list[str]:
    try:
        raw = driver.execute_script(
            r"""
            const out = new Set();
            try {
              document.querySelectorAll('a[href]').forEach(a => {
                try { const h = a.href; if (h) out.add(String(h)); } catch (e) {}
              });
            } catch (e) {}
            return Array.from(out).sort();
            """
        )
        if isinstance(raw, list):
            return [str(x) for x in raw if x]
    except Exception:
        pass
    return []


def dump_debug_bundle(
    driver: Any,
    *,
    target_url: str,
    out_dir: Path,
    html: Optional[str] = None,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    html = html if html is not None else (driver.page_source or "")
    cur = str(getattr(driver, "current_url", "") or "")
    title, vis = _read_title_visible(driver)
    blocked, reason = _verification_report(driver, html)
    meta = {
        "target_url": target_url,
        "current_url": cur,
        "title": title,
        "verification_or_blocked": blocked,
        "diagnostic_reason": reason,
        "has_next_data": du.has_next_data_script(html),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "visible_body_head.txt").write_text((vis or "")[:2000], encoding="utf-8")
    (out_dir / "page.html").write_text(html or "", encoding="utf-8")
    hrefs = _collect_anchor_hrefs(driver)
    (out_dir / "hrefs.txt").write_text("\n".join(hrefs), encoding="utf-8")
    with contextlib.suppress(Exception):
        driver.save_screenshot(str(out_dir / "screenshot.png"))


def run_interaction_probes(driver: Any) -> None:
    """Scroll in steps + EN/NL/FR 'show more' + aria-expanded=false (best-effort)."""
    with contextlib.suppress(Exception):
        for frac in (0.25, 0.5, 0.75, 1.0, 1.0):
            driver.execute_script(
                "window.scrollTo(0, Math.floor(document.body.scrollHeight * arguments[0]));",
                frac,
            )
            time.sleep(0.35)
    with contextlib.suppress(Exception):
        driver.execute_script(
            r"""
            const needles = [
              'show more', 'load more', 'more',
              'meer', 'toon meer',
              'voir plus', 'afficher plus', 'charger plus',
            ];
            const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
            const root = document.querySelector('main') || document.body;
            const els = Array.from(root.querySelectorAll('button, a, [role="button"]'));
            let clicks = 0;
            for (const el of els) {
              if (clicks >= 14) break;
              const t = norm(el.textContent || el.innerText || '');
              if (!t) continue;
              if (!needles.some(n => t.includes(n))) continue;
              try {
                el.scrollIntoView({block:'center'});
                el.click();
                clicks++;
              } catch (e) {}
            }
            const closed = Array.from(root.querySelectorAll('[aria-expanded="false"]')).slice(0, 12);
            for (const el of closed) {
              if (clicks >= 22) break;
              try {
                el.scrollIntoView({block:'center'});
                el.click();
                clicks++;
              } catch (e) {}
            }
            return clicks;
            """
        )
    time.sleep(0.45)


@dataclass
class ScanRow:
    url: str
    verification: str  # yes / partial / no
    direct_ticket_urls: int
    dated_event_urls: int
    subpages_checked: int
    final_ticket_urls: int
    debug_dir: str = ""
    hub_mode: int = 0
    reason: str = ""
    sample_tickets: list[str] = field(default_factory=list)


def _harvest_counts(driver: Any, page_url: str) -> tuple[set[str], dict[str, int]]:
    html = driver.page_source or ""
    cand = tch.harvest_candidate_urls_from_page(driver, html, page_url)
    counts = tch.count_candidates_by_kind(cand)
    return cand, counts


def scan_one_url(
    driver: Any,
    url: str,
    *,
    headed: bool,
    interact: bool,
    debug_dump: bool,
) -> ScanRow:
    u = du.normalize_url(url) or url
    driver.get(u)
    du.wait_for_page_content(driver, headless=not headed)
    time.sleep(0.5 if headed else 0.35)
    if interact:
        run_interaction_probes(driver)
    du.scroll_for_lazy_content(driver)
    with contextlib.suppress(Exception):
        du.try_click_tickets_tab(driver)
    du.expand_main_accordions(driver, max_clicks=14)

    cand0, counts0 = _harvest_counts(driver, u)
    direct_tickets = tch.filter_ticket_urls(cand0)

    tickets, counts = extract_ticket_urls_from_loaded_selenium_page(
        driver, event_url=u, headless_for_hub_wait=not headed
    )
    html1 = driver.page_source or ""
    final_n = len(tickets)
    blocked, reason = _verification_report(driver, html1)

    slug = _safe_slug(u)
    dbg_path = ""
    if debug_dump or final_n == 0:
        ddir = DEBUG_ROOT / slug
        dump_debug_bundle(driver, target_url=u, out_dir=ddir, html=html1)
        dbg_path = str(ddir.relative_to(ROOT))

    title_v, vis_v = _read_title_visible(driver)
    partial = du.looks_like_verification_html(
        html1,
        current_url=str(getattr(driver, "current_url", "") or ""),
        title=title_v,
        visible_text=vis_v[:2000],
    )
    ver = "yes" if blocked else ("partial" if partial else "no")

    subpages = int(counts.get("subpages_checked", 0)) if isinstance(counts, dict) else 0

    if blocked:
        du.log_verification_blocked(
            logging.getLogger("quick_scan"),
            url=u,
            title=title_v,
            current_url=str(getattr(driver, "current_url", "") or ""),
        )

    return ScanRow(
        url=u,
        verification=ver,
        direct_ticket_urls=len(direct_tickets),
        dated_event_urls=int(counts0.get("dated_event_url", 0)),
        subpages_checked=subpages,
        final_ticket_urls=final_n,
        debug_dir=dbg_path,
        hub_mode=int(counts.get("hub_mode", 0)) if isinstance(counts, dict) else 0,
        reason=reason if final_n == 0 else "",
        sample_tickets=list(tickets)[:5],
    )


def investigate_single(
    driver: Any,
    url: str,
    *,
    headed: bool,
    interact: bool,
    debug_dump: bool,
) -> None:
    u = du.normalize_url(url) or url
    driver.get(u)
    du.wait_for_page_content(driver, headless=not headed)
    time.sleep(0.6)
    if interact:
        run_interaction_probes(driver)
    du.scroll_for_lazy_content(driver)
    with contextlib.suppress(Exception):
        du.try_click_tickets_tab(driver)
    du.expand_main_accordions(driver, max_clicks=14)

    html = driver.page_source or ""
    title, vis = _read_title_visible(driver)
    cur = str(getattr(driver, "current_url", "") or "")
    blocked = du.is_blocked_for_discovery(html, title=title, visible_text=vis[:8000], current_url=cur)
    vonly = du.looks_like_verification_html(html, current_url=cur, title=title, visible_text=vis[:8000])

    cand = tch.harvest_candidate_urls_from_page(driver, html, u)
    by_kind = tch.count_candidates_by_kind(cand)
    tickets = sorted(tch.filter_ticket_urls(cand))
    tickets2, _ = extract_ticket_urls_from_loaded_selenium_page(
        driver, event_url=u, headless_for_hub_wait=not headed
    )
    html = driver.page_source or ""
    title2, vis2 = _read_title_visible(driver)
    cur2 = str(getattr(driver, "current_url", "") or "")
    blocked2 = du.is_blocked_for_discovery(html, title=title2, visible_text=vis2[:8000], current_url=cur2)
    vonly2 = du.looks_like_verification_html(html, current_url=cur2, title=title2, visible_text=vis2[:8000])
    merged_final = sorted(set(tickets) | set(tickets2))

    print("\n=== single-url investigation ===", flush=True)
    print(f"url: {u}", flush=True)
    print(f"current_url (after extract): {cur2}", flush=True)
    print(f"title (after extract): {title2!r}", flush=True)
    print(f"verification_blocked (initial page): {blocked}", flush=True)
    print(f"verification_blocked (after extract): {blocked2}", flush=True)
    print(f"looks_like_verification_html (initial): {vonly}", flush=True)
    print(f"looks_like_verification_html (after extract): {vonly2}", flush=True)
    print(f"has __NEXT_DATA__: {du.has_next_data_script(html)}", flush=True)
    print(f"counts_by_kind: {by_kind}", flush=True)
    print(f"harvest_ticket_urls (pattern): {len(tickets)}", flush=True)
    print(f"step2_extract_ticket_urls: {len(tickets2)}", flush=True)
    print("top_20_candidates:", flush=True)
    for c in sorted(cand)[:20]:
        print(f"  [{tch.classify_ticketswap_url(c)}] {c}", flush=True)
    if merged_final:
        print("final_ticket_urls (sample):", flush=True)
        for t in merged_final[:15]:
            print(f"  {t}", flush=True)
    else:
        if blocked2 or vonly2:
            print("reason: verification / bot interstitial — no reliable ticket extraction.", flush=True)
        elif not du.has_next_data_script(html) and len(html) > 10_000:
            print("reason: no __NEXT_DATA__ in HTML — possible shell, blocked response, or non-Next page.", flush=True)
        else:
            print("reason: no ticket URLs matched (UI may need more interaction or different page shape).", flush=True)

    if debug_dump or not merged_final:
        ddir = DEBUG_ROOT / _safe_slug(u)
        dump_debug_bundle(driver, target_url=u, out_dir=ddir, html=html)
        print(f"debug_dir: {ddir}", flush=True)


def _collect_listing_targets(driver, listing_url: str, *, headed: bool, max_targets: int) -> list[str]:
    driver.get(listing_url)
    du.wait_for_page_content(driver, headless=not headed)
    time.sleep(0.75)
    du.scroll_for_lazy_content(driver)
    with contextlib.suppress(Exception):
        du.expand_category_listing_show_more(driver, listing_url, "festival-tickets", max_clicks=1)
    du.scroll_for_lazy_content(driver)
    html = driver.page_source or ""
    cur = str(getattr(driver, "current_url", "") or "")
    title, vis = _read_title_visible(driver)
    if du.is_blocked_for_discovery(html, title=title, visible_text=vis[:8000], current_url=cur):
        print(
            "NOTE\tlisting blocked/verification — trying pipeline live collector.",
            flush=True,
        )
    cands = du.merge_link_candidates(html, driver, listing_url)
    out: list[str] = []
    for h in sorted(cands):
        n = du.normalize_url(h)
        if not n or du.is_ticket_url(n):
            continue
        if du.is_festival_page(n) or du.is_event_page(n):
            if n not in out:
                out.append(n)
        if len(out) >= max_targets:
            break
    if len(out) < max_targets:
        try:
            live = mr._collect_listing_event_urls_live(driver, listing_url, limit_events=max_targets)
            for n in live:
                if n and n not in out:
                    out.append(n)
                if len(out) >= max_targets:
                    break
        except Exception:
            pass
    return out


def _print_table(rows: list[ScanRow]) -> None:
    print("", flush=True)
    print(
        "URL | verification | direct_ticket_urls | dated_event_urls | subpages_checked | final_ticket_urls | debug_dir",
        flush=True,
    )
    print("-" * 140, flush=True)
    for r in rows:
        short = r.url if len(r.url) < 90 else r.url[:87] + "..."
        print(
            f"{short} | {r.verification} | {r.direct_ticket_urls} | {r.dated_event_urls} | {r.subpages_checked} | {r.final_ticket_urls} | {r.debug_dir}",
            flush=True,
        )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--headed", action="store_true", default=False, help="Explicit headed (default is headed unless --headless).")
    p.add_argument("--headless", action="store_true", default=False)
    p.add_argument("--slow", action="store_true", help="Longer page-ready / post-load waits (STEP2_SLOW_* env).")
    p.add_argument("--debug-dump", action="store_true", help="Always save tmp/ticketswap_debug/<slug>/ for each scanned URL.")
    p.add_argument("--interact", action="store_true", help="Enable scroll/load-more interaction rounds.")
    p.add_argument("--no-interact", action="store_true", help="Disable interaction rounds.")
    p.add_argument(
        "--manual-verification",
        action="store_true",
        help="Pause for Enter when TicketSwap shows verification (headed only).",
    )
    p.add_argument("--profile-dir", type=str, default="", help="Chrome user-data-dir (overrides TICKETSWAP_PROFILE_DIR).")
    p.add_argument(
        "--anonymous-profile",
        action="store_true",
        help="Ephemeral Chrome profile (same as --fresh-profile).",
    )
    p.add_argument(
        "--fresh-profile",
        action="store_true",
        help="Ephemeral Chrome profile (no persisted user-data-dir).",
    )
    p.add_argument("--paris-events", type=int, default=4)
    p.add_argument("--skip-eighteen", action="store_true")
    p.add_argument("--single-url", type=str, default="", help="Investigate one URL and print a compact report.")
    args = p.parse_args()
    headed = bool(du.resolve_discovery_headed(args))
    args.headed = headed

    if args.slow:
        du.apply_step2_slow_timings()
    _cfg.STEP2_USE_ANONYMOUS_PROFILE = bool(args.anonymous_profile or args.fresh_profile)
    _cfg.STEP2_DRIVER_USER_DATA_DIR = (
        str(Path(args.profile_dir.strip()).resolve()) if str(args.profile_dir or "").strip() else None
    )
    if args.no_interact:
        _cfg.STEP2_INTERACT_ENABLED = False
    elif args.interact:
        _cfg.STEP2_INTERACT_ENABLED = True
    else:
        _cfg.STEP2_INTERACT_ENABLED = bool(headed)
    _cfg.STEP2_MANUAL_VERIFICATION_PRESS_ENTER = bool(args.manual_verification)
    _cfg.warn_if_step2_profile_missing(logging.getLogger("quick_scan"))
    if not headed:
        _cfg.warn_step2_headless_without_trusted_profile(logging.getLogger("quick_scan"))

    prof = _cfg.STEP2_DRIVER_USER_DATA_DIR or _cfg.persistent_browser_user_data_dir() or (
        "anonymous" if _cfg.STEP2_USE_ANONYMOUS_PROFILE else "none"
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger().setLevel(logging.INFO)
    print(
        f"BROWSER_MODE headed={str(bool(headed)).lower()} profile_dir={prof} "
        f"interact={str(bool(_cfg.STEP2_INTERACT_ENABLED)).lower()} slow={str(bool(getattr(_cfg, 'STEP2_SLOW_MODE', False))).lower()}",
        flush=True,
    )

    driver = du.new_driver(headless=not headed)
    driver.set_page_load_timeout(90)
    rows: list[ScanRow] = []

    try:
        if args.single_url.strip():
            investigate_single(
                driver,
                args.single_url.strip(),
                headed=headed,
                interact=bool(_cfg.STEP2_INTERACT_ENABLED),
                debug_dump=bool(args.debug_dump),
            )
            return 0

        if not args.skip_eighteen:
            print("\n=== 18 targets ===\n", flush=True)
            for url in EIGHTEEN_URLS:
                try:
                    row = scan_one_url(
                        driver,
                        url,
                        headed=headed,
                        interact=bool(_cfg.STEP2_INTERACT_ENABLED),
                        debug_dump=bool(args.debug_dump),
                    )
                    rows.append(row)
                    st = "SUCCESS" if row.final_ticket_urls > 0 else "FAIL"
                    print(f"{st}\tickets={row.final_ticket_urls}\tver={row.verification}\t{url}", flush=True)
                except Exception as e:
                    print(f"ERROR\t{url}\t{e!r}", flush=True)
                    rows.append(
                        ScanRow(
                            url=url,
                            verification="?",
                            direct_ticket_urls=0,
                            dated_event_urls=0,
                            subpages_checked=0,
                            final_ticket_urls=0,
                            reason=repr(e),
                        )
                    )

        listing = _load_paris_listing_from_cache()
        if int(args.paris_events) > 0:
            print("\n=== Paris listing sample ===\n", flush=True)
            print(f"listing={listing}", flush=True)
            targets = _collect_listing_targets(driver, listing, headed=headed, max_targets=int(args.paris_events))
            if not targets:
                print("FAIL\tno targets from listing", flush=True)
            for url in targets:
                try:
                    row = scan_one_url(
                        driver,
                        url,
                        headed=headed,
                        interact=bool(_cfg.STEP2_INTERACT_ENABLED),
                        debug_dump=bool(args.debug_dump),
                    )
                    rows.append(row)
                    st = "SUCCESS" if row.final_ticket_urls > 0 else "FAIL"
                    print(f"{st}\tickets={row.final_ticket_urls}\t{url}", flush=True)
                except Exception as e:
                    print(f"ERROR\t{url}\t{e!r}", flush=True)

        if rows:
            _print_table(rows)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()
        du.restore_step2_slow_timings()
        _cfg.STEP2_USE_ANONYMOUS_PROFILE = False
        _cfg.STEP2_DRIVER_USER_DATA_DIR = None
        _cfg.STEP2_INTERACT_ENABLED = False
        _cfg.STEP2_MANUAL_VERIFICATION_PRESS_ENTER = False
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
