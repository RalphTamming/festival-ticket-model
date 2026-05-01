"""
STEP 2: event page -> ticket-type URL discovery (Playwright-first).

Goal: given an event URL like:
  https://www.ticketswap.com/festival-tickets/<event-slug>
discover deep ticket URLs like:
  https://www.ticketswap.com/festival-tickets/<event-slug>/<ticket-type-slug>/<numeric-id>

This script is focused and debuggable:
- tries static extraction (existing discover_urls logic)
- tries embedded JSON extraction (existing discover_urls logic)
- then uses Playwright network interception to capture JSON/XHR/GraphQL responses and extract ticket types
- can fall back to DB when available (explicitly logged)

Usage:
  python step2_discover_ticket_urls.py --event-url "<EVENT_URL>" --headed --debug
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

import config
import db as dbmod
from discovery import discover_urls as du


TICKET_PATH_RE = re.compile(
    r"(/(?:festival-tickets|concert-tickets|club-tickets|sports-tickets)/[^/]+/[^/]+/\d+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Step2Result:
    event_url: str
    status: str  # ok | blocked | no_data | error
    verification: bool
    strategy: str  # static | embedded_json | network | db_fallback | none
    ticket_urls: list[str]
    debug_dir: Optional[str] = None


def _ensure_debug_dir() -> Path:
    d = Path(config.DEBUG_DIR) / "step2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_key_from_event_url(event_url: str) -> str:
    n = du.normalize_url(event_url) or event_url
    # best effort: keep last path segment after category prefix
    p = n
    for pref in ("festival-tickets", "concert-tickets", "club-tickets", "sports-tickets"):
        if f"/{pref}/" in p:
            p = p.split(f"/{pref}/", 1)[-1]
            break
    p = re.sub(r"[^a-zA-Z0-9_-]+", "_", p)[:80] or "event"
    return p


def _guess_hub_slug_from_event_url(event_url: str) -> str:
    """
    Heuristic: event slugs often look like "<hub-slug>-2026-...".
    Return the prefix before the first "-20xx-" year segment when present.
    """
    n = du.normalize_url(event_url) or event_url
    slug = n.split("/festival-tickets/")[-1].strip("/").split("?", 1)[0]
    m = re.match(r"^(.*?)-(19|20)\d{2}-", slug)
    if m:
        return m.group(1).strip("-")
    # fallback: first 2 hyphen tokens (works for awakenings-upclose)
    parts = [p for p in slug.split("-") if p]
    return "-".join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else "")


def _is_verification_text(text: str) -> bool:
    t = (text or "").lower()
    return "unable to verify" in t or "verifying" in t


def _looks_like_404(text: str) -> bool:
    t = (text or "").lower()
    return "hmm, 404" in t or "we’re a bit lost" in t or "we're a bit lost" in t or "couldn’t find that page" in t or "couldn't find that page" in t


def _extract_ticket_urls_from_any_json(obj: Any, *, event_url: str) -> set[str]:
    """
    Generic JSON walker that searches for:
    - deep ticket paths (/festival-tickets/.../.../123)
    - EventType nodes with (slug,id) where id decodes to EventType:<num> or is numeric
    """

    out: set[str] = set()
    base = du.normalize_url(event_url) or event_url

    def add_path(p: str) -> None:
        n = du.normalize_url(p, base=base)
        if n and du.is_ticket_url(n) and (du.event_url_from_ticket_url(n) == du.normalize_url(event_url)):
            out.add(n)

    def visit(x: Any) -> None:
        if x is None:
            return
        if isinstance(x, str):
            for m in TICKET_PATH_RE.finditer(x):
                add_path(m.group(1))
            return
        if isinstance(x, list):
            for it in x:
                visit(it)
            return
        if isinstance(x, dict):
            # common patterns: {"slug": "...-tickets", "id": "..."} or {"id": 123, "slug": "..."}
            slug = x.get("slug") if isinstance(x.get("slug"), str) else None
            _id = x.get("id")
            if slug and isinstance(_id, str) and "RXZlbnRUeXBl" in _id:
                # base64-decode EventType id -> "EventType:<num>"
                try:
                    decoded = base64.b64decode(_id).decode("utf-8", errors="ignore")
                    tail = decoded.split(":")[-1].strip()
                    if tail.isdigit():
                        add_path(f"{event_url.rstrip('/')}/{slug}/{tail}")
                except Exception:
                    pass
            if slug and isinstance(_id, (str, int)):
                # try numeric id
                num = None
                if isinstance(_id, int):
                    num = str(_id)
                else:
                    # sometimes "EventType:123" or base64; try last colon chunk
                    tail = _id.split(":")[-1].strip()
                    if tail.isdigit():
                        num = tail
                if num and slug and slug.endswith(("tickets", "ticket")):
                    add_path(f"{event_url.rstrip('/')}/{slug}/{num}")

            for v in x.values():
                visit(v)
            return

    visit(obj)
    return out


def discover_ticket_urls_from_event_playwright(
    event_url: str,
    *,
    headed: bool,
    debug: bool,
    db_fallback: bool,
) -> Step2Result:
    ev = du.normalize_url(event_url) or event_url
    debug_dir = None
    if debug:
        root = _ensure_debug_dir()
        sub = root / _safe_key_from_event_url(ev)
        sub.mkdir(parents=True, exist_ok=True)
        debug_dir = str(sub.resolve())

    # Strategy A/B/C: Playwright-first (with persistent Chrome profile when enabled).
    with sync_playwright() as p:
        udd = config.persistent_browser_user_data_dir()
        browser = None
        try:
            if udd:
                # Reuse the same Chrome profile as Selenium runs. This dramatically helps with TicketSwap verification.
                context = p.chromium.launch_persistent_context(
                    user_data_dir=udd,
                    headless=not headed,
                    channel="chrome",
                    args=[
                        f"--profile-directory={config.BROWSER_PROFILE_NAME}",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-infobars",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-notifications",
                        "--disable-popup-blocking",
                        "--lang=nl-NL",
                        "--window-size=1920,1080",
                    ],
                )
                page = context.new_page()
            else:
                browser = p.chromium.launch(headless=not headed, channel="chrome")
                context = browser.new_context()
                page = context.new_page()
        except PlaywrightError:
            # Common when the persistent Chrome profile is already in use.
            # Fall back to a non-persistent context so discovery can still proceed.
            browser = p.chromium.launch(headless=not headed, channel="chrome")
            context = browser.new_context()
            page = context.new_page()

        captured: list[dict[str, Any]] = []
        json_hits: list[str] = []
        xhr_fetch_urls: list[str] = []

        def on_response(resp) -> None:
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                url = resp.url
                try:
                    rt = resp.request.resource_type
                except Exception:
                    rt = ""
                if rt in ("xhr", "fetch"):
                    xhr_fetch_urls.append(url)

                # We care about JSON/GraphQL-ish responses; many are text/plain but contain JSON.
                if not ("json" in ct or "graphql" in ct or "application/octet-stream" in ct or "text/plain" in ct):
                    # still allow obvious API endpoints
                    if not any(k in url.lower() for k in ("graphql", "api", "_next/data")):
                        return

                body = resp.text()
                if not body or len(body) < 2:
                    return
                if body[0] not in "{[":
                    return
                low = body.lower()
                is_graphql_public = "/api/graphql/public" in url.lower()
                # quick keyword filter so we don't store huge irrelevant JSON
                if not is_graphql_public and not any(
                    k in low
                    for k in (
                        "eventtype",
                        "eventtypes",
                        "tickettype",
                        "tickettypes",
                        "listingcount",
                        "wantedcount",
                        "/festival-tickets/",
                    )
                ):
                    return
                json_hits.append(url)
                captured.append({"url": url, "body": body[:200_000]})
            except Exception:
                return

        page.on("response", on_response)

        try:
            page.goto(ev, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            body_text = ""
            try:
                body_text = page.locator("body").inner_text(timeout=2000)
            except Exception:
                body_text = ""
            verification = _is_verification_text(body_text) or du.looks_like_verification(page.title() or "")
            is_404 = _looks_like_404(body_text)

            html = page.content()
            if du.looks_like_verification(html) and du.is_blocked_for_discovery(html):
                if debug and debug_dir:
                    Path(debug_dir, "verification.html").write_text(html, encoding="utf-8")
                # If headed, allow manual solve once in the persistent profile.
                if headed:
                    page.wait_for_timeout(int(getattr(config, "MANUAL_VERIFY_WAIT_SECONDS", 90)) * 1000)
                    html = page.content()
                    if du.looks_like_verification(html) and du.is_blocked_for_discovery(html):
                        return Step2Result(ev, "blocked", True, "none", [], debug_dir=debug_dir)
                else:
                    return Step2Result(ev, "blocked", True, "none", [], debug_dir=debug_dir)

            if is_404:
                if debug and debug_dir:
                    Path(debug_dir, "page.html").write_text(html, encoding="utf-8")
                return Step2Result(ev, "no_data", bool(verification), "none", [], debug_dir=debug_dir)

            # Static/embedded JSON from HTML
            static_candidates = du.extract_ticket_urls_from_page_text(html, base_url=ev) | du.extract_ticket_urls_from_eventtype_cache(html, base_url=ev)
            static_ticket_urls = sorted({u for u in static_candidates if du.is_ticket_url(u)})
            if static_ticket_urls:
                if debug and debug_dir:
                    Path(debug_dir, "page.html").write_text(html, encoding="utf-8")
                return Step2Result(ev, "ok", bool(verification), "embedded_json", static_ticket_urls, debug_dir=debug_dir)

            # Strategy C: network interception
            page.wait_for_timeout(2500)
            found: set[str] = set()
            snippets: list[dict[str, Any]] = []
            for item in captured:
                u = item["url"]
                body = item["body"]
                # attempt JSON parse
                try:
                    obj = json.loads(body)
                except Exception:
                    obj = None
                if obj is not None:
                    found |= _extract_ticket_urls_from_any_json(obj, event_url=ev)
                    lowb = body.lower()
                    if "/api/graphql/public" in u.lower() or any(k in lowb for k in ("eventtype", "tickettype", "tickettypes")):
                        snippets.append({"url": u, "sample": body[:4000]})
                else:
                    # string scan fallback
                    for m in TICKET_PATH_RE.finditer(body):
                        found.add(du.normalize_url(m.group(1), base=ev) or m.group(1))

            found = {u for u in found if du.is_ticket_url(u)}
            ticket_urls = sorted(found)

            if debug and debug_dir:
                Path(debug_dir, "page.html").write_text(html, encoding="utf-8")
                Path(debug_dir, "network_urls.txt").write_text("\n".join(json_hits), encoding="utf-8")
                Path(debug_dir, "xhr_fetch_urls.txt").write_text("\n".join(xhr_fetch_urls), encoding="utf-8")
                Path(debug_dir, "network_snippets.json").write_text(json.dumps(snippets, ensure_ascii=False, indent=2), encoding="utf-8")
                try:
                    page.screenshot(path=str(Path(debug_dir, "screenshot.png")), full_page=True)
                except Exception:
                    pass

            if ticket_urls:
                return Step2Result(ev, "ok", bool(verification), "network", ticket_urls, debug_dir=debug_dir)

            # Strategy D: DB fallback
            if db_fallback and Path(config.DB_PATH).exists():
                try:
                    conn = dbmod.connect(config.DB_PATH)
                    try:
                        rows = conn.execute(
                            "select ticket_url from ticket_urls where event_url = ? order by ticket_url",
                            (ev,),
                        ).fetchall()
                        from_db = [r[0] for r in rows if r and r[0]]
                    finally:
                        conn.close()
                except Exception:
                    from_db = []
                if from_db:
                    return Step2Result(ev, "ok", bool(verification), "db_fallback", from_db, debug_dir=debug_dir)

            # Strategy E: hub fallback (bounded) — only when everything else fails
            hub_slug = _guess_hub_slug_from_event_url(ev)
            if hub_slug:
                hub_url = du.normalize_url(f"/festival-tickets/a/{hub_slug}")
            else:
                hub_url = None
            if hub_url:
                driver = None
                try:
                    # Use the existing Selenium/UC logic because it's already tuned for TicketSwap.
                    driver = du.new_driver(headless=not headed)
                    driver.get(hub_url)
                    html_h = du.wait_for_page_content(driver, headless=bool(not headed))
                    if not du.is_blocked_for_discovery(html_h):
                        cands = du.gather_hub_page_candidates(driver, hub_url)
                        tus = sorted(
                            {
                                u
                                for u in cands
                                if du.is_ticket_url(u) and (du.event_url_from_ticket_url(u) == ev)
                            }
                        )
                    else:
                        tus = []
                    if debug and debug_dir:
                        Path(debug_dir, "hub_fallback_url.txt").write_text(hub_url, encoding="utf-8")
                        Path(debug_dir, "hub_fallback_tickets.txt").write_text("\n".join(tus), encoding="utf-8")
                    if tus:
                        return Step2Result(ev, "ok", bool(verification), "hub_fallback", tus, debug_dir=debug_dir)
                except Exception:
                    pass
                finally:
                    try:
                        if driver is not None:
                            driver.quit()
                    except Exception:
                        pass

            return Step2Result(ev, "no_data", bool(verification), "none", [], debug_dir=debug_dir)
        finally:
            context.close()
            if browser is not None:
                browser.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="STEP 2: discover deep TicketSwap ticket URLs from an event page.")
    p.add_argument("--event-url", required=True)
    p.add_argument("--headed", action="store_true", default=False, help="Run non-headless (recommended if verification appears).")
    p.add_argument("--debug", action="store_true", default=False, help="Write debug artifacts to debug/step2/")
    p.add_argument("--db-fallback", action="store_true", default=True, help="Allow fallback to ticketswap.db if URLs exist.")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    res = discover_ticket_urls_from_event_playwright(
        args.event_url,
        headed=bool(args.headed),
        debug=bool(args.debug),
        db_fallback=bool(args.db_fallback),
    )
    print(f"event_url: {res.event_url}")
    print(f"status: {res.status}")
    print(f"verification_detected: {res.verification}")
    print(f"strategy: {res.strategy}")
    print(f"ticket_urls_found: {len(res.ticket_urls)}")
    if res.debug_dir:
        print(f"debug_dir: {res.debug_dir}")
    for u in res.ticket_urls:
        print(u)
    return 0 if res.status == "ok" and res.ticket_urls else (2 if res.status == "blocked" else 1)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

