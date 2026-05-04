"""
Harvest and classify TicketSwap URLs from Selenium + HTML (diagnostics / STEP2 support).

No DB. Intended for ``scripts/quick_scan_step2_urls.py`` and similar local probes.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal
from urllib.parse import urlparse

from discovery import discover_urls as du
from discovery import ticketswap_relaxed_extract as tsx

UrlKind = Literal["ticket_url", "dated_event_url", "hub_url", "listing_or_other"]

_REL_ABS = re.compile(
    r"https://www\.ticketswap\.com(?:/[a-z]{2}(?:-[a-z]{2})?)?/[^\s\"'<>]+",
    re.I,
)
_REL_PATH = re.compile(
    r'["\'](/(?:festival-tickets|concert-tickets|club-tickets|sports-tickets)[^"\'\s<>]*)["\']',
    re.I,
)


def classify_ticketswap_url(url: str) -> UrlKind:
    n = du.normalize_url(url) or url
    if not n or "ticketswap.com" not in (urlparse(n).netloc or "").lower():
        return "listing_or_other"
    path = (urlparse(n).path or "").rstrip("/")
    q = urlparse(n).query or ""
    if du.is_festival_page(n):
        return "hub_url"
    if du.is_ticket_url(n) or tsx.is_relaxed_festival_ticket_url(n):
        return "ticket_url"
    if du.is_event_page(n) and du.is_plausible_event_page(n):
        return "dated_event_url"
    if tsx.is_hub_child_event_url(n):
        return "dated_event_url"
    if path.rstrip("/") in ("/festival-tickets", "") and "festival-tickets" in (path + "?" + q).lower():
        return "listing_or_other"
    if "/festival-tickets/" in path and path.count("/") >= 2:
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 2 and parts[0] == "festival-tickets" and parts[1] != "a":
            if not path.endswith("/") and not re.search(r"/\d{5,}$", path):
                return "dated_event_url"
    return "listing_or_other"


def _urls_from_json_ld(html: str, base: str) -> set[str]:
    out: set[str] = set()
    if not html:
        return out
    for m in re.finditer(
        r'<script[^>]+type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    ):
        blob = m.group(1).strip()
        if not blob:
            continue
        try:
            data = json.loads(blob)
        except Exception:
            continue
        stack: list[Any] = [data]
        while stack:
            x = stack.pop()
            if isinstance(x, str):
                if "ticketswap.com" in x or x.startswith("/"):
                    n = du.normalize_url(x, base=base)
                    if n:
                        out.add(n)
            elif isinstance(x, dict):
                for k in ("url", "@id", "sameAs", "ticketUrl", "offers"):
                    v = x.get(k)
                    if isinstance(v, str):
                        if "ticketswap.com" in v or v.startswith("/"):
                            n = du.normalize_url(v, base=base)
                            if n:
                                out.add(n)
                    elif isinstance(v, (list, dict)):
                        stack.append(v)
                stack.extend(x.values())
            elif isinstance(x, list):
                stack.extend(x)
    return out


def _urls_from_script_json(html: str, base: str) -> set[str]:
    out: set[str] = set()
    if not html:
        return out
    for m in re.finditer(
        r'<script[^>]+type\s*=\s*["\']application/json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    ):
        blob = m.group(1)
        for sub in _REL_ABS.finditer(blob or ""):
            n = du.normalize_url(sub.group(0))
            if n:
                out.add(n)
        for sub in _REL_PATH.finditer(blob or ""):
            n = du.normalize_url(sub.group(1), base=base)
            if n:
                out.add(n)
    return out


def _urls_from_inline_scripts(html: str, base: str) -> set[str]:
    out: set[str] = set()
    if not html:
        return out
    for m in re.finditer(r"<script([^>]*)>(.*?)</script>", html, re.I | re.DOTALL):
        attrs, inner = m.group(1) or "", m.group(2) or ""
        al = attrs.lower()
        if "application/ld+json" in al or "application/json" in al:
            continue
        if len(inner) > 500_000:
            inner = inner[:500_000]
        for sub in _REL_ABS.finditer(inner):
            n = du.normalize_url(sub.group(0))
            if n:
                out.add(n)
        for sub in _REL_PATH.finditer(inner):
            n = du.normalize_url(sub.group(1), base=base)
            if n:
                out.add(n)
    return out


def _urls_from_performance_api(driver: Any, base: str) -> set[str]:
    out: set[str] = set()
    try:
        raw = driver.execute_script(
            """
            try {
              const a = performance.getEntriesByType('resource') || [];
              return a.map(e => String(e.name || '')).filter(Boolean).slice(0, 400);
            } catch (e) { return []; }
            """
        )
        if not isinstance(raw, list):
            return out
        for name in raw:
            if "ticketswap.com" not in str(name).lower():
                continue
            n = du.normalize_url(str(name), base=base)
            if n:
                out.add(n)
    except Exception:
        pass
    return out


def _urls_from_browser_logs(driver: Any, base: str) -> set[str]:
    out: set[str] = set()
    for log_type in ("browser",):
        try:
            entries = driver.get_log(log_type)
        except Exception:
            continue
        for e in entries[:200]:
            msg = str((e or {}).get("message") or "")
            for sub in _REL_ABS.finditer(msg):
                n = du.normalize_url(sub.group(0))
                if n:
                    out.add(n)
    return out


def harvest_candidate_urls_from_page(driver: Any, html: str, base_url: str) -> set[str]:
    """
    Broad harvest: DOM merge, relaxed extract, Next data, JSON-LD, scripts, regex on HTML,
    performance resource names, browser console log lines.
    """
    base = du.normalize_url(base_url) or base_url
    h = html or ""
    found: set[str] = set()
    found |= du.merge_link_candidates(h, driver, base)
    found |= tsx.extract_relaxed_festival_ticket_urls_from_html(h, base_url=base)
    found |= du.extract_next_data_link_candidates(h, base_url=base)
    found |= _urls_from_json_ld(h, base)
    found |= _urls_from_script_json(h, base)
    found |= _urls_from_inline_scripts(h, base)
    for m in _REL_ABS.finditer(h):
        n = du.normalize_url(m.group(0))
        if n:
            found.add(n)
    for m in _REL_PATH.finditer(h):
        n = du.normalize_url(m.group(1), base=base)
        if n:
            found.add(n)
    found |= _urls_from_performance_api(driver, base)
    found |= _urls_from_browser_logs(driver, base)
    return {u for u in found if u}


def count_candidates_by_kind(urls: set[str]) -> dict[UrlKind, int]:
    counts: dict[UrlKind, int] = {
        "ticket_url": 0,
        "dated_event_url": 0,
        "hub_url": 0,
        "listing_or_other": 0,
    }
    for u in urls:
        counts[classify_ticketswap_url(u)] += 1
    return counts


def filter_ticket_urls(urls: set[str]) -> set[str]:
    out: set[str] = set()
    for u in urls:
        n = du.normalize_url(u) or u
        if du.is_ticket_url(n):
            out.add(n)
    return out
