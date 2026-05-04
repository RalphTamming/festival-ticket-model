"""
Relaxed TicketSwap festival ticket URL discovery (pattern-only).

Used by:
- `discover_urls.py` for DOM/HTML extraction and hub expansion
- Local HTTP tests (`tests/test_relaxed_festival_ticket_urls.py`) without Selenium/DB

Rules (festival-tickets only):
- Path contains ``/festival-tickets/`` and is not a hub ``/festival-tickets/a/...``
- Path ends with ``/<at least 5 digits>`` (numeric listing / ticket-type id)
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse, urlunparse

# Raw HTML / JSON scan (absolute URLs only, per product spec).
RAW_FESTIVAL_TICKET_URL_RE = re.compile(
    r"https://www\.ticketswap\.com/festival-tickets/[^\s\"<>]+/\d{5,}",
    re.IGNORECASE,
)

_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)


def _strip_ticketswap_locale_path(path: str) -> str:
    p = path or "/"
    m = re.match(r"^/([a-z]{2}|[a-z]{2}-[a-z]{2})(/festival-tickets(?:/.*)?)$", p, re.I)
    if m:
        return m.group(2)
    return p


def normalize_ticketswap_url(url: str, base: str = "https://www.ticketswap.com") -> Optional[str]:
    """Minimal normalizer (mirrors ``discover_urls.normalize_url`` for TicketSwap)."""
    if not url:
        return None
    absolute = urljoin(base, url.strip())
    p = urlparse(absolute)
    if not p.netloc:
        return None
    if "ticketswap.com" not in p.netloc.lower():
        return None
    scheme = "https"
    netloc = "www.ticketswap.com"
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    path = _strip_ticketswap_locale_path(path)
    return urlunparse((scheme, netloc, path, "", p.query or "", ""))


def path_matches_relaxed_festival_ticket(path: str) -> bool:
    p = (path or "").rstrip("/")
    pl = p.lower()
    if "/festival-tickets/a/" in pl:
        return False
    if "/festival-tickets/" not in pl:
        return False
    return bool(re.search(r"/festival-tickets/.+/\d{5,}$", p, re.IGNORECASE))


def is_relaxed_festival_ticket_url(url: str) -> bool:
    n = normalize_ticketswap_url(url)
    if not n:
        return False
    return path_matches_relaxed_festival_ticket(urlparse(n).path or "")


def event_base_path_from_relaxed_festival_ticket(path: str) -> Optional[str]:
    """
    Map ``/festival-tickets/<event>/(<type>/)?<id>`` -> ``/festival-tickets/<event>``.
    """
    p = (path or "").rstrip("/")
    if not path_matches_relaxed_festival_ticket(p):
        return None
    parts = [x for x in p.split("/") if x]
    if len(parts) < 3 or parts[0] != "festival-tickets":
        return None
    if not parts[-1].isdigit() or len(parts[-1]) < 5:
        return None
    parts = parts[:-1]
    if len(parts) < 2:
        return None
    return "/" + "/".join(parts[:2])


def extract_anchor_hrefs_from_html(html: str) -> list[str]:
    if not html:
        return []
    return [m.group(1) for m in _HREF_RE.finditer(html)]


def extract_relaxed_festival_ticket_urls_from_html(html: str, *, base_url: str) -> set[str]:
    """
    1) Every ``<a href=...>`` normalized to TicketSwap https
    2) Keep relaxed festival ticket paths
    3) Regex fallback on raw HTML for absolute ``https://www.ticketswap.com/festival-tickets/.../NNNNN``
    """
    out: set[str] = set()
    if not html:
        return out
    base = normalize_ticketswap_url(base_url) or base_url
    for raw in extract_anchor_hrefs_from_html(html):
        n = normalize_ticketswap_url(raw, base=base)
        if n and is_relaxed_festival_ticket_url(n):
            out.add(n)
    for m in RAW_FESTIVAL_TICKET_URL_RE.finditer(html):
        n = normalize_ticketswap_url(m.group(0))
        if n:
            out.add(n)
    return out


def is_hub_child_event_url(url: str) -> bool:
    """
    A dated / hashed event page: ``/festival-tickets/<one-slug>`` (not ``a/<hub>``).
    """
    n = normalize_ticketswap_url(url)
    if not n:
        return False
    path = (urlparse(n).path or "").rstrip("/")
    parts = [x for x in path.split("/") if x]
    if len(parts) != 2 or parts[0] != "festival-tickets":
        return False
    if parts[1] == "a":
        return False
    if path_matches_relaxed_festival_ticket(path):
        return False
    return True


def extract_hub_child_event_urls_from_html(
    html: str,
    *,
    hub_url: str,
    extra_urls: Optional[Iterable[str]] = None,
) -> set[str]:
    """Deep event URLs linked from a festival hub page."""
    out: set[str] = set()
    base = normalize_ticketswap_url(hub_url) or hub_url
    for raw in extract_anchor_hrefs_from_html(html or ""):
        n = normalize_ticketswap_url(raw, base=base)
        if n and is_hub_child_event_url(n):
            out.add(n)
    if extra_urls:
        for u in extra_urls:
            n = normalize_ticketswap_url(u, base=base)
            if n and is_hub_child_event_url(n):
                out.add(n)
    for m in re.finditer(
        r'https://www\.ticketswap\.com/festival-tickets/(?!a/)([^"?\s#<>]+)',
        html or "",
        re.I,
    ):
        tail = m.group(1).rstrip("/")
        if "/" in tail:
            continue
        n = normalize_ticketswap_url(f"/festival-tickets/{tail}", base=base)
        if n and is_hub_child_event_url(n):
            out.add(n)
    return out


def collect_festival_ticket_urls_with_requests(
    event_url: str,
    *,
    session: Any,
    max_hub_children: int = 10,
    timeout: float = 25.0,
) -> list[str]:
    """
    Fetch ``event_url`` with ``requests``; for hub pages follow linked child events
    (same pattern as Selenium hub expansion) and merge relaxed ticket URLs.
    """
    if session is None or not hasattr(session, "get"):
        raise TypeError("session must implement get(url, timeout=..., headers=...)")

    root = normalize_ticketswap_url(event_url) or event_url
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,nl;q=0.8",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    found: set[str] = set()

    def merge_page(html: str, base: str, *, restrict_event_base: Optional[str]) -> None:
        chunk = extract_relaxed_festival_ticket_urls_from_html(html, base_url=base)
        if restrict_event_base is None:
            found.update(chunk)
            return
        rb = (normalize_ticketswap_url(restrict_event_base) or restrict_event_base).rstrip("/")
        for u in chunk:
            eb_path = event_base_path_from_relaxed_festival_ticket(urlparse(u).path or "")
            if not eb_path:
                continue
            efull = (normalize_ticketswap_url(eb_path) or "").rstrip("/")
            if efull == rb:
                found.add(u)

    r = session.get(root, timeout=timeout, headers=headers)
    r.raise_for_status()
    html = r.text
    path = urlparse(root).path or ""
    parts = [x for x in path.rstrip("/").split("/") if x]
    is_hub = len(parts) >= 2 and parts[0] == "festival-tickets" and parts[1] == "a"

    if is_hub:
        merge_page(html, root, restrict_event_base=None)
        children = sorted(extract_hub_child_event_urls_from_html(html, hub_url=root))[: int(max_hub_children)]
        for child in children:
            try:
                rc = session.get(child, timeout=timeout, headers=headers)
                if rc.status_code != 200:
                    continue
                merge_page(rc.text, child, restrict_event_base=None)
            except OSError:
                continue
    else:
        merge_page(html, root, restrict_event_base=root)

    return sorted(found)
