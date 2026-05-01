"""
Scrape one TicketSwap ticket-type price page and return structured snapshot data.

This file contains the "best working" logic adapted from your previous `scraper.py`:
- Selenium + undetected-chromedriver for reliability
- BeautifulSoup parsing for listings/prices
- Listing fingerprinting for liquidity tracking (best-effort)
- Batch runs: `market_scrape_session` + `scrape_market_with_driver` reuse one browser (persistent profile + login).

CLI:
  python scrape_market.py --url "<ticketswap ticket url>"
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import urljoin, urlparse, urlunparse

import undetected_chromedriver as uc
from bs4 import BeautifulSoup

import config
from discovery import discover_urls as discover_urls_mod


# Keep in sync with discovery.discover_urls.SUPPORTED_CATEGORY_PREFIXES
TICKET_URL_RE = re.compile(
    r"^/(?P<category>festival-tickets|concert-tickets|club-tickets|sports-tickets)/(?P<event_slug>[^/]+)/(?P<ticket_type_slug>[^/]+)/(?P<numeric_id>\d+)(?:[/?#].*)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ListingSnapshot:
    listing_fingerprint: str
    seller_hint: Optional[str]
    quantity: Optional[int]
    price_per_ticket: Optional[float]
    currency: Optional[str]
    raw_text: str
    listing_href: Optional[str] = None


@dataclass(frozen=True)
class MarketSnapshot:
    ticket_url: str
    scraped_at_utc: datetime
    status: str
    error_message: Optional[str] = None

    event_name: Optional[str] = None
    event_url: Optional[str] = None
    venue: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    event_date_local: Optional[str] = None
    ticket_type_label: Optional[str] = None

    face_value: Optional[float] = None
    current_primary_price: Optional[float] = None
    primary_availability: Optional[str] = None
    sold_out_flag: Optional[bool] = None
    official_resale_restriction_flag: Optional[bool] = None

    currency: Optional[str] = None
    listing_count: Optional[int] = None
    available_count: Optional[int] = None
    wanted_count: Optional[int] = None
    sold_count: Optional[int] = None
    lowest_ask: Optional[float] = None
    highest_ask: Optional[float] = None
    median_ask: Optional[float] = None
    average_ask: Optional[float] = None

    new_listings_since_prev: Optional[int] = None
    removed_listings_since_prev: Optional[int] = None
    estimated_sale_speed_listings_per_hour: Optional[float] = None

    listings: list[ListingSnapshot] = field(default_factory=list)
    raw_debug: dict[str, Any] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _strip_ticketswap_locale_path(path: str) -> str:
    """Same rules as discover_urls: /nl/festival-tickets/... -> /festival-tickets/..."""
    p = path or "/"
    m = re.match(r"^/([a-z]{2}|[a-z]{2}-[a-z]{2})(/festival-tickets(?:/.*)?)$", p, re.I)
    if m:
        return m.group(2)
    return p


def normalize_url(url: str, base: str = "https://www.ticketswap.com") -> Optional[str]:
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


def ticket_type_from_ticket_url(ticket_url: str) -> tuple[Optional[str], Optional[str]]:
    n = normalize_url(ticket_url)
    if not n:
        return None, None
    p = urlparse(n)
    m = TICKET_URL_RE.match(p.path or "")
    if not m:
        return None, None
    slug = m.group("ticket_type_slug")
    label = " ".join(w.capitalize() for w in slug.replace("-", " ").split()) if slug else None
    return slug or None, label or None


LOGGER = logging.getLogger("ticketswap.scrape")

# Some environments (notably Selenium + certain fonts/locales) can surface the Euro symbol as U+FFFD "�".
_PRICE_RE = re.compile(r"([€$£\uFFFD])\s*([0-9][0-9\.\s,]*)")
_EVENT_DATETIME_RE = re.compile(
    r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{1,2}:\d{2}\s*[AP]M)\b",
    re.I,
)
_STATS_RE = re.compile(r"(\d+)\s+available\s*[•·\uFFFD]\s*(\d+)\s+sold\s*[•·\uFFFD]\s*(\d+)\s+wanted", re.I)
_WANTED_RE = re.compile(r"(\d{1,8})\s*wanted\b", re.I)
_QUANTITY_RE = re.compile(r"\b(\d+)\s+tickets?\b", re.I)
_MONTH_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def currency_from_symbol(symbol: Optional[str]) -> Optional[str]:
    return {"€": "EUR", "\uFFFD": "EUR", "$": "USD", "£": "GBP"}.get(symbol) if symbol else None


def parse_price_value(numeric_part: str) -> Optional[float]:
    if not numeric_part:
        return None
    s = numeric_part.replace("\u00a0", " ").replace(" ", "").strip()
    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts[-1]) in (1, 2):
            s = "".join(parts[:-1]).replace(".", "") + "." + parts[-1]
        else:
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts[-1]) not in (1, 2):
            s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def parse_money_from_text(text: str) -> tuple[Optional[str], Optional[float]]:
    if not text:
        return None, None
    m = _PRICE_RE.search(text.replace("\u00a0", " "))
    if not m:
        return None, None
    symbol = m.group(1)
    value = parse_price_value(m.group(2))
    return currency_from_symbol(symbol), value


def parse_header_stats(page_text: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
    m = _STATS_RE.search(page_text)
    if m:
        return int(m.group(1)), int(m.group(2)), int(m.group(3))
    wanted = None
    m2 = _WANTED_RE.search(page_text)
    if m2:
        wanted = int(m2.group(1))
    return None, None, wanted


_URL_DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")


def parse_dates_in_ticket_url(ticket_url: str) -> list[str]:
    """All YYYY-MM-DD tokens in the URL path (event slugs often embed the show date)."""
    if not ticket_url:
        return []
    p = urlparse(ticket_url).path or ""
    return [f"{m.group(1)}-{m.group(2)}-{m.group(3)}" for m in _URL_DATE_RE.finditer(p)]


def extract_event_date_from_json_ld(soup: BeautifulSoup) -> Optional[str]:
    """ISO YYYY-MM-DD from schema.org Event startDate in JSON-LD if present."""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            stack = data["@graph"]
        elif isinstance(data, list):
            stack = data
        else:
            stack = [data]
        for obj in stack:
            if not isinstance(obj, dict):
                continue
            tpe = obj.get("@type")
            is_event = tpe == "Event" or (isinstance(tpe, list) and "Event" in tpe)
            if not is_event:
                continue
            sd = obj.get("startDate") or obj.get("endDate")
            if isinstance(sd, str):
                m = re.match(r"(20\d{2}-\d{2}-\d{2})", sd)
                if m:
                    return m.group(1)
    return None


def resolve_event_date_with_debug(
    soup: BeautifulSoup,
    page_text: str,
    ticket_url: str,
) -> tuple[Optional[str], dict[str, Any]]:
    """
    Combine visible text, JSON-LD, and URL slug dates. If sources disagree, record all in debug
    and prefer URL slug date when the page year does not match any URL year.
    """
    url_dates = parse_dates_in_ticket_url(ticket_url)
    page_date: Optional[str] = None
    m = _EVENT_DATETIME_RE.search(page_text)
    if m:
        month_txt = m.group(2).title()
        day = int(m.group(3))
        start = max(0, m.start() - 400)
        end = min(len(page_text), m.end() + 400)
        window = page_text[start:end]
        year_match = re.search(r"\b(20\d{2})\b", window)
        if not year_match:
            year_match = re.search(r"\b(20\d{2})\b", page_text)
        if year_match:
            year = int(year_match.group(1))
            month_num = _MONTH_TO_NUM.get(month_txt[:3].lower())
            if month_num:
                page_date = f"{year:04d}-{month_num:02d}-{day:02d}"

    jsonld_date = extract_event_date_from_json_ld(soup)
    chosen: Optional[str] = None

    if not page_date and jsonld_date:
        page_date = jsonld_date

    if not page_date:
        chosen = url_dates[0] if url_dates else jsonld_date
    elif not url_dates:
        chosen = page_date
    elif page_date in url_dates:
        chosen = page_date
    else:
        page_y = page_date[:4]
        url_years = {d[:4] for d in url_dates}
        if page_y not in url_years:
            chosen = url_dates[0]
        else:
            chosen = page_date

    if chosen is None and jsonld_date:
        chosen = jsonld_date

    disagree = (
        page_date is not None
        and bool(url_dates)
        and chosen is not None
        and page_date != chosen
        and page_date not in url_dates
    )
    debug: dict[str, Any] = {
        "event_date_from_page_text": page_date,
        "event_date_from_json_ld": jsonld_date,
        "event_dates_from_url": url_dates,
        "event_date_chosen": chosen,
        "event_date_sources_disagree": disagree,
    }
    return chosen, debug


def parse_event_date_local(page_text: str, ticket_url: str = "") -> Optional[str]:
    """Backward-compatible: no soup; URL + visible text only."""
    soup = BeautifulSoup("", "html.parser")
    return resolve_event_date_with_debug(soup, page_text, ticket_url)[0]


def looks_like_verification(html: str) -> bool:
    h = (html or "").lower()
    # Do not use window.__tswac: it is present on normal pages too and causes false blocks.
    return (
        "<title>verifying</title>" in h
        or "unable to verify" in h
        or 'meta name="ts-cv"' in h
    )


def new_driver(*, headless: bool) -> uc.Chrome:
    options = uc.ChromeOptions()
    config.apply_persistent_chrome_profile(options)
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    udd = config.persistent_browser_user_data_dir()
    kw: dict = dict(options=options, headless=headless, use_subprocess=True)
    if udd is not None:
        kw["user_data_dir"] = udd
    if config.CHROME_VERSION_MAIN is not None:
        kw["version_main"] = config.CHROME_VERSION_MAIN
    return uc.Chrome(**kw)


def extract_event_name(soup: BeautifulSoup) -> Optional[str]:
    for h in soup.select("h1, h2, h3"):
        t = h.get_text(" ", strip=True)
        if t and "tickets on ticketswap" not in t.lower() and not t.lower().startswith("tickets - "):
            return t.strip()
    return None


def extract_ticket_type_label(soup: BeautifulSoup, url: str) -> Optional[str]:
    for h in soup.select("h1, h2, h3"):
        t = h.get_text(" ", strip=True)
        if t and t.startswith("Tickets - "):
            return t.replace("Tickets - ", "", 1).replace("－", "-").strip()
    # Fallback to URL slug label.
    _, label = ticket_type_from_ticket_url(url)
    return label


_LOCATION_SKIP = re.compile(
    r"how to sell|become a partner|sell your tickets|language|english|nederlands|français|deutsch|"
    r"privacy|terms|help|contact|download|app store|google play|ticketswap|cookie|log ?in|sign up|"
    r"about us|careers|press|partners",
    re.I,
)
_BAD_LOCATION_TEXT = re.compile(r"^\d+$|^[•·]+$")


def _location_from_json_ld(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            stack = data["@graph"]
        elif isinstance(data, list):
            stack = data
        else:
            stack = [data]
        for obj in stack:
            if not isinstance(obj, dict):
                continue
            tpe = obj.get("@type")
            is_event = tpe == "Event" or (isinstance(tpe, list) and "Event" in tpe)
            if is_event:
                loc = obj.get("location")
                if isinstance(loc, dict):
                    name = (loc.get("name") or "").strip() or None
                    addr = loc.get("address")
                    city, country = None, None
                    if isinstance(addr, dict):
                        city = (addr.get("addressLocality") or "").strip() or None
                        country = (addr.get("addressCountry") or "").strip() or None
                    if name or city or country:
                        return name, city, country
                if isinstance(loc, str) and loc.strip():
                    return loc.strip(), None, None
    return None, None, None


def _clean_location_candidate(text: str) -> Optional[str]:
    t = (text or "").strip()
    if not t or len(t) > 120 or _LOCATION_SKIP.search(t) or _BAD_LOCATION_TEXT.match(t):
        return None
    return t


def extract_location_triple(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Prefer schema.org Event in JSON-LD, then a very small link set next to the main title.
    Avoid scanning the whole page (footer/nav). If unsure, return (None, None, None).
    """
    v, c, co = _location_from_json_ld(soup)
    if v or c or co:
        return v, c, co

    root = soup.select_one("main, [role='main'], article")
    if not root:
        return None, None, None

    h1 = root.find("h1")
    if not h1:
        return None, None, None

    items: list[str] = []
    seen: set[str] = set()

    def take_anchor(a) -> None:
        if not getattr(a, "get", None):
            return
        href = (a.get("href") or "").lower()
        if "partner" in href or "help." in href or "blog." in href or href.endswith("/sell"):
            return
        t = _clean_location_candidate(a.get_text(" ", strip=True))
        if t and t not in seen:
            seen.add(t)
            items.append(t)

    scope = h1.parent if h1.parent else root
    for a in scope.find_all("a", href=True, limit=20):
        take_anchor(a)
    for sib in h1.find_next_siblings(limit=6):
        if hasattr(sib, "find_all"):
            for a in sib.find_all("a", href=True, limit=12):
                take_anchor(a)

    if len(items) < 2:
        return None, None, None

    venue = items[0] if len(items) >= 1 else None
    city = items[1] if len(items) >= 2 else None
    country = items[2] if len(items) >= 3 else None
    return venue, city, country


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def extract_listings(soup: BeautifulSoup) -> list[ListingSnapshot]:
    listings: list[ListingSnapshot] = []
    seen_fp: set[str] = set()
    seen_nodes: set[int] = set()

    root = soup.select_one("main, [role='main'], article") or soup

    def try_card(card) -> None:
        if id(card) in seen_nodes:
            return
        raw_text = card.get_text(" ", strip=True)
        if not raw_text or "ticket" not in raw_text.lower():
            return
        price_node = card.select_one("footer strong")
        if not price_node:
            # Some pages use div cards without a <footer>.
            price_node = card.select_one("strong")
        if not price_node:
            return
        currency, price = parse_money_from_text(price_node.get_text(" ", strip=True))
        if price is None:
            return
        qty_match = _QUANTITY_RE.search(raw_text)
        quantity = int(qty_match.group(1)) if qty_match else None
        seller_hint = None
        img = card.find("img")
        if img and img.get("alt"):
            seller_hint = img.get("alt")
        href_raw = card.get("href") or ""
        href_norm = normalize_url(href_raw) or (href_raw.strip() or None)
        fp_base = f"{href_norm or href_raw}|{quantity or ''}|{currency or ''}|{price:.2f}"
        fp = slugify(fp_base) or f"listing-{len(listings)+1}"
        if fp in seen_fp:
            return
        seen_fp.add(fp)
        seen_nodes.add(id(card))
        listings.append(
            ListingSnapshot(
                listing_fingerprint=fp,
                seller_hint=seller_hint,
                quantity=quantity,
                price_per_ticket=price,
                currency=currency,
                raw_text=raw_text,
                listing_href=href_norm,
            )
        )

    for card in soup.select("a.styles_link__Jm_hk"):
        try_card(card)
    # Newer TicketSwap markup: listing cards are often divs in the Available grid.
    for card in root.select("div.styles_card__lfwEU"):
        try_card(card)
    for card in root.select("a[href*='listing']"):
        try_card(card)
    for card in root.select("a[href]"):
        if "festival-tickets" in (card.get("href") or "") and card.select_one("footer strong"):
            try_card(card)

    return listings


def parse_market_html(
    html: str,
    *,
    ticket_url: str,
    scraped_at_utc: Optional[datetime] = None,
    page_title: Optional[str] = None,
) -> MarketSnapshot:
    """
    Parse already-fetched TicketSwap HTML (same logic as after a successful browser load).
    Useful for tests and for replaying saved debug HTML.
    """
    scraped_at = scraped_at_utc or utc_now()
    soup = BeautifulSoup(html or "", "html.parser")
    page_text = soup.get_text(separator=" ", strip=True)

    event_name = extract_event_name(soup)
    ticket_type_label = extract_ticket_type_label(soup, ticket_url)
    venue, city, country = extract_location_triple(soup)
    available_count, sold_count, wanted_count = parse_header_stats(page_text)
    event_date_local, date_debug = resolve_event_date_with_debug(soup, page_text, ticket_url)

    listings = extract_listings(soup)
    prices = [l.price_per_ticket for l in listings if l.price_per_ticket is not None]
    currency = listings[0].currency if listings else ("EUR" if "€" in page_text else None)

    raw_debug: dict[str, Any] = {
        "url": ticket_url,
        "page_title": page_title,
        "prices_sample": prices[:25],
        **date_debug,
    }

    return MarketSnapshot(
        ticket_url=ticket_url,
        scraped_at_utc=scraped_at,
        status="ok" if listings or prices else "no_data",
        error_message=None,
        event_name=event_name,
        event_url=None,
        venue=venue,
        city=city,
        country=country,
        event_date_local=event_date_local,
        ticket_type_label=ticket_type_label,
        currency=currency,
        listing_count=len(listings) if listings else (len(prices) if prices else 0),
        available_count=available_count,
        wanted_count=wanted_count,
        sold_count=sold_count,
        lowest_ask=min(prices) if prices else None,
        highest_ask=max(prices) if prices else None,
        median_ask=statistics.median(prices) if prices else None,
        average_ask=(sum(prices) / len(prices)) if prices else None,
        listings=listings,
        raw_debug=raw_debug,
    )


@contextlib.contextmanager
def market_scrape_session(*, headless: bool):
    """One Chrome + persistent profile for many `scrape_market_with_driver` calls (keeps login)."""
    driver = new_driver(headless=headless)
    try:
        yield driver
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def scrape_market_with_driver(
    driver: uc.Chrome,
    url: str,
    *,
    debug_dir: Path,
    headless: bool,
    manual_wait_seconds: int = 0,
) -> MarketSnapshot:
    scraped_at = utc_now()
    debug_dir.mkdir(parents=True, exist_ok=True)

    n = normalize_url(url)
    if not n:
        return MarketSnapshot(ticket_url=url, scraped_at_utc=scraped_at, status="error", error_message="not_a_ticketswap_url")
    url = n
    p = urlparse(url)
    if not TICKET_URL_RE.match(p.path or ""):
        pass

    try:
        driver.get(url)
        time.sleep(config.PAGE_LOAD_SLEEP_SECONDS)
        discover_urls_mod.scroll_for_lazy_content(driver)
        discover_urls_mod.expand_main_accordions(driver)
        html = driver.page_source or ""

        if looks_like_verification(html):
            if not headless and manual_wait_seconds > 0:
                time.sleep(manual_wait_seconds)
                discover_urls_mod.scroll_for_lazy_content(driver)
                discover_urls_mod.expand_main_accordions(driver)
                html = driver.page_source or ""

        if looks_like_verification(html):
            _save_debug(debug_dir, label="blocked_verification", url=url, html=html, driver=driver)
            return MarketSnapshot(
                ticket_url=url,
                scraped_at_utc=scraped_at,
                status="blocked",
                error_message="verification_page_detected",
                raw_debug={"page_title": driver.title, "url": url},
            )

        snap = parse_market_html(html, ticket_url=url, scraped_at_utc=scraped_at, page_title=driver.title)
        if snap.status != "no_data":
            return snap

        # Second pass: listings are sometimes below the fold or behind a tab/anchor.
        try:
            driver.execute_script(
                r"""
                const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
                const root = document.querySelector('main') || document.body || document;
                const els = Array.from(root.querySelectorAll('a,button,[role="button"],[role="tab"]'));
                const pick = els.find(el => norm(el.textContent || el.innerText || '').startsWith('available'));
                if (pick) { pick.scrollIntoView({block:'center'}); try { pick.click(); } catch (e) {} }
                """
            )
        except Exception:
            pass
        for _ in range(3):
            discover_urls_mod.scroll_for_lazy_content(driver)
            time.sleep(0.65)
        html2 = driver.page_source or ""
        snap2 = parse_market_html(html2, ticket_url=url, scraped_at_utc=scraped_at, page_title=driver.title)
        if snap2.status != "no_data":
            return snap2

        _save_debug(debug_dir, label="no_data", url=url, html=html2, driver=driver)
        return snap2
    except Exception as e:
        with contextlib.suppress(Exception):
            _save_debug(debug_dir, label="exception", url=url, html=driver.page_source or "", driver=driver)
        return MarketSnapshot(ticket_url=url, scraped_at_utc=scraped_at, status="error", error_message=f"{type(e).__name__}: {e}")


def scrape_market_url(
    url: str,
    *,
    headless: bool,
    debug_dir: Path,
    manual_wait_seconds: int = 0,
    driver: Optional[uc.Chrome] = None,
) -> MarketSnapshot:
    """
    Scrape one ticket-type URL.

    When ``driver`` is passed (e.g. stress test / batch after discovery), it is reused so a second
    Chrome is not started on the same persistent profile (that often raises SessionNotCreatedException
    on Windows: "chrome not reachable").
    """
    if driver is not None:
        return scrape_market_with_driver(
            driver,
            url,
            debug_dir=debug_dir,
            headless=headless,
            manual_wait_seconds=manual_wait_seconds,
        )
    with market_scrape_session(headless=headless) as session_driver:
        return scrape_market_with_driver(
            session_driver,
            url,
            debug_dir=debug_dir,
            headless=headless,
            manual_wait_seconds=manual_wait_seconds,
        )


def _save_debug(debug_dir: Path, *, label: str, url: str, html: str, driver: uc.Chrome) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", label)[:60]
    path_key = re.sub(r"[^a-zA-Z0-9_-]+", "_", urlparse(url).path.strip("/")[:60] or "root")
    (debug_dir / f"{ts}_{safe}_{path_key}.html").write_text(html or "", encoding="utf-8")
    with contextlib.suppress(Exception):
        driver.save_screenshot(str(debug_dir / f"{ts}_{safe}_{path_key}.png"))


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape one TicketSwap ticket-type price URL.")
    p.add_argument("--url", required=True, help="TicketSwap ticket-type URL.")
    p.add_argument("--headless", action="store_true", default=False)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(list(argv))


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    snap = scrape_market_url(
        args.url,
        headless=bool(args.headless),
        debug_dir=config.DEBUG_DIR,
        manual_wait_seconds=int(config.MANUAL_VERIFY_WAIT_SECONDS) if not bool(args.headless) else 0,
    )
    print(json.dumps(asdict(snap), ensure_ascii=False, indent=2, default=str))
    return 0 if snap.status in {"ok", "no_data"} else 2


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))

