"""
Example Playwright scraper (template).

This is intentionally a generic starting point:
- The selectors are placeholders and will NOT be correct for real marketplaces.
- Replace selectors and extraction logic per target site.
- Keep scraping legal: only use on websites where you have permission and where ToS allows it.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional
import json
import re

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .base import BaseScraper, MarketSnapshot


# --- Placeholder selectors (REPLACE THESE PER SITE) ---
# Price elements for each listing (e.g. "€ 34,50")
PRICE_SELECTOR = "[data-testid='listing-price']"

# Optional "wanted/zocht" ticket count somewhere on the page
WANTED_COUNT_SELECTOR = "[data-testid='wanted-count']"

# Optional: a container that indicates listings have loaded
LISTINGS_CONTAINER_SELECTOR = "[data-testid='listings']"


_EURO_PRICE_RE = re.compile(r"(?P<num>[0-9][0-9\.\s]*([,][0-9]{1,2})?)")


def parse_euro_price(text: str) -> Optional[float]:
    """
    Parse common Euro price formats into a float.

    Examples handled:
    - "€ 12,50" -> 12.50
    - "EUR 1.234,00" -> 1234.00
    - "12,-" -> 12.0
    - "  9.95 " -> 9.95
    Returns None if no number-like pattern is found.
    """

    if not text:
        return None

    cleaned = (
        text.replace("\u00a0", " ")
        .replace("€", " ")
        .replace("EUR", " ")
        .replace("eur", " ")
        .strip()
        .lower()
    )
    cleaned = cleaned.replace(",-", ",00").replace(".-", ",00")

    m = _EURO_PRICE_RE.search(cleaned)
    if not m:
        return None

    num = m.group("num")
    # Remove spaces and thousands separators (.)
    num = num.replace(" ", "").replace(".", "")
    # Decimal comma -> dot
    num = num.replace(",", ".")

    try:
        return float(num)
    except ValueError:
        return None


def parse_int_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.findall(r"\d+", text.replace("\u00a0", " "))
    if not digits:
        return None
    try:
        return int(digits[0])
    except ValueError:
        return None


class ExamplePlaywrightScraper(BaseScraper):
    """
    Example Playwright scraper using the sync API for simplicity.

    Replace selectors above and, if needed, the extraction logic inside scrape().
    """

    def scrape(self) -> MarketSnapshot:
        raw_payload: Dict[str, Any] = {
            "selectors": {
                "price": PRICE_SELECTOR,
                "wanted_count": WANTED_COUNT_SELECTOR,
                "listings_container": LISTINGS_CONTAINER_SELECTOR,
            }
        }

        prices: List[float] = []
        wanted_count: Optional[int] = None
        listing_count: Optional[int] = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_navigation_timeout(self.navigation_timeout_ms)
            page.set_default_timeout(self.selector_timeout_ms)

            try:
                page.goto(self.url, wait_until="domcontentloaded")

                # Optional wait: if a listings container exists, wait for it.
                # (If the selector is wrong, we don't want to fail hard.)
                try:
                    page.wait_for_selector(LISTINGS_CONTAINER_SELECTOR, state="attached", timeout=3_000)
                except PlaywrightTimeoutError:
                    raw_payload["warning"] = "Listings container selector not found quickly (placeholder?)"

                # Extract prices (as texts) and parse.
                price_elements = page.query_selector_all(PRICE_SELECTOR)
                raw_payload["price_elements_found"] = len(price_elements)

                price_texts: List[str] = []
                for el in price_elements:
                    try:
                        t = (el.inner_text() or "").strip()
                    except Exception:
                        t = ""
                    if t:
                        price_texts.append(t)
                        parsed = parse_euro_price(t)
                        if parsed is not None:
                            prices.append(parsed)

                raw_payload["price_texts_sample"] = price_texts[:10]
                listing_count = len(price_elements) if price_elements else None

                # Optional wanted count extraction.
                wanted_el = page.query_selector(WANTED_COUNT_SELECTOR)
                if wanted_el is not None:
                    wanted_text = (wanted_el.inner_text() or "").strip()
                    raw_payload["wanted_text"] = wanted_text
                    wanted_count = parse_int_from_text(wanted_text)
                else:
                    raw_payload["wanted_text"] = None

            finally:
                context.close()
                browser.close()

        min_price = min(prices) if prices else None
        max_price = max(prices) if prices else None
        avg_price = (sum(prices) / len(prices)) if prices else None

        snapshot = MarketSnapshot(
            site_name=self.site_name,
            label=self.label,
            url=self.url,
            scraped_at=MarketSnapshot.now_utc(),
            min_price=min_price,
            max_price=max_price,
            avg_price=avg_price,
            listing_count=listing_count,
            wanted_count=wanted_count,
            raw_payload=raw_payload,
        )

        # Ensure raw_payload is JSON-serializable (helpful for DB storage).
        json.dumps(asdict(snapshot), default=str)
        return snapshot

