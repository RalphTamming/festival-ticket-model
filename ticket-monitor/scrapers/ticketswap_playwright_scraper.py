"""
TicketSwap Playwright scraper (best-effort V1).

Notes:
- TicketSwap is a dynamic site; markup/selectors may change over time.
- This implementation uses a small set of likely selectors plus a regex fallback
  that scans the page for Euro-like prices.
- If it cannot reliably find listing prices, it still returns a snapshot with
  prices as None and logs will show what was detected in raw_payload.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .base import BaseScraper, MarketSnapshot
from .example_playwright_scraper import parse_euro_price, parse_int_from_text


_WANTED_RE = re.compile(r"\b(?:gezocht|wanted)\b[^0-9]{0,20}(\d{1,9})", re.IGNORECASE)


def _dedupe_preserve_order(values: List[float]) -> List[float]:
    seen = set()
    out: List[float] = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


class TicketSwapPlaywrightScraper(BaseScraper):
    """
    TicketSwap-specific scraper.

    This is still "Version 1": simple, synchronous, and conservative.
    """

    # Try specific selectors first; keep this list short and easy to tweak.
    PRICE_SELECTORS: Tuple[str, ...] = (
        # Common patterns across modern React apps:
        "[data-testid*='price' i]",
        "[class*='price' i]",
        # Some sites embed amounts in spans within listing cards:
        "article span:has-text('€')",
        "div span:has-text('€')",
    )

    def scrape(self) -> MarketSnapshot:
        raw_payload: Dict[str, Any] = {
            "attempted_price_selectors": list(self.PRICE_SELECTORS),
            "warnings": [],
        }

        prices: List[float] = []
        listing_count: Optional[int] = None
        wanted_count: Optional[int] = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_navigation_timeout(self.navigation_timeout_ms)
            page.set_default_timeout(self.selector_timeout_ms)

            try:
                page.goto(self.url, wait_until="domcontentloaded")

                # Give client-side rendering a moment; keep it short.
                page.wait_for_timeout(1_000)

                # Attempt 1: selector-based extraction
                selector_hits = []
                for sel in self.PRICE_SELECTORS:
                    try:
                        loc = page.locator(sel)
                        count = loc.count()
                        if count <= 0:
                            continue
                        texts = loc.all_inner_texts()
                        parsed = [parse_euro_price(t) for t in texts]
                        parsed = [p for p in parsed if p is not None]
                        if parsed:
                            selector_hits.append(
                                {
                                    "selector": sel,
                                    "element_count": count,
                                    "text_sample": texts[:10],
                                    "parsed_sample": parsed[:10],
                                }
                            )
                            prices.extend(parsed)
                    except Exception as e:
                        raw_payload["warnings"].append(f"Selector failed: {sel} ({type(e).__name__})")

                raw_payload["selector_hits"] = selector_hits

                # Attempt 1b: Playwright text-regex locator for anything that looks like "€ 12,34"
                # This is often more robust than CSS selectors when markup changes.
                if not prices:
                    try:
                        # Ensure at least one match exists before sampling.
                        count = page.locator("text=/€\\s*[0-9][0-9\\.\\s]*(?:,[0-9]{1,2})?/").count()
                        raw_payload["text_regex_match_count"] = count
                        if count > 0:
                            # Collect up to first 200 matches to keep it bounded.
                            loc = page.locator("text=/€\\s*[0-9][0-9\\.\\s]*(?:,[0-9]{1,2})?/")
                            texts = loc.all_inner_texts()[:200]
                            raw_payload["text_regex_text_sample"] = texts[:20]
                            for t in texts:
                                v = parse_euro_price(t)
                                if v is not None:
                                    prices.append(v)
                    except Exception as e:
                        raw_payload["warnings"].append(f"Text-regex extraction failed ({type(e).__name__})")

                # Attempt 2 (fallback): regex scan of the full page text for € prices
                # This can over-match (fees, banners). It's a fallback for robustness.
                if not prices:
                    body_text = page.locator("body").inner_text()
                    raw_payload["fallback_body_text_sample"] = body_text[:1000]

                    euro_like_texts = re.findall(r"€\s*[0-9][0-9\.\s]*(?:,[0-9]{1,2})?", body_text)
                    raw_payload["fallback_euro_like_count"] = len(euro_like_texts)
                    raw_payload["fallback_euro_like_sample"] = euro_like_texts[:20]
                    for t in euro_like_texts:
                        v = parse_euro_price(t)
                        if v is not None:
                            prices.append(v)

                prices = _dedupe_preserve_order(prices)

                # Best-effort listing count:
                # If we have selector hits, use the max element_count as a proxy.
                if selector_hits:
                    listing_count = max(h["element_count"] for h in selector_hits)  # type: ignore[arg-type]

                # Wanted count best-effort: search for "Gezocht/Wanted <n>" in body text
                try:
                    body_text = page.locator("body").inner_text()
                    m = _WANTED_RE.search(body_text)
                    if m:
                        wanted_count = parse_int_from_text(m.group(1))
                        raw_payload["wanted_match"] = m.group(0)[:100]
                    else:
                        raw_payload["wanted_match"] = None
                except Exception as e:
                    raw_payload["warnings"].append(f"Wanted extraction failed ({type(e).__name__})")

            finally:
                context.close()
                browser.close()

        min_price = min(prices) if prices else None
        max_price = max(prices) if prices else None
        avg_price = (sum(prices) / len(prices)) if prices else None

        return MarketSnapshot(
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

