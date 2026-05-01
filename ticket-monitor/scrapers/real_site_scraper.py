"""
Real-site Playwright scraper template (visible-browser friendly).

This file is meant to be adapted per site by replacing selectors and (if needed)
extraction logic. It is intentionally transparent and maintainable:
- no stealth plugins
- no fingerprint spoofing
- no captcha/verification bypass

Use `login_and_save_session.py` to create a trusted storage-state session, then
enable `USE_STORAGE_STATE=true` so this scraper reuses that session.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import logging
import re

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

import config
from .base import BaseScraper, MarketSnapshot
from .example_playwright_scraper import parse_euro_price, parse_int_from_text


class RealSiteScraper(BaseScraper):
    """
    Scraper template for a real marketplace page.

    TODO: Replace selectors for your site.
    """

    # --- Replaceable selectors ---
    LISTINGS_CONTAINER_SELECTOR = "[data-testid='listings']"  # TODO: replace selector for your site
    LISTING_PRICE_SELECTOR = "[data-testid='listing-price']"  # TODO: replace selector for your site
    WANTED_COUNT_SELECTOR = "[data-testid='wanted-count']"  # TODO: replace selector for your site (optional)

    # Optional: selector that indicates user is logged in (site-specific)
    LOGGED_IN_INDICATOR_SELECTOR = None  # e.g. "a[href*='/account']"

    def __init__(
        self,
        *,
        site_name: str,
        label: str,
        url: str,
        headless: bool,
        navigation_timeout_ms: int,
        selector_timeout_ms: int,
    ) -> None:
        # Keep BaseScraper signature stable; read runtime settings from config.
        super().__init__(
            site_name=site_name,
            label=label,
            url=url,
            headless=headless,
            navigation_timeout_ms=navigation_timeout_ms,
            selector_timeout_ms=selector_timeout_ms,
        )
        self.log = logging.getLogger(f"scraper.{site_name}.{label}")

    def scrape(self) -> MarketSnapshot:
        raw: Dict[str, Any] = {
            "url": self.url,
            "selectors": {
                "listings_container": self.LISTINGS_CONTAINER_SELECTOR,
                "listing_price": self.LISTING_PRICE_SELECTOR,
                "wanted_count": self.WANTED_COUNT_SELECTOR,
                "logged_in_indicator": self.LOGGED_IN_INDICATOR_SELECTOR,
            },
            "blockers": [],
            "warnings": [],
            "storage_state": {
                "enabled": config.USE_STORAGE_STATE,
                "path": config.STORAGE_STATE_PATH,
                "loaded": False,
            },
        }

        with sync_playwright() as p:
            self.log.info("Launching Chromium. headless=%s slow_mo_ms=%s", config.HEADLESS, config.SLOW_MO_MS)
            # Prefer persistent context (normal browser profile) if enabled.
            if config.USE_PERSISTENT_CONTEXT:
                self.log.info("Using persistent context. user_data_dir=%s channel=%s", config.USER_DATA_DIR, config.BROWSER_CHANNEL)
                context = p.chromium.launch_persistent_context(
                    user_data_dir=config.USER_DATA_DIR,
                    headless=config.HEADLESS,
                    slow_mo=config.SLOW_MO_MS,
                    channel=config.BROWSER_CHANNEL,
                )
                browser = None
            else:
                browser = p.chromium.launch(headless=config.HEADLESS, slow_mo=config.SLOW_MO_MS, channel=config.BROWSER_CHANNEL)
                context = self._new_context(browser, raw)
            page = context.new_page()
            page.set_default_timeout(config.TIMEOUT_MS)
            page.set_default_navigation_timeout(config.TIMEOUT_MS)

            try:
                self.goto_page(page, raw)

                blockers = self.detect_blockers(page)
                raw["blockers"] = blockers
                if blockers:
                    self.log.warning("Blockers detected: %s", blockers)
                    # Helpful context for debugging: a small body-text sample.
                    try:
                        raw["body_text_sample"] = (page.locator("body").inner_text() or "")[:1000]
                    except Exception:
                        raw["body_text_sample"] = None
                    try:
                        raw["page_title"] = page.title()
                    except Exception:
                        raw["page_title"] = None

                if config.SAVE_DEBUG_SCREENSHOT:
                    self._save_screenshot(page, suffix="after_load")
                if config.SAVE_DEBUG_HTML:
                    self._save_html(page, suffix="after_load")

                price_texts = self.extract_price_texts(page, raw)
                raw["price_texts_sample"] = price_texts[:20]

                prices = [self.parse_price(t) for t in price_texts]
                prices = [p for p in prices if p is not None]

                wanted_count = self.extract_wanted_count(page, raw)

                listing_count: Optional[int]
                if price_texts:
                    listing_count = len(price_texts)
                else:
                    listing_count = None
                    raw["warnings"].append("No listing prices found (selector may need updating).")
                    self.log.warning("No listing prices found. Update LISTING_PRICE_SELECTOR for this site.")

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
                    raw_payload=raw,
                )

                # Ensure JSON-serializable payload
                json.dumps(asdict(snapshot), default=str, ensure_ascii=False)
                return snapshot

            finally:
                try:
                    context.close()
                finally:
                    if browser is not None:
                        browser.close()

    # --- Helper methods (meant to be adapted) ---

    def _new_context(self, browser: Browser, raw: Dict[str, Any]) -> BrowserContext:
        state_path = Path(config.STORAGE_STATE_PATH)
        if config.USE_STORAGE_STATE and state_path.exists():
            self.log.info("Using storage state: %s", state_path)
            raw["storage_state"]["loaded"] = True
            return browser.new_context(storage_state=str(state_path))
        if config.USE_STORAGE_STATE and not state_path.exists():
            self.log.warning(
                "USE_STORAGE_STATE=true but storage state not found at %s. Run login_and_save_session.py first.",
                state_path,
            )
        return browser.new_context()

    def goto_page(self, page: Page, raw: Dict[str, Any]) -> None:
        self.log.info("Navigating to page.")
        page.goto(self.url, wait_until="domcontentloaded")
        page.wait_for_timeout(1_000)

        # Optional: wait for listings container if you have a reliable selector.
        if self.LISTINGS_CONTAINER_SELECTOR:
            try:
                page.wait_for_selector(self.LISTINGS_CONTAINER_SELECTOR, timeout=min(10_000, config.TIMEOUT_MS))
                self.log.info("Listings container attached.")
            except Exception:
                raw["warnings"].append("Listings container not found quickly (selector may be wrong).")
                self.log.warning("Listings container not found quickly.")

    def detect_blockers(self, page: Page) -> List[str]:
        try:
            text = (page.locator("body").inner_text() or "").lower()
        except Exception:
            return ["target_closed"]
        blockers: List[str] = []

        # Verification / bot-check pages often have very distinctive titles or messages.
        try:
            title = (page.title() or "").lower()
        except Exception:
            title = ""

        if "verifying" in title or "unable to verify" in text or "verify you are human" in text:
            blockers.append("verification_page_detected")
        if "log in" in text or "login" in text:
            # Very generic; you may want a site-specific indicator instead.
            blockers.append("login_maybe_required")

        if self.LOGGED_IN_INDICATOR_SELECTOR:
            try:
                if page.locator(self.LOGGED_IN_INDICATOR_SELECTOR).count() <= 0:
                    blockers.append("logged_in_indicator_missing")
            except Exception:
                blockers.append("logged_in_indicator_check_failed")

        return blockers

    def extract_price_texts(self, page: Page, raw: Dict[str, Any]) -> List[str]:
        """
        Extract raw listing price texts from the page.

        TODO: Replace LISTING_PRICE_SELECTOR for your site.
        """
        self.log.info("Extracting price elements using selector: %s", self.LISTING_PRICE_SELECTOR)
        try:
            loc = page.locator(self.LISTING_PRICE_SELECTOR)
            count = loc.count()
            raw["price_element_count"] = count
            self.log.info("Price elements found: %d", count)
            if count <= 0:
                return []
            texts = [t.strip() for t in loc.all_inner_texts() if t and t.strip()]
            raw["price_texts_raw_sample"] = texts[:20]
            return texts
        except Exception as e:
            raw["warnings"].append(f"Price extraction failed: {type(e).__name__}")
            self.log.exception("Price extraction failed.")
            return []

    def parse_price(self, text: str) -> Optional[float]:
        return parse_euro_price(text)

    def extract_wanted_count(self, page: Page, raw: Dict[str, Any]) -> Optional[int]:
        """
        Optional wanted/gezocht count extraction.

        TODO: Replace WANTED_COUNT_SELECTOR for your site (or return None).
        """
        if not self.WANTED_COUNT_SELECTOR:
            return None
        try:
            el = page.locator(self.WANTED_COUNT_SELECTOR)
            if el.count() <= 0:
                raw["wanted_text"] = None
                return None
            txt = (el.first.inner_text() or "").strip()
            raw["wanted_text"] = txt
            return parse_int_from_text(txt)
        except Exception as e:
            raw["warnings"].append(f"Wanted extraction failed: {type(e).__name__}")
            self.log.exception("Wanted extraction failed.")
            return None

    # --- Debug artifacts ---

    def _debug_dir(self) -> Path:
        d = Path(__file__).resolve().parents[1] / "debug"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _slug(self) -> str:
        base = f"{self.site_name}_{self.label}".lower()
        base = re.sub(r"[^a-z0-9]+", "_", base).strip("_")
        return base[:80] or "target"

    def _save_screenshot(self, page: Page, *, suffix: str) -> None:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self._debug_dir() / f"{self._slug()}_{suffix}_{ts}.png"
        self.log.info("Saving screenshot: %s", path)
        page.screenshot(path=str(path), full_page=True)

    def _save_html(self, page: Page, *, suffix: str) -> None:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self._debug_dir() / f"{self._slug()}_{suffix}_{ts}.html"
        self.log.info("Saving HTML: %s", path)
        html = page.content()
        path.write_text(html, encoding="utf-8")

