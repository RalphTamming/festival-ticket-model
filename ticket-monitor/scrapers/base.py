"""
Scraper interfaces and shared types.

Keep these stable; new site scrapers should implement BaseScraper.scrape().
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class MarketSnapshot:
    """
    One market snapshot at a point in time.

    Prices are floats (EUR) when available; None if not found.
    raw_payload stores optional debug metadata (selectors used, raw texts, etc.).
    """

    site_name: str
    label: str
    url: str
    scraped_at: datetime
    min_price: Optional[float]
    max_price: Optional[float]
    avg_price: Optional[float]
    listing_count: Optional[int]
    wanted_count: Optional[int]
    raw_payload: Dict[str, Any]

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)


class BaseScraper(ABC):
    """Abstract scraper. Implementations should be deterministic and side-effect free."""

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
        self.site_name = site_name
        self.label = label
        self.url = url
        self.headless = headless
        self.navigation_timeout_ms = navigation_timeout_ms
        self.selector_timeout_ms = selector_timeout_ms

    @abstractmethod
    def scrape(self) -> MarketSnapshot:
        """Scrape the configured URL and return a MarketSnapshot."""
        raise NotImplementedError

