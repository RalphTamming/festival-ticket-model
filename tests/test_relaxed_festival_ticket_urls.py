"""
Relaxed festival ticket URL extraction tests (no DB).

- **Synthetic**: always runs — proves href + regex + hub→child merge logic.
- **Live**: runs only when TicketSwap returns real HTML (not the ``Verifying`` interstitial).

Run from repo root:
  python -m unittest tests.test_relaxed_festival_ticket_urls -v
"""

from __future__ import annotations

import unittest
from typing import Optional

import requests
from urllib.parse import urlparse

from discovery.ticketswap_relaxed_extract import (
    collect_festival_ticket_urls_with_requests,
    extract_hub_child_event_urls_from_html,
    extract_relaxed_festival_ticket_urls_from_html,
)


LIVE_TEST_URLS = [
    "https://www.ticketswap.com/festival-tickets/a/awakenings-upclose",
    "https://www.ticketswap.com/festival-tickets/a/dekmantel-festival",
    "https://www.ticketswap.com/festival-tickets/a/festifest",
    "https://www.ticketswap.com/festival-tickets/a/het-landjuweel",
    "https://www.ticketswap.com/festival-tickets/a/lente-kabinet",
    "https://www.ticketswap.com/festival-tickets/a/music-on-festival",
    "https://www.ticketswap.com/festival-tickets/a/springbreak-festival",
    "https://www.ticketswap.com/festival-tickets/a/joy-flow-festival",
    "https://www.ticketswap.com/festival-tickets/hemmeland-live-festival-monnickendam-hemmeland-2026-05-14-CXAX2jkuni3hLnv7zQUU4",
]

_BROWSER_HEADERS = {
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


def _is_verification_shell(html: str) -> bool:
    h = (html or "").lower()
    return "<title>verifying</title>" in h or 'name="ts-cv"' in h


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class _FakeSession:
    """Minimal session stub: ``mapping[url] -> html``."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def get(self, url: str, timeout: Optional[float] = None, headers: Optional[dict] = None) -> _FakeResponse:
        body = self.mapping.get(url)
        if body is None:
            return _FakeResponse("", 404)
        return _FakeResponse(body, 200)


class TestRelaxedExtractionSynthetic(unittest.TestCase):
    def test_relaxed_href_and_regex_fallback(self) -> None:
        html = """
        <html><body>
        <a href="/festival-tickets/some-event-2026-amsterdam-CXabc/weekend-regular-tickets/5314233">t</a>
        <script type="application/json">
          "https://www.ticketswap.com/festival-tickets/other-event/day-tickets/1000001"
        </script>
        </body></html>
        """
        base = "https://www.ticketswap.com/festival-tickets/some-event-2026-amsterdam-CXabc"
        found = extract_relaxed_festival_ticket_urls_from_html(html, base_url=base)
        self.assertGreaterEqual(len(found), 2, found)

    def test_hub_child_event_detection(self) -> None:
        hub = "https://www.ticketswap.com/festival-tickets/a/awakenings-upclose"
        html = """
        <a href="/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXdeadbeef123">dated</a>
        <a href="/festival-tickets/a/other-hub">skip</a>
        """
        kids = extract_hub_child_event_urls_from_html(html, hub_url=hub)
        self.assertTrue(
            any("awakenings-upclose-2026-may-amsterdam-CXdeadbeef123" in k for k in kids),
            kids,
        )
        self.assertFalse(any("/festival-tickets/a/other-hub" == k.rstrip("/") for k in kids))

    def test_collect_hub_merges_child_pages(self) -> None:
        hub = "https://www.ticketswap.com/festival-tickets/a/awakenings-upclose"
        child = "https://www.ticketswap.com/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXdeadbeef123"
        child_path = urlparse(child).path or ""
        hub_html = f"""
        <html><body>
          <a href="{child_path}">go</a>
          <p>https://www.ticketswap.com/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXdeadbeef123/early-tickets/2000001</p>
        </body></html>
        """
        child_html = """
        <a href="/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXdeadbeef123/weekend-tickets/5314233">x</a>
        """
        fake = _FakeSession({hub: hub_html, child: child_html})
        found = collect_festival_ticket_urls_with_requests(hub, session=fake, max_hub_children=5, timeout=5.0)
        self.assertGreaterEqual(len(found), 2, found)
        for u in found:
            self.assertIn("/festival-tickets/", u)
            self.assertRegex(u, r"/\d{5,}$")


class TestRelaxedFestivalTicketUrlsLive(unittest.TestCase):
    """Best-effort live fetch; skipped when TicketSwap serves only the verification shell."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.session = requests.Session()
        cls._live_usable: Optional[bool] = None
        try:
            r = cls.session.get(LIVE_TEST_URLS[0], timeout=25, headers=_BROWSER_HEADERS)
            cls._live_usable = r.status_code == 200 and not _is_verification_shell(r.text)
        except OSError:
            cls._live_usable = False

    def test_each_live_url_returns_tickets_when_site_allows(self) -> None:
        if not self._live_usable:
            raise unittest.SkipTest("TicketSwap returned verification shell or network error; synthetic tests cover logic.")
        for url in LIVE_TEST_URLS:
            with self.subTest(url=url):
                found = collect_festival_ticket_urls_with_requests(
                    url,
                    session=self.session,
                    max_hub_children=12,
                    timeout=30.0,
                )
                n = len(found)
                status = "SUCCESS" if n > 0 else "FAIL"
                print(f"\n{status}  n={n:3d}  {url}")
                if n:
                    for u in found[:5]:
                        print(f"    {u}")
                    if n > 5:
                        print(f"    ... ({n - 5} more)")
                self.assertGreater(n, 0, f"Expected >0 ticket URLs for {url!r} (got {n})")


if __name__ == "__main__":
    unittest.main()
