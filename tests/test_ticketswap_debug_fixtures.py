"""
Fixture HTML for Awakenings-style hub and 909-style hub: relaxed ticket extraction + classification.

Ticket *names* are arbitrary; only URL shape (festival-tickets + numeric id >= 5 digits) matters.
"""

from __future__ import annotations

import unittest

from discovery import discover_urls as du
from discovery import ticketswap_candidate_harvest as tch
from discovery import ticketswap_relaxed_extract as tsx

AWAKENINGS_HUB_HTML = """
<html><head><title>Awakenings Upclose</title>
<script id="__NEXT_DATA__" type="application/json">{}</script>
</head><body>
<a href="/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXabc123/weekend-loyalty-ticket/827341">loyalty</a>
<a href="/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXabc123/weekend-regular-tickets/5314233">regular</a>
<p>Also: https://www.ticketswap.com/festival-tickets/awakenings-upclose-2026-may-amsterdam-CXabc123/single-day-sunday/1000001</p>
</body></html>
"""

NINE_O_HUB_HTML = """
<html><body>
<script type="application/json">
  "https://www.ticketswap.com/festival-tickets/909-festival-2026-rotterdam-CXxyz/day-tickets/2000002"
</script>
<a href="/festival-tickets/a/other-festival">other hub</a>
</body></html>
"""


class TestFixtureHubExtraction(unittest.TestCase):
    def test_awakenings_hub_extracts_two_plus_regex_ticket_urls(self) -> None:
        base = "https://www.ticketswap.com/festival-tickets/a/awakenings-upclose"
        found = tsx.extract_relaxed_festival_ticket_urls_from_html(AWAKENINGS_HUB_HTML, base_url=base)
        self.assertGreaterEqual(len(found), 2, found)
        for u in found:
            self.assertIn("/festival-tickets/", u)
            self.assertRegex(u, r"/\d{5,}$")
        kinds = {tch.classify_ticketswap_url(u) for u in found}
        self.assertIn("ticket_url", kinds)

    def test_909_snippet_extracts_ticket_from_json_script(self) -> None:
        base = "https://www.ticketswap.com/festival-tickets/a/909-festival"
        found = tsx.extract_relaxed_festival_ticket_urls_from_html(NINE_O_HUB_HTML, base_url=base)
        self.assertTrue(any("2000002" in u for u in found), found)

    def test_hub_vs_dated_classification(self) -> None:
        hub = "https://www.ticketswap.com/festival-tickets/a/909-festival"
        dated = "https://www.ticketswap.com/festival-tickets/909-festival-2026-rotterdam-CXxyz"
        self.assertEqual(tch.classify_ticketswap_url(hub), "hub_url")
        self.assertEqual(tch.classify_ticketswap_url(dated), "dated_event_url")

    def test_is_ticket_url_relaxed_endings(self) -> None:
        u = "https://www.ticketswap.com/festival-tickets/foo-bar/some-type-name/5314233"
        self.assertTrue(du.is_ticket_url(u))


if __name__ == "__main__":
    unittest.main()
