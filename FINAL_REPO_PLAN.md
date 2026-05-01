## Final repo plan (minimal TicketSwap pipeline)

### What the pipeline does now

- **Discovery (`discover_urls.py`)**
  - Stores TicketSwap **ticket-type price URLs** into `ticketswap.db` table `ticket_urls`
  - Always accepts deep ticket URLs as seeds (so the pipeline works even if festival pages are blocked)
  - Best-effort scans festival/event pages for links when accessible

- **Scrape one page (`scrape_market.py`)**
  - Opens a TicketSwap ticket-type URL with Selenium + undetected-chromedriver
  - Parses listings and prices from HTML
  - Returns a structured `MarketSnapshot` (and per-listing fingerprints for liquidity tracking)
  - Saves HTML/screenshot to `debug/` on failures/blocks

- **Scheduler (`run_scheduler.py`)**
  - Maintains `scrape_schedule` rows in `ticketswap.db`
  - Selects due URLs, scrapes them, and stores snapshots into `market_snapshots` (+ `listing_snapshots`)
  - Applies temporary backoff after repeated failures

### DB schema (single DB: `ticketswap.db`)

Tables:
- `events`: event identity (best-effort) + room for expansion (`raw_json`)
- `ticket_urls`: canonical deep URLs, ticket type label, active flag
- `discovery_runs`: discovery audit trail
- `scrape_schedule`: next/last scrape times, interval tier, backoff state
- `market_snapshots`: one row per scrape result (status, prices, counts, debug JSON)
- `listing_snapshots`: optional per-listing fingerprints and prices per snapshot

### Field availability categories

#### A) Available now from TicketSwap directly (best-effort)

From a ticket-type price page:
- **Resale snapshot**: listing prices (min/max/median/avg), listing count proxy
- **Wanted/sold counts**: sometimes visible in page text (best-effort)
- **Ticket type label**: from page headings or URL
- **Event identity**: event name + venue/city/country (best-effort; depends on page structure)
- **Event date**: best-effort parse of visible date text (not guaranteed)

#### B) Derivable now from repeated snapshots (stored over time)

Using `listing_snapshots` and repeated `market_snapshots`:
- price movement over time (delta min/median, volatility)
- liquidity proxies (new/removed listing fingerprints)
- sale-speed proxy (removed listings per hour, crude)

#### C) Requires external enrichment later (nullable placeholders / `raw_json`)

Needs APIs or other data sources:
- weather (forecast + actual)
- search interest / social buzz / lineup strength
- official fees (buyer/seller fee pct), accurate face value
- competing events counts, calendar flags

#### D) Requires historical build-up over time

These are only possible once you have enough stored history:
- prior edition behavior curves
- final pre-event dynamics
- model horizons (1d/3d/7d) once you accumulate snapshots

### Notes on limitations (by design)

- The pipeline does **not** implement stealth/captcha solving/proxy rotation.
- If TicketSwap shows “verifying / unable to verify”, scraping may be blocked until you complete it manually.
- Discovery from festival pages may be blocked; seeding deep ticket URLs keeps the pipeline usable.

