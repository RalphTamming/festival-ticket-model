# TicketSwap minimal pipeline

This repo contains **one** simple, working pipeline for TicketSwap data collection:

- `discover_urls.py` discovers/updates TicketSwap ticket-type URLs in SQLite
- `run_scheduler.py` selects due URLs, scrapes them, and stores snapshots in SQLite
- `scrape_market.py` scrapes **one** TicketSwap ticket-type URL (manual tool)

All state is stored in a single SQLite DB: `ticketswap.db`.

## Setup

Install Python deps you already use in this project:

- `undetected-chromedriver`
- `selenium`
- `beautifulsoup4`

Make sure Google Chrome is installed. If you see a Chrome/ChromeDriver mismatch, set `CHROME_VERSION_MAIN` in `config.py` (default is 146).

## 1) Run discovery

Edit `SEED_URLS` in `config.py` (festival pages and/or direct deep ticket URLs), then run:

```powershell
python discover_urls.py
```

## 2) Run scheduler once (scrapes due URLs)

```powershell
python run_scheduler.py
```

Testing helpers:

- Force everything due now:

```powershell
python run_scheduler.py --force-due-once
```

- Print due URLs only:

```powershell
python run_scheduler.py --print-only
```

## 3) Manual single-page scrape

```powershell
python scrape_market.py --url "<ticketswap ticket-type price url>"
```

## Debugging

- Debug artifacts (HTML / screenshots on failures) go to `./debug/`.

