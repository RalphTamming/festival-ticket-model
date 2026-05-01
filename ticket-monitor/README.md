# Ticket Monitor (Version 1.5)

This repository is a local MVP that **monitors ticket marketplace pages** every 10 minutes and stores **market snapshots** into **SQLite** so you can analyze price movements over time.

**Important:** Only use this on websites where automated access/scraping is allowed (Terms of Service + robots policy + any required permissions).

## What changed in Version 1.5

- **Visible browser mode** by default (`HEADLESS=false`) so you can watch scraping live
- **Manual session reuse** via Playwright **storage state** (you login/verify once, then the scraper reuses that session)
- **Debug artifacts** (optional HTML + screenshot saving) to quickly adapt selectors on real pages
- DB includes **status** + **error_message** so you can track failures/blocks/no-data runs

Non-goals: stealth plugins, anti-detection tricks, fingerprint spoofing, captcha solvers, verification bypass logic, proxy rotation, cloud deployment, Docker, Postgres.

## Project structure

```text
ticket-monitor/
├── app.py
├── config.py
├── db.py
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
├── login_and_save_session.py
├── scrapers/
│   ├── __init__.py
│   ├── base.py
│   └── example_playwright_scraper.py
│   └── real_site_scraper.py
└── logs/
```

## Setup

From the `ticket-monitor/` directory:

1) Create and activate a virtual environment

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) Install dependencies

```powershell
pip install -r requirements.txt
```

3) Install Playwright browsers (required once)

```powershell
playwright install
```

4) Create a `.env` (recommended)

Copy `.env.example` to `.env` and edit values if desired.

5) (Optional) Save a manual session (recommended for real sites)

This opens a visible browser window. You log in / complete verification yourself.
Then you press Enter in the terminal to save `playwright_state.json`.

```powershell
python login_and_save_session.py
```

6) Run the monitor

```powershell
python app.py
```

Behavior:
- Runs one scrape immediately on startup
- Then runs every `SCRAPE_INTERVAL_MINUTES`
- Logs to `logs/ticket_monitor.log` and console
- Writes SQLite DB to `DATABASE_PATH` (default: `./data/ticket_monitor.sqlite3`)

## How to customize targets

Edit `config.py` and update `TARGETS`.

Each target has:
- `site_name`: short identifier for the marketplace/site
- `label`: human-friendly label (e.g., “Event X - Standing”)
- `url`: the page to scrape
- `scraper_type`: which scraper implementation to use

Example:

```python
TARGETS = [
    Target(
        site_name="My Site",
        label="Example Event",
        url="https://example.com/tickets/event/123",
        scraper_type="real_site",
    ),
]
```

## How to change selectors (real sites)

The “real site” scraper lives in `scrapers/real_site_scraper.py`.

To adapt it, update:
- `LISTINGS_CONTAINER_SELECTOR`
- `LISTING_PRICE_SELECTOR`
- `WANTED_COUNT_SELECTOR` (optional)

If you enable debug saving (`SAVE_DEBUG_HTML=true`, `SAVE_DEBUG_SCREENSHOT=true`), the scraper will write files into `debug/` so you can inspect the exact HTML it received and quickly update selectors.

## About the scrapers (important limitations)

`scrapers/example_playwright_scraper.py` is a **template**:
- The selectors are placeholders and **will not match real sites**.
- You must replace selectors and sometimes the extraction logic for each site.
- Pages can be highly dynamic; you may need extra waits or scrolling logic.

If no prices are found, the app will still store a snapshot with prices as `NULL` and log a warning. This is intentional for stability.

## Debugging a blocked page

If you see blocker messages like “unable to verify”:
- Run `python login_and_save_session.py`
- Complete the normal verification/login manually in the visible browser
- Save the session
- Re-run `python app.py` with `USE_STORAGE_STATE=true`

If you *still* see verification even after saving storage state, enable persistent profile mode:
- `USE_PERSISTENT_CONTEXT=true`
- `USER_DATA_DIR=./pw_user_data`
- `BROWSER_CHANNEL=chrome`

This makes Playwright behave more like a normal, stable browser profile across runs.

This project **does not** include anti-bot evasion or verification bypass logic. It relies on transparent, manual, allowed flows.

## Legal / compliance reminder

This project is intended for **legal monitoring** only:
- Use only on sites where scraping/automation is permitted.
- Respect robots policies and Terms of Service.
- Keep traffic low (this repo defaults to a polite 10-minute interval).

