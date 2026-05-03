"""
Central SQLite schema + DB helpers for the minimal TicketSwap pipeline.

Single database file: `ticketswap.db`.
"""

from __future__ import annotations

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional
import uuid

if TYPE_CHECKING:
    from scraping.scrape_market import MarketSnapshot


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


DDL = """
CREATE TABLE IF NOT EXISTS discovery_runs (
  discovery_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  status TEXT NOT NULL,
  seeds_json TEXT NOT NULL,
  events_scanned INTEGER NOT NULL DEFAULT 0,
  ticket_urls_seen INTEGER NOT NULL DEFAULT 0,
  new_ticket_urls INTEGER NOT NULL DEFAULT 0,
  updated_ticket_urls INTEGER NOT NULL DEFAULT 0,
  parse_failures INTEGER NOT NULL DEFAULT 0,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  event_url TEXT NOT NULL UNIQUE,
  event_name TEXT,
  organizer_or_series TEXT,
  venue TEXT,
  city TEXT,
  country TEXT,
  start_datetime_utc TEXT,
  end_datetime_utc TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS ticket_urls (
  ticket_url_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_url TEXT NOT NULL UNIQUE,
  event_id TEXT NOT NULL,
  event_url TEXT NOT NULL,
  ticket_type_slug TEXT,
  ticket_type_label TEXT,
  first_seen_at_utc TEXT NOT NULL,
  last_seen_at_utc TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  missing_runs_count INTEGER NOT NULL DEFAULT 0,
  discovery_method TEXT NOT NULL,
  last_discovery_run_id INTEGER,
  FOREIGN KEY(event_id) REFERENCES events(event_id) ON DELETE CASCADE,
  FOREIGN KEY(last_discovery_run_id) REFERENCES discovery_runs(discovery_run_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_ticket_urls_event_id ON ticket_urls(event_id);
CREATE INDEX IF NOT EXISTS idx_ticket_urls_active ON ticket_urls(is_active, last_seen_at_utc);

CREATE TABLE IF NOT EXISTS scrape_schedule (
  ticket_url_id INTEGER PRIMARY KEY,
  active_for_scraping INTEGER NOT NULL DEFAULT 1,
  scrape_interval_minutes INTEGER NOT NULL,
  scrape_priority INTEGER NOT NULL,
  last_scraped_at_utc TEXT,
  next_scrape_at_utc TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  backoff_until_utc TEXT,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(ticket_url_id) REFERENCES ticket_urls(ticket_url_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scrape_schedule_due ON scrape_schedule(active_for_scraping, next_scrape_at_utc);

CREATE TABLE IF NOT EXISTS market_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_url_id INTEGER NOT NULL,
  ticket_url TEXT NOT NULL,
  scraped_at_utc TEXT NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT,

  event_name TEXT,
  event_url TEXT,
  venue TEXT,
  city TEXT,
  country TEXT,
  event_date_local TEXT,
  ticket_type_label TEXT,

  currency TEXT,
  listing_count INTEGER,
  wanted_count INTEGER,
  sold_count INTEGER,
  lowest_ask REAL,
  highest_ask REAL,
  median_ask REAL,
  average_ask REAL,

  new_listings_since_prev INTEGER,
  removed_listings_since_prev INTEGER,
  estimated_sale_speed_listings_per_hour REAL,

  raw_debug_json TEXT,
  FOREIGN KEY(ticket_url_id) REFERENCES ticket_urls(ticket_url_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_url_time ON market_snapshots(ticket_url_id, scraped_at_utc);

CREATE TABLE IF NOT EXISTS listing_snapshots (
  listing_snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id INTEGER NOT NULL,
  listing_fingerprint TEXT NOT NULL,
  seller_hint TEXT,
  quantity INTEGER,
  price_per_ticket REAL,
  currency TEXT,
  raw_text TEXT,
  FOREIGN KEY(snapshot_id) REFERENCES market_snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_snapshot ON listing_snapshots(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_listing_snapshots_fingerprint ON listing_snapshots(listing_fingerprint);

CREATE TABLE IF NOT EXISTS festival_hubs (
  hub_slug TEXT PRIMARY KEY,
  hub_url TEXT NOT NULL UNIQUE,
  first_seen_at_utc TEXT NOT NULL,
  last_seen_at_utc TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  source_url TEXT,
  last_discovery_run_utc TEXT,
  last_events_found INTEGER,
  last_deep_ticket_urls_found INTEGER,
  last_pages_blocked INTEGER,
  last_parse_failures INTEGER
);

CREATE INDEX IF NOT EXISTS idx_festival_hubs_active ON festival_hubs(is_active, last_seen_at_utc);

CREATE TABLE IF NOT EXISTS ticket_types (
  ticket_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_url TEXT NOT NULL UNIQUE,
  event_id TEXT NOT NULL,
  event_url TEXT NOT NULL,
  ticket_type_slug TEXT,
  ticket_type_label TEXT,
  first_seen_at_utc TEXT NOT NULL,
  last_seen_at_utc TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  FOREIGN KEY(event_id) REFERENCES events(event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ticket_types_event_id ON ticket_types(event_id);
CREATE INDEX IF NOT EXISTS idx_ticket_types_status ON ticket_types(status);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  scope TEXT,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  status TEXT NOT NULL,
  counts_json TEXT,
  error_summary TEXT
);

CREATE TABLE IF NOT EXISTS app_kv (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at_utc TEXT NOT NULL
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()
    _migrate_schema(conn)


def _migrate_schema(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, "listing_snapshots", "listing_href", "TEXT")
    # Events: keep legacy fields and add simplified pipeline fields.
    _ensure_column(conn, "events", "event_slug", "TEXT")
    _ensure_column(conn, "events", "event_date_local", "TEXT")
    _ensure_column(conn, "events", "category", "TEXT")
    _ensure_column(conn, "events", "location", "TEXT")
    _ensure_column(conn, "events", "region", "TEXT")
    _ensure_column(conn, "events", "first_seen_at_utc", "TEXT")
    _ensure_column(conn, "events", "last_seen_at_utc", "TEXT")
    _ensure_column(conn, "events", "status", "TEXT")
    # Snapshots: link to ticket_types/pipeline_runs and store listings JSON.
    _ensure_column(conn, "market_snapshots", "ticket_type_id", "INTEGER")
    _ensure_column(conn, "market_snapshots", "run_id", "TEXT")
    _ensure_column(conn, "market_snapshots", "listings_json", "TEXT")
    _ensure_column(conn, "market_snapshots", "days_until_event", "INTEGER")
    _ensure_column(conn, "market_snapshots", "hours_until_event", "REAL")
    _ensure_column(conn, "market_snapshots", "event_weekday", "TEXT")
    _ensure_column(conn, "market_snapshots", "event_month", "INTEGER")
    _ensure_column(conn, "market_snapshots", "total_available_quantity", "INTEGER")
    _ensure_column(conn, "market_snapshots", "is_sold_out", "INTEGER")
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, col_type: str) -> None:
    cols = {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def upsert_festival_hub(
    conn: sqlite3.Connection,
    *,
    hub_slug: str,
    hub_url: str,
    source_url: Optional[str],
) -> tuple[bool, bool]:
    """Returns (was_new, was_updated_last_seen)."""
    now = _utc_now_iso()
    row = conn.execute("SELECT hub_slug FROM festival_hubs WHERE hub_slug=?", (hub_slug,)).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO festival_hubs (
              hub_slug, hub_url, first_seen_at_utc, last_seen_at_utc, is_active, source_url
            ) VALUES (?, ?, ?, ?, 1, ?)
            """,
            (hub_slug, hub_url, now, now, source_url),
        )
        conn.commit()
        return True, False
    conn.execute(
        """
        UPDATE festival_hubs
        SET hub_url=?, last_seen_at_utc=?, is_active=1, source_url=COALESCE(?, source_url)
        WHERE hub_slug=?
        """,
        (hub_url, now, source_url, hub_slug),
    )
    conn.commit()
    return False, True


def list_active_festival_hub_urls(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT hub_url FROM festival_hubs WHERE is_active=1 ORDER BY last_seen_at_utc DESC"
    ).fetchall()
    return [str(r["hub_url"]) for r in rows]


def update_festival_hub_discovery_stats(
    conn: sqlite3.Connection,
    *,
    hub_slug: str,
    events_found: int,
    deep_ticket_urls_found: int,
    pages_blocked: int,
    parse_failures: int,
) -> None:
    now = _utc_now_iso()
    conn.execute(
        """
        UPDATE festival_hubs SET
          last_discovery_run_utc=?,
          last_events_found=?,
          last_deep_ticket_urls_found=?,
          last_pages_blocked=?,
          last_parse_failures=?
        WHERE hub_slug=?
        """,
        (now, events_found, deep_ticket_urls_found, pages_blocked, parse_failures, hub_slug),
    )
    conn.commit()


def count_active_ticket_urls_for_hub_slug(conn: sqlite3.Connection, hub_slug: str) -> int:
    if not hub_slug:
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM ticket_urls tu
        JOIN events e ON e.event_id = tu.event_id
        WHERE tu.is_active = 1
          AND (e.event_id = ? OR e.event_id LIKE ? OR e.event_url LIKE ?)
        """,
        (hub_slug, f"{hub_slug}-%", f"%{hub_slug}%"),
    ).fetchone()
    return int(row["c"])


def upsert_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    event_url: str,
    event_name: Optional[str],
    organizer_or_series: Optional[str] = None,
    venue: Optional[str] = None,
    city: Optional[str] = None,
    country: Optional[str] = None,
    start_datetime_utc: Optional[datetime] = None,
    end_datetime_utc: Optional[datetime] = None,
    raw: Optional[dict] = None,
) -> None:
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO events (
          event_id, event_url, event_name, organizer_or_series, venue, city, country,
          start_datetime_utc, end_datetime_utc, created_at_utc, updated_at_utc, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
          event_url=excluded.event_url,
          event_name=COALESCE(excluded.event_name, events.event_name),
          organizer_or_series=COALESCE(excluded.organizer_or_series, events.organizer_or_series),
          venue=COALESCE(excluded.venue, events.venue),
          city=COALESCE(excluded.city, events.city),
          country=COALESCE(excluded.country, events.country),
          start_datetime_utc=COALESCE(excluded.start_datetime_utc, events.start_datetime_utc),
          end_datetime_utc=COALESCE(excluded.end_datetime_utc, events.end_datetime_utc),
          updated_at_utc=excluded.updated_at_utc,
          raw_json=COALESCE(excluded.raw_json, events.raw_json)
        """,
        (
            event_id,
            event_url,
            event_name,
            organizer_or_series,
            venue,
            city,
            country,
            start_datetime_utc.isoformat().replace("+00:00", "Z") if start_datetime_utc else None,
            end_datetime_utc.isoformat().replace("+00:00", "Z") if end_datetime_utc else None,
            now,
            now,
            _safe_json(raw) if raw else None,
        ),
    )
    conn.commit()


def upsert_ticket_url(
    conn: sqlite3.Connection,
    *,
    ticket_url: str,
    event_id: str,
    event_url: str,
    ticket_type_slug: Optional[str],
    ticket_type_label: Optional[str],
    discovery_method: str,
    discovery_run_id: Optional[int],
) -> tuple[bool, bool, int]:
    """
    Returns (was_new, was_updated, ticket_url_id).
    """
    now = _utc_now_iso()
    row = conn.execute("SELECT ticket_url_id FROM ticket_urls WHERE ticket_url=?", (ticket_url,)).fetchone()
    if row is None:
        cur = conn.execute(
            """
            INSERT INTO ticket_urls (
              ticket_url, event_id, event_url, ticket_type_slug, ticket_type_label,
              first_seen_at_utc, last_seen_at_utc, is_active, missing_runs_count,
              discovery_method, last_discovery_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?)
            """,
            (
                ticket_url,
                event_id,
                event_url,
                ticket_type_slug,
                ticket_type_label,
                now,
                now,
                discovery_method,
                discovery_run_id,
            ),
        )
        conn.commit()
        return True, False, int(cur.lastrowid)

    ticket_url_id = int(row["ticket_url_id"])
    conn.execute(
        """
        UPDATE ticket_urls
        SET
          event_id=?,
          event_url=?,
          ticket_type_slug=COALESCE(?, ticket_type_slug),
          ticket_type_label=COALESCE(?, ticket_type_label),
          last_seen_at_utc=?,
          is_active=1,
          missing_runs_count=0,
          discovery_method=?,
          last_discovery_run_id=?
        WHERE ticket_url_id=?
        """,
        (event_id, event_url, ticket_type_slug, ticket_type_label, now, discovery_method, discovery_run_id, ticket_url_id),
    )
    conn.commit()
    return False, True, ticket_url_id


def create_discovery_run(conn: sqlite3.Connection, *, seeds_json: str) -> int:
    cur = conn.execute(
        "INSERT INTO discovery_runs (started_at_utc, status, seeds_json) VALUES (?, ?, ?)",
        (_utc_now_iso(), "running", seeds_json),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_discovery_run(
    conn: sqlite3.Connection,
    discovery_run_id: int,
    *,
    status: str,
    events_scanned: int,
    ticket_urls_seen: int,
    new_ticket_urls: int,
    updated_ticket_urls: int,
    parse_failures: int,
    notes: Optional[str] = None,
) -> None:
    conn.execute(
        """
        UPDATE discovery_runs
        SET finished_at_utc=?, status=?, events_scanned=?, ticket_urls_seen=?, new_ticket_urls=?,
            updated_ticket_urls=?, parse_failures=?, notes=?
        WHERE discovery_run_id=?
        """,
        (
            _utc_now_iso(),
            status,
            events_scanned,
            ticket_urls_seen,
            new_ticket_urls,
            updated_ticket_urls,
            parse_failures,
            notes,
            discovery_run_id,
        ),
    )
    conn.commit()


def mark_missing_ticket_urls(conn: sqlite3.Connection, *, seen_urls: set[str], missing_runs_threshold: int) -> int:
    rows = conn.execute("SELECT ticket_url_id, ticket_url, missing_runs_count FROM ticket_urls WHERE is_active=1").fetchall()
    newly_inactivated = 0
    for r in rows:
        url = str(r["ticket_url"])
        if url in seen_urls:
            continue
        missing = int(r["missing_runs_count"] or 0) + 1
        is_active = 0 if missing >= missing_runs_threshold else 1
        if is_active == 0:
            newly_inactivated += 1
        conn.execute(
            "UPDATE ticket_urls SET missing_runs_count=?, is_active=? WHERE ticket_url_id=?",
            (missing, is_active, int(r["ticket_url_id"])),
        )
    conn.commit()
    return newly_inactivated


def insert_market_snapshot(conn: sqlite3.Connection, *, ticket_url_id: int, snap: MarketSnapshot) -> int:
    cur = conn.execute(
        """
        INSERT INTO market_snapshots (
          ticket_url_id, ticket_url, scraped_at_utc, status, error_message,
          event_name, event_url, venue, city, country, event_date_local, ticket_type_label,
          currency, listing_count, wanted_count, sold_count, lowest_ask, highest_ask, median_ask, average_ask,
          new_listings_since_prev, removed_listings_since_prev, estimated_sale_speed_listings_per_hour,
          raw_debug_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_url_id,
            snap.ticket_url,
            snap.scraped_at_utc.isoformat().replace("+00:00", "Z") if getattr(snap.scraped_at_utc, "tzinfo", None) else str(snap.scraped_at_utc),
            snap.status,
            snap.error_message,
            snap.event_name,
            snap.event_url,
            snap.venue,
            snap.city,
            snap.country,
            snap.event_date_local,
            snap.ticket_type_label,
            snap.currency,
            snap.listing_count,
            snap.wanted_count,
            snap.sold_count,
            snap.lowest_ask,
            snap.highest_ask,
            snap.median_ask,
            snap.average_ask,
            snap.new_listings_since_prev,
            snap.removed_listings_since_prev,
            snap.estimated_sale_speed_listings_per_hour,
            _safe_json(snap.raw_debug) if snap.raw_debug else None,
        ),
    )
    snapshot_id = int(cur.lastrowid)

    for l in snap.listings:
        insert_listing_snapshot(conn, snapshot_id=snapshot_id, listing=l)

    conn.commit()
    return snapshot_id


def insert_listing_snapshot(conn: sqlite3.Connection, *, snapshot_id: int, listing: Any) -> None:
    href = getattr(listing, "listing_href", None)
    conn.execute(
        """
        INSERT INTO listing_snapshots (
          snapshot_id, listing_fingerprint, seller_hint, quantity, price_per_ticket, currency, raw_text, listing_href
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            listing.listing_fingerprint,
            listing.seller_hint,
            listing.quantity,
            listing.price_per_ticket,
            listing.currency,
            listing.raw_text,
            href,
        ),
    )


def get_ticket_urls_due(conn: sqlite3.Connection, *, limit: int) -> Iterable[sqlite3.Row]:
    now = _utc_now_iso()
    return conn.execute(
        """
        SELECT
          tu.ticket_url_id,
          tu.ticket_url,
          tu.event_url,
          tu.ticket_type_label,
          e.event_name,
          e.start_datetime_utc,
          ss.scrape_interval_minutes,
          ss.scrape_priority,
          ss.last_scraped_at_utc,
          ss.next_scrape_at_utc,
          ss.consecutive_failures,
          ss.backoff_until_utc,
          ss.active_for_scraping
        FROM ticket_urls tu
        JOIN events e ON e.event_id = tu.event_id
        JOIN scrape_schedule ss ON ss.ticket_url_id = tu.ticket_url_id
        WHERE
          tu.is_active=1
          AND ss.active_for_scraping=1
          AND (ss.next_scrape_at_utc IS NULL OR ss.next_scrape_at_utc <= ?)
          AND (ss.backoff_until_utc IS NULL OR ss.backoff_until_utc <= ?)
        ORDER BY ss.scrape_priority DESC, ss.next_scrape_at_utc ASC
        LIMIT ?
        """,
        (now, now, int(limit)),
    ).fetchall()


def upsert_schedule_row(
    conn: sqlite3.Connection,
    *,
    ticket_url_id: int,
    active_for_scraping: bool,
    scrape_interval_minutes: int,
    scrape_priority: int,
    next_scrape_at_utc: Optional[datetime],
    update_next: bool,
) -> None:
    now = _utc_now_iso()
    next_iso = next_scrape_at_utc.isoformat().replace("+00:00", "Z") if next_scrape_at_utc else None
    if update_next:
        conn.execute(
            """
            INSERT INTO scrape_schedule (
              ticket_url_id, active_for_scraping, scrape_interval_minutes, scrape_priority,
              last_scraped_at_utc, next_scrape_at_utc, consecutive_failures, backoff_until_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, NULL, ?, 0, NULL, ?)
            ON CONFLICT(ticket_url_id) DO UPDATE SET
              active_for_scraping=excluded.active_for_scraping,
              scrape_interval_minutes=excluded.scrape_interval_minutes,
              scrape_priority=excluded.scrape_priority,
              next_scrape_at_utc=excluded.next_scrape_at_utc,
              consecutive_failures=0,
              backoff_until_utc=NULL,
              updated_at_utc=excluded.updated_at_utc
            """,
            (ticket_url_id, 1 if active_for_scraping else 0, scrape_interval_minutes, scrape_priority, next_iso, now),
        )
    else:
        conn.execute(
            """
            INSERT INTO scrape_schedule (
              ticket_url_id, active_for_scraping, scrape_interval_minutes, scrape_priority,
              last_scraped_at_utc, next_scrape_at_utc, consecutive_failures, backoff_until_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, NULL, ?, 0, NULL, ?)
            ON CONFLICT(ticket_url_id) DO UPDATE SET
              active_for_scraping=excluded.active_for_scraping,
              scrape_interval_minutes=excluded.scrape_interval_minutes,
              scrape_priority=excluded.scrape_priority,
              updated_at_utc=excluded.updated_at_utc
            """,
            (ticket_url_id, 1 if active_for_scraping else 0, scrape_interval_minutes, scrape_priority, next_iso, now),
        )
    conn.commit()


def mark_scrape_success(conn: sqlite3.Connection, *, ticket_url_id: int, next_scrape_at_utc: datetime) -> None:
    now = _utc_now_iso()
    conn.execute(
        """
        UPDATE scrape_schedule
        SET last_scraped_at_utc=?,
            next_scrape_at_utc=?,
            consecutive_failures=0,
            backoff_until_utc=NULL,
            updated_at_utc=?
        WHERE ticket_url_id=?
        """,
        (now, next_scrape_at_utc.isoformat().replace("+00:00", "Z"), now, ticket_url_id),
    )
    conn.commit()


def mark_scrape_failure(
    conn: sqlite3.Connection,
    *,
    ticket_url_id: int,
    consecutive_failures: int,
    backoff_until_utc: datetime,
    next_scrape_at_utc: datetime,
) -> None:
    now = _utc_now_iso()
    conn.execute(
        """
        UPDATE scrape_schedule
        SET consecutive_failures=?,
            backoff_until_utc=?,
            next_scrape_at_utc=?,
            updated_at_utc=?
        WHERE ticket_url_id=?
        """,
        (
            consecutive_failures,
            backoff_until_utc.isoformat().replace("+00:00", "Z"),
            next_scrape_at_utc.isoformat().replace("+00:00", "Z"),
            now,
            ticket_url_id,
        ),
    )
    conn.commit()


def create_pipeline_run(
    conn: sqlite3.Connection,
    *,
    mode: str,
    scope: Optional[str],
) -> str:
    run_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, mode, scope, started_at_utc, status)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, mode, scope, _utc_now_iso(), "running"),
    )
    conn.commit()
    return run_id


def finish_pipeline_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    counts: Optional[dict[str, Any]] = None,
    error_summary: Optional[str] = None,
) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs
        SET finished_at_utc=?, status=?, counts_json=?, error_summary=?
        WHERE run_id=?
        """,
        (_utc_now_iso(), status, _safe_json(counts) if counts is not None else None, error_summary, run_id),
    )
    conn.commit()


def upsert_event_record(
    conn: sqlite3.Connection,
    *,
    event_url: str,
    event_slug: str,
    event_name: Optional[str],
    event_date_local: Optional[str],
    category: str,
    location: Optional[str],
    country: Optional[str],
    region: Optional[str],
    status: str = "active",
) -> str:
    """
    Upsert into events and return event_id.
    Uses event_slug as stable event_id in this simplified pipeline.
    """
    now = _utc_now_iso()
    event_id = event_slug
    conn.execute(
        """
        INSERT INTO events (
          event_id, event_url, event_slug, event_name, event_date_local, category,
          location, city, country, region, first_seen_at_utc, last_seen_at_utc, status,
          created_at_utc, updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_url) DO UPDATE SET
          event_slug=excluded.event_slug,
          event_name=COALESCE(excluded.event_name, events.event_name),
          event_date_local=COALESCE(excluded.event_date_local, events.event_date_local),
          category=COALESCE(excluded.category, events.category),
          location=COALESCE(excluded.location, events.location),
          city=COALESCE(excluded.city, events.city),
          country=COALESCE(excluded.country, events.country),
          region=COALESCE(excluded.region, events.region),
          last_seen_at_utc=excluded.last_seen_at_utc,
          status=excluded.status,
          updated_at_utc=excluded.updated_at_utc
        """,
        (
            event_id,
            event_url,
            event_slug,
            event_name,
            event_date_local,
            category,
            location,
            location,
            country,
            region,
            now,
            now,
            status,
            now,
            now,
        ),
    )
    row = conn.execute("SELECT event_id FROM events WHERE event_url=?", (event_url,)).fetchone()
    conn.commit()
    return str(row["event_id"]) if row and row["event_id"] else event_id


def upsert_ticket_type_record(
    conn: sqlite3.Connection,
    *,
    ticket_url: str,
    event_id: str,
    event_url: str,
    ticket_type_slug: Optional[str],
    ticket_type_label: Optional[str],
    status: str = "active",
) -> int:
    now = _utc_now_iso()
    row = conn.execute("SELECT ticket_type_id FROM ticket_types WHERE ticket_url=?", (ticket_url,)).fetchone()
    if row is None:
        cur = conn.execute(
            """
            INSERT INTO ticket_types (
              ticket_url, event_id, event_url, ticket_type_slug, ticket_type_label,
              first_seen_at_utc, last_seen_at_utc, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ticket_url, event_id, event_url, ticket_type_slug, ticket_type_label, now, now, status),
        )
        conn.commit()
        return int(cur.lastrowid)
    ticket_type_id = int(row["ticket_type_id"])
    conn.execute(
        """
        UPDATE ticket_types
        SET event_id=?, event_url=?, ticket_type_slug=COALESCE(?, ticket_type_slug),
            ticket_type_label=COALESCE(?, ticket_type_label), last_seen_at_utc=?, status=?
        WHERE ticket_type_id=?
        """,
        (event_id, event_url, ticket_type_slug, ticket_type_label, now, status, ticket_type_id),
    )
    conn.commit()
    return ticket_type_id


def list_ticket_types_for_monitoring(conn: sqlite3.Connection, *, limit: Optional[int] = None) -> list[sqlite3.Row]:
    sql = """
    SELECT
      tt.ticket_type_id,
      tt.ticket_url,
      tt.ticket_type_slug,
      tt.ticket_type_label,
      tt.status AS ticket_status,
      tt.last_seen_at_utc,
      e.event_id,
      e.event_url,
      e.event_slug,
      e.event_name,
      e.event_date_local,
      e.start_datetime_utc,
      e.category,
      e.location,
      e.country,
      e.region,
      e.status AS event_status
    FROM ticket_types tt
    JOIN events e ON e.event_id = tt.event_id
    WHERE tt.status = 'active' AND COALESCE(e.status, 'active') <> 'inactive'
    ORDER BY COALESCE(e.event_date_local, '9999-12-31') ASC, tt.ticket_type_id ASC
    """
    if limit is None:
        return conn.execute(sql).fetchall()
    return conn.execute(sql + " LIMIT ?", (int(limit),)).fetchall()


def list_ticket_urls_for_event(conn: sqlite3.Connection, *, event_url: str) -> list[str]:
    """
    Read known ticket URLs for an event from both new and legacy tables.
    """
    out: list[str] = []
    rows = conn.execute(
        """
        SELECT ticket_url
        FROM ticket_types
        WHERE event_url = ? AND status = 'active'
        ORDER BY ticket_type_id
        """,
        (event_url,),
    ).fetchall()
    out.extend([str(r["ticket_url"]) for r in rows if r and r["ticket_url"]])
    if not out:
        rows_old = conn.execute(
            """
            SELECT ticket_url
            FROM ticket_urls
            WHERE event_url = ? AND is_active = 1
            ORDER BY ticket_url_id
            """,
            (event_url,),
        ).fetchall()
        out.extend([str(r["ticket_url"]) for r in rows_old if r and r["ticket_url"]])
    # de-dup while preserving order
    return list(dict.fromkeys(out))


def latest_snapshot_for_ticket_type(conn: sqlite3.Connection, ticket_type_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM market_snapshots
        WHERE ticket_type_id=?
        ORDER BY scraped_at_utc DESC, snapshot_id DESC
        LIMIT 1
        """,
        (int(ticket_type_id),),
    ).fetchone()


def insert_market_snapshot_for_ticket_type(
    conn: sqlite3.Connection,
    *,
    ticket_type_id: int,
    run_id: Optional[str],
    snap: MarketSnapshot,
) -> int:
    ticket_url_id_row = conn.execute(
        "SELECT ticket_url_id FROM ticket_urls WHERE ticket_url=?",
        (snap.ticket_url,),
    ).fetchone()
    if ticket_url_id_row is None:
        tt_row = conn.execute(
            """
            SELECT ticket_url, event_id, event_url, ticket_type_slug, ticket_type_label
            FROM ticket_types
            WHERE ticket_type_id=?
            """,
            (int(ticket_type_id),),
        ).fetchone()
        if tt_row is not None:
            cur_ticket = conn.execute(
                """
                INSERT INTO ticket_urls (
                  ticket_url, event_id, event_url, ticket_type_slug, ticket_type_label,
                  first_seen_at_utc, last_seen_at_utc, is_active, missing_runs_count,
                  discovery_method, last_discovery_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 'monitoring_seed', NULL)
                """,
                (
                    str(tt_row["ticket_url"]),
                    str(tt_row["event_id"]),
                    str(tt_row["event_url"]),
                    tt_row["ticket_type_slug"],
                    tt_row["ticket_type_label"],
                    _utc_now_iso(),
                    _utc_now_iso(),
                ),
            )
            ticket_url_id = int(cur_ticket.lastrowid)
        else:
            raise ValueError(f"Unable to map ticket_type_id={ticket_type_id} to legacy ticket_url_id.")
    else:
        ticket_url_id = int(ticket_url_id_row["ticket_url_id"])

    listings_payload = [_listing_json_payload(x) for x in (snap.listings or [])]
    total_available_quantity = sum(
        int(item.get("quantity", 0) or 0) for item in listings_payload if item.get("quantity") is not None
    )
    cur = conn.execute(
        """
        INSERT INTO market_snapshots (
          ticket_url_id, ticket_type_id, run_id, ticket_url, scraped_at_utc, status, error_message,
          event_name, event_url, venue, city, country, event_date_local, ticket_type_label,
          currency, listing_count, wanted_count, sold_count, lowest_ask, highest_ask, median_ask, average_ask,
          new_listings_since_prev, removed_listings_since_prev, estimated_sale_speed_listings_per_hour,
          listings_json, raw_debug_json, days_until_event, hours_until_event, event_weekday, event_month,
          total_available_quantity, is_sold_out
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_url_id,
            int(ticket_type_id),
            run_id,
            snap.ticket_url,
            snap.scraped_at_utc.isoformat().replace("+00:00", "Z")
            if getattr(snap.scraped_at_utc, "tzinfo", None)
            else str(snap.scraped_at_utc),
            snap.status,
            snap.error_message,
            snap.event_name,
            snap.event_url,
            snap.venue,
            snap.city,
            snap.country,
            snap.event_date_local,
            snap.ticket_type_label,
            snap.currency,
            snap.listing_count,
            snap.wanted_count,
            snap.sold_count,
            snap.lowest_ask,
            snap.highest_ask,
            snap.median_ask,
            snap.average_ask,
            snap.new_listings_since_prev,
            snap.removed_listings_since_prev,
            snap.estimated_sale_speed_listings_per_hour,
            _safe_json(listings_payload) if listings_payload else "[]",
            _safe_json(snap.raw_debug) if snap.raw_debug else None,
            _event_days_until(snap),
            _event_hours_until(snap),
            _event_weekday(snap),
            _event_month(snap),
            total_available_quantity,
            1 if int(snap.listing_count or 0) == 0 else 0,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _event_days_until(snap: MarketSnapshot) -> Optional[int]:
    event_dt = _event_datetime_utc_from_snapshot(snap)
    if event_dt is None:
        return None
    now = datetime.now(timezone.utc)
    return int((event_dt.date() - now.date()).days)


def _event_hours_until(snap: MarketSnapshot) -> Optional[float]:
    event_dt = _event_datetime_utc_from_snapshot(snap)
    if event_dt is None:
        return None
    now = datetime.now(timezone.utc)
    return round((event_dt - now).total_seconds() / 3600.0, 3)


def _event_weekday(snap: MarketSnapshot) -> Optional[str]:
    event_dt = _event_datetime_utc_from_snapshot(snap)
    if event_dt is None:
        return None
    return event_dt.strftime("%A")


def _event_month(snap: MarketSnapshot) -> Optional[int]:
    event_dt = _event_datetime_utc_from_snapshot(snap)
    if event_dt is None:
        return None
    return int(event_dt.month)


def _event_datetime_utc_from_snapshot(snap: MarketSnapshot) -> Optional[datetime]:
    val = (snap.event_date_local or "").strip()
    if not val:
        return None
    try:
        return datetime.fromisoformat(f"{val}T00:00:00+00:00")
    except ValueError:
        return None


def _listing_json_payload(listing: Any) -> dict[str, Any]:
    href = getattr(listing, "listing_href", None)
    listing_id = _listing_id_from_href(href)
    return {
        "listing_id": listing_id,
        "listing_url": href,
        "price": getattr(listing, "price_per_ticket", None),
        "quantity": getattr(listing, "quantity", None),
        "currency": getattr(listing, "currency", None),
        "seller_hint": getattr(listing, "seller_hint", None),
        "fingerprint": getattr(listing, "listing_fingerprint", None),
        "raw_text": getattr(listing, "raw_text", None),
    }


def _listing_id_from_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    digits = "".join(ch for ch in str(href).rstrip("/").split("/")[-1] if ch.isdigit())
    return digits or None


def kv_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM app_kv WHERE key=?", (str(key),)).fetchone()
    if row is None:
        return None
    return row["value"]


def kv_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO app_kv (key, value, updated_at_utc) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_utc=excluded.updated_at_utc
        """,
        (str(key), str(value), now),
    )
    conn.commit()

