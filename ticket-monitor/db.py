"""
SQLite storage for market snapshots.

Version 1 keeps schema simple and append-only: each scrape produces one row.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable, Optional
import json
import os
import sqlite3

from scrapers.base import MarketSnapshot


DDL = """
CREATE TABLE IF NOT EXISTS market_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  label TEXT NOT NULL,
  url TEXT NOT NULL,
  scraped_at TEXT NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT NULL,
  min_price REAL NULL,
  max_price REAL NULL,
  avg_price REAL NULL,
  listing_count INTEGER NULL,
  wanted_count INTEGER NULL,
  raw_payload TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_url_time
  ON market_snapshots (url, scraped_at);
"""


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()
    # Ensure V1.5 columns exist even for older DBs.
    _ensure_columns(conn)


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """
    Best-effort migration for V1 -> V1.5:
    add status/error_message columns if an existing DB predates them.
    """
    cols = conn.execute("PRAGMA table_info(market_snapshots)").fetchall()
    names = {c["name"] for c in cols}
    if "status" not in names:
        conn.execute("ALTER TABLE market_snapshots ADD COLUMN status TEXT NOT NULL DEFAULT 'unknown'")
    if "error_message" not in names:
        conn.execute("ALTER TABLE market_snapshots ADD COLUMN error_message TEXT NULL")
    conn.commit()


def save_snapshot(
    conn: sqlite3.Connection,
    snapshot: MarketSnapshot,
    *,
    status: str = "ok",
    error_message: Optional[str] = None,
) -> None:
    payload = snapshot.raw_payload
    raw_payload_json = json.dumps(payload, ensure_ascii=False, default=str) if payload is not None else None

    _ensure_columns(conn)
    conn.execute(
        """
        INSERT INTO market_snapshots (
          site_name, label, url, scraped_at,
          status, error_message,
          min_price, max_price, avg_price,
          listing_count, wanted_count, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot.site_name,
            snapshot.label,
            snapshot.url,
            snapshot.scraped_at.isoformat(),
            status,
            error_message,
            snapshot.min_price,
            snapshot.max_price,
            snapshot.avg_price,
            snapshot.listing_count,
            snapshot.wanted_count,
            raw_payload_json,
        ),
    )
    conn.commit()


def fetch_latest_snapshots(conn: sqlite3.Connection, *, limit: int = 10) -> Iterable[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM market_snapshots
        ORDER BY datetime(scraped_at) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

