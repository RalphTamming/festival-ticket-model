"""
Export `market_snapshots` and `listing_snapshots` to CSV for analysis.

CLI:
  python export_to_csv.py
  python export_to_csv.py --db path/to.db --out ./exports
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import config

SNAPSHOT_EXPORT_COLS = [
    "event_name",
    "ticket_type_label",
    "event_date_local",
    "city",
    "country",
    "scraped_at_utc",
    "listing_count",
    "sold_count",
    "wanted_count",
    "lowest_ask",
    "median_ask",
    "highest_ask",
    "average_ask",
]

# Extra columns for filtered snapshot exports (easier to judge outcomes).
SNAPSHOT_META_COLS = [
    "snapshot_id",
    "ticket_url_id",
    "ticket_url",
    "status",
    "error_message",
]

LISTING_EXPORT_COLS = [
    "snapshot_id",
    "listing_fingerprint",
    "quantity",
    "price_per_ticket",
    "currency",
    "raw_text",
    "listing_href",
]

DISCOVERY_COVERAGE_COLS = [
    "hub_url",
    "hub_slug",
    "events_found",
    "deep_ticket_urls_found",
    "active_ticket_urls_after_run",
    "pages_blocked",
    "parse_failures",
    "last_run_utc",
]

SCRAPE_COVERAGE_COLS = [
    "event_name",
    "ticket_type_label",
    "ticket_url",
    "latest_status",
    "latest_scraped_at_utc",
    "listing_count",
    "sold_count",
    "wanted_count",
    "lowest_ask",
    "median_ask",
    "highest_ask",
]

MARKET_SNAPSHOT_ALL = [
    "snapshot_id",
    "ticket_url_id",
    "ticket_url",
    "scraped_at_utc",
    "status",
    "error_message",
    "event_name",
    "event_url",
    "venue",
    "city",
    "country",
    "event_date_local",
    "ticket_type_label",
    "currency",
    "listing_count",
    "wanted_count",
    "sold_count",
    "lowest_ask",
    "highest_ask",
    "median_ask",
    "average_ask",
    "new_listings_since_prev",
    "removed_listings_since_prev",
    "estimated_sale_speed_listings_per_hour",
    "raw_debug_json",
]

FESTIVAL_SUMMARY_COLS = [
    "event_name",
    "ticket_type_label",
    "latest_scraped_at_utc",
    "latest_status",
    "event_date_local",
    "city",
    "country",
    "listing_count",
    "sold_count",
    "wanted_count",
    "lowest_ask",
    "median_ask",
    "highest_ask",
    "average_ask",
    "ticket_url",
]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def export_snapshots_csv(conn: sqlite3.Connection, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        f"SELECT {', '.join(SNAPSHOT_EXPORT_COLS)} FROM market_snapshots ORDER BY snapshot_id"
    ).fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SNAPSHOT_EXPORT_COLS)
        for r in rows:
            w.writerow([r[c] for c in SNAPSHOT_EXPORT_COLS])
    return len(rows)


def _export_snapshots_filtered(conn: sqlite3.Connection, path: Path, where_sql: str) -> int:
    cols = SNAPSHOT_META_COLS + SNAPSHOT_EXPORT_COLS
    q = f"SELECT {', '.join(cols)} FROM market_snapshots WHERE {where_sql} ORDER BY snapshot_id"
    rows = conn.execute(q).fetchall()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])
    return len(rows)


def export_snapshots_ok_csv(conn: sqlite3.Connection, path: Path) -> int:
    return _export_snapshots_filtered(conn, path, "status = 'ok'")


def export_snapshots_ok_or_no_data_csv(conn: sqlite3.Connection, path: Path) -> int:
    return _export_snapshots_filtered(conn, path, "status IN ('ok', 'no_data')")


def export_festival_summary_csv(conn: sqlite3.Connection, path: Path) -> int:
    """Latest snapshot per ticket_url_id (ticket type)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sql = """
    WITH ranked AS (
      SELECT
        ms.*,
        ROW_NUMBER() OVER (
          PARTITION BY ms.ticket_url_id
          ORDER BY ms.scraped_at_utc DESC, ms.snapshot_id DESC
        ) AS rn
      FROM market_snapshots ms
    )
    SELECT
      COALESCE(r.event_name, e.event_name) AS event_name,
      COALESCE(r.ticket_type_label, tu.ticket_type_label) AS ticket_type_label,
      r.scraped_at_utc AS latest_scraped_at_utc,
      r.status AS latest_status,
      r.event_date_local,
      r.city,
      r.country,
      r.listing_count,
      r.sold_count,
      r.wanted_count,
      r.lowest_ask,
      r.median_ask,
      r.highest_ask,
      r.average_ask,
      tu.ticket_url AS ticket_url
    FROM ranked r
    JOIN ticket_urls tu ON tu.ticket_url_id = r.ticket_url_id
    LEFT JOIN events e ON e.event_id = tu.event_id
    WHERE r.rn = 1
    ORDER BY event_name, ticket_type_label
    """
    rows = conn.execute(sql).fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(FESTIVAL_SUMMARY_COLS)
        for r in rows:
            w.writerow([r[c] for c in FESTIVAL_SUMMARY_COLS])
    return len(rows)


def export_discovery_coverage_csv(conn: sqlite3.Connection, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT
          fh.hub_url AS hub_url,
          fh.hub_slug AS hub_slug,
          COALESCE(fh.last_events_found, 0) AS events_found,
          COALESCE(fh.last_deep_ticket_urls_found, 0) AS deep_ticket_urls_found,
            (
            SELECT COUNT(*)
            FROM ticket_urls tu
            JOIN events e ON e.event_id = tu.event_id
            WHERE tu.is_active = 1
              AND (
                e.event_id = fh.hub_slug
                OR e.event_id LIKE fh.hub_slug || '-%'
                OR e.event_url LIKE '%' || fh.hub_slug || '%'
                OR e.event_id LIKE REPLACE(fh.hub_slug, '-festival', '') || '%'
                OR e.event_url LIKE '%' || REPLACE(fh.hub_slug, '-festival', '') || '%'
              )
          ) AS active_ticket_urls_after_run,
          COALESCE(fh.last_pages_blocked, 0) AS pages_blocked,
          COALESCE(fh.last_parse_failures, 0) AS parse_failures,
          fh.last_discovery_run_utc AS last_run_utc
        FROM festival_hubs fh
        WHERE fh.is_active = 1
        ORDER BY fh.last_seen_at_utc DESC
        """
    ).fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(DISCOVERY_COVERAGE_COLS)
        for r in rows:
            w.writerow([r[c] for c in DISCOVERY_COVERAGE_COLS])
    return len(rows)


def export_scrape_coverage_csv(conn: sqlite3.Connection, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    sql = """
    WITH latest AS (
      SELECT
        ms.*,
        ROW_NUMBER() OVER (
          PARTITION BY ms.ticket_url_id
          ORDER BY ms.scraped_at_utc DESC, ms.snapshot_id DESC
        ) AS rn
      FROM market_snapshots ms
    )
    SELECT
      COALESCE(l.event_name, e.event_name) AS event_name,
      COALESCE(l.ticket_type_label, tu.ticket_type_label) AS ticket_type_label,
      tu.ticket_url AS ticket_url,
      l.status AS latest_status,
      l.scraped_at_utc AS latest_scraped_at_utc,
      l.listing_count AS listing_count,
      l.sold_count AS sold_count,
      l.wanted_count AS wanted_count,
      l.lowest_ask AS lowest_ask,
      l.median_ask AS median_ask,
      l.highest_ask AS highest_ask
    FROM latest l
    JOIN ticket_urls tu ON tu.ticket_url_id = l.ticket_url_id
    LEFT JOIN events e ON e.event_id = tu.event_id
    WHERE l.rn = 1
    ORDER BY event_name, ticket_type_label
    """
    rows = conn.execute(sql).fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SCRAPE_COVERAGE_COLS)
        for r in rows:
            w.writerow([r[c] for c in SCRAPE_COVERAGE_COLS])
    return len(rows)


def export_listings_csv(conn: sqlite3.Connection, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        f"SELECT {', '.join(LISTING_EXPORT_COLS)} FROM listing_snapshots ORDER BY snapshot_id, listing_snapshot_id"
    ).fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LISTING_EXPORT_COLS)
        for r in rows:
            w.writerow([r[c] for c in LISTING_EXPORT_COLS])
    return len(rows)


def export_full_dataset_csv(conn: sqlite3.Connection, path: Path) -> int:
    """One row per listing; snapshot columns repeated. Snapshots with no listings appear once (listing fields empty)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ms_cols = ", ".join(f"ms.{c} AS ms_{c}" for c in MARKET_SNAPSHOT_ALL)
    sql = f"""
    SELECT
      {ms_cols},
      ls.listing_snapshot_id AS ls_listing_snapshot_id,
      ls.listing_fingerprint AS ls_listing_fingerprint,
      ls.quantity AS ls_quantity,
      ls.price_per_ticket AS ls_price_per_ticket,
      ls.currency AS ls_currency,
      ls.raw_text AS ls_raw_text,
      ls.seller_hint AS ls_seller_hint,
      ls.listing_href AS ls_listing_href
    FROM market_snapshots ms
    LEFT JOIN listing_snapshots ls ON ls.snapshot_id = ms.snapshot_id
    ORDER BY ms.snapshot_id, ls.listing_snapshot_id
    """
    rows = conn.execute(sql).fetchall()
    if not rows:
        headers: list[str] = []
        for c in MARKET_SNAPSHOT_ALL:
            headers.append(f"ms_{c}")
        headers.extend(
            [
                "ls_listing_snapshot_id",
                "ls_listing_fingerprint",
                "ls_quantity",
                "ls_price_per_ticket",
                "ls_currency",
                "ls_raw_text",
                "ls_seller_hint",
                "ls_listing_href",
            ]
        )
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
        return 0

    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow([r[h] for h in headers])
    return len(rows)


def export_all_csvs(
    db_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
) -> dict[str, Path]:
    db_path = db_path or config.DB_PATH
    output_dir = output_dir or Path(".")
    output_dir = Path(output_dir)
    conn = _connect(db_path)
    try:
        paths = {
            "snapshots": output_dir / "snapshots.csv",
            "snapshots_ok": output_dir / "snapshots_ok.csv",
            "snapshots_ok_or_no_data": output_dir / "snapshots_ok_or_no_data.csv",
            "festival_summary": output_dir / "festival_summary.csv",
            "listings": output_dir / "listings.csv",
            "full_dataset": output_dir / "full_dataset.csv",
            "discovery_coverage": output_dir / "discovery_coverage.csv",
            "scrape_coverage": output_dir / "scrape_coverage.csv",
        }
        export_snapshots_csv(conn, paths["snapshots"])
        export_snapshots_ok_csv(conn, paths["snapshots_ok"])
        export_snapshots_ok_or_no_data_csv(conn, paths["snapshots_ok_or_no_data"])
        export_festival_summary_csv(conn, paths["festival_summary"])
        export_listings_csv(conn, paths["listings"])
        export_full_dataset_csv(conn, paths["full_dataset"])
        export_discovery_coverage_csv(conn, paths["discovery_coverage"])
        export_scrape_coverage_csv(conn, paths["scrape_coverage"])
        return paths
    finally:
        conn.close()


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return 100.0 * part / total


def _db_status_and_quality(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0])
        print("")
        print("=== Snapshot outcomes (SQLite) ===")
        print(f"total snapshots: {total}")
        rows = conn.execute("SELECT status, COUNT(*) AS c FROM market_snapshots GROUP BY status ORDER BY status").fetchall()
        by_status = {str(r["status"]): int(r["c"]) for r in rows}
        for s in sorted(by_status.keys()):
            print(f"  {s}: {by_status[s]}")
        ok_n = by_status.get("ok", 0)
        nd_n = by_status.get("no_data", 0)
        blk_n = by_status.get("blocked", 0)
        print(f"ok: {ok_n} | no_data: {nd_n} | blocked: {blk_n} (other statuses in table above)")

        if ok_n == 0:
            print("")
            print("=== Field quality (ok snapshots only) ===")
            print("(no ok rows)")
            return

        miss_name = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM market_snapshots
                WHERE status='ok' AND (event_name IS NULL OR TRIM(event_name) = '')
                """
            ).fetchone()[0]
        )
        miss_date = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM market_snapshots
                WHERE status='ok' AND (event_date_local IS NULL OR TRIM(event_date_local) = '')
                """
            ).fetchone()[0]
        )
        miss_city = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM market_snapshots
                WHERE status='ok' AND (city IS NULL OR TRIM(city) = '')
                """
            ).fetchone()[0]
        )
        miss_country = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM market_snapshots
                WHERE status='ok' AND (country IS NULL OR TRIM(country) = '')
                """
            ).fetchone()[0]
        )
        miss_either_loc = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM market_snapshots
                WHERE status='ok'
                  AND (
                    (city IS NULL OR TRIM(city) = '')
                    OR (country IS NULL OR TRIM(country) = '')
                  )
                """
            ).fetchone()[0]
        )
        print("")
        print("=== Field quality (ok snapshots only) ===")
        print(f"% missing event_name: {_pct(miss_name, ok_n):.1f}")
        print(f"% missing event_date_local: {_pct(miss_date, ok_n):.1f}")
        print(f"% missing city: {_pct(miss_city, ok_n):.1f}")
        print(f"% missing country: {_pct(miss_country, ok_n):.1f}")
        print(f"% missing city OR country: {_pct(miss_either_loc, ok_n):.1f}")
    finally:
        conn.close()


def print_quality_report(paths: dict[str, Path], *, db_path: Optional[Path] = None) -> None:
    snap_p = paths["snapshots"]
    list_p = paths["listings"]
    full_p = paths["full_dataset"]
    db_path = db_path or config.DB_PATH

    def count_csv_rows(p: Path) -> int:
        if not p.exists():
            return 0
        with p.open(encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            return max(0, sum(1 for _ in r) - 1)

    n_snap = count_csv_rows(snap_p)
    n_ok = count_csv_rows(paths["snapshots_ok"])
    n_ok_nd = count_csv_rows(paths["snapshots_ok_or_no_data"])
    n_fs = count_csv_rows(paths["festival_summary"])
    n_list = count_csv_rows(list_p)
    n_full = count_csv_rows(full_p)

    print("")
    print("=== CSV row counts ===")
    print(f"snapshots.csv: {n_snap}")
    print(f"snapshots_ok.csv: {n_ok}")
    print(f"snapshots_ok_or_no_data.csv: {n_ok_nd}")
    print(f"festival_summary.csv: {n_fs}")
    print(f"listings.csv: {n_list}")
    print(f"full_dataset.csv: {n_full}")
    print(f"discovery_coverage.csv: {count_csv_rows(paths['discovery_coverage'])}")
    print(f"scrape_coverage.csv: {count_csv_rows(paths['scrape_coverage'])}")

    if db_path.exists():
        _db_status_and_quality(db_path)

    if not list_p.exists() or n_list == 0:
        print("")
        print("% rows missing price (listings.csv): n/a (no listing rows)")
    else:
        with list_p.open(encoding="utf-8", newline="") as f:
            lr = list(csv.DictReader(f))
        miss_price = sum(
            1
            for r in lr
            if r.get("price_per_ticket") is None or str(r.get("price_per_ticket", "")).strip() == ""
        )
        print("")
        print("=== Listings ===")
        print(f"% rows missing price (listings.csv): {_pct(miss_price, len(lr)):.1f}")

    if full_p.exists() and n_full > 0:
        with full_p.open(encoding="utf-8", newline="") as f:
            fr = list(csv.DictReader(f))
        by_event: dict[str, list[float]] = defaultdict(list)
        for r in fr:
            ev = (r.get("ms_event_name") or "").strip() or "(unknown event)"
            p = r.get("ls_price_per_ticket")
            if p is None or str(p).strip() == "":
                continue
            try:
                by_event[ev].append(float(p))
            except ValueError:
                continue
        print("")
        print("=== Min / max price per event (full_dataset, rows with listing price) ===")
        for ev in sorted(by_event.keys(), key=lambda x: x.lower()):
            prices = by_event[ev]
            print(f"  {ev}: min={min(prices):.2f} max={max(prices):.2f} (n={len(prices)})")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export TicketSwap SQLite snapshots to CSV.")
    p.add_argument("--db", type=Path, default=None, help="SQLite path (default: config.DB_PATH)")
    p.add_argument("--out", type=Path, default=Path("."), help="Output directory")
    p.add_argument("--quiet", action="store_true", help="Skip quality report")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(list(argv or []))
    db = args.db or config.DB_PATH
    paths = export_all_csvs(db_path=db, output_dir=args.out)
    print("Wrote:")
    for k, p in paths.items():
        print(f"  {k}: {p.resolve()}")
    if not args.quiet:
        print_quality_report(paths, db_path=db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
