#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/outputs data/debug data/backups data/exports

LOG_FILE="logs/week_production_test.log"
touch "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

DISCOVERY_SCOPE="western_europe_festivals_verified"
DISCOVERY_INTERVAL_SECONDS=86400
RUN_DURATION_SECONDS=$((7 * 24 * 3600))
TZ_NAME="Europe/Amsterdam"
SUMMARY_HOUR=23
SUMMARY_MINUTE=30

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
else
  echo "[$(date -Is)] ERROR: .venv not found at $ROOT_DIR/.venv"
  exit 1
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

export LOCAL_TIMEZONE="$TZ_NAME"
export DAILY_REPORT_TIME="23:30"
export TELEGRAM_ERROR_ONLY_MODE="true"
export ENABLE_WEEKLY_EXPORT="false"

START_EPOCH="$(date +%s)"
END_EPOCH="$((START_EPOCH + RUN_DURATION_SECONDS))"
NEXT_DISCOVERY_EPOCH="$START_EPOCH"
LAST_MONITOR_KEY=""
LAST_SUMMARY_DATE=""

RUN_FAILED=0
RUN_FAIL_REASON=""

send_telegram_message() {
  local text="$1"
  python - "$text" <<'PY'
import json
import os
import sys
from urllib.request import Request, urlopen

text = sys.argv[1]
token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
if not token or not chat_id:
    raise SystemExit(0)
body = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
req = Request(
    url=f"https://api.telegram.org/bot{token}/sendMessage",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urlopen(req, timeout=15):
        pass
except Exception:
    pass
PY
}

send_telegram_document_if_small() {
  local file_path="$1"
  local caption="$2"
  python - "$file_path" "$caption" <<'PY'
import os
import sys
from pathlib import Path
from urllib.request import Request, urlopen

file_path = Path(sys.argv[1])
caption = sys.argv[2]
token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
if not token or not chat_id or (not file_path.exists()):
    raise SystemExit(0)
if file_path.stat().st_size > (45 * 1024 * 1024):
    raise SystemExit(0)
boundary = "----TicketSwapBoundaryWeekRun"
payload = []
payload.extend([f"--{boundary}\r\n".encode(), b'Content-Disposition: form-data; name="chat_id"\r\n\r\n', f"{chat_id}\r\n".encode()])
if caption:
    payload.extend([f"--{boundary}\r\n".encode(), b'Content-Disposition: form-data; name="caption"\r\n\r\n', f"{caption}\r\n".encode()])
payload.extend(
    [
        f"--{boundary}\r\n".encode(),
        f'Content-Disposition: form-data; name="document"; filename="{file_path.name}"\r\n'.encode(),
        b"Content-Type: text/csv\r\n\r\n",
        file_path.read_bytes(),
        b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ]
)
req = Request(
    url=f"https://api.telegram.org/bot{token}/sendDocument",
    data=b"".join(payload),
    headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    method="POST",
)
try:
    with urlopen(req, timeout=30):
        pass
except Exception:
    pass
PY
}

alert_problem() {
  local title="$1"
  local details="$2"
  send_telegram_message "TicketSwap ALERT: ${title}
${details}"
}

on_exit() {
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    alert_problem "script_crash" "run_week_production_test.sh exited unexpectedly (code=${rc})"
  fi
}
trap on_exit EXIT

verify_scope_ready() {
  python <<'PY'
import json
from pathlib import Path

import config

locations = [
    ("Amsterdam", "Netherlands"),
    ("Rotterdam", "Netherlands"),
    ("Utrecht", "Netherlands"),
    ("Eindhoven", "Netherlands"),
    ("Groningen", "Netherlands"),
    ("Brussels", "Belgium"),
    ("Antwerp", "Belgium"),
    ("Ghent", "Belgium"),
    ("Berlin", "Germany"),
    ("Hamburg", "Germany"),
    ("Cologne", "Germany"),
    ("Munich", "Germany"),
    ("Paris", "France"),
    ("Lyon", "France"),
    ("Marseille", "France"),
]
cache_path = Path("data/location_cache.json")
cache = {}
if cache_path.exists():
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            cache = raw
    except Exception:
        cache = {}
missing = []
for city, country in locations:
    key = f"{city},{country}"
    entry = cache.get(key)
    if not isinstance(entry, dict):
        missing.append(key)
        continue
    if not str(entry.get("resulting_url", "") or "").strip():
        missing.append(key)
scope_urls = list(config.SCOPES.get("western_europe_festivals_verified", {}).get("listing_urls", []))
payload = {"scope_urls": len(scope_urls), "missing_cache_entries": missing}
print(json.dumps(payload))
if len(scope_urls) == 0:
    raise SystemExit(3)
PY
}

latest_run_payload() {
  local mode="$1"
  python - "$mode" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

mode = sys.argv[1]
db_path = Path("ticketswap.db")
if not db_path.exists():
    print(json.dumps({"status": "missing_db", "counts": {}}))
    raise SystemExit(0)
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row
row = conn.execute(
    "SELECT run_id, mode, scope, status, counts_json, error_summary, started_at_utc, finished_at_utc "
    "FROM pipeline_runs WHERE mode=? ORDER BY started_at_utc DESC LIMIT 1",
    (mode,),
).fetchone()
if row is None:
    print(json.dumps({"status": "missing_run", "counts": {}}))
    raise SystemExit(0)
counts = {}
if row["counts_json"]:
    try:
        counts = json.loads(row["counts_json"])
    except Exception:
        counts = {}
print(
    json.dumps(
        {
            "status": row["status"],
            "scope": row["scope"],
            "error_summary": row["error_summary"],
            "counts": counts,
            "started_at_utc": row["started_at_utc"],
            "finished_at_utc": row["finished_at_utc"],
        }
    )
)
PY
}

run_discovery() {
  echo "=== DISCOVERY START $(date -Is) ==="
  local tmp_log
  tmp_log="$(mktemp)"
  xvfb-run -a python run_pipeline.py \
    --mode discovery \
    --scope "$DISCOVERY_SCOPE" \
    --headed \
    --vps-safe-mode \
    --step2-browser selenium \
    --require-fresh-step2 \
    --wait-for-manual-verification 2>&1 | tee "$tmp_log"
  local rc=${PIPESTATUS[0]}
  local out
  out="$(cat "$tmp_log")"
  rm -f "$tmp_log"

  if [[ $rc -ne 0 ]]; then
    RUN_FAILED=1
    RUN_FAIL_REASON="discovery command failed (code=${rc})"
    alert_problem "discovery_crash_or_failure" "$RUN_FAIL_REASON"
    if [[ "$out" == *"sqlite"* ]] || [[ "$out" == *"database"* ]]; then
      alert_problem "database_error" "Detected DB error during discovery"
    fi
    return 1
  fi

  local payload
  payload="$(latest_run_payload discovery)"
  echo "[$(date -Is)] discovery payload: $payload"
  python - "$payload" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
status = str(data.get("status") or "")
counts = data.get("counts") or {}
blocked = int(counts.get("step2_blocked", 0) or 0)
if status in {"failed", "verification_blocked", "verification_blocked_partial"}:
    raise SystemExit(10)
if blocked > 0:
    raise SystemExit(11)
PY
  local check_rc=$?
  if [[ $check_rc -ne 0 ]]; then
    RUN_FAILED=1
    RUN_FAIL_REASON="discovery status indicates fresh STEP2 failure/blocking"
    alert_problem "fresh_step2_failure_or_blocking" "$payload"
    return 1
  fi

  echo "=== DISCOVERY END $(date -Is) RESULT=SUCCESS ==="
  return 0
}

run_monitoring() {
  echo "=== MONITORING START $(date -Is) ==="
  local tmp_log
  tmp_log="$(mktemp)"
  xvfb-run -a python run_pipeline.py --mode monitoring --headed 2>&1 | tee "$tmp_log"
  local rc=${PIPESTATUS[0]}
  local out
  out="$(cat "$tmp_log")"
  rm -f "$tmp_log"

  if [[ $rc -ne 0 ]]; then
    RUN_FAILED=1
    RUN_FAIL_REASON="monitoring command failed (code=${rc})"
    alert_problem "script_crash_or_monitoring_failure" "$RUN_FAIL_REASON"
    if [[ "$out" == *"sqlite"* ]] || [[ "$out" == *"database"* ]]; then
      alert_problem "database_error" "Detected DB error during monitoring"
    fi
    return 1
  fi

  local payload
  payload="$(latest_run_payload monitoring)"
  echo "[$(date -Is)] monitoring payload: $payload"
  python - "$payload" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
status = str(data.get("status") or "")
counts = data.get("counts") or {}
if status == "failed":
    raise SystemExit(20)
scrape_error = int(counts.get("scrape_error", 0) or 0)
due = int(counts.get("due_ticket_types", 0) or 0)
if scrape_error >= 5 or (scrape_error >= 3 and due > 0 and (scrape_error / max(due, 1)) >= 0.30):
    raise SystemExit(21)
PY
  local check_rc=$?
  if [[ $check_rc -eq 20 ]]; then
    alert_problem "monitoring_failure" "$payload"
    return 1
  fi
  if [[ $check_rc -eq 21 ]]; then
    alert_problem "scrape_error_spike" "$payload"
  fi
  echo "=== MONITORING END $(date -Is) RESULT=SUCCESS ==="
  return 0
}

send_daily_summary() {
  local today_local="$1"
  local summary
  summary="$(python <<'PY'
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

tz = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Europe/Amsterdam"))
now_local = datetime.now(tz)
day_start_local = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=tz)
day_end_local = day_start_local + timedelta(days=1)
day_start_utc = day_start_local.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
day_end_utc = day_end_local.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

conn = sqlite3.connect("ticketswap.db")
conn.row_factory = sqlite3.Row
runs = conn.execute(
    """
    SELECT mode, status, counts_json, error_summary
    FROM pipeline_runs
    WHERE started_at_utc >= ? AND started_at_utc < ?
    """,
    (day_start_utc, day_end_utc),
).fetchall()

discovery_runs = sum(1 for r in runs if str(r["mode"]) == "discovery")
monitoring_runs = sum(1 for r in runs if str(r["mode"]) == "monitoring")

acc = {
    "scrape_ok": 0,
    "scrape_no_data": 0,
    "scrape_blocked": 0,
    "scrape_error": 0,
    "step2_blocked": 0,
    "step2_fresh_ok": 0,
}
failures = 0
for r in runs:
    if str(r["status"]) == "failed" or str(r["error_summary"] or "").strip():
        failures += 1
    try:
        counts = json.loads(r["counts_json"] or "{}")
    except Exception:
        counts = {}
    for k in acc:
        acc[k] += int(counts.get(k, 0) or 0)

events_tracked = int(conn.execute("SELECT COUNT(*) AS c FROM events WHERE COALESCE(status,'active')='active'").fetchone()["c"])
ticket_types_tracked = int(conn.execute("SELECT COUNT(*) AS c FROM ticket_types WHERE status='active'").fetchone()["c"])
snapshots_collected = int(
    conn.execute(
        "SELECT COUNT(*) AS c FROM market_snapshots WHERE scraped_at_utc >= ? AND scraped_at_utc < ?",
        (day_start_utc, day_end_utc),
    ).fetchone()["c"]
)
status = "healthy"
if failures > 0 or acc["scrape_error"] > 0 or acc["step2_blocked"] > 0 or acc["scrape_blocked"] > 0:
    status = "attention_needed"
payload = {
    "date": now_local.date().isoformat(),
    "discovery_runs": discovery_runs,
    "monitoring_runs": monitoring_runs,
    "events_tracked": events_tracked,
    "ticket_types_tracked": ticket_types_tracked,
    "snapshots_collected_today": snapshots_collected,
    **acc,
    "system_status": status,
}
print(json.dumps(payload))
PY
)"
  send_telegram_message "TicketSwap daily summary: ${summary}"
  LAST_SUMMARY_DATE="$today_local"
}

export_weekly_csvs() {
  python <<'PY'
import csv
import sqlite3
from pathlib import Path

exports = Path("data/exports")
exports.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect("ticketswap.db")
conn.row_factory = sqlite3.Row

def write_csv(path: Path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow([r[c] for c in columns])

events_rows = conn.execute(
    """
    SELECT event_id, event_url, event_slug, event_name, event_date_local, category, location, country, region,
           first_seen_at_utc, last_seen_at_utc, status
    FROM events
    ORDER BY COALESCE(event_date_local, '9999-12-31'), event_slug
    """
).fetchall()
write_csv(
    exports / "weekly_events.csv",
    events_rows,
    [
        "event_id", "event_url", "event_slug", "event_name", "event_date_local", "category", "location",
        "country", "region", "first_seen_at_utc", "last_seen_at_utc", "status",
    ],
)

ticket_rows = conn.execute(
    """
    SELECT ticket_type_id, ticket_url, event_id, event_url, ticket_type_label, ticket_type_slug,
           first_seen_at_utc, last_seen_at_utc, status
    FROM ticket_types
    ORDER BY ticket_type_id ASC
    """
).fetchall()
write_csv(
    exports / "weekly_ticket_types.csv",
    ticket_rows,
    [
        "ticket_type_id", "ticket_url", "event_id", "event_url", "ticket_type_label",
        "ticket_type_slug", "first_seen_at_utc", "last_seen_at_utc", "status",
    ],
)

snap_rows = conn.execute(
    """
    SELECT snapshot_id, ticket_type_id, ticket_url, scraped_at_utc, status, currency, listing_count, wanted_count,
           lowest_ask, highest_ask, median_ask, average_ask, error_message, run_id,
           days_until_event, hours_until_event, event_weekday, event_month, total_available_quantity, is_sold_out
    FROM market_snapshots
    WHERE scraped_at_utc >= datetime('now', '-7 days')
    ORDER BY scraped_at_utc DESC
    """
).fetchall()
write_csv(
    exports / "weekly_market_snapshots.csv",
    snap_rows,
    [
        "snapshot_id", "ticket_type_id", "ticket_url", "scraped_at_utc", "status", "currency", "listing_count",
        "wanted_count", "lowest_ask", "highest_ask", "median_ask", "average_ask", "error_message", "run_id",
        "days_until_event", "hours_until_event", "event_weekday", "event_month", "total_available_quantity",
        "is_sold_out",
    ],
)

summary_rows = conn.execute(
    """
    WITH run_counts AS (
      SELECT
        mode,
        status,
        CAST(json_extract(counts_json, '$.scrape_ok') AS INTEGER) AS scrape_ok,
        CAST(json_extract(counts_json, '$.scrape_no_data') AS INTEGER) AS scrape_no_data,
        CAST(json_extract(counts_json, '$.scrape_blocked') AS INTEGER) AS scrape_blocked,
        CAST(json_extract(counts_json, '$.scrape_error') AS INTEGER) AS scrape_error,
        CAST(json_extract(counts_json, '$.step2_blocked') AS INTEGER) AS step2_blocked,
        CAST(json_extract(counts_json, '$.step2_fresh_ok') AS INTEGER) AS step2_fresh_ok
      FROM pipeline_runs
      WHERE started_at_utc >= datetime('now', '-7 days')
    )
    SELECT
      SUM(CASE WHEN mode='discovery' THEN 1 ELSE 0 END) AS discovery_runs,
      SUM(CASE WHEN mode='monitoring' THEN 1 ELSE 0 END) AS monitoring_runs,
      COALESCE(SUM(scrape_ok), 0) AS scrape_ok,
      COALESCE(SUM(scrape_no_data), 0) AS scrape_no_data,
      COALESCE(SUM(scrape_blocked), 0) AS scrape_blocked,
      COALESCE(SUM(scrape_error), 0) AS scrape_error,
      COALESCE(SUM(step2_blocked), 0) AS step2_blocked,
      COALESCE(SUM(step2_fresh_ok), 0) AS step2_fresh_ok,
      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_runs,
      (SELECT COUNT(*) FROM events WHERE COALESCE(status,'active')='active') AS events_tracked,
      (SELECT COUNT(*) FROM ticket_types WHERE status='active') AS ticket_types_tracked,
      (SELECT COUNT(*) FROM market_snapshots WHERE scraped_at_utc >= datetime('now', '-7 days')) AS snapshots_collected
    FROM run_counts
    """
).fetchall()
write_csv(
    exports / "weekly_summary.csv",
    summary_rows,
    [
        "discovery_runs", "monitoring_runs", "scrape_ok", "scrape_no_data", "scrape_blocked", "scrape_error",
        "step2_blocked", "step2_fresh_ok", "failed_runs", "events_tracked", "ticket_types_tracked",
        "snapshots_collected",
    ],
)
PY
}

finalize_weekly_report() {
  export_weekly_csvs
  local files=(
    "data/exports/weekly_events.csv"
    "data/exports/weekly_ticket_types.csv"
    "data/exports/weekly_market_snapshots.csv"
    "data/exports/weekly_summary.csv"
  )
  local message="TicketSwap weekly production test complete.
CSV exports:
- data/exports/weekly_events.csv
- data/exports/weekly_ticket_types.csv
- data/exports/weekly_market_snapshots.csv
- data/exports/weekly_summary.csv"
  send_telegram_message "$message"
  for f in "${files[@]}"; do
    send_telegram_document_if_small "$f" "TicketSwap weekly export: $(basename "$f")"
  done
}

echo "=== WEEK PRODUCTION TEST START $(date -Is) ==="
if ! verify_scope_ready; then
  RUN_FAILED=1
  RUN_FAIL_REASON="scope verification failed: ${DISCOVERY_SCOPE}"
  alert_problem "scope_verification_failed" "$RUN_FAIL_REASON"
  exit 2
fi

while true; do
  now_epoch="$(date +%s)"
  if [[ "$now_epoch" -ge "$END_EPOCH" ]]; then
    break
  fi

  if [[ "$now_epoch" -ge "$NEXT_DISCOVERY_EPOCH" ]]; then
    if ! run_discovery; then
      echo "[$(date -Is)] discovery failed; stopping week run."
      break
    fi
    NEXT_DISCOVERY_EPOCH="$((now_epoch + DISCOVERY_INTERVAL_SECONDS))"
  fi

  time_info="$(TZ="$TZ_NAME" date '+%Y-%m-%d %H %M %Y-%m-%dT%H')"
  today_local="$(awk '{print $1}' <<<"$time_info")"
  hour_local="$(awk '{print $2}' <<<"$time_info")"
  minute_local="$(awk '{print $3}' <<<"$time_info")"
  hour_key="$(awk '{print $4}' <<<"$time_info")"

  if [[ "$minute_local" == "00" ]]; then
    if (( 10#$hour_local >= 8 && 10#$hour_local <= 23 )); then
      if [[ "$hour_key" != "$LAST_MONITOR_KEY" ]]; then
        run_monitoring || true
        LAST_MONITOR_KEY="$hour_key"
      fi
    fi
  fi

  if (( 10#$hour_local > SUMMARY_HOUR || (10#$hour_local == SUMMARY_HOUR && 10#$minute_local >= SUMMARY_MINUTE) )); then
    if [[ "$LAST_SUMMARY_DATE" != "$today_local" ]]; then
      send_daily_summary "$today_local"
    fi
  fi

  sleep 60
done

echo "=== WEEK PRODUCTION TEST END $(date -Is) ==="
finalize_weekly_report

if [[ "$RUN_FAILED" -eq 1 ]]; then
  echo "Run ended with failures: $RUN_FAIL_REASON"
  exit 2
fi
echo "Run completed successfully for 7 days."
exit 0
