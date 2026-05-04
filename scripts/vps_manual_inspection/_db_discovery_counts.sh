#!/usr/bin/env bash
set -euo pipefail
cd /root/festival-ticket-model
export SQLITE="/usr/bin/sqlite3"
export DB="ticketswap.db"
"$SQLITE" "$DB" "PRAGMA busy_timeout=8000;"
echo "events_since_run_start:"
"$SQLITE" "$DB" "select count(*) from events where first_seen_at_utc >= '2026-05-03T22:53:00Z';"
echo "ticket_types_since_run_start:"
"$SQLITE" "$DB" "select count(*) from ticket_types where first_seen_at_utc >= '2026-05-03T22:53:00Z';"
echo "latest_discovery_run:"
"$SQLITE" "$DB" -header -column "select run_id, status, started_at_utc, finished_at_utc from pipeline_runs where mode='discovery' order by started_at_utc desc limit 1;"
