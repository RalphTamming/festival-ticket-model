#!/usr/bin/env bash
# Install the daily week-runner cron line if not already present (VPS).
set -euo pipefail
ROOT="${ROOT:-/root/festival-ticket-model}"
MARKER="vps_manual_inspection/_launch_week.sh"
LINE="CRON_TZ=Europe/Amsterdam"$'\n'"0 0 * * * cd $ROOT && bash scripts/vps_manual_inspection/_launch_week.sh >> logs/week_runner_cron.log 2>&1"

if crontab -l 2>/dev/null | grep -Fq "$MARKER"; then
  echo "Cron already references _launch_week.sh; not modifying crontab."
  exit 0
fi
mkdir -p "$ROOT/logs"
(crontab -l 2>/dev/null || true; echo "$LINE") | crontab -
echo "Installed week runner cron (CRON_TZ=Europe/Amsterdam, 00:00 daily)."
