#!/usr/bin/env bash
# Start 18-hub validation in the background (VPS). Log: logs/vps18_validate2.log
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
mkdir -p logs
# shellcheck disable=SC1091
source ".venv/bin/activate"
export TICKETSWAP_BROWSER_MODE=headed_vps
export TICKETSWAP_HEADLESS=0
export DISPLAY="${DISPLAY:-:99}"
export TICKETSWAP_VPS_CLEAN_SLATE=1
export TICKETSWAP_VPS_ENSURE_XVFB=1
: > logs/vps18_validate2.log
nohup python -m pipeline.run_pipeline \
  --mode discovery \
  --headed-vps \
  --profile-dir /opt/ticketswap/profile \
  --vps-eighteen-hubs \
  --require-fresh-step2 \
  --suppress-per-event-step2-alerts \
  --debug >> logs/vps18_validate2.log 2>&1 &
echo "pid=$!"
sleep 2
head -n 15 logs/vps18_validate2.log
