#!/usr/bin/env bash
# Smoke STEP2 in headed_vps mode (Xvfb/VNC + persistent profile). See scripts/README_step2_vps.md
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export TICKETSWAP_BROWSER_MODE="${TICKETSWAP_BROWSER_MODE:-headed_vps}"
export TICKETSWAP_HEADLESS="${TICKETSWAP_HEADLESS:-0}"
exec python scripts/smoke_step2_vps.py "$@"
