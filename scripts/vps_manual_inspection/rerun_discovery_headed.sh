#!/usr/bin/env bash
# Re-run the two diagnostic discovery commands on the VPS using headed Chrome
# pointed at the VNC display so you can WATCH them happen in real time.
#
# Run AFTER you have:
#   1. Started VNC (bash /root/start_vnc.sh)
#   2. Opened the VNC viewer on your laptop (via SSH tunnel)
#   3. Optionally logged in / passed verification with /root/manual_chrome.sh
#   4. Closed the manual Chrome window so its profile is free again.
#
# Usage:
#   bash /root/rerun_discovery_headed.sh

set -euo pipefail

REPO="${REPO:-/root/festival-ticket-model}"
VENV="${VENV:-$REPO/.venv}"
DISPLAY_NUM="${DISPLAY_NUM:-1}"
LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR"

ts() { date -u +%Y%m%dT%H%M%SZ; }
T="$(ts)"
LOG_STEP1="$LOG_DIR/manual_step1_$T.log"
LOG_DISC="$LOG_DIR/manual_discovery_$T.log"

if ! ss -tlnp 2>/dev/null | grep -q "127.0.0.1:590${DISPLAY_NUM}\b"; then
  echo "ERROR: VNC :$DISPLAY_NUM is not running. Start it first: bash /root/start_vnc.sh" >&2
  exit 1
fi

if pgrep -af "user-data-dir=.*\.ticketswap_browser_profile" | grep -v "$$" >/dev/null 2>&1; then
  echo "ERROR: a Chrome process is still using the persistent profile."     >&2
  echo "Close the manual Chrome window first."                              >&2
  pgrep -af "user-data-dir=.*\.ticketswap_browser_profile" | grep -v "$$"   >&2 || true
  exit 2
fi

cd "$REPO"
# shellcheck disable=SC1091
. "$VENV/bin/activate"

export DISPLAY=":${DISPLAY_NUM}"
unset HEADLESS
echo "Using DISPLAY=$DISPLAY (VNC desktop :$DISPLAY_NUM)."
echo "Persistent profile: $REPO/.ticketswap_browser_profile"
echo

echo "=== STEP 1: listing collection (Amsterdam location=3) ==="
echo "Log: $LOG_STEP1"
python -m discovery.step1_collect_listing_urls \
  --url "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3" \
  --min-events 1 \
  --max-show-more 2 2>&1 | tee "$LOG_STEP1"

echo
echo "=== STEP 2: full discovery on amsterdam_festivals (1 event, shared_listing_click) ==="
echo "Log: $LOG_DISC"
python run_pipeline.py \
  --mode discovery \
  --scope amsterdam_festivals \
  --headed \
  --limit-events 1 \
  --vps-safe-mode \
  --require-fresh-step2 \
  --suppress-per-event-step2-alerts \
  --step2-discovery-strategy shared_listing_click 2>&1 | tee "$LOG_DISC"

echo
echo "Done. Logs:"
echo "  $LOG_STEP1"
echo "  $LOG_DISC"
