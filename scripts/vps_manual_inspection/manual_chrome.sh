#!/usr/bin/env bash
# Manual Chrome launcher for VPS visual inspection.
#
# Opens Google Chrome on DISPLAY=:1 (the VNC desktop) using the SAME persistent
# profile the pipeline uses. After you log in / pass any TicketSwap verification
# here, the cookies/session are saved into the profile and the pipeline will
# pick them up on the next run.
#
# Usage (inside an SSH session on the VPS):
#   bash /root/manual_chrome.sh
#
# Or from inside the VNC desktop's xfce4-terminal:
#   bash /root/manual_chrome.sh
#
# Notes:
# - We do NOT bypass any verification. You verify manually like a real user.
# - We do NOT run anything in headless mode here.
# - We refuse to launch if the pipeline is currently using the persistent profile,
#   to avoid corrupting Chrome's profile state.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/root/festival-ticket-model}"
PROFILE_ROOT="${TICKETSWAP_PROFILE_DIR:-$REPO_ROOT/.ticketswap_browser_profile}"
PROFILE_NAME="${BROWSER_PROFILE_NAME:-Default}"
URL="${1:-https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3}"

if [ ! -d "$PROFILE_ROOT" ]; then
  echo "ERROR: persistent profile dir not found: $PROFILE_ROOT" >&2
  exit 1
fi

# Only consider real Chrome binaries (comm = chrome / google-chrome / chromium),
# not the shell process that's running this very script. We use the full,
# absolute profile path so an unrelated string elsewhere on the cmdline cannot
# match.
_chrome_using_profile() {
  ps -eo pid=,comm=,args= 2>/dev/null \
    | awk -v p="$PROFILE_ROOT" '
        $2 ~ /^(chrome|google-chrome|chromium|chromium-browser|chrome_crashpad_handler)$/ \
        && index($0, "--user-data-dir=" p) > 0 { print }
      '
}
existing="$(_chrome_using_profile)"
if [ -n "$existing" ]; then
  echo "ERROR: another Chrome process is currently using the persistent profile." >&2
  echo "Close the pipeline / stop the discovery run before logging in manually." >&2
  printf '%s\n' "$existing" >&2
  exit 2
fi

# Clean up only single-instance lock files (not user data) in case a previous
# session crashed without cleaning them up. This does NOT delete cookies/login.
for f in SingletonLock SingletonCookie SingletonSocket; do
  if [ -e "$PROFILE_ROOT/$f" ]; then
    rm -f "$PROFILE_ROOT/$f"
    echo "removed stale $f"
  fi
done

if [ -z "${DISPLAY:-}" ]; then
  export DISPLAY=:1
fi

echo "Launching Chrome:"
echo "  DISPLAY=$DISPLAY"
echo "  user-data-dir=$PROFILE_ROOT"
echo "  profile-directory=$PROFILE_NAME"
echo "  url=$URL"
echo
echo "When the browser opens in the VNC viewer:"
echo "  1. Browse normally."
echo "  2. If TicketSwap shows a verification screen, complete it manually."
echo "  3. Log in if you have an account (optional, but helps trust)."
echo "  4. Visit a few festival pages so cookies build up."
echo "  5. Close the browser window when done."
echo

# Match the pipeline's flags as closely as possible so the resulting profile
# state is what the pipeline will see on the next automated run.
exec google-chrome \
  --user-data-dir="$PROFILE_ROOT" \
  --profile-directory="$PROFILE_NAME" \
  --no-sandbox \
  --no-first-run \
  --no-default-browser-check \
  --disable-blink-features=AutomationControlled \
  --disable-infobars \
  --disable-notifications \
  --disable-popup-blocking \
  --window-size=1500,900 \
  --lang=en-US \
  "$URL"
