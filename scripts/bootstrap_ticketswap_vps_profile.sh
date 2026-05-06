#!/usr/bin/env bash
# One-off headed Chrome to TicketSwap for manual login / trust (no passwords stored by this script).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export REPO_ROOT="$ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

if [[ "$(uname -s)" == "Linux" ]]; then
  if [[ -z "${DISPLAY:-}" ]]; then
    echo "DISPLAY not set; starting Xvfb on :99 (override with DISPLAY=:99 or your VNC display)." >&2
    export DISPLAY="${DISPLAY:-:99}"
    if ! command -v Xvfb >/dev/null 2>&1; then
      echo "Install Xvfb (e.g. apt install xvfb) or set DISPLAY to an existing X server." >&2
      exit 1
    fi
    if ! command -v xdpyinfo >/dev/null 2>&1 || ! xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then
      Xvfb "$DISPLAY" -screen 0 1920x1080x24 &
      sleep 1
    fi
  fi
  # If we're pointing at an existing `xvfb-run` Xvfb, ensure XAUTHORITY matches its `-auth` cookie.
  if [[ -z "${XAUTHORITY:-}" ]]; then
    disp_num="${DISPLAY#:}"
    disp_num="${disp_num%%.*}"
    xvfb_pid="$(pgrep -a Xvfb 2>/dev/null | awk -v d=":${disp_num:-99}" '$0 ~ d {print $1; exit}')"
    if [[ -n "${xvfb_pid:-}" ]]; then
      auth_file="$(tr '\0' ' ' </proc/$xvfb_pid/cmdline | sed -n 's/.*\-auth \(\S\+\).*/\1/p')"
      if [[ -n "${auth_file:-}" ]] && [[ -f "$auth_file" ]]; then
        export XAUTHORITY="$auth_file"
      fi
    fi
  fi
fi

: "${TICKETSWAP_PROFILE_DIR:?Set TICKETSWAP_PROFILE_DIR to your Chrome user-data-dir}"
export TICKETSWAP_HEADLESS="${TICKETSWAP_HEADLESS:-0}"

# VPS: pinned ChromeDriver is usually /usr/local/bin/chromedriver-cft — prefer Selenium over UC unless overridden.
if [[ -z "${TICKETSWAP_DRIVER_IMPL:-}" ]] && [[ -x /usr/local/bin/chromedriver-cft ]]; then
  export TICKETSWAP_DRIVER_IMPL=selenium
  export TICKETSWAP_CHROMEDRIVER_PATH="${TICKETSWAP_CHROMEDRIVER_PATH:-/usr/local/bin/chromedriver-cft}"
fi

echo "Opening headed Chrome with profile: $TICKETSWAP_PROFILE_DIR"
echo "Complete login or verification manually (VNC/noVNC/ssh -X as needed)."
echo "This script does not read or store passwords — only the Chrome profile on disk is updated."
echo "When finished, close the Chrome window. If stdin is a TTY, press Enter here to exit."

python - <<'PY'
import contextlib
import os
import sys
import time

_repo = os.environ.get("REPO_ROOT", ".")
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from discovery import discover_urls as du

d = du.new_driver(headless=False)
try:
    d.set_page_load_timeout(120)
    d.get("https://www.ticketswap.com/")
    time.sleep(2.0)
    if sys.stdin.isatty():
        input("Press Enter after you have closed Chrome...\n")
    else:
        print("Non-interactive: waiting until Chrome window count drops...", flush=True)
        while True:
            time.sleep(3.0)
            try:
                if len(d.window_handles) == 0:
                    break
            except Exception:
                break
finally:
    with contextlib.suppress(Exception):
        d.quit()
PY

echo "Bootstrap session finished."
