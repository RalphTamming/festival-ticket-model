#!/usr/bin/env bash
# Smoke STEP2 in headed_vps mode (Xvfb/VNC + persistent profile). See scripts/README_step2_vps.md
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export TICKETSWAP_BROWSER_MODE="${TICKETSWAP_BROWSER_MODE:-headed_vps}"
export TICKETSWAP_HEADLESS="${TICKETSWAP_HEADLESS:-0}"

# Xvfb started via `xvfb-run` publishes an MIT-MAGIC-COOKIE and requires matching Xauthority.
# If DISPLAY is set (typically :99) but XAUTHORITY is missing, Chrome often fails immediately with:
#   "Authorization required, but no authorization protocol specified"
if [[ "$(uname -s)" == "Linux" ]]; then
  if [[ -z "${DISPLAY:-}" ]]; then
    export DISPLAY="${DISPLAY:-:99}"
  fi
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

exec python scripts/smoke_step2_vps.py "$@"
