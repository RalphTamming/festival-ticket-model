#!/usr/bin/env bash
# Ensure a working X11 display for headed Chrome (default DISPLAY :99).
set -u -o pipefail

DISP="${DISPLAY:-:99}"
# Normalize to :N form for Xvfb matching
case "$DISP" in
  :*) ;;
  *) DISP=":${DISP}" ;;
esac
export DISPLAY="$DISP"

if command -v xdpyinfo >/dev/null 2>&1; then
  if xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then
    echo "[vps_ensure_xvfb] DISPLAY=$DISPLAY already usable"
    exit 0
  fi
fi

if ! command -v Xvfb >/dev/null 2>&1; then
  echo "[vps_ensure_xvfb] ERROR: xdpyinfo failed and Xvfb not installed" >&2
  exit 1
fi

echo "[vps_ensure_xvfb] starting Xvfb on $DISPLAY"
# -ac: allow local connections without xauth file (typical VPS automation)
Xvfb "$DISPLAY" -screen 0 1920x1080x24 -ac >/dev/null 2>&1 &
XVFB_PID=$!
sleep 1
if command -v xdpyinfo >/dev/null 2>&1; then
  if xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then
    echo "[vps_ensure_xvfb] Xvfb ready DISPLAY=$DISPLAY pid=$XVFB_PID"
    exit 0
  fi
fi
echo "[vps_ensure_xvfb] WARNING: Xvfb started but xdpyinfo still failing; continuing" >&2
exit 0
