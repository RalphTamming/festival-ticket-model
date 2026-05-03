#!/usr/bin/env bash
# Stop the TigerVNC server on display :1.
#
# Usage on the VPS:
#   bash /root/stop_vnc.sh
set -euo pipefail
DISPLAY_NUM="${DISPLAY_NUM:-1}"
vncserver -kill ":${DISPLAY_NUM}" || true
rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}" 2>/dev/null || true
echo "VNC :$DISPLAY_NUM stopped."
