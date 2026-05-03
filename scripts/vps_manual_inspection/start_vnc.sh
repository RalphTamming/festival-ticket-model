#!/usr/bin/env bash
# Start the TigerVNC server on display :1 bound to localhost only.
#
# Usage on the VPS:
#   bash /root/start_vnc.sh
#
# Then on YOUR LOCAL machine (Windows PowerShell), open an SSH tunnel:
#   ssh -L 5901:localhost:5901 root@<VPS_IP>
#
# And connect a VNC client to:  localhost:5901
# Password is whatever you set with `vncpasswd` (file: /root/.vnc/passwd).

set -euo pipefail

DISPLAY_NUM="${DISPLAY_NUM:-1}"
GEOMETRY="${GEOMETRY:-1600x900}"
DEPTH="${DEPTH:-24}"

if ss -tlnp 2>/dev/null | grep -q "127.0.0.1:590${DISPLAY_NUM}\b"; then
  echo "VNC :$DISPLAY_NUM is already running on 127.0.0.1:590${DISPLAY_NUM}."
  exit 0
fi

# Clean up stale X locks if a previous run crashed.
rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}" 2>/dev/null || true

vncserver ":${DISPLAY_NUM}" \
  -localhost yes \
  -geometry "$GEOMETRY" \
  -depth "$DEPTH" \
  -SecurityTypes VncAuth \
  -PasswordFile /root/.vnc/passwd

sleep 1
echo
echo "--- listening sockets (must be 127.0.0.1 only) ---"
ss -tlnp | grep "590${DISPLAY_NUM}" || true
echo
echo "VNC :$DISPLAY_NUM is up. SSH-tunnel from your laptop:"
echo "  ssh -L 590${DISPLAY_NUM}:localhost:590${DISPLAY_NUM} root@<VPS_IP>"
echo "Then connect a VNC client to localhost:590${DISPLAY_NUM}"
