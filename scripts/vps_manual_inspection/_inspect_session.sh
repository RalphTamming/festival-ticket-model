#!/usr/bin/env bash
# Read-only inspection of the running VPS VNC + Chrome session.
# Does NOT touch the running Chrome; only lists windows, copies the History DB
# for read-only inspection, and takes a screenshot of DISPLAY=:1.

set -e

PROFILE_DEFAULT="/root/festival-ticket-model/.ticketswap_browser_profile/Default"
HISTORY_COPY="/tmp/history_copy.sqlite"
SHOTS_DIR="/tmp/vnc_shots"
mkdir -p "$SHOTS_DIR"

# ---- ensure helper tools are present (silent if already installed) ----
need=()
for bin in wmctrl xdotool scrot sqlite3; do
    command -v "$bin" >/dev/null 2>&1 || need+=("$bin")
done
if [ "${#need[@]}" -gt 0 ]; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${need[@]}" >/dev/null 2>&1 || true
fi

echo "=== windows on :1 ==="
DISPLAY=:1 wmctrl -l 2>/dev/null | head -20 || echo '(wmctrl produced nothing)'

echo
echo "=== active Chrome window title (if any) ==="
DISPLAY=:1 xdotool search --name "TicketSwap" getwindowname %@ 2>/dev/null | head -5 || true
DISPLAY=:1 xdotool search --name "Google Chrome" getwindowname %@ 2>/dev/null | head -5 || true

echo
echo "=== last 15 ticketswap URLs from persistent profile History ==="
if [ -e "$PROFILE_DEFAULT/History" ]; then
    cp -f "$PROFILE_DEFAULT/History" "$HISTORY_COPY"
    sqlite3 "$HISTORY_COPY" <<'SQL'
.timeout 5000
.mode column
.headers on
.width 19 80
SELECT
    datetime(last_visit_time/1000000 - 11644473600, 'unixepoch', 'localtime') AS visited,
    url
FROM urls
WHERE url LIKE '%ticketswap%'
ORDER BY last_visit_time DESC
LIMIT 15;
SQL
else
    echo "(no History DB at $PROFILE_DEFAULT/History)"
fi

echo
echo "=== last visited TicketSwap event/festival page (best guess) ==="
if [ -e "$HISTORY_COPY" ]; then
    sqlite3 "$HISTORY_COPY" <<'SQL'
.mode list
.separator ' | '
SELECT
    datetime(last_visit_time/1000000 - 11644473600, 'unixepoch', 'localtime') AS visited,
    url
FROM urls
WHERE url LIKE '%ticketswap.com/festival-tickets/%'
  AND url NOT LIKE '%/festival-tickets/?%'
  AND url NOT LIKE '%/festival-tickets/a/%'
ORDER BY last_visit_time DESC
LIMIT 1;
SQL
fi

echo
echo "=== screenshot of DISPLAY=:1 ==="
if DISPLAY=:1 scrot -o "$SHOTS_DIR/desktop.png" 2>/dev/null; then
    ls -la "$SHOTS_DIR/desktop.png"
else
    echo "(scrot failed - is anything visible on :1?)"
fi
