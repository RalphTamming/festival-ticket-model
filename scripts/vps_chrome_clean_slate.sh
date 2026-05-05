#!/usr/bin/env bash
# Kill stray Chrome / chromedriver / Xvfb before headed_vps discovery (VPS hygiene).
# Intended for single-tenant Linux servers. Set TICKETSWAP_SKIP_CLEAN_SLATE=1 to no-op.
set -u -o pipefail

if [[ "${TICKETSWAP_SKIP_CLEAN_SLATE:-}" =~ ^(1|true|yes|on)$ ]]; then
  echo "[vps_chrome_clean_slate] skipped (TICKETSWAP_SKIP_CLEAN_SLATE)"
  exit 0
fi

echo "[vps_chrome_clean_slate] killing stray chromedriver / chrome / Xvfb"
# chromedriver first: a later `pkill -f chrome` pattern can match chromedriver's command line.
pkill -f chromedriver 2>/dev/null || true
pkill -f chrome 2>/dev/null || true
pkill -f Xvfb 2>/dev/null || true

sleep 2

echo "[vps_chrome_clean_slate] memory:"
if command -v free >/dev/null 2>&1; then
  free -h || true
fi

if command -v dmesg >/dev/null 2>&1; then
  echo "[vps_chrome_clean_slate] recent OOM / killed (dmesg, best-effort):"
  dmesg -T 2>/dev/null | grep -i killed | tail -n 20 || true
fi

echo "[vps_chrome_clean_slate] done"
