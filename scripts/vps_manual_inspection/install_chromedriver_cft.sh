#!/usr/bin/env bash
# Pin ChromeDriver to match installed Google Chrome (Chrome-for-Testing CDN).
set -euo pipefail

CHROME_BIN="${CHROME_BIN:-/opt/google/chrome/chrome}"
DEST="${DEST:-/usr/local/bin/chromedriver-cft}"

if [[ ! -x "$CHROME_BIN" ]]; then
  echo "ERROR: Chrome not found at $CHROME_BIN"
  exit 1
fi

VERSION="$("$CHROME_BIN" --version | awk '{print $3}')"
ZIP_URL="https://storage.googleapis.com/chrome-for-testing-public/${VERSION}/linux64/chromedriver-linux64.zip"
TMPZIP="/tmp/chromedriver_linux64_${VERSION}.zip"
TMPDIR="/tmp/chromedriver_unpack_${VERSION}"

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"

echo "Chrome version: $VERSION"
echo "Fetching $ZIP_URL"
wget -q -O "$TMPZIP" "$ZIP_URL"
unzip -qo "$TMPZIP" -d "$TMPDIR"
install -m 0755 "${TMPDIR}/chromedriver-linux64/chromedriver" "$DEST"

echo "Installed: $DEST"
"$DEST" --version
