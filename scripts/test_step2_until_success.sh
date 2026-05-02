#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/debug/step2_vps_live
LOG_FILE="logs/step2_live_test.log"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
else
  echo "[$(date -Is)] ERROR: .venv missing" | tee -a "$LOG_FILE"
  exit 1
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

EVENTS=(
  "https://www.ticketswap.com/festival-tickets/bondgenoten-festival-2026-amsterdam-lofi-2026-05-29-CZrBG4iowfg4JsxTeEx7j"
  "https://www.ticketswap.com/festival-tickets/ij-hallen-12-december-2026-amsterdam-ij-hallen-2026-12-12-CWwr2uag4pr6YcqQnguuW"
  "https://www.ticketswap.com/festival-tickets/soenda-festival-2026-utrecht-ruigenhoekse-polder-2026-05-30-5D4Kfqx4CYJuQi9gUvyyno"
)

echo "[$(date -Is)] Starting repeated STEP2 live test loop" | tee -a "$LOG_FILE"
success=0

for round in 1 2 3; do
  echo "[$(date -Is)] Round $round" | tee -a "$LOG_FILE"
  for ev in "${EVENTS[@]}"; do
    echo "[$(date -Is)] Test selenium $ev" | tee -a "$LOG_FILE"
    set +e
    out=$(python step2_vps_live_test.py --event-url "$ev" --browser selenium --headed --retries 3 --verification-wait 60 --wait-for-manual-verification --debug 2>&1)
    code=$?
    set -e
    echo "$out" | tee -a "$LOG_FILE"
    fresh=$(echo "$out" | python -c "import sys,json; s=sys.stdin.read().strip(); 
try:
 d=json.loads(s); print(int(d.get('fresh_ticket_urls_found',0)))
except Exception:
 print(0)")
    if [[ "$code" -eq 0 && "$fresh" -gt 0 ]]; then
      echo "[$(date -Is)] SUCCESS via selenium on $ev" | tee -a "$LOG_FILE"
      success=1
      break 2
    fi

    echo "[$(date -Is)] Selenium failed, trying playwright $ev" | tee -a "$LOG_FILE"
    set +e
    out2=$(python step2_vps_live_test.py --event-url "$ev" --browser playwright --headed --retries 2 --verification-wait 60 --wait-for-manual-verification --debug 2>&1)
    code2=$?
    set -e
    echo "$out2" | tee -a "$LOG_FILE"
    fresh2=$(echo "$out2" | python -c "import sys,json; s=sys.stdin.read().strip(); 
try:
 d=json.loads(s); print(int(d.get('fresh_ticket_urls_found',0)))
except Exception:
 print(0)")
    if [[ "$code2" -eq 0 && "$fresh2" -gt 0 ]]; then
      echo "[$(date -Is)] SUCCESS via playwright on $ev" | tee -a "$LOG_FILE"
      success=1
      break 2
    fi
    sleep 20
  done
  sleep 45
done

if [[ "$success" -eq 1 ]]; then
  echo "[$(date -Is)] STEP2 live test succeeded" | tee -a "$LOG_FILE"
  exit 0
fi

echo "[$(date -Is)] STEP2 live test did not discover fresh ticket URLs" | tee -a "$LOG_FILE"
exit 1
