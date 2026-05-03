#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/outputs data/debug data/backups

LOG_FILE="logs/test_10h_run.log"
touch "$LOG_FILE"

exec > >(tee -a "$LOG_FILE") 2>&1

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
else
  echo "[$(date -Is)] ERROR: .venv not found at $ROOT_DIR/.venv"
  exit 1
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

run_discovery() {
  echo "=== DISCOVERY RUN START $(date) ==="
  xvfb-run -a python run_pipeline.py \
    --mode discovery \
    --scope amsterdam_festivals \
    --headed \
    --limit-events 10 \
    --vps-safe-mode \
    --step2-browser selenium \
    --require-fresh-step2 \
    --wait-for-manual-verification
  local rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "=== DISCOVERY RUN END $(date) RESULT=SUCCESS EXIT_CODE=$rc ==="
  else
    echo "=== DISCOVERY RUN END $(date) RESULT=FAILED EXIT_CODE=$rc ==="
  fi
}

run_monitoring() {
  echo "=== MONITORING RUN START $(date) ==="
  xvfb-run -a python run_pipeline.py \
    --mode monitoring \
    --headed \
    --limit-tickets 20
  local rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "=== MONITORING RUN END $(date) RESULT=SUCCESS EXIT_CODE=$rc ==="
  else
    echo "=== MONITORING RUN END $(date) RESULT=FAILED EXIT_CODE=$rc ==="
  fi
}

echo "=== TEST START $(date) ==="

# T0: discovery
run_discovery

# Wait 2.5 hours
sleep 9000

# T+2.5h: monitoring
run_monitoring

# Wait 2.5 hours
sleep 9000

# T+5h: monitoring
run_monitoring

# Wait 2.5 hours
sleep 9000

# T+7.5h: discovery
run_discovery

# Wait 1.25 hours
sleep 4500

# T+8.75h: monitoring
run_monitoring

# Wait 1.25 hours
sleep 4500

# T+10h: final monitoring
run_monitoring

echo "=== TEST COMPLETE $(date) ==="
