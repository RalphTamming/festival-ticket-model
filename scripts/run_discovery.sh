#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/outputs data/debug data/backups

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
else
  echo "[$(date -Is)] ERROR: .venv not found at $ROOT_DIR/.venv" >&2
  exit 1
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

echo "[$(date -Is)] Starting discovery run"
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed >> logs/discovery.log 2>&1
echo "[$(date -Is)] Discovery run completed"
