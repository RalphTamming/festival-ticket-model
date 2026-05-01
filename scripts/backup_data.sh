#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/backups data/outputs
LOG_FILE="logs/backup.log"

{
  echo "[$(date -Is)] Starting backup"

  BACKUP_DIR="data/backups/$(date +%F)"
  mkdir -p "$BACKUP_DIR"

  if [[ -f "ticketswap.db" ]]; then
    cp -f "ticketswap.db" "$BACKUP_DIR/ticketswap.db"
    echo "[$(date -Is)] Backed up ticketswap.db"
  else
    echo "[$(date -Is)] WARNING: ticketswap.db not found"
  fi

  shopt -s nullglob
  CSV_FILES=(data/outputs/*.csv)
  if (( ${#CSV_FILES[@]} > 0 )); then
    cp -f "${CSV_FILES[@]}" "$BACKUP_DIR/"
    echo "[$(date -Is)] Backed up ${#CSV_FILES[@]} CSV file(s)"
  else
    echo "[$(date -Is)] WARNING: no CSV files found in data/outputs"
  fi
  shopt -u nullglob

  # Optional retention optimization: gzip backup folders older than 7 days.
  find data/backups -mindepth 1 -maxdepth 1 -type d -mtime +7 -print0 | while IFS= read -r -d '' old_dir; do
    archive_path="${old_dir}.tar.gz"
    if [[ ! -f "$archive_path" ]]; then
      tar -czf "$archive_path" -C "$(dirname "$old_dir")" "$(basename "$old_dir")"
      rm -rf "$old_dir"
      echo "[$(date -Is)] Compressed old backup: $archive_path"
    fi
  done

  echo "[$(date -Is)] Backup completed"
} >> "$LOG_FILE" 2>&1
