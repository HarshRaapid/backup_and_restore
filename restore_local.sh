#!/usr/bin/env bash
set -euo pipefail

# ============================================
# Restore ONLY (from local snapshot folder)
# - Reads dump from BACKUP_ROOT/latest (or --snapshot-dir)
# - Prevents running while backup is active (lock)
# ============================================

# ---------- helpers ----------
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
log()  { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# ---------- defaults ----------
BACKUP_ROOT="/backups"
SNAPSHOT_DIR=""                 # if not set, will use BACKUP_ROOT/latest
THREADS=12
SSL_MODE="REQUIRED"

DEST_HOST=""; DEST_PORT="3306"; DEST_USER=""; DEST_PASS=""

# ---------- usage ----------
usage() {
  cat <<EOF
Usage:
  $0 --dest-host HOST --dest-user USER [--dest-port 3306] [--dest-pass PASS | --dest-pass-prompt]
     [--backup-root DIR] [--snapshot-dir DIR] [--threads N] [--ssl-mode MODE]

Notes:
  - By default, restores from BACKUP_ROOT/latest
  - Will refuse to run if a backup/another restore is in progress

Examples:
  $0 --dest-host prod...backup-test.mysql.database.azure.com --dest-user azureuser --dest-pass-prompt
  $0 --dest-host ... --dest-user ... --snapshot-dir /backups/latest --threads 16
EOF
}

# ---------- parse flags ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dest-host) DEST_HOST="$2"; shift 2 ;;
    --dest-port) DEST_PORT="$2"; shift 2 ;;
    --dest-user) DEST_USER="$2"; shift 2 ;;
    --dest-pass) DEST_PASS="$2"; shift 2 ;;
    --dest-pass-prompt) read -rsp "Restore MySQL password: " DEST_PASS; echo; shift ;;

    --backup-root) BACKUP_ROOT="$2"; shift 2 ;;
    --snapshot-dir) SNAPSHOT_DIR="$2"; shift 2 ;;
    --threads) THREADS="$2"; shift 2 ;;
    --ssl-mode) SSL_MODE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown flag: $1"; usage; exit 1 ;;
  esac
done

# ---------- validate ----------
need myloader; need date
[[ -n "$DEST_HOST" && -n "$DEST_USER" ]] || { echo "Missing --dest-host/--dest-user"; exit 1; }

# Snapshot path
SNAPSHOT_DIR="${SNAPSHOT_DIR:-${BACKUP_ROOT}/latest}"
[[ -d "$SNAPSHOT_DIR" ]] || { echo "Snapshot not found: $SNAPSHOT_DIR"; exit 1; }

# ---------- lock (mutual exclusion with backup) ----------
LOCK_DIR="${BACKUP_ROOT}/.job.lock"
mkdir -p "$BACKUP_ROOT"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Another job is running (backup/restore). Aborting."
  exit 1
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

# ---------- restore ----------
log "Restoring snapshot ${SNAPSHOT_DIR} → ${DEST_HOST}:${DEST_PORT}"
MYSQL_PWD="${DEST_PASS:-}" myloader \
  -h "$DEST_HOST" -P "$DEST_PORT" -u "$DEST_USER" \
  -d "$SNAPSHOT_DIR" -t "$THREADS" -o --verbose=2 --ssl-mode="$SSL_MODE"

log "Restore completed."
