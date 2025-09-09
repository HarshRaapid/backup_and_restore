#!/usr/bin/env bash
set -euox pipefail

# ============================================
# MySQL → mydumper → Azure Storage (upload-batch)
# - Keeps ONE local snapshot at: ~/immutable_backup
# - No auth/retention logic here (per requirements)
# ============================================

# ---------- tiny helpers ----------
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
log()  { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# ---------- fixed Azure Storage targets ----------
AZ_ACCOUNT="prodmysqlimmutablebackup"
AZ_CONTAINER="mysql-immutable-backups"
LOCAL_DIR="${HOME}/immutable_backup"

# ---------- tunables (override via flags) ----------
THREADS=12
CHUNK_MB=512
REGEX_EXCLUDE='^(?!(mysql|sys|performance_schema|information_schema))'
SRC_HOST=""; SRC_PORT="3306"; SRC_USER=""; SRC_PASS=""

usage() {
  cat <<EOF
Usage:
  $0 --src-host HOST --src-user USER [--src-port 3306] [--src-pass PASS | --src-pass-prompt]
  Optional: --threads N (default ${THREADS}), --chunk-mb N (default ${CHUNK_MB}),
            --regex-exclude REGEX (default excludes system DBs)

This script will:
  1) Clear ${LOCAL_DIR}
  2) Run mydumper into ${LOCAL_DIR}
  3) Upload via: az storage blob upload-batch --account-name ${AZ_ACCOUNT} --destination ${AZ_CONTAINER} --source ${LOCAL_DIR}
EOF
}

# ---------- parse flags ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-host) SRC_HOST="$2"; shift 2 ;;
    --src-port) SRC_PORT="$2"; shift 2 ;;
    --src-user) SRC_USER="$2"; shift 2 ;;
    --src-pass) SRC_PASS="$2"; shift 2 ;;
    --src-pass-prompt) read -rsp "Source MySQL password: " SRC_PASS; echo; shift ;;
    --threads) THREADS="$2"; shift 2 ;;
    --chunk-mb) CHUNK_MB="$2"; shift 2 ;;
    --regex-exclude) REGEX_EXCLUDE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    *) echo "Unknown flag: $1"; usage; exit 1 ;;
  esac
done

# ---------- validate ----------
need mydumper
need az
[[ -n "${SRC_HOST}" && -n "${SRC_USER}" ]] || { echo "Missing --src-host/--src-user"; usage; exit 1; }

# ---------- clean local snapshot dir ----------
log "Clearing ${LOCAL_DIR}"
rm -rf "${LOCAL_DIR}" || true
mkdir -p "${LOCAL_DIR}"

# ---------- run mydumper ----------
log "Running mydumper from ${SRC_HOST}:${SRC_PORT} → ${LOCAL_DIR}"
MYSQL_PWD="${SRC_PASS:-}" mydumper \
  -h "${SRC_HOST}" -P "${SRC_PORT}" -u "${SRC_USER}" \
  -o "${LOCAL_DIR}" -t "${THREADS}" --compress \
  -F "${CHUNK_MB}" -G -R -E \
  -L "${LOCAL_DIR}/mydumper.log" \
  -B ra_audit_apigateway \
  -T cm_code \
  --ssl --ssl-mode=REQUIRED


# ---------- upload (batch) ----------

log "Uploading ${LOCAL_DIR} → account=${AZ_ACCOUNT} container=${AZ_CONTAINER}"

az storage blob upload-batch \
  --account-name "${AZ_ACCOUNT}" \
  --destination "${AZ_CONTAINER}" \
  --source "${LOCAL_DIR}" \
  --overwrite true

log "Done."
