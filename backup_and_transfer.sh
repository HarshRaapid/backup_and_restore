#!/usr/bin/env bash
set -euo pipefail

# ============================================
# MySQL → mydumper → ADLS
# - Keeps only ONE local snapshot: BACKUP_ROOT/latest
# - All config via flags
# - Uses a lock directory to prevent concurrent runs
# ============================================

# ---------- tiny helpers ----------
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
log()  { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# ---------- defaults (override via flags) ----------
BACKUP_ROOT="/backups"               # where 'latest' snapshot lives
THREADS=12                           # mydumper parallel threads
CHUNK_MB=512                         # split big tables every N MB
SSL_MODE="REQUIRED"                  # Azure MySQL requires TLS
REGEX_EXCLUDE='^(?!(mysql|sys|performance_schema|information_schema))'
RETENTION_REMOTE_DAYS=7              # keep N days in ADLS
AUTH_METHOD="SAS"                    # SAS | SP | MSI
REMOTE_CONTAINER="mysql-backups"
REMOTE_PREFIX="full-server"

# MySQL source (required)
SRC_HOST=""; SRC_PORT="3306"; SRC_USER=""; SRC_PASS=""

# ADLS (required)
STG_ACCOUNT=""; SAS_TOKEN=""; SP_APP_ID=""; SP_TENANT_ID=""; SP_SECRET=""

# ---------- usage ----------

usage() {
  cat <<EOF
Usage:
  $0 [flags]

Required (source MySQL):
  --src-host HOST   --src-port PORT  --src-user USER
  [--src-pass PASS | --src-pass-prompt]

Azure Storage (one auth method required):
  --stg-account NAME  --container NAME  --prefix PATH
  --auth-method SAS|SP|MSI
    SAS: --sas-token "sv=...&sig=..."
    SP : --sp-app-id ID --sp-tenant-id ID --sp-secret SECRET
    MSI: (no extra flags)

Optional:
  --backup-root DIR          (default: /backups)
  --threads N                (default: 12)
  --chunk-mb N               (default: 512)
  --ssl-mode MODE            (default: REQUIRED)
  --regex-exclude REGEX      (default: exclude system DBs)
  --retention-remote-days N  (default: 7)
  -h|--help


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

    --stg-account) STG_ACCOUNT="$2"; shift 2 ;;
    --container)   REMOTE_CONTAINER="$2"; shift 2 ;;
    --prefix)      REMOTE_PREFIX="$2"; shift 2 ;;
    --auth-method) AUTH_METHOD="$2"; shift 2 ;;
    --sas-token)   SAS_TOKEN="$2"; shift 2 ;;
    --sp-app-id)   SP_APP_ID="$2"; shift 2 ;;
    --sp-tenant-id) SP_TENANT_ID="$2"; shift 2 ;;
    --sp-secret)   SP_SECRET="$2"; shift 2 ;;

    --backup-root) BACKUP_ROOT="$2"; shift 2 ;;
    --threads)     THREADS="$2"; shift 2 ;;
    --chunk-mb)    CHUNK_MB="$2"; shift 2 ;;
    --ssl-mode)    SSL_MODE="$2"; shift 2 ;;
    --regex-exclude) REGEX_EXCLUDE="$2"; shift 2 ;;
    --retention-remote-days) RETENTION_REMOTE_DAYS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown flag: $1"; usage; exit 1 ;;
  esac
done

# ---------- validate ----------
need mydumper; need azcopy; need sha256sum; need date
[[ -n "$SRC_HOST" && -n "$SRC_USER" ]] || { echo "Missing --src-host/--src-user"; exit 1; }
[[ -n "$STG_ACCOUNT" ]] || { echo "Missing --stg-account"; exit 1; }
if [[ "$AUTH_METHOD" == "SAS" ]]; then [[ -n "$SAS_TOKEN" ]] || { echo "Missing --sas-token"; exit 1; }; fi

# ---------- lock (mutual exclusion with restore) ----------
LOCK_DIR="${BACKUP_ROOT}/.job.lock"
mkdir -p "$BACKUP_ROOT"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Another job is running (backup/restore). Aborting."
  exit 1
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

# ---------- constants ----------
TS_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_DIR="${BACKUP_ROOT}/latest"   # single local snapshot
REMOTE_BASE="https://${STG_ACCOUNT}.dfs.core.windows.net/${REMOTE_CONTAINER}/${REMOTE_PREFIX}"
with_sas() { [[ "$AUTH_METHOD" == "SAS" ]] && echo "$1?$SAS_TOKEN" || echo "$1"; }

# ---------- fresh local snapshot (only one) ----------
log "Preparing clean local snapshot at: ${LOCAL_DIR}"
rm -rf "${LOCAL_DIR}" || true
mkdir -p "${LOCAL_DIR}"

# ---------- run mydumper ----------
log "Backing up from ${SRC_HOST}:${SRC_PORT} → ${LOCAL_DIR}"
MYSQL_PWD="${SRC_PASS:-}" mydumper \
  -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" \
  -o "$LOCAL_DIR" -t "$THREADS" --compress \
  -F "$CHUNK_MB" -G -R -E \
  -L "${LOCAL_DIR}/mydumper.log" \
  --regex "$REGEX_EXCLUDE" \
  --ssl-mode="$SSL_MODE"

# ---------- checksums ----------
log "Generating checksums"
( cd "$LOCAL_DIR"
  find . -type f \( -name '*.gz' -o -name '*.sql' -o -name '*.json' -o -name '*.txt' \) \
    -print0 | sort -z | xargs -0 sha256sum > checksums.sha256
)

# ---------- upload to ADLS (timestamped remote path) ----------
DEST_URL_TS="$(with_sas "${REMOTE_BASE}/${TS_UTC}")"
log "Uploading ${LOCAL_DIR} → ${DEST_URL_TS}"
az_login() {
  case "$AUTH_METHOD" in
    SAS) : ;;
    SP) azcopy login --service-principal --application-id "$SP_APP_ID" --tenant-id "$SP_TENANT_ID" >/dev/null ;;
    MSI) azcopy login --identity >/dev/null ;;
    *) echo "Invalid --auth-method"; exit 1 ;;
  esac
}
az_logout() { [[ "$AUTH_METHOD" == "SAS" ]] || azcopy logout >/dev/null || true; }
az_login
azcopy copy "${LOCAL_DIR}" "${DEST_URL_TS}" \
  --recursive=true --from-to=LocalBlobFS --check-length=true --overwrite=ifSourceNewer
printf '{"timestamp":"%s","source_host":"%s","threads":%d,"chunk_mb":%d}\n' \
  "$TS_UTC" "$SRC_HOST" "$THREADS" "$CHUNK_MB" \
  | azcopy copy --from-to=PipeBlobFS /dev/stdin "${DEST_URL_TS}/manifest.json" >/dev/null

# ---------- remote retention (keep N days) ----------
if [[ "$RETENTION_REMOTE_DAYS" -gt 0 ]]; then
  log "Applying remote retention: ${RETENTION_REMOTE_DAYS} days"
  # list remote folders under prefix, then delete older than cutoff
  list_dirs() { azcopy list "$(with_sas "$REMOTE_BASE")" --recursive=false | awk '/^DIR /{print $2}'; }
  ts_to_epoch() { date -u -d "${1:0:4}-${1:4:2}-${1:6:2} ${1:9:2}:${1:11:2}:${1:13:2}Z" +%s; }
  now=$(date -u +%s); cutoff=$(( now - RETENTION_REMOTE_DAYS*86400 ))
  for f in $(list_dirs | grep -E '^[0-9]{8}T[0-9]{6}Z$' || true); do
    e=$(ts_to_epoch "$f" || echo 0)
    if [[ "$e" -gt 0 && "$e" -lt "$cutoff" ]]; then
      log "Deleting remote: $f"
      azcopy rm "$(with_sas "${REMOTE_BASE}/${f}")" --recursive=true --from-to=BlobFS >/dev/null
    fi
  done
fi
az_logout

log "Done. Local snapshot: ${LOCAL_DIR} | Remote: ${REMOTE_BASE}/${TS_UTC}"
