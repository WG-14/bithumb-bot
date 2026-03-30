#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

if [[ -n "${BITHUMB_ENV_FILE:-}" ]]; then
  if [[ ! -f "$BITHUMB_ENV_FILE" ]]; then
    echo "[BACKUP] env file not found: $BITHUMB_ENV_FILE" >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$BITHUMB_ENV_FILE"
  set +a
fi

MODE="${MODE:-paper}"

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s\n' "$path"
  else
    echo "[BACKUP] relative path is not allowed: $path" >&2
    exit 1
  fi
}

path_query() {
  local kind="$1"
  PROJECT_ROOT="$PROJECT_ROOT" PYTHONPATH="$PROJECT_ROOT/src" MODE="$MODE" \
  ENV_ROOT="${ENV_ROOT:-}" RUN_ROOT="${RUN_ROOT:-}" DATA_ROOT="${DATA_ROOT:-}" \
  LOG_ROOT="${LOG_ROOT:-}" BACKUP_ROOT="${BACKUP_ROOT:-}" ARCHIVE_ROOT="${ARCHIVE_ROOT:-}" \
  python3 -m bithumb_bot.paths --project-root "$PROJECT_ROOT" --kind "$kind"
}

if [[ -n "${DB_PATH:-}" ]]; then
  DB_PATH="$(resolve_path "$DB_PATH")"
else
  DB_PATH="$(path_query primary-db)"
fi

if [[ -n "${BACKUP_DIR:-}" ]]; then
  BACKUP_DIR="$(resolve_path "$BACKUP_DIR")"
elif [[ -n "${BACKUP_ROOT:-}" ]]; then
  BACKUP_DIR="$(resolve_path "$BACKUP_ROOT")/$MODE/db"
else
  BACKUP_DIR="$(path_query backup-db-dir)"
fi

RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"
RETENTION_COUNT="${BACKUP_RETENTION_COUNT:-30}"
VERIFY_RESTORE="${BACKUP_VERIFY_RESTORE:-0}"

mkdir -p "$BACKUP_DIR"

if [[ ! -f "$DB_PATH" ]]; then
  echo "[BACKUP] database file not found: $DB_PATH" >&2
  exit 1
fi

ts="$(date +%Y%m%d_%H%M%S)"
base_name="$(basename "$DB_PATH")"
backup_path="$BACKUP_DIR/${base_name}.${ts}.sqlite"
tmp_backup="$backup_path.tmp"

sqlite3 "$DB_PATH" ".timeout 5000" ".backup '$tmp_backup'"
mv "$tmp_backup" "$backup_path"

if [[ -f "${DB_PATH}-wal" ]]; then
  cp "${DB_PATH}-wal" "$BACKUP_DIR/${base_name}.${ts}.wal"
fi
if [[ -f "${DB_PATH}-shm" ]]; then
  cp "${DB_PATH}-shm" "$BACKUP_DIR/${base_name}.${ts}.shm"
fi

find "$BACKUP_DIR" -type f -name "${base_name}.*" -mtime "+${RETENTION_DAYS}" -delete

mapfile -t backups < <(ls -1t "$BACKUP_DIR"/${base_name}.* 2>/dev/null || true)
if (( ${#backups[@]} > RETENTION_COUNT )); then
  for old_file in "${backups[@]:RETENTION_COUNT}"; do
    rm -f "$old_file"
  done
fi

if [[ "$VERIFY_RESTORE" == "1" ]]; then
  python3 "$PROJECT_ROOT/tools/verify_sqlite_restore.py" "$backup_path"
fi

echo "[BACKUP] created $backup_path"
