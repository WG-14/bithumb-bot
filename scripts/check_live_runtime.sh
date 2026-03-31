#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="${BITHUMB_BOT_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd -P)}"
cd "${REPO_ROOT}"

if [[ -n "${BITHUMB_ENV_FILE:-}" && -f "${BITHUMB_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${BITHUMB_ENV_FILE}"
  set +a
fi

MODE="${MODE:-live}"
if [[ "${MODE}" != "live" ]]; then
  echo "[WARN] check_live_runtime.sh is intended for MODE=live (current MODE=${MODE})" >&2
fi

path_query() {
  local kind="$1"
  MODE="$MODE" \
  ENV_ROOT="${ENV_ROOT:-}" RUN_ROOT="${RUN_ROOT:-}" DATA_ROOT="${DATA_ROOT:-}" \
  LOG_ROOT="${LOG_ROOT:-}" BACKUP_ROOT="${BACKUP_ROOT:-}" ARCHIVE_ROOT="${ARCHIVE_ROOT:-}" \
  python3 "$REPO_ROOT/scripts/path_query.py" --project-root "$REPO_ROOT" --kind "$kind"
}

RUN_LOCK_PATH="${RUN_LOCK_PATH:-$(path_query run-lock)}"
RUNTIME_STATE_PATH="$(path_query runtime-state)"
PRIMARY_DB_PATH="${DB_PATH:-$(path_query primary-db)}"
BACKUP_DB_DIR="${BACKUP_DIR:-$(path_query backup-db-dir)}"
BACKUP_SNAPSHOTS_DIR="$(path_query backup-snapshots-dir)"

echo "== systemd: bithumb-bot.service =="
sudo systemctl status bithumb-bot.service --no-pager || true
echo

echo "== systemd: healthcheck timer =="
sudo systemctl status bithumb-bot-healthcheck.timer --no-pager || true
echo

echo "== systemd: backup timer =="
sudo systemctl status bithumb-bot-backup.timer --no-pager || true
echo

echo "== recent journal (live) =="
sudo journalctl -u bithumb-bot.service -n 50 --no-pager || true
echo

echo "== recent journal (healthcheck) =="
sudo journalctl -u bithumb-bot-healthcheck.service -n 30 --no-pager || true
echo

echo "== managed runtime paths =="
printf 'mode=%s\nrun_lock=%s\nruntime_state=%s\nprimary_db=%s\nbackup_db_dir=%s\nbackup_snapshots_dir=%s\n' \
  "$MODE" "$RUN_LOCK_PATH" "$RUNTIME_STATE_PATH" "$PRIMARY_DB_PATH" "$BACKUP_DB_DIR" "$BACKUP_SNAPSHOTS_DIR"
echo

echo "== runtime/db files =="
ls -lh "$PRIMARY_DB_PATH" "$RUN_LOCK_PATH" "$RUNTIME_STATE_PATH" 2>/dev/null || true
echo

echo "== backup db files =="
ls -1t "$BACKUP_DB_DIR" 2>/dev/null | head -20 || true
echo

echo "== backup snapshot files =="
ls -1t "$BACKUP_SNAPSHOTS_DIR" 2>/dev/null | head -20 || true
