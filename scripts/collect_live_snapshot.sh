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
  echo "[WARN] collect_live_snapshot.sh is intended for MODE=live (current MODE=${MODE})" >&2
fi

path_query() {
  local kind="$1"
  PROJECT_ROOT="$REPO_ROOT" PYTHONPATH="$REPO_ROOT/src" MODE="$MODE" \
  ENV_ROOT="${ENV_ROOT:-}" RUN_ROOT="${RUN_ROOT:-}" DATA_ROOT="${DATA_ROOT:-}" \
  LOG_ROOT="${LOG_ROOT:-}" BACKUP_ROOT="${BACKUP_ROOT:-}" ARCHIVE_ROOT="${ARCHIVE_ROOT:-}" \
  python3 -m bithumb_bot.paths --project-root "$REPO_ROOT" --kind "$kind"
}

TS="$(date +%Y%m%d_%H%M%S)"
SNAPSHOT_ROOT="${SNAPSHOT_ROOT:-$(path_query backup-snapshots-dir)}"
PRIMARY_DB_PATH="${DB_PATH:-$(path_query primary-db)}"
RUN_LOCK_PATH="${RUN_LOCK_PATH:-$(path_query run-lock)}"
RUNTIME_STATE_PATH="$(path_query runtime-state)"
OUT_DIR="${SNAPSHOT_ROOT}/live_${TS}"
mkdir -p "${OUT_DIR}"

echo "Collecting snapshot into ${OUT_DIR}"

{
  echo "== date =="
  date
  echo
  echo "== pwd =="
  pwd
  echo
  echo "== managed paths =="
  printf 'mode=%s\nsnapshot_root=%s\nprimary_db=%s\nrun_lock=%s\nruntime_state=%s\n' \
    "$MODE" "$SNAPSHOT_ROOT" "$PRIMARY_DB_PATH" "$RUN_LOCK_PATH" "$RUNTIME_STATE_PATH"
} > "${OUT_DIR}/00_meta.txt"

{
  echo "== bithumb-bot.service =="
  sudo systemctl status bithumb-bot.service --no-pager || true
  echo
  echo "== bithumb-bot-healthcheck.timer =="
  sudo systemctl status bithumb-bot-healthcheck.timer --no-pager || true
  echo
  echo "== bithumb-bot-backup.timer =="
  sudo systemctl status bithumb-bot-backup.timer --no-pager || true
} > "${OUT_DIR}/10_systemd_status.txt"

{
  echo "== journal: bithumb-bot.service =="
  sudo journalctl -u bithumb-bot.service -n 200 --no-pager || true
  echo
  echo "== journal: bithumb-bot-healthcheck.service =="
  sudo journalctl -u bithumb-bot-healthcheck.service -n 100 --no-pager || true
  echo
  echo "== journal: bithumb-bot-backup.service =="
  sudo journalctl -u bithumb-bot-backup.service -n 100 --no-pager || true
} > "${OUT_DIR}/20_journal.txt"

{
  if [[ -n "${BITHUMB_ENV_FILE:-}" && -f "${BITHUMB_ENV_FILE}" ]]; then
    echo "== env redacted (${BITHUMB_ENV_FILE}) =="
    grep -E '^[A-Z0-9_]+=' "${BITHUMB_ENV_FILE}" | sed 's/=.*$/=REDACTED/' || true
  else
    echo "== env redacted =="
    echo "BITHUMB_ENV_FILE not set or missing; skipped"
  fi
} > "${OUT_DIR}/30_env_redacted.txt"

{
  echo "== repo ops files =="
  find docs scripts deploy/systemd -maxdepth 2 -type f 2>/dev/null | sort
  echo
  echo "== runtime/db files =="
  ls -lh "${PRIMARY_DB_PATH}" "${RUN_LOCK_PATH}" "${RUNTIME_STATE_PATH}" 2>/dev/null || true
  echo
  echo "== snapshot root recent files =="
  find "${SNAPSHOT_ROOT}" -maxdepth 2 -type f 2>/dev/null | sort | tail -50
} > "${OUT_DIR}/40_files.txt"

echo "Done: ${OUT_DIR}"
