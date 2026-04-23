#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="${BITHUMB_BOT_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd -P)}"
cd "${REPO_ROOT}"

DEFAULT_ENV_FILE="/home/ec2-user/bithumb-runtime/env/live.verify.env"
DEFAULT_SERVICE_NAME="bithumb-bot.service"
DEFAULT_MODE="live"
JOURNAL_PATTERN='error|traceback|exception|fatal|warn|critical|fail|blocked|halt|reject|dust'
LOG_PATTERN='error|traceback|exception|fatal|warn|critical|fail|blocked|halt|reject|dust|resume|reconcile|preflight'

BITHUMB_ENV_FILE="${BITHUMB_ENV_FILE:-${DEFAULT_ENV_FILE}}"
SERVICE_NAME="${SERVICE_NAME:-${DEFAULT_SERVICE_NAME}}"
MODE="${MODE:-${DEFAULT_MODE}}"

if [[ -f "${BITHUMB_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${BITHUMB_ENV_FILE}"
  set +a
fi

if [[ "${MODE}" != "live" ]]; then
  echo "[WARN] diag_unknown.sh is intended for MODE=live (current MODE=${MODE})" >&2
fi

path_query() {
  local kind="$1"
  MODE="$MODE" \
  ENV_ROOT="${ENV_ROOT:-}" RUN_ROOT="${RUN_ROOT:-}" DATA_ROOT="${DATA_ROOT:-}" \
  LOG_ROOT="${LOG_ROOT:-}" BACKUP_ROOT="${BACKUP_ROOT:-}" ARCHIVE_ROOT="${ARCHIVE_ROOT:-}" \
  python3 "$REPO_ROOT/scripts/path_query.py" --project-root "$REPO_ROOT" --kind "$kind"
}

validate_live_override_path() {
  local key="$1"
  local path="$2"
  if [[ "${MODE}" != "live" ]]; then
    return 0
  fi
  PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}" python3 - "$REPO_ROOT" "$key" "$path" <<'PY'
from pathlib import Path
import sys
from bithumb_bot.paths import PathManager, PathPolicyError

project_root = Path(sys.argv[1]).resolve()
key = sys.argv[2]
path = sys.argv[3]
try:
    PathManager._resolve_explicit_root(key, path, "live", project_root)
except PathPolicyError as exc:
    print(f"[DIAG] {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

run_with_optional_sudo() {
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo -n "$@"
  else
    "$@"
  fi
}

latest_file_in_dir() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    find "$dir" -maxdepth 1 -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-
  fi
}

first_existing_file() {
  local candidate
  for candidate in "$@"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

db_has_table() {
  local table="$1"
  if [[ ! -f "${PRIMARY_DB_PATH}" ]]; then
    return 1
  fi
  sqlite3 -readonly "${PRIMARY_DB_PATH}" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='${table}' LIMIT 1;" 2>/dev/null | grep -q '^1$'
}

db_section() {
  local label="$1"
  local sql="$2"
  {
    echo "== ${label} =="
    sqlite3 -readonly -header -column "${PRIMARY_DB_PATH}" "${sql}" 2>&1 || true
    echo
  } >> "${OUT_DIR}/70_db_checks.txt"
}

report_section() {
  local name="$1"
  local dir="$2"
  local latest
  latest="$(latest_file_in_dir "$dir")"
  {
    echo "== ${name} =="
    echo "dir=${dir}"
    if [[ -n "${latest}" && -f "${latest}" ]]; then
      echo "latest=${latest}"
      stat "${latest}" 2>&1 || true
      echo
      sed -n '1,200p' "${latest}" 2>&1 || true
    else
      echo "latest=missing"
    fi
    echo
  } >> "${OUT_DIR}/80_reports.txt"
}

PRIMARY_DB_PATH="${DB_PATH:-$(path_query primary-db)}"
RUN_LOCK_PATH="${RUN_LOCK_PATH:-$(path_query run-lock)}"
RUNTIME_STATE_PATH="$(path_query runtime-state)"
BACKUP_DB_DIR="${BACKUP_DIR:-$(path_query backup-db-dir)}"
BACKUP_SNAPSHOTS_DIR="${SNAPSHOT_ROOT:-$(path_query backup-snapshots-dir)}"
BACKUP_MODE_DIR="$(path_query backup-mode-dir)"
LOG_DIR="$(path_query log-dir)"
DATA_DIR="$(path_query data-dir)"
DERIVED_DIR="$(path_query derived-dir)"
REPORTS_DIR="$(path_query reports-dir)"
DIAGNOSTICS_ROOT="${DIAGNOSTICS_ROOT:-${BACKUP_MODE_DIR}/diagnostics}"

validate_live_override_path "DB_PATH" "${PRIMARY_DB_PATH}"
validate_live_override_path "RUN_LOCK_PATH" "${RUN_LOCK_PATH}"
validate_live_override_path "BACKUP_DIR" "${BACKUP_DB_DIR}"
validate_live_override_path "SNAPSHOT_ROOT" "${BACKUP_SNAPSHOTS_DIR}"
validate_live_override_path "DIAGNOSTICS_ROOT" "${DIAGNOSTICS_ROOT}"

TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${DIAGNOSTICS_ROOT}/unknown_${TS}"
mkdir -p "${OUT_DIR}"

APP_LOG_CANDIDATE="$(find "${LOG_DIR}" -maxdepth 1 -type f -name '*.log' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2- || true)"
if [[ -z "${APP_LOG_CANDIDATE}" ]]; then
  APP_LOG_CANDIDATE="$(latest_file_in_dir "${LOG_DIR}/app")"
fi
if [[ -z "${APP_LOG_CANDIDATE}" ]]; then
  APP_LOG_CANDIDATE="$(first_existing_file "$(find "${LOG_DIR}" -type f \( -name 'live-run-*.log' -o -name 'live-verify-*.log' \) -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)" || true)"
fi

{
  echo "timestamp=${TS}"
  echo "date=$(date --iso-8601=seconds)"
  echo "pwd=$(pwd)"
  echo "repo_root=${REPO_ROOT}"
  echo "mode=${MODE}"
  echo "service_name=${SERVICE_NAME}"
  echo "env_file=${BITHUMB_ENV_FILE}"
  echo "out_dir=${OUT_DIR}"
  echo "diagnostics_root=${DIAGNOSTICS_ROOT}"
  echo "primary_db=${PRIMARY_DB_PATH}"
  echo "run_lock=${RUN_LOCK_PATH}"
  echo "runtime_state=${RUNTIME_STATE_PATH}"
  echo "backup_db_dir=${BACKUP_DB_DIR}"
  echo "backup_snapshots_dir=${BACKUP_SNAPSHOTS_DIR}"
  echo "log_dir=${LOG_DIR}"
  echo "reports_dir=${REPORTS_DIR}"
  echo "derived_dir=${DERIVED_DIR}"
  echo "app_log_candidate=${APP_LOG_CANDIDATE:-missing}"
} > "${OUT_DIR}/00_meta.txt"

{
  echo "== git root =="
  git rev-parse --show-toplevel 2>&1 || true
  echo
  echo "== head =="
  git rev-parse HEAD 2>&1 || true
  echo
  echo "== branch =="
  git branch --show-current 2>&1 || true
  echo
  echo "== status =="
  git status --short --branch 2>&1 || true
  echo
  echo "== recent commits =="
  git log --decorate --oneline -n 20 2>&1 || true
} > "${OUT_DIR}/10_git.txt"

{
  echo "== service status: ${SERVICE_NAME} =="
  run_with_optional_sudo systemctl status "${SERVICE_NAME}" --no-pager 2>&1 || true
  echo
  echo "== service cat: ${SERVICE_NAME} =="
  run_with_optional_sudo systemctl cat "${SERVICE_NAME}" 2>&1 || true
  echo
  echo "== service status: bithumb-bot-healthcheck.service =="
  run_with_optional_sudo systemctl status bithumb-bot-healthcheck.service --no-pager 2>&1 || true
  echo
  echo "== timer status: bithumb-bot-healthcheck.timer =="
  run_with_optional_sudo systemctl status bithumb-bot-healthcheck.timer --no-pager 2>&1 || true
  echo
  echo "== service status: bithumb-bot-backup.service =="
  run_with_optional_sudo systemctl status bithumb-bot-backup.service --no-pager 2>&1 || true
  echo
  echo "== timer status: bithumb-bot-backup.timer =="
  run_with_optional_sudo systemctl status bithumb-bot-backup.timer --no-pager 2>&1 || true
} > "${OUT_DIR}/20_service.txt"

{
  echo "== journal: ${SERVICE_NAME} =="
  run_with_optional_sudo journalctl -u "${SERVICE_NAME}" -n 500 --no-pager 2>&1 || true
  echo
  echo "== journal: bithumb-bot-healthcheck.service =="
  run_with_optional_sudo journalctl -u bithumb-bot-healthcheck.service -n 200 --no-pager 2>&1 || true
  echo
  echo "== journal: bithumb-bot-backup.service =="
  run_with_optional_sudo journalctl -u bithumb-bot-backup.service -n 200 --no-pager 2>&1 || true
  echo
  echo "== journal pattern matches (${JOURNAL_PATTERN}) =="
  run_with_optional_sudo journalctl -u "${SERVICE_NAME}" -u bithumb-bot-healthcheck.service -u bithumb-bot-backup.service -n 2000 --no-pager 2>&1 \
    | grep -Ein "${JOURNAL_PATTERN}" || true
} > "${OUT_DIR}/30_journal.txt"

{
  echo "== env file keys (${BITHUMB_ENV_FILE}) =="
  if [[ -f "${BITHUMB_ENV_FILE}" ]]; then
    grep -E '^[A-Z0-9_]+=' "${BITHUMB_ENV_FILE}" | sed 's/=.*$/=REDACTED/' || true
  else
    echo "env_file_missing"
  fi
  echo
  echo "== important live keys =="
  for key in \
    MODE DB_PATH ENV_ROOT RUN_ROOT DATA_ROOT LOG_ROOT BACKUP_ROOT ARCHIVE_ROOT \
    LIVE_DRY_RUN LIVE_REAL_ORDER_ARMED MAX_ORDER_KRW MAX_DAILY_LOSS_KRW MAX_DAILY_ORDER_COUNT \
    MAX_ORDERBOOK_SPREAD_BPS MAX_MARKET_SLIPPAGE_BPS LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS \
    SLACK_WEBHOOK_URL BITHUMB_API_KEY BITHUMB_API_SECRET \
    LIVE_MIN_ORDER_QTY LIVE_ORDER_QTY_STEP MIN_ORDER_NOTIONAL_KRW LIVE_ORDER_MAX_QTY_DECIMALS \
    STRATEGY_NAME SMA_SHORT SMA_LONG SMA_FILTER_GAP_MIN_RATIO SMA_FILTER_VOL_MIN_RANGE_RATIO \
    SMA_COST_EDGE_ENABLED SMA_COST_EDGE_MIN_RATIO PAIR MARKET; do
    if [[ -n "${!key:-}" ]]; then
      echo "${key}=present"
    else
      echo "${key}=missing"
    fi
  done
} > "${OUT_DIR}/40_env_redacted.txt"

{
  echo "== managed paths =="
  printf 'mode=%s\nprimary_db=%s\nrun_lock=%s\nruntime_state=%s\nbackup_db_dir=%s\nbackup_snapshots_dir=%s\nlog_dir=%s\ndata_dir=%s\nreports_dir=%s\nderived_dir=%s\ndiagnostics_root=%s\n' \
    "${MODE}" "${PRIMARY_DB_PATH}" "${RUN_LOCK_PATH}" "${RUNTIME_STATE_PATH}" "${BACKUP_DB_DIR}" "${BACKUP_SNAPSHOTS_DIR}" "${LOG_DIR}" "${DATA_DIR}" "${REPORTS_DIR}" "${DERIVED_DIR}" "${DIAGNOSTICS_ROOT}"
  echo
  echo "== check_live_runtime.sh =="
  "${REPO_ROOT}/scripts/check_live_runtime.sh" 2>&1 || true
} > "${OUT_DIR}/50_paths.txt"

{
  echo "== managed root listing =="
  for dir in \
    "${ENV_ROOT:-}" "${RUN_ROOT:-}" "${DATA_ROOT:-}" "${LOG_ROOT:-}" "${BACKUP_ROOT:-}" "${ARCHIVE_ROOT:-}" \
    "${DATA_DIR}" "${LOG_DIR}" "${BACKUP_MODE_DIR}"; do
    if [[ -n "${dir}" && -d "${dir}" ]]; then
      echo "-- ${dir}"
      find "${dir}" -maxdepth 3 -mindepth 0 -printf '%TY-%Tm-%Td %TH:%TM %y %p\n' 2>/dev/null | sort | tail -n 200 || true
      echo
    fi
  done
  echo "== runtime disk usage =="
  du -sh "${DATA_DIR}" "${LOG_DIR}" "${BACKUP_MODE_DIR}" 2>/dev/null || true
  echo
  echo "== recent runtime files =="
  find "${DATA_DIR}" "${LOG_DIR}" "${BACKUP_MODE_DIR}" -type f -printf '%T@ %TY-%Tm-%Td %TH:%TM:%TS %p\n' 2>/dev/null \
    | sort -nr | head -n 200 || true
} > "${OUT_DIR}/60_runtime_tree.txt"

{
  echo "== db file info =="
  ls -lh "${PRIMARY_DB_PATH}" 2>&1 || true
  stat "${PRIMARY_DB_PATH}" 2>&1 || true
  echo
  echo "== sqlite quick_check =="
  sqlite3 -readonly "${PRIMARY_DB_PATH}" "PRAGMA quick_check;" 2>&1 || true
  echo
  echo "== sqlite integrity_check =="
  sqlite3 -readonly "${PRIMARY_DB_PATH}" "PRAGMA integrity_check;" 2>&1 || true
  echo
  echo "== sqlite tables =="
  sqlite3 -readonly "${PRIMARY_DB_PATH}" ".tables" 2>&1 || true
  echo
} > "${OUT_DIR}/70_db_checks.txt"

if db_has_table trades; then
  db_section "trades latest rows" "SELECT * FROM trades ORDER BY rowid DESC LIMIT 20;"
fi
if db_has_table orders; then
  db_section "orders latest rows" "SELECT * FROM orders ORDER BY rowid DESC LIMIT 20;"
fi
if db_has_table fills; then
  db_section "fills latest rows" "SELECT * FROM fills ORDER BY rowid DESC LIMIT 20;"
fi
if db_has_table balance_snapshots; then
  db_section "balance_snapshots latest rows" "SELECT * FROM balance_snapshots ORDER BY rowid DESC LIMIT 20;"
fi
if db_has_table reconcile_events; then
  db_section "reconcile_events latest rows" "SELECT * FROM reconcile_events ORDER BY rowid DESC LIMIT 20;"
fi

REPORTS_BASE="${REPORTS_DIR}"
report_section "ops_report" "${REPORTS_BASE}/ops_report"
report_section "recovery_report" "${REPORTS_BASE}/recovery_report"
report_section "cash_drift_report" "${REPORTS_BASE}/cash_drift_report"
report_section "fee_diagnostics" "${REPORTS_BASE}/fee_diagnostics"
report_section "strategy_validation" "${REPORTS_BASE}/strategy_validation"
report_section "market_catalog_snapshot" "${DERIVED_DIR}/market_catalog_snapshot"

{
  echo "== latest file log candidate =="
  if [[ -n "${APP_LOG_CANDIDATE:-}" && -f "${APP_LOG_CANDIDATE}" ]]; then
    echo "path=${APP_LOG_CANDIDATE}"
    stat "${APP_LOG_CANDIDATE}" 2>&1 || true
    echo
    echo "== tail =="
    tail -n 200 "${APP_LOG_CANDIDATE}" 2>&1 || true
    echo
    echo "== grep (${LOG_PATTERN}) =="
    grep -Ein "${LOG_PATTERN}" "${APP_LOG_CANDIDATE}" 2>&1 || true
  else
    echo "path=missing"
  fi
} > "${OUT_DIR}/90_logs.txt"

JOURNAL_MATCH_COUNT="$(grep -Eic "${JOURNAL_PATTERN}" "${OUT_DIR}/30_journal.txt" 2>/dev/null || true)"
LOG_MATCH_COUNT="$(grep -Eic "${LOG_PATTERN}" "${OUT_DIR}/90_logs.txt" 2>/dev/null || true)"
REPORT_FILE_COUNT="$(
  {
    [[ -d "${REPORTS_BASE}" ]] && find "${REPORTS_BASE}" -maxdepth 2 -type f 2>/dev/null
    [[ -d "${DERIVED_DIR}/market_catalog_snapshot" ]] && find "${DERIVED_DIR}/market_catalog_snapshot" -maxdepth 2 -type f 2>/dev/null
    true
  } | wc -l | tr -d ' '
)"

{
  echo "# Unknown Incident Bundle"
  echo
  echo "## Metadata"
  echo "- generated_at: $(date --iso-8601=seconds)"
  echo "- mode: ${MODE}"
  echo "- service: ${SERVICE_NAME}"
  echo "- env_file: ${BITHUMB_ENV_FILE}"
  echo "- repo_root: ${REPO_ROOT}"
  echo "- out_dir: ${OUT_DIR}"
  echo "- primary_db: ${PRIMARY_DB_PATH}"
  echo "- run_lock: ${RUN_LOCK_PATH}"
  echo "- runtime_state: ${RUNTIME_STATE_PATH}"
  echo
  echo "## Quick Observations"
  echo "- journal pattern matches: ${JOURNAL_MATCH_COUNT}"
  echo "- file log pattern matches: ${LOG_MATCH_COUNT}"
  echo "- report files discovered: ${REPORT_FILE_COUNT}"
  if [[ -f "${PRIMARY_DB_PATH}" ]]; then
    echo "- database file exists and DB checks were attempted"
  else
    echo "- database file missing at ${PRIMARY_DB_PATH}"
  fi
  if [[ -n "${APP_LOG_CANDIDATE:-}" && -f "${APP_LOG_CANDIDATE}" ]]; then
    echo "- latest file log candidate: ${APP_LOG_CANDIDATE}"
  else
    echo "- no file log candidate was found under managed live logs"
  fi
  echo
  echo "## File List"
  find "${OUT_DIR}" -maxdepth 1 -type f -printf '- %f\n' | sort
  echo
  echo "## Suggested LLM Prompt"
  echo "Analyze this live incident diagnostics bundle for a safety-first Bithumb trading bot."
  echo "Rank the top likely root causes, cite exact files and lines from the bundle, note state-integrity or wrong-order risk first, and separate evidence from inference."
  echo "Call out anything suggesting duplicate-order risk, reconcile/restart risk, halt/preflight blocks, dust-only suppression, or reporting inconsistency."
  echo "If more evidence is needed, ask only for additional read-only follow-up checks in the constrained spec format used by scripts/diag_followup.sh."
} > "${OUT_DIR}/summary.md"

{
  echo "# Bundle Manifest"
  echo "out_dir=${OUT_DIR}"
  echo
  find "${OUT_DIR}" -maxdepth 1 -type f ! -name 'bundle.txt' -printf '%f\n' | sort | while read -r name; do
    sha256sum "${OUT_DIR}/${name}" 2>/dev/null || true
  done
} > "${OUT_DIR}/bundle.txt"

echo "${OUT_DIR}"
