#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="${BITHUMB_BOT_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd -P)}"
cd "${REPO_ROOT}"

DEFAULT_ENV_FILE="/home/ec2-user/bithumb-runtime/env/live.verify.env"
DEFAULT_SERVICE_NAME="bithumb-bot.service"
DEFAULT_MODE="live"
DEFAULT_LOG_PATTERN='error|traceback|exception|fatal|warn|critical|fail|blocked|halt|reject|dust|resume|reconcile|preflight'

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
  echo "[WARN] diag_followup.sh is intended for MODE=live (current MODE=${MODE})" >&2
fi

usage() {
  cat <<'EOF'
Usage: scripts/diag_followup.sh [options]

Options:
  --title TEXT         Title used in the output directory name
  --cmd SPEC           Add one follow-up spec (repeatable)
  --cmd-file PATH      Read follow-up specs from file, one per line
  --out-dir PATH       Output directory. Relative paths are resolved under the managed diagnostics root.
  -h, --help           Show this help text

Supported SPEC formats:
  sqlite:SQL
  journal:N
  journal_since:TIME_EXPR
  service_status:UNIT
  service_cat:UNIT
  file_tail:PATH[:N]
  file_head:PATH[:N]
  file_grep:PATH:REGEX
  loggrep:REGEX
  report:NAME
  runtime_recent:N
  runtime_tree:N
  db_check
  table_info:TABLE
  table_sample:TABLE[:N]
  shell:COMMAND
EOF
}

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
    print(f"[FOLLOWUP] {exc}", file=sys.stderr)
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

safe_shell_command() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import shlex
import sys

allowed = {
    "tail",
    "head",
    "grep",
    "journalctl",
    "systemctl",
    "sqlite3",
    "find",
    "ls",
    "du",
    "cat",
    "stat",
    "git",
    "pwd",
}

cmd = sys.argv[1]
if any(ch in cmd for ch in ";|&><`$(){}"):
    raise SystemExit("unsafe shell metacharacter detected")
parts = shlex.split(cmd)
if not parts:
    raise SystemExit("empty shell command")
if parts[0] not in allowed:
    raise SystemExit(f"command not allowed: {parts[0]}")
if parts[0] == "systemctl":
    if len(parts) < 3 or parts[1] not in {"status", "cat"}:
        raise SystemExit("systemctl only allows status/cat")
if parts[0] == "sqlite3" and "-readonly" not in parts:
    raise SystemExit("sqlite3 shell command must include -readonly")
for item in parts:
    sys.stdout.write(item)
    sys.stdout.write("\0")
PY
}

require_readonly_sql() {
  local sql="$1"
  python3 - "$sql" <<'PY'
import re
import sys

sql = sys.argv[1].strip()
normalized = re.sub(r"\s+", " ", sql).strip().rstrip(";").strip()
lowered = normalized.lower()

allowed = [
    r"select\b",
    r"pragma quick_check\b",
    r"pragma integrity_check\b",
    r"pragma table_info\s*\(",
    r"\.tables$",
]
if not normalized:
    raise SystemExit("sql spec is empty")
if ";" in normalized:
    raise SystemExit("multiple statements are not allowed")
if not any(re.match(pattern, lowered) for pattern in allowed):
    raise SystemExit(f"sql not allowed: {sql}")
print(normalized)
PY
}

sanitize_title() {
  local raw="${1:-followup}"
  local cleaned
  cleaned="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/_/g; s/^_+//; s/_+$//; s/_+/_/g')"
  if [[ -z "${cleaned}" ]]; then
    cleaned="followup"
  fi
  printf '%s\n' "${cleaned}"
}

db_has_table() {
  local table="$1"
  if [[ ! -f "${PRIMARY_DB_PATH}" ]]; then
    return 1
  fi
  sqlite3 -readonly "${PRIMARY_DB_PATH}" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='${table}' LIMIT 1;" 2>/dev/null | grep -q '^1$'
}

require_identifier() {
  local raw="$1"
  if [[ ! "${raw}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "invalid identifier: ${raw}" >&2
    return 1
  fi
}

resolve_report_file() {
  local name="$1"
  case "${name}" in
    ops_report) latest_file_in_dir "${REPORTS_DIR}/ops_report" ;;
    recovery_report) latest_file_in_dir "${REPORTS_DIR}/recovery_report" ;;
    cash_drift_report) latest_file_in_dir "${REPORTS_DIR}/cash_drift_report" ;;
    fee_diagnostics) latest_file_in_dir "${REPORTS_DIR}/fee_diagnostics" ;;
    strategy_validation) latest_file_in_dir "${REPORTS_DIR}/strategy_validation" ;;
    market_catalog_snapshot) latest_file_in_dir "${DERIVED_DIR}/market_catalog_snapshot" ;;
    *)
      echo "unknown report name: ${name}" >&2
      return 1
      ;;
  esac
}

append_result() {
  local spec="$1"
  {
    echo "===== SPEC ====="
    echo "${spec}"
    echo "===== RESULT ====="
  } >> "${RESULTS_FILE}"
}

TITLE=""
OUT_DIR_INPUT=""
declare -a SPECS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --title)
      TITLE="${2:-}"
      shift 2
      ;;
    --cmd)
      SPECS+=("${2:-}")
      shift 2
      ;;
    --cmd-file)
      CMD_FILE="${2:-}"
      if [[ -f "${CMD_FILE}" ]]; then
        while IFS= read -r line || [[ -n "${line}" ]]; do
          [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
          SPECS+=("${line}")
        done < "${CMD_FILE}"
      else
        echo "cmd file not found: ${CMD_FILE}" >&2
        exit 1
      fi
      shift 2
      ;;
    --out-dir)
      OUT_DIR_INPUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ${#SPECS[@]} -eq 0 ]]; then
  echo "at least one --cmd or --cmd-file entry is required" >&2
  usage >&2
  exit 1
fi

PRIMARY_DB_PATH="${DB_PATH:-$(path_query primary-db)}"
RUN_LOCK_PATH="${RUN_LOCK_PATH:-$(path_query run-lock)}"
RUNTIME_STATE_PATH="$(path_query runtime-state)"
BACKUP_MODE_DIR="$(path_query backup-mode-dir)"
LOG_DIR="$(path_query log-dir)"
DATA_DIR="$(path_query data-dir)"
DERIVED_DIR="$(path_query derived-dir)"
REPORTS_DIR="$(path_query reports-dir)"
DIAGNOSTICS_ROOT="${DIAGNOSTICS_ROOT:-${BACKUP_MODE_DIR}/diagnostics}"

validate_live_override_path "DB_PATH" "${PRIMARY_DB_PATH}"
validate_live_override_path "RUN_LOCK_PATH" "${RUN_LOCK_PATH}"
validate_live_override_path "DIAGNOSTICS_ROOT" "${DIAGNOSTICS_ROOT}"

TS="$(date +%Y%m%d_%H%M%S)"
SANITIZED_TITLE="$(sanitize_title "${TITLE:-followup}")"
DEFAULT_OUT_DIR="${DIAGNOSTICS_ROOT}/followup_${SANITIZED_TITLE}_${TS}"

if [[ -n "${OUT_DIR_INPUT}" ]]; then
  if [[ "${OUT_DIR_INPUT}" = /* ]]; then
    OUT_DIR="${OUT_DIR_INPUT}"
  else
    OUT_DIR="${DIAGNOSTICS_ROOT}/${OUT_DIR_INPUT}"
  fi
else
  OUT_DIR="${DEFAULT_OUT_DIR}"
fi

validate_live_override_path "FOLLOWUP_OUT_DIR" "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

RESULTS_FILE="${OUT_DIR}/10_results.txt"
LATEST_APP_LOG="$(find "${LOG_DIR}" -maxdepth 1 -type f -name '*.log' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2- || true)"
if [[ -z "${LATEST_APP_LOG}" ]]; then
  LATEST_APP_LOG="$(latest_file_in_dir "${LOG_DIR}/app")"
fi
if [[ -z "${LATEST_APP_LOG}" ]]; then
  LATEST_APP_LOG="$(first_existing_file "$(find "${LOG_DIR}" -type f \( -name 'live-run-*.log' -o -name 'live-verify-*.log' \) -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)" || true)"
fi

{
  echo "timestamp=${TS}"
  echo "date=$(date --iso-8601=seconds)"
  echo "repo_root=${REPO_ROOT}"
  echo "mode=${MODE}"
  echo "service_name=${SERVICE_NAME}"
  echo "env_file=${BITHUMB_ENV_FILE}"
  echo "out_dir=${OUT_DIR}"
  echo "diagnostics_root=${DIAGNOSTICS_ROOT}"
  echo "primary_db=${PRIMARY_DB_PATH}"
  echo "run_lock=${RUN_LOCK_PATH}"
  echo "runtime_state=${RUNTIME_STATE_PATH}"
  echo "log_dir=${LOG_DIR}"
  echo "reports_dir=${REPORTS_DIR}"
  echo "derived_dir=${DERIVED_DIR}"
  echo "title=${TITLE:-followup}"
  echo "latest_app_log=${LATEST_APP_LOG:-missing}"
} > "${OUT_DIR}/00_meta.txt"

printf '%s\n' "${SPECS[@]}" > "${OUT_DIR}/requested_specs.txt"
: > "${RESULTS_FILE}"

for spec in "${SPECS[@]}"; do
  append_result "${spec}"
  case "${spec}" in
    sqlite:*)
      if sql="$(require_readonly_sql "${spec#sqlite:}" 2>&1)"; then
        sqlite3 -readonly -header -column "${PRIMARY_DB_PATH}" "${sql}" >> "${RESULTS_FILE}" 2>&1 || true
      else
        echo "${sql}" >> "${RESULTS_FILE}"
      fi
      ;;
    journal:[0-9]*)
      count="${spec#journal:}"
      run_with_optional_sudo journalctl -u "${SERVICE_NAME}" -n "${count}" --no-pager >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    journal_since:*)
      since_expr="${spec#journal_since:}"
      run_with_optional_sudo journalctl -u "${SERVICE_NAME}" --since "${since_expr}" --no-pager >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    service_status:*)
      unit="${spec#service_status:}"
      run_with_optional_sudo systemctl status "${unit}" --no-pager >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    service_cat:*)
      unit="${spec#service_cat:}"
      run_with_optional_sudo systemctl cat "${unit}" >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    file_tail:*)
      payload="${spec#file_tail:}"
      path="${payload%:*}"
      count="${payload##*:}"
      if [[ "${path}" == "${count}" ]]; then
        count=200
      fi
      tail -n "${count}" "${path}" >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    file_head:*)
      payload="${spec#file_head:}"
      path="${payload%:*}"
      count="${payload##*:}"
      if [[ "${path}" == "${count}" ]]; then
        count=200
      fi
      head -n "${count}" "${path}" >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    file_grep:*)
      payload="${spec#file_grep:}"
      path="${payload%%:*}"
      regex="${payload#*:}"
      grep -Ein "${regex}" "${path}" >> "${RESULTS_FILE}" 2>&1 || true
      ;;
    loggrep:*)
      regex="${spec#loggrep:}"
      {
        echo "-- journal"
        run_with_optional_sudo journalctl -u "${SERVICE_NAME}" -n 2000 --no-pager 2>&1 | grep -Ein "${regex}" || true
        echo
        echo "-- latest_file_log"
        if [[ -n "${LATEST_APP_LOG:-}" && -f "${LATEST_APP_LOG}" ]]; then
          grep -Ein "${regex}" "${LATEST_APP_LOG}" 2>&1 || true
        else
          echo "latest app log missing"
        fi
      } >> "${RESULTS_FILE}"
      ;;
    report:*)
      report_name="${spec#report:}"
      report_file="$(resolve_report_file "${report_name}" || true)"
      if [[ -n "${report_file}" && -f "${report_file}" ]]; then
        {
          echo "report_file=${report_file}"
          stat "${report_file}" 2>&1 || true
          echo
          sed -n '1,200p' "${report_file}" 2>&1 || true
        } >> "${RESULTS_FILE}"
      else
        echo "report missing for ${report_name}" >> "${RESULTS_FILE}"
      fi
      ;;
    runtime_recent:[0-9]*)
      count="${spec#runtime_recent:}"
      find "${DATA_DIR}" "${LOG_DIR}" "${BACKUP_MODE_DIR}" -type f -printf '%T@ %TY-%Tm-%Td %TH:%TM:%TS %p\n' 2>/dev/null \
        | sort -nr | head -n "${count}" >> "${RESULTS_FILE}" || true
      ;;
    runtime_tree:[0-9]*)
      depth="${spec#runtime_tree:}"
      for dir in "${DATA_DIR}" "${LOG_DIR}" "${BACKUP_MODE_DIR}"; do
        {
          echo "-- ${dir}"
          find "${dir}" -maxdepth "${depth}" -printf '%TY-%Tm-%Td %TH:%TM %y %p\n' 2>/dev/null | sort
          echo
        } >> "${RESULTS_FILE}"
      done
      ;;
    db_check)
      {
        echo "== quick_check =="
        sqlite3 -readonly "${PRIMARY_DB_PATH}" "PRAGMA quick_check;" 2>&1 || true
        echo
        echo "== integrity_check =="
        sqlite3 -readonly "${PRIMARY_DB_PATH}" "PRAGMA integrity_check;" 2>&1 || true
        echo
        echo "== tables =="
        sqlite3 -readonly "${PRIMARY_DB_PATH}" ".tables" 2>&1 || true
      } >> "${RESULTS_FILE}"
      ;;
    table_info:*)
      table="${spec#table_info:}"
      if require_identifier "${table}"; then
        sqlite3 -readonly -header -column "${PRIMARY_DB_PATH}" "PRAGMA table_info(${table});" >> "${RESULTS_FILE}" 2>&1 || true
      else
        echo "invalid table name: ${table}" >> "${RESULTS_FILE}"
      fi
      ;;
    table_sample:*)
      payload="${spec#table_sample:}"
      table="${payload%:*}"
      count="${payload##*:}"
      if [[ "${table}" == "${count}" ]]; then
        count=20
      fi
      if ! [[ "${count}" =~ ^[0-9]+$ ]]; then
        echo "invalid sample count: ${count}" >> "${RESULTS_FILE}"
      elif ! require_identifier "${table}"; then
        echo "invalid table name: ${table}" >> "${RESULTS_FILE}"
      elif db_has_table "${table}"; then
        sqlite3 -readonly -header -column "${PRIMARY_DB_PATH}" "SELECT * FROM ${table} ORDER BY rowid DESC LIMIT ${count};" >> "${RESULTS_FILE}" 2>&1 || true
      else
        echo "table missing: ${table}" >> "${RESULTS_FILE}"
      fi
      ;;
    shell:*)
      raw_command="${spec#shell:}"
      if ! mapfile -d '' -t shell_parts < <(safe_shell_command "${raw_command}"); then
        echo "unsafe shell command rejected: ${raw_command}" >> "${RESULTS_FILE}"
      elif [[ ${#shell_parts[@]} -eq 0 ]]; then
        echo "unsafe shell command rejected: ${raw_command}" >> "${RESULTS_FILE}"
      elif [[ "${shell_parts[0]}" == "journalctl" ]]; then
        run_with_optional_sudo "${shell_parts[@]}" >> "${RESULTS_FILE}" 2>&1 || true
      elif [[ "${shell_parts[0]}" == "systemctl" ]]; then
        run_with_optional_sudo "${shell_parts[@]}" >> "${RESULTS_FILE}" 2>&1 || true
      else
        "${shell_parts[@]}" >> "${RESULTS_FILE}" 2>&1 || true
      fi
      ;;
    *)
      echo "unsupported spec: ${spec}" >> "${RESULTS_FILE}"
      ;;
  esac
  echo >> "${RESULTS_FILE}"
done

{
  echo "# Follow-Up Bundle"
  echo
  echo "## Metadata"
  echo "- generated_at: $(date --iso-8601=seconds)"
  echo "- mode: ${MODE}"
  echo "- service: ${SERVICE_NAME}"
  echo "- env_file: ${BITHUMB_ENV_FILE}"
  echo "- out_dir: ${OUT_DIR}"
  echo "- primary_db: ${PRIMARY_DB_PATH}"
  echo "- run_lock: ${RUN_LOCK_PATH}"
  echo
  echo "## Requested Specs"
  printf '%s\n' "${SPECS[@]}" | sed 's/^/- /'
  echo
  echo "## Suggested LLM Prompt"
  echo "Update the root-cause ranking for this live incident using the prior unknown bundle plus this follow-up bundle."
  echo "Cite exact evidence, explicitly note what changed confidence, and keep wrong-order, duplicate-order, reconcile, and restart-safety risks first."
  echo "Request more read-only checks only if the current evidence is still insufficient, and express them strictly in diag_followup.sh spec format."
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
