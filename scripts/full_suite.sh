#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
WORK_DIR="${CODEX_PYTEST_WORK_DIR:-${TMPDIR:-/tmp}/bithumb-bot-codex-pytest}"
LOG_DIR="${CODEX_PYTEST_LOG_DIR:-${WORK_DIR}/logs}"
ITERATION="${CODEX_PYTEST_ITERATION:-manual}"

mkdir -p "${LOG_DIR}" "${WORK_DIR}"

timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
log_file="${LOG_DIR}/full_suite_${timestamp}_iter${ITERATION}.log"
latest_log_file="${WORK_DIR}/latest_full_suite_log"

cd "${PROJECT_ROOT}" || exit 2

pytest_workers="${PYTEST_XDIST_WORKERS:-8}"
pytest_dist="${PYTEST_XDIST_DIST:-worksteal}"
full_suite_command="PYTEST_XDIST_WORKERS=\"${pytest_workers}\" PYTEST_XDIST_DIST=\"${pytest_dist}\" ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh"

pytest_exit=0
artifact_exit="not_reached"
final_exit=0

{
  echo "[FULL-SUITE] utc_start=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "[FULL-SUITE] project_root=${PROJECT_ROOT}"
  echo "[FULL-SUITE] iteration=${ITERATION}"
  echo "[FULL-SUITE] command=${full_suite_command}"
  echo "[FULL-SUITE] log_file=${log_file}"
  echo
} | tee "${log_file}"

PYTEST_XDIST_WORKERS="${pytest_workers}" \
PYTEST_XDIST_DIST="${pytest_dist}" \
./scripts/run_full_pytest_tests.sh 2>&1 | tee -a "${log_file}"
pytest_exit="${PIPESTATUS[0]}"

{
  echo
  echo "[FULL-SUITE] pytest_runner_exit_code=${pytest_exit}"
} | tee -a "${log_file}"

if [[ "${pytest_exit}" -eq 0 ]]; then
  ./scripts/check_repo_runtime_artifacts.sh 2>&1 | tee -a "${log_file}"
  artifact_exit="${PIPESTATUS[0]}"
  final_exit="${artifact_exit}"
else
  final_exit="${pytest_exit}"
fi

{
  echo
  echo "[FULL-SUITE] artifact_check_exit_code=${artifact_exit}"
  echo "[FULL-SUITE] final_exit_code=${final_exit}"
  echo "[FULL-SUITE] log_file=${log_file}"
  echo "[FULL-SUITE] utc_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
} | tee -a "${log_file}"

printf '%s\n' "${log_file}" > "${latest_log_file}"

exit "${final_exit}"
