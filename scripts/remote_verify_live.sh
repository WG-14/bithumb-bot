#!/usr/bin/env bash
set -uo pipefail

# EC2-side verification for the deployed main branch.
# This script does not create repository-local runtime artifacts.

APP_DIR="${BITHUMB_REMOTE_APP_DIR:-${HOME}/apps/bithumb-bot}"
PYTEST_TMPDIR="${BITHUMB_PYTEST_TMPDIR:-${HOME}/tmp/pytest-tmp}"

passed_stages=()
failed_stages=()
skipped_stages=()

record_pass() {
  passed_stages+=("$1")
}

record_fail() {
  failed_stages+=("$1")
}

record_skip() {
  skipped_stages+=("$1")
}

run_stage_collect() {
  local stage="$1"
  shift
  echo
  echo "[REMOTE-VERIFY] ${stage}"
  if "$@"; then
    record_pass "${stage}"
  else
    local exit_code=$?
    echo "[REMOTE-VERIFY] failed: ${stage} (exit ${exit_code})" >&2
    record_fail "${stage} (exit ${exit_code})"
  fi
}

print_summary() {
  echo
  echo "[REMOTE-VERIFY] summary"
  echo "[REMOTE-VERIFY] passed stages: ${#passed_stages[@]}"
  for stage in "${passed_stages[@]}"; do
    echo "[REMOTE-VERIFY]   PASS ${stage}"
  done

  echo "[REMOTE-VERIFY] failed stages: ${#failed_stages[@]}"
  for stage in "${failed_stages[@]}"; do
    echo "[REMOTE-VERIFY]   FAIL ${stage}"
  done

  echo "[REMOTE-VERIFY] skipped stages: ${#skipped_stages[@]}"
  for stage in "${skipped_stages[@]}"; do
    echo "[REMOTE-VERIFY]   SKIP ${stage}"
  done
}

if [[ ! -d "${APP_DIR}" ]]; then
  echo "[REMOTE-VERIFY] app directory not found: ${APP_DIR}" >&2
  record_fail "remote preflight: app directory not found: ${APP_DIR}"
  print_summary
  exit 1
fi

if ! cd "${APP_DIR}"; then
  echo "[REMOTE-VERIFY] failed to enter app directory: ${APP_DIR}" >&2
  record_fail "remote preflight: failed to enter app directory: ${APP_DIR}"
  print_summary
  exit 1
fi

run_stage_collect "git fetch origin --prune" git fetch origin --prune
run_stage_collect "git switch main" git switch main
run_stage_collect "git pull --ff-only origin main" git pull --ff-only origin main
run_stage_collect "git status" git status
run_stage_collect "git log --oneline -n 3" git log --oneline -n 3
run_stage_collect "mkdir -p ${PYTEST_TMPDIR}" mkdir -p "${PYTEST_TMPDIR}"
run_stage_collect "clear ${PYTEST_TMPDIR}" bash -lc "find \"${PYTEST_TMPDIR}\" -mindepth 1 -maxdepth 1 -exec rm -rf {} +"
run_stage_collect "TMPDIR=${PYTEST_TMPDIR} uv sync --locked" env TMPDIR="${PYTEST_TMPDIR}" uv sync --locked
run_stage_collect "TMPDIR=${PYTEST_TMPDIR} uv run pytest -q" env TMPDIR="${PYTEST_TMPDIR}" uv run pytest -q

print_summary

if [[ "${#failed_stages[@]}" -eq 0 ]]; then
  echo
  echo "[REMOTE-VERIFY] success"
  exit 0
fi

echo
echo "[REMOTE-VERIFY] completed with failed stages" >&2
exit 1
