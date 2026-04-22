#!/usr/bin/env bash
set -uo pipefail

# EC2-side verification for the deployed main branch.
# This script assumes runtime/live data is configured by the external live verify
# env file and does not create repository-local runtime artifacts.

APP_DIR="${BITHUMB_REMOTE_APP_DIR:-${HOME}/apps/bithumb-bot}"
LIVE_VERIFY_ENV="${BITHUMB_LIVE_VERIFY_ENV:-/home/ec2-user/bithumb-runtime/env/live.verify.env}"
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

echo
echo "[REMOTE-VERIFY] load ${LIVE_VERIFY_ENV}"
live_env_loaded=false
if [[ ! -f "${LIVE_VERIFY_ENV}" ]]; then
  echo "[REMOTE-VERIFY] live verify env file not found: ${LIVE_VERIFY_ENV}" >&2
  record_fail "load ${LIVE_VERIFY_ENV}"
else
  set -a
  set +u
  # shellcheck disable=SC1090
  source "${LIVE_VERIFY_ENV}"
  source_exit=$?
  set -u
  set +a
  if [[ "${source_exit}" -eq 0 ]]; then
    record_pass "load ${LIVE_VERIFY_ENV}"
    live_env_loaded=true
  else
    echo "[REMOTE-VERIFY] failed to load live verify env: ${LIVE_VERIFY_ENV} (exit ${source_exit})" >&2
    record_fail "load ${LIVE_VERIFY_ENV} (exit ${source_exit})"
  fi
fi

run_stage_collect "export ENV_ROOT=/home/ec2-user/bithumb-runtime/env" export ENV_ROOT=/home/ec2-user/bithumb-runtime/env
run_stage_collect "export MODE=live" export MODE=live

run_live_command() {
  local name="$1"
  shift
  if [[ "${live_env_loaded}" == "true" ]]; then
    run_stage_collect "MODE=live uv run bithumb-bot ${name}" env MODE=live uv run bithumb-bot "$@"
  else
    echo
    echo "[REMOTE-VERIFY] skipping MODE=live uv run bithumb-bot ${name}; live verify env did not load"
    record_skip "MODE=live uv run bithumb-bot ${name}"
  fi
}

run_live_command "broker-diagnose" broker-diagnose
run_live_command "audit" audit
run_live_command "audit-ledger" audit-ledger
run_live_command "health" health
run_live_command "recovery-report" recovery-report
run_live_command "reconcile" reconcile
run_live_command "recovery-report" recovery-report
run_live_command "restart-checklist" restart-checklist
run_live_command "ops-report --limit 50" ops-report --limit 50

print_summary

if [[ "${#failed_stages[@]}" -eq 0 ]]; then
  echo
  echo "[REMOTE-VERIFY] success"
  exit 0
fi

echo
echo "[REMOTE-VERIFY] completed with failed stages" >&2
exit 1
