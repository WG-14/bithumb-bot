#!/usr/bin/env bash
set -euo pipefail

# EC2-side verification for the deployed main branch.
# This script assumes runtime/live data is configured by the external live verify
# env file and does not create repository-local runtime artifacts.

APP_DIR="${BITHUMB_REMOTE_APP_DIR:-${HOME}/apps/bithumb-bot}"
LIVE_VERIFY_ENV="${BITHUMB_LIVE_VERIFY_ENV:-/home/ec2-user/bithumb-runtime/env/live.verify.env}"

stage="remote preflight"

on_error() {
  local exit_code=$?
  echo "[REMOTE-VERIFY] failed during stage: ${stage}" >&2
  exit "$exit_code"
}
trap on_error ERR

run_stage() {
  stage="$1"
  shift
  echo
  echo "[REMOTE-VERIFY] ${stage}"
  "$@"
}

if [[ ! -d "${APP_DIR}" ]]; then
  echo "[REMOTE-VERIFY] app directory not found: ${APP_DIR}" >&2
  exit 1
fi

cd "${APP_DIR}"

run_stage "git fetch origin --prune" git fetch origin --prune
run_stage "git switch main" git switch main
run_stage "git pull --ff-only origin main" git pull --ff-only origin main
run_stage "git status" git status
run_stage "git log --oneline -n 3" git log --oneline -n 3
run_stage "uv sync --locked" uv sync --locked
run_stage "uv run pytest -q" uv run pytest -q

stage="load live verify environment"
echo
echo "[REMOTE-VERIFY] ${stage}"
if [[ ! -f "${LIVE_VERIFY_ENV}" ]]; then
  echo "[REMOTE-VERIFY] live verify env file not found: ${LIVE_VERIFY_ENV}" >&2
  exit 1
fi

cd "${APP_DIR}"
set -a
# shellcheck disable=SC1090
source "${LIVE_VERIFY_ENV}"
set +a
export ENV_ROOT=/home/ec2-user/bithumb-runtime/env
export MODE=live

run_live_command() {
  local name="$1"
  shift
  run_stage "MODE=live uv run bithumb-bot ${name}" env MODE=live uv run bithumb-bot "$@"
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

stage="complete"
echo
echo "[REMOTE-VERIFY] success"
