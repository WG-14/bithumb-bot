#!/usr/bin/env bash
set -euo pipefail

# Local operator pipeline:
# 1. read scripts/codex_request.txt
# 2. run Codex against this repository
# 3. commit and push Codex changes
# 4. run EC2 verification with live.verify.env
# 5. notify the final EC2 verification result through ntfy

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

REQUEST_FILE="${CODEX_REQUEST_FILE:-${SCRIPT_DIR}/codex_request.txt}"
REMOTE_VERIFY_SCRIPT="${REMOTE_VERIFY_SCRIPT:-${SCRIPT_DIR}/remote_verify_live.sh}"
NOTIFY_SCRIPT="${NOTIFY_SCRIPT:-${SCRIPT_DIR}/notify_ntfy.sh}"
CODEX_BIN="${CODEX_BIN:-codex}"
SSH_KEY="${BITHUMB_EC2_SSH_KEY:-${HOME}/.ssh/bithumb-bot-paper.pem}"
EC2_TARGET="${BITHUMB_EC2_TARGET:-ec2-user@3.39.93.137}"
REMOTE_VERIFY_MODE="${REMOTE_VERIFY_MODE:-smoke}"

stage="preflight"

notify() {
  local title="$1"
  local priority="$2"
  local message="$3"

  if [[ -x "${NOTIFY_SCRIPT}" && -n "${NTFY_TOPIC:-}" ]]; then
    "${NOTIFY_SCRIPT}" "${title}" "${priority}" "${message}" || true
  else
    echo "[PIPELINE] ntfy notification skipped; set NTFY_TOPIC and ensure ${NOTIFY_SCRIPT} is executable" >&2
  fi
}

fail() {
  local message="$1"
  echo "[PIPELINE] ${message}" >&2
  notify "bithumb-bot pipeline failed" "high" "${message}"
  exit 1
}

on_error() {
  local exit_code=$?
  trap - ERR
  local message="bithumb-bot Codex pipeline failed during stage: ${stage}"
  echo "[PIPELINE] ${message}" >&2
  notify "bithumb-bot pipeline failed" "high" "${message}"
  exit "$exit_code"
}
trap on_error ERR

run_stage() {
  stage="$1"
  shift
  echo
  echo "[PIPELINE] ${stage}"
  "$@"
}

git_status_porcelain() {
  git status --porcelain=v1 --untracked-files=all
}

dirty_paths_except_request() {
  local request_rel="$1"
  git_status_porcelain | while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    local path="${line:3}"
    if [[ "${path}" == *" -> "* ]]; then
      path="${path##* -> }"
    fi
    if [[ "${path}" != "${request_rel}" ]]; then
      printf '%s\n' "${line}"
    fi
  done
}

dirty_paths_excluding_request_file() {
  local request_rel="$1"
  dirty_paths_except_request "${request_rel}"
}

cd "${PROJECT_ROOT}"

case "${REMOTE_VERIFY_MODE}" in
  smoke|full)
    ;;
  *)
    fail "invalid REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE}; expected smoke or full"
    ;;
esac

if [[ ! -f "${REQUEST_FILE}" ]]; then
  fail "request file not found: ${REQUEST_FILE}"
fi

if [[ ! -s "${REQUEST_FILE}" ]]; then
  fail "request file is empty: ${REQUEST_FILE}"
fi

if [[ ! -x "${REMOTE_VERIFY_SCRIPT}" ]]; then
  fail "remote verify script is not executable: ${REMOTE_VERIFY_SCRIPT}"
fi

if [[ ! -x "${NOTIFY_SCRIPT}" ]]; then
  fail "ntfy helper is not executable: ${NOTIFY_SCRIPT}"
fi

if [[ -z "${NTFY_TOPIC:-}" ]]; then
  fail "NTFY_TOPIC is required for success and failure notifications"
fi

if ! command -v "${CODEX_BIN}" >/dev/null 2>&1; then
  fail "Codex binary not found: ${CODEX_BIN}"
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  fail "SSH key not found: ${SSH_KEY}"
fi

request_rel="$(realpath --relative-to="${PROJECT_ROOT}" "${REQUEST_FILE}")"
pre_existing_non_request="$(dirty_paths_except_request "${request_rel}")"

if [[ -n "${pre_existing_non_request}" ]]; then
  echo "[PIPELINE] refusing to run with pre-existing non-request changes:" >&2
  printf '%s\n' "${pre_existing_non_request}" >&2
  fail "refusing to run with pre-existing non-request changes"
fi

run_stage "run Codex request from ${request_rel}" "${CODEX_BIN}" exec --full-auto --cd "${PROJECT_ROOT}" - < "${REQUEST_FILE}"

post_codex_non_request="$(dirty_paths_excluding_request_file "${request_rel}")"
if [[ -z "${post_codex_non_request}" ]]; then
  stage="check Codex modifications"
  echo "[PIPELINE] Codex completed but did not modify any file other than the request file." >&2
  notify "bithumb-bot pipeline failed" "high" \
    "Codex did not modify any file other than ${request_rel}; no commit was created."
  exit 1
fi

run_stage "git status" git status
run_stage "check repo runtime artifacts" ./scripts/check_repo_runtime_artifacts.sh
run_stage "git add ." git add .
run_stage "git commit -m apply" git commit -m "apply"
run_stage "git push" git push

stage="remote EC2 verification"
echo
echo "[PIPELINE] ${stage} (REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE})"
if ssh \
    -i "${SSH_KEY}" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    "${EC2_TARGET}" \
    "REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE} bash -s" < "${REMOTE_VERIFY_SCRIPT}"; then
  remote_verify_exit=0
else
  remote_verify_exit=$?
fi

stage="complete"
if [[ "${remote_verify_exit}" -eq 0 ]]; then
  notify "bithumb-bot pipeline succeeded" "default" \
    "Codex changes were committed, pushed, and verified on EC2 with REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE}."
  echo
  echo "[PIPELINE] success"
  exit 0
fi

notify "bithumb-bot pipeline failed" "high" \
  "Codex changes were committed and pushed, but EC2 verification completed with one or more failed stages in REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE}."
echo
echo "[PIPELINE] EC2 verification completed with failed stages" >&2
exit "${remote_verify_exit}"
