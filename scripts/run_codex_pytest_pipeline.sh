#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

REQUEST_FILE="${CODEX_PYTEST_REQUEST_FILE:-${SCRIPT_DIR}/codex_pytest_repair_prompt.md}"
FULL_SUITE_SCRIPT="${FULL_SUITE_SCRIPT:-${SCRIPT_DIR}/full_suite.sh}"
PACKET_SCRIPT="${PACKET_SCRIPT:-${SCRIPT_DIR}/make_failure_packet.sh}"
REMOTE_VERIFY_SCRIPT="${REMOTE_VERIFY_SCRIPT:-${SCRIPT_DIR}/remote_verify_live.sh}"
NOTIFY_SCRIPT="${NOTIFY_SCRIPT:-${SCRIPT_DIR}/notify_ntfy.sh}"
ARTIFACT_CHECK_SCRIPT="${ARTIFACT_CHECK_SCRIPT:-${SCRIPT_DIR}/check_repo_runtime_artifacts.sh}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_PYTEST_MAX_ITERATIONS="${CODEX_PYTEST_MAX_ITERATIONS:-3}"
CODEX_PYTEST_WORK_DIR="${CODEX_PYTEST_WORK_DIR:-${TMPDIR:-/tmp}/bithumb-bot-codex-pytest}"
CODEX_PYTEST_COMMIT_PUSH="${CODEX_PYTEST_COMMIT_PUSH:-1}"
CODEX_PYTEST_REMOTE_VERIFY="${CODEX_PYTEST_REMOTE_VERIFY:-1}"
REMOTE_VERIFY_MODE="${REMOTE_VERIFY_MODE:-smoke}"
CODEX_PYTEST_ALLOW_DIRTY="${CODEX_PYTEST_ALLOW_DIRTY:-0}"
CODEX_PYTEST_STRICT_MOCK_GUARD="${CODEX_PYTEST_STRICT_MOCK_GUARD:-0}"
SSH_KEY="${BITHUMB_EC2_SSH_KEY:-${HOME}/.ssh/bithumb-bot-paper.pem}"
EC2_TARGET="${BITHUMB_EC2_TARGET:-ec2-user@3.39.93.137}"

stage="preflight"
guard_dir=""
last_signature=""

notify() {
  local title="$1"
  local priority="$2"
  local message="$3"

  if [[ -x "${NOTIFY_SCRIPT}" && -n "${NTFY_TOPIC:-}" ]]; then
    "${NOTIFY_SCRIPT}" "${title}" "${priority}" "${message}" || true
  else
    echo "[PYTEST-PIPELINE] ntfy notification skipped; set NTFY_TOPIC and ensure ${NOTIFY_SCRIPT} is executable" >&2
  fi
}

fail() {
  local message="$1"
  echo "[PYTEST-PIPELINE] ${message}" >&2
  notify "bithumb-bot pytest pipeline failed" "high" "${message}"
  exit 1
}

run_stage() {
  stage="$1"
  shift
  echo
  echo "[PYTEST-PIPELINE] ${stage}"
  "$@"
}

git_status_porcelain() {
  git status --porcelain=v1 --untracked-files=all
}

git_worktree_fingerprint() {
  {
    git status --porcelain=v1 --untracked-files=all
    git diff --binary --no-ext-diff
    git diff --cached --binary --no-ext-diff
  } | sha256sum | awk '{print $1}'
}

cleanup_codex_pytest_guard() {
  if [[ -n "${guard_dir}" && -d "${guard_dir}" ]]; then
    rm -rf "${guard_dir}"
  fi
  guard_dir=""
}

on_error() {
  local exit_code=$?
  trap - ERR
  cleanup_codex_pytest_guard
  local message="bithumb-bot Codex pytest pipeline failed during stage: ${stage}"
  echo "[PYTEST-PIPELINE] ${message}" >&2
  notify "bithumb-bot pytest pipeline failed" "high" "${message}"
  exit "${exit_code}"
}
trap on_error ERR
trap cleanup_codex_pytest_guard EXIT

install_codex_pytest_guard() {
  local real_uv
  real_uv="$(command -v uv || true)"
  if [[ -z "${real_uv}" ]]; then
    fail "uv binary not found; required for Codex focused pytest guard"
  fi

  guard_dir="$(mktemp -d "${CODEX_PYTEST_WORK_DIR%/}/guard.XXXXXX")"
  cat > "${guard_dir}/uv" <<'GUARD'
#!/usr/bin/env bash
set -euo pipefail

real_uv="${CODEX_PYTEST_REAL_UV:?}"

guard_error() {
  echo "[CODEX-PYTEST-GUARD] Pytest Repair Mode blocks selector-less full pytest." >&2
  echo "[CODEX-PYTEST-GUARD] Full-suite validation belongs to the WSL wrapper." >&2
  echo "[CODEX-PYTEST-GUARD] Do not run ./scripts/run_full_pytest_tests.sh inside Codex." >&2
  echo "[CODEX-PYTEST-GUARD] $1" >&2
  exit 125
}

is_pytest_path_selector() {
  local arg="$1"
  [[ "${arg}" == tests/*.py || "${arg}" == tests/*/*.py || "${arg}" == tests/*.py::* || "${arg}" == tests/*/*.py::* ]]
}

validate_pytest_args() {
  local has_selector=0
  local has_expression=0
  local path_selector_count=0
  local arg

  for arg in "$@"; do
    case "${arg}" in
      -k|-m)
        has_expression=1
        ;;
      tests|tests/)
        guard_error "Broad tests target is blocked; use focused selectors from the failure packet."
        ;;
      -*)
        ;;
      *)
        if is_pytest_path_selector "${arg}"; then
          has_selector=1
          path_selector_count=$((path_selector_count + 1))
        elif [[ "${arg}" == tests/* ]]; then
          guard_error "Only focused test files or test function selectors are allowed."
        fi
        ;;
    esac
  done

  if [[ "${has_selector}" -eq 0 && "${has_expression}" -eq 0 ]]; then
    guard_error "Selector-less pytest is blocked."
  fi

  if [[ "${path_selector_count}" -gt 1 && "${has_expression}" -eq 0 ]]; then
    guard_error "Multiple pytest path selectors require a focused -k or -m expression."
  fi
}

if [[ "${1:-}" == "run" ]]; then
  shift
  if [[ "${1:-}" == "pytest" ]]; then
    shift
    validate_pytest_args "$@"
    exec "${real_uv}" run pytest "$@"
  fi
  if [[ "${1:-}" == "python" && "${2:-}" == "-m" && "${3:-}" == "pytest" ]]; then
    shift 3
    validate_pytest_args "$@"
    exec "${real_uv}" run python -m pytest "$@"
  fi
  if [[ "${1:-}" == "python" && ( "${2:-}" == "./scripts/run_full_pytest_tests.sh" || "${2:-}" == "scripts/run_full_pytest_tests.sh" ) ]]; then
    guard_error "Full pytest runner is blocked inside Codex."
  fi
  exec "${real_uv}" run "$@"
fi

exec "${real_uv}" "$@"
GUARD
  chmod +x "${guard_dir}/uv"
  export CODEX_PYTEST_REAL_UV="${real_uv}"
  export PATH="${guard_dir}:${PATH}"
}

run_codex_pytest_repair_with_guard() {
  local codex_input_file="$1"
  cleanup_codex_pytest_guard
  install_codex_pytest_guard
  run_stage "run Codex focused pytest repair" \
    "${CODEX_BIN}" exec --full-auto --cd "${PROJECT_ROOT}" - < "${codex_input_file}"
  cleanup_codex_pytest_guard
}

detect_forbidden_repair_patterns() {
  local forbidden_file strict_file
  forbidden_file="${CODEX_PYTEST_WORK_DIR}/forbidden_repair_patterns.txt"
  strict_file="${CODEX_PYTEST_WORK_DIR}/strict_mock_guard_patterns.txt"
  mkdir -p "${CODEX_PYTEST_WORK_DIR}"

  git diff -U0 -- '*.py' ':(exclude).git' \
    | grep -E '^\+.*(pytest\.mark\.skip|pytest\.mark\.skipif|pytest\.mark\.xfail|@unittest\.skip|@unittest\.skipIf|@unittest\.skipUnless|xfail)' \
    > "${forbidden_file}" || true

  if [[ -s "${forbidden_file}" ]]; then
    echo "[PYTEST-PIPELINE] forbidden skip/xfail additions:" >&2
    cat "${forbidden_file}" >&2
    fail "Codex added forbidden skip/xfail-style bypass patterns"
  fi

  if [[ "${CODEX_PYTEST_STRICT_MOCK_GUARD}" == "1" ]]; then
    git diff -U0 -- '*.py' ':(exclude).git' \
      | grep -E '^\+.*(MagicMock|Mock\(|patch\(|monkeypatch\.setattr)' \
      > "${strict_file}" || true
    if [[ -s "${strict_file}" ]]; then
      echo "[PYTEST-PIPELINE] strict mock guard additions:" >&2
      cat "${strict_file}" >&2
      fail "Codex added mock/patch patterns while CODEX_PYTEST_STRICT_MOCK_GUARD=1"
    fi
  fi
}

complete_success() {
  run_stage "final repo runtime artifact guard" "${ARTIFACT_CHECK_SCRIPT}"

  if [[ -n "$(git_status_porcelain)" && "${CODEX_PYTEST_COMMIT_PUSH}" == "1" ]]; then
    run_stage "git add ." git add .
    run_stage "git commit -m pytest-repair" git commit -m "pytest-repair"
    run_stage "git push" git push

    if [[ "${CODEX_PYTEST_REMOTE_VERIFY}" == "1" ]]; then
      if [[ "${REMOTE_VERIFY_MODE}" == "full" ]]; then
        fail "REMOTE_VERIFY_MODE=full is out of scope for this pytest repair pipeline; use smoke or run remote full verification separately"
      fi
      if [[ ! -x "${REMOTE_VERIFY_SCRIPT}" ]]; then
        fail "remote verify script is not executable: ${REMOTE_VERIFY_SCRIPT}"
      fi
      if [[ ! -f "${SSH_KEY}" ]]; then
        fail "SSH key not found: ${SSH_KEY}"
      fi

      stage="EC2 smoke verification"
      echo
      echo "[PYTEST-PIPELINE] ${stage} (REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE})"
      if ! ssh \
          -i "${SSH_KEY}" \
          -o BatchMode=yes \
          -o StrictHostKeyChecking=accept-new \
          "${EC2_TARGET}" \
          "REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE} bash -s" < "${REMOTE_VERIFY_SCRIPT}"; then
        fail "EC2 smoke verification failed with REMOTE_VERIFY_MODE=${REMOTE_VERIFY_MODE}"
      fi
    fi

    notify "bithumb-bot pytest pipeline succeeded" "default" \
      "WSL wrapper full-suite validation passed, changes were committed and pushed, and remote verification mode was ${REMOTE_VERIFY_MODE}."
  else
    notify "bithumb-bot pytest pipeline succeeded" "default" \
      "WSL wrapper full-suite validation passed. Commit/push was skipped or no repository changes existed."
  fi

  echo
  echo "[PYTEST-PIPELINE] success"
}

cd "${PROJECT_ROOT}"
mkdir -p "${CODEX_PYTEST_WORK_DIR}"

if [[ ! -s "${REQUEST_FILE}" ]]; then
  fail "pytest repair prompt file is missing or empty: ${REQUEST_FILE}"
fi
for required_script in "${FULL_SUITE_SCRIPT}" "${PACKET_SCRIPT}" "${ARTIFACT_CHECK_SCRIPT}" "${NOTIFY_SCRIPT}"; do
  if [[ ! -x "${required_script}" ]]; then
    fail "required script is not executable: ${required_script}"
  fi
done
if [[ -z "${NTFY_TOPIC:-}" ]]; then
  fail "NTFY_TOPIC is required for success and failure notifications"
fi
if ! command -v "${CODEX_BIN}" >/dev/null 2>&1; then
  fail "Codex binary not found: ${CODEX_BIN}"
fi
if [[ "${CODEX_PYTEST_COMMIT_PUSH}" == "1" && "${CODEX_PYTEST_REMOTE_VERIFY}" == "1" ]]; then
  if [[ "${REMOTE_VERIFY_MODE}" == "full" ]]; then
    fail "REMOTE_VERIFY_MODE=full is out of scope for this pytest repair pipeline; default smoke verification is required"
  fi
  if [[ ! -x "${REMOTE_VERIFY_SCRIPT}" ]]; then
    fail "remote verify script is not executable: ${REMOTE_VERIFY_SCRIPT}"
  fi
  if [[ ! -f "${SSH_KEY}" ]]; then
    fail "SSH key not found: ${SSH_KEY}"
  fi
fi
if [[ "${CODEX_PYTEST_ALLOW_DIRTY}" != "1" && -n "$(git_status_porcelain)" ]]; then
  echo "[PYTEST-PIPELINE] refusing to run with pre-existing repository changes:" >&2
  git_status_porcelain >&2
  fail "refusing to run with pre-existing repository changes"
fi

iteration=1
while (( iteration <= CODEX_PYTEST_MAX_ITERATIONS )); do
  export CODEX_PYTEST_ITERATION="${iteration}"
  echo
  echo "[PYTEST-PIPELINE] iteration ${iteration}/${CODEX_PYTEST_MAX_ITERATIONS}"

  stage="WSL wrapper full-suite validation"
  if "${FULL_SUITE_SCRIPT}"; then
    complete_success
    exit 0
  fi

  codex_input_file="$("${PACKET_SCRIPT}")"
  packet_dir="$(dirname -- "${codex_input_file}")"
  signature_file="${packet_dir}/failure_signature.sha256"
  signature="$(<"${signature_file}")"

  if [[ -n "${last_signature}" && "${signature}" == "${last_signature}" ]]; then
    fail "same failure signature repeated twice: ${signature}"
  fi
  last_signature="${signature}"

  fingerprint_before="$(git_worktree_fingerprint)"
  run_codex_pytest_repair_with_guard "${codex_input_file}"
  run_stage "post-Codex repo runtime artifact guard" "${ARTIFACT_CHECK_SCRIPT}"
  detect_forbidden_repair_patterns
  fingerprint_after="$(git_worktree_fingerprint)"

  if [[ "${fingerprint_after}" == "${fingerprint_before}" ]]; then
    fail "Codex returned without changing repository diff"
  fi

  iteration=$((iteration + 1))
done

fail "maximum Codex pytest repair iterations reached: ${CODEX_PYTEST_MAX_ITERATIONS}"
