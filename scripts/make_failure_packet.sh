#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
WORK_DIR="${CODEX_PYTEST_WORK_DIR:-${TMPDIR:-/tmp}/bithumb-bot-codex-pytest}"
ITERATION="${CODEX_PYTEST_ITERATION:-manual}"
LATEST_LOG_FILE="${WORK_DIR}/latest_full_suite_log"

cd "${PROJECT_ROOT}"

if [[ $# -gt 1 ]]; then
  echo "[FAILURE-PACKET] usage: $0 [full-suite-log-path]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  log_file="$1"
else
  if [[ ! -f "${LATEST_LOG_FILE}" ]]; then
    echo "[FAILURE-PACKET] latest log pointer is missing: ${LATEST_LOG_FILE}" >&2
    exit 1
  fi
  log_file="$(<"${LATEST_LOG_FILE}")"
fi

if [[ ! -f "${log_file}" ]]; then
  echo "[FAILURE-PACKET] full-suite log is missing: ${log_file}" >&2
  exit 1
fi

timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
packet_dir="${WORK_DIR}/packets/${timestamp}_iter${ITERATION}"
mkdir -p "${packet_dir}"

echo "[FAILURE-PACKET] creating packet in ${packet_dir}" >&2
cp "${log_file}" "${packet_dir}/full_suite.log"

failed_tests_file="${packet_dir}/failed_tests.txt"
preflight_file="${packet_dir}/preflight_failure.txt"
collection_file="${packet_dir}/first_collection_import_config_error.txt"
artifact_file="${packet_dir}/runtime_artifact_failure.txt"
workspace_file="${packet_dir}/pytest_workspace_summary.txt"

grep -E '^(FAILED|ERROR) tests/[^[:space:]]+' "${log_file}" \
  | awk '{print $2}' \
  | sed 's/[[:space:]].*$//' \
  | sort -u > "${failed_tests_file}" || true

grep -E '\[PYTEST-PREFLIGHT\] failed|\[PYTEST-PREFLIGHT\] pytest did not start|preflight_failure\.json' \
  "${log_file}" > "${preflight_file}" || true

awk '
  /ERROR collecting|ImportError|ModuleNotFoundError|ConftestImportFailure|ConfigError|INTERNALERROR|pytest UsageError/ {
    if (!seen) {
      seen=1
      remaining=80
    }
  }
  seen && remaining > 0 {
    print
    remaining--
  }
' "${log_file}" > "${collection_file}"

awk '
  /\[RUNTIME-ARTIFACT-CHECK\] repo-local runtime\/test artifacts detected/ {
    seen=1
    remaining=80
  }
  seen && remaining > 0 {
    print
    remaining--
  }
' "${log_file}" > "${artifact_file}"

grep -E '\[PYTEST-WORKSPACE\]|retained_size_bytes|large_file_bytes|keeping workspace' \
  "${log_file}" > "${workspace_file}" || true

git status --porcelain=v1 --untracked-files=all > "${packet_dir}/git_status.txt"
git diff --stat > "${packet_dir}/git_diff_stat.txt"
git diff --binary > "${packet_dir}/git_diff.patch"

{
  echo "# Repro Commands"
  echo
  echo "Wrapper-only full-suite validation command. Codex must not run this command:"
  echo
  echo '```bash'
  echo 'PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=loadfile ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh'
  echo '```'
  echo
  echo "Focused examples derived from the failure packet:"
  echo
  if [[ -s "${failed_tests_file}" ]]; then
    while IFS= read -r selector; do
      [[ -z "${selector}" ]] && continue
      test_file="${selector%%::*}"
      echo '```bash'
      if [[ "${selector}" == *"::"* ]]; then
        echo "uv run pytest ${selector} -q"
      else
        echo "uv run pytest ${test_file} -q"
      fi
      echo '```'
    done < "${failed_tests_file}"
  else
    echo "No focused pytest selectors were extracted. Inspect the collection, preflight, and artifact excerpts before choosing any focused command."
  fi
} > "${packet_dir}/repro_commands.txt"

cat > "${packet_dir}/constraints.md" <<'EOF'
# Constraints

- Preserve existing system purpose, operational intent, and repository safety contracts.
- Do not weaken fail-close behavior, live safety guards, recovery correctness, state integrity, accounting correctness, path/storage contracts, exposure authority, reconciliation, or operator-facing reporting.
- Do not add pytest skip, skipif, xfail, or loosened assertions to force a pass.
- Do not add unrealistic mocks to hide production behavior.
- Do not run full-suite validation.
- Do not run selector-less pytest.
- Do not run broad `tests` or `tests/` pytest targets.
- Run only focused pytest commands derived from the failure packet.
- If a test expectation conflicts with a repository safety contract, stop and report the conflict instead of weakening production behavior.
EOF

signature_input="${packet_dir}/signature_input.txt"
cat \
  "${failed_tests_file}" \
  "${preflight_file}" \
  "${collection_file}" \
  "${artifact_file}" \
  > "${signature_input}"
sha256sum "${signature_input}" | awk '{print $1}' > "${packet_dir}/failure_signature.sha256"
rm -f "${signature_input}"

codex_input="${packet_dir}/codex_input.md"
{
  echo "# WSL-Owned Pytest Repair Packet"
  echo
  echo "Codex is the repair agent, not the validation authority."
  echo "The WSL wrapper is the only authority allowed to run the full-suite command."
  echo "Use the failure packet as baseline evidence."
  echo "Run only focused pytest commands derived from the packet."
  echo "Do not run the full suite."
  echo
  echo "The wrapper-owned success command is:"
  echo
  echo '```bash'
  echo 'PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=loadfile ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh'
  echo '```'
  echo
  echo "Codex must not run that command, \`./scripts/run_full_pytest_tests.sh\`, \`./scripts/check_repo_runtime_artifacts.sh\`, selector-less pytest, broad \`tests\` pytest targets, or raw \`uv run pytest -q\`."
  echo
  echo "## Repository Repair Prompt"
  echo
  if [[ -s "${SCRIPT_DIR}/codex_pytest_repair_prompt.md" ]]; then
    sed \
      -e 's/Run the full suite first:/Do not run the full suite. The WSL wrapper already ran it and attached failure evidence below. Original prompt line retained only as historical context:/' \
      -e 's/uv run pytest -q/WRAPPER-OWNED-FULL-SUITE/g' \
      "${SCRIPT_DIR}/codex_pytest_repair_prompt.md"
  else
    echo "scripts/codex_pytest_repair_prompt.md is missing or empty."
  fi
  echo
  echo "## Packet Metadata"
  echo
  echo "- project_root: ${PROJECT_ROOT}"
  echo "- packet_dir: ${packet_dir}"
  echo "- full_suite_log: ${log_file}"
  echo "- iteration: ${ITERATION}"
  echo "- failure_signature: $(<"${packet_dir}/failure_signature.sha256")"
  echo
  echo "## Failed Tests"
  echo
  if [[ -s "${failed_tests_file}" ]]; then
    sed 's/^/- /' "${failed_tests_file}"
  else
    echo "No FAILED/ERROR test selectors were extracted."
  fi
  echo
  echo "## Preflight Failure Excerpt"
  echo '```text'
  sed -n '1,120p' "${preflight_file}"
  echo '```'
  echo
  echo "## First Collection Import Config Error Excerpt"
  echo '```text'
  sed -n '1,120p' "${collection_file}"
  echo '```'
  echo
  echo "## Runtime Artifact Failure Excerpt"
  echo '```text'
  sed -n '1,120p' "${artifact_file}"
  echo '```'
  echo
  echo "## Pytest Workspace Summary"
  echo '```text'
  sed -n '1,160p' "${workspace_file}"
  echo '```'
  echo
  echo "## Git Diff Stat"
  echo '```text'
  sed -n '1,160p' "${packet_dir}/git_diff_stat.txt"
  echo '```'
  echo
  echo "## Repro Commands"
  echo '```text'
  sed -n '1,220p' "${packet_dir}/repro_commands.txt"
  echo '```'
  echo
  echo "## Recent Full-Suite Log Tail"
  echo '```text'
  tail -n 220 "${log_file}"
  echo '```'
  echo
  echo "## Required Behavior"
  echo
  cat "${packet_dir}/constraints.md"
} > "${codex_input}"

echo "[FAILURE-PACKET] wrote ${codex_input}" >&2
printf '%s\n' "${codex_input}"
