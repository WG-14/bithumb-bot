#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/scripts/lib/pytest_workspace.sh"

bithumb_pytest_setup_workspace "parallel-research-safety"
export BITHUMB_PYTEST_SUMMARY_ON_SUCCESS=1
status=0
trap 'status=$?; bithumb_pytest_cleanup_workspace "$status"; exit "$status"' EXIT

export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"

PARALLEL_RESEARCH_SAFETY_MARKER_EXPR="parallel_e2e or memory_sensitive"

bithumb_pytest_sanitize_unsafe_env "parallel research safety pytest runner"

bithumb_pytest_run_preflight "research test policy" uv run python scripts/check_research_test_policy.py
bithumb_pytest_mark_pytest_started
if [[ -n "${PYTEST_BIN:-}" ]]; then
  pytest_cmd=("$PYTEST_BIN")
else
  pytest_cmd=(uv run pytest)
fi
"${pytest_cmd[@]}" -q -n "${PYTEST_XDIST_WORKERS:-2}" --dist="${PYTEST_XDIST_DIST:-loadfile}" \
  tests/test_research_process_runtime.py \
  -W error::DeprecationWarning
"${pytest_cmd[@]}" -q -n "${PYTEST_XDIST_WORKERS:-2}" --dist="${PYTEST_XDIST_DIST:-loadfile}" \
  -m "$PARALLEL_RESEARCH_SAFETY_MARKER_EXPR" \
  -W error::DeprecationWarning

./scripts/check_repo_runtime_artifacts.sh
