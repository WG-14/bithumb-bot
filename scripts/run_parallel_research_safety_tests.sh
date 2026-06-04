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

bithumb_pytest_mark_pytest_started
if [[ -n "${PYTEST_BIN:-}" ]]; then
  pytest_cmd=("$PYTEST_BIN")
else
  pytest_cmd=(uv run pytest)
fi
"${pytest_cmd[@]}" -q -n "${PYTEST_XDIST_WORKERS:-2}" --dist="${PYTEST_XDIST_DIST:-loadfile}" \
  tests/test_research_process_runtime.py \
  tests/test_research_backtest_reproducibility.py::test_parallel_research_failure_is_committed_by_main_process \
  tests/test_research_backtest_reproducibility.py::test_parallel_executor_maps_future_level_exception_to_failed_work_result \
  -W error::DeprecationWarning

./scripts/check_repo_runtime_artifacts.sh
