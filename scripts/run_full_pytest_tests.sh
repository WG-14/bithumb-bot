#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/scripts/lib/pytest_workspace.sh"

bithumb_pytest_setup_workspace "full"
export BITHUMB_PYTEST_SUMMARY_ON_SUCCESS=1
status=0
trap 'status=$?; bithumb_pytest_cleanup_workspace "$status"; exit "$status"' EXIT

export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"

bithumb_pytest_run_preflight "research test policy" uv run python scripts/check_research_test_policy.py
bithumb_pytest_run_preflight "strategy PR workload guard" uv run python scripts/check_strategy_pr_workload_guard.py
bithumb_pytest_run_preflight "research workload budget full" uv run python scripts/check_research_workload_budget.py --suite full
bithumb_pytest_mark_pytest_started
uv run pytest -q
