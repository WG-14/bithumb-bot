#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/scripts/lib/pytest_workspace.sh"

RESEARCH_NIGHTLY_MARKER_EXPR="research_kernel or research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e or nightly or slow_research or memory_sensitive"
duration_log="$(mktemp "${TMPDIR:-/tmp}/bithumb-research-nightly-durations.XXXXXX.log")"
export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"

bithumb_pytest_setup_workspace "research-nightly"
status=0
trap 'status=$?; rm -f "$duration_log"; bithumb_pytest_cleanup_workspace "$status"; exit "$status"' EXIT

bithumb_pytest_run_preflight "research test policy" uv run python scripts/check_research_test_policy.py
bithumb_pytest_run_preflight "strategy PR workload guard" uv run python scripts/check_strategy_pr_workload_guard.py
bithumb_pytest_run_preflight "research workload budget research-nightly" uv run python scripts/check_research_workload_budget.py --suite research-nightly
bithumb_pytest_mark_pytest_started
uv run pytest -q \
  -m "$RESEARCH_NIGHTLY_MARKER_EXPR" \
  --durations=100 \
  --durations-min=0.25 | tee "$duration_log"
uv run python scripts/check_research_e2e_inventory_durations.py "$duration_log"
