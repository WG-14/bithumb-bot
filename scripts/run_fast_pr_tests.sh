#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/scripts/lib/pytest_workspace.sh"

FAST_MARKER_EXPR="not research_kernel and not research_e2e and not audit_e2e and not walk_forward_e2e and not parallel_e2e and not nightly and not slow_research and not memory_sensitive"
export BITHUMB_TEST_TIER=fast
export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"
duration_log="$(mktemp "${TMPDIR:-/tmp}/bithumb-fast-pytest-durations.XXXXXX.log")"

bithumb_pytest_setup_workspace "fast"
status=0
trap 'status=$?; rm -f "$duration_log"; bithumb_pytest_cleanup_workspace "$status"; exit "$status"' EXIT

bithumb_pytest_sanitize_unsafe_env "fast PR pytest runner"

bithumb_pytest_run_preflight "research test policy" uv run python scripts/check_research_test_policy.py
bithumb_pytest_run_preflight "strategy PR workload guard" uv run python scripts/check_strategy_pr_workload_guard.py
bithumb_pytest_mark_pytest_started
uv run pytest -q \
  -m "$FAST_MARKER_EXPR" \
  --durations=50 \
  --durations-min=0.25 | tee "$duration_log"
uv run python scripts/check_fast_test_durations.py "$duration_log" --max-seconds 10
