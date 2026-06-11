#!/usr/bin/env bash
set -u
set -o pipefail

if [[ "${BITHUMB_CODEX_BLOCK_BROAD_TEST_RUNNERS:-0}" == "1" ]]; then
  echo "[CODEX-BROAD-RUNNER-GUARD] Codex ${BITHUMB_CODEX_MODE:-session} must not run ${BASH_SOURCE[0]}." >&2
  echo "[CODEX-BROAD-RUNNER-GUARD] Run only focused validation directly related to the patch or failure packet." >&2
  exit 126
fi

mkdir -p test-logs
LOG_DIR="test-logs/patch_diag_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

run() {
  local title="$1"
  shift

  echo
  echo "============================================================"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $title"
  echo "COMMAND: $*"
  echo "============================================================"

  local start end status
  start=$(date +%s)
  "$@" 2>&1 | tee "$LOG_DIR/${title// /_}.log"
  status=${PIPESTATUS[0]}
  end=$(date +%s)

  echo "---- RESULT: exit_code=$status elapsed=$((end - start))s ----" | tee -a "$LOG_DIR/${title// /_}.log"
  return 0
}

echo "Log dir: $LOG_DIR"

run "git state" bash -lc '
  git status --short
  git branch --show-current
  git log -1 --oneline --decorate
'

run "uv sync" uv sync

run "collect all" bash -lc '
  uv run pytest --collect-only -q | tee "$0/collect_all_raw.log" | grep "::" | wc -l
' "$LOG_DIR"

run "collect marker inventory" bash -lc '
  for expr in \
    "unit" \
    "contract" \
    "integration" \
    "resource_guard" \
    "memory_sensitive" \
    "research_kernel" \
    "research_e2e" \
    "audit_e2e" \
    "walk_forward_e2e" \
    "parallel_e2e" \
    "slow_research" \
    "slow_integration" \
    "nightly"
  do
    count=$(uv run pytest --collect-only -q -m "$expr" | grep "::" | wc -l)
    printf "%-24s %s\n" "$expr" "$count"
  done
'

FAST_EXPR='not slow_research and not slow_integration and not research_kernel and not research_e2e and not audit_e2e and not walk_forward_e2e and not parallel_e2e and not nightly and not memory_sensitive'

run "collect default fast candidate" bash -lc "
  uv run pytest --collect-only -q -m \"$FAST_EXPR\" | tee \"$LOG_DIR/collect_fast_raw.log\" | grep '::' | wc -l
"

run "fast tier guard tests" env BITHUMB_TEST_TIER=fast uv run pytest -q \
  tests/test_research_backtest_reproducibility.py::test_contract_research_backtest_wrapper_enforces_fast_budget \
  tests/test_research_backtest_reproducibility.py::test_fast_tier_blocks_production_backtest_before_io_and_tick_execution \
  tests/test_research_backtest_reproducibility.py::test_fast_tier_blocks_production_walk_forward_before_io_and_tick_execution \
  tests/test_research_backtest_reproducibility.py::test_fast_tier_allows_bounded_contract_evaluator_path \
  --durations=20 --durations-min=0

run "formerly slow smoke tests" env BITHUMB_TEST_TIER=fast uv run pytest -q \
  tests/test_research_backtest_reproducibility.py::test_stress_report_is_candidate_order_independent \
  tests/test_research_backtest_reproducibility.py::test_different_stress_seed_changes_auditable_seed_hash \
  tests/test_research_backtest_reproducibility.py::test_candidate_profile_hash_remains_promotion_bound_while_behavior_hash_is_logical \
  tests/test_research_backtest_reproducibility.py::test_audit_trace_verification_detects_tamper_and_missing_stream \
  tests/test_research_walk_forward.py::test_walk_forward_report_persists_artifact_discovery_metadata \
  --durations=20 --durations-min=0

run "contract and resource guard research file" env BITHUMB_TEST_TIER=fast uv run pytest -q \
  tests/test_research_backtest_reproducibility.py \
  -m "contract or resource_guard" \
  --durations=50 --durations-min=0.2

run "default fast suite" env BITHUMB_TEST_TIER=fast uv run pytest -q \
  -m "$FAST_EXPR" \
  --durations=80 --durations-min=1.0

E2E_EXPR='research_kernel or research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e or slow_research or nightly'

run "research e2e suite" uv run pytest -q \
  -m "$E2E_EXPR" \
  --durations=80 --durations-min=1.0

echo
echo "DONE: $LOG_DIR"
