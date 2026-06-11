#!/usr/bin/env bash
set -u
set -o pipefail

if [[ "${BITHUMB_CODEX_BLOCK_BROAD_TEST_RUNNERS:-0}" == "1" ]]; then
  echo "[CODEX-BROAD-RUNNER-GUARD] Codex ${BITHUMB_CODEX_MODE:-session} must not run ${BASH_SOURCE[0]}." >&2
  echo "[CODEX-BROAD-RUNNER-GUARD] Run only focused validation directly related to the patch or failure packet." >&2
  exit 126
fi

FAST_MARKER_EXPR="not research_kernel and not research_e2e and not audit_e2e and not walk_forward_e2e and not parallel_e2e and not nightly and not slow_research and not memory_sensitive"
RESEARCH_NIGHTLY_MARKER_EXPR="research_kernel or research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e or nightly or slow_research or memory_sensitive"

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

  "$@"
  status=$?

  end=$(date +%s)

  echo
  echo "---- RESULT: exit_code=$status elapsed=$((end - start))s ----"

  # Continue diagnostics after failures.
  return 0
}

run "research backtest reproducibility durations" \
  uv run pytest -q tests/test_research_backtest_reproducibility.py --durations=50 --durations-min=0

run "research walk forward durations" \
  uv run pytest -q tests/test_research_walk_forward.py --durations=20 --durations-min=0

run "collect count: research E2E classes" \
  bash -lc "uv run pytest --collect-only -q -m '$RESEARCH_NIGHTLY_MARKER_EXPR' | grep '::' | wc -l"

run "collect count: memory_sensitive" \
  bash -lc 'uv run pytest --collect-only -q -m "memory_sensitive" | grep "::" | wc -l'

run "collect count: default PR fast suite" \
  bash -lc "uv run pytest --collect-only -q -m '$FAST_MARKER_EXPR' | grep '::' | wc -l"

run "cProfile: stress order independence test" \
  uv run python -m cProfile -o /tmp/stress_order.prof -m pytest -q \
  tests/test_research_backtest_reproducibility.py::test_stress_report_is_candidate_order_independent

run "print cProfile top cumulative time" \
  uv run python - <<'PY'
import pstats

p = pstats.Stats("/tmp/stress_order.prof")
p.strip_dirs().sort_stats("cumtime").print_stats(40)
PY

echo
echo "============================================================"
echo "Manual inspection checklist"
echo "============================================================"
echo "- SQLite insert/load"
echo "- dataset quality report"
echo "- strategy loop"
echo "- hash/content payload"
echo "- JSON artifact write"
echo "- audit trace write"
echo "- parallel executor overhead"

run "collect all tests" \
  uv run pytest --collect-only -q

echo
echo "DONE REMAINING NON-FULL-SUITE: $(date '+%Y-%m-%d %H:%M:%S')"
