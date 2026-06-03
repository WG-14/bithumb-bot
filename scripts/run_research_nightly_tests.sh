#!/usr/bin/env bash
set -euo pipefail

duration_log="$(mktemp "${TMPDIR:-/tmp}/bithumb-research-nightly-durations.XXXXXX.log")"
trap 'rm -f "$duration_log"' EXIT

uv run python scripts/check_research_test_policy.py
uv run pytest -q \
  -m "research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e or nightly or slow_research or memory_sensitive" \
  --durations=100 \
  --durations-min=0.25 | tee "$duration_log"
uv run python scripts/check_research_e2e_inventory_durations.py "$duration_log"
