#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

required_tests=(
  "tests/test_live_broker.py::test_lot_native_gate_live_execute_signal_sell_ignores_exit_sizing_qty_source_shadow"
  "tests/test_live_broker.py::test_lot_native_gate_sell_no_executable_exit_suppression_keeps_observational_position_qty_non_authoritative"
  "tests/test_decision_telemetry.py::test_lot_native_gate_canonical_exposure_snapshot_ignores_shadow_top_level_sell_authority_fields"
)

collected="$(uv run pytest --collect-only -q -m lot_native_regression_gate "$@")"
for test_id in "${required_tests[@]}"; do
  if ! grep -Fqx "$test_id" <<<"$collected"; then
    printf 'missing required lot-native regression coverage: %s\n' "$test_id" >&2
    exit 1
  fi
done

uv run pytest -q -m lot_native_regression_gate "$@"
