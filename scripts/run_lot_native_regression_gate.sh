#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

required_tests=(
  # SELL authority is canonical lot-native on the normal submit path.
  "tests/test_live_broker.py::test_authority_boundary_live_execute_signal_sell_uses_exit_sizing_executable_qty_for_final_submit_payload"

  # Live SELL suppression boundary: mixed observational qty must not regain authority.
  "tests/test_live_broker.py::test_lot_native_gate_sell_dust_unsellable_rejects_observational_qty_authority"
  "tests/test_live_broker.py::test_lot_native_gate_sell_no_executable_exit_suppression_keeps_observational_position_qty_non_authoritative"
  "tests/test_live_broker.py::test_lot_native_gate_sell_dust_error_path_keeps_canonical_authority_separate_from_observational_qty"

  # Live SELL submit boundary: exit sizing shadow qty sources must stay non-authoritative.
  "tests/test_live_broker.py::test_lot_native_gate_live_execute_signal_sell_ignores_exit_sizing_qty_source_shadow"

  # Dust remains a normal state transition and qty aggregation does not restore exitability.
  "tests/test_lot_native_contract.py::test_position_state_model_bases_exitability_and_flatness_on_lot_state_not_qty_aggregation"

  # Operator/emergency SELL-capable path: flatten must stay on canonical lot-native authority.
  "tests/test_operator_commands.py::test_flatten_position_qty_only_portfolio_does_not_restore_sell_authority"
  "tests/test_operator_commands.py::test_flatten_position_reserved_exit_qty_does_not_bypass_canonical_sell_authority"

  # Lifecycle and restart recovery must preserve lot-native authority.
  "tests/test_trade_lifecycle.py::test_partial_exit_keeps_remaining_sell_authority_lot_native"
  "tests/test_trade_lifecycle.py::test_recovery_reconstructs_lot_native_exposure_and_dust_after_restart"
  "tests/test_trade_lifecycle.py::test_recovery_does_not_infer_executable_semantics_from_qty_without_lot_counts"
  "tests/test_recovery_restart_regression.py::test_lot_native_gate_reconcile_does_not_clear_halt_from_qty_only_holdings_without_lot_native_exposure"

  # Decision/reporting boundary: shadow top-level qty fields must not outrank canonical normalized exposure.
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
