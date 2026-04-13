# Lot-Native Change Checklist

Use this checklist for changes touching SELL authority, position-state authority,
recovery/reconcile authority handling, `decision_context`, or reporting/telemetry
surfaces that expose lot-native execution state.

## Required Gate

Run the dedicated lot-native regression gate before and after the change:

```bash
./scripts/run_lot_native_regression_gate.sh
./scripts/check_lot_native_authority_residue.sh
```

## Checklist

- Confirm SELL authority still comes from `position_state.normalized_exposure.sellable_executable_lot_count`.
- Confirm submitted SELL qty remains derived from canonical lot-native state, not `raw_total_asset_qty`, `position_qty`, `submit_payload_qty`, or other observational qty fields.
- Confirm dust remains non-executable tracking and is not merged into executable SELL inventory.
- Confirm persisted lot states remain `open_exposure` and `dust_tracking`.
- Confirm `reserved_exit` remains a normalized reservation / accounting dimension and is not treated as a stored `position_state` peer.
- Confirm qty-only or legacy compatibility data still fails closed in recovery/reconcile and does not restore executable authority.
- Confirm reporting/telemetry surfaces touched by the change read authority-sensitive exposure fields from canonical lot-native state and treat qty residue as diagnostic only.
- Confirm new or updated tests covering the touched authority-sensitive path are included in the lot-native gate.
- Confirm the touched SELL live-path regressions still collect through `scripts/run_lot_native_regression_gate.sh`, especially the shadow-submit-source and no-executable-exit suppression tests in `tests/test_live_broker.py`.
