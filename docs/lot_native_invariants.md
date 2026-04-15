# Lot-Native Invariants

This is the short extension guide for authority-sensitive work. Use it together
with `docs/lot_native_contract.md` and the lot-native gate scripts.

## Core Invariants

- Canonical SELL authority comes from `position_state.normalized_exposure.sellable_executable_lot_count`.
- Submitted SELL qty is derived from canonical lot-native state; qty snapshots are broker/reporting values only.
- Dust is a normal non-executable state transition and must not become SELL authority.
- Persisted lot states remain `open_exposure` and `dust_tracking`.
- Normalized runtime holding states are computed interpretations layered on top of persisted lot rows plus reservation/dust logic; current normalized states include `open_exposure`, `reserved_exit_pending`, `dust_only`, `flat`, and `non_executable_position`.
- `reserved_exit` remains a separate normalized reservation / accounting dimension, not a persisted lot-state peer.
- Aggregate or raw qty must not override canonical lot-native SELL authority.
- Qty-only or compatibility residue must fail closed in recovery and reconcile flows.

## New Path Rule

For any new SELL, exit, recovery, or reconcile path:

- Read final SELL authority from `position_state.normalized_exposure.sellable_executable_lot_count`, `reserved_exit_lot_count`, `exit_allowed`, and `exit_block_reason`.
- Treat `holding_authority_state` as the current external/materialized holding interpretation surface; treat `terminal_state` as internal normalized model terminology rather than the sole external authority field.
- Treat `reserved_exit_lot_count` as normalized reservation authority derived from the current exposure context, not as a stored `position_state` row value.
- Treat raw qty, aggregate qty, and observational telemetry fields as derived-only; they must not grant or restore SELL authority.
- Extend the nearest `lot_native_regression_gate` tests when a change can affect SELL authority, reserved-exit handling, or qty-only fail-closed behavior.

## Required Checks

Run these before and after authority-sensitive changes:

```bash
./scripts/run_lot_native_regression_gate.sh
./scripts/check_lot_native_authority_residue.sh
```
