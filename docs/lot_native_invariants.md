# Lot-Native Invariants

This is the short extension guide for authority-sensitive work. Use it together
with `docs/lot_native_contract.md` and the lot-native gate scripts.

## Core Invariants

- Canonical SELL authority comes from `position_state.normalized_exposure.sellable_executable_lot_count`.
- Submitted SELL qty is derived from canonical lot-native state; qty snapshots are broker/reporting values only.
- Dust is a normal non-executable state transition and must not become SELL authority.
- `open_exposure`, `dust_tracking`, and `reserved_exit` remain separate semantics.
- Aggregate or raw qty must not override canonical lot-native SELL authority.
- Qty-only or compatibility residue must fail closed in recovery and reconcile flows.

## New Path Rule

For any new SELL, exit, recovery, or reconcile path:

- Read final SELL authority from `position_state.normalized_exposure.sellable_executable_lot_count`, `reserved_exit_lot_count`, `exit_allowed`, and `exit_block_reason`.
- Treat raw qty, aggregate qty, and observational telemetry fields as derived-only; they must not grant or restore SELL authority.
- Extend the nearest `lot_native_regression_gate` tests when a change can affect SELL authority, reserved-exit handling, or qty-only fail-closed behavior.

## Required Checks

Run these before and after authority-sensitive changes:

```bash
./scripts/run_lot_native_regression_gate.sh
./scripts/check_lot_native_authority_residue.sh
```
