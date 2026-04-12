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

## Required Checks

Run these before and after authority-sensitive changes:

```bash
./scripts/run_lot_native_regression_gate.sh
./scripts/check_lot_native_authority_residue.sh
```
