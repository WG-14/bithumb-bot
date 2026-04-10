# Lot-First Sizing Migration Notes

This note tracks the remaining residue for the lot-first sizing migration.
It is a reinforcement document, not a final declaration for the full system.

## Current State

- The system already has a substantial lot-first shape in the sizing and state layers.
- SELL boundary practical PASS is complete.
- `build_sell_execution_sizing()` is already lot-authoritative.
- Live SELL submit path final `order_qty` is already closed under canonical sellable lot count authority.
- The remaining migration residue is now downstream of sizing-boundary logic, not inside the SELL boundary itself.

## Closure Direction For This Batch

- Treat practical live-operation target PASS as established baseline, not as open work.
- Close or classify the remaining downstream compatibility residue without reopening SELL boundary authority.
- Keep legacy and qty-first residue visible only as compatibility or provenance, never as executable semantic authority.
- Keep `open_exposure` and `dust_tracking` separation intact while the downstream residue is tightened.

## Remaining Downstream Residue

At batch start, the last core residue was recovery/lifecycle semantic handling
for qty-only legacy rows, especially `legacy_lot_metadata_missing`.

After this batch:

- recovery and lifecycle no longer expose `legacy_lot_metadata_missing` as an active semantic state
- qty-only legacy rows still fail closed and do not regain executable lot authority
- remaining downstream surfaces are compatibility attribution such as `compatibility:fallback:no_executable_open_lots` and `compatibility:context.position_state_source`

These compatibility surfaces are explanatory only. They do not reopen SELL
boundary authority and they do not restore qty-first executable semantics.

## Compatibility Notes

- No database migration is required for the current storage shape.
- Existing quantity fields remain available as derived compatibility values.
- New lot-count fields are additive and derived:
  - `open_lot_count`
  - `dust_tracking_lot_count`
  - `reserved_exit_lot_count`
  - `sellable_executable_lot_count`

## Batch Completion Line

This batch passes when:

- practical PASS remains baseline and is not reopened
- the recovery/lifecycle semantic residue is removed or reduced so it no longer acts as authority-relevant lifecycle meaning
- qty-only legacy rows still remain non-executable
- any remaining downstream compatibility surfaces are documented and tested as derived compatibility rather than executable semantic authority
- the completion line for this batch stays separate from the final full declaration line

## Final Termination Condition

The full lot-native target is reached when all downstream consumers treat
lot-native state as the sole semantic authority and any remaining compatibility
surfaces are purely derived explanation rather than unresolved lifecycle
meaning.
