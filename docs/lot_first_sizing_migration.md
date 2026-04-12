# Lot-First Sizing Migration Notes

This note tracks the remaining declaration-closing work after the SELL sizing
boundary was secured.

The sizing boundary itself is already complete.
The remaining work for this batch is to remove the semantic residue that still
prevents full lot-native declaration completion in `decision_context` and
`reporting`.

## Current Frame

### Already achieved premise: SELL boundary practical PASS / contract PASS

- The system already has a substantial lot-first shape in the sizing and state layers.
- SELL boundary practical PASS is complete.
- `build_sell_execution_sizing()` is already lot-authoritative.
- Live SELL submit path final `order_qty` is already closed under canonical sellable lot count authority.
- The remaining migration residue is now downstream of sizing-boundary logic, not inside the SELL boundary itself.

### This batch focus: declaration-closing residue removal

- Treat practical live-operation target PASS as the established baseline, not the finish line.
- Remove the remaining downstream compatibility/provenance residue that still blocks full declaration.
- Keep legacy and qty-first residue visible only as non-authoritative residue until it disappears from the contract surface.
- Keep `open_exposure` and `dust_tracking` separation intact while the declaration residue is removed.

### Final completion condition: full lot-native declaration

- All downstream consumers explain executable meaning using only lot-derived truth.
- No compatibility fallback authority remains in `decision_context`.
- No compatibility or provenance truth-source layer remains in `reporting`, even as a primary-field layer.

## Current Batch Focus

The current contract PASS already includes:

- final SELL quantity authority from canonical sellable lot count
- qty-only legacy rows failing closed without regaining executable authority
- `legacy_lot_metadata_missing` no longer being the desired semantic authority model
- downstream fallback or provenance being treated as residue that must be removed for full declaration

The remaining declaration-closing gap for this batch still includes real code
paths, not just cosmetic residue:

- removing the remaining `decision_context` compatibility fallback / provenance residue
- removing the remaining `reporting` truth-source / provenance primary-field residue
- removing fail-closed lot-metadata-gap residue from primary/emitted semantic surfaces without restoring qty-first authority

These items are the direct closure targets for full lot-native declaration.
They are not later-stage extras.

## Closure Targets

### `decision_context` compatibility fallback / provenance

This residue must disappear because:

- current contract PASS already holds when SELL authority remains lot-native
- full declaration still FAILS while legacy compatibility fallback or provenance remains in `decision_context`

Required outcome:

- `decision_context` no longer depends on legacy compatibility fallback authority or provenance
- `decision_context` no longer carries an explicit compatibility residue bucket

### `reporting` truth-source / provenance primary layer

This residue must disappear because:

- current contract PASS already holds when SELL authority remains lot-native
- full declaration still FAILS while reporting retains truth-source or provenance as a primary-field layer

Required outcome:

- `reporting` no longer preserves compatibility, truth-source, or provenance layers as primary fields
- `reporting` no longer carries an explicit truth-source or provenance layer at the primary-field level

## Compatibility Notes

- No database migration is required for the current storage shape.
- Existing quantity fields remain available as derived compatibility values.
- Current lot-count fields are authoritative lot-native state, not just additive reporting fields:
  - `open_lot_count`
  - `dust_tracking_lot_count`
  - `reserved_exit_lot_count`
  - `sellable_executable_lot_count`
- Qty fields remain derived compatibility, reporting, and broker-interface values materialized from those lot-authoritative fields.

## Batch Completion Line

This batch passes when:

- practical PASS remains baseline and is not reopened
- contract PASS remains preserved
- the remaining `decision_context` compatibility fallback / provenance residue is removed
- the remaining `reporting` truth-source / provenance primary-field residue is removed
- canonical downstream truth remains lot-derived and authoritative
- the completion line for this batch is the full lot-native declaration line

## Final Termination Condition

The full lot-native target is reached when all downstream consumers treat
lot-native state as the sole semantic authority, `decision_context` no longer
needs legacy compatibility fallback or provenance, and `reporting` no longer
retains compatibility, truth-source, or provenance layers as primary fields.
