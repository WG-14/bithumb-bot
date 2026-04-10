# Lot-Native Contract

This document is a contract for the current batch, not a design introduction.
It keeps the already-completed SELL boundary practical PASS as baseline and
defines the remaining path toward full lot-native declaration.

## Batch Scope

### In-batch target

The goal of the minimum target remains:

- the final SELL quantity is derived from the canonical sellable lot count
- qty stays a derived compatibility value
- dust-only remainder, boundary-below-min, and no executable exit are normal suppression outcomes
- open_exposure and dust_tracking stay separate on the SELL path

The current batch is narrower than the original SELL-boundary closure work.
Its target is to define and tighten the remaining downstream residue after the
already-achieved practical PASS baseline.

### Out-of-batch target

The full lot-native target is broader than this batch.
It requires every downstream consumer to treat lot-native state as the only semantic authority, including all remaining compatibility surfaces and any residual qty-first assumptions outside the SELL boundary.

## Canonical Authority

- The final authority for executable SELL quantity is the canonical sellable lot count.
- `requested_qty`, `executable_qty`, and submitted SELL order quantity must be derived from that lot count.
- `qty` fields are derived values for compatibility, reporting, and broker interfacing only.
- `sellable_qty` or any other qty-like snapshot value does not have semantic authority over the SELL boundary.

## Suppression Semantics

The following outcomes are suppression outcomes, not submit failures:

- `dust_only_remainder`
- `boundary_below_min`
- `no_executable_exit_lot`

These outcomes mean the SELL path should suppress submission or exit processing cleanly.
They do not mean the system failed to submit a valid SELL order.

## State Semantics

- `open_exposure` means lot-native executable exposure.
- `dust_tracking` means operator-only residual tracking.
- `sellable_executable_lot_count` is the executable SELL lot count after applying the lot-native state rules.
- flatness and exitability must be judged from lot-native state, not from qty aggregation.
- `open_exposure` and `dust_tracking` must not be merged into a single SELL executable inventory.

## Batch Evaluation

### Minimum target

PASS:

- the SELL boundary keeps lot-native meaning for executable exposure
- dust-only and below-min outcomes remain suppression outcomes
- qty remains derived only

PARTIAL:

- some compatibility outputs still exist, but they no longer drive SELL authority

FAIL:

- qty regains semantic authority for SELL submission
- dust_tracking is treated as sellable inventory

### Practical live-operation target

PASS:

- the final SELL order quantity is always derived from the canonical sellable lot count
- qty rounding does not override lot authority
- dust-only remainder, boundary-below-min, and no executable exit are normal suppression outcomes
- open_exposure and dust_tracking stay separated in executable state
- flatness and exitability are lot-native judgments
- this target is already achieved and is the baseline for later batches

PARTIAL:

- the SELL path is mostly lot-native, but one or more compatibility surfaces still leak qty authority

FAIL:

- SELL submission can still be driven by qty aggregation or by mixed open_exposure/dust_tracking semantics

## Remaining Downstream Residue After Practical PASS

After practical PASS, the remaining residue was downstream of the SELL boundary.
At the start of this batch, the only in-scope core residue was the
recovery/lifecycle semantic marker `legacy_lot_metadata_missing`.

With this batch complete:

- recovery and lifecycle fail closed on qty-only legacy rows without exposing `legacy_lot_metadata_missing` as an authority-relevant semantic state
- qty-only legacy rows still do not regain executable lot authority
- decision_context and reporting may still show compatibility attribution such as `compatibility:fallback:no_executable_open_lots` and `compatibility:context.position_state_source`, but those surfaces are compatibility explanation rather than lifecycle semantic authority

Out of scope for this batch:

- reopening SELL boundary quantity authority
- redesigning the already-closed SELL submit path
- broad execution, strategy, or storage refactors

## Full Lot-Native Exclusive Authority

After SELL-boundary practical PASS, full lot-native exclusive authority means:

- downstream consumers do not rely on qty-only rows or qty-only snapshots as semantic authority for executable state
- legacy compatibility may exist only as derived or non-authoritative compatibility
- recovery and lifecycle do not reconstruct executable semantics from qty-only residue and do not preserve legacy semantic markers such as `legacy_lot_metadata_missing` as active lifecycle meaning
- decision_context fallback truth sources may remain as compatibility indicators, but they must not act as semantic authority in decision meaning
- reporting compatibility and provenance fields may remain for operator explanation, but they are derived compatibility signals rather than authority-bearing semantics
- compatibility-only truth sources should be labeled clearly enough that they
  cannot be mistaken for canonical lot-native authority

## Current Batch Evaluation

This section defines PASS, PARTIAL, and FAIL only for the current downstream-residue batch.

### Current batch PASS

- practical live-operation target remains PASS and is not reopened
- recovery and lifecycle residue is closed as semantic authority: qty-only legacy rows fail closed and `legacy_lot_metadata_missing` is no longer emitted as an active lifecycle semantic state
- decision_context fallback truth sources are explicitly classified as compatibility indicators rather than semantic authority
- reporting fallback and provenance fields are explicitly classified as derived compatibility rather than semantic authority
- the contract and tests clearly distinguish lifecycle semantic closure from compatibility attribution that may still appear downstream

### Current batch PARTIAL

- practical live-operation target remains PASS
- qty-only legacy rows still fail closed, but recovery or lifecycle still emits an authority-relevant legacy semantic residue
- the separation between lifecycle closure and downstream compatibility attribution remains unclear

### Current batch FAIL

- the batch reopens SELL boundary authority as if it were still incomplete
- qty-only recovery residue is treated as executable lot-native authority
- recovery or lifecycle still treats `legacy_lot_metadata_missing` as active semantic authority
- decision_context fallback truth sources are allowed to act as semantic authority
- reporting provenance fields are described as if they carry semantic authority
- the current-batch completion line and the final full-declaration line are merged or confused

## Current Batch Completion Conditions

This batch is complete when:

- practical live-operation target remains documented as already achieved baseline
- recovery and lifecycle no longer expose `legacy_lot_metadata_missing` as an authority-relevant semantic state
- qty-only legacy rows remain non-executable and non-authoritative
- any remaining decision_context or reporting compatibility surfaces are explicitly labeled as derived compatibility rather than lifecycle semantic authority
- the contract tests gate that distinction precisely

## Final Full Lot-Native Declaration Conditions

PASS:

- all SELL-boundary and downstream state consumers use lot-native authority end-to-end
- qty-only reasoning is removed from semantic decision making
- the remaining compatibility layer is purely derived or explanatory

PARTIAL:

- some lot-native semantics remain incomplete outside the batch boundary

FAIL:

- any executable SELL decision still depends on qty as the authority

The system can be declared fully lot-native only when:

- no SELL decision uses qty as semantic authority
- no executable state mixes open_exposure with dust_tracking
- recovery and lifecycle no longer carry legacy-compatible executable semantic residue
- decision_context fallback truth sources do not stand in for unresolved executable semantics
- reporting compatibility and provenance surfaces are retained only as derived operator explanation, not as semantic authority
- all remaining compatibility outputs are derived only
- lot-native state is the exclusive authority across recovery, lifecycle, reporting, and execution paths
