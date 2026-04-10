# Lot-Native Contract

This document defines the current lot-native batch contract and the full
declaration completion line.

The already-achieved contract PASS is the starting baseline.
The direct goal of this batch is full lot-native declaration completion, which
means the remaining `decision_context` compatibility fallback/provenance and
the remaining `reporting` truth-source/provenance primary-field layer must be
removed.

## Batch Scope

### Already-satisfied contract premises

These conditions are already satisfied and remain maintained premises for this
batch:

- the final SELL quantity is derived from the canonical sellable lot count
- qty stays a derived compatibility value
- dust-only remainder, boundary-below-min, and no executable exit are normal suppression outcomes
- open_exposure and dust_tracking stay separate on the SELL path
- qty-only legacy rows fail closed in recovery and lifecycle
- `legacy_lot_metadata_missing` is not an active lifecycle semantic state

These premises are not the finish line. They are the baseline that must stay
intact while the remaining declaration gap is closed.

### Full declaration target

The full lot-native declaration is complete only when every downstream
consumer treats lot-native state as the only semantic authority and no longer
retains compatibility or provenance as a primary field layer.

For this batch, that specifically means:

- `decision_context` no longer depends on legacy compatibility fallback authority or provenance
- `reporting` no longer preserves compatibility, truth-source, or provenance layers as primary fields

The goal of this batch is therefore not to defer those residues. It is to close
them as direct completion conditions for the full declaration.

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

### Contract PASS baseline

PASS:

- the SELL boundary keeps lot-native meaning for executable exposure
- dust-only and below-min outcomes remain suppression outcomes
- qty remains derived only

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

FAIL:

- SELL submission can still be driven by qty aggregation or by mixed open_exposure/dust_tracking semantics

## Full Declaration PASS

PASS is true only when all of the following are true at the same time:

- the contract PASS baseline above remains intact
- `decision_context` no longer emits, preserves, or depends on legacy compatibility fallback authority or provenance
- `decision_context` no longer carries an explicit compatibility residue bucket
- `reporting` no longer preserves truth-source, provenance, or compatibility layers as primary fields
- `reporting` no longer carries an explicit provenance or truth-source layer at the primary-field level
- the executable SELL path remains lot-native and canonical at the boundary
- recovery and lifecycle continue to fail closed for qty-only legacy rows without reintroducing semantic authority

FAIL is true if any of the following remain:

- any executable SELL decision still depends on qty as authority
- `decision_context` still contains legacy compatibility fallback authority or provenance
- `decision_context` still contains a compatibility residue bucket
- `reporting` still exposes truth-source or provenance layers as primary fields
- `reporting` still exposes compatibility layers as primary fields
- `open_exposure` and `dust_tracking` are merged into a single executable inventory
- recovery or lifecycle restores qty-only legacy rows as executable semantic authority

## Batch Completion Line

This batch is complete only when the document, tests, and working instructions
all describe the full lot-native declaration as the direct target, not as a
later refinement.

The completion line is:

- current contract PASS remains preserved
- the remaining `decision_context` compatibility fallback/provenance residue is gone
- the remaining `reporting` truth-source/provenance primary-field residue is gone
- no already-satisfied SELL-boundary or recovery premise is reopened

## Declaration Rule

The system can be declared fully lot-native only when the canonical lot
authority at the SELL boundary remains intact and the remaining downstream
compatibility/provenance residue disappears from `decision_context` and
`reporting`.
