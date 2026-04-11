# Lot-Native Contract

This document defines the current verified lot-native contract, the P0
completion line for this batch, and the remaining explicit cleanup boundary.

The already-achieved contract PASS is the starting baseline.
The direct P0 goal of this batch is to keep canonical lot-native SELL
authority intact while removing truth-source/provenance residue from the
primary emitted and reporting flows.

The code and tests now verify that:

- canonical SELL quantity and SELL lot count are taken from lot-native state
- emitted `decision_context` payloads do not carry compatibility/provenance residue
- primary reporting summaries no longer read truth-source/provenance fields in their main flow

The remaining work after that is an explicit internal compatibility-boundary
cleanup task, not external semantic authority.

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

### Verified P0 target

The verified P0 target is complete when every externally consumed SELL and
reporting path uses lot-native state as semantic authority and no longer
retains compatibility or provenance as a primary field layer.

For this batch, that means:

- `decision_context` output no longer emits compatibility fallback authority or provenance residue
- `reporting` primary summaries no longer preserve compatibility, truth-source, or provenance layers as primary fields

This is the direct contract enforced by the current tests and runtime behavior.

### Remaining explicit boundary

The remaining non-P0 work is the internal compatibility boundary:

- `decision_context` still contains internal fail-closed compatibility normalization while building canonical output
- diagnostic-only broker/reporting evidence may still retain source or truth-source metadata outside the primary semantic flow

That remaining boundary must stay non-authoritative. It is cleanup scope, not
semantic authority.

## Canonical Authority

- Singular shared contract surface:
  - executable authority: `position_state.normalized_exposure.open_lot_count`
  - SELL authority: `position_state.normalized_exposure.sellable_executable_lot_count`
  - BUY authority: `position_state.normalized_exposure.entry_allowed`
  - flatness authority: `position_state.normalized_exposure.terminal_state`
  - dust authority: `position_state.normalized_exposure.dust_tracking_lot_count`
  - reserved exit authority: `position_state.normalized_exposure.reserved_exit_lot_count`
  - qty semantic authority: none; qty remains derived only
  - legacy qty-only recovery: fail closed
- The final authority for executable SELL quantity is the canonical sellable lot count.
- `requested_qty`, `executable_qty`, and submitted SELL order quantity must be derived from that lot count.
- `qty` fields are derived values for compatibility, reporting, and broker interfacing only.
- `sellable_qty` or any other qty-like snapshot value does not have semantic authority over the SELL boundary.

## Developer Guardrail

When extending the SELL path, keep this implementation boundary explicit:

- Authoritative SELL path:
  - `src/bithumb_bot/decision_context.py`
  - canonical lot-native SELL authority is resolved from `position_state.normalized_exposure.sellable_executable_lot_count`
  - canonical SELL qty remains derived from `position_state.normalized_exposure.sellable_executable_qty`
- Non-authoritative surfaces:
  - fail-closed compatibility fallback handling in `decision_context.py` is an adapter boundary only
  - qty normalization and dust-guard helpers in `src/bithumb_bot/broker/live.py` are observational/support-only and must not become SELL authority inputs
  - `open_exposure`, `dust_tracking`, and `reserved_exit` remain separate semantics and must not be merged into one executable inventory
- Prohibited shortcuts in new code:
  - do not source SELL eligibility or SELL sizing from qty aggregation, `submit_payload_qty`, `position_qty`, or other qty-only snapshots
  - do not treat compatibility/fallback fields as canonical SELL authority
  - do not sum `open_exposure` and `dust_tracking` to create executable SELL inventory
- Focused regression references:
  - `tests/test_lot_native_contract.py`
  - `tests/test_trade_lifecycle.py`
  - `tests/test_live_broker.py`

### Forbidden Qty-First Patterns

Keep these rules explicit in new decision-path code:

- SELL authority must remain canonical lot-native authority from `position_state.normalized_exposure.sellable_executable_lot_count`.
- observational qty such as `position_qty`, `requested_qty`, dust previews, or raw holdings snapshots is diagnostic-only and must not decide SELL eligibility or sizing.
- `open_exposure`, `dust_tracking`, and `reserved_exit` must not be recombined into executable SELL authority from qty aggregation.
- compatibility or fallback fields may remain only as derived or fail-closed adapter inputs and must not be promoted to canonical SELL authority.

## Regression Gate

Use the dedicated lot-native gate before and after changing SELL authority,
decision-context extraction, reporting interpretation, recovery, or lifecycle
logic:

```bash
uv run pytest -q -m lot_native_regression_gate
```

This gate must keep passing for these invariants:

- SELL authority comes from canonical lot/open-exposure state only
- dust remains a normal non-sellable state transition
- aggregate qty alone does not restore exit authority
- partial fill and restart reconciliation preserve lot-native state
- qty-only legacy or compatibility rows stay fail-closed

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

## P0 PASS

PASS is true only when all of the following are true at the same time:

- the contract PASS baseline above remains intact
- `decision_context` output no longer emits compatibility fallback authority or provenance
- `decision_context` output no longer carries an explicit compatibility residue bucket
- `reporting` primary summaries no longer preserve truth-source, provenance, or compatibility layers as primary fields
- the executable SELL path remains lot-native and canonical at the boundary
- recovery and lifecycle continue to fail closed for qty-only legacy rows without reintroducing semantic authority

FAIL is true if any of the following remain:

- any executable SELL decision still depends on qty as authority
- emitted `decision_context` payloads still expose compatibility fallback authority or provenance
- emitted `decision_context` payloads still contain a compatibility residue bucket
- primary reporting summaries still expose truth-source or provenance layers as primary fields
- primary reporting summaries still expose compatibility layers as primary fields
- `open_exposure` and `dust_tracking` are merged into a single executable inventory
- recovery or lifecycle restores qty-only legacy rows as executable semantic authority

## Remaining P1 Boundary

The following may still remain after P0, but only behind an explicit
non-authoritative boundary:

- internal fail-closed compatibility normalization inside `decision_context`
- diagnostic-only source or truth-source metadata in broker/reporting evidence

These are not allowed to regain semantic authority over SELL, position, or
recovery state.

## Batch Completion Line

This batch's P0 work is complete only when the document, tests, and working
instructions all describe the verified external contract the same way.

The completion line is:

- current contract PASS remains preserved
- emitted `decision_context` compatibility fallback/provenance residue is gone
- `reporting` primary-flow truth-source/provenance residue is gone
- no already-satisfied SELL-boundary or recovery premise is reopened

## Declaration Rule

The system can be declared P0-complete for this batch when canonical lot
authority at the SELL boundary remains intact and the remaining downstream
compatibility/provenance residue disappears from emitted `decision_context`
and primary `reporting` flows.
