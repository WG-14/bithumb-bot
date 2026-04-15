# Lot-Native Contract

This document defines the current verified lot-native contract, the current
canonical SELL authority surface, and the remaining explicit compatibility /
fail-closed cleanup boundary.

The already-achieved contract PASS is the starting baseline.
Canonical SELL authority is already lot-native at the public boundary.
Internal compatibility and fail-closed handling still remain in code and are
part of the current cleanup boundary.

The code and tests now verify that:

- canonical SELL quantity and SELL lot count are taken from lot-native state
- the public SELL boundary still treats qty as derived from lot-native authority
- compatibility and provenance residue remains a fail-closed cleanup boundary rather than canonical authority
- schema-time compatibility/backfill handling does not canonically reconstruct lot-native SELL authority from qty-only legacy rows

The remaining work after that is explicit compatibility-boundary cleanup, not a
change to canonical SELL semantic authority.

## Batch Scope

### Already-satisfied contract premises

These conditions are already satisfied and remain maintained premises for this
batch:

- the final SELL quantity is derived from the canonical sellable lot count
- qty stays a derived compatibility value
- dust-only remainder, boundary-below-min, and no executable exit are normal suppression outcomes
- open_exposure and dust_tracking stay separate on the SELL path
- qty-only legacy rows fail closed in recovery and lifecycle
- `legacy_lot_metadata_missing` no longer defines the desired semantic authority model, but it still exists as an internal fail-closed blocker or compatibility-fallback marker

These premises are not the finish line. They are the baseline that must stay
intact while the remaining declaration gap is closed.

### Current verified external contract

The current verified external contract is narrower than full migration
completion: the public SELL authority boundary is lot-native, and public SELL
qty remains derived from that lot-native authority. That does not mean every
fail-closed blocker or compatibility marker has already disappeared from all
internal fail-closed or diagnostic-only paths.

Current materialization paths still include compatibility-aware fail-closed
normalization while building internal or diagnostic context. That
normalization remains an adapter layer only; it does not displace the
lot-native canonical SELL authority surface.

For this batch, that means:

- canonical SELL authority at the public boundary is `position_state.normalized_exposure.sellable_executable_lot_count`
- qty remains a derived materialization from the normalized lot-native authority surface
- fail-closed blocker reasons such as `legacy_lot_metadata_missing` may still appear in internal fail-closed paths and current code paths, while recovery/lifecycle summary surfaces still fail closed without restoring executable authority
- compatibility and provenance handling must remain non-authoritative even where fail-closed markers still exist

This is the current external contract, not a statement that every compatibility
or fail-closed path has already disappeared from internal code paths or
diagnostic-only outputs.

### Remaining explicit boundary

The remaining non-P0 work is the internal compatibility boundary:

- `decision_context` still contains compatibility-aware fail-closed normalization while materializing non-authoritative consumer output
- fail-closed `legacy_lot_metadata_missing` handling still exists in the current code path as an internal or intermediate blocker/reason and fallback marker
- diagnostic-only broker/reporting evidence may still retain source or truth-source metadata outside the primary semantic flow

That remaining boundary must stay non-authoritative. It is cleanup scope, not
semantic authority.

## Canonical Authority

- Shared semantic authority:
  - executable authority: `position_state.normalized_exposure.open_lot_count`
  - SELL authority: `position_state.normalized_exposure.sellable_executable_lot_count`
  - BUY authority: `position_state.normalized_exposure.entry_allowed`
  - entry-gate flatness signal: `position_state.normalized_exposure.effective_flat` and `entry_gate_effective_flat`
  - internal normalized flatness model: `position_state.normalized_exposure.terminal_state`
  - dust authority: `position_state.normalized_exposure.dust_tracking_lot_count`
  - reserved exit authority: `position_state.normalized_exposure.reserved_exit_lot_count`
  - qty semantic authority: none; qty remains derived only
  - legacy qty-only recovery: fail closed
- Consumer surfaces built from that semantic authority:
  - execution handoff: SELL submission authority comes from `position_state.normalized_exposure.sellable_executable_lot_count`, and qty handoff fields remain derived only
  - recovery/lifecycle summary surface: the materialized interpretation label `holding_authority_state` together with `sellable_executable_lot_count`, `exit_allowed`, and `exit_block_reason`
  - decision/reporting materialization surface: derived qty fields and compatibility aliases may be emitted or recorded as non-authoritative materializations
- `effective_flat` and `entry_gate_effective_flat` are BUY entry-gate interpretations only. They must not be used as SELL authority, recovery authority, or executable-position authority.
- Current recovery and lifecycle interpretation should be read from the materialized holding/exit summary fields `holding_authority_state`, `sellable_executable_lot_count`, `exit_allowed`, and `exit_block_reason`; `holding_authority_state` is an interpretive label derived from the underlying lot counts and exit flags, not a root authority field. Qty-only legacy rows still fail closed there and can resolve to non-executable or no-open-lot summary outcomes without regaining executable SELL authority.
- Canonical authority remains the lot-native semantic authority surface above; execution, recovery/lifecycle summaries, and decision/reporting materializations are distinct consumer surfaces built from it.
- Derived qty materialization fields such as `sellable_executable_qty` and `open_exposure_qty` remain operational outputs re-materialized from lot-native authority via lot sizing for broker payloads and reporting.
- Legacy/reporting compatibility aliases such as `position_qty`, `submit_payload_qty`, and `normalized_exposure_qty` may still appear in emitted or recorded context, but they are non-authoritative alias surfaces and must not be promoted to canonical SELL authority.
- The final authority for executable SELL quantity is the canonical sellable lot count.
- `requested_qty`, `executable_qty`, and submitted SELL order quantity must be re-materialized from that lot count via lot sizing.
- `qty` fields are non-authoritative derived values, not semantic SELL authority. They are still operationally required for broker interfacing, execution handoff, compatibility materialization, and reporting, even when the emitted context still includes qty aliases alongside canonical lot-native fields.
- `sellable_qty` or any other qty-like snapshot value does not have semantic authority over the SELL boundary.

## Developer Guardrail

When extending the SELL path, keep this implementation boundary explicit:

- Authoritative SELL path:
  - `src/bithumb_bot/decision_context.py`
  - canonical lot-native SELL authority is resolved from `position_state.normalized_exposure.sellable_executable_lot_count`
  - canonical SELL qty is re-materialized from that lot count via lot sizing; `position_state.normalized_exposure.sellable_executable_qty` is a derived handoff field, not the authority source
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
./scripts/run_lot_native_regression_gate.sh
```

Treat this gate as the required completion target for changes touching:

- SELL authority
- position-state authority surfaces
- recovery or reconcile authority handling
- `decision_context` canonical extraction
- reporting or telemetry flows that expose lot-native authority context

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
`reserved_for_open_sell_orders` is a distinct normal reserved-exit suppression path: executable exposure still exists, but normal SELL submission remains blocked because open SELL orders already reserved the sellable lots.

## State Semantics

- `open_exposure` means lot-native executable exposure.
- `dust_tracking` means operator-only residual tracking.
- `reserved_exit` means open executable lots already reserved by open SELL lifecycle state.
- `sellable_executable_lot_count` is the executable SELL lot count after applying the lot-native state rules.
- `reserved_exit_pending` means executable exposure exists but the current sellable lot count is zero because open SELL orders already reserved that inventory.
- `reserved_for_open_sell_orders` is the corresponding block/suppression interpretation for that reserved-exit-pending path, and it is distinct from dust-only remainder, `no_executable_exit_lot`, and fail-closed non-executable position outcomes.
- `terminal_state` remains part of the internal normalized position-state model.
- Current recorded/materialized flatness and exit interpretation should be read from the derived interpretation label `holding_authority_state` together with `sellable_executable_lot_count`, `exit_allowed`, and `exit_block_reason`, not from qty aggregation.
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

## Current PASS Baseline

The current PASS baseline keeps all of the following true at the same time:

- the contract PASS baseline above remains intact
- the executable SELL path remains lot-native and canonical at the boundary
- recovery and lifecycle continue to fail closed for qty-only legacy rows without reintroducing semantic authority
- fail-closed blocker paths such as `legacy_lot_metadata_missing` may still remain visible in internal or diagnostic-only surfaces
- internal fail-closed compatibility handling may still remain behind the normalized authority boundary

This baseline fails if any of the following become true:

- any executable SELL decision still depends on qty as authority
- `open_exposure` and `dust_tracking` are merged into a single executable inventory
- recovery or lifecycle restores qty-only legacy rows as executable semantic authority
- fail-closed blocker or compatibility handling becomes canonical SELL authority

## Remaining Internal Boundary

The following still remain today, but only behind an explicit
non-authoritative boundary:

- internal fail-closed compatibility normalization inside `decision_context`
- diagnostic-only source or truth-source metadata in broker/reporting evidence

These are not allowed to regain semantic authority over SELL, position, or
recovery state.

## Documentation Rule

This document must describe the same external contract that the current code
and tests verify:

- canonical lot-native SELL authority remains intact at the boundary
- fail-closed blocker reasons such as `legacy_lot_metadata_missing` may still
  appear in internal fail-closed paths
- internal fail-closed compatibility handling may still remain behind the
  normalized authority boundary
