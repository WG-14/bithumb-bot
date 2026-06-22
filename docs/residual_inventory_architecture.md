# Residual Inventory Architecture Decision

## Decision

The project keeps lot-native executable authority and will model future dust
re-bucketing only as an explicit ledger/projection event.

This selects Option A:

- `open_exposure` remains executable lot authority.
- `dust_tracking` remains non-executable residual tracking.
- Normal SELL authority must not sum `open_exposure + dust_tracking`.
- Any future conversion of tracked dust into executable inventory requires an
  explicit re-bucketing event with provenance.
- That future event must prove quantity conservation, fee and cost-basis
  preservation, and deterministic replay before implementation.

Option B, splitting exchange execution inventory from accounting lots, is out
of scope for this task.

## Required Future Implementation Gate

Dust re-bucketing is not implemented by this decision document. A later
implementation must include focused tests proving:

- total quantity is preserved across the re-bucketing event
- fee totals and cost basis are preserved or explicitly reallocated with
  evidence
- replay from ledger evidence produces the same projection
- normal SELL logic still does not consume `dust_tracking` without a
  re-bucketing event

Until those tests exist and pass, tracked dust remains tracked residual only.

## Current Operator Meaning

A converged sub-min residual with no open orders, no submit-unknown orders, no
recovery-required orders, and matching broker/local/projection quantities is a
tracked non-executable residual. It is not exchange sellability proof and does
not require manual mobile-app closeout.
