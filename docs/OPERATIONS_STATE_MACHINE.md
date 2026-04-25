# Operations State Machine

This document defines the operator-facing runtime state semantics for live and paper operation.

The design goal is safety-first recovery without overloading one order status to mean:

- exchange execution truth
- accounting completion
- operator-review severity
- process halt state

## State Axes

Runtime behavior should be interpreted across four separate axes.

### 1. Submission State

- `PENDING_SUBMIT`: local intent is recorded and broker submission is in progress.
- `SUBMIT_UNKNOWN`: broker submission outcome is ambiguous and must be reconciled before new orders.
- `NEW` / `PARTIAL`: broker order is known and may still execute.
- `CANCEL_REQUESTED`: closeout/cancel flow is in progress.
- `FAILED` / `CANCELED` / `FILLED`: terminal submission or execution outcomes.

### 2. Exchange Execution State

- `NEW` and `PARTIAL` mean the broker still reports executable order state.
- `FILLED` and `CANCELED` are terminal exchange execution states.
- `ACCOUNTING_PENDING` is a compatibility status used when execution evidence exists but the ledger cannot safely finalize the fill yet.
  It is not a terminal historical state.
  It blocks new submissions, but it is treated as auto-recovering rather than operator-recovery-required.

### 3. Accounting State

- `accounting_complete`: authoritative fee and ledger application are complete.
- `fee_pending`: fill evidence is present but authoritative fee/accounting is not yet complete.
- `repaired`: accounting was later finalized by an explicit repair event.
- `fee_gap_recovery_required`: accounting drift remains unresolved and requires operator action.

Broker fill observations are diagnostic evidence.
Authoritative ledger application remains in `fills`, `trades`, portfolio replay, and lot projection state.

### 4. Operator / Risk State

- `OK`: process loop and new entries may proceed.
- `DEGRADED`: process loop may continue with warnings.
- `AUTO_RECOVERING`: the system is waiting on bounded reconcile/accounting convergence and keeps retrying safely.
- `OPERATOR_REVIEW_REQUIRED`: new orders are blocked until the operator resolves a real ambiguity or inconsistency.
- `HARD_HALTED`: trading stays blocked due to kill switch, loss limit, unresolved broker conflict, submit ambiguity, or other hard risk.

## Order Status Semantics

Current authoritative order-status classifications:

- Open / unresolved:
  - `PENDING_SUBMIT`
  - `NEW`
  - `PARTIAL`
  - `SUBMIT_UNKNOWN`
  - `ACCOUNTING_PENDING`
  - `RECOVERY_REQUIRED`
  - `CANCEL_REQUESTED`
- Terminal:
  - `FILLED`
  - `FAILED`
  - `CANCELED`

`RECOVERY_REQUIRED` is operator-review-required and unresolved.
It must not also be treated as a terminal historical state.

`ACCOUNTING_PENDING` is auto-recovering and unresolved.
It represents delayed accounting convergence, not a hard recovery incident by itself.

## Fee-Pending Handling

Fee-pending fill handling is asynchronous accounting, not immediate manual recovery.

Expected flow:

1. Broker fill is observed.
2. If fee/accounting is incomplete, `broker_fill_observations` records `accounting_status='fee_pending'`.
3. The order transitions to `ACCOUNTING_PENDING`.
4. New submissions remain blocked by the unresolved-order gate.
5. The process loop stays alive and reconcile continues retrying safely.
   `run_loop_allowed=1` is valid while `resume_ready=0` for this state.
6. If authoritative fee later becomes available, the fill is applied idempotently and the order reaches its terminal accounted state.
7. If fee attribution remains ambiguous or invalid, operator repair or review is still required.

Manual DB editing is not a normal recovery path.
Repairs must be recorded as explicit events such as `fee_pending_accounting_repair`.

## Resume / Pause Policy

The process should auto-clear a repair-completed pause only when the existing readiness policy says it is safe.

Safe auto-clear requires all of the following:

- no unresolved open orders
- no `RECOVERY_REQUIRED` orders
- no active fee/accounting issue
- broker / portfolio / lot projection convergence
- no blocking incident class
- no hard halt state

If those conditions are not met, the process remains blocked by the normal readiness gate instead of blindly calling `resume`.

When the only active issue is fee/accounting latency in `ACCOUNTING_PENDING`, startup and restart should continue in a degraded auto-recovering mode rather than converting that condition into a hard stopped process. New submissions still remain blocked until accounting converges.

## Dust-Only Residual

`dust_only` residual is accounting evidence, not executable SELL authority.

- harmless tracked dust may allow `run_loop_allowed=1` and `new_entry_allowed=1`
- closeout may still remain blocked with `closeout_blocked:dust_only_remainder`
- dust-only residual alone should not force manual resume when policy marks it tracked-only and non-blocking

## Audit Invariants

Audit must fail on at least these invariants:

- negative or impossible portfolio/trade snapshots
- terminal `FILLED` orders with `qty_filled <= 0`
- orphan fills
- terminal orders retaining `local_intent_state='PENDING_SUBMIT'`
- authoritative accounting replay mismatch against portfolio state

## Operator Commands

Common commands:

- `uv run bithumb-bot recovery-report`
- `uv run bithumb-bot restart-checklist`
- `uv run bithumb-bot reconcile`
- `uv run bithumb-bot fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <source> --apply --yes`
- `uv run bithumb-bot rebuild-position-authority --apply --yes`

Use repair commands only when auto-recovery cannot finalize the evidence safely on its own.
