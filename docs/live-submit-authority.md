# Live Submit Authority

Live real-order authority is typed. Dict payloads exist for storage, logs,
diagnostics, and replay observability; they are not submit authority.

## Authority Chain

Strategy authority flows through:

```text
StrategyPolicy / RuntimeDecisionAdapter
-> StrategyDecisionV2
-> DecisionEnvelope
```

`DecisionEnvelope.as_persistence_context()` emits non-authoritative persistence
and observability material. It preserves policy and replay hashes, but mutation
of that dict must not change execution authority.

Execution authority flows through:

```text
PortfolioAllocator
-> PortfolioTarget
-> target delta
-> exchange order rules
-> ExecutionAuthorityEnvelope
-> TypedExecutionPlanningInput
-> ExecutionDecisionSummary
-> ExecutionSubmitPlan
```

For `MODE=live`, `LIVE_DRY_RUN=false`, and `LIVE_REAL_ORDER_ARMED=true`, live
real-order startup and submit require `EXECUTION_ENGINE=target_delta`. General
`lot_native` promotion submit is not live-real-order eligible. Strategy
`execution_intent` remains a non-authoritative hint for traceability only and
must not size live submitted quantity, notional, target exposure, or delta.

The live service consumes only typed `ExecutionDecisionSummary` and typed
`ExecutionSubmitPlan` for live real-order submission. The final broker-facing
dict must be produced by `ExecutionSubmitPlan.as_final_payload()`, which adds:

- `schema_version`
- `authority_label`
- `submit_plan_hash`
- `content_hash`
- `source`
- `authority`
- `pre_submit_proof_status`
- `block_reason`
- `submit_expected`
- `idempotency_key`

The broker rejects submit-plan dicts that do not validate as this final typed
serialization.

## Submit Authority Matrix

Submit authority is mode-aware. The policy considers `MODE`, `LIVE_DRY_RUN`,
`LIVE_REAL_ORDER_ARMED`, `EXECUTION_ENGINE`, plan kind, source, authority, side,
`submit_expected`, and `pre_submit_proof_status`.

- Live real-order target-delta: allows only `target_submit_plan` with
  `source=target_delta` and `authority=canonical_target_delta_sizing` or
  another explicit target-delta authority accepted by policy.
- Live real-order residual close: allowed only as the explicit
  `residual_inventory_policy` exception for SELL, with proof passed and all
  residual safety gates satisfied.
- Live real-order rejects `buy_submit_plan(source=strategy_position)`,
  `configured_strategy_order_size`, `strategy_execution_intent`, and
  `research_compatibility_execution_intent`.
- Live dry-run may build and observe target-delta plans, but
  `LiveSignalExecutionService` does not invoke the live executor.
- Paper and research compatibility are separate policy modes and do not imply
  live real-order eligibility.

## Non-Authoritative Dicts

These fields are compatibility and observability surfaces only:

- `decision_context`
- `observability_context`
- `observability_payload`
- persistence context from `DecisionEnvelope`
- `execution_decision` inside persisted context

They may record typed summaries and hashes for audit and replay, but they must
not override typed `ExecutionDecisionSummary` or typed `ExecutionSubmitPlan`.

## Compatibility Surfaces

`StrategyDecision` and the dict-like helpers on `ExecutionSubmitPlan` are legacy
compatibility surfaces for diagnostic, research, paper, or older caller paths.
New production-bound strategies must produce `StrategyDecisionV2` through a
`StrategyPolicy` or `RuntimeDecisionAdapter`.

## Forbidden Live Real-Order Paths

Live real orders must not be submitted from:

- forged `decision_context["execution_decision"]`
- forged observability payloads
- dict-only target, residual, or buy submit plans
- direct production imports of `broker.live.live_execute_signal`
- raw broker calls that bypass `LiveSignalExecutionService`

The approved bridge is:

```text
engine.run_loop
-> SignalExecutionRequest
-> LiveSignalExecutionService
-> ExecutionSubmitPlan.as_final_payload()
-> broker/live.py
```

Missing or invalid typed authority fails closed with `[ORDER_SKIP]` logging.

## Audit Fields

Runtime planning and persistence artifacts carry enough hashes to reconstruct:

```text
strategy decisions
-> StrategyPreference
-> PortfolioAllocator decision
-> PortfolioTarget
-> previous/current exposure
-> target delta
-> exchange order rules
-> risk decision marker
-> final ExecutionSubmitPlan
-> submitted qty/notional
```

Relevant fields include `portfolio_target_hash`, `allocation_decision_hash`,
`strategy_contribution_hash`, `submit_plan_hash`,
`execution_submit_plan_hash`, `submit_authority_mode`,
`submit_authority_policy_hash`, and `risk_decision_hash`. The runtime
strategy-set manifest also records `submit_authority_mode` and
`submit_authority_policy_hash` so a run-start artifact binds the submit policy
used by later execution plans.
