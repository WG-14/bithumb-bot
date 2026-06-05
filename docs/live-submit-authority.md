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
serialization. Schema validation proves only shape, final serialization, and
content hash integrity. It is not the authority admission policy.

## Submit Authority Matrix

Submit authority is mode-aware. The policy considers `MODE`, `LIVE_DRY_RUN`,
`LIVE_REAL_ORDER_ARMED`, `EXECUTION_ENGINE`, plan kind, source, authority, side,
`submit_expected`, and `pre_submit_proof_status`.

- Live real-order target-delta: allows only `target_submit_plan` with
  `source=target_delta` and `authority=canonical_target_delta_sizing` or
  another explicit target-delta authority accepted by policy. It must also have
  `submit_expected=true`, `pre_submit_proof_status=passed`,
  `portfolio_target_authoritative=true`, `portfolio_target_hash`,
  `allocation_decision_hash`, and `strategy_contribution_hash`.
- Live real-order residual close: allowed only as the explicit
  `residual_inventory_policy` exception for SELL, with proof passed and all
  residual safety gates satisfied. The plan must have
  `source=residual_inventory`, `authority=residual_inventory_policy`,
  `side=SELL`, `submit_expected=true`, `pre_submit_proof_status=passed`, final
  payload validation, `RESIDUAL_LIVE_SELL_MODE=enabled`, and normal live
  real-order arming.
- Live real-order rejects `buy_submit_plan(source=strategy_position)`,
  `configured_strategy_order_size`, `strategy_execution_intent`, and
  `research_compatibility_execution_intent`.
- Live dry-run may build and observe target-delta plans, but
  `LiveSignalExecutionService` does not invoke the live executor.
- Paper and research compatibility are separate policy modes and do not imply
  live real-order eligibility.

`SubmitAuthorityPolicy` is the single mode-aware admission matrix. Both
`LiveSignalExecutionService` and `broker/live.py` consult it before creating or
passing any submit intent. This means a direct lower-boundary call to
`broker.live.live_execute_signal()` cannot make `lot_native`,
`strategy_position`, configured-size, or strategy `execution_intent` BUY sizing
live-real-order authoritative.

## Non-Authoritative Dicts

These fields are compatibility and observability surfaces only:

- `decision_context`
- `observability_context`
- `observability_payload`
- persistence context from `DecisionEnvelope`
- `execution_decision` inside persisted context

They may record typed summaries and hashes for audit and replay, but they must
not override typed `ExecutionDecisionSummary` or typed `ExecutionSubmitPlan`.
`execution_intent` may remain in these surfaces only as
`non_authoritative_strategy_hint` or observability material. It must not change
`qty`, `notional_krw`, `target_exposure_krw`, or `delta_krw` in live real-order
promotion and broker submission paths.

## Compatibility Surfaces

`StrategyDecision` and the dict-like helpers on `ExecutionSubmitPlan` are legacy
compatibility surfaces for diagnostic, research, paper, or older caller paths.
New production-bound strategies must produce `StrategyDecisionV2` through a
`StrategyPolicy` or `RuntimeDecisionAdapter`.

`EXECUTION_ENGINE=lot_native` is not live-real-order compatible. Any retained
legacy `lot_native` BUY submit-plan parsing is isolated to paper, research, or
diagnostic compatibility and remains auditable through
`legacy_lot_native_compat_enabled` plus the allowed source/authority lists in
the submit authority policy. New live-eligible strategies must use the
target-delta allocator path even when there is only one strategy instance.

## Risk And Exposure Semantics

`risk_budget_krw` is not an exposure cap and is not a live loss-budget engine.
The exposure-cap concept is `max_target_exposure_krw`. Runtime strategy
manifests, strategy preferences, allocation decisions, portfolio targets, and
submit plans emit a `risk_decision` artifact and `risk_decision_hash` recording:

- `risk_budget_interpreted_as_exposure_cap=false`
- `loss_budget_supported=false`
- `loss_budget_authority=unsupported_fail_closed`
- `exposure_cap_source=max_target_exposure_krw` when an exposure cap applies

The deprecated marker
`deprecated:risk_budget_krw_not_enforced_as_loss_budget` may still appear as
`risk_budget_legacy_marker` for compatibility, but it is no longer the only
runtime evidence field.

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
`submit_authority_policy_hash`, `allowed_submit_plan_sources`,
`allowed_submit_plan_authorities`,
`legacy_lot_native_compat_enabled`, `risk_decision`, and
`risk_decision_hash`. The runtime strategy-set manifest records the same submit
policy mode, allowed sources/authorities, legacy compatibility flag, and policy
hash so a run-start artifact binds the submit policy used by later execution
plans.
