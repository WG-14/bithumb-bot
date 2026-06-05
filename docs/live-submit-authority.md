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

`risk_budget_krw` is deprecated compatibility metadata only. It is not an
exposure cap, loss budget, sizing input, submit eligibility input, or
operational risk-policy input. The exposure-cap concept is
`max_target_exposure_krw`.

Exposure-cap metadata is recorded as an `exposure_boundary_artifact` with
`exposure_boundary_artifact_hash`. Production-facing runtime, allocation,
portfolio-target, execution-plan, and manifest artifacts do not publish
exposure-cap metadata as generic `risk_decision` or `risk_decision_hash`.
If compatibility copies are retained, they are named
`legacy_non_authoritative_exposure_risk_decision` and
`legacy_non_authoritative_exposure_risk_decision_hash`; live-real-order
admission must not consume them. Typed operational risk decisions use
layer-specific fields such as `strategy_risk_decision_hash`,
`strategy_risk_evidence_hash`, `portfolio_risk_decision_hash`,
`portfolio_risk_evidence_hash`, `pre_submit_risk_decision_hash`, and
`pre_submit_risk_evidence_hash`. The evidence hash is the canonical hash of the
decision evidence payload. The decision hash binds policy hash, input hash,
evidence hash, state source, status, reason code, and outcome.

Exposure-boundary artifacts record:

- `risk_budget_interpreted_as_exposure_cap=false`
- `exposure_cap_source=max_target_exposure_krw` when an exposure cap applies

The runtime risk authority chain is three-layered:

```text
StrategyRiskProfile
-> StrategyRiskStateProvider(strategy_instance_id, pair, interval, as_of_ts_ms)
-> StrategyRiskDecision
-> PortfolioAllocator / PortfolioTarget
-> PortfolioRiskDecision
-> ExecutionSubmitPlan
-> PreSubmitRiskDecision
-> SubmitAuthorityPolicy / live broker submission
```

Strategy-level loss/order/drawdown/cooldown policy is represented separately
from exposure caps by the typed risk policy fields `max_daily_loss_krw`,
`max_daily_order_count`, `max_trade_count_per_day`, `max_drawdown_pct`, and
`cooldown_after_loss_min`. Runtime strategy risk state is strategy-instance
scoped where enforced policy uses it; if the live path cannot derive a required
strategy-instance state field reliably, it fails closed with explicit
missing-state evidence. A selected strategy risk-policy violation blocks
authoritative `PortfolioTarget` adoption before a submittable target-delta plan
can be created, and the blocking strategy risk decision hash is carried in
allocation and execution context.
Research/non-live missing risk policy is not silent: runtime materialization
creates an explicit disabled telemetry `StrategyRiskProfile` with
`policy_status=disabled_explicit`, `missing_policy=disabled_explicit`,
`risk_enforcement_mode=telemetry`, and
`risk_profile_source=research_missing_policy_explicit`.

After the authoritative `PortfolioTarget` is created, a separate
`PortfolioRiskDecision` records `portfolio_risk_decision_hash`,
`portfolio_risk_policy_hash`, `portfolio_risk_input_hash`,
`portfolio_risk_evidence_hash`, `portfolio_risk_state_source`, effective
limits, and replayable evidence. A non-ALLOW portfolio risk decision prevents
submittable target-delta planning.

Immediately before live broker submission,
`RuntimeRiskEngineAdapter.evaluate_pre_submit()` evaluates the stable
`ExecutionSubmitPlan` hash. Live-real-order submission requires:

- `pre_submit_risk_status=ALLOW`
- `pre_submit_risk_decision_hash`
- `pre_submit_risk_policy_hash`
- `pre_submit_risk_input_hash`
- `pre_submit_risk_evidence_hash`
- `pre_submit_risk_plan_hash`
- `pre_submit_risk_reason_code`
- `pre_submit_risk_state_source`

`pre_submit_risk_plan_hash` must equal the stable
`ExecutionSubmitPlan.submit_plan_hash` evaluated by the risk engine. The final
broker submission path validates this proof after runtime DB/broker state is
available and before placing the order. After the proof is attached, the final
broker-bound submit payload is written back to
`execution_plan.execution_submit_plan_json` for the matching stable
`execution_submit_plan_hash`. If a post-proof approval check blocks submission,
the same column records the proof fields plus
`final_submit_payload_persistence_status=post_proof_submit_skipped` and a skip
reason, so replay can distinguish "broker submission not reached" from missing
evidence.

Operators can replay persisted risk-layer hashes without broker access:

```bash
uv run bithumb-bot risk-layer-replay --db <runtime.sqlite> --decision-id <id> --json
uv run bithumb-bot risk-layer-replay --db <runtime.sqlite> --execution-plan-id <id> --json
```

The verifier opens SQLite read-only, never submits orders, never calls live
broker APIs, and reports explicit pass/fail/not-applicable status for strategy,
portfolio, and pre-submit risk layers. Each layer separates
`stored_payload_integrity_status` from `source_reconstruction_status` and
`final_layer_status`; a stored decision dict hashing to itself is payload
integrity evidence, not by itself proof that the runtime source state can be
reconstructed.

Hash order is deterministic:

1. `ExecutionSubmitPlan.content_hash()` hashes the typed submit plan fields and
   extra payload, excluding `content_hash`, `submit_plan_hash`, and
   `pre_submit_risk_*` proof fields so the proof can bind to the evaluated plan
   without changing that plan hash.
2. `ExecutionSubmitPlan.as_final_payload()` sets `submit_plan_hash` to that
   stable content hash, then adds schema/version authority fields and computes
   the final payload `content_hash` over the final serialization while still
   excluding `content_hash` and `submit_plan_hash`.
3. `RuntimeRiskEngineAdapter.evaluate_pre_submit()` receives the stable
   `submit_plan_hash` in `SubmitPlan.evidence`.
4. The live lower boundary requires `pre_submit_risk_plan_hash` to match the
   stable `submit_plan_hash` before any real order is placed.
5. The final broker-bound payload, including all `pre_submit_risk_*` proof
   fields, is persisted in `execution_plan.execution_submit_plan_json` and can
   be inspected by `risk-layer-replay --execution-plan-id <id>`.

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
-> exposure-boundary artifact and typed risk-layer decisions
-> final ExecutionSubmitPlan
-> submitted qty/notional
```

Relevant fields include `portfolio_target_hash`, `allocation_decision_hash`,
`strategy_contribution_hash`, `submit_plan_hash`,
`execution_submit_plan_hash`, `submit_authority_mode`,
`submit_authority_policy_hash`, `allowed_submit_plan_sources`,
`allowed_submit_plan_authorities`,
`legacy_lot_native_compat_enabled`, `exposure_boundary_artifact_hash`,
`strategy_risk_decision_hash`, `portfolio_risk_decision_hash`, and
`pre_submit_risk_decision_hash`. The runtime strategy-set manifest records the
same submit policy mode, allowed sources/authorities, legacy compatibility
flag, and policy hash so a run-start artifact binds the submit policy used by
later execution plans.

To trace a live decision, start with `strategy_decisions.id` or
`execution_plan.id`, then inspect:

- strategy source: `strategy_preferences[*].strategy_risk_profile`,
  `strategy_risk_decision`, and the `strategy_risk_*` hashes
- portfolio source: `portfolio_target.target_json.portfolio_risk_decision` and
  the `portfolio_risk_*` hashes
- pre-submit source: `execution_plan.execution_submit_plan_json` and the
  `pre_submit_risk_*` hashes, especially `pre_submit_risk_plan_hash`
- blocking layer: the first layer whose status is not `ALLOW`, or whose replay
  `final_layer_status` is `fail`
