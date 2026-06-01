# Portfolio Allocation Authority

Strategy output is not execution authority.

The supported production boundary is a multi-strategy / single-pair runtime:
multiple active strategy instances may decide on the same closed candle for one
runtime pair, and deterministic priority allocation produces exactly one
authoritative `PortfolioTarget` for that pair.

The runtime authority chain is:

```text
Runtime strategy set
  -> persisted RuntimeStrategySetManifest
  -> one RuntimeDecisionAdapter execution per active strategy on the same closed candle
  -> one StrategyDecisionV2 per active strategy
  -> RuntimeStrategyDecisionResultBundle linked to the manifest
  -> one StrategyPreference per strategy decision
  -> SignalAggregator
  -> PortfolioAllocator
  -> authoritative PortfolioTarget linked through the allocation decision
  -> risk / readiness / target-delta planning
  -> ExecutionSubmitPlan linked to the same manifest
  -> execution service
```

`StrategyDecisionV2.execution_intent` is a non-authoritative strategy hint. It may be serialized for reproducibility and diagnostics, but it does not decide final portfolio exposure, final order size, conflict resolution, or live submit eligibility.

## Contracts

- `StrategyPreference` records a strategy's typed preference: signal direction, desired exposure or weight hints, confidence, horizon, exposure cap, reason, policy hashes, position snapshot hash, and non-authoritative execution intent hint.
- `SignalAggregator` validates typed strategy preferences and creates a deterministic preference set.
- `PortfolioAllocator` converts one or more preferences into one authoritative `PortfolioTarget` for the runtime pair. The allocator remains portfolio-shaped for future extension, but production execution is single-pair only.
- `PortfolioTarget` carries allocator policy, allocator config hash, strategy contribution hash, allocation input hash, final target hash, conflict metadata, authoritativeness, and fail-closed reason.
- `ExecutionSubmitPlan` remains the final execution authority.

Strategy modules and runtime adapters must not create live orders or authoritative submit plans. Their `final_signal` and `execution_intent` fields are strategy preferences only. `StrategyDecisionV2.execution_intent` is preserved as a non-authoritative hint for diagnostics and reproducibility.

## Runtime Strategy Set

The runtime strategy set is resolved before decision collection.

Configuration contract:

- If `RUNTIME_STRATEGY_SET_JSON` is set to structured object form, it must include `market_scope` and a `strategies` list. The object form is the production contract.
- `market_scope.mode` must be `single_pair` for production. The reserved future mode `multi_pair_portfolio` is an explicit unsupported concept and fails closed with `multi_pair_runtime_unsupported`.
- `market_scope.pair` and `market_scope.interval` must match the runtime `PAIR` and `INTERVAL`. Multi-pair production runtime is not supported until pair-specific target state, pair-specific runtime data preflight, pair-scoped strategy decision bundles or bundle partitioning, pair-specific allocation targets, pair-specific execution plans, pair-specific submit/reconcile loops, and cross-pair risk budget semantics are implemented.
- Each strategy object supports `strategy_name` or `name`, `enabled`, `pair`, `interval`, `parameters`, `runtime_adapter_config`, `approved_profile_path`, `approved_profile_hash`, `priority`, `weight`, `desired_exposure_krw`, `max_target_exposure_krw`, and legacy alias `risk_budget_krw`.
- Legacy list-form `RUNTIME_STRATEGY_SET_JSON` remains paper/dev compatibility only. Live-like modes require object form with `market_scope`.
- If `RUNTIME_STRATEGY_SET_JSON` is unset and `ACTIVE_STRATEGIES` is set, `ACTIVE_STRATEGIES` is parsed only as a compatibility/diagnostic strategy-name list and all other fields use safe defaults. It does not carry per-instance parameters, approved profiles, priority, weight, or risk authority. In `MODE=live`, multiple `ACTIVE_STRATEGIES` fail closed unless a structured runtime strategy-set contract is provided.
- If neither multi-strategy variable is set, the resolver returns exactly one enabled strategy from `STRATEGY_NAME`.
- `pair` defaults to `settings.PAIR`, `priority` defaults to `100`, `weight` defaults to `1.0`, and desired exposure defaults to `TARGET_EXPOSURE_KRW` when set or `MAX_ORDER_KRW`.
- The current run loop is explicitly single-pair. Every active strategy spec must use `settings.PAIR`; pair mismatches fail during startup validation with `multi_pair_runtime_unsupported` before adapter execution in paper, live dry-run, and live real-order paths.
- Operators can validate and inspect the materialized active set without placing orders with `uv run bithumb-bot runtime-strategy-set-lint` and `uv run bithumb-bot runtime-strategy-set-dump`.

Strict runtime parameter authority is limited to `approved_profile` and `runtime_strategy_spec`. `STRATEGY_PARAMETERS_JSON` and plugin `runtime_parameter_adapter.from_settings()` are compatibility fallbacks only and are surfaced as `paper_legacy_compat`; they are rejected for live/live-like or profile-bound runtime.

In live multi-strategy runtime, every active strategy instance must carry its own `approved_profile_path` and `approved_profile_hash`. A global approved-profile selector is allowed only for the single-strategy case and is rejected for live multi-strategy mode.

The collector executes every active strategy's registered `RuntimeDecisionAdapter` for the same closed candle timestamp. Live/promotion-grade execution requires typed `RuntimeStrategyDecisionResult` values containing `StrategyDecisionV2`. Missing adapters, legacy dict-only handoffs, invalid typed results, or mixed candle timestamps fail closed instead of continuing with a partial strategy set.

## Performance Gate Scope

For live real-order target-delta planning, performance gate authority follows
allocator authority. The planner evaluates only allocator-selected BUY/SELL
strategy contributions, keyed by `strategy_instance_id`, `strategy_name`, and
`pair`; allocator-unselected strategies do not block submit. HOLD-only selected
allocations are not blocked by unrelated BUY/SELL strategy history.

All selected BUY/SELL contributions must pass. A selected contribution failure
fails closed with `selected_strategy_performance_gate_blocked` and records
`performance_gate_scope`, `performance_gate_policy`,
`per_strategy_gate_results`, and `blocking_strategy_instance_ids` in planning
context.

## Single Strategy

Single-strategy runtime is the degenerate multi-strategy case. The selected strategy still produces `StrategyDecisionV2`, but the run loop adapts it to `StrategyPreference`, aggregates it, allocates a `PortfolioTarget`, and only then invokes execution planning.

For the initial deterministic allocator policy:

- `BUY` targets configured target exposure.
- `SELL` targets zero exposure.
- `HOLD` maintains the previous persisted target exposure when available.
- `HOLD` without previous target exposure fails closed.
- Equal-priority `BUY` plus `HOLD` selects `BUY`.
- Equal-priority `SELL` plus `HOLD` selects `SELL`.
- Equal-priority `BUY` plus `SELL`, including `BUY` plus `SELL` plus `HOLD`, fails closed.
- Higher-priority strategies win over lower-priority conflicting strategies; lower numeric priority is higher authority.
- BUY target exposure is the weighted average of selected BUY strategy desired exposures. If any selected BUY contribution carries `max_target_exposure_krw`, the result is capped by the sum of selected BUY exposure caps.
- `risk_budget_krw` remains a backward-compatible alias for `max_target_exposure_krw`; it is not a true maximum-loss budget. Allocation payloads declare `risk_budget_semantics=max_target_exposure_cap` and record pre-cap target exposure, cap amount, whether the cap applied, and cap source.

## Runtime Manifest

At persistence time the materialized runtime strategy-set manifest is stored in SQLite as recovery-critical `trades` data. The manifest includes active strategy instances, raw and materialized parameters, normalized parameter source, strategy parameter hash, approved profile path/hash, runtime and plugin contract hashes, strategy version, execution and risk config hashes, market scope, single-pair enforcement, and the manifest hash.

Run-start manifests are candle-independent blueprints. Runtime data preflight is
decision-cycle evidence and is linked from decision bundles through
`runtime_data_availability_report_hash`, `feature_snapshot_hash`, and related
runtime-data contract hashes.

The persisted chain is:

```text
runtime_strategy_set_manifest
  -> runtime_strategy_decision_bundle
  -> portfolio_allocation_decision
  -> execution_plan
  -> execution_submit_plan
```

Allocation and execution plan rows carry the same manifest id/hash. Compatibility projections in `strategy_decisions` remain non-authoritative; replay should use the manifest-to-plan chain.

## Multi Strategy Conflicts

The initial policy supports deterministic priority allocation. Strategies default to equal priority. If equal-priority top strategies conflict between `BUY` and `SELL`, allocation fails closed instead of guessing.

Conflict metadata is included in the allocation decision, target, logs, and decision context:

- selected priority
- selected strategies
- selected signals
- conflict count
- primary block reason
- allocator policy and version
- allocator config hash
- strategy contribution hash
- allocation decision hash
- portfolio target hash and authoritativeness

In multi-strategy mode, the persisted runtime context may include a representative typed strategy decision for compatibility and observability. That representative is labeled non-authoritative; the allocator-derived `PortfolioTarget` remains the execution-planning authority.

## Fail Closed

Target-delta execution planning blocks when allocator authority is missing or malformed:

- missing strategy preference
- missing portfolio allocation
- allocation target count other than one in the single-pair runtime, reported as `single_pair_allocation_target_count_mismatch`
- allocation target pair not matching the runtime pair, reported as `single_pair_allocation_target_pair_mismatch`
- non-authoritative portfolio target
- missing or inconsistent portfolio target hash
- missing allocator input hash
- missing strategy contribution hash
- legacy dict/context-only live real-order path

Observability dictionaries remain non-authoritative. Live real-order submission still requires typed execution summary and typed `ExecutionSubmitPlan`, and target-delta live submission additionally requires authoritative portfolio target metadata on the typed plan.
