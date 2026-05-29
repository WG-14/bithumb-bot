# Strategy Plugin Authoring

Strategy authoring has two distinct paths:

1. fast research-only strategies for experiments and backtests
2. promotion-grade extensions for runtime replay, approved profiles, live dry-run, and live real-order eligibility

Live safety is not weakened by the research-only API. A strategy without a promotion extension fails closed for promotion export, runtime replay, live dry-run, and live real-order.

Do not register promotion-grade runtime strategies in `bithumb_bot.strategy.registry`. That module is compatibility-only for smoke strategy policies and legacy DB-bound construction.

## Fast Research Path

Use `bithumb_bot.strategy_authoring.ResearchOnlyStrategyPlugin` or one of its helpers:

- `research_plugin_from_decide_snapshot()`
- `research_plugin_from_event_builder()`

A research-only strategy should provide only:

- `strategy_name`
- `version`
- a plugin-local `StrategySpec`
- `required_data` and optional data
- either a snapshot decision function or a research event builder
- a diagnostics namespace, or the default strategy name

Research-only authors must not declare runtime/live vocabulary such as `StrategyRuntimeCapabilities`, runtime replay builders, runtime parameter adapters, approved-profile requirements, live dry-run eligibility, or live real-order eligibility. The authoring adapter normalizes research-only plugins into the internal registry with `promotion_extension_missing`.

Research-only plugins can run through the generic research/backtest pipeline and emit reproducibility evidence, including strategy spec hash, dataset hash, deterministic decision hashes, `promotion_grade=false`, `promotion_extension_missing_reason=promotion_extension_missing`, and `recommended_next_action=promote_strategy_contract`.

Research-only plugins are not promotion evidence. If promotion is attempted before adding a promotion extension, gates must fail closed with stable reason codes such as:

- `promotion_extension_missing`
- `promotion_runtime_unsupported_for_strategy`
- `runtime_replay_unsupported_for_strategy`
- `live_dry_run_not_allowed_for_strategy`
- `live_real_order_not_allowed_for_strategy`

`threshold_research_only` is the minimal built-in template for this path. It demonstrates that a strategy can be added without runtime replay, live, approved-profile, or adapter boilerplate. `canary_non_sma` is not the minimal template; it remains a promotion-grade architecture proof.

## Promotion-Grade Path

Use `bithumb_bot.strategy_authoring.PromotionGradeStrategyExtension` for strategies that opt into runtime/promotion/live contracts. The extension owns the heavy requirements:

- runtime replay builder
- runtime parameter adapter
- runtime decision adapter factory
- policy assembly factory
- export normalizer or equivalence exporter when needed
- approved-profile requirement
- runtime capability declaration
- live dry-run eligibility
- live real-order eligibility
- fail-closed reason

Promotion-grade strategies are normalized into `ResearchStrategyPlugin` for the existing registry, contract hashing, runtime replay, approved profile verification, and live preflight gates. Runtime capability is explicit and must not be inferred from adapter presence.

Runtime fail-safe strategies such as `safe_hold` are outside the research parity target. They may declare typed runtime decision support and policy assembly for fail-closed runtime fallback behavior, but they must remain `research_runnable=false`, have no `research_event_builder`, reject research execution explicitly, and remain ineligible for live real orders unless a separate reviewed promotion contract changes that.

Promotion-bound strategies must preserve existing evidence:

- plugin contract hash
- runtime decision request hash
- replay fingerprint hash
- approved profile hash
- runtime contract hash
- policy hash fields

Production-bound manifests still fail closed when runtime-bound behavior parameters, replay support, runtime adapters, policy assembly, approved-profile evidence, or decision equivalence evidence are missing.

## Required Architecture

The supported research architecture is:

`StrategySpec` -> plugin authoring object -> normalized `ResearchStrategyPlugin` -> plugin-owned `research_event_builder` -> `research.backtest_runner.run_plugin_backtest` -> strategy-neutral `research.backtest_kernel` -> runtime replay, promotion, and live capability gates.

`research.backtest_runner` is generic and strategy-neutral. It may call explicit plugin hooks such as `research_parameter_materializer` and `research_event_builder`, but it must not branch on strategy names or own strategy-specific defaults. Strategy-specific research materialization, exploratory legacy behavior, empty-event policy, event generation, diagnostics, and payload adaptation belong in plugin-owned modules.

`research.strategy_registry` owns normalized contract dataclasses, validation, registration, discovery, listing, resolving, and test reload behavior only. Built-in plugins are loaded through `bithumb_bot.strategy_plugins.iter_builtin_strategy_plugins()` using lazy imports.

## StrategySpec Ownership

New strategies should define `StrategySpec` in the plugin module that owns the strategy. This keeps new strategy PRs from modifying common research/runtime engine files.

`research/strategy_spec.py` still contains common dataclasses, validation helpers, compatibility helpers, and historical built-in specs. It is no longer the required central edit point for every new strategy. Existing centralized specs remain for backward compatibility unless a focused migration safely moves them into plugin-local modules.

Architecture guard tests should continue to prevent new strategy-specific branches from entering common research files such as `backtest_runner`, `backtest_kernel`, `backtest_engine`, `backtest_support`, and `strategy_registry`.

## Tests

Research-only strategy tests should prove:

- registration and discovery
- research/backtest execution through the generic runner
- deterministic reproducibility fields and `promotion_grade=false`
- fail-closed promotion/runtime/live behavior
- no runtime/live/promotion boilerplate in the public authoring path

Promotion-grade strategy tests should prove:

- explicit runtime parameter adapter
- runtime decision adapter factory
- replay support when required
- policy assembly
- approved-profile binding
- live dry-run and live real-order capability behavior
- preserved decision, replay, runtime, policy, and profile hashes

New strategy PRs should normally modify one plugin file and one focused test file. They should not add strategy-specific branches to common research or runtime gateway files.

## Compatibility

`ResearchStrategyPlugin` remains the normalized internal registry representation. Existing code may still inspect it for contract hashes, runtime capability validation, profile verification, and live preflight. Public research-only authoring should use `strategy_authoring` instead of hand-writing a broad `ResearchStrategyPlugin`.

`strategy.registry` is legacy/smoke compatibility only. `research.backtest_engine`, `research.backtest_loop`, and compatibility re-exports from `research.backtest_kernel` are compatibility-only for old import paths and must not regain strategy, risk, execution, or ledger authority.
