# Strategy Plugin Authoring

Strategy authoring has three public levels:

1. Level 1: `level_1_research_only` strategies for experiments and backtests
2. Level 2: `level_2_replay_compatible` strategies that can prove deterministic read-only replay but are not live eligible
3. Level 3: `level_3_promotion_grade` strategies with runtime adapters, approved-profile binding, and explicit execution capability gates

Live safety is not weakened by the research-only API. A strategy without a promotion extension fails closed for promotion export, runtime replay, live dry-run, and live real-order.
Level 3 authoring does not mean unrestricted live dry-run or real-order eligibility. Live dry-run and real-order authority are separate operational capability fields and remain blocked unless the strategy contract explicitly allows them and the runtime supplies the required approved profile, evidence, and scope.

Do not register promotion-grade runtime strategies in `bithumb_bot.strategy.registry`. That module is compatibility-only for smoke strategy policies and legacy DB-bound construction.

## Which Level

Use this decision tree:

- Choose Level 1 when the strategy is exploratory and only needs research/backtest events.
- Choose Level 2 when the strategy needs deterministic replay exports or replay comparison, but must not run in live dry-run or live real-order mode.
- Choose Level 3 when the strategy needs promotion-grade runtime decisions. Live dry-run and live real-order eligibility are separate capability declarations and may remain blocked even for a Level 3 strategy.

New strategy PRs should normally be small. Level 1 usually needs one plugin file and one focused test file. Level 2 usually adds only the replay-compatible plugin file plus one focused replay contract test. Level 3 adds the explicit runtime adapter/policy assembly surface and focused live/promotion contract tests.

Public contract helpers are available from `bithumb_bot.strategy_contract_testing`:

- `assert_research_only_contract(plugin)` for Level 1
- `assert_replay_compatible_contract(plugin, dataset, params, tmp_path)` for Level 2
- `assert_live_eligible_contract(plugin, tmp_path, params, pair, interval)` for Level 3

The default fast PR guard checks this document and the pull request template on
all runs. On GitHub pull_request runs it also evaluates the actual changed files
against the pull request title/body evidence. Local focused verification can
exercise the same diff-aware path explicitly:

```bash
uv run python scripts/check_strategy_pr_workload_guard.py \
  --require-diff-aware \
  --changed-file src/bithumb_bot/strategy_plugins/example.py \
  --evidence-file /path/to/pr-evidence.md
```

When no PR metadata or explicit changed-file/evidence arguments are available,
the guard reports that only the diff-aware portion was skipped; that output is
not full diff-aware validation.

| Level | Required hooks | Forbidden vocabulary | Stable fail-closed reasons | Expected files | Test helper |
| --- | --- | --- | --- | --- | --- |
| Level 1 research-only | `StrategySpec`, `research_event_builder` or `decide_snapshot` | runtime adapter, approved profile, live dry-run, live real-order, promotion extension | `promotion_extension_missing`, `promotion_runtime_unsupported_for_strategy`, `runtime_replay_unsupported_for_strategy`, `live_dry_run_not_allowed_for_strategy` | one plugin file, built-in manifest entry or external entry point, and one focused test file | `assert_research_only_contract` |
| Level 2 replay-compatible | Level 1 hooks plus parameter schema, deterministic policy material, replay fingerprint material, read-only replay builder | `Settings` fields, `runtime_parameter_adapter.from_settings()`, approved profile requirement, live dry-run, live real-order | `replay_compatible_not_live_eligible`, `promotion_runtime_unsupported_for_strategy`, `runtime_decision_adapter_unsupported_for_strategy`, `live_real_order_not_allowed_for_strategy` | one replay plugin file, built-in manifest entry or external entry point, and one focused replay contract test | `assert_replay_compatible_contract` |
| Level 3 promotion-grade | Level 1 hooks plus runtime decision adapter, policy assembly, approved-profile binding, execution intent contract, replay support, live capability declaration | direct `ResearchStrategyPlugin(...)` assembly in new strategy modules, strategy-specific common-engine branches, production legacy parameter fallback | strategy-specific capability reason, `approved_profile_required_for_strategy`, decision-equivalence/runtime-contract/profile validation reasons | plugin file, built-in manifest entry or external entry point, plus focused promotion/runtime contract tests | `assert_live_eligible_contract` |

## Registration Paths

Built-in in-repo plugins and external plugins use different registration
contracts.

Built-in plugins live under `src/bithumb_bot/strategy_plugins/` and must be
registered in the explicit built-in manifest at
`src/bithumb_bot/strategy_plugins/builtin_manifest.py`. The manifest stores
deterministic `module:object` references and
`iter_builtin_strategy_plugins()` loads from that manifest with lazy imports.
This is intentionally not package-wide auto-scanning; helper modules,
experimental modules, and test-only modules must not become runtime-discoverable
by accident.

Built-in manifest entries may point to one public plugin object, a callable that
returns plugin objects, or an iterable export such as `STRATEGY_PLUGINS`. The
loader applies the same coercion semantics as external entry-point discovery:
public Level 1/2/3 authoring objects are normalized into
`ResearchStrategyPlugin`, and every resulting plugin is registered
individually. The manifest entry is still required, even for `STRATEGY_PLUGINS`,
so in-repo discovery remains explicit and reviewable.

External packages must not edit the built-in manifest. They register through the
`bithumb_bot.strategy_plugins` entry-point group in their package metadata.

Any public in-repo plugin export such as `*_PLUGIN`, `STRATEGY_PLUGIN`, or
`STRATEGY_PLUGINS` must either appear in the built-in manifest or be explicitly
allowlisted in the focused discovery guard test with a clear reason. A new
built-in strategy is not complete until it appears in
`list_research_strategy_plugins()` and can be resolved with
`resolve_research_strategy_plugin()`.

Operators and reviewers can inspect the read-only discovery surface without a
trading DB, broker credentials, order submission, or runtime artifact writes:

```bash
uv run bithumb-bot strategy-plugin-inventory --json
```

Operators can also request one target-specific verdict without combining nested
inventory fields:

```bash
uv run bithumb-bot strategy-plugin-validate --strategy <name> --target <target> --json
```

Supported targets are `research_backtest`, `runtime_replay`,
`runtime_decision`, `live_dry_run`, and `live_real_order`. The command is
read-only: it does not open the trading DB, use broker or network APIs, submit
orders, or write artifacts. It emits deterministic JSON with `allowed`,
`blocking_reasons`, `next_required_action`, `required_evidence`, and the current
supported runtime scope. Use this as the final static operator path before
separate runtime/live preflight evidence.

The strategy plugin inventory emits deterministic JSON sorted by strategy name.
Each entry includes source attribution, built-in manifest object path when
available, canonical authoring level, capability level, operational capability,
operator verdict, runtime scope support, parameter authority, legacy fallback
status, contract hashes, live eligibility, fail-closed reason,
decision-evidence contract hash, and data requirements. Use it to verify that a
strategy is discoverable through
`list_research_strategy_plugins()` / `resolve_research_strategy_plugin()` while
preserving strategy-neutral common execution, risk, data, and runtime core
paths.

The canonical generated contract and inventory payload include these
operator-facing fields:

- `strategy_name`
- `authoring_level`
- `capability_level`
- `runtime_replay_supported`
- `runtime_decision_supported`
- `live_dry_run_allowed`
- `live_real_order_allowed`
- `approved_profile_required`
- `runtime_data_requirements`
- `risk_profile_required`
- `promotion_evidence_required`
- `supported_runtime_scope`
- `fail_closed_reason`
- `next_required_action`

The generated payload also separates `operational_capability` from
`authoring_level`, and reports `operator_verdict.targets` for
`research_backtest`, `runtime_replay`, `runtime_decision`, `live_dry_run`, and
`live_real_order`. Blocked target verdicts include reason codes and a
`next_required_action` such as `promote_strategy_contract`,
`add_live_eligible_contract_for_runtime_or_live`, `supply_approved_profile`, or
`do_not_promote`.

## Level 1: Fast Research Path

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

## Level 2: Replay-Compatible Path

Use `bithumb_bot.strategy_authoring.ReplayCompatibleStrategyPlugin`,
`ReplayCompatibleStrategyExtension`, or
`build_replay_compatible_strategy_plugin()`.

A replay-compatible strategy should provide only:

- research identity and a plugin-local `StrategySpec`
- parameter schema and materialization sufficient for deterministic behavior
- pure policy or policy assembly material
- deterministic decision artifact material
- replay fingerprint material
- a read-only replay strategy or replay decision material

Level 2 must not require Settings fields, approved-profile runtime authority,
live dry-run capability, live real-order capability, production runtime
parameter adapters, or execution intent for real orders. Default runtime
capability is fail-closed for live:

- `research_supported=true`
- `replay_decisions_supported=true`
- `promotion_export_supported=true`
- `runtime_decision_supported=false`
- `approved_profile_required=false`
- `live_dry_run_allowed=false`
- `live_real_order_allowed=false`

`replay_threshold` is the minimal built-in template for this path. It shows pure
threshold policy material, parameter schema validation, centralized replay
fingerprints, and read-only SQLite replay without live eligibility.

## Level 3: Promotion-Grade Path

Use `bithumb_bot.strategy_authoring.PromotionGradeStrategyExtension` with
`build_live_eligible_strategy_plugin()`. The builder normalizes the public
authoring object into the registry representation, so new live-eligible strategy
modules should not hand-write `ResearchStrategyPlugin(...)`.

The extension owns the heavy requirements:

- runtime replay builder
- runtime parameter adapter
- runtime decision adapter factory
- policy assembly factory
- export normalizer or equivalence exporter when needed
- approved-profile requirement
- runtime capability declaration
- live dry-run eligibility when explicitly allowed
- live real-order eligibility when explicitly allowed
- fail-closed reason

Promotion-grade strategies are normalized into `ResearchStrategyPlugin` for the existing registry, contract hashing, runtime replay, approved profile verification, and live preflight gates. Runtime capability is explicit and must not be inferred from adapter presence or from the Level 3 authoring label. The canonical authoring level is `level_3_promotion_grade`; historical wording such as `level_3_live_eligible` is a legacy alias only and is not live authority.

Runtime fail-safe strategies such as `safe_hold` are outside the research parity target. They may declare typed runtime decision support and policy assembly for fail-closed runtime fallback behavior, but they must remain `research_runnable=false`, have no `research_event_builder`, reject research execution explicitly, and remain ineligible for live real orders unless a separate reviewed promotion contract changes that.

Promotion-bound strategies must preserve existing evidence:

- plugin contract hash
- runtime decision request hash
- replay fingerprint hash
- approved profile hash
- runtime contract hash
- policy hash fields

Production-bound manifests still fail closed when runtime-bound behavior parameters, replay support, runtime adapters, policy assembly, approved-profile evidence, or decision equivalence evidence are missing.

Runtime parameter authority is centralized at the runtime strategy boundary. A
promotion-grade strategy must accept parameters from an approved profile or from
`RuntimeStrategySpec.parameters`; `runtime_parameter_adapter.from_settings()` is
paper legacy compatibility only and must not be required for strict runtime operation.
New strategies should not add strategy-specific fields to `Settings`.
`STRATEGY_PARAMETERS_JSON` is the same paper legacy compatibility surface; it is
not production authority for promotion, live dry-run, or live real-order runtime.
These compatibility fallbacks live under the explicit
`bithumb_bot.legacy_compat.runtime_parameters` boundary and may be invoked only
by the central `ParameterAuthorityResolver`.

Production runtime decisions must enter the adapter through
`decide_feature_snapshot(request, feature_snapshot)`. DB-bound methods such as
`decide(conn, ...)` and `decide_database_snapshot(conn, ...)` are compatibility
or diagnostic surfaces only and are forbidden as promotion/live production
decision authority. Adapter-owned feature projectors must also be connection
free: `project_feature_snapshot(request, feature_snapshot)`. A DB-bound
`project_feature_snapshot(conn, request, feature_snapshot)` signature is rejected
as promotion/live authority. Additional DB-backed data must be declared through
runtime data requirements and supplied by the runtime data provider/preflight
snapshot path.

Structured runtime selection uses `RUNTIME_STRATEGY_SET_JSON` with
`market_scope.mode="single_pair"` for the current runtime. Every active strategy
instance must match the configured pair and interval. Multi-pair runtime remains
unsupported until readiness, target state, allocation, execution submit, and
persistence are pair-scoped.
Level 3 eligibility applies only inside the currently supported
multi-strategy / single-pair / single-interval runtime scope. Multi-pair and
multi-interval runtime remain fail-closed and are surfaced in inventory as
unsupported scope.

Use `max_target_exposure_krw` for allocator exposure caps. Historical
`risk_budget_krw` inputs are deprecated non-authoritative metadata and are not
maximum-loss budgets. They do not cap target exposure in live promotion.

At run start, the runtime persists a materialized strategy-set manifest in the
trade DB. It records active instance ids, raw and materialized parameters,
parameter source/audit, approved-profile bindings, plugin/runtime/strategy
hashes, execution and risk config hashes, market scope, exposure-cap semantics,
and deterministic run-start request hashes. Decision bundles, allocation
decisions, and execution plans reference the same manifest hash.

## Required Architecture

The supported research architecture is:

`StrategySpec` -> plugin authoring object -> normalized `ResearchStrategyPlugin` -> plugin-owned `research_event_builder` -> `research.backtest_runner.run_plugin_backtest` -> strategy-neutral `research.backtest_kernel` -> runtime replay, promotion, and live capability gates.

`ResearchStrategyPlugin` is the internal normalized registry representation. It
exists for discovery, contract hashing, capability validation, approved-profile
verification, runtime replay, and live preflight gates. It is not the normal
public authoring API for new strategy modules. Public authoring should use:

- `ResearchOnlyStrategyPlugin` or its helpers for Level 1
- `ReplayCompatibleStrategyPlugin` or `build_replay_compatible_strategy_plugin()` for Level 2
- `PromotionGradeStrategyExtension` with `build_live_eligible_strategy_plugin()` for Level 3

`research.backtest_runner` is generic and strategy-neutral. It may call explicit plugin hooks such as `research_parameter_materializer` and `research_event_builder`, but it must not branch on strategy names or own strategy-specific defaults. Strategy-specific research materialization, exploratory legacy behavior, empty-event policy, event generation, diagnostics, and payload adaptation belong in plugin-owned modules.

`research.strategy_registry` owns normalized contract dataclasses, validation, registration, discovery, listing, resolving, and test reload behavior only. Built-in plugins are declared in `strategy_plugins/builtin_manifest.py` and loaded through `bithumb_bot.strategy_plugins.iter_builtin_strategy_plugins()` using lazy imports.

Existing `sma_with_filter`, `safe_hold`, and baseline direct
`ResearchStrategyPlugin(...)` construction has been narrowed. `sma_with_filter`
and `canary_non_sma` use the public Level 3 builder. `safe_hold` remains an
explicit runtime fail-safe special case outside normal research parity. The
baseline plugins remain allowlisted as baseline-only legacy because they are
research comparators, not promotion/runtime strategies. New strategy plugin
files are guarded by tests and should not directly construct the internal
dataclass.

## StrategySpec Ownership

New strategies should define `StrategySpec` in the plugin module that owns the strategy. This keeps new strategy PRs from modifying common research/runtime engine files.

`research/strategy_spec.py` still contains common dataclasses, validation helpers, compatibility helpers, and historical built-in specs. It is no longer the required central edit point for every new strategy. Existing centralized specs remain for backward compatibility unless a focused migration safely moves them into plugin-local modules.

Architecture guard tests should continue to prevent new strategy-specific branches from entering common research files such as `backtest_runner`, `backtest_kernel`, `backtest_engine`, `backtest_support`, and `strategy_registry`.

## Tests

### Research Test Tiering And Workload Budget

New strategy tests must not multiply default PR runtime by strategy count,
candidate count, scenario count, split/window count, and tick count. The default
PR suite is for contract and integration feedback, not full production research
matrices.

For every new strategy PR, include the expected default-fast
`estimated_strategy_runs` delta or the exact phrase `no default-fast workload
delta` in the PR checklist. Also include the research/nightly workload delta for
`estimated_strategy_runs`, `estimated_tick_events`, and
`estimated_audit_stream_rows`. Fast strategy tests should use a fake or minimal
`DatasetSnapshot`, direct policy contracts, pure replay material, or a
deterministic evaluator with `assert_fast_research_workload`. Strategy canaries
in the default suite should focus on common kernel contracts and minimal
datasets.

Do not add unmarked direct calls to `run_research_backtest` or
`run_research_walk_forward`. The static policy check rejects direct production
runner use unless the test has an expensive research marker and an entry in the
comprehensive workload inventory at `tests/policy/research_e2e_inventory.json`
explaining why lower-level contract coverage is insufficient.

Production backtest or report canaries belong in `research_e2e`, `nightly`, or
the dedicated research suite. Real walk-forward canaries belong in
`walk_forward_e2e`; real complete-external audit coverage belongs in
`audit_e2e`; real parallel worker evidence belongs in `parallel_e2e`. Keep those
E2E tests as the smallest representative smoke or acceptance checks and cover
hash, report, audit, artifact, and promotion payload semantics with lower-level
contract tests. Any new `research_e2e`, `audit_e2e`, `walk_forward_e2e`,
`parallel_e2e`, `research_kernel`, `slow_research`, `nightly`, or
`memory_sensitive` test must be disclosed in the PR checklist with workload
delta and justification.

The repository statically checks that the PR template and this authoring guide
continue to require those workload disclosures. The actual changed-file
classification remains review-enforced because local policy checks do not
always have the pull request diff available.

Level 1 research-only strategy tests should prove:

- registration and discovery
- research/backtest execution through the generic runner
- deterministic reproducibility fields and `promotion_grade=false`
- fail-closed promotion/runtime/live behavior
- no runtime/live/promotion boilerplate in the public authoring path
- no full default-fast research matrices

Level 2 replay-compatible strategy tests should prove:

- deterministic pure policy or deterministic replay decision material
- deterministic replay fingerprint and replay fingerprint hash
- read-only replay behavior
- parameter schema and runtime-bound parameter validation
- `live_dry_run_allowed=false`
- `live_real_order_allowed=false`
- runtime/live preflight fail-closed reason codes
- no full default-fast research matrices

Level 3 promotion-grade strategy tests should prove:

- explicit runtime parameter adapter
- runtime decision adapter factory
- replay support when required
- policy assembly and approved-profile binding
- replay/equivalence contracts
- no full default-fast research matrices
- live dry-run and live real-order capability behavior, including blocked verdicts when capability flags are false
- preserved decision, replay, runtime, policy, and profile hashes

New strategy PRs should normally modify one plugin file and one focused test file. They should not add strategy-specific branches to common research or runtime gateway files.

## Entry-Point Plugin Packages

External packages register plugins through the `bithumb_bot.strategy_plugins`
entry-point group. The entry point may return a single public authoring object,
a callable that returns one, or an iterable of public authoring objects. It must
not require editing common engine files.

Minimal `pyproject.toml` shape:

```toml
[project.entry-points."bithumb_bot.strategy_plugins"]
my_research_strategy = "my_package.my_strategy:RESEARCH_ONLY_PLUGIN"
my_replay_strategy = "my_package.my_strategy:REPLAY_COMPATIBLE_PLUGIN"
```

The repository scaffold in `examples/strategy_plugin_package/` includes Level 1
and Level 2 examples. Use the public contract helpers to verify discovery,
registration, generic backtest behavior, deterministic runtime replay, and
fail-closed live gates. Entry-point plugins should keep runtime artifacts out of
the repository and should not add strategy-specific fields to `Settings`.

## Canary Replay Sequence

`canary_non_sma` intentionally omits research events before
`CANARY_ORDER_START_INDEX`. Runtime replay over a pre-start candle emits `HOLD`.
The equivalence interpretation is
`pre_start_hold_omission_is_deterministic_and_not_replay_mismatch`; replay
fingerprints and tests bind that policy so omission is explicit rather than an
accidental mismatch.

## Compatibility

`ResearchStrategyPlugin` remains the normalized internal registry representation.
Existing code may still inspect it for contract hashes, runtime capability
validation, profile verification, and live preflight. Public strategy authoring
should use `strategy_authoring` instead of hand-writing a broad
`ResearchStrategyPlugin`.

`strategy.registry` is legacy/smoke compatibility only. `research.backtest_engine`, `research.backtest_loop`, and compatibility re-exports from `research.backtest_kernel` are compatibility-only for old import paths and must not regain strategy, risk, execution, or ledger authority.
