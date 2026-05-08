# Research Validation Lifecycle

This repository separates research-stage candidate variables from runtime env values.
Research manifests define hypotheses, data splits, parameter spaces, cost models, and acceptance gates.

Root `backtest.py` and any simple close-price SMA script are smoke backtests only. This is a smoke backtest only. It must not be used as evidence for strategy promotion, approved profiles, live readiness, or capital allocation. Official evidence comes from the `research-backtest` and `research-walk-forward` CLI paths, their managed artifacts, and explicit promotion/profile gates.
Runtime env/profile values should be treated as verified outputs of that process, not mutable knobs to tune until a backtest looks good.

## Lifecycle

```text
hypothesis
-> dataset snapshot
-> train / validation / final holdout split
-> parameter-space exploration
-> fee/slippage-aware backtest
-> out-of-sample validation
-> rolling walk-forward validation
-> parameter stability evidence
-> candidate artifact
-> operator-reviewed promotion artifact
-> paper validation consideration
```

The research engine is a pure replay/simulation path. It does not call the live broker, order lifecycle, run loop, recovery commands, or lot-native SELL authority code.

## Commands

Canonical commands:

```bash
uv run bithumb-bot sync-orderbook-top
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-walk-forward --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-promote-candidate --experiment-id sma_filter_v1_2026_05 --candidate-id candidate_001
```

Research commands follow the explicit env model. They do not implicitly load repo-root `.env`.
Use `BITHUMB_ENV_FILE`, `BITHUMB_ENV_FILE_PAPER`, or process env to select DB and runtime roots.

## Manifest Format

Manifests are JSON to avoid adding another dependency. See:

- [`examples/research/sma_filter_manifest.example.json`](/examples/research/sma_filter_manifest.example.json)

Required sections:

- `experiment_id`, `hypothesis`, `strategy_name`, `market`, `interval`
- `dataset.source=sqlite_candles`, `dataset.snapshot_id`, `train`, `validation`, optional `final_holdout`
- `parameter_space`
- `cost_model.fee_rate`, `cost_model.slippage_bps` for legacy fixed-bps manifests
- `execution_model` for normalized fixed-bps or stress execution scenarios. Stress scenarios may configure slippage bps, latency, partial-fill rate, order-failure rate, market-order extra cost, scenario policy, scenario role, seed, and calibration requirements. Unsupported execution-model fields fail manifest parsing rather than being ignored.
- `acceptance_gate`

Optional section:

- `walk_forward.train_window_days`, `test_window_days`, `step_days`, `min_windows`
- `dataset.top_of_book` to opt into SQLite top-of-book quote joins. Supported fields are `source=sqlite_orderbook_top_snapshots`, `required`, `join_tolerance_ms` (default `3000`), `missing_policy`, optional `quote_source`, and `min_coverage_pct`. Unsupported dataset or top-of-book fields fail manifest parsing rather than being ignored.

When `acceptance_gate.walk_forward_required=true`, the `walk_forward` section is required. All values must be positive integers.

Currently supported research strategies:

- `sma_with_filter`

Unknown research strategy names fail before simulation with an operator-readable unsupported strategy error. The research registry is not connected to live strategy execution.
Live SMA execution is regime-policy gated through `sma_with_filter`. Plain `sma_cross` remains a legacy paper/test/backtest compatibility strategy and is rejected in `MODE=live` with `plain_sma_live_not_allowed`.

## Artifacts

Research outputs are runtime artifacts and must not be written into the repository.
They are resolved through `PathManager`:

```text
DATA_ROOT/<mode>/derived/research/<experiment_id>/...
DATA_ROOT/<mode>/reports/research/<experiment_id>/...
```

Reports include manifest hash, dataset fingerprint, dataset quality reports and hashes, candidate profile hash, content hash, repository version, metrics, gate results, and artifact paths.
Reports aggregate by stable `parameter_candidate_id`; they do not treat each execution scenario as a separate promotion candidate. Each top-level candidate contains `scenario_policy`, pass/fail counts, required scenario count, required scenario ids, `final_holdout_present`, `final_holdout_required_for_promotion`, `candidate_profile_hash`, and `scenario_results[]`. Each scenario result records scenario identity, `scenario_role`, `scenario_role_source`, execution model payload/hash, cost model, train/validation/final-holdout/walk-forward metrics when present, regime gate result, execution-calibration gate, scenario acceptance result, fail reasons, and execution metadata. Candle-only datasets do not contain top-of-book, orderbook depth, or intra-candle path data; trade metadata records that limitation instead of fabricating quotes or depth. When top-of-book is configured and joined, execution metadata carries `best_bid`, `best_ask`, and `spread_bps`, while `reference_price` remains candle close.
`generated_at` is included for operator context but excluded from the deterministic `content_hash`.

Current-generation research reports also carry one deterministic `dataset_quality_report` payload per split. Each report records expected candle count from the manifest date range and interval, actual candle row count, expected buckets actually present, coverage percentage, missing bucket count/ranges/sample, duplicate-key diagnostics, timestamp monotonicity and interval consistency diagnostics, OHLC invariant violations, non-positive prices, negative volume, first/last timestamp, the candle-table schema fingerprint, the split dataset content hash, quality gate status, quality gate reasons, and a deterministic report `content_hash`. `coverage_pct` is based on expected buckets actually present, not raw row count, so duplicates or unexpected buckets cannot push coverage above 100%. Missing diagnostics are bounded for long 1m historical splits. When `dataset.top_of_book` is configured, the report also records `top_of_book_requested`, `top_of_book_required`, source, join tolerance, expected signal/candle count, joined count, missing count/sample, coverage percentage, quote gate status, and reason codes. Reports also include a deterministic top-level `top_of_book_quality_summary` with requested/required flags, joined and missing quote counts, aggregate coverage, affected split names, quote gate status, limitations, and operator next action. Optional missing quote coverage is `WARN`, adds candidate/report warning code `top_of_book_optional_coverage_warning`, and is printed in the CLI summary; required or fail-policy missing coverage fails closed. The combined `dataset_quality_hash` is included in research lineage, candidate profiles, and promotion artifacts. Unsupported interval formats fail closed instead of being treated as zero-coverage data.

Dataset quality is a research gate. Missing candles, OHLC invariant violations, non-positive prices, negative volume, duplicate keys, non-monotonic timestamps, interval mismatches, or unexpected buckets make the affected split `FAIL` and propagate reason-coded failures such as `dataset_quality_train_missing_candles` into candidate gates. Warning-mode quality output is not promotion evidence. Candle-only data remains valid only for candle-appropriate directional/filter strategies such as the current `sma_with_filter`; it is not evidence for spread-sensitive, latency-sensitive, partial-fill-sensitive, microstructure, or intra-candle path-dependent strategies.
If `dataset_quality_gate_status=FAIL`, do not promote. Repair or rebuild the candle dataset, rerun `research-backtest`, and verify that the corrected report carries the expected `dataset_quality_hash`.

## Top-Of-Book Quotes

Top-of-book snapshots are persisted in SQLite table `orderbook_top_snapshots` as best bid/ask only. This is not full orderbook depth, not queue position, not trade ticks, and not an intra-candle path reconstruction. It helps research surface spread and quote availability evidence without claiming liquidity depth or tick-level replay.

Collect one current public quote snapshot with:

```bash
uv run bithumb-bot sync-orderbook-top
```

The command validates the current public best bid/ask, computes `spread_bps`, writes only to the configured managed SQLite DB, and prints pair, bid, ask, spread, source, row count, and next action. It does not write repo-local artifacts. To use quote joins in research, add `dataset.top_of_book` to the manifest and rerun `research-backtest` or `research-walk-forward`. Research joins use the nearest stored snapshot within `join_tolerance_ms`; missing snapshots outside that tolerance remain missing evidence. If optional quote coverage is incomplete, reports and CLI output print `top_of_book_gate_status=WARN`, coverage percentage, missing count, affected splits, and the next action. If required quote coverage is missing, reports and CLI output include a fail-closed quote gate and the same next action: collect orderbook top snapshots with `sync-orderbook-top`, rerun research, and verify `top_of_book_coverage_pct`.

Top-of-book remains optional for `sma_with_filter`; candle-only runs still work and remain valid only for candle-appropriate strategies. Future quote-sensitive strategies can require `top_of_book`; when a strategy requires it and the manifest lacks it, validation fails closed with `research_data_requirement_top_of_book_missing`.

Top-of-book fields are metadata for current research execution evidence. Fill pricing still uses candle close as `reference_price` plus the configured fixed/stress bps model. Quote-aware execution pricing, spread gates, full depth, trade ticks, queue position, market impact, latency-aware quote replay, and intra-candle path reconstruction remain future work unless explicitly implemented and tested.

The research CLI prints an operator-facing run summary derived from the report payload without mutating the persisted artifact. The summary includes candidate gate counts, top candidate fail reasons, walk-forward window counts, top window fail reasons, promotion eligibility, nearest failed candidate diagnostics, and a conservative next action.
`nearest_failed_candidate_id` is diagnostic only and must not be used as a promotion candidate. `promotion_allowed=0` means do not run `research-promote-candidate`.

Candidate artifacts include parameter stability diagnostics. The stability score is based on one-grid-step neighboring candidates whose validation metrics remain gate-compatible. Isolated spikes do not satisfy `parameter_stability_required=true` merely because the grid has enough candidates.
Promotion artifacts also carry `live_regime_policy`; old or malformed artifacts without valid regime policy are rejected for promotion and fail closed for live/replay BUY entries when used through `STRATEGY_CANDIDATE_PROFILE_PATH`.

## Scenario Policy

Supported scenario policies are `legacy_cost_model_single_pass`, `single_scenario`, and `must_pass_base_and_survive_stress`.

`legacy_cost_model_single_pass` preserves old fixed-bps cost-model behavior: a parameter candidate can pass if one legacy fixed-bps scenario passes. This is retained for compatibility only.

When an `execution_model` omits `scenario_policy`, parsing defaults by generated scenario count: exactly one generated scenario uses `single_scenario`; multiple generated scenarios use `must_pass_base_and_survive_stress`. This prevents a scalar execution model from silently requiring stress-suite evidence that does not exist. Legacy `cost_model`-only manifests still use `legacy_cost_model_single_pass`.

`single_scenario` requires exactly one scenario result and that result must pass.

`must_pass_base_and_survive_stress` is evaluated at the same parameter-candidate level. The base scenario and every required stress scenario must be present for that same `parameter_candidate_id`; a base-only pass or stress-only pass is not promotion evidence. Required scenario failures produce fail reasons such as `scenario_policy_no_passing_base_scenario`, `scenario_policy_no_passing_stress_scenario`, `scenario_policy_required_scenario_failed:<scenario_id>:<reason>`, `scenario_result_missing`, or `scenario_policy_unsupported`.

`execution_model.scenario_role` is optional and, when supplied, must be either `base` or `stress`. A scalar manifest role applies to every generated scenario product and is emitted as `scenario_role_source=manifest`. When omitted, roles are derived deterministically as scenario index 0 = `base` and later scenarios = `stress`, emitted as `scenario_role_source=derived`. For an explicit multi-scenario `must_pass_base_and_survive_stress` manifest, a scalar role that makes every scenario only `base` or only `stress` is rejected at manifest parse time with `execution_model.scenario_role conflicts with must_pass_base_and_survive_stress`; that policy needs same-candidate evidence for both roles. `single_scenario` keeps its existing parse contract, and legacy `cost_model`-only manifests keep `legacy_cost_model_single_pass`.

Unsupported scenario policies fail closed. `best_candidate_id` is selected only from top-level aggregated candidates whose policy result is `PASS`.

## Stress Determinism

Stress execution does not share mutable RNG state across candidates. Each stochastic fill derives deterministic randomness from the scenario hash, base seed, stable parameter candidate id, split name, scenario id, signal timestamp, side, order type, and reference price. Reports include `base_seed`, `derived_seed_hash`, and `seed_derivation_inputs` in execution metadata so an operator can audit the randomness source without depending on candidate enumeration order.

Parameter-space list ordering is not semantic evidence. The manifest hash normalizes parameter-space values for hashing, and parameter candidate ids are hash-based from parameter values rather than enumeration index.

## Calibration Binding

Execution calibration artifacts are bound to the manifest market and interval. A mismatch fails the research gate with `execution_calibration_market_mismatch` or `execution_calibration_interval_mismatch`.

When `execution_model.calibration_required=true`, the calibration artifact must carry a valid `content_hash`. Missing hashes fail with `execution_calibration_content_hash_missing`; hash mismatches still fail with `execution_calibration_content_hash_mismatch`. Calibration also compares observed `partial_fill_rate` and `unfilled_rate` against scenario `partial_fill_rate` and `order_failure_rate`, and fails with `execution_calibration_partial_fill_rate_exceeds_assumption` or `execution_calibration_unfilled_rate_exceeds_assumption` when live execution is worse than the research scenario. Required or fail-strict calibration also enforces the execution-quality minimum sample count and a passing execution-quality gate, with `execution_calibration_sample_count_below_required` and `execution_calibration_quality_gate_not_passed` as explicit fail reasons. If calibration is optional and strictness is `warn`, missing or failing calibration remains explicit in the report but does not by itself fail an otherwise passing candidate. Candidate profiles and promotion artifacts expose warn-mode breaches through `has_execution_calibration_warning`, `execution_calibration_warning_reasons`, and `promotion_warnings`; successful `research-promote-candidate` CLI output prints those same fields so an operator does not need to open JSON to notice the warning. Required calibration failures still refuse promotion and do not produce a successful promotion block.

Walk-forward reports include rolling train/test windows, per-window metrics, pass/fail reasons, and aggregate evidence:

- `window_count`
- `pass_window_count`
- `fail_window_count`
- `mean_test_return_pct`
- `median_test_return_pct`
- `worst_test_return_pct`
- `return_consistency_pass`

If fewer than `walk_forward.min_windows` complete windows exist, the command fails with `walk_forward_insufficient_windows`.

## Promotion

`research-promote-candidate` generates an operator-reviewable promotion artifact.
It verifies that the backtest/OOS candidate exists, has validation evidence, passed the acceptance gate, and has a candidate profile hash.
It recomputes the canonical backtest report hash from the report body before binding `backtest_report_hash` into the promotion artifact. It does not trust the embedded backtest report `content_hash` field; missing or stale embedded source hashes fail closed with `backtest_report_content_hash_missing` or `backtest_report_hash_mismatch`.
It recomputes `sha256_prefixed(build_candidate_profile(candidate))` for the backtest/OOS candidate and refuses promotion with `backtest_candidate_profile_hash_mismatch` if the report was tampered with after generation.
For current-generation reports with lineage, promotion also requires passing dataset quality evidence and a `sha256:` `dataset_quality_hash`. Missing dataset-quality evidence fails with `dataset_quality_missing` or `dataset_quality_report_missing`; failed quality gates propagate their split-specific reasons.

Research reports now carry deterministic experiment lineage. Lineage records the experiment id, experiment family id, hypothesis id/status, manifest hash and canonical hash, dataset snapshot id, dataset content, dataset quality and split hashes, safe data-source fingerprint, repository version, command name and normalized command-args hash, cost/execution model hash, calibration hash when present, search budget, parameter grid size, attempt index, failed-candidate count, holdout reuse count, and dataset reuse policy. `lineage_hash` excludes volatile creation time. The hash proves the recorded lineage did not drift; it does not by itself prove the experiment can be reproduced.
Current-generation promotion requires valid lineage by default and refuses no-lineage reports with `promotion refused: lineage_missing`. Historical no-lineage reports require explicit operator intent with `--allow-legacy-lineage`; that compatibility path records `legacy_compatibility_used=true`, `lineage_required=false`, `lineage_hash=null`, `dataset_quality_legacy_bypass_used=true`, `legacy_lineage_compatibility_used`, and `legacy_dataset_quality_bypass_used` in promotion warnings. This is compatibility-only and is not full new-generation dataset-quality verification. Live profile promotion refuses profiles carrying `dataset_quality_legacy_bypass_used=true`; rebuild current-generation lineage and dataset-quality evidence instead of using the bypass for live readiness. Do not use the compatibility path for new research.

When walk-forward evidence is required, promotion also requires the matching candidate in `walk_forward_report.json` to pass real rolling walk-forward validation.
It recomputes the canonical walk-forward report hash before binding `walk_forward_report_hash` into the promotion artifact. It does not trust the embedded walk-forward report `content_hash` field; missing or stale embedded source hashes fail closed with `walk_forward_report_content_hash_missing` or `walk_forward_report_hash_mismatch`.
The walk-forward candidate must match the backtest/OOS candidate's experiment, strategy name, parameter candidate id, parameter values, cost model, and manifest hash, and its candidate profile hash is independently recomputed.
Missing, mismatched, failed, or tampered walk-forward evidence is reported with source-specific reasons such as `walk_forward_missing`, `walk_forward_candidate_mismatch`, `walk_forward_gate_not_passed`, `walk_forward_metrics_missing`, or `walk_forward_candidate_profile_hash_mismatch`.

The promotion artifact binds the evidence sources by recording `lineage_hash`, `validation_evidence_source`, `backtest_report_path`, `backtest_report_hash`, `backtest_candidate_profile_hash`, `backtest_candidate_profile_verified`, `walk_forward_required`, `walk_forward_report_path`, `walk_forward_report_hash`, `walk_forward_evidence_source`, `walk_forward_candidate_profile_hash`, and `walk_forward_candidate_profile_verified`.
If walk-forward is not required, the promotion artifact explicitly records `walk_forward_required=false` and null walk-forward evidence hash/source fields.

Before generating an approved profile, run reproducibility verification:

```bash
uv run bithumb-bot research-reproduce \
  --promotion "$DATA_ROOT/paper/reports/research/<experiment>/promotion_<candidate>.json"
```

`research-reproduce` loads the promotion artifact, verifies the promotion content hash, validates lineage, reopens recorded backtest and walk-forward reports when required, repeats the same source artifact truth check by recomputing their canonical hashes from report bodies excluding embedded `content_hash`, and reports specific failure reasons such as `lineage_missing`, `lineage_hash_mismatch`, `backtest_report_hash_mismatch`, `backtest_report_embedded_content_hash_mismatch`, `walk_forward_required_but_missing`, `walk_forward_report_hash_mismatch`, `walk_forward_report_embedded_content_hash_mismatch`, `dataset_content_hash_mismatch`, `dataset_quality_hash_mismatch`, `candidate_hash_mismatch`, `command_args_hash_mismatch`, and `calibration_hash_mismatch`. Old promotion artifacts without lineage are explicit `legacy_compatibility_used=true` and fail reproducibility verification instead of being treated as full lifecycle evidence.
If a report hash mismatches, regenerate the report from the manifest and dataset snapshot rather than editing the hash. If walk-forward evidence is missing or mismatched, rerun walk-forward validation. If decision-equivalence later fails on profile, market, interval, or data fingerprint, rerun decision-equivalence from matching research/runtime decision evidence before attempting another profile transition.

Promotion writes an operator-reviewable artifact only after these checks pass. It does not edit `.env`, `BITHUMB_ENV_FILE_LIVE`, `BITHUMB_ENV_FILE_PAPER`, or live secrets.

When a manifest requires execution calibration, promotion fails closed unless the backtest candidate carries passing execution-calibration evidence bound to the same market, interval, and calibration content hash. A malformed, missing, hashless, mismatched, insufficient, or breached calibration artifact is a rejection condition. Calibration artifacts are generated from `execution-quality-report --write-calibration` under `DATA_ROOT/<mode>/reports/execution_quality/` and can be supplied to research commands with `--execution-calibration <path>`.

Final-holdout evidence is required for promotion by default through `acceptance_gate.final_holdout_required_for_promotion=true`. Promotion refuses missing final-holdout evidence with `final_holdout_evidence_missing`. Final-holdout metrics are included in the candidate profile hash so changing final-holdout promotion evidence changes the hash.

The operator next step is review. Promotion evidence does not imply live readiness and does not edit env files or secrets.

A clean pytest pass is not promotion readiness. Tests show code contracts are currently satisfied; promotion readiness additionally requires complete scenario-policy evidence, compatible calibration evidence when required, final-holdout evidence, walk-forward evidence when required, operator review, approved-profile generation, and separate paper/live readiness gates.

## Approved Profiles

Approved profiles are the manual approval contract between research evidence and runtime configuration. They are operator-reviewable `reports` artifacts and are written atomically. The deterministic `profile_content_hash` explicitly excludes `generated_at` and `profile_content_hash` from the profile hash payload.

Generate a paper profile from a reviewed promotion artifact:

```bash
uv run bithumb-bot profile-generate \
  --promotion "$DATA_ROOT/paper/reports/research/<experiment>/promotion_<candidate>.json" \
  --mode paper \
  --out "$DATA_ROOT/paper/reports/profiles/<profile_id>.json"
```

Old promotion artifacts that predate embedded `market` or `interval` must be generated with explicit `--market` and `--interval`; missing values fail closed.

Compare and verify the profile against the intended env file before running:

```bash
uv run bithumb-bot profile-diff \
  --profile "$DATA_ROOT/paper/reports/profiles/<profile_id>.json" \
  --target-env "$BITHUMB_ENV_FILE_PAPER" \
  --json

uv run bithumb-bot profile-verify \
  --profile "$DATA_ROOT/paper/reports/profiles/<profile_id>.json" \
  --env "$BITHUMB_ENV_FILE_PAPER"
```

Both commands are credential-free. `profile-diff` compares approved profile values against env/runtime values only; its JSON output states that source promotion and evidence artifacts were not verified. Use `profile-verify` for the full env selector, runtime contract, source promotion, and evidence artifact-chain check. `profile-diff` and `profile-verify` require the env selector `APPROVED_STRATEGY_PROFILE_PATH` to resolve to the exact same path as `--profile`; the legacy `STRATEGY_APPROVED_PROFILE_PATH` is considered only as an older approved-profile alias after the canonical selector, and the canonical selector wins if both are set. `STRATEGY_CANDIDATE_PROFILE_PATH` is legacy regime-policy-only compatibility and is not an approved-profile selector. `profile-verify` exits non-zero on schema errors, hash mismatch, env selector mismatch, source promotion path-policy failure, source promotion content-hash drift, evidence content-hash drift, mode mismatch, ambiguous live arming flags, missing required fields, strategy parameter drift, market/interval drift, or cost model drift.

Runtime and CLI audit fields distinguish the full approved-profile path from legacy compatibility. A full approved selector emits `approved_profile_loaded=true`, `approved_profile_schema_hash_valid=true`, `approved_profile_source_verified=true`, `approved_profile_evidence_verified=true`, `approved_profile_runtime_verified=true`, `approved_profile_contract_scope=full_approved_profile`, `approved_profile_verification_ok=true`, and `legacy_candidate_profile_path_used=false`; it does not emit `legacy_profile_contract_scope`. A legacy `STRATEGY_CANDIDATE_PROFILE_PATH` compatibility load emits `legacy_candidate_profile_path_used=true`, `legacy_profile_contract_scope=regime_policy_only`, `approved_profile_contract_scope=legacy_regime_policy_only`, and does not mark source, evidence, or runtime verification as true. `approved_profile_verification_ok=true` means full approved-profile verification only; legacy regime-policy-only loading is reported as loaded-but-not-fully-verified.

Promotion between runtime approval states is explicit:

```bash
uv run bithumb-bot profile-promote \
  --profile "$DATA_ROOT/paper/reports/profiles/<paper_profile>.json" \
  --mode live_dry_run \
  --paper-validation-evidence "$DATA_ROOT/paper/reports/<paper_validation>.json" \
  --out "$DATA_ROOT/live/reports/profiles/<live_dry_run_profile>.json"

uv run bithumb-bot profile-promote \
  --profile "$DATA_ROOT/live/reports/profiles/<live_dry_run_profile>.json" \
  --mode small_live \
  --live-readiness-evidence "$DATA_ROOT/live/reports/<live_readiness>.json" \
  --out "$DATA_ROOT/live/reports/profiles/<small_live_profile>.json"
```

Each transition verifies the parent profile, reopens and rehashes the parent source promotion artifact, rechecks parent evidence artifact hashes when present, records `parent_profile_hash`, and refuses mode skipping before any child profile is written. Source promotion and evidence artifact paths must exist, resolve outside the repository, and have their byte content hash stored in the profile. Current custody policy rejects repository-local artifacts and accepts absolute repository-external artifacts, including managed `DATA_ROOT/<mode>/reports/...` paths; operators are responsible for preserving external absolute source/evidence artifacts outside managed roots. Those fields are included in the child profile hash. `profile-generate` creates paper profiles only; live-compatible profiles must come from `profile-promote`. Live dry-run startup accepts only a verified `live_dry_run` approved profile selected by `APPROVED_STRATEGY_PROFILE_PATH` or its older alias when the canonical selector is unset. Live armed execution accepts only a verified `small_live` approved profile selected the same way.
`profile-promote` requires typed semantic evidence for both paper validation and live readiness. Evidence artifacts are `reports` artifacts and must carry `evidence_schema_version=1`, `evidence_type`, mode, market, interval, strategy name, approved profile hash, source promotion hash, observation start/end/duration, decision counts, blocked-decision counts, closed lifecycle counts, gross/fee/net PnL, expectancy/profit-factor/fee-drag fields when applicable, execution-quality status and breach count, unresolved open order count, recovery blocker count, runtime/profile drift status, `db_data_fingerprint`, thresholds, decision-equivalence report path/content hash, and a deterministic `content_hash`. `generated_at` is operator context and is excluded from the deterministic hash.
Decision-equivalence is mandatory transition evidence for both paper validation and live readiness. Validation recomputes the decision-equivalence report hash from canonical report body excluding embedded `content_hash` and fails closed on missing path or hash, missing report file, hash mismatch, `ok=false`, nonzero mismatched decision count, non-empty missing research/runtime decision lists, profile hash mismatch, market mismatch, interval mismatch, comparable DB fingerprint mismatch, incomplete canonical decisions, `promotion_grade_comparison!=true`, or a legacy shallow comparison schema. Promotion evidence must use `comparison_contract_version=canonical_decision_v1`, `canonical_schema=true`, and `promotion_grade_comparison=true`. Rerun decision-equivalence when profile, market, interval, or data fingerprints drift.

Typed paper/live readiness evidence validation exists as a promotion contract. Effective promotion thresholds are repository-trusted policy, not self-declared evidence policy. Evidence artifact thresholds are retained as report metadata and must be at least as strict as the repository policy; weaker artifact thresholds fail closed with a policy-threshold reason code. `db_data_fingerprint` must be a non-empty `sha256:` value so the observation source is auditable. Live readiness rejects `execution_quality_status=not_applicable` by default; promotion to `small_live` requires real execution-quality applicability unless the repository policy is deliberately changed. Promotion fails closed when any required semantic field is missing, malformed, below threshold, weaker than trusted policy, or mismatched with the parent approved profile.

`strategy_performance.py` remains an operational closed-lifecycle guard over `trade_lifecycles`; it is not a research approval mechanism and is not a substitute for research promotion, paper validation evidence, or live readiness evidence. Root/simple smoke backtests remain smoke-only and must not be used as promotion evidence.

The decision loop is repo-owned. Use `bithumb-bot research-export-decisions --manifest <manifest.json> --candidate-id <candidate_id> --split validation --out <research_decisions.json>` to generate research decisions from the manifest, selected candidate, and research backtest path. Use `bithumb-bot runtime-replay-decisions --profile <approved_profile.json> --db <paper_or_runtime.sqlite> --through-ts-list <timestamps.json> --out <runtime_decisions.json>` to replay runtime SMA decisions at explicit closed-candle timestamps without live broker calls or order submission. Then run `bithumb-bot decision-equivalence --research-decisions <research_decisions.json> --runtime-decisions <runtime_decisions.json> --profile-hash <profile_hash> --market <market> --interval <interval> --data-fingerprint <dataset_or_db_hash>`.

The `decision-equivalence` command is a credential-free intermediate contract for comparing repo-generated research decisions with runtime replay decisions exported for the same candle snapshot and approved profile. Canonical decision events are represented by `decision_contract_version=1` and include candle/timestamp basis, raw and final signal, blocked filters and block reason, SMA features, edge/cost fields, fee/slippage/order-rule hashes, regime decision, position and dust hashes, entry/exit gates, exit rule/reason/evaluation hash, execution-timing policy hash, replay fingerprint hash, profile hash, and dataset or DB fingerprint. A canonical-looking payload is not promotion-grade unless required semantic fields are present and non-empty; `decision_contract_version=1` alone is insufficient. Runtime order-rule identity is part of the canonical decision contract and must be an actual non-empty order-rule snapshot hash, not the hash of `{}`. Manual decision JSON and legacy shallow inputs may be loaded for diagnostics, but legacy inputs emit `comparison_contract_version=legacy_shallow_v1`, recommend canonical regeneration, and are not accepted as promotion evidence. The command does not call live broker APIs and does not prove execution quality, orderbook/depth behavior, or intra-candle path behavior. Profile transition evidence fails closed when decision-equivalence evidence is missing, hash-mismatched, bound to the wrong profile or dataset, legacy/shallow, incomplete, blocked, not promotion-grade, not ok, or carries any nonzero mismatch count.

The current SMA runtime exit-rule set is explicit: `opposite_cross` and `max_holding_time`. `opposite_cross` includes the configured minimum take-profit floor, round-trip fee floor, and small-loss tolerance band. There are no separate hard `stop_loss` or standalone `take_profit` exit rules in the current strategy contract; those names are rejected by the exit-rule factory and must not be implied in promotion evidence.

Official lifecycle:

```text
hypothesis / manifest
-> dataset snapshot
-> research-backtest
-> research-walk-forward
-> research-promote-candidate
-> research-reproduce
-> approved profile generation
-> profile verification
-> decision equivalence / paper validation evidence
-> live-dry-run readiness
-> small-live promotion
```

Smoke backtests are quick diagnostics only. Official research evidence is the managed research report plus lineage. Promotion evidence is the promotion artifact and its `content_hash`/`lineage_hash`. Approved profile evidence is the profile artifact plus source promotion verification. Decision-equivalence evidence is the deterministic comparison report embedded in paper/live transition evidence. Runtime observability is the startup and decision context audit fields, including profile hash, promotion hash, lineage hash, manifest hash, dataset hash, backtest/walk-forward hashes when present, decision-equivalence hash, block reason, missing/mismatch lists, and whether legacy compatibility was used.

Runtime still keeps research separated from live execution: profiles verify approved values; they do not auto-apply values to env files and do not arm live trading.

## Current Scope

The engine supports SQLite candle snapshots, optional top-of-book quote joins, and a pure SMA-style simulation with fee/slippage costs.
Typed paper/live readiness evidence validation exists as a promotion contract.
However, automatic generation of those evidence artifacts from full paper/live operational logs remains a later-stage integration unless separately implemented.
Candle-only research remains path-limited. Top-of-book storage and quote coverage joins are available when explicitly configured, but full orderbook depth, trade ticks, spread-gated execution policy, quote-aware slippage modeling, latency-aware quote replay, and intra-candle path support remain separate follow-up work and should not be claimed as available.

Operator route:

- [`docs/runbooks/research-to-paper.md`](/docs/runbooks/research-to-paper.md)
