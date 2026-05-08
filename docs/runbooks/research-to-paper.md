# Research To Paper Runbook

## Purpose

Use this runbook to move from a research hypothesis to an operator-reviewed promotion artifact and then to paper validation consideration.

Research artifacts are evidence, not authorization. Promotion does not edit env files and does not imply live readiness. Live execution remains protected by the existing live preflight, arming, run-lock, recovery, duplicate-order, ledger, and lot-native SELL authority gates.

## Commands

1. Verify candle sync and DB readiness.

```bash
uv run bithumb-bot health
uv run bithumb-bot candles --limit 5
```

Confirm the active `DB_PATH` is a repository-external runtime path and contains the manifest market/interval candles.

2. Create or review the manifest.

```bash
sed -n '1,220p' examples/research/sma_filter_manifest.example.json
```

Review the hypothesis, dataset split dates, `snapshot_id`, parameter grid, cost model, acceptance gate, final-holdout policy, and `walk_forward` window configuration. Do not tune runtime env values until a backtest looks good.
If the manifest uses `execution_model`, review every scenario, `scenario_policy`, `scenario_role`, stress seed, and whether execution calibration is required. If `scenario_policy` is omitted, one generated scenario defaults to `single_scenario` and multiple generated scenarios default to `must_pass_base_and_survive_stress`; legacy `cost_model`-only manifests keep `legacy_cost_model_single_pass`. If `scenario_role` is omitted, reports mark roles as derived from scenario order, with index 0 as `base` and later scenarios as `stress`, and emit `scenario_role_source=derived`. A scalar manifest-supplied `base` or `stress` role applies to every generated scenario and emits `scenario_role_source=manifest`; do not pair an explicit multi-scenario `must_pass_base_and_survive_stress` policy with a scalar role that leaves only one role type, because manifest parsing rejects that impossible evidence contract. `must_pass_base_and_survive_stress` is same-candidate evidence: a base pass for one parameter candidate and a stress pass for another candidate do not combine. Root/simple smoke backtests are not research evidence and must not be used as evidence for strategy promotion, approved profiles, live readiness, or capital allocation.

For official research, the manifest should carry stable experiment-family metadata when available: `experiment_family_id`, `hypothesis_id`, `hypothesis_status`, `pre_registered_at`, `attempt_index`, `holdout_reuse_count`, and `dataset_reuse_policy`. These are lineage and observability fields. They do not solve overfitting by themselves, but they make repeated final-holdout use and researcher degrees of freedom visible before promotion.

3. Run the deterministic research backtest.

```bash
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json
```

When execution-quality calibration evidence exists and the manifest requires or should compare it, pass it explicitly:

```bash
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json --execution-calibration "$DATA_ROOT/paper/reports/execution_quality/<calibration>.json"
```

Review the printed `manifest_hash`, `dataset_content_hash`, `content_hash`, report path, derived path, candidate count, `gate_result`, `candidate_gate_counts`, `top_fail_reasons`, `promotion_allowed`, `nearest_failed_candidate_id`, and `next_action`.
Then inspect `dataset_quality_gate_status`, `dataset_quality_hash`, and `dataset_quality_gate_reasons` in the report. Dataset quality failures are structural evidence failures, not tuning hints. Missing candle buckets, OHLC invariant violations, non-positive prices, negative volume, duplicate keys, non-monotonic timestamps, interval mismatches, or unsupported interval formats must be resolved by fixing the source dataset or manifest before promotion.
If `promotion_allowed=0`, do not run `research-promote-candidate`. `nearest_failed_candidate_id` is diagnostic only and must not be promoted.

4. Inspect the report artifact and hashes.

```bash
jq '.manifest_hash, .dataset_content_hash, .dataset_quality_hash, .dataset_quality_gate_status, .dataset_quality_gate_reasons, .content_hash, .best_candidate_id' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
jq '.dataset_quality_reports | to_entries[] | {split: .key, status: .value.quality_gate_status, reasons: .value.quality_gate_reasons, expected: .value.expected_candle_count, actual: .value.actual_candle_count, missing: .value.missing_bucket_count, hash: .value.content_hash}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
jq '.candidates[] | {candidate_id: .parameter_candidate_id, profile_hash: .candidate_profile_hash, gate: .acceptance_gate_result, reasons: .gate_fail_reasons, scenario_policy: .scenario_policy, pass: .scenario_pass_count, fail: .scenario_fail_count, required: .required_scenario_count, final_holdout_present: .final_holdout_present, stability: .parameter_stability}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
jq '.candidates[] | {candidate_id: .parameter_candidate_id, scenarios: [.scenario_results[] | {id: .scenario_id, role: .scenario_role, role_source: .scenario_role_source, gate: .scenario_acceptance_gate_result, reasons: .scenario_fail_reasons, calibration: .execution_calibration_gate.status}]}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
```

Confirm the report path is under `DATA_ROOT/<mode>/reports/research/...`, not the repository. For stress scenarios, inspect `base_seed`, `derived_seed_hash`, and `seed_derivation_inputs` in scenario execution metadata; they must be tied to candidate id, scenario id, split name, and seed, not to candidate enumeration order.

5. Run rolling walk-forward validation.

```bash
uv run bithumb-bot research-walk-forward --manifest examples/research/sma_filter_manifest.example.json
```

This command must produce real rolling train/test window evidence when `walk_forward_required=true`.
Review the printed `walk_forward_window_summary` and `top_window_fail_reasons` before inspecting the full artifact.

6. Inspect rolling walk-forward evidence.

```bash
jq '.candidates[] | {candidate_id: .parameter_candidate_id, gate: .walk_forward_gate_result, wf: .walk_forward_metrics}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/walk_forward_report.json"
```

Review every window date range, test return, fail reason, `window_count`, `pass_window_count`, `fail_window_count`, `mean_test_return_pct`, `median_test_return_pct`, `worst_test_return_pct`, and `return_consistency_pass`.

7. Promote only an operator-reviewed passing candidate.

```bash
uv run bithumb-bot research-promote-candidate --experiment-id sma_filter_v1_2026_05 --candidate-id <candidate_id>
```

Promotion requires valid lineage, passing dataset quality evidence, backtest/OOS evidence, same-candidate scenario-policy evidence, and final-holdout evidence by default. If walk-forward is required, promotion also requires walk-forward evidence for the same experiment, strategy, parameters, cost model, execution model, calibration gate, and manifest.
Promotion refuses candidates with missing lineage, missing or failed dataset quality evidence, missing validation evidence, failed scenario policy, missing final holdout, failed backtest gates, missing or failed walk-forward evidence, mismatched walk-forward candidates, missing/hashless/mismatched/breached required execution-calibration evidence, or tampered candidate profile hashes.
Historical no-lineage reports may be promoted only with explicit `--allow-legacy-lineage` after operator review. That path records `legacy_compatibility_used=true`, `lineage_required=false`, `lineage_hash=null`, and `legacy_lineage_compatibility_used`; do not use it for new research.
Both evidence sources are hash-verified and bound into the promotion artifact. `research-promote-candidate` recomputes canonical backtest/walk-forward report hashes before binding them into the promotion artifact. It does not trust embedded report `content_hash` fields, and it fails closed with source-specific missing or mismatch reasons when source evidence has drifted. Current-generation promotion artifacts also carry `lineage_hash`, backtest/walk-forward report hashes, manifest and dataset hashes, command-args hash, repository version, candidate profile hash, calibration hash when present, and experiment-family observability fields. On success, read the printed `has_execution_calibration_warning`, `execution_calibration_warning_reasons`, and `promotion_warnings` lines before moving to artifact review; optional warn-mode calibration breaches can still promote, but they are operator-visible warnings, not approval signals.

8. Reproduce and review the promotion artifact.

```bash
uv run bithumb-bot research-reproduce \
  --promotion "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/promotion_<candidate_id>.json"

jq '{profile: .strategy_profile_id, hash: .verified_candidate_profile_hash, gate: .gate_result, lineage_hash: .lineage_hash, legacy: .legacy_compatibility_used, scenario_policy: .scenario_policy, scenario_pass_count: .scenario_pass_count, scenario_fail_count: .scenario_fail_count, calibration_warning: .has_execution_calibration_warning, calibration_warning_reasons: .execution_calibration_warning_reasons, promotion_warnings: .promotion_warnings, final_holdout_present: .final_holdout_present, backtest_report_hash: .backtest_report_hash, backtest_hash: .backtest_candidate_profile_hash, backtest_verified: .backtest_candidate_profile_verified, wf_required: .walk_forward_required, wf_report_hash: .walk_forward_report_hash, wf_hash: .walk_forward_candidate_profile_hash, wf_verified: .walk_forward_candidate_profile_verified, family: .experiment_family_id, hypothesis: .hypothesis_id, attempt: .attempt_index, holdout_reuse: .holdout_reuse_count, next: .operator_next_step}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/promotion_<candidate_id>.json"
```

`research-reproduce` must return `ok=true` for current-generation promotion artifacts. It repeats the same source artifact truth check as promotion by recomputing report hashes from canonical report bodies instead of trusting embedded `content_hash` fields, and fails closed with specific reasons such as `lineage_missing`, `lineage_hash_mismatch`, `backtest_report_hash_mismatch`, `backtest_report_embedded_content_hash_mismatch`, `walk_forward_required_but_missing`, `walk_forward_report_hash_mismatch`, `walk_forward_report_embedded_content_hash_mismatch`, `dataset_content_hash_mismatch`, `dataset_quality_hash_mismatch`, `candidate_hash_mismatch`, `command_args_hash_mismatch`, or `calibration_hash_mismatch`. Old promotion artifacts without lineage are reported as legacy compatibility and are not full reproducibility evidence.
If a report hash mismatches, regenerate the research report from the manifest and dataset snapshot. If walk-forward is missing or mismatched, rerun walk-forward validation. Do not repair reproducibility by editing recorded hashes.

Verify the profile hash, candidate parameter values, lineage hash, scenario policy counts, final-holdout presence, dataset fingerprint, manifest hash, content hash, backtest evidence source/hash, calibration hash/market/interval when required, experiment family metadata, holdout reuse count, and walk-forward evidence source/hash when required. Optional warn-mode calibration breaches are not hard promotion failures, but `has_execution_calibration_warning=true`, `execution_calibration_warning_reasons`, and `promotion_warnings` must appear in the candidate profile, promotion artifact, and successful promotion CLI output, then be reviewed. Required calibration failures still refuse promotion. Promotion does not edit `.env`, `BITHUMB_ENV_FILE_PAPER`, `BITHUMB_ENV_FILE_LIVE`, or secrets.

A clean `uv run pytest -q` pass is not approval to allocate capital. It only validates code behavior. Promotion readiness still requires complete fail-closed research evidence, operator review, approved-profile verification, paper validation, and live readiness checks.

9. Generate and verify the approved paper profile.

```bash
uv run bithumb-bot profile-generate \
  --promotion "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/promotion_<candidate_id>.json" \
  --mode paper \
  --out "$DATA_ROOT/paper/reports/profiles/<paper_profile>.json"

uv run bithumb-bot profile-diff \
  --profile "$DATA_ROOT/paper/reports/profiles/<paper_profile>.json" \
  --target-env "$BITHUMB_ENV_FILE_PAPER" \
  --json

uv run bithumb-bot profile-verify \
  --profile "$DATA_ROOT/paper/reports/profiles/<paper_profile>.json" \
  --env "$BITHUMB_ENV_FILE_PAPER"
```

Set `APPROVED_STRATEGY_PROFILE_PATH` in the paper env file only after operator review. `STRATEGY_APPROVED_PROFILE_PATH` is an older approved-profile alias used only when the canonical selector is unset; if both are present, the canonical selector wins. `STRATEGY_CANDIDATE_PROFILE_PATH` is legacy regime-policy-only compatibility and cannot satisfy live approved-profile requirements. Do not automate promotion into paper or live env files. Keep paper and live storage roots separate. `profile-diff` compares profile values to env/runtime values and does not verify source promotion or evidence artifacts. The full `profile-verify` chain checks that the env selector resolves to the exact `--profile` path, then checks strategy name, market, interval, strategy parameters, cost model, source promotion artifact path and content hash, lineage hash when required, candidate profile hash, manifest hash, dataset content hash, profile mode, evidence artifact hashes, decision-equivalence artifact hashes, and regime policy.

In runtime and decision telemetry, the approved selector path is reported as `approved_profile_contract_scope=full_approved_profile` with `legacy_candidate_profile_path_used=false` only after full source, evidence, and runtime verification succeeds; it does not emit `legacy_profile_contract_scope`. Legacy `STRATEGY_CANDIDATE_PROFILE_PATH` usage is reported as `approved_profile_contract_scope=legacy_regime_policy_only` and `legacy_profile_contract_scope=regime_policy_only`; it may load regime policy for compatibility, but it must not be read as approved-profile verification. `approved_profile_verification_ok=true` is reserved for the full approved-profile contract.

10. Run paper observation.

```bash
uv run bithumb-bot run --short <paper_value> --long <paper_value>
```

Use only explicit env files or process env. Do not reintroduce repo-root `.env` autoloading.

11. Inspect decision and strategy telemetry.

```bash
uv run bithumb-bot decision-telemetry --limit 200
uv run bithumb-bot experiment-report --sample-threshold 30 --top-n 3
uv run bithumb-bot strategy-report
```

Review paper behavior, suppressed decisions, order intent evidence, and operator reports before considering any small-live readiness checklist.

12. Consider small-live readiness only after paper evidence.

Research promotion, paper validation, and live readiness are separate gates. Live execution still requires existing live safety configuration, explicit arming, notifier requirements, loss limits, order count limits, preflight checks, run locks, reconciliation, and operator intervention when consistency is unclear.

`profile-promote` verifies the parent profile's source promotion and existing evidence artifacts before creating a child profile. Source promotion and new evidence artifacts are verified by path policy, existence, byte content hash, typed evidence schema, decision-equivalence report hash, and semantic readiness thresholds. Decision-equivalence is mandatory for both paper validation and live readiness; validation recomputes the report hash from canonical report body excluding embedded `content_hash`. Generate decisions with repo-owned commands: `bithumb-bot research-export-decisions --manifest <manifest.json> --candidate-id <candidate_id> --split validation --out <research_decisions.json>`, `bithumb-bot runtime-replay-decisions --profile <approved_profile.json> --db <paper_or_runtime.sqlite> --through-ts-list <timestamps.json> --out <runtime_decisions.json>`, then `bithumb-bot decision-equivalence ...`. The report must use `comparison_contract_version=canonical_decision_v1`, `canonical_schema=true`, and `promotion_grade_comparison=true`; legacy shallow or incomplete canonical decision-equivalence reports are diagnostic only and fail closed for promotion. A `decision_contract_version=1` payload is not promotion evidence unless required semantic fields are present, each decision is bound to the requested profile/market/interval/data fingerprint, and runtime order-rule identity is populated from an actual non-empty order-rule snapshot hash. The current custody policy rejects repository-local artifacts and accepts absolute repository-external artifacts, including managed `DATA_ROOT/<mode>/reports/...` paths; operators remain responsible for custody of external absolute source/evidence artifacts outside managed roots. It stores resolved evidence path, `sha256:` content hash, the decision-equivalence report path/hash, and the approved profile hash that the evidence validated, and those fields are included in the child profile hash. Promotion fails closed on malformed evidence, profile/source hash mismatch, insufficient observation window, insufficient decision or closed lifecycle count, execution-quality breaches, unresolved orders, recovery blockers, runtime/profile drift, missing decision-equivalence evidence, decision-equivalence hash mismatch, wrong decision-equivalence profile, market, interval, dataset or comparable DB fingerprint, legacy decision-equivalence schema, incomplete canonical decisions, non-promotion-grade decision-equivalence, blocked decision equivalence, non-empty missing research/runtime decision lists, or nonzero decision mismatch count. Rerun decision-equivalence if profile, market, interval, dataset, or DB fingerprints drift.

To promote beyond paper, use explicit profile transitions. `profile-generate` creates paper profiles only; live-compatible profiles must be created with `profile-promote`.

```bash
uv run bithumb-bot profile-promote \
  --profile "$DATA_ROOT/paper/reports/profiles/<paper_profile>.json" \
  --mode live_dry_run \
  --paper-validation-evidence "$DATA_ROOT/paper/reports/<paper_validation>.json" \
  --out "$DATA_ROOT/live/reports/profiles/<live_dry_run_profile>.json"
```

After setting `APPROVED_STRATEGY_PROFILE_PATH` in the live env file to the verified `live_dry_run` profile, run live dry-run observation:

```bash
uv run bithumb-bot live-dry-run --short <paper_value> --long <paper_value>
```

Promote to small live only after live readiness evidence:

```bash
uv run bithumb-bot profile-promote \
  --profile "$DATA_ROOT/live/reports/profiles/<live_dry_run_profile>.json" \
  --mode small_live \
  --live-readiness-evidence "$DATA_ROOT/live/reports/<live_readiness>.json" \
  --out "$DATA_ROOT/live/reports/profiles/<small_live_profile>.json"
```

Live dry-run startup fails closed unless the approved selector points to a verified `live_dry_run` profile. Live armed startup fails closed unless the approved selector points to a verified `small_live` profile whose runtime contract matches the effective settings. Ambiguous live flags fail with explicit reason codes such as `live_mode_arming_flags_ambiguous` or `live_mode_not_dry_run_or_armed`. Roll back by editing only `APPROVED_STRATEGY_PROFILE_PATH` to a previous approved profile and rerunning `profile-verify`; no profile CLI mutates env files.
