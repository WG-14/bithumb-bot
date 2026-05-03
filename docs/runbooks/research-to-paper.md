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

Review the hypothesis, dataset split dates, `snapshot_id`, parameter grid, cost model, acceptance gate, and `walk_forward` window configuration. Do not tune runtime env values until a backtest looks good.

3. Run the deterministic research backtest.

```bash
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json
```

Review the printed `manifest_hash`, `dataset_content_hash`, `content_hash`, report path, derived path, candidate count, `gate_result`, `candidate_gate_counts`, `top_fail_reasons`, `promotion_allowed`, `nearest_failed_candidate_id`, and `next_action`.
If `promotion_allowed=0`, do not run `research-promote-candidate`. `nearest_failed_candidate_id` is diagnostic only and must not be promoted.

4. Inspect the report artifact and hashes.

```bash
jq '.manifest_hash, .dataset_content_hash, .content_hash, .best_candidate_id' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
jq '.candidates[] | {candidate_id: .parameter_candidate_id, profile_hash: .candidate_profile_hash, gate: .acceptance_gate_result, reasons: .gate_fail_reasons, stability: .parameter_stability}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/backtest_report.json"
```

Confirm the report path is under `DATA_ROOT/<mode>/reports/research/...`, not the repository.

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

Promotion requires backtest/OOS evidence. If walk-forward is required, promotion also requires walk-forward evidence for the same experiment, strategy, parameters, cost model, and manifest.
Promotion refuses candidates with missing validation evidence, failed backtest gates, missing or failed walk-forward evidence, mismatched walk-forward candidates, or tampered candidate profile hashes.
Both evidence sources are hash-verified and bound into the promotion artifact.

8. Review the promotion artifact.

```bash
jq '{profile: .strategy_profile_id, hash: .verified_candidate_profile_hash, gate: .gate_result, backtest_hash: .backtest_candidate_profile_hash, backtest_verified: .backtest_candidate_profile_verified, wf_required: .walk_forward_required, wf_hash: .walk_forward_candidate_profile_hash, wf_verified: .walk_forward_candidate_profile_verified, next: .operator_next_step}' "$DATA_ROOT/paper/reports/research/sma_filter_v1_2026_05/promotion_<candidate_id>.json"
```

Verify the profile hash, candidate parameter values, dataset fingerprint, manifest hash, content hash, backtest evidence source, and walk-forward evidence source when required. Promotion does not edit `.env`, `BITHUMB_ENV_FILE_PAPER`, `BITHUMB_ENV_FILE_LIVE`, or secrets.

9. Manually prepare paper env/profile consideration.

Copying values into a paper env/profile is a manual operator action. Do not automate promotion into paper or live env files. Keep paper and live storage roots separate.

10. Run paper or live-dry-run observation.

```bash
uv run bithumb-bot run --short <paper_value> --long <paper_value>
uv run bithumb-bot live-dry-run --short <paper_value> --long <paper_value>
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
