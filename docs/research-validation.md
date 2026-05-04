# Research Validation Lifecycle

This repository separates research-stage candidate variables from runtime env values.
Research manifests define hypotheses, data splits, parameter spaces, cost models, and acceptance gates.
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
- `cost_model.fee_rate`, `cost_model.slippage_bps`
- `acceptance_gate`

Optional section:

- `walk_forward.train_window_days`, `test_window_days`, `step_days`, `min_windows`

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

Reports include manifest hash, dataset fingerprint, candidate profile hash, content hash, repository version, metrics, gate results, and artifact paths.
`generated_at` is included for operator context but excluded from the deterministic `content_hash`.

The research CLI prints an operator-facing run summary derived from the report payload without mutating the persisted artifact. The summary includes candidate gate counts, top candidate fail reasons, walk-forward window counts, top window fail reasons, promotion eligibility, nearest failed candidate diagnostics, and a conservative next action.
`nearest_failed_candidate_id` is diagnostic only and must not be used as a promotion candidate. `promotion_allowed=0` means do not run `research-promote-candidate`.

Candidate artifacts include parameter stability diagnostics. The stability score is based on one-grid-step neighboring candidates whose validation metrics remain gate-compatible. Isolated spikes do not satisfy `parameter_stability_required=true` merely because the grid has enough candidates.
Promotion artifacts also carry `live_regime_policy`; old or malformed artifacts without valid regime policy are rejected for promotion and fail closed for live/replay BUY entries when used through `STRATEGY_CANDIDATE_PROFILE_PATH`.

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
It recomputes `sha256_prefixed(build_candidate_profile(candidate))` for the backtest/OOS candidate and refuses promotion with `backtest_candidate_profile_hash_mismatch` if the report was tampered with after generation.

When walk-forward evidence is required, promotion also requires the matching candidate in `walk_forward_report.json` to pass real rolling walk-forward validation.
The walk-forward candidate must match the backtest/OOS candidate's experiment, strategy name, parameter candidate id, parameter values, cost model, and manifest hash, and its candidate profile hash is independently recomputed.
Missing, mismatched, failed, or tampered walk-forward evidence is reported with source-specific reasons such as `walk_forward_missing`, `walk_forward_candidate_mismatch`, `walk_forward_gate_not_passed`, `walk_forward_metrics_missing`, or `walk_forward_candidate_profile_hash_mismatch`.

The promotion artifact binds the evidence sources by recording `validation_evidence_source`, `backtest_candidate_profile_hash`, `backtest_candidate_profile_verified`, `walk_forward_required`, `walk_forward_evidence_source`, `walk_forward_candidate_profile_hash`, and `walk_forward_candidate_profile_verified`.
If walk-forward is not required, the promotion artifact explicitly records `walk_forward_required=false` and null walk-forward evidence hash/source fields.

Promotion writes an operator-reviewable artifact only after these checks pass. It does not edit `.env`, `BITHUMB_ENV_FILE_LIVE`, `BITHUMB_ENV_FILE_PAPER`, or live secrets.

The operator next step is review. Promotion evidence does not imply live readiness and does not edit env files or secrets.

## Approved Profiles

Approved profiles are the manual approval contract between research evidence and runtime configuration. They are operator-reviewable `reports` artifacts and are written atomically. The deterministic `profile_content_hash` excludes `generated_at`, matching the research report hashing convention.

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

Both commands are credential-free. `profile-verify` exits non-zero on schema errors, hash mismatch, source promotion content-hash drift, mode mismatch, missing required fields, strategy parameter drift, market/interval drift, or cost model drift.

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

Each transition verifies the parent profile, records `parent_profile_hash`, and refuses mode skipping. `profile-generate` creates paper profiles only; live-compatible profiles must come from `profile-promote`. Live dry-run startup accepts only a verified `live_dry_run` approved profile selected by `APPROVED_STRATEGY_PROFILE_PATH`. Live armed execution accepts only a verified `small_live` approved profile selected by `APPROVED_STRATEGY_PROFILE_PATH`.

Runtime still keeps research separated from live execution: profiles verify approved values; they do not auto-apply values to env files and do not arm live trading.

## Current Scope

The engine supports SQLite candle snapshots and a pure SMA-style simulation with fee/slippage costs.
Paper shadow validation, small-live readiness checks, and operational log revalidation are intentionally later stages.

Operator route:

- [`docs/runbooks/research-to-paper.md`](/docs/runbooks/research-to-paper.md)
