# Forward Return Diagnostics

## Purpose

Use `research-forward-diagnostics` to inspect as-of feature buckets against
future gross returns, MFE, and MAE for feature-mining diagnostics. The command is
read-only with respect to trading state and is outside strategy promotion,
approved profile, runtime replay, and live execution boundaries.

## Inputs

- A research manifest.
- A manifest split: `train`, `validation`, or `final_holdout`.
- A comma-separated feature list.
- A comma-separated positive integer horizon list.
- A bucket method such as `quantile:10`.
- An entry price mode: `next_open` or `signal_close`.

## Command

```bash
uv run bithumb-bot research-forward-diagnostics \
  --manifest <manifest.json> \
  --split train \
  --features sma_gap,range_ratio,volume_ratio,breakout_distance,rolling_return,zscore,regime \
  --horizons 1,3,5 \
  --bucket quantile:10 \
  --entry-price next_open \
  --min-bucket-count 30 \
  --json
```

## Outputs

```text
DATA_ROOT/<mode>/reports/research/<experiment_id>/forward_diagnostics_report.json

DATA_ROOT/<mode>/derived/research/<experiment_id>/forward_diagnostics/feature_bucket_metrics.csv
DATA_ROOT/<mode>/derived/research/<experiment_id>/forward_diagnostics/feature_horizon_metrics.csv
DATA_ROOT/<mode>/derived/research/<experiment_id>/forward_diagnostics/warnings.json
```

The report has `artifact_type=forward_return_diagnostic_report`,
`diagnostic_only=true`, and false promotion/readiness/capital allocation
evidence flags.

## Diagnostic-only policy

forward-return diagnostics output must not be used as strategy promotion evidence
forward-return diagnostics output must not be used as approved profile evidence
forward-return diagnostics output must not be used as live readiness evidence
forward-return diagnostics output must not be used as capital allocation evidence

## Not promotion evidence

The output can suggest feature-mining hypotheses, but it does not validate a
strategy, execution model, order lifecycle, costs, risk policy, walk-forward
stability, approved profile, paper behavior, or live readiness.

## Recommended next step

If a diagnostic result suggests a useful feature, encode the hypothesis in a
research manifest and run the normal validation lifecycle with
`research-validate`.
