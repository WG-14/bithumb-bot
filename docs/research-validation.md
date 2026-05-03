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
-> walk-forward validation
-> candidate artifact
-> operator-reviewed promotion artifact
-> paper/live env consideration
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

## Artifacts

Research outputs are runtime artifacts and must not be written into the repository.
They are resolved through `PathManager`:

```text
DATA_ROOT/<mode>/derived/research/<experiment_id>/...
DATA_ROOT/<mode>/reports/research/<experiment_id>/...
```

Reports include manifest hash, dataset fingerprint, candidate profile hash, content hash, repository version, metrics, gate results, and artifact paths.
`generated_at` is included for operator context but excluded from the deterministic `content_hash`.

## Promotion

`research-promote-candidate` generates an operator-reviewable promotion artifact.
It verifies that the candidate exists, has validation evidence, passed the acceptance gate, and has a candidate profile hash.
It does not edit `.env`, `BITHUMB_ENV_FILE_LIVE`, or live secrets.

The operator next step is review. Copying values into paper or live runtime env remains an explicit human action after validation evidence is accepted.

## Current Scope

The initial engine supports SQLite candle snapshots and a pure SMA-style simulation with fee/slippage costs.
Paper shadow validation, small-live readiness checks, and operational log revalidation are intentionally later stages.
