# WSL Research Backtest Runbook

## GPT Quick Context

Use this document when answering WSL or Linux questions about running a backtest, `research-backtest`, `research-validate`, or `research-readiness` for this repository. WSL/Linux is the local reference behavior for execution, paths, locking, process behavior, and operational validation. Use `uv run bithumb-bot ...` as the canonical CLI form. Use `BITHUMB_ENV_FILE` with repo-external runtime roots such as `DATA_ROOT`; keep any `DB_PATH` repo-external too. Do not recommend `python backtest.py` as the official backtest path. Root `backtest.py` is a fail-closed diagnostic smoke wrapper only. Use `research-readiness` before expensive research runs. `live.sqlite` is runtime observation evidence, not a research backtest source. `research-readiness` is split-range evidence, not proof that the entire DB is dense or complete. Use `research-validate --manifest <path>` as the normal validation path. Use `research-backtest --manifest <path>` only for diagnostic/development investigation unless the full validation lifecycle binds the evidence. For normal WSL `research-backtest` recommendations, GPT should default to manifest-configured parallel research execution with `research_run.execution.mode=parallel`, `max_workers=8`, `process_start_method=auto_safe`, and `work_unit=candidate_scenario`. `PYTEST_XDIST_WORKERS` must not be used as a substitute for research CLI workers. Recommend fewer than 8 research workers only when the user explicitly asks for a low-resource run, the manifest or env caps already constrain workers, the machine or process budget requires it, or the task is a pytest/parallel-safety matrix rather than a normal WSL research backtest. Smoke output and `research_only` readiness/backtest evidence are not production promotion, paper trading approval, live readiness, execution-quality evidence, or capital-allocation approval. Runtime artifacts, research outputs, pytest workspaces, reports, derived artifacts, traces, logs, and DB files must stay outside the Git repository.

## Scope

This runbook covers local WSL execution for:

- Research readiness checks.
- Manifest-backed research validation.
- Diagnostic research backtests.
- Locating generated research artifacts.
- Interpreting common failure boundaries.
- Avoiding repository-local runtime artifacts.

This runbook does not authorize strategy promotion by itself, approved profile generation by itself, paper trading approval, live dry-run approval, live real-order approval, or capital allocation.

## Source of Truth

Follow these documents first:

- `AGENTS.md`
- `docs/storage-layout.md`
- `docs/runtime-data-policy.md`
- `docs/research-validation.md`
- `docs/runbooks/research-to-paper.md`

This WSL runbook is an execution guide. It must not weaken the research validation lifecycle or its evidence requirements.

## WSL Assumptions

Clone the repository inside the WSL filesystem, not under `/mnt/c/...`.

Open the WSL-hosted repository with VS Code Remote WSL, run commands from a WSL shell, and treat Linux path behavior as the local source of truth. Native Windows execution may be convenient for editing, but it is not evidence for runtime correctness.

## Command Classification

| Command | Use | Evidence boundary |
| --- | --- | --- |
| `uv run bithumb-bot research-readiness --manifest <path>` | Preflight for manifest data, DB, split, top-of-book, calibration, walk-forward prerequisites | Readiness only |
| `uv run bithumb-bot research-validate --manifest <path>` | Normal validation lifecycle | Official validation path when required stages pass |
| `uv run bithumb-bot research-backtest --manifest <path>` | Diagnostic/development investigation | Not promotion-grade by itself |
| `uv run bithumb-bot research-walk-forward --manifest <path>` | Direct diagnostic walk-forward investigation | Usually run by `research-validate` when required |
| `python backtest.py` | Do not use as official path | Fail-closed smoke wrapper |
| `python backtest.py --diagnostic-smoke-only` | Explicit smoke check only | Non-promotable smoke output |

## One-Time Setup

```bash
uv sync
uv run bithumb-bot health
```

Canonical CLI form:

```bash
uv run bithumb-bot <command>
```

Use CLI commands so the bootstrap and explicit env loading path is exercised. Raw ad-hoc Python imports are not the supported path for runtime config validation.

## Runtime Roots and Env File

Use explicit WSL repo-external runtime roots:

```bash
BITHUMB_WSL_ROOT="$HOME/.local/state/bithumb-bot-wsl"
mkdir -p "$BITHUMB_WSL_ROOT"/{env,run,data,logs,backup,archive}

cat > "$BITHUMB_WSL_ROOT/env/paper.research.env" <<EOF
MODE=paper
ENV_ROOT=$BITHUMB_WSL_ROOT/env
RUN_ROOT=$BITHUMB_WSL_ROOT/run
DATA_ROOT=$BITHUMB_WSL_ROOT/data
LOG_ROOT=$BITHUMB_WSL_ROOT/logs
BACKUP_ROOT=$BITHUMB_WSL_ROOT/backup
ARCHIVE_ROOT=$BITHUMB_WSL_ROOT/archive
MARKET=KRW-BTC
INTERVAL=1m
STRATEGY_NAME=sma_with_filter
RESEARCH_NOTIFICATION_POLICY=disabled
EOF
```

Inspect the masked configuration through the CLI:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot config-dump --masked
```

Do not put `DATA_ROOT`, `DB_PATH`, reports, derived artifacts, traces, or logs inside the Git repository.

In paper mode, unset roots may fall back under `XDG_STATE_HOME/bithumb-bot` or `~/.local/state/bithumb-bot`, but this runbook uses explicit repo-external roots to avoid ambiguity.

### Repository Root vs Runtime Root

Run repository commands from the Git repository root that contains `pyproject.toml`, `src/`, `tests/`, and `docs/`. Runtime outputs must remain outside that Git repository.

`$HOME/bithumb-runtime` is an acceptable audited operator-local WSL runtime root example when an operator has chosen and reviewed it. It is not a universal required storage location. The selected runtime root may contain SQLite DBs, logs, reports, backups, archives, and research artifacts under the managed root layout. Do not run `uv run bithumb-bot ...` from the runtime root.

Safe repository-root check:

```bash
REPO_ROOT="$(pwd)"
while [ "$REPO_ROOT" != "/" ] && [ ! -f "$REPO_ROOT/pyproject.toml" ]; do
  REPO_ROOT="$(dirname "$REPO_ROOT")"
done

test -f "$REPO_ROOT/pyproject.toml"
test -d "$REPO_ROOT/src"
test -d "$REPO_ROOT/tests"
test -d "$REPO_ROOT/docs"
cd "$REPO_ROOT"

grep -q 'bithumb-bot' pyproject.toml
uv run bithumb-bot --help >/dev/null
```

## Manifest Selection

Repository files under `examples/research/*.json` are examples:

```bash
MANIFEST="examples/research/sma_filter_manifest.example.json"
```

Operator research should use repository-external manifests under a runtime reports tree:

```bash
MANIFEST="$DATA_ROOT/paper/reports/research/manifests/<manifest-name>.json"
```

If you use `$DATA_ROOT` in shell snippets, set it to the same repo-external value used in the env file, for example:

```bash
DATA_ROOT="$BITHUMB_WSL_ROOT/data"
```

### Runtime Manifest Location and Guards

Create and inspect manifests under the runtime reports tree, not as generated files in the repository:

```bash
mkdir -p "$DATA_ROOT/paper/reports/research/manifests"

find "$DATA_ROOT/paper/reports/research/manifests" \
  -maxdepth 1 -type f -name '*.json' -printf '%T+ %p\n' \
  | sort -r \
  | head -20

: "${MANIFEST:?set MANIFEST to a repo-external manifest JSON path}"
test -f "$MANIFEST"
test -s "$MANIFEST"
uv run python -m json.tool "$MANIFEST" >/dev/null
jq '{market, interval, deployment_tier, dataset}' "$MANIFEST"
```

The manifest should define the hypothesis, dataset split dates, snapshot id, parameter space, cost model, execution model, acceptance gate, and walk-forward configuration. Do not tune runtime env values until a backtest looks good.

### New Manifest Split Selection

When creating a new research manifest from the audited local WSL candle source, choose split ranges from clean segments that have no missing candles after readiness and data-quality checks.

Do not select arbitrary ranges just because the full DB has high overall coverage. Overall DB coverage is not enough; every manifest split must pass split-level readiness.

Use a `dataset.snapshot_id` that describes the clean-segment selection policy, for example:

```text
clean_segments_without_missing_candles_v1
```

Split guidance:

- `train`: long enough for candidate exploration, `missing_count = 0`, and not overly concentrated in one market regime.
- `validation`: separate from train, `missing_count = 0`, and usable as candidate-selection evidence.
- `final_holdout`: time-separated from validation, `missing_count = 0`, and not repeatedly reused during candidate search.

## Preflight: Config and Readiness

Inspect config first:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot config-dump --masked
```

Run readiness before expensive research:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
DB_PATH="$DATA_ROOT/paper/trades/paper.sqlite" \
uv run bithumb-bot research-readiness --manifest "$MANIFEST"
```

`research-readiness` does not prove the full SQLite DB is dense or complete. It verifies whether the selected manifest split ranges are usable from the configured dataset source. Readiness is split-range evidence, not full-DB completeness evidence.

For candle-only `research_only` runs, require all of these before running a sweep or diagnostic backtest:

- `status = PASS`
- `next_actions = ["none"]`
- every required split has `missing_count = 0`
- every required split has `coverage_pct = 100.0`
- every required split has `quality_status = PASS`
- duplicate candle keys, interval mismatches, OHLC violations, non-positive prices, and negative volumes are all zero

JSON output to a repo-external runtime report path:

```bash
set -o pipefail
mkdir -p "$DATA_ROOT/paper/reports/research/readiness"

BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
DB_PATH="$DATA_ROOT/paper/trades/paper.sqlite" \
uv run bithumb-bot research-readiness --manifest "$MANIFEST" --json \
  | tee "$DATA_ROOT/paper/reports/research/readiness/readiness.preview.json"
```

Focused readiness summary:

```bash
jq '{
  status,
  db_path,
  market,
  interval,
  splits: (.splits // [] | map({
    split: (.split // .name),
    start: (.start // .start_utc),
    end: (.end // .end_utc),
    missing_count,
    coverage_pct,
    quality_status
  })),
  next_actions
}' "$DATA_ROOT/paper/reports/research/readiness/readiness.preview.json"
```

Inspect:

- `status`
- `manifest_path`
- `manifest_hash`
- `mode`
- `db_path`
- `env_file`
- `env_loaded`
- `env_exists`
- `market`
- `interval`
- `splits`
- `top_of_book`
- `execution_capability`
- `execution_calibration`
- `walk_forward`
- `next_actions`

### Audited Candle Source Boundary

For the audited local WSL research runtime, the reviewed long-range candle source for diagnostic research backtests is:

```text
$DATA_ROOT/paper/trades/paper.sqlite
```

Do not treat live DBs, live backups, retry logs, readiness reports, or backtest report artifacts as alternate candle source DBs. Those files are operational evidence or generated artifacts, not research backtest source datasets.

This statement is scoped to the audited local WSL runtime. It must not turn `$HOME/bithumb-runtime` or any other example runtime root into a universal required storage location.

### Live SQLite Boundary

`$DATA_ROOT/live/trades/live.sqlite` is runtime observation evidence. It may be sparse, partial, or live-runtime-specific. It is not the canonical long-range candle source for research backtests.

Do not use `live.sqlite` as the research backtest source DB. Do not copy OHLCV values from `live.sqlite` over `paper.sqlite`. If live observations reveal a data issue, refresh or replace the research dataset through the reviewed paper/research data path, then rerun manifest-level readiness and data-quality checks.

<details>
<summary>Observed live vs paper SQLite comparison</summary>

Non-normative observed comparison notes from one audited local WSL runtime:

```text
live DB path form:
$DATA_ROOT/live/trades/live.sqlite

market: KRW-BTC
interval: 1m
live_rows: 13,217
live_first_utc: 2026-04-03 23:50:00
live_last_utc: 2026-04-30 04:01:00

same_ts_rows = 13,217
ohlcv_diff_rows = 8
live_only_ts = 0
paper_only_ts_inside_live_range = 24,181
```

`live_only_ts = 0` does not make `live.sqlite` a better research source. The large `paper_only_ts_inside_live_range` count shows broader candle coverage in `paper.sqlite` for the compared interval. Differing OHLCV rows are another reason live observations must not be copied over paper candles.

</details>

## Research Completion Notifications

Notification settings are runtime/operator configuration. Do not put `NTFY_TOPIC`, `NTFY_SERVER`, webhook URLs, or notification secrets in a research manifest.

The manifest defines the research hypothesis, data, candidates, cost model, and validation policy. Notification delivery belongs in the explicit env file or a CLI policy override.

The WSL env example above uses `RESEARCH_NOTIFICATION_POLICY=disabled` for quiet local diagnostic runs. For notification-enabled WSL research, use a repository-external env file and document only placeholder values, never a real private topic:

```bash
cat >> "$BITHUMB_WSL_ROOT/env/paper.research.env" <<'EOF'
NOTIFIER_ENABLED=true
NTFY_TOPIC=<topic>
NTFY_SERVER=https://ntfy.sh
RESEARCH_NOTIFICATION_POLICY=best_effort
EOF
```

Diagnose the loaded notification configuration:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot notification-diagnose --json
```

Probe delivery before an expensive run:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot notification-diagnose --probe
```

Normal notification-enabled diagnostic backtest:

```bash
set -o pipefail
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
DB_PATH="$DATA_ROOT/paper/trades/paper.sqlite" \
uv run bithumb-bot research-backtest --manifest "$MANIFEST"
```

Strict completion notification policy:

```bash
set -o pipefail
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
DB_PATH="$DATA_ROOT/paper/trades/paper.sqlite" \
uv run bithumb-bot research-backtest \
  --manifest "$MANIFEST" \
  --notification-policy require_delivery
```

`best_effort` lets the research command complete even if notification delivery fails, while recording the delivery result. Use `require_delivery` when the command must fail if notification delivery is not configured or the completion notification is not delivered.

The same `--notification-policy` option is available on `research-backtest`, `research-walk-forward`, and `research-validate`.

Notification delivery results are written to:

```text
DATA_ROOT/<mode>/reports/notifications/notification_events.jsonl
```

The outbox record stores delivery metadata such as `message_hash`, `final_status`, `attempted_transports`, `delivered_transports`, `failure_classes`, `http_statuses`, and `source_command`; it does not store the raw message text.

## Official Validation Path

Use `research-validate` for the normal validation lifecycle:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot research-validate --manifest "$MANIFEST"
```

With execution calibration:

```bash
BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
uv run bithumb-bot research-validate \
  --manifest "$MANIFEST" \
  --execution-calibration "$DATA_ROOT/paper/reports/execution_quality/<calibration>.json"
```

`research-validate` is the normal validation lifecycle command. It can run readiness, backtest, policy-required walk-forward, promotion, reproduce, and write `validation_run.json`.

Validation stages include:

- readiness
- dataset_quality
- backtest
- final_holdout
- stress_suite
- statistical_validation
- final_selection
- walk_forward
- promotion_eligibility
- promotion
- reproduce

## Diagnostic Backtest Path

Run diagnostic/development backtests only after readiness PASS for the selected manifest and source DB:

```bash
set -o pipefail
mkdir -p "$DATA_ROOT/paper/reports/research/diagnostic"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
BACKTEST_LOG="$DATA_ROOT/paper/reports/research/diagnostic/research-backtest.$RUN_TS.log"
READINESS_ARCHIVE="$DATA_ROOT/paper/reports/research/diagnostic/readiness.before-backtest.$RUN_TS.json"

BITHUMB_ENV_FILE="$BITHUMB_WSL_ROOT/env/paper.research.env" \
DB_PATH="$DATA_ROOT/paper/trades/paper.sqlite" \
uv run bithumb-bot research-backtest --manifest "$MANIFEST" \
  2>&1 | tee "$BACKTEST_LOG"

cp "$DATA_ROOT/paper/reports/research/readiness/readiness.preview.json" \
  "$READINESS_ARCHIVE"
```

The `tee` output and copied readiness JSON preserve diagnostic evidence under repo-external runtime report directories.

A successful `research-backtest` process exit means the diagnostic command completed. It does not mean the strategy is production-ready, promotion-ready, paper-ready, live-ready, execution-quality-approved, capital-allocation-ready, or bound into the full validation lifecycle.

If walk-forward is required, a standalone report may correctly show `standalone_backtest_not_full_validation=true` or `walk_forward_required_but_not_executed_in_this_run`. In that case, run `research-validate` for the full lifecycle.

## Smoke Backtest Boundary

Do not use this as the research validation path:

```bash
python backtest.py
```

Explicit smoke-only execution is:

```bash
python backtest.py --diagnostic-smoke-only
```

Root `backtest.py` is a fail-closed diagnostic smoke wrapper only. Smoke output is non-promotable and must not be used for strategy promotion, approved profiles, live readiness, or capital allocation.

## Artifact Locations

Research outputs belong under managed runtime roots, not under the Git repository:

```text
DATA_ROOT/<mode>/reports/research/<experiment_id>/
DATA_ROOT/<mode>/derived/research/<experiment_id>/
DATA_ROOT/<mode>/reports/research/<experiment_id>/validation_run.json
DATA_ROOT/<mode>/reports/research/<experiment_id>/backtest_report.json
DATA_ROOT/<mode>/reports/research/<experiment_id>/walk_forward_report.json
DATA_ROOT/<mode>/reports/research/<experiment_id>/promotion_<candidate_id>.json
DATA_ROOT/<mode>/reports/notifications/notification_events.jsonl
```

Reports are operator-readable runtime artifacts. Derived research outputs are computed intermediates. Keep both under repo-external `DATA_ROOT`.

## Observed WSL Candle Source Snapshot

This is observed local WSL operational evidence, not a universal storage contract and not a claim that the values remain current. The audited candle DB path form is:

```text
$DATA_ROOT/paper/trades/paper.sqlite
```

Observed non-normative notes from one local WSL candle source:

| Field | Observed value |
| --- | --- |
| `market` | `KRW-BTC` |
| `interval` | `1m` |
| `rows` | `1,743,415` |
| `distinct_ts` | `1,743,415` |
| `duplicate_ts` | `0` |
| `first_utc` | `2023-01-01 00:00:00` |
| `last_utc` | `2026-05-01 23:59:00` |

The full DB was not treated as a completely dense 1-minute DB. Operators must rerun readiness and data-quality checks after refreshing or replacing datasets.

<details>
<summary>Observed research-only readiness PASS example</summary>

This is non-normative, dataset-specific readiness evidence from one local WSL runtime:

```text
manifest:
$DATA_ROOT/paper/reports/research/manifests/sma_filter_mh45_stop_loss_sweep_parallel_w4.json

market = KRW-BTC
interval = 1m
deployment_tier = research_only
dataset.source = sqlite_candles
dataset.snapshot_id = clean_segments_without_missing_candles_v1

train:
2024-01-05 ~ 2024-02-10
expected = 53,280
present  = 53,280
missing  = 0
coverage = 100.0
quality_status = PASS

validation:
2024-10-14 ~ 2024-11-27
expected = 64,800
present  = 64,800
missing  = 0
coverage = 100.0
quality_status = PASS

final_holdout:
2026-01-01 ~ 2026-02-28
expected = 84,960
present  = 84,960
missing  = 0
coverage = 100.0
quality_status = PASS
```

The example shows that a manifest can be usable even when the full DB has missing buckets, as long as the selected split ranges are clean. This is `research_only` evidence, not production readiness, paper approval, or live readiness.

</details>

<details>
<summary>Observed missing candle notes</summary>

Non-normative observed gap notes:

- `gap_ranges = 2,513`
- `total_missing_buckets = 9,065`
- `max_gap_buckets = 629`
- representative large gaps:
  - `629 minutes`
    - `UTC: 2025-03-23 15:31 ~ 2025-03-24 01:59`
    - `KST: 2025-03-24 00:31 ~ 2025-03-24 10:59`
  - `420 minutes`
    - `UTC: 2026-03-29 16:00 ~ 2026-03-29 22:59`
    - `KST: 2026-03-30 01:00 ~ 2026-03-30 07:59`
  - `389 minutes`
    - `UTC: 2025-10-04 16:01 ~ 2025-10-04 22:29`
    - `KST: 2025-10-05 01:01 ~ 2025-10-05 07:29`

Treat these observed gaps as persistent dataset-quality evidence unless repaired by a reviewed backfill/retry process. Do not assume they are WSL copy errors. Do not weaken readiness gates because gaps remain after retries.

</details>

## Report Inspection Commands

Backtest report inspection:

```bash
REPORT="$DATA_ROOT/paper/reports/research/<experiment_id>/backtest_report.json"

jq '{
  manifest_hash,
  dataset_content_hash,
  dataset_quality_hash,
  dataset_quality_gate_status,
  dataset_quality_gate_reasons,
  content_hash,
  best_candidate_id,
  promotion_eligibility_gate_result,
  promotion_blocking_reasons,
  promotion_allowed,
  next_action
}' "$REPORT"
```

Validation run inspection:

```bash
VALIDATION_RUN="$DATA_ROOT/paper/reports/research/<experiment_id>/validation_run.json"

jq '{
  validation_run_id,
  experiment_id,
  manifest_hash,
  validation_policy_source,
  validation_policy_required_stage_names,
  required_stage_names,
  selected_candidate_id,
  backtest_report_hash,
  walk_forward_report_hash,
  promotion_artifact_hash,
  reproduce_ok,
  promotion_allowed,
  end_to_end_validation_result,
  fail_closed_reasons
}' "$VALIDATION_RUN"

jq '.stages[] | {
  name,
  required,
  status,
  reasons,
  artifact_paths,
  artifact_hashes
}' "$VALIDATION_RUN"
```

## Parallel Research on WSL

`PYTEST_XDIST_WORKERS` controls pytest workers only. It does not control
`research-backtest` or `research-validate` research worker processes, and it
must not be used as a substitute for research CLI worker configuration.

WSL `research-backtest` parallelism is configured in the manifest. For normal
WSL `research-backtest` guidance, request up to 8 research workers with this
manifest execution policy:

```json
"research_run": {
  "execution": {
    "mode": "parallel",
    "max_workers": 8,
    "process_start_method": "auto_safe",
    "work_unit": "candidate_scenario"
  }
}
```

In this runbook, "8 workers" means up to 8 research worker processes requested
by the manifest. It does not mean 8 CPU cores are reserved, and it is not proof
that 8 workers actually ran.

Optional caps:

```bash
export BITHUMB_RESEARCH_MAX_WORKERS=4
export BITHUMB_TOTAL_PROCESS_BUDGET=6
```

Actual or effective workers may be lower than requested because of
`BITHUMB_RESEARCH_MAX_WORKERS`, `BITHUMB_TOTAL_PROCESS_BUDGET`, WSL resource
limits, or too few candidate/scenario work units. The generated report's
`execution_observability` is the evidence for actual execution.

Inspect the latest generated report:

```bash
REPORT="$(find "$DATA_ROOT/paper/reports/research" -name backtest_report.json -print | sort | tail -1)"
jq '.execution_observability' "$REPORT"
```

Check at least:

- `requested_execution_mode`
- `requested_max_workers`
- `actual_execution_mode`
- `parallel_executor_used`
- `research_max_workers_requested`
- `research_max_workers_effective`
- `requested_process_start_method`
- `effective_process_start_method`
- `work_units`

## Disk and Workspace Safety

Before runs:

```bash
df -h /
du -sh "$BITHUMB_WSL_ROOT" /tmp/bithumb-bot-pytest-* /tmp/pytest-of-$USER 2>/dev/null || true
./scripts/check_repo_runtime_artifacts.sh
```

After runs:

```bash
./scripts/check_repo_runtime_artifacts.sh
df -h /
du -sh "$BITHUMB_WSL_ROOT" /tmp/bithumb-bot-pytest-* /tmp/pytest-of-$USER 2>/dev/null || true
```

Do not clean up by deleting random files inside the Git repository. Generated runtime and research artifacts should not be there in the first place.

## Failure Interpretation

| Symptom | Meaning | Next action |
| --- | --- | --- |
| `python backtest.py` exits 2 | Expected fail-closed smoke wrapper behavior | Use `research-validate --manifest <path>` |
| empty or non-file `MANIFEST` | The command has no reviewed manifest input | Set `MANIFEST` to a non-empty JSON file under the repo-external runtime reports tree and run JSON/JQ inspection |
| `research-readiness` fails | Dataset/env/calibration/walk-forward prerequisite is not ready | Inspect `next_actions`; fix data/env/manifest first |
| split-level missing candles in readiness | The selected manifest split is not usable from the configured dataset source | Use clean segments; run targeted backfill/retry when appropriate; classify remaining gaps as persistent dataset evidence; do not use missing ranges for validation or final holdout; rerun readiness before backtest or validation |
| `dataset_quality_gate_status=FAIL` | Dataset evidence failure | Fix dataset or manifest; do not tune strategy around it |
| attempted use of `live.sqlite` as research backtest source | Live runtime observation evidence is being misused as research source data | Stop and use `$DATA_ROOT/paper/trades/paper.sqlite` or a reviewed immutable research dataset |
| treating full-DB coverage as sufficient without manifest readiness | Whole-DB summaries do not prove selected split readiness | Run `research-readiness --manifest "$MANIFEST"` against the exact source DB and require split-level PASS |
| `walk_forward_required_but_not_executed_in_this_run` | Standalone diagnostic backtest did not run full lifecycle | Run `research-validate` |
| `promotion_allowed=0` | Candidate is not promotable | Do not run profile generation or live readiness from this evidence |
| `validation_run_not_passed` | Full validation did not pass | Inspect `.stages[]` in `validation_run.json` |
| `notification_policy=require_delivery notifier_unconfigured` | Strict notification policy was requested, but notifier configuration is missing or disabled | Configure notification settings in a repository-external env file or use `best_effort`/`disabled` for diagnostic runs |
| `require_delivery` run exits non-zero after command completion | The research command completed, but the completion notification was not delivered | Inspect `DATA_ROOT/<mode>/reports/notifications/notification_events.jsonl`; fix notifier delivery before treating the run as strict-policy complete |
| repo artifact checker fails | Runtime/research artifacts leaked into repo | Move outputs to managed runtime roots and fix path usage |

## Minimum WSL Research Backtest Checklist

```text
[ ] Current shell is in the Git repository root.
[ ] `MANIFEST` is set and points to a real JSON file.
[ ] Manifest `market` is `KRW-BTC`.
[ ] Manifest `interval` is `1m`.
[ ] `DB_PATH` points to `$DATA_ROOT/paper/trades/paper.sqlite`.
[ ] `research-readiness --json` was run for this exact manifest and DB.
[ ] `status = PASS`.
[ ] `next_actions = ["none"]`.
[ ] Train split `missing_count = 0`.
[ ] Validation split `missing_count = 0`.
[ ] Final holdout split, when present, `missing_count = 0`.
[ ] The result is understood as `research_only` when the manifest is `research_only`.
[ ] Readiness JSON is preserved under repo-external runtime reports.
[ ] Diagnostic backtest log path is repo-external.
```

## Do Not Do

- Do not run `python backtest.py` as the official research path.
- Do not run `uv run bithumb-bot` from the runtime root.
- Do not treat smoke output as promotion evidence.
- Do not treat standalone `research-backtest` success as paper/live readiness.
- Do not run readiness or backtest with an empty `MANIFEST`.
- Do not use `live.sqlite` as the research backtest source DB.
- Do not overwrite `paper.sqlite` with OHLCV values from `live.sqlite`.
- Do not assume arbitrary full-DB ranges are backtest-safe without manifest-level readiness.
- Do not run sweep or backtest from a manifest that has not passed readiness.
- Do not interpret `research_only` PASS as production or live readiness.
- Do not use validation or final-holdout results with missing candles as promotion evidence.
- Do not synthesize OHLCV candles to bypass dataset quality gates.
- Do not weaken gates because missing buckets appear persistent after retries.
- Do not write `DATA_ROOT`, `DB_PATH`, reports, derived artifacts, traces, or logs into the repository.
- Do not use `./data`, `./tmp`, `./backups`, or repo-root `*.log` for runtime artifacts.
- Do not edit generated report hashes, registry rows, validation runs, or promotion artifacts by hand.
- Do not tune runtime env values until a backtest looks good.
- Do not put real ntfy topics, webhook URLs, or notification secrets in manifests, docs, logs, or examples; keep them in repository-external env files.
- Do not use native Windows path behavior as runtime correctness evidence.

## Related Documents

- `AGENTS.md`
- `README.md`
- `docs/storage-layout.md`
- `docs/runtime-data-policy.md`
- `docs/research-validation.md`
- `docs/runbooks/research-to-paper.md`
- `docs/pre-merge-checklist.md`
