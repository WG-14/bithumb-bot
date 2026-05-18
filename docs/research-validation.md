# Research Validation Lifecycle

This repository separates research-stage candidate variables from runtime env values.
Research manifests define hypotheses, data splits, parameter spaces, cost models, and acceptance gates.

Root `backtest.py` and any simple close-price SMA script are smoke backtests only. This is a smoke backtest only. It must not be used as evidence for strategy promotion, approved profiles, live readiness, or capital allocation. For production-bound targets, the normal custody path is `research-validate`. It runs the policy-required lifecycle stages, creates the promotion artifact, reproduces it, and writes the hash-bound `validation_run.json` operator record. `research-backtest` and `research-walk-forward` remain diagnostic/development commands unless a specific runbook says otherwise; process success from a diagnostic command must not be interpreted as promotion-grade validation success. A standalone `research-backtest` may correctly report `validation_run_complete=false`, `diagnostic_only=true`, `standalone_backtest_not_full_validation=true`, and `walk_forward_required_but_not_executed_in_this_run` when walk-forward is required. That marker is a diagnostic truth about the standalone command, not a reason for `research-validate` to stop before running the policy-required walk-forward stage.
Runtime env/profile values should be treated as verified outputs of that process, not mutable knobs to tune until a backtest looks good.

## Validation Run Stages

`validation_run.json` is the operator-facing custody record for research-to-paper validation. The validation stage list is policy-driven from `deployment_tier`; a manifest may require stricter evidence, but production-bound tiers cannot weaken minimum required stages.

Current stages are:

```text
readiness
dataset_quality
backtest
final_holdout
stress_suite
statistical_validation
final_selection
walk_forward
promotion_eligibility
promotion
reproduce
```

Each stage records `name`, `required`, `status`, `input_hashes`, `output_hashes`, `artifact_paths`, `artifact_hashes`, and `reasons`. `validation_run.json` also records `validation_policy_source`, `validation_policy_required_stage_names`, and effective requirement booleans such as `effective_walk_forward_required`, `effective_final_holdout_required`, `effective_stress_suite_required`, `effective_statistical_validation_required`, and `effective_final_selection_required`. Required stages fail closed when report evidence is missing, failed, unavailable, stale, or screening-only. Promotion eligibility is evaluated after the required evidence sequence has run, so a pre-walk-forward standalone backtest marker does not prevent `research-validate` from executing walk-forward. Recovery is to regenerate or rebind evidence through the research commands from the fixed manifest and managed runtime paths; do not manually edit hashes, reports, JSONL registries, or evidence artifacts.

Production-bound `paper_candidate`, `live_dry_run_candidate`, and `small_live_candidate` validation requires final holdout, walk-forward, stress suite, statistical validation, final selection, promotion eligibility, promotion, and reproduce stages. Execution calibration, audit/trace evidence, approved-profile chain readiness, and related policy checks remain enforced by the existing report, promotion, reproduction, and profile gates; the validation-run stages expose those outcomes instead of weakening them.

## Audit Trail Evidence

`research_run.report_detail=summary` is a compact diagnostic/reporting mode. It
is not, by itself, a complete experiment record. Embedded report retention caps
such as `max_decisions_retained` and `max_equity_points_retained` control report
preview size only; they must not be treated as forensic audit retention.

Complete replay evidence is written as external candidate/scenario/split trace
artifacts when `research_run.audit_trail.mode=complete_external` is enabled, or
when legacy `research_run.artifact_policy.full_decisions_external_jsonl=true` is
used. The managed storage location is:

```text
DATA_ROOT/<mode>/derived/research/<experiment_id>/
  traces/
    <candidate_id>/
      <scenario_id>/
        <split_name>/
          decisions.jsonl
          equity.jsonl
          executions.jsonl
          trace_index.json
  trace_manifest.json
```

The trace files are classified as `data/<mode>/derived` because they are
research replay evidence, not live order lifecycle recovery state. Each JSONL row
contains the experiment id, manifest hash, dataset content hash, candidate id,
scenario id, split, sequence number, event payload, payload hash, previous event
hash, and event hash. `trace_index.json` records row counts, first/last
timestamps, stream hashes, hash-chain head/tail, and terminal completion status.
`trace_manifest.json` aggregates all per-split indexes and has its own content
hash.

Reports expose the audit policy, audit status, trace manifest ref/path/hash, and
verification result. They do not embed full trace rows. Operators can verify a
completed trace set with:

```bash
uv run bithumb-bot research-verify-audit --experiment-id <experiment_id>
```

Verification fails closed for missing manifests, missing indexes, missing stream
files, row-count mismatches, stream hash mismatches, hash-chain mismatches,
report/reference hash mismatches, and non-terminal trace statuses.
`completed`, `failed`, and `aborted` are terminal audit statuses. A failed or
aborted candidate can therefore have valid forensic evidence even though it is
not promotion-eligible.

Re-running the same experiment id rewrites the exact candidate/scenario/split
trace streams and `trace_manifest.json` for that managed scope before appending
new rows. Operators should treat the report-bound `audit_trail_trace_manifest_ref`
and `audit_trail_trace_manifest_hash` as the custody binding for the current run.
Do not merge old trace rows into a new report by copying JSONL files.

Walk-forward runs in complete external mode trace each rolling
`window_NNN_train` and `window_NNN_test` split. Each window row carries
`train_audit_trace_index` and `test_audit_trace_index`, and the top-level
`trace_manifest.json` includes those indexes alongside train, validation, and
final-holdout indexes.

Production/promotion-bound evidence requires complete trace evidence when the
audit policy marks it required for promotion. Missing or corrupt trace evidence
adds machine-readable audit fail reasons such as
`audit_trail_trace_manifest_missing`, `audit_trail_trace_index_missing`,
`audit_trail_decision_stream_missing`, `audit_trail_equity_stream_missing`,
`audit_trail_execution_stream_missing`, `audit_trail_hash_chain_mismatch`,
`audit_trail_row_count_mismatch`, `audit_trail_non_terminal_status`, and
`audit_trail_required_for_promotion`.
Promotion, `research-registry-validate`, and `research-reproduce` re-open the report-bound trace
manifest, recompute its content hash, and rerun JSONL/index verification against
the current files. If a trace file is deleted or tampered with after report
generation, promotion fails before writing an artifact and reproduction fails
closed before approving the artifact chain. Recovery is to rerun the
research command from the manifest and dataset so fresh traces, report hashes,
statistical evidence, and registry bindings are generated together; do not patch
hashes or edit trace files by hand.

Candidate subprocess isolation remains explicitly pending unless a report shows
real worker-process evidence. The current in-process evaluator reports
`subprocess_candidate_isolation_pending`; operators must not treat that field as
implemented process isolation until each candidate/scenario/split has worker PID,
exit, timeout/resource, seed, and terminal trace-status evidence.

## Cost Assumption Contract

Production-bound manifests (`paper_candidate`, `live_dry_run_candidate`, and `small_live_candidate`) must define explicit execution scenarios with typed cost assumptions. At least one scenario must be a `base` cost assumption with a label, fee source, fee authority policy, slippage source, and `promotable_as_base=true`. Stress scenarios must be labeled as stress and are survival evidence only; they are not runtime base cost authority.

Use the realistic account fee as the base assumption. For the current Bithumb app-fee operating reference, examples use `fee_rate=0.0004` and `slippage_bps=10` for the base scenario. A `fee_rate=0.0025` scenario is stress/conservative validation unless the operator explicitly documents that it is the actual account fee tier. Legacy top-level `cost_model` remains accepted for `research_only` compatibility, but it is marked as legacy cost provenance and cannot satisfy production-bound promotion requirements by itself.

Approved profiles bind the promoted base cost assumption into the profile hash. Runtime verification compares that base fee/slippage against the effective runtime contract. On mismatch, run:

```bash
uv run bithumb-bot config-dump --masked
```

Then regenerate or select an approved profile whose base cost assumption matches the current runtime fee/slippage contract, or adjust the runtime env and rerun the dump.

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

## Statistical Evidence Grades

Research reports now separate screening evidence from promotion-grade statistical evidence.
The existing centered max bootstrap over candidate summary metrics is
`SCREENING_SUMMARY_BOOTSTRAP` only. It may expose
`summary_metric_max_bootstrap_p_value` / `selection_adjusted_summary_p_value`,
but it must not populate `white_reality_check_p_value` or claim a full White's
Reality Check method.
Official `research-backtest` manifest parsing accepts
`statistical_validation.bootstrap.method=metric_centered_max_bootstrap` for
screening evidence and `white_reality_check_block_bootstrap` for promotion-grade
WRC evidence. The WRC path only generates `PROMOTION_GRADE_WRC` when the official
candidate return panel is an aligned `portfolio_bar_return` panel built from
retained bar-level equity curves. If that panel cannot be generated or validated,
the run remains fail-closed with machine-readable promotion-grade limitations.

Production-bound promotion requires `PROMOTION_GRADE_WRC` or
`PROMOTION_GRADE_WRC_SPA_DSR` evidence. If only screening evidence is present,
promotion fails closed with `statistical_evidence_grade_insufficient`.
Production-bound validation also marks the `statistical_validation` stage
non-passable for screening-only evidence such as `SCREENING_SUMMARY_BOOTSTRAP`
or unavailable promotion-grade capability. Stage reasons include explicit
machine-readable statuses such as `SCREENING_ONLY_NOT_PROMOTABLE`,
`UNAVAILABLE_CAPABILITY`, and `MISSING_RETURN_PANEL` where applicable.
SPA and Deflated Sharpe gates remain fail-closed when configured and unavailable;
no placeholder p-values are acceptable.

Every statistical evidence artifact is bound to a canonical
`candidate_return_panel` artifact. When all candidates retain complete and
matching validation equity curves, the official panel is an aligned
`portfolio_bar_return` panel with a cash benchmark, excess-return series,
ordered bar time index, candidate ids, scenario ids where present, parameter
values, observation counts, missing-observation policy, return unit, split,
metadata hashes, series hashes, and content hashes. This panel is marked
`promotion_grade_available=true` and is the only current input accepted for
official WRC generation.

When embedded equity curves are omitted by summary-mode retention caps but a
complete external audit trail is present, the return-panel builder uses the
external equity traces as the preferred source for aligned
`portfolio_bar_return` generation. If trace evidence is missing, malformed, or
unaligned, promotion-grade return-panel evidence remains unavailable and the
statistical path fails closed rather than falling back to summary previews.

If the aligned panel cannot be generated, the official path falls back to the
smallest honest diagnostic panel available from the engine: validation split
`trade_return` series derived from closed trade records. That artifact is
machine-marked with `promotion_grade_available=false`,
`official_candidate_equity_curve_missing_or_unaligned`,
`trade_return_panel_cannot_satisfy_promotion_grade_wrc`, and
`official_wrc_generation_requires_aligned_bar_return_panel`. It must not be
treated as WRC input. Promotion-grade methods that require unavailable return
units fail closed rather than infer precision.
`sharpe_like` is not accepted for production-bound statistical selection because
it is not period-return Sharpe evidence and must not fall back to `return_pct`.

For `multiple_testing_scope=experiment_family`, the run must bind a durable
family trial registry at
`DATA_ROOT/<mode>/reports/research/families/<experiment_family_id>/trial_registry.jsonl`.
The registry records experiment id, manifest hash, hypothesis metadata, attempt
index, holdout reuse count, dataset hash, parameter-space hash, candidate count,
return panel hash, statistical evidence hash, result status, and row hash. The
current registry binding uses an explicit two-phase design: the row records the
pre-registry evidence hash and the finalized evidence records the row hash, which
avoids a recursive hash cycle while keeping both artifacts reproducible. Missing,
stale, tampered, or partially mismatched registry evidence fails closed; the
system must not silently fall back to current-experiment-only selection. Family-wide
statistical aggregation is still not implemented, so production-bound promotion
claims under `experiment_family` fail closed until the registry contributes to the
actual statistical universe.

Production-bound final-holdout exposure is also bound to the experiment attempt
registry at `DATA_ROOT/<mode>/reports/research/_registry/experiment_registry.jsonl`.
This registry is promotion-grade audit evidence, not a metadata ledger. It
records reserved, completed, rejected, promoted, and aborted attempt events as
append-only JSONL rows with row hashes and prior-registry hashes. Manual JSONL or
hash editing is not valid recovery.

For production-bound manifests, checked reservation happens before the
`final_holdout` split is loaded. The reservation row uses only the semantic
final-holdout identity needed for reuse counting. The final-holdout split hash,
content hash, full dataset content hash, and full dataset quality hash are bound
later in the append-only `research_attempt_completed` row and in the report /
statistical evidence artifacts. A reservation row with
`final_holdout_content_pending_until_completion=true` is therefore not missing
content evidence; it is a pre-content reservation that must have matching
completion/artifact content before promotion.

The registry separates final-holdout meaning from artifact integrity:

- `final_holdout_identity_hash` is the semantic reuse-counting key based on
  dataset source, market, interval, final-holdout start, and final-holdout end.
- `final_holdout_reuse_key_hash` is the key used to compute
  `computed_holdout_reuse_count` and should equal the semantic identity hash.
- `final_holdout_content_hash` hashes reproducibility/integrity material such as
  dataset snapshot id, final-holdout split hash, and dataset quality hash.
- `final_holdout_fingerprint` is retained as a compatibility alias for the
  semantic identity hash.

Reuse counting uses the semantic identity hash, not byte-identical split content.
Changing `dataset_snapshot_id`, refilling candles, or changing split content
does not reset `computed_holdout_reuse_count` for the same market/interval/date
range. Content hash mismatches are still fail-closed integrity failures and are
reported separately from identity/reuse-key mismatches.

Identity-source provenance is part of the audit chain. Registry rows, reports,
lineage, statistical evidence, candidate profiles, promotion artifacts,
promotion lineage, and approved profiles carry `hypothesis_identity_source` and
`experiment_family_identity_source`. These fields explain whether identity came
from explicit manifest ids, `manifest.hypothesis`, or the experiment id fallback.
If a registry row has fallback-derived identity and a later artifact omits or
changes that source, validation treats it as stale or incomplete provenance
rather than silently accepting a missing comparison.

Experiment-registry completion uses explicit two-phase evidence binding to avoid
a recursive hash cycle:

1. Statistical evidence is built without
   `experiment_registry_completion_row_hash`.
2. The pre-completion evidence `content_hash` is recorded in the
   `research_attempt_completed` row as `statistical_evidence_hash` with
   `statistical_evidence_hash_phase=pre_completion_evidence_hash`.
3. The final statistical evidence then records
   `experiment_registry_completion_row_hash`,
   `experiment_registry_bound_evidence_hash`, and
   `experiment_registry_evidence_hash_phase=pre_completion_evidence_hash`, and
   its final `content_hash` is recomputed.

Promotion and reproduction for `paper_candidate`, `live_dry_run_candidate`, and
`small_live_candidate` fail closed when the registry path is missing, row hashes
or prior hashes do not recompute, a completed/aborted/final evidence binding is
missing or stale, evidence phase is missing or wrong, identity/content/reuse-key
hashes mismatch, computed counters mismatch, budgets are exceeded, or completion
status is not promotion-permitted. `research_only` outputs may retain diagnostic
warnings, but those warnings cannot bypass production-bound promotion.

Production-bound manifests that declare `attempt_index` or
`holdout_reuse_count` are checked under the same registry lock that appends the
reservation. The locked operation computes `computed_attempt_index` and
`computed_holdout_reuse_count`, checks declared counters and configured
statistical budgets, then appends exactly one event. A declared/computed
mismatch or budget excess appends an uncounted `research_attempt_rejected` event
for audit visibility and fails closed with
`declared_attempt_index_mismatch` and/or
`declared_holdout_reuse_count_mismatch`, and when applicable
`experiment_registry_budget_exceeded`, `attempt_budget_exceeded`, or
`holdout_reuse_budget_exceeded`. Rejected events do not increment future attempt
or holdout reuse counters and must not leave a stale counted reservation.

Registry lifecycle status is separate from statistical gate result:

- `result_status=IN_PROGRESS` for a counted reservation.
- `result_status=COMPLETED` for a completed lifecycle event.
- `result_status=ABORTED` for an interrupted counted attempt.
- `result_status=REJECTED` for an uncounted preflight rejection.

Only `COMPLETED` is promotion-permitted at the lifecycle layer.
`statistical_gate_result=PASS|FAIL|UNKNOWN` remains separate evidence about the
candidate/statistical gate. A completed attempt with
`statistical_gate_result=FAIL` is valid completed audit evidence, but promotion
still fails unless the candidate, statistical, stress, execution, evidence-grade,
and artifact-binding gates all pass.

If a run is interrupted after a counted reservation, use the append-only
operator commands:

```bash
uv run bithumb-bot research-registry-inspect --row-hash <reservation_row_hash>
uv run bithumb-bot research-registry-validate --experiment-id <experiment_id>
uv run bithumb-bot research-mark-attempt-aborted --row-hash <reservation_row_hash> --reason "<operator reason>"
```

Aborted attempts remain counted exposure, are not promotion-permitted, and must
not be repaired by editing existing JSONL rows.

`research-registry-validate --experiment-id <id>` reports its validation scope.
When `backtest_report.json` is unavailable, output is `validation_scope=registry_only`,
`artifact_binding_valid=unknown`, `evidence_loaded=false`, and
`warning=artifact_binding_not_checked`; this means the command validated registry
row shape and lifecycle only, not promotion-grade artifact binding. When the
report is present under `DATA_ROOT/<mode>/reports/research/<id>/`, output is
`validation_scope=registry_and_artifacts`. The command determines one
`artifact_bound_row_hash` from
`statistical_selection_evidence.experiment_registry_row_hash`, falling back to
`backtest_report.experiment_registry_row_hash`, validates artifact binding for
that row exactly once, and reports `artifact_binding_valid` plus
`artifact_reasons` for content-hash, registry-binding, evidence-binding, or
return-panel mismatches. All reservation rows for the experiment remain visible
in `registry_lifecycle_summary`; extra incomplete or aborted rows are lifecycle
evidence, not proof that the current artifacts are bound to those rows. If
report and evidence disagree on the bound row hash, validation fails closed with
`experiment_registry_report_evidence_row_hash_mismatch`. If the artifact-bound
row is absent from the registry, validation fails closed with
`experiment_registry_artifact_bound_row_missing`. Operators must not treat
registry-only validation as proof that the final report/evidence/promotion
artifact chain is current.

Each `registry_lifecycle_summary` row separates row validity from promotion
permission:

- `registry_row_valid=true` means the reservation row hash recomputes.
- `completion_row_valid=true` means the completion or abort row hash recomputes
  when such a row exists; it is also true when no completion row exists.
- `lifecycle_complete=true` means the lifecycle status is promotion-permitted,
  currently only `COMPLETED`.
- `promotion_permitted=true` mirrors lifecycle completion at the registry layer.
- `ok=true` means the registry row is valid, the completion/abort row is valid,
  and the lifecycle is promotion-permitted.
- `row_valid_only=true` means the reservation row is hash-valid but the
  lifecycle is not promotion-permitted. This commonly appears for old
  `IN_PROGRESS` or `ABORTED` rows that remain useful exposure evidence but are
  not valid promotion evidence.

An incomplete non-bound row can therefore appear with `registry_row_valid=true`,
`lifecycle_complete=false`, `promotion_permitted=false`, `row_valid_only=true`,
and `ok=false` without failing the current artifact-bound validation. An
artifact-bound row with the same incomplete lifecycle fails closed because the
report/evidence are pointing at a non-promotion-permitted attempt. For a
completed artifact-bound row, operators should expect `artifact_bound=true`,
`registry_row_valid=true`, `completion_row_valid=true`,
`lifecycle_complete=true`, `promotion_permitted=true`,
`artifact_binding_valid=true`, and `ok=true`.

Current stable registry refusal and validation reasons include:
`experiment_registry_bound_evidence_hash_missing`,
`experiment_registry_evidence_hash_phase_mismatch`,
`experiment_registry_statistical_evidence_hash_mismatch`,
`experiment_registry_identity_source_missing`,
`experiment_registry_final_holdout_identity_mismatch`,
`experiment_registry_final_holdout_content_mismatch`,
`experiment_registry_final_holdout_reuse_key_mismatch`,
`experiment_registry_artifact_bound_row_missing`,
`experiment_registry_artifact_bound_row_hash_mismatch`,
`experiment_registry_report_evidence_row_hash_mismatch`,
`artifact_binding_not_checked`, `attempt_budget_exceeded`, and
`holdout_reuse_budget_exceeded`.

The research engine is a pure replay/simulation path. It does not call the live broker, order lifecycle, run loop, recovery commands, or lot-native SELL authority code.

## Decision-Equivalence Claim Scope

Decision-equivalence evidence is scoped evidence, not a blanket lifecycle proof. The current repo-owned positive-supported state classes are `flat_no_dust_no_position` and `open_exposure`, proven through profile-bound research export, runtime replay, and validated decision-equivalence artifacts. The research backtest has a partial `lot_native_simulation_v1` surface that can model deterministic BUY-fill open lots, SELL-submit reserved lots, and partial SELL-fill reserved/open-lot reductions through the shared position-authority shape. The runtime adapter may classify `reserved_exit_pending`, but it remains model-scaffolded and fail-closed transition evidence until repo-owned runtime-replay fixture coverage exists.

Reports must therefore expose `claims_scope`, `state_coverage_matrix`, and `outcome`. The `lot_native_simulation_v1` surface is partial, not full lifecycle equivalence. Dust-only, reserved-exit-pending without repo-owned fixture evidence, non-executable residue, recovery-blocked, and any unsupported or ambiguous lifecycle state still fails closed with `FAIL_CLOSED_UNMODELED_STATE` / `research_model_lacks_lot_native_authority` or a more specific unsupported reason. `FAIL_CLOSED_UNMODELED_STATE` is safe behavior, but it is not profile-transition evidence and is not evidence of full lifecycle equivalence.

Operators should read report outcomes as follows:

- `PASS_POSITIVE_EQUIVALENCE`: positive equivalence only for explicitly modeled supported state classes.
- `FAIL_CLOSED_UNMODELED_STATE`: no promotion-grade lifecycle equivalence claim; extend the research lot-native model before claiming support.
- `FAIL_ACTUAL_DRIFT`: inspect semantic decision drift before promotion.
- `FAIL_INCOMPLETE_CANONICAL_PAYLOAD`: regenerate canonical exports with required fields.
- `FAIL_EXPORT_BINDING`: regenerate repo-owned, profile-bound decision exports.

Profile transitions accept only scope-aware positive reports: `outcome=PASS_POSITIVE_EQUIVALENCE`, `ok=true`, `promotion_grade_comparison=true`, non-empty `claims_scope.positive_equivalence_state_classes`, empty `claims_scope.unsupported_state_classes`, `claims_scope.fail_closed_unmodeled_state_count=0`, and a `state_coverage_matrix` covering the declared state classes. Full lifecycle equivalence is not proven unless `claims_scope.full_lifecycle_equivalence_supported=true`; the current implementation still claims only explicitly modeled state equivalence.

## Commands

Canonical commands:

```bash
uv run bithumb-bot sync-orderbook-top
uv run bithumb-bot research-readiness --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot backfill-candles --market KRW-BTC --interval 1m --start 2023-01-01 --end 2026-05-01 --batch-size 200
uv run bithumb-bot research-missing-candles --manifest "$MANIFEST" --out "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json"
uv run bithumb-bot retry-missing-candles --manifest "$MANIFEST" --missing-ranges "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json" --out "$DATA_ROOT/paper/reports/research/<experiment>/retry_attempts.json"
uv run bithumb-bot classify-persistent-missing-candles --manifest "$MANIFEST" --missing-ranges "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json" --retry-attempts "$DATA_ROOT/paper/reports/research/<experiment>/retry_attempts.json" --out "$DATA_ROOT/paper/reports/research/<experiment>/persistent_missing_classification.json"
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-walk-forward --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-verify-audit --experiment-id <experiment_id>
uv run bithumb-bot research-promote-candidate --experiment-id sma_filter_v1_2026_05 --candidate-id candidate_001
uv run bithumb-bot research-reproduce --promotion "$DATA_ROOT/paper/reports/research/<experiment>/promotion_<candidate>.json"
```

Research commands follow the explicit env model. They do not implicitly load repo-root `.env`.
Use `BITHUMB_ENV_FILE`, `BITHUMB_ENV_FILE_PAPER`, or process env to select DB and runtime roots.

## Backtest Progress And Failure Signals

`research-backtest` emits operator-visible stage lines before and during candidate evaluation. The progress stream is diagnostic stdout and does not change report payload hashes or promotion evidence. Typical stages include `start`, `load_split`, `quality_report`, `workload`, `evaluate`, `report_write`, and `complete`. The workload line includes candidate count, scenario count, split candle counts, estimated strategy runs, deployment tier, top-of-book requested/required status, and execution calibration requirement status.

If `timeout` or the operating system stops the process, a final summary and report artifact may not exist. Interpret common return codes as:

- `rc=124`: the `timeout` command killed the process.
- `rc=130`: interrupted with Ctrl+C / SIGINT.
- `rc=137`: usually SIGKILL, commonly from external kill or possible OOM.

Empty stdout plus no report usually means the process was killed or hung before it could print the final summary or caught error. With current progress output, the last `[RESEARCH-BACKTEST] stage=...` line identifies the last entered stage before the stop.

Recommended timeout capture:

```bash
set -o pipefail

timeout 300 uv run bithumb-bot research-backtest \
  --manifest "$TINY_MANIFEST" \
  2>&1 | tee "$OUTDIR/research_backtest_tiny_stdout.txt"

echo "backtest_rc=${PIPESTATUS[0]}"
```

`deployment_tier=research_only` backtests are diagnostic research outputs, not production promotion evidence. The research SMA performance fixes do not relax dataset quality gates, missing-candle policy, top-of-book requirements, execution calibration requirements, promotion gates, or production evidence requirements.

## Data Readiness And Historical Backfill

Run `research-readiness` before `research-backtest` when using production or production-like manifests:

```bash
uv run bithumb-bot research-readiness --manifest "$MANIFEST"
```

The command is read-only and SQL/streaming-backed. It prints split-level scan progress, manifest path and hash, effective `MODE`, resolved `DB_PATH`, market, interval, split ranges, candle coverage by split, top-of-book readiness, execution calibration readiness, and walk-forward readiness. It exits non-zero when required data or evidence is missing, so operators can see why `research-backtest` will fail before generating research artifacts. The output labels production readiness separately from research-only candle diagnostics.

Historical candle acquisition uses the configured runtime DB and explicit date range:

```bash
uv run bithumb-bot backfill-candles \
  --market KRW-BTC \
  --interval 1m \
  --start 2023-01-01 \
  --end 2026-05-01 \
  --batch-size 200
```

`backfill-candles` fetches Bithumb public minute candles backward from `--end` to `--start`, uses `candle_date_time_utc` as the canonical `candles.ts` bucket start, and writes with `INSERT OR REPLACE`. DB candle timestamps are UTC epoch milliseconds derived from `candle_date_time_utc`. The Bithumb minute candle API `to` cursor is a separate exchange API contract and is treated as KST-local naive ISO seconds, using the oldest returned candle's `candle_date_time_kst` as the next page boundary. Do not use UTC-naive DB timestamps as API cursors; that can create synthetic repeated 541-minute gaps. The command prints request count, fetched count, written count, duplicate/stall counters, batch oldest/newest timestamps, `next_api_cursor`, the `api_cursor_timezone=Asia/Seoul` / `db_timestamp_timezone=UTC` contract, page-boundary gap summary, and final candle coverage.

Backfill writes are idempotent, so after deploying a cursor/data contract fix operators can rerun the same backfill range against an existing sparse DB. Do not delete the DB solely to repair sparse candle coverage. Use `--dry-run` to fetch and print progress without writing. A non-dry-run backfill exits non-zero when the requested candle range remains incomplete, even if the API returned no older candles cleanly. Dry-run may exit zero for incomplete coverage because it is read/report mode, but it still prints `NOT_EVALUATED_BY_BACKFILL` and not-ready guidance. The command does not print a research `PASS`; pass/fail evidence comes from dataset quality and research gates.

Generate a missing candle artifact before targeted retries:

```bash
uv run bithumb-bot research-missing-candles \
  --manifest "$MANIFEST" \
  --out "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json"
```

`research-missing-candles` is a candle-only diagnostic artifact command. It records the manifest hash, DB path, market, interval, exact missing UTC epoch millisecond ranges, UTC display strings, KST display strings, bucket counts, initial classification, and `retry_utc_days`. It intentionally does not evaluate `orderbook_top_snapshots` or any top-of-book production gate, so it remains cheap even when top-of-book tables are large. Use this artifact instead of manually translating KST-readable gaps into UTC retry dates.

Run bounded targeted retries from the artifact:

```bash
uv run bithumb-bot retry-missing-candles \
  --manifest "$MANIFEST" \
  --missing-ranges "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json" \
  --min-buckets 20 \
  --max-attempts 1 \
  --out "$DATA_ROOT/paper/reports/research/<experiment>/retry_attempts.json"
```

The retry artifact records every selected range, before/after coverage, retry UTC days, recovered bucket counts, and final classification such as `retried_recovered` or `retry_persistent_missing`. If an individual bounded backfill attempt raises, `retry-missing-candles` preserves structured error evidence in that day's `backfill_attempts` entry and continues the selected retry workflow. API transient, rate-limit, retry-exhaustion, retryable status, and request-failure evidence can support a later `api_unavailable_candidate` classification. Persistent missing ranges and retry errors are recovery evidence for further investigation only. They do not authorize synthetic OHLCV candles and they do not weaken production gates.

Classify retry-persistent ranges into a diagnostic evidence artifact:

```bash
uv run bithumb-bot classify-persistent-missing-candles \
  --manifest "$MANIFEST" \
  --missing-ranges "$DATA_ROOT/paper/reports/research/<experiment>/missing_ranges.json" \
  --retry-attempts "$DATA_ROOT/paper/reports/research/<experiment>/retry_attempts.json" \
  --out "$DATA_ROOT/paper/reports/research/<experiment>/persistent_missing_classification.json"
```

The classifier writes `artifact_type=persistent_missing_candle_classification` with schema version 1. It binds to the manifest hash, missing-ranges hash, retry-attempts hash, resolved `DB_PATH`, DB schema fingerprint, market, and interval. It includes per-range candidate evidence and summary counts for `exchange_gap_candidate`, `api_unavailable_candidate`, `no_trade_missing_candidate`, and `unclassified_missing`. The artifact has `policy_effect=diagnostic_only_no_gate_relaxation`, `gate_effect=none` per range, and explicit limitations showing that synthetic OHLCV is not authorized, production gates are not relaxed, top-of-book is not satisfied, and execution calibration is not satisfied.

Classification is an evidence-plane improvement, not a gate bypass. It does not generate synthetic candles, does not infer OHLCV from neighboring candles, does not satisfy top-of-book requirements, does not satisfy execution calibration, and does not turn missing candles into split-level `PASS`. `dataset_quality_policy.missing_candle_policy=diagnostic_only` remains report/canonical metadata only. It does not satisfy production readiness, does not convert missing candles into `PASS`, and does not permit `allow_classified_no_trade_missing` to synthesize OHLCV or bypass gates. Candidate classifications require operator review and additional exchange/API evidence before any future reviewed exception policy could use them.

You may attach the classification artifact to readiness for visibility:

```bash
uv run bithumb-bot research-readiness \
  --manifest "$MANIFEST" \
  --missing-classification "$DATA_ROOT/paper/reports/research/<experiment>/persistent_missing_classification.json"
```

The readiness report then includes `persistent_missing_classification.status=DIAGNOSTIC_ONLY` and `production_gate_effect=none`. Readiness validates classification artifacts semantically, not only by content hash: malformed counts, unsafe classifications, gate-relaxing range fields, unsafe limitation flags, or invalid lineage hashes fail readiness. This section is diagnostic only. It does not change split `quality_status`, does not change top-level `status` from `FAIL` to `PASS`, and does not satisfy production-bound readiness while missing candles remain unresolved.

Expected missing-candle evidence workflow:

```text
research-readiness
-> backfill-candles
-> research-missing-candles
-> retry-missing-candles
-> classify-persistent-missing-candles
-> research-readiness with classification artifact
-> resolve candle/top_of_book/execution_calibration gates separately
-> research-backtest only after required gates pass
```

Backfill uses the repository env and path contract. Set `BITHUMB_ENV_FILE` or the appropriate explicit env selector, verify `MODE` and `DB_PATH`, and do not point runtime data at the repository. For large EC2 backfills, stop paper/live writers first if they share the same DB so ingestion and research do not compete with runtime writes.

Example EC2 sequence:

```bash
export BITHUMB_ENV_FILE=/home/ec2-user/bithumb-runtime/env/paper.research.env

uv run bithumb-bot config-dump --masked
```

```bash
MANIFEST=/home/ec2-user/bithumb-runtime/data/paper/reports/research/manifests/sma_filter_prod_krw_btc.json

uv run bithumb-bot research-readiness --manifest "$MANIFEST" --json

uv run bithumb-bot backfill-candles \
  --market KRW-BTC \
  --interval 1m \
  --start 2023-01-01 \
  --end 2026-05-01 \
  --batch-size 200

uv run bithumb-bot research-missing-candles \
  --manifest "$MANIFEST" \
  --out "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/missing_ranges.json"

uv run bithumb-bot retry-missing-candles \
  --manifest "$MANIFEST" \
  --missing-ranges "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/missing_ranges.json" \
  --min-buckets 20 \
  --max-attempts 1 \
  --out "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/retry_attempts.json"

uv run bithumb-bot classify-persistent-missing-candles \
  --manifest "$MANIFEST" \
  --missing-ranges "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/missing_ranges.json" \
  --retry-attempts "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/retry_attempts.json" \
  --out "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/persistent_missing_classification.json"

uv run bithumb-bot research-readiness \
  --manifest "$MANIFEST" \
  --missing-classification "$DATA_ROOT/paper/reports/research/sma_filter_prod_krw_btc/persistent_missing_classification.json"
uv run bithumb-bot research-backtest --manifest "$MANIFEST"
```

Use `config-dump --masked`, not direct Python imports of `bithumb_bot.config.settings`, for EC2 env verification. Direct imports do not exercise the CLI bootstrap path used by operator commands. `research-readiness --json` is the authoritative manifest/data readiness check before `research-backtest`.

Manual SQLite coverage inspection:

```bash
DB=/home/ec2-user/bithumb-runtime/data/paper/trades/paper.sqlite

sqlite3 -header -column "$DB" "
SELECT
  pair,
  interval,
  COUNT(*) AS rows,
  datetime(MIN(ts)/1000,'unixepoch') AS first_utc,
  datetime(MAX(ts)/1000,'unixepoch') AS last_utc
FROM candles
WHERE pair='KRW-BTC'
  AND interval='1m'
GROUP BY pair, interval;
"

sqlite3 -header -column "$DB" "
SELECT
  COUNT(*) AS top_rows,
  datetime(MIN(ts)/1000,'unixepoch') AS first_utc,
  datetime(MAX(ts)/1000,'unixepoch') AS last_utc
FROM orderbook_top_snapshots
WHERE pair='KRW-BTC';
"
```

Correct production sequence:

1. Stop live or paper execution if it shares the research DB.
2. Verify env loading and resolved DB path with `bithumb-bot config-dump --masked`.
3. Run `research-readiness`; this is the production gate evaluator for candle coverage, top-of-book requirements, execution calibration, and walk-forward prerequisites.
4. Backfill candles.
5. Generate `research-missing-candles` artifact and run bounded `retry-missing-candles` when gaps remain.
6. Rerun `research-readiness`.
7. Collect or backfill real top-of-book data if available.
8. Rerun `research-backtest`.
9. Proceed to walk-forward, calibration, promotion, and profile gates only after required gates pass.

Candle coverage is necessary but not sufficient for production promotion. Candle backfill and `research-missing-candles` only address historical candle coverage. They do not satisfy a production manifest that requires `dataset.top_of_book.required=true`, `missing_policy=fail`, and full top-of-book coverage. Execution calibration remains a separate evidence gate. Do not reconstruct fake top-of-book from candles, do not synthesize missing OHLCV from classified gaps, do not disable required top-of-book gates for production evidence, and do not shorten manifest dates merely to match the current DB. A production top-of-book requirement needs real `orderbook_top_snapshots` coverage or a separately reviewed non-production candle-only manifest.

## Manifest Format

Manifests are JSON to avoid adding another dependency. See:

- [`examples/research/sma_filter_manifest.example.json`](/examples/research/sma_filter_manifest.example.json)
- [`examples/research/sma_filter_manifest.production.example.json`](/examples/research/sma_filter_manifest.production.example.json)

Required sections:

- `experiment_id`, `hypothesis`, `strategy_name`, `market`, `interval`
- `dataset.source=sqlite_candles`, `dataset.snapshot_id`, `train`, `validation`, optional `final_holdout`
- `parameter_space`
- `cost_model.fee_rate`, `cost_model.slippage_bps` for legacy fixed-bps manifests
- `execution_model` for normalized fixed-bps or stress execution scenarios. Stress scenarios may configure slippage bps, latency, partial-fill rate, order-failure rate, market-order extra cost, scenario policy, scenario role, seed, and calibration requirements. Unsupported execution-model fields fail manifest parsing rather than being ignored.
- `deployment_tier` defaults to `research_only` when absent. Production-bound values such as `paper_candidate`, `live_dry_run_candidate`, and `small_live_candidate` activate the repo-owned production calibration policy and require `statistical_validation`, `stress_suite`, and `final_selection`.
- `acceptance_gate`

Optional section:

- `walk_forward.train_window_days`, `test_window_days`, `step_days`, `min_windows`
- `dataset.top_of_book` to opt into SQLite top-of-book quote joins. Supported fields are `source=sqlite_orderbook_top_snapshots`, `required`, `join_tolerance_ms` (default `3000`), `missing_policy`, optional `quote_source`, and `min_coverage_pct`. Unsupported dataset or top-of-book fields fail manifest parsing rather than being ignored.
- `statistical_validation` is optional for `research_only` diagnostics and required for production-bound promotion. Supported fields are `required_for_promotion`, `benchmark`, `primary_metric`, `selection_universe`, `multiple_testing_scope`, `bootstrap`, and `gates`. Unsupported or malformed statistical fields fail manifest parsing.
- `stress_suite` is optional for `research_only` diagnostics and required for production-bound manifests. It is independent of execution stress. Supported implemented sections are `trade_removal`, `trade_order_monte_carlo`, `period_ablation`, `parameter_perturbation`, and `risk_adjusted_score`. Unsupported stress-suite fields fail manifest parsing rather than being ignored.
- `final_selection` is optional only for `research_only` diagnostics and required for production-bound manifests. It declares the deterministic policy for choosing one candidate from the eligible candidate universe after acceptance, statistical, stress, dataset-quality, final-holdout, and production-calibration gates have produced evidence.
- `research_run.report_detail` controls report compactness. `summary` is compact report mode, not the full experiment record.
- `research_run.artifact_policy.full_decisions_external_jsonl=true` is the legacy switch that maps to complete external audit mode when `research_run.audit_trail` is not supplied.
- `research_run.audit_trail` declares external audit-trace requirements. Supported fields are `mode` (`summary_only` or `complete_external`), `decisions_required`, `equity_required`, `executions_required`, `hash_chain_required`, and `required_for_promotion`. Production-bound manifests should use `mode=complete_external` with all required booleans true.

When `acceptance_gate.walk_forward_required=true`, the `walk_forward` section is required. All values must be positive integers.

## Statistical Selection Contract

Researcher-freedom metadata such as `search_budget`, `parameter_grid_size`, declared `attempt_index`, declared `holdout_reuse_count`, `dataset_reuse_policy`, hypothesis ids, and lineage hashes is observability, not a statistical defense by itself. Production-bound promotion now requires a `StatisticalSelectionContract`, a `statistical_selection_evidence` artifact, and registry-backed final-holdout/attempt evidence so the selected candidate is judged as a winner selected from a candidate universe, not as an isolated backtest.

The current official `research-backtest` report-generation path accepts and emits `bootstrap.method=metric_centered_max_bootstrap` for screening evidence and supports `white_reality_check_block_bootstrap` only when an aligned promotion-grade return panel exists. Summary bootstrap computes a deterministic, seeded max-statistic bootstrap over candidate primary metric summaries and emits `summary_metric_max_bootstrap_p_value`. It is `SCREENING_SUMMARY_BOOTSTRAP`; it does not populate `white_reality_check_p_value`, and it is not promotion-grade. Official reports and candidate profiles expose `official_promotion_grade_wrc_generation_available=false` and warning `promotion_grade_statistical_generation_unavailable` when only screening evidence is available. That warning is operator visibility only; it does not satisfy any statistical gate. Evidence cannot claim WRC if the manifest/statistical contract only declared summary bootstrap. Promotion-grade WRC evidence must use a manifest contract, evidence `bootstrap_method`, evidence `statistical_method`, evidence `white_reality_check_method`, and `bootstrap_sampling_contract.method` that all identify the same supported method. The sampling contract's canonical method field is `method`; `method_name` is compatibility-only and is not sufficient for promotion-grade evidence. Promotion-grade WRC evidence must be recomputable from the bound return panel and a supported bootstrap sampling contract with method provenance. Full WRC requires aligned `bar_excess_return` or `portfolio_bar_return` panel evidence; `trade_return` panels are screening/diagnostic and reproducibility inputs and cannot satisfy full promotion-grade WRC. SPA, Deflated Sharpe, and family-wide statistical aggregation are not implemented; if `max_spa_p_value` or `min_deflated_sharpe_probability` is configured before those methods are implemented, promotion fails closed with `spa_method_unavailable` or `deflated_sharpe_missing`, and experiment-family promotion-grade statistical aggregation fails closed rather than silently falling back.

Lineage metadata records researcher freedom: manifest, dataset, experiment-family, attempt, holdout-reuse, and search-budget context. For production-bound tiers, `attempt_index` and `holdout_reuse_count` mean registry-computed values; declared manifest counters are preserved separately as `declared_attempt_index` and `declared_holdout_reuse_count`. Statistical evidence is the enforcement artifact. The `selection_universe_hash` binds the manifest hash, dataset content hash, dataset quality hash when present, experiment-family and hypothesis metadata, candidate ids and parameter values, required scenario ids, primary metric source, benchmark policy, and the statistical validation contract. The separate `candidate_metric_values_hash` binds the exact candidate metric universe used for the statistical test, including candidate ids, parameter values, scenario policy, required scenario ids, primary metric/source, validation metric values, missing-metric markers, and candidate acceptance-gate results. The `research_freedom_hash` binds experiment-family and hypothesis identity, dataset snapshot, train/validation/final-holdout split hashes, final-holdout fingerprint, parameter-space hash, registry-computed counters, registry path, registry prior hash, and registry row hash.

Promotion and reproduction recompute `candidate_metric_values_hash` from the current source `backtest_report.json` candidate list and compare that recomputed value with the statistical evidence, report, selected candidate, promotion artifact, and lineage fields. Reproduction also validates the declared return-panel binding, family-trial registry binding, and experiment registry binding. The family trial registry remains the statistical evidence binding for experiment-family scope. It is not the global final-holdout usage ledger and does not replace the experiment registry. A missing, unmatched, or non-recomputing family registry row hash reports `experiment_family_registry_row_hash_mismatch`; true semantic drift in experiment id, manifest hash, dataset hash, attempt index, holdout reuse count, parameter-space hash, or candidate count reports `experiment_family_registry_stale`. Matching copied hash strings are not sufficient. Editing candidate metrics, candidate rows, required scenario ids, primary metric/source, benchmark values, return-panel rows, family registry rows, experiment registry rows, or copied hashes requires regenerating or appending valid evidence from the same manifest and dataset snapshot; manually editing hashes is not valid recovery.

The evidence artifact also records top-level `required_scenario_ids`, `candidate_metric_values_summary`, `metric_value_count`, and `missing_metric_count`. Promotion fails closed when `report.candidate_count`, `evidence.candidate_count`, or `candidate_metric_values_summary.candidate_count` differs from the actual `len(report.candidates)`, when `metric_value_count != candidate_count`, when any metric is missing, when `effective_trial_count` is lower than the maximum trial universe implied by candidate count, parameter grid size, search budget, attempt index, and holdout reuse count, or when metadata differs between the report/candidate and evidence. The evidence artifact is strict JSON under:

```text
DATA_ROOT/<mode>/reports/research/<experiment_id>/statistical_selection_evidence.json
```

This is a `reports` artifact. It is diagnostic/promotion evidence, not recovery-critical trade lifecycle state. Operators should regenerate it from the same manifest and dataset snapshot rather than editing recorded hashes.

## Final Selection Contract

`acceptance_gate` is a minimum eligibility gate. It does not decide which eligible candidate is production-bound. `statistical_validation` defends against selection bias and multiple testing, and `stress_suite` records fragility and execution-stress evidence. The `final_selection` contract is the separate, manifest-declared rule that ranks eligible candidates and chooses the final candidate.

Production-bound manifests must set `final_selection.required_for_promotion=true`. The contract currently supports `method=lexicographic`, `candidate_universe=acceptance_gate_passed_required_scenarios`, explicit `must_pass` fields, explicit null handling, and a ranking list that must end with `parameter_candidate_id` ascending for deterministic tie-breaking. Unsupported final-selection fields fail manifest parsing, including typo-prone nested keys in `must_pass`, `selection_exposure_policy`, and `unsupported_metric_policy`. `selection_exposure_policy.final_holdout_usage=confirmatory_metric_in_rank` requires at least one `final_holdout.` ranking metric, and any such metric requires `counts_as_holdout_reuse=true`. Changing the final-selection contract changes the manifest hash.

Backtest reports expose `final_selection_required`, `final_selection_contract`, `final_selection_contract_hash`, `final_selection_gate_result`, `final_selection_fail_reasons`, `selected_candidate_id`, `selected_candidate_score_hash`, `candidate_final_scores_hash`, and `candidate_final_scores`. `candidate_final_scores` is canonicalized by final-selection rank order and candidate id before hashing, so `candidate_final_scores_hash`, `selected_candidate_score_hash`, and the public score order are independent of incoming candidate order and independent of legacy `_candidate_rank_key`. `best_candidate_id` equals `selected_candidate_id` when contract-backed selection passes. The legacy `_candidate_rank_key` order remains diagnostic compatibility for `research_only` reports without `final_selection`; those reports emit `legacy_implicit_final_rank_policy_v1`, show a non-PASS final-selection gate, and are visibly non-promotable in CLI summaries.

Promotion refuses a requested candidate unless it equals `report.selected_candidate_id` and the final-selection contract hash, candidate score hash, all-candidate score hash, and gate result recompute from the source backtest report. Reproduction reopens the source report and fails closed on missing or drifted final-selection contract or score evidence, selected-candidate mismatch, a non-PASS final-selection gate, or attempts to promote a candidate not selected by the contract. Copied hashes or manual JSON edits are not recovery. Regenerate research from the manifest and dataset.

Final-selection rank components may reference benchmark excess-return fields such as `validation.benchmark.excess_return_vs_buy_and_hold_pct` and `final_holdout.benchmark.excess_return_vs_buy_and_hold_pct`. Reports include split-level benchmark metrics for cash and buy-and-hold. Buy-and-hold is computed deterministically from the split's first candle open to last candle close; if candle evidence is insufficient, required final-selection metrics that depend on it fail closed through normal required-metric handling. Statistical selection treats missing `buy_and_hold` or `configured` benchmark evidence as a missing metric, not as zero. Cash remains the only benchmark that can use zero by definition.

Sharpe and Sortino are not faked from closed-trade summaries. Required `sharpe_ratio` or `sortino_ratio` final-selection metrics fail closed with `final_selection_sharpe_unavailable_without_period_return_series` or `final_selection_sortino_unavailable_without_period_return_series` until a deterministic, aligned, hash-bound period return panel exists.

Stable statistical refusal reasons include `statistical_contract_missing`, `statistical_contract_mismatch`, `statistical_method_contract_mismatch`, `statistical_evidence_missing`, `statistical_evidence_hash_missing`, `statistical_evidence_hash_mismatch`, `statistical_evidence_grade_insufficient`, `selection_universe_hash_missing`, `selection_universe_hash_mismatch`, `candidate_metric_values_hash_missing`, `candidate_metric_values_hash_mismatch`, `candidate_metric_values_hash_recompute_mismatch`, `statistical_method_provenance_missing`, `statistical_method_unavailable`, `promotion_grade_statistical_computation_missing`, `bootstrap_sampling_contract_missing`, `bootstrap_sampling_contract_malformed`, `bootstrap_sampling_contract_method_mismatch`, `white_reality_check_p_value_recompute_mismatch`, `promotion_grade_requires_aligned_return_panel`, `experiment_family_statistical_universe_not_implemented`, `return_panel_missing`, `return_panel_hash_missing`, `return_panel_hash_mismatch`, `return_panel_candidate_count_mismatch`, `return_panel_observation_count_mismatch`, `return_panel_scenario_id_mismatch`, `return_panel_time_index_mismatch`, `return_panel_panel_content_hash_mismatch`, `return_panel_series_alignment_mismatch`, `return_panel_series_malformed`, `experiment_family_universe_missing`, `experiment_family_registry_row_hash_mismatch`, `experiment_family_registry_statistical_evidence_hash_mismatch`, `experiment_family_registry_prior_hash_mismatch`, `experiment_family_registry_return_panel_hash_mismatch`, `experiment_family_registry_stale`, `experiment_registry_missing`, `experiment_registry_path_missing`, `experiment_registry_row_hash_missing`, `experiment_registry_row_hash_mismatch`, `experiment_registry_prior_hash_mismatch`, `experiment_registry_stale`, `experiment_registry_attempt_index_mismatch`, `experiment_registry_holdout_reuse_count_mismatch`, `experiment_registry_final_holdout_fingerprint_mismatch`, `experiment_registry_final_holdout_identity_mismatch`, `experiment_registry_final_holdout_content_mismatch`, `experiment_registry_final_holdout_reuse_key_mismatch`, `experiment_registry_identity_source_missing`, `experiment_registry_bound_evidence_hash_missing`, `experiment_registry_evidence_hash_phase_mismatch`, `experiment_registry_statistical_evidence_hash_mismatch`, `experiment_registry_artifact_bound_row_missing`, `experiment_registry_artifact_bound_row_hash_mismatch`, `experiment_registry_report_evidence_row_hash_mismatch`, `artifact_binding_not_checked`, `experiment_registry_budget_exceeded`, `experiment_registry_incomplete_attempt`, `declared_attempt_index_mismatch`, `declared_holdout_reuse_count_mismatch`, `statistical_metadata_mismatch`, `statistical_candidate_count_mismatch`, `statistical_attempt_index_mismatch`, `statistical_holdout_reuse_count_mismatch`, `statistical_search_budget_mismatch`, `statistical_parameter_grid_size_mismatch`, `statistical_dataset_reuse_policy_mismatch`, `statistical_benchmark_mismatch`, `statistical_primary_metric_mismatch`, `statistical_metric_values_missing`, `statistical_metric_value_count_mismatch`, `statistical_effective_trial_count_underreported`, `reality_check_p_value_missing`, `reality_check_p_value_failed`, `spa_method_unavailable`, `deflated_sharpe_missing`, `effective_trial_count_missing`, `holdout_reuse_budget_exceeded`, and `attempt_budget_exceeded`.

## Strategy Robustness Stress Suite

`StressExecutionModel` tests bad fills: slippage, latency, partial fills, and order failures. The `stress_suite` contract tests a different failure mode: fragile PnL dependence. A strategy can survive execution stress but still depend on one lucky winner, an unsafe trade ordering path, or a weak risk-adjusted score. The statistical summary bootstrap is also separate; it corrects selected candidate summary metrics and is not trade-order Monte Carlo over closed trade PnL.

When configured, each candidate scenario carries `stress_suite_contract`, `stress_suite_contract_hash`, `validation_stress_suite`, `final_holdout_stress_suite` when final holdout exists or final holdout is required for promotion, `stress_suite_gate_result`, and `stress_suite_fail_reasons`. The report top level exposes `stress_suite_required`, `stress_suite_gate_result`, compact fail reasons, and stress evidence from the best passing candidate. If no candidate passes and the stress suite is required, the report and CLI summary fall back to the first ranked failed candidate so operators see `stress_suite_gate_result=FAIL` and the concrete stress reasons instead of `none`. This fallback is observability only and does not make `promotion_allowed=1`. Stress evidence is embedded in report/candidate/profile/promotion JSON and content-hashed with `stress_suite_hash`; it is a `reports` evidence payload and not recovery-critical trade lifecycle state.

Promotion treats candidate-level stress fields as authoritative. When `stress_suite_required=true` or the selected candidate is production-bound by `deployment_tier`, the selected candidate must carry a dictionary `stress_suite_contract`, a valid `stress_suite_contract_hash`, validation stress evidence bound to that contract hash, and final-holdout stress evidence when final holdout is present or required. Report-level stress contracts are compared for drift, but they are not a fallback that can hide missing candidate-level contract evidence. Required stress evidence must be hash-valid, contract-bound, and `gate_result=PASS`. `stress_suite_required=false` or a missing flag is not an escape hatch for `paper_candidate`, `live_dry_run_candidate`, or `small_live_candidate` artifacts.

Top-N trade removal uses closed SELL-side realized trades from Metrics V2, sorted by positive `net_pnl`. For each configured `top_n_by_net_pnl`, the suite removes the top winners and recomputes realized return, return retention, profit factor, expectancy, win rate, and remaining trade count. Stress-suite `win_rate` uses the same 0-1 ratio convention as Metrics V2, not a 0-100 percentage. It does not claim full equity replay. If `max_mdd_multiplier` is configured, the current implementation fails that case with `trade_removal_mdd_replay_unavailable` because max drawdown cannot be reconstructed safely from closed-trade summaries alone.

Period ablation supports `calendar_years="auto"` or an explicit year list. The current method is `leave_one_calendar_year_out_closed_trade_exit_year`: it groups closed SELL-side realized trades by UTC `exit_ts` calendar year, removes one configured year at a time, and recomputes realized return, return retention, profit factor, expectancy, win rate, removed trade count, and remaining trade count. `period_ablation.min_return_retention_pct` controls whether each leave-one-year-out case passes; a case fails with `stress_period_ablation_return_retention_failed` when retained return is below that threshold. The section records per-year cases plus aggregate `pass_ratio` and fails when that ratio is below `min_pass_ratio`. This is not a full signal rerun over a removed dataset period; it carries `period_ablation_uses_closed_trade_exit_year_not_full_signal_rerun` so operators do not mistake it for full data ablation.

Example:

```json
"period_ablation": {
  "calendar_years": "auto",
  "min_pass_ratio": 0.8,
  "min_return_retention_pct": 50.0
}
```

Parameter perturbation supports configured `relative_pct` values and `numeric_params_only=true`. The current method is `existing_grid_relative_parameter_perturbation`: for each numeric parameter in the selected candidate, the suite computes each relative target value and searches the already evaluated same-scenario parameter grid for an exact matching parameter set. Matched candidates contribute their validation and final-holdout metrics plus the pre-stress scenario gate result. Missing matches are recorded as missing evidence and fail the case; they are not silently skipped. This is not a synthetic rerun engine and carries `parameter_perturbation_uses_existing_grid_candidates_not_synthetic_reruns`. To get positive evidence, include the intended +/- perturbation values in the manifest parameter grid and regenerate research.

Trade-order Monte Carlo shuffles the closed-trade PnL sequence with a deterministic seed derived from manifest/candidate/scenario/split/contract context. It records terminal equity percentiles, max-drawdown percentiles, longest losing streak percentiles, survival probability, seed, seed-material hash, and explicit limitations. The payload limitation codes are `monte_carlo_uses_closed_trade_pnl_not_bar_return_series` and `monte_carlo_does_not_reconstruct_intratrade_equity_path`. Survival means shuffled max drawdown does not exceed `ruin_max_drawdown_pct`. Too few closed trades fail closed rather than producing optimistic evidence.

Risk-adjusted scoring currently computes `calmar_ratio` from Metrics V2 CAGR and max drawdown. Sharpe and Sortino are not faked from trade summaries; they remain `null` with `sharpe_unavailable_without_period_return_series` and `sortino_unavailable_without_period_return_series` limitations unless a reliable period return series is implemented later.

Operator CLI summaries for research reports and successful promotion artifacts print `stress_suite_required`, `stress_suite_gate_result`, `stress_suite_fail_reasons`, `stress_trade_removal_status`, `stress_period_ablation_status`, `stress_period_ablation_pass_ratio`, `stress_parameter_perturbation_status`, `stress_parameter_perturbation_pass_ratio`, `stress_monte_carlo_survival_probability`, and `stress_monte_carlo_max_drawdown_pct_p95` so operators do not need to inspect JSON to see the stress-suite state. When every candidate fails because of required stress evidence, the report summary uses the first ranked failed candidate's stress evidence and still prints `promotion_allowed=0`.

Stable stress refusal reasons include `stress_suite_required_but_missing`, `stress_suite_gate_not_passed`, `stress_suite_hash_missing`, `stress_suite_hash_mismatch`, `stress_suite_contract_mismatch`, `final_holdout_stress_suite_required_but_missing`, `final_holdout_stress_suite_hash_missing`, `final_holdout_stress_suite_hash_mismatch`, `final_holdout_stress_suite_gate_not_passed`, `stress_suite_evidence_malformed`, `stress_trade_removal_no_closed_trades`, `stress_trade_removal_return_retention_failed`, `stress_trade_removal_mdd_replay_unavailable`, `stress_monte_carlo_no_closed_trades`, `stress_monte_carlo_insufficient_trades`, `stress_monte_carlo_survival_probability_failed`, `stress_period_ablation_no_closed_trades`, `stress_period_ablation_exit_timestamp_missing`, `stress_period_ablation_no_matching_years`, `stress_period_ablation_pass_ratio_failed`, `stress_period_ablation_required_data_missing`, `stress_period_ablation_return_retention_failed`, `stress_parameter_perturbation_no_numeric_parameters`, `stress_parameter_perturbation_candidate_missing`, `stress_parameter_perturbation_pass_ratio_failed`, `stress_parameter_perturbation_required_data_missing`, `stress_parameter_perturbation_constraint_invalid`, and `stress_risk_adjusted_calmar_missing`.

When the stress suite fails, treat the candidate as fragile evidence. Revise the strategy hypothesis, widen or improve data, reduce dependence on one year or regime, reduce single-trade dependency, adjust risk controls, and rerun research from the manifest and dataset. Do not manually edit JSON, copied hashes, backtest reports, candidate profiles, or promotion artifacts to recover missing or failed stress evidence; regeneration is the recovery path.

## Experiment Attempt Registry

Production-bound research with a final holdout writes an append-only experiment lifecycle registry at:

```text
DATA_ROOT/<mode>/reports/research/_registry/experiment_registry.jsonl
```

This is a managed `reports` artifact resolved through `PathManager`; it must
remain outside the repository and mode-separated under `DATA_ROOT/<mode>`. The
registry is operator-auditable promotion evidence for overfitting defense. It is
not trade lifecycle recovery state.

Production-bound research with `final_holdout` performs checked registry
reservation before the final-holdout split is loaded. The reservation row is the
only counted attempt event and uses the semantic final-holdout identity while
content is still unavailable. `research_attempt_reserved` rows count even if
the run is interrupted later. `research_attempt_rejected` rows are audit
evidence only: declared counter mismatches and budget excesses are checked
under the registry lock, append `counted_attempt=false`, and do not append a
counted reservation.

The registry separates final-holdout reuse identity from content integrity:

- `final_holdout_identity_hash` is the semantic reuse-counting key based on
  dataset source, market, interval, and final-holdout date range.
- `final_holdout_reuse_key_hash` is the actual key used to compute
  `computed_holdout_reuse_count` and should equal the semantic identity hash.
- `final_holdout_fingerprint` is retained only as a compatibility alias for the
  semantic identity hash.
- `final_holdout_content_hash` is the reproducibility/integrity key based on
  dataset snapshot id, final-holdout split hash, and dataset quality hash.
- `final_holdout_content_pending_until_completion=true` means reservation
  occurred before final-holdout content was loaded; content fields must be
  bound in completion, evidence, and report artifacts before promotion.

`computed_attempt_index` is one plus the number of prior counted reservations
for the same experiment family and hypothesis id. `computed_holdout_reuse_count`
is the number of prior counted reservations with the same
`final_holdout_reuse_key_hash`. Changing `dataset_snapshot_id`, backfilled
candle content, or final-holdout split bytes does not reset semantic holdout
reuse for the same market, interval, and date range. Content mismatches remain
separate fail-closed integrity failures.

Lifecycle status is append-only and separate from statistical gate result:

- `IN_PROGRESS` is a counted reservation.
- `COMPLETED` is a completed lifecycle event.
- `ABORTED` is an interrupted counted attempt.
- `REJECTED` is an uncounted preflight rejection.

Only `COMPLETED` is promotion-permitted. Aborted attempts remain counted
exposure but are not promotion-permitted. `statistical_gate_result` is
`PASS|FAIL|UNKNOWN` and remains separate evidence about candidate/statistical
quality; a completed row with `statistical_gate_result=FAIL` is complete audit
evidence, not permission to promote.

Completion uses two-phase evidence binding to avoid a recursive hash cycle.
`research_attempt_completed` records the pre-completion statistical evidence
hash. Final evidence stores that value in
`experiment_registry_bound_evidence_hash` and must set
`experiment_registry_evidence_hash_phase=pre_completion_evidence_hash`. The
final statistical evidence `content_hash` is recomputed after
`experiment_registry_completion_row_hash` and
`experiment_registry_bound_evidence_hash` are inserted, so the final
`content_hash` and bound evidence hash can intentionally differ.

Promotion and reproduction validate the reservation row, prior registry hash,
completion row, evidence phase, bound evidence hash, final-holdout
identity/content/reuse-key fields, computed counters, declared counters, and
budget compliance. Missing, stale, tampered, mismatched, incomplete, or
over-budget registry evidence fails closed for `paper_candidate`,
`live_dry_run_candidate`, and `small_live_candidate`.

For `research_only`, the current workflow remains diagnostic and reproducible:
registry absence is reported as `registry_gate_result=WARN` with
`experiment_registry_missing`, but the run is not blocked. That compatibility
does not apply to production-bound promotion.

`research-registry-validate --experiment-id <id>` is an operator audit command.
It prints `validation_scope=registry_only` when report/evidence/panel artifacts
are absent; in that mode, artifact binding is not checked and
`artifact_binding_valid=unknown` with `artifact_binding_not_checked` is not
promotion-grade proof. It prints `validation_scope=registry_and_artifacts` when
it loads report, evidence, or return-panel artifacts from
`DATA_ROOT/<mode>/reports/research/<experiment_id>/`.

In artifact scope, `artifact_bound_row_hash` identifies the reservation row
referenced by report/evidence. `artifact_binding_valid` and `artifact_reasons`
describe whether that artifact chain binds to the registry, evidence phase,
content hashes, return panel, and lifecycle state. If report and evidence point
at different rows, validation fails closed with
`experiment_registry_report_evidence_row_hash_mismatch` and
`experiment_registry_artifact_bound_row_hash_mismatch`. If the referenced row is
missing from the registry, validation fails closed with
`experiment_registry_artifact_bound_row_missing`.

`registry_lifecycle_summary` lists all reservation rows for the experiment
separately, so older incomplete or aborted rows remain visible without being
falsely marked artifact-valid for the current report/evidence chain. Extra
incomplete non-bound rows do not fail current artifact-bound validation.
Artifact-bound incomplete rows do fail validation.

Each `registry_lifecycle_summary` row uses these fields:

- `registry_row_valid=true` means the reservation row hash recomputes.
- `completion_row_valid=true` means the completion/abort row hash recomputes,
  or is true when no completion/abort row exists.
- `lifecycle_complete=true` means the lifecycle status is promotion-permitted,
  currently only `COMPLETED`.
- `promotion_permitted=true` mirrors `lifecycle_complete` at the registry
  layer.
- `row_valid_only=true` means the row hash is valid but the lifecycle is not
  promotion-permitted.
- `ok=true` means `registry_row_valid`, `completion_row_valid`, and
  `lifecycle_complete` are all true.

An incomplete row is expected to report `registry_row_valid=true`,
`completion_row_valid=true`, `lifecycle_complete=false`,
`promotion_permitted=false`, `row_valid_only=true`, `ok=false`, and reason
`experiment_registry_incomplete_attempt`. A completed row is expected to report
`registry_row_valid=true`, `completion_row_valid=true`,
`lifecycle_complete=true`, `promotion_permitted=true`, `row_valid_only=false`,
and `ok=true`.

Stable experiment-registry refusal and validation reasons include
`experiment_registry_bound_evidence_hash_missing`,
`experiment_registry_evidence_hash_phase_mismatch`,
`experiment_registry_statistical_evidence_hash_mismatch`,
`experiment_registry_identity_source_missing`,
`experiment_registry_final_holdout_identity_mismatch`,
`experiment_registry_final_holdout_content_mismatch`,
`experiment_registry_final_holdout_reuse_key_mismatch`,
`experiment_registry_artifact_bound_row_missing`,
`experiment_registry_artifact_bound_row_hash_mismatch`,
`experiment_registry_report_evidence_row_hash_mismatch`,
`artifact_binding_not_checked`, `attempt_budget_exceeded`, and
`holdout_reuse_budget_exceeded`.

Interrupted runs should be recovered by appending an aborted event with the
existing tooling or by regenerating the research from the manifest and dataset
snapshot. Do not manually edit JSONL rows, prior hashes, row hashes, reports,
evidence, or promotion artifacts as a normal recovery path. Manual hash editing
invalidates the audit trail.

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

Reports include manifest hash, dataset fingerprint, dataset quality reports and hashes, candidate profile hash, content hash, repository version, metrics, gate results, statistical selection fields when configured, and artifact paths.
Reports aggregate by stable `parameter_candidate_id`; they do not treat each execution scenario as a separate promotion candidate. Each top-level candidate contains `scenario_policy`, pass/fail counts, required scenario count, required scenario ids, `final_holdout_present`, `final_holdout_required_for_promotion`, `candidate_profile_hash`, legacy `metrics`, `metrics_schema_version=2`, `metrics_gate_policy`, `metrics_gate_policy_hash`, `train_metrics_v2`, `validation_metrics_v2`, `final_holdout_metrics_v2` when present, and `scenario_results[]`. Each scenario result records scenario identity, `scenario_role`, `scenario_role_source`, execution model payload/hash, cost model, metrics gate policy/hash, train/validation/final-holdout/walk-forward metrics when present, regime gate result, execution-calibration gate, scenario acceptance result, fail reasons, and execution metadata. Candle-only datasets do not contain top-of-book, orderbook depth, or intra-candle path data; trade metadata records that limitation instead of fabricating quotes or depth. When top-of-book is configured and joined, execution metadata carries `best_bid`, `best_ask`, and `spread_bps`, while `reference_price` remains candle close.
`generated_at` is included for operator context but excluded from the deterministic `content_hash`.

## Metrics Contract V2

`metrics_v2` is the promotion-grade metrics contract extension. The legacy `metrics` object remains present for compatibility and keeps its existing field names and formulas. Research JSON artifacts are strict JSON: `Infinity`, `-Infinity`, `NaN`, and other non-finite floats are not emitted into reports, candidate profiles, promotion artifacts, lineage hashes, or canonical content hashes. New reports and candidate profiles include `metrics_schema_version=2` and nested sections:

- `return_risk`: total return, CAGR, max drawdown, realized return, ending unrealized PnL, and whether an open position remains at the end.
- `trade_quality`: closed trade count, execution attempt count, win rate, average win/loss, payoff ratio, profit factor, expectancy in KRW and percent, max consecutive losses, and single-trade dependency.
- `time_exposure`: evaluation start/end, elapsed time, calendar days, active bar count, exposure time percentage, and closed-position holding-time statistics.
- `cost_execution`: total fees, total slippage, fee/slippage drag ratios, filled/partial/failed/skipped execution counts, quote coverage, and quote-age percentiles.

Formula definitions are deterministic:

- `total_return_pct` is the same portfolio-level final-equity return represented by legacy `return_pct`.
- `cagr_pct` annualizes `total_return_pct` over elapsed calendar time; it is `null` when elapsed time is zero, invalid, or numerically unsafe.
- `realized_return_pct` is closed-trade net PnL divided by starting cash.
- `unrealized_pnl_end` is final marked asset value minus the open position cost basis. `open_position_at_end=true` keeps that state explicit.
- `expectancy_per_trade_krw` is mean net PnL over closed SELL-side realized records.
- `expectancy_per_trade_pct` is mean closed-trade return percentage over allocated entry notional. It is `null` and reason-coded when entry notional is unavailable.
- `payoff_ratio` is average win divided by absolute average loss; it is `null` when wins or losses are missing.
- `profit_factor` uses gross winning PnL divided by absolute gross losing PnL. All-win samples use `profit_factor: null`, `profit_factor_unbounded: true`, and limitation reason `profit_factor_unbounded_no_losses`; no-win/no-loss samples use `profit_factor: null` and `profit_factor_unbounded: false`.
- `exposure_time_pct` is time with an active position divided by elapsed evaluation time.
- Holding-time metrics use closed position intervals only. Open intervals are reported through `open_position_at_end` and `limitation_reasons`; they do not contaminate closed holding-time stats.
- `metrics_v2.cost_execution.fee_drag_ratio` and `metrics_v2.cost_execution.slippage_drag_ratio` use total traded notional as denominator. They emit `fee_drag_ratio_basis="traded_notional"` and `slippage_drag_ratio_basis="traded_notional"` even when no traded notional exists and the ratios are `null` with a limitation reason.
- Same-named drag ratio fields must include a basis in research, promotion, reporting, and runtime evidence. Values with different bases are not equivalent and must not be compared as though they use the same denominator.
- `execution_count` means execution attempts. `closed_trade_count` means realized closed SELL-side trade records.
- Failed and skipped execution attempts still produce an equity mark for the evaluated candle before control leaves the branch. A failed/skipped SELL while a position remains open therefore contributes to the equity curve, legacy max drawdown, and `metrics_v2.return_risk.max_drawdown_pct`.
- Partial SELL fills close only the realized filled quantity. Any residual position remains open through period end, contributes to exposure time and `unrealized_pnl_end`, and is not converted into a fully closed holding interval.

Acceptance gates can optionally evaluate `metrics_v2` with these fields:

```json
{
  "min_cagr_pct": null,
  "min_expectancy_per_trade_krw": null,
  "min_expectancy_per_trade_pct": null,
  "max_exposure_time_pct": null,
  "max_avg_holding_time_minutes": null,
  "max_fee_drag_ratio": null,
  "max_slippage_drag_ratio": null,
  "reject_open_position_at_end": false,
  "metrics_contract_required": false
}
```

Absent or `null` fields preserve old behavior. When a field is configured, it is evaluated from `validation_metrics_v2` and, when present, `final_holdout_metrics_v2`. Missing required v2 values fail with stable reason codes such as `metrics_v2_missing`, `metrics_contract_missing`, `metrics_v2_required_field_missing`, `min_cagr_failed`, `min_expectancy_per_trade_krw_failed`, `min_expectancy_per_trade_pct_failed`, `max_exposure_time_failed`, `max_avg_holding_time_failed`, `max_fee_drag_ratio_failed`, `max_slippage_drag_ratio_failed`, and `open_position_at_end_failed`.

`metrics_gate_policy` is the deterministic copy of the applied metrics gate policy built from `acceptance_gate`, not from candidate output. It includes the metrics schema version, all optional metrics thresholds, `reject_open_position_at_end`, and `metrics_contract_required`. `metrics_gate_policy_hash` proves that this policy payload did not drift inside the candidate/profile/promotion evidence; it does not prove that the strategy is profitable or live-ready. Candidate profile hashes include the policy and hash, so changing only a metrics threshold changes the manifest hash and the candidate/profile evidence hash.

When `metrics_contract_required=true`, promotion fails closed if `metrics_gate_policy`, `metrics_gate_policy_hash`, `validation_metrics_v2`, or required final-holdout `final_holdout_metrics_v2` is missing or malformed. Stable promotion refusal reasons include `metrics_gate_policy_missing`, `metrics_gate_policy_hash_missing`, `metrics_gate_policy_hash_mismatch`, `validation_metrics_v2_missing`, `final_holdout_metrics_v2_missing`, and `metrics_contract_missing`.

Current-generation research reports also carry one deterministic `dataset_quality_report` payload per split. Each report records expected candle count from the manifest date range and interval, actual candle row count, expected buckets actually present, coverage percentage, missing bucket count/ranges/sample, duplicate-key diagnostics, timestamp monotonicity and interval consistency diagnostics, OHLC invariant violations, non-positive prices, negative volume, first/last timestamp, the candle-table schema fingerprint, the split dataset content hash, quality gate status, quality gate reasons, and a deterministic report `content_hash`. `coverage_pct` is based on expected buckets actually present, not raw row count, so duplicates or unexpected buckets cannot push coverage above 100%. Missing diagnostics are bounded for long 1m historical splits. When `dataset.top_of_book` is configured, the report also records `top_of_book_requested`, `top_of_book_required`, source, join tolerance, expected signal/candle count, joined count, missing count/sample, coverage percentage, quote gate status, and reason codes. Reports distinguish candle quality coverage, candle-nearest top quote coverage, signal-level execution quote coverage that is not computable without running a strategy signal pass, and signal-level depth coverage that is likewise not computable in readiness-only scans. Reports also include a deterministic top-level `top_of_book_quality_summary` with requested/required flags, joined and missing quote counts, aggregate coverage, affected split names, quote gate status, limitations, and operator next action. Optional missing quote coverage is `WARN`, adds candidate/report warning code `top_of_book_optional_coverage_warning`, and is printed in the CLI summary; required or fail-policy missing coverage fails closed. The combined `dataset_quality_hash` is included in research lineage, candidate profiles, and promotion artifacts. Unsupported interval formats fail closed instead of being treated as zero-coverage data.

Dataset quality is a research gate. Missing candles, OHLC invariant violations, non-positive prices, negative volume, duplicate keys, non-monotonic timestamps, interval mismatches, or unexpected buckets make the affected split `FAIL` and propagate reason-coded failures such as `dataset_quality_train_missing_candles` into candidate gates. Warning-mode quality output is not promotion evidence. Candle-only data remains valid only for candle-appropriate directional/filter strategies such as the current `sma_with_filter`; it is not evidence for spread-sensitive, latency-sensitive, partial-fill-sensitive, microstructure, or intra-candle path-dependent strategies.
If `dataset_quality_gate_status=FAIL`, do not promote. Repair or rebuild the candle dataset, rerun `research-backtest`, and verify that the corrected report carries the expected `dataset_quality_hash`.

## Top-Of-Book Quotes

Top-of-book snapshots are persisted in SQLite table `orderbook_top_snapshots` as best bid/ask only. This is quote evidence, not executable liquidity evidence. It is not queue position, not trade ticks, and not an intra-candle path reconstruction.

When the raw public orderbook payload includes both `bid_size` and `ask_size` for every orderbook unit, the sync path also stores guarded L2 levels in `orderbook_depth_levels` with side, level index, price, size, cumulative size, cumulative notional, source, and observed timestamp. The top-of-book table remains the compatibility surface. If a payload lacks size fields, depth evidence is not fabricated and `l2_depth_rows_available=false` remains the correct report outcome.

Stored L2 rows are evidence, not a full replay engine. Dataset quality, readiness, and research reports distinguish literal row existence (`l2_depth_rows_available`) from complete bid/ask snapshot evidence (`l2_depth_complete_snapshots_available`, `l2_depth_snapshot_count`), row count, first/last depth timestamps, sources, and deterministic depth content hashes. `depth_available` is retained only as a compatibility field for complete stored L2 snapshots, with `depth_available_semantics=stored_l2_depth_complete_snapshots_exist_not_execution_model_used`; it does not mean a depth execution model was used. `full_orderbook_depth_available=false`, `top_of_book_is_full_depth=false`, `queue_position_available=false`, `trade_ticks_available=false`, `market_impact_model_available=false`, and `intra_candle_path_available=false` remain the correct claims unless future code implements and tests those capabilities.

Collect one current public quote snapshot with:

```bash
uv run bithumb-bot sync-orderbook-top
```

The command validates the current public best bid/ask, computes `spread_bps`, writes only to the configured managed SQLite DB, and prints pair, bid, ask, spread, source, top row count, optional depth row count, and next action. It does not write repo-local artifacts. To use quote joins in research, add `dataset.top_of_book` to the manifest and rerun `research-backtest` or `research-walk-forward`. Research joins use the nearest stored snapshot within `join_tolerance_ms`; missing snapshots outside that tolerance remain missing evidence. If optional quote coverage is incomplete, reports and CLI output print `top_of_book_gate_status=WARN`, coverage percentage, missing count, affected splits, and the next action. If required quote coverage is missing, reports and CLI output include a fail-closed quote gate and the same next action: collect orderbook top snapshots with `sync-orderbook-top`, rerun research, and verify `top_of_book_coverage_pct`.

Top-of-book remains optional for `sma_with_filter`; candle-only runs still work and remain valid only for candle-appropriate strategies. Future quote-sensitive strategies can require `top_of_book`; when a strategy requires it and the manifest lacks it, validation fails closed with `research_data_requirement_top_of_book_missing`. If a production-bound manifest requires full depth, trade ticks, queue position, market impact, or intra-candle path reconstruction and the corresponding evidence/model is unavailable, readiness and capability contracts fail closed with explicit unavailable capability reasons.

Top-of-book fields are metadata for current research execution evidence. Fill pricing still uses candle close or the configured execution-timing reference plus the configured fixed/stress bps model. A standalone depth-walk research execution model can consume an explicit L2 snapshot and compute VWAP, partial fill, remaining quantity, levels consumed, and depth sufficiency, but it is not wired into `research-backtest` or `research-walk-forward`. Manifest `execution_model.type="depth_walk"` is therefore rejected. Reports expose `depth_walk_execution_model_available=true` and `depth_walk_execution_model_used=false` until a future patch wires per-signal depth snapshot selection into the research execution path. Trade tick replay, queue position, calibrated market impact, full orderbook replay, and intra-candle path reconstruction remain unsupported unless explicitly implemented and tested.

## Execution Reality Contract

Research, candidate profiles, promotion artifacts, approved profiles, and paper execution evidence carry an `execution_reality_contract` plus deterministic `execution_contract_hash`. The hash is content-based, canonical, and excludes runtime-only timestamps such as `generated_at`.

The contract records the fill-reference policy, quote wait and missing-quote policy, required promotion reality level, top-of-book requirements, calibration binding, execution model assumptions for latency, partial fills, and order failure, and explicit unsupported capabilities. Top-of-book is classified as quote evidence only: `top_of_book_is_full_depth=false`. Stored L2 depth identity is reported separately from capability availability; rows in `orderbook_depth_levels` do not set `full_orderbook_depth_available=true` and do not satisfy `depth_required=true`. Trade ticks, queue position, market impact, and intra-candle path reconstruction remain unavailable unless a future patch implements real storage/API support and tests.

Production-bound manifests must declare their execution timing contract explicitly. Omitted `execution_timing`, `execution_timing: {}`, legacy `candle_close_legacy`, same-candle close fills, or a missing `min_execution_reality_level_for_promotion` fail at manifest parse time before research reports or candidate artifacts are generated. Candle-only production candidates must at least declare `fill_reference_policy="next_candle_open"`, `allow_same_candle_close_fill=false`, and `min_execution_reality_level_for_promotion="candle_next_open"`. Orderbook-based production candidates must also declare fail-closed quote behavior and a production-safe `dataset.top_of_book` contract with `required=true`, `missing_policy="fail"`, and `min_coverage_pct=100`.

Production-bound promotion/profile verification fails closed when the contract is missing, hash-mismatched, or requires unsupported execution capabilities. Profiles also compare their contract to the source promotion artifact, and profile/runtime comparison reports field-level execution contract mismatches when a runtime contract is supplied.

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

When `execution_model.calibration_required=true`, the calibration artifact must carry a valid `content_hash`. Missing hashes fail with `execution_calibration_content_hash_missing`; hash mismatches still fail with `execution_calibration_content_hash_mismatch`. Calibration also compares observed `partial_fill_rate` and `unfilled_rate` against scenario `partial_fill_rate` and `order_failure_rate`, and fails with `execution_calibration_partial_fill_rate_exceeds_assumption` or `execution_calibration_unfilled_rate_exceeds_assumption` when live execution is worse than the research scenario. Required or fail-strict calibration also enforces the execution-quality minimum sample count and a passing execution-quality gate, with `execution_calibration_sample_count_below_required` and `execution_calibration_quality_gate_not_passed` as explicit fail reasons. If calibration is optional and strictness is `warn`, missing or failing calibration remains explicit in the report but does not by itself fail an otherwise passing research-only candidate. Candidate profiles and promotion artifacts expose warn-mode breaches through `has_execution_calibration_warning`, `execution_calibration_warning_reasons`, and `promotion_warnings`; successful `research-promote-candidate` CLI output prints those same fields so an operator does not need to open JSON to notice the warning. Required calibration failures still refuse promotion and do not produce a successful promotion block.

Production-bound candidates are stricter. The centralized production calibration policy fails closed unless the candidate uses an explicit `execution_model`, sets `calibration_required=true`, sets `calibration_strictness="fail"`, carries a passing execution calibration gate, exposes a single deterministic calibration artifact hash, and preserves matching market, interval, fill-reference policy, execution-quality PASS, and minimum sample evidence. `warn` strictness is diagnostic only and cannot satisfy `profile-generate`, `profile-promote`, `live_dry_run`, or `small_live` transition evidence. Candidate profiles, promotion artifacts, approved profiles, and CLI failures expose stable production policy reason codes such as `production_execution_calibration_required`, `production_execution_calibration_strictness_must_be_fail`, `production_execution_calibration_hash_missing`, and `production_execution_calibration_hash_inconsistent`.

Execution calibration is not a complete execution-realism model. It binds observed cost and latency evidence to scenario assumptions, but it does not create full-depth order book evidence, queue position, trade ticks, or intra-candle path evidence when those data are absent. Production manifests should set `execution_timing.min_execution_reality_level_for_promotion` and top-of-book dataset requirements high enough for the strategy being considered.

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

Research reports now carry deterministic experiment lineage. Lineage records the experiment id, experiment family id, hypothesis id/status, manifest hash and canonical hash, dataset snapshot id, dataset content, dataset quality and split hashes, safe data-source fingerprint, repository version, command name and normalized command-args hash, cost/execution model hash, calibration hash when present, search budget, parameter grid size, attempt index, failed-candidate count, holdout reuse count, and dataset reuse policy. `lineage_hash` excludes volatile creation time. The hash proves the recorded lineage did not drift; it does not by itself prove the selected candidate survived statistical selection correction.
Current-generation promotion requires valid lineage by default and refuses no-lineage reports with `promotion refused: lineage_missing`. Historical no-lineage reports require explicit operator intent with `--allow-legacy-lineage`; that compatibility path records `legacy_compatibility_used=true`, `lineage_required=false`, `lineage_hash=null`, `dataset_quality_legacy_bypass_used=true`, `legacy_lineage_compatibility_used`, and `legacy_dataset_quality_bypass_used` in promotion warnings. This is compatibility-only and is not full new-generation dataset-quality verification. Live profile promotion refuses profiles carrying `dataset_quality_legacy_bypass_used=true`; rebuild current-generation lineage and dataset-quality evidence instead of using the bypass for live readiness. Do not use the compatibility path for new research.

When walk-forward evidence is required, promotion also requires the matching candidate in `walk_forward_report.json` to pass real rolling walk-forward validation.
It recomputes the canonical walk-forward report hash before binding `walk_forward_report_hash` into the promotion artifact. It does not trust the embedded walk-forward report `content_hash` field; missing or stale embedded source hashes fail closed with `walk_forward_report_content_hash_missing` or `walk_forward_report_hash_mismatch`.
The walk-forward candidate must match the backtest/OOS candidate's experiment, strategy name, parameter candidate id, parameter values, cost model, and manifest hash, and its candidate profile hash is independently recomputed.
Missing, mismatched, failed, or tampered walk-forward evidence is reported with source-specific reasons such as `walk_forward_missing`, `walk_forward_candidate_mismatch`, `walk_forward_gate_not_passed`, `walk_forward_metrics_missing`, or `walk_forward_candidate_profile_hash_mismatch`.

Production-bound promotion requires valid statistical selection evidence. A candidate with `acceptance_gate_result=PASS` is insufficient when statistical validation is required. Promotion verifies the evidence content hash, embedded content hash, selection universe hash, candidate metric values hash, manifest/dataset hashes, statistical contract, candidate count, search budget, parameter grid size, attempt index, holdout reuse count, dataset reuse policy, benchmark, primary metric/source, summary-metric bootstrap p-value, effective trial count, attempt budget, holdout reuse budget, and any configured statistical methods. Missing, tampered, mismatched, underreported, incomplete, or failed statistical evidence refuses promotion before an artifact is written.

When `stress_suite.required_for_promotion=true` or the candidate is production-bound by `deployment_tier`, promotion also requires passing stress evidence. Missing candidate-level contract body or hash, missing validation stress evidence, missing required final-holdout stress evidence, failed stress gates, stale stress hashes, or contract mismatches refuse promotion before an artifact is written. Production-bound promotion derives this requirement from `deployment_tier`; a missing, false, stale, or manually removed `stress_suite_required` flag cannot bypass the gate. Reproduction rechecks the embedded stress evidence hash, verifies required final-holdout stress evidence, verifies contract binding, and compares the promoted stress evidence back to the source `backtest_report.json` candidate.

The promotion artifact binds the evidence sources by recording `lineage_hash`, `validation_evidence_source`, `backtest_report_path`, `backtest_report_hash`, `candidate_profile_hash`, `backtest_candidate_profile_hash`, `backtest_candidate_profile_verified`, `statistical_evidence_path`, `statistical_evidence_hash`, `selection_universe_hash`, `candidate_metric_values_hash`, `final_selection_contract_hash`, `selected_candidate_id`, `selected_candidate_score_hash`, `candidate_final_scores_hash`, `candidate_metric_values_summary`, `metric_value_count`, `missing_metric_count`, `statistical_gate_result`, `summary_metric_max_bootstrap_p_value`, `white_reality_check_p_value`, `white_reality_check_method`, `promotion_grade_limitations`, `stress_suite_required`, `stress_suite_contract_hash`, `validation_stress_suite`, `final_holdout_stress_suite`, `stress_suite_gate_result`, `stress_suite_fail_reasons`, `walk_forward_required`, `walk_forward_report_path`, `walk_forward_report_hash`, `walk_forward_evidence_source`, `walk_forward_candidate_profile_hash`, and `walk_forward_candidate_profile_verified`. Promotion artifacts generated from `research-validate` also bind `validation_policy_source`, `validation_policy_required_stage_names`, `effective_walk_forward_required`, `effective_final_holdout_required`, `effective_stress_suite_required`, `effective_statistical_validation_required`, `effective_final_selection_required`, and the corresponding manifest-declared requirement flags. Operators must treat effective deployment-tier policy fields as the production evidence truth; weaker manifest flags are not sufficient production evidence. Promotion lineage also includes the final-selection hash fields, and its `lineage_hash` changes if those custody fields drift.
Promotion artifacts also expose metrics contract evidence at top level: `metrics_schema_version`, `validation_metrics_v2`, `final_holdout_metrics_v2`, `metrics_gate_policy`, `metrics_gate_policy_hash`, `metrics_contract_required`, and compact `metrics_v2_summary` fields for CAGR, expectancy, exposure, holding time, open-position state, and fee/slippage drag with ratio basis fields. Operators should not need to inspect nested candidate-profile internals to see the metrics contract and applied gate policy.
If walk-forward is not required, the promotion artifact explicitly records `walk_forward_required=false` and null walk-forward evidence hash/source fields.

Before generating an approved profile, run reproducibility verification:

```bash
uv run bithumb-bot research-reproduce \
  --promotion "$DATA_ROOT/paper/reports/research/<experiment>/promotion_<candidate>.json"
```

`research-reproduce` loads the promotion artifact, verifies the promotion content hash, validates lineage, reopens recorded backtest, statistical evidence, stress-suite evidence, and walk-forward reports when required, repeats the same source artifact truth check by recomputing canonical hashes from artifact bodies excluding embedded `content_hash`, revalidates the source report's complete external audit-trail binding against current trace manifest/index/stream files, recomputes `candidate_metric_values_hash` from the source backtest report's current candidates, compares final-selection fields across promotion, lineage, and the source backtest report, and requires statistical and stress-suite evidence for every production-bound deployment tier even if an old or tampered artifact omits or disables `statistical_validation_required` or `stress_suite_required`. It reports specific failure reasons such as `lineage_missing`, `lineage_hash_mismatch`, `backtest_report_hash_mismatch`, `backtest_report_embedded_content_hash_mismatch`, `audit_trail_required_for_promotion`, `audit_trail_trace_manifest_missing`, `audit_trail_trace_manifest_hash_mismatch`, `audit_trail_trace_index_missing`, `audit_trail_decision_stream_missing`, `audit_trail_equity_stream_missing`, `audit_trail_execution_stream_missing`, `audit_trail_hash_chain_mismatch`, `audit_trail_row_count_mismatch`, `audit_trail_stream_hash_mismatch`, `audit_trail_report_reference_hash_mismatch`, `audit_trail_non_terminal_status`, `statistical_evidence_missing`, `statistical_evidence_hash_mismatch`, `statistical_evidence_embedded_content_hash_mismatch`, `selection_universe_hash_mismatch`, `candidate_metric_values_hash_mismatch`, `candidate_metric_values_hash_recompute_mismatch`, `final_selection_contract_hash_mismatch`, `final_selection_score_hash_mismatch`, `final_selection_selected_candidate_mismatch`, `candidate_not_selected_by_final_selection_contract`, `statistical_candidate_count_mismatch`, `stress_suite_contract_mismatch`, `stress_suite_required_but_missing`, `stress_suite_hash_mismatch`, `final_holdout_stress_suite_required_but_missing`, `final_holdout_stress_suite_hash_missing`, `final_holdout_stress_suite_hash_mismatch`, `final_holdout_stress_suite_gate_not_passed`, `walk_forward_required_but_missing`, `walk_forward_report_hash_mismatch`, `walk_forward_report_embedded_content_hash_mismatch`, `dataset_content_hash_mismatch`, `dataset_quality_hash_mismatch`, `candidate_hash_mismatch`, `command_args_hash_mismatch`, `calibration_hash_missing`, and `calibration_hash_mismatch`. Old promotion artifacts without lineage are explicit `legacy_compatibility_used=true` and fail reproducibility verification instead of being treated as full lifecycle evidence. Recovery from missing or failed stress, final-selection, benchmark, or audit-trace evidence is to regenerate the research report and promotion artifact from the manifest and dataset, not to edit JSON, copy hashes, trace indexes, trace manifests, or JSONL trace rows by hand.
If a report hash, audit trace binding, statistical evidence hash, selection universe hash, or candidate metric values hash mismatches, regenerate the research report and statistical evidence from the same manifest and dataset snapshot rather than editing recorded hashes. If trace streams are missing or corrupt, run `research-verify-audit --experiment-id <experiment_id>` first to identify the missing manifest/index/stream or hash-chain failure, then investigate artifact loss or rerun research from the manifest and dataset snapshot. If walk-forward evidence is missing or mismatched, rerun walk-forward validation. If decision-equivalence later fails on profile, market, interval, or data fingerprint, rerun decision-equivalence from matching research/runtime decision evidence before attempting another profile transition.

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
`profile-promote` requires typed semantic evidence for both paper validation and live readiness. Evidence artifacts are `reports` artifacts and must carry `evidence_schema_version=1`, `evidence_type`, mode, market, interval, strategy name, approved profile hash, source promotion hash, observation start/end/duration, decision counts, blocked-decision counts, closed lifecycle counts, gross/fee/net PnL, expectancy/profit-factor/fee-drag fields when applicable, fee-drag basis when fee drag is present, execution-quality status and breach count, unresolved open order count, recovery blocker count, runtime/profile drift status, `db_data_fingerprint`, thresholds, decision-equivalence report path/content hash, and a deterministic `content_hash`. `generated_at` is operator context and is excluded from the deterministic hash.
Decision-equivalence is mandatory transition evidence for both paper validation and live readiness. Validation recomputes the decision-equivalence report hash from canonical report body excluding embedded `content_hash` and fails closed on missing path or hash, missing report file, hash mismatch, `outcome` missing or not `PASS_POSITIVE_EQUIVALENCE`, missing `claims_scope`, missing `state_coverage_matrix`, `ok=false`, nonzero mismatched decision count, non-empty missing research/runtime decision lists, profile hash mismatch, market mismatch, interval mismatch, comparable DB fingerprint mismatch, incomplete canonical decisions, `promotion_grade_comparison!=true`, unsupported state classes, fail-closed unmodeled states, or a legacy shallow comparison schema. Promotion evidence must use `comparison_contract_version=canonical_decision_v1`, `canonical_schema=true`, and `promotion_grade_comparison=true`. Rerun decision-equivalence when profile, market, interval, or data fingerprints drift.

Typed paper/live readiness evidence validation exists as a promotion contract. Effective promotion thresholds are repository-trusted policy, not self-declared evidence policy. Evidence artifact thresholds are retained as report metadata and must be at least as strict as the repository policy; weaker artifact thresholds fail closed with a policy-threshold reason code. `db_data_fingerprint` must be a non-empty `sha256:` value so the observation source is auditable. Live readiness rejects `execution_quality_status=not_applicable` by default; promotion to `small_live` requires real execution-quality applicability unless the repository policy is deliberately changed. Promotion fails closed when any required semantic field is missing, malformed, below threshold, weaker than trusted policy, or mismatched with the parent approved profile.
Typed evidence artifacts remain strict JSON and deterministic-hash artifacts. Paper/live readiness evidence rejects raw `Infinity`, `-Infinity`, and `NaN` values during content-hash validation with `<label>_schema_invalid:non_finite_json`, and rejects numeric strings that parse to non-finite values with the relevant field-level `<label>_schema_invalid:<field>` reason code. This improves malformed-artifact diagnosis and recovery action selection; it does not imply live readiness.

`strategy_performance.py` remains an operational closed-lifecycle guard over `trade_lifecycles`; it is not a research approval mechanism and is not a substitute for research promotion, paper validation evidence, or live readiness evidence. Runtime strategy performance exposes `fee_drag_ratio` with `fee_drag_ratio_basis="traded_notional"` when lifecycle matched quantity and entry/exit prices are available. It also preserves the historical gross-PnL denominator as `fee_to_gross_pnl_ratio` with `fee_to_gross_pnl_ratio_basis="gross_pnl_abs"`; `LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO` remains bound to that gross-PnL compatibility basis. This basis observability improves metric comparison safety but does not imply live readiness. Root/simple smoke backtests remain smoke-only and must not be used as promotion evidence.

The decision loop is repo-owned. For promotion evidence, research export must be bound to the reviewed approved profile:

```bash
uv run bithumb-bot research-export-decisions \
  --manifest <manifest.json> \
  --candidate-id <candidate_id> \
  --split validation \
  --profile <approved_profile.json> \
  --out <research_decisions.json>

uv run bithumb-bot runtime-replay-decisions \
  --profile <approved_profile.json> \
  --db <paper_or_runtime.sqlite> \
  --through-ts-list <timestamps.json> \
  --out <runtime_decisions.json>

uv run bithumb-bot decision-equivalence \
  --research-decisions <research_decisions.json> \
  --runtime-decisions <runtime_decisions.json> \
  --profile-hash <same approved profile hash> \
  --market <market> \
  --interval <interval> \
  --data-fingerprint <dataset_or_db_hash>
```

`research-export-decisions --profile` validates that the approved profile strategy, market, interval, manifest hash, dataset hash, selected candidate id, strategy parameters, cost model, and rebuilt candidate profile hash are compatible with the selected manifest/candidate/split, then writes canonical research decisions with the approved profile's `profile_content_hash`. The legacy no-profile export path is diagnostic only and emits `source=research_legacy_unbound`, `promotion_grade_export=false`, and `recommended_next_action=rerun_research_export_decisions_with_approved_profile`.

`runtime-replay-decisions` constructs the runtime SMA strategy from the approved profile and passes the approved profile's actual `regime_policy` plus approved-profile audit fields into candidate regime policy evaluation. It is read-only against SQLite, does not call live broker APIs, and does not submit orders.

The `decision-equivalence` command is a credential-free intermediate contract for comparing repo-generated research decisions with runtime replay decisions exported for the same candle snapshot and approved profile. Promotion-grade comparison requires validated repo-owned export wrappers with `source=research` and `source=runtime_replay`, valid wrapper `content_hash`, `repo_owned_export_artifacts=true`, matching decision counts, and wrapper/decision metadata bound to profile hash, market, interval, and dataset or DB fingerprint. Direct canonical-looking decision arrays, malformed wrappers, and manually prepared JSON are diagnostic only: the report sets `legacy_or_unverified_export=true`, `promotion_grade_comparison=false`, and recommends `regenerate_decisions_with_repo_owned_export_commands`.

Canonical decision events are represented by `decision_contract_version=1` and include candle/timestamp basis, raw and final signal, blocked filters and block reason, SMA features, edge/cost fields, fee/slippage/order-rule hashes, regime decision, position and dust hashes, entry/exit gates, exit rule/reason/evaluation hash, execution-timing policy hash, replay fingerprint hash, profile hash, and dataset or DB fingerprint. A canonical-looking payload is not promotion-grade unless required semantic fields are present and non-empty; `decision_contract_version=1` alone is insufficient. Promotion-grade export decisions must include `position_authority` with a non-empty `state_class`, and its `position_state_hash`, `order_rules_hash`, and `fee_authority_hash` must match the enclosing decision. Runtime order-rule identity is part of the canonical decision contract and must be an actual non-empty order-rule snapshot hash, not the hash of `{}`. The repo-owned positive equivalence classes are flat/no-dust/no-position and open-exposure lots when runtime replay emits complete lot-native authority fields. Reserved-exit-pending lots are model-scaffolded, may be classified by the runtime adapter, and remain fail-closed rather than production-grade positive evidence until repo-owned runtime-replay fixtures pass. Runtime-only states with dust, residue, recovery blockers, or executable lot authority that research cannot model must fail closed with position/dust mismatch reason codes rather than being silently promoted. The command does not call live broker APIs and does not prove execution quality, orderbook/depth behavior, or intra-candle path behavior. Profile transition evidence fails closed when decision-equivalence evidence is missing, hash-mismatched, bound to the wrong profile or dataset, legacy/shallow, incomplete, blocked, unverified, missing research/runtime export hashes, not promotion-grade, not ok, carries any nonzero mismatch count, lacks scope fields, or reports unsupported/fail-closed state scope.

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
Candle-only research remains path-limited. Top-of-book storage and quote coverage joins are available when explicitly configured. Guarded L2 depth storage and a queue-unaware depth-walk model exist for explicit depth snapshots, but production-grade queue, trade tick replay, calibrated market impact, and intra-candle path support remain separate follow-up work and should not be claimed as available.

Operator route:

- [`docs/runbooks/research-to-paper.md`](/docs/runbooks/research-to-paper.md)
