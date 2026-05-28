# bithumb-bot

Safety-first Bithumb trading bot.

The repository is optimized for:

- Wrong-order prevention
- Duplicate-order prevention
- State integrity and restart recovery
- Loss limits and emergency-stop behavior
- Operational observability and recoverability

## Position State Model

This bot uses lot-native executable position semantics.
The notes in this section describe the current implementation and its compatibility/reporting surfaces; they should not be read as a claim that every conceptual authority layer is already fully unified in code and emitted context.

- `open_exposure` is the canonical lot-native executable exposure.
- `dust_tracking` is operator-tracking residue and is kept separate from executable exposure.
- Dust operability is a projection on top of preserved evidence. A dust-only, sub-minimum, zero-executable state may be treated as flat for new-entry policy while still remaining accounting residue and excluded from SELL authority.
- `reserved_exit` is executable exposure that is already reserved by open SELL lifecycle state.
- `sellable_executable_lot_count` is the canonical SELL authority after subtracting reserved exit lots from open executable lots.
- `effective_flat` and `entry_gate_effective_flat` are BUY entry-gate interpretations only. They are not proof of zero holdings and do not define SELL authority, recovery authority, executable-position authority, recovery completeness, literal flatness, or restart safety.
- In the current implementation, SELL authority is grounded in `build_position_state_model()` outputs such as `normalized_exposure.sellable_executable_lot_count`, `normalized_exposure.exit_allowed`, `normalized_exposure.exit_block_reason`, `normalized_exposure.terminal_state`, and operator diagnostics. Legacy wording such as `holding_authority_state` should not be read as a current emitted/runtime authority field or canonical authority surface.
- Resume/recovery authority is a separate safety layer. In the current implementation it is determined from reconcile outcomes, runtime health, unresolved or recovery-required order state, halt conditions, dust resume policy, and explicit resume-eligibility checks; SELL authority or harmless dust alone is not sufficient to resume trading.
- Persisted lot-state row values remain `open_exposure` and `dust_tracking`.
- Current terminal/operator-facing normalized holding states are computed on top of persisted lot rows plus reservation and dust logic, and include `open_exposure`, `reserved_exit_pending`, `dust_only`, `flat`, and `non_executable_position`.
- `reserved_exit_pending` is a real normalized terminal state: executable exposure still exists, but normal SELL submission is blocked because the sellable lots are already reserved by open SELL orders.
- `dust_only`, `flat`, and `non_executable_position` remain distinct normalized outcomes and should not be collapsed into qty-first state interpretation.
- If no executable exit lot exists, SELL must be suppressed rather than submitted as a failed order. In the current implementation, that suppression is an observable/reportable outcome that can carry reason-coded telemetry and operator-facing reporting context; it is not just an invisible strategy no-op.
- Lot counts are the canonical executable state meaning.
- Qty remains non-authoritative, but it is still operationally required as a derived surface for broker payloads, sell-boundary handling, and reporting.
- Alias qty fields such as `position_qty`, `submit_payload_qty`, and `normalized_exposure_qty` may still appear in emitted/reporting context, but they are derived or compatibility/reporting surfaces and are not canonical SELL authority inputs.
- Current external/terminal SELL authority is lot-native, but current context materialization still passes through compatibility-aware fail-closed normalization for legacy or non-executable cases.

## Quick Start

```bash
uv sync
uv run pytest -q
uv run bithumb-bot health
```

## Canonical CLI

Use this command form as the canonical entrypoint:

```bash
uv run bithumb-bot <command>
```

Equivalent forms:

```bash
uv run python -m bithumb_bot <command>
uv run python bot.py <command>
```

The `project.scripts` entry in `pyproject.toml` defines the canonical CLI. Current operator-facing output and recovery guidance may still reference `uv run python bot.py <command>` as a compatibility surface.

## Env Loading Rules

- Do not rely on implicit `.env` autoloading.
- Use explicit env files for operator, live, and healthcheck operations.
- `BITHUMB_ENV_FILE` takes priority when it is set.
- `MODE=live` uses `BITHUMB_ENV_FILE_LIVE` when `BITHUMB_ENV_FILE` is not set.
- The supported runtime modes are `paper` and `live`.
- `MODE=paper` uses `BITHUMB_ENV_FILE_PAPER` when `BITHUMB_ENV_FILE` is not set.
- `MODE=test` only appears here as an env-selection compatibility edge case in the helper logic; it is not a normal operator/runtime mode.
- Explicit env files remain the operating standard for healthcheck and live-operation commands.
- Bootstrap loads the selected explicit env file opportunistically; if the file is missing, later config validation still fails when required settings are absent.

Example:

```bash
BITHUMB_ENV_FILE=.env uv run bithumb-bot health
```

Runtime artifacts must not be written into the repository. In `MODE=live`, every managed runtime root must be explicitly configured as an absolute repository-external path. In `MODE=paper`, `PathManager` falls back to the default runtime root under `XDG_STATE_HOME/bithumb-bot` or `~/.local/state/bithumb-bot` when a managed root is unset.

## Common Commands

```bash
uv run bithumb-bot sync
uv run bithumb-bot sync-orderbook-top
uv run bithumb-bot ticker
uv run bithumb-bot candles --limit 5
uv run bithumb-bot signal --short 7 --long 30
uv run bithumb-bot explain --short 7 --long 30
uv run bithumb-bot status
uv run bithumb-bot trades --limit 20
uv run bithumb-bot ops-report --limit 20
uv run bithumb-bot execution-quality-report --limit 200 --compare-manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot decision-telemetry --limit 200
uv run bithumb-bot decision-attribution --limit 500
uv run bithumb-bot strategy-report
uv run bithumb-bot research-backtest --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-walk-forward --manifest examples/research/sma_filter_manifest.example.json
uv run bithumb-bot research-verify-audit --experiment-id <id>
uv run bithumb-bot research-promote-candidate --experiment-id <id> --candidate-id <id>
uv run bithumb-bot research-promote-candidate --experiment-id <id> --candidate-id <id> --allow-legacy-lineage
uv run bithumb-bot research-reproduce --promotion <promotion.json>
uv run bithumb-bot profile-generate --promotion <promotion.json> --mode paper --out <profile.json>
uv run bithumb-bot research-export-decisions --manifest <manifest.json> --candidate-id <id> --split validation --profile <profile.json> --out <research_decisions.json>
uv run bithumb-bot runtime-replay-decisions --profile <profile.json> --db <paper_or_runtime.sqlite> --through-ts-list <timestamps.json> --out <runtime_decisions.json>
uv run bithumb-bot replay-decision --db <paper_or_runtime.sqlite> --strategy sma_with_filter --candle-ts <closed_candle_ts_ms> --json
uv run bithumb-bot decision-equivalence --research-decisions <research_decisions.json> --runtime-decisions <runtime_decisions.json> --profile-hash <profile_hash> --market <market> --interval <interval> --data-fingerprint <dataset_or_db_hash>
uv run bithumb-bot candidate-regime-policy-equivalence-evidence --backtest-report <backtest_report.json> --candidate-id <id> --decision-equivalence-report <decision_equivalence.json> --bind
uv run bithumb-bot profile-diff --profile <profile.json> --target-env <env-file> --json
uv run bithumb-bot profile-verify --profile <profile.json> --env <env-file>
uv run bithumb-bot config-dump --masked
uv run bithumb-bot runtime-strategy-set-lint
uv run bithumb-bot runtime-strategy-set-dump
uv run bithumb-bot live-dry-run
uv run bithumb-bot cash-drift-report --recent-limit 5
uv run bithumb-bot experiment-report --sample-threshold 30 --top-n 3
uv run bithumb-bot fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <evidence>
uv run bithumb-bot run
```

Root `backtest.py` is a fail-closed compatibility wrapper for a smoke backtest only. It does not run unless invoked with `--diagnostic-smoke-only`, and its output must not be used as evidence for strategy promotion, approved profiles, live readiness, or capital allocation. The official validation path is `uv run bithumb-bot research-validate --manifest ...`; `uv run bithumb-bot research-backtest --manifest ...` remains diagnostic/development evidence unless it is part of the full validation lifecycle. Full validation then requires walk-forward validation, lineage-backed promotion artifact review, `research-reproduce`, approved-profile generation or transition, mandatory decision-equivalence evidence, and separate paper/live-readiness checks. `--allow-legacy-lineage` is only an explicit compatibility escape hatch for reviewed historical artifacts, not the normal promotion path.

Command and evidence boundary:

| Command or artifact | Boundary | Evidence status | Operator next action |
| --- | --- | --- | --- |
| `python backtest.py` | Fail-closed by default | No evidence | Use `uv run bithumb-bot research-validate --manifest <path>` |
| `python backtest.py --diagnostic-smoke-only` | Diagnostic smoke only | Non-promotable; never approved-profile, live-readiness, or capital-allocation evidence | Use manifest-backed validation for any promotion path |
| `tools/diagnostic_smoke_backtest.py` | Diagnostic implementation outside the package runtime namespace | Not a promotion-grade backtest engine | Keep smoke output out of promotion artifacts |
| `uv run bithumb-bot research-backtest --manifest ...` | Research/development run unless bound inside full validation lifecycle | Diagnostic/development evidence by itself | Run `research-validate` for official validation lifecycle |
| `uv run bithumb-bot research-validate --manifest ...` | Official validation lifecycle | Manifest-backed validation evidence when all required stages pass | Use resulting validation/promotion artifacts for profile workflow |
| `decision-equivalence` | Submit-plan scoped unless explicit lifecycle evidence is present and verified | `SUBMIT_PLAN_EQUIVALENCE_ONLY`; not full lifecycle equivalence | Do not claim full lifecycle equivalence without fill, live-submit, broker response, accounting replay, and position lifecycle evidence |

Promotion-grade decision-equivalence evidence must be generated through the repo-owned `research-export-decisions --profile` and `runtime-replay-decisions` commands, then compared as validated export wrappers. Manual JSON decision arrays are diagnostic only. Promotion-grade decision exports must include explicit `position_authority` with a state class and hashes matching the decision's `position_state_hash`, `order_rules_hash`, and `fee_authority_hash`. The repo-owned positive-supported classes are currently `flat_no_dust_no_position` and `open_exposure` when runtime replay emits complete lot-native authority fields. `reserved_exit_pending` has partial `lot_native_simulation_v1` and runtime-adapter scaffolding, and the runtime adapter may classify the state, but it remains fail-closed transition evidence and is not production-grade positive evidence until a repo-owned runtime-replay fixture passes. Runtime-only dust, residue, non-executable-position, recovery-blocked, or otherwise unsupported states fail closed unless explicitly modeled and proven through repo-owned export/replay evidence. A fail-closed unmodeled state is safe, but it is not transition evidence and is not evidence of full research/paper/runtime lifecycle equivalence.

SMA decision exports carry `policy_contract_hash`, `policy_input_hash`, and `policy_decision_hash` as canonical diagnostics for the typed pure strategy decision contract. `policy_input_hash` identifies the authoritative decision inputs assembled by the plugin-owned SMA assembly boundary, including stable market, position, fee/order-rule, runtime-bound parameter, candidate-regime policy, exit-policy, and execution-sizing material. `policy_decision_hash` identifies the resulting signal, block reasons, exit result, and typed execution intent. Replay fingerprints bind both hashes plus replay timing/materialization metadata; result hashes alone must not be used to prove input equivalence. These hashes are replay and drift evidence for live/research comparison; they do not replace `strategy_behavior_hash`, approved-profile binding, runtime safety gates, or lot-native position authority.

Decision-equivalence reports include `claims_scope`, `state_coverage_matrix`, and an `outcome` such as `PASS_POSITIVE_EQUIVALENCE`, `FAIL_CLOSED_UNMODELED_STATE`, `FAIL_ACTUAL_DRIFT`, `FAIL_INCOMPLETE_CANONICAL_PAYLOAD`, or `FAIL_EXPORT_BINDING`. Profile transitions require scope-aware reports with `outcome=PASS_POSITIVE_EQUIVALENCE`, `ok=true`, `promotion_grade_comparison=true`, no unsupported state classes, and no fail-closed unmodeled states. Operators must distinguish explicitly modeled submit-plan/state equivalence from full lifecycle equivalence; current reports must not be read as proving lot-native lifecycle equivalence unless `claims_scope.full_lifecycle_equivalence_supported=true` and the report also shows typed/hash-bound simulated fill, paper submit/fill, live submit response, accounting replay, and position lifecycle evidence. Fail-closed unmodeled states are safety behavior, not positive equivalence or lifecycle proof.

The current execution-authority boundary is implemented in `src/bithumb_bot/decision_envelope.py`, `src/bithumb_bot/run_loop_execution_planner.py`, and `src/bithumb_bot/execution_service.py`. `DecisionEnvelope.strategy_decision` is typed strategy authority; persistence and observability dictionaries emitted from the envelope are explicitly non-authoritative. `ExecutionPlanBundle.submit_plan` and `ExecutionDecisionSummary` carry typed `ExecutionSubmitPlan` authority for submit decisions. Live real-order execution rejects dict-only submit authority, and paper typed/promotion execution consumes the same typed submit plan through `PaperSignalExecutionService` and `src/bithumb_bot/broker/paper.py`; paper broker logic may perform final cash/order-rule safety validation only as an execution-stage adjustment recorded against the typed plan. Research virtual execution also consumes typed `ExecutionSubmitPlan` authority through `SignalExecutionRequest`, with research-only timing/depth inputs carried in a typed non-authoritative research execution context.

Live broker submission requires the final validated serialization from `ExecutionSubmitPlan.as_final_payload()`, including schema version, authority label, and content hash. The broker-facing architecture and forbidden live real-order bypass paths are documented in [`docs/live-submit-authority.md`](/docs/live-submit-authority.md).

Portfolio target authority is documented in [`docs/portfolio-allocation-authority.md`](/docs/portfolio-allocation-authority.md). Strategy output becomes a typed non-authoritative `StrategyPreference`; `SignalAggregator` and `PortfolioAllocator` produce the authoritative `PortfolioTarget`; target-delta planning consumes that target before producing `ExecutionSubmitPlan`. Single-strategy runtime uses the same allocator path as the degenerate multi-strategy case. Missing or malformed portfolio target authority fails closed.

Multi-strategy runtime orchestration is configured with `RUNTIME_STRATEGY_SET_JSON`. `ACTIVE_STRATEGIES` remains a compatibility/diagnostic name-list shortcut only; it does not carry per-instance parameter, approved-profile, priority, weight, or risk authority, and live mode rejects multiple `ACTIVE_STRATEGIES` unless a structured strategy-set contract is provided. When neither is set, runtime resolves exactly one active strategy from `STRATEGY_NAME`. Active strategies are collected on the same closed candle, converted to typed `StrategyPreference`s, and allocated into one authoritative `PortfolioTarget` for the configured `settings.PAIR` before execution planning. The current run loop enforces a single-pair invariant at startup across paper, live dry-run, and live real-order paths.

Current equivalence evidence is submit-plan scoped by default. `src/bithumb_bot/decision_equivalence.py` emits `claim_scope=submit_plan_equivalence_only`, `submit_plan_equivalence_supported=true`, and `full_lifecycle_equivalence_supported=false` unless explicit typed/hash-bound lifecycle evidence is supplied and validated. Full lifecycle equivalence requires research simulated fill events, paper submit/fill events, live submit responses, accounting replay outputs, and position lifecycle snapshots. Promotion-grade gates fail closed if a report attempts to claim full lifecycle equivalence without that evidence. For submit-plan-only reports, the operator next action is to keep using manifest-backed validation and add lifecycle evidence fixtures before making any lifecycle-equivalence claim.

When a production-bound `sma_with_filter` candidate requires live candidate-regime policy but research did not apply that policy directly, promotion requires a separate `candidate_regime_policy_equivalence` evidence artifact. Generate it from a promotion-grade decision-equivalence report with `candidate-regime-policy-equivalence-evidence --bind`; the command writes a reports artifact, records its path and `sha256:` hash on the research candidate, recomputes the candidate profile hash, and leaves promotion fail-closed if the artifact is missing or tampered.

Use `config-dump --masked` for operator config inspection. Direct Python imports of
`bithumb_bot.config.settings` do not run the CLI bootstrap path and are not the
supported way to validate `BITHUMB_ENV_FILE`-loaded runtime configuration.

Operator reporting reference:

- [`docs/OPERATOR_REPORTING.md`](/docs/OPERATOR_REPORTING.md)

Research validation reference:

- [`docs/research-validation.md`](/docs/research-validation.md)
- [`docs/strategy-plugin-authoring.md`](/docs/strategy-plugin-authoring.md)

## Smoke / Manual DB Validation

- Smoke and manual validation must use absolute paths outside the repository.
- Do not point smoke/manual DB validation at repo-relative paths such as `./tmp`, `./data`, or `./backups`.
- Use an env-injected absolute runtime root and a repository-external temp directory instead.
- `tools/oms_smoke.py` defaults to `DB_PATH`, and you can override it with `--db-path` when needed.

Example:

```bash
tmp_dir="$(mktemp -d)"
MODE=paper \
RUN_ROOT="$tmp_dir/run" DATA_ROOT="$tmp_dir/data" LOG_ROOT="$tmp_dir/logs" BACKUP_ROOT="$tmp_dir/backup" ENV_ROOT="$tmp_dir/env" \
DB_PATH="$tmp_dir/data/paper/trades/paper.sqlite" \
uv run bithumb-bot sync
MODE=paper DB_PATH="$tmp_dir/data/paper/trades/paper.sqlite" uv run python tools/oms_smoke.py
```

To check for forbidden repo-local runtime artifacts:

```bash
./scripts/check_repo_runtime_artifacts.sh
```

## Path Policy

Authoritative references:

- [`docs/storage-layout.md`](/docs/storage-layout.md)
- [`docs/runtime-data-policy.md`](/docs/runtime-data-policy.md)

Rules:

- In `MODE=live`, `ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, and `BACKUP_ROOT` must be injected through env as absolute repository-external roots.
- In `MODE=paper`, those managed roots default under `XDG_STATE_HOME/bithumb-bot` or `~/.local/state/bithumb-bot` when unset; explicit overrides may still be supplied.
- `ARCHIVE_ROOT` defaults to the same runtime root's `archive/` subtree when unset in both modes, and in `MODE=live` an explicit `ARCHIVE_ROOT` must still be absolute and repository-external.
- Managed subtrees such as `run/<mode>`, `data/<mode>/*`, `logs/<mode>/*`, and `backup/<mode>/*` must be resolved through `PathManager`.
- `DB_PATH`, `RUN_LOCK_PATH`, `BACKUP_DIR`, and `SNAPSHOT_ROOT` are legacy compatibility override surfaces documented for the current storage contract; do not infer broader or newer override support from this list.
- In `MODE=live`, these overrides must still be absolute, repository-external, and mode-correct.
- Live helper scripts and deployment helpers should consult `PathManager` rather than inventing their own path scheme.
- Runtime artifacts belong under the managed runtime roots, not in the repository.

Expected artifact placement:

- Run lock, PID, and runtime state: `RUN_ROOT/<mode>/`
- DB: `DATA_ROOT/<mode>/trades/`
- Ops, strategy, fee, and recovery reports: `DATA_ROOT/<mode>/reports/<topic>/`
- Trade ledger artifacts: `DATA_ROOT/<mode>/trades/<topic>/`
- Derived artifacts: `DATA_ROOT/<mode>/derived/<topic>/`
- Raw artifacts: `DATA_ROOT/<mode>/raw/<topic>/`
- Logs: `LOG_ROOT/<mode>/<kind>/`
- Snapshot archives: `BACKUP_ROOT/<mode>/snapshots/`
- DB backups: `BACKUP_ROOT/<mode>/db/`

## Live Safety

- Real-order flow requires explicit arming.
- Live strategy selection is validated through the `ResearchStrategyPlugin` capability contract. `MODE=live` fails closed with `live_strategy_capability_validation_failed` unless the selected plugin declares promotion-grade runtime decisions, runtime replay, runtime decision adapter support, live eligibility for the requested arming mode, and the required approved-profile behavior. Legacy smoke-only selections such as `sma_cross` are rejected because they are not plugin-manifest promotion runtime strategies.
- Set `APPROVED_STRATEGY_PROFILE_PATH` to a reviewed approved profile before paper, live-dry-run, or live armed validation. `STRATEGY_APPROVED_PROFILE_PATH` is an older alias used only when the canonical selector is unset; if both are set, the canonical selector wins. `profile-generate` creates paper profiles only; live-compatible profiles must be created through explicit `profile-promote` transitions. The CLI never mutates env files or arms live trading. `profile-diff` compares profile values to env/runtime values and does not verify the artifact chain; `profile-verify` is the full env selector, runtime contract, source promotion, and evidence chain check. Runtime audit fields use `approved_profile_contract_scope=full_approved_profile`, `approved_profile_verification_ok=true`, and `legacy_candidate_profile_path_used=false` only for that full selector path; approved-profile success paths do not emit a legacy contract scope. `STRATEGY_CANDIDATE_PROFILE_PATH` is reported as `approved_profile_contract_scope=legacy_regime_policy_only` / `legacy_profile_contract_scope=regime_policy_only` and does not claim source, evidence, or runtime verification. Source promotion and evidence artifacts are verified for repository-external path policy, existence, content hash, lineage hash when required, typed schema, decision-equivalence hash, and semantic paper/live readiness thresholds; managed `DATA_ROOT/<mode>/reports/...` paths are accepted, and other repository-external absolute paths remain operator-custodied. Live dry-run startup fails closed unless it points to a verified `live_dry_run` profile. Live armed execution fails closed unless it points to a `small_live` approved profile whose strategy, market, interval, parameter, cost, source promotion hash, lineage hash, candidate profile hash, manifest hash, dataset hash, decision-equivalence hash, semantic evidence hashes, and regime policy contract matches runtime settings. Legacy `STRATEGY_CANDIDATE_PROFILE_PATH` remains a regime-policy compatibility selector only; it is not sufficient for live approval.
- `LIVE_DRY_RUN=true` is the safe starting point for live bring-up and post-change validation.
- `LIVE_REAL_ORDER_ARMED=true` is required before real orders are allowed.
- Live preflight must fail fast when required limits, notifier configuration, or safety inputs are missing.
- Current implementation runtime order is safety-first: preflight and startup reconcile/gate checks run before the steady-state loop, and each live loop iteration passes through runtime health, unresolved-order, halt, and submission-gate checks before strategy decision and submit-or-suppress handling.
- Current implementation strategy decisions are evaluated from guarded closed-candle input; incomplete, stale, or duplicate runtime candle input is skipped rather than treated as a fresh decision trigger.
- Recovery remains an operator-mediated workflow: commands such as `reconcile`, `recover-order`, and `resume` are explicit safety-gated procedures, not automatic recovery purely from passive state inspection.
- Current implementation risk handling is not limited to signal-time entry rejection; depending on runtime state it may also retain or trigger halt, cancel/reconcile, or flatten-position intervention paths.
- Do not merge paper and live storage.
- Do not weaken live preflight or emergency-stop behavior.

## 24/7 Ops

- Systemd units live under `deploy/systemd/`.
- Operator runbook: [`docs/RUNBOOK.md`](/docs/RUNBOOK.md)
- Limited unattended checklist: [`docs/LIMITED_UNATTENDED_CHECKLIST.md`](/docs/LIMITED_UNATTENDED_CHECKLIST.md)
- Backup script: `scripts/backup_sqlite.sh`

The rendered units use `BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_LIVE@` so the env file is injected explicitly.

## Test Groups

- Fast regression set:
  - `uv run pytest -q -m fast_regression`
- Slow integration/live-like set:
  - `uv run pytest -q -m slow_integration`

Prefer the fast regression set first. Keep the slow set separate unless you are validating restart, recovery, or live-like execution paths.
