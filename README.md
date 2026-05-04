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
uv run bithumb-bot research-promote-candidate --experiment-id <id> --candidate-id <id>
uv run bithumb-bot profile-generate --promotion <promotion.json> --mode paper --out <profile.json>
uv run bithumb-bot profile-diff --profile <profile.json> --target-env <env-file> --json
uv run bithumb-bot profile-verify --profile <profile.json> --env <env-file>
uv run bithumb-bot config-dump --masked
uv run bithumb-bot live-dry-run --short 7 --long 30
uv run bithumb-bot cash-drift-report --recent-limit 5
uv run bithumb-bot experiment-report --sample-threshold 30 --top-n 3
uv run bithumb-bot fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <evidence>
uv run bithumb-bot run --short 7 --long 30
```

Use `config-dump --masked` for operator config inspection. Direct Python imports of
`bithumb_bot.config.settings` do not run the CLI bootstrap path and are not the
supported way to validate `BITHUMB_ENV_FILE`-loaded runtime configuration.

Operator reporting reference:

- [`docs/OPERATOR_REPORTING.md`](/docs/OPERATOR_REPORTING.md)

Research validation reference:

- [`docs/research-validation.md`](/docs/research-validation.md)

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
- Live SMA operation uses `sma_with_filter`; `MODE=live` rejects plain `sma_cross` with `plain_sma_live_not_allowed`.
- Set `APPROVED_STRATEGY_PROFILE_PATH` to a reviewed approved profile before paper or live-dry-run validation. Live armed execution fails closed unless it points to a `small_live` approved profile whose strategy, market, interval, parameter, cost, promotion hash, candidate profile hash, manifest hash, dataset hash, and regime policy contract matches runtime settings. Legacy `STRATEGY_CANDIDATE_PROFILE_PATH` remains a regime-policy compatibility selector only; it is not sufficient for live armed approval.
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
