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

- `open_exposure` is the canonical lot-native executable exposure.
- `dust_tracking` is operator-tracking residue and is kept separate from executable exposure.
- `reserved_exit` is executable exposure that is already reserved by open SELL lifecycle state.
- `sellable_executable_lot_count` is the canonical SELL authority after subtracting reserved exit lots from open executable lots.
- Current terminal/operator-facing states include `open_exposure`, `reserved_exit_pending`, `dust_only`, `flat`, and `non_executable_position`.
- `reserved_exit_pending` is a real normalized terminal state: executable exposure still exists, but normal SELL submission is blocked because the sellable lots are already reserved by open SELL orders.
- `dust_only`, `flat`, and `non_executable_position` remain distinct normalized outcomes and should not be collapsed into qty-first state interpretation.
- If no executable exit lot exists, SELL must be suppressed rather than submitted as a failed order.
- Lot counts are the canonical executable state meaning.
- Qty remains an exchange-interface and reporting value, derived from the lot-native state.
- Current external/terminal authority is lot-native, but internal fail-closed compatibility and fallback handling still remains in code for legacy or non-executable cases.

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

The `project.scripts` entry in `pyproject.toml` defines the canonical CLI.

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
uv run bithumb-bot decision-telemetry --limit 200
uv run bithumb-bot strategy-report
uv run bithumb-bot cash-drift-report --recent-limit 5
uv run bithumb-bot experiment-report --sample-threshold 30 --top-n 3
uv run bithumb-bot run --short 7 --long 30
```

Operator reporting reference:

- [`docs/OPERATOR_REPORTING.md`](/docs/OPERATOR_REPORTING.md)

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
- `LIVE_DRY_RUN=true` is the safe starting point for live bring-up and post-change validation.
- `LIVE_REAL_ORDER_ARMED=true` is required before real orders are allowed.
- Live preflight must fail fast when required limits, notifier configuration, or safety inputs are missing.
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
