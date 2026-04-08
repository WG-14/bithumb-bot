# bithumb-bot

Safety-first Bithumb trading bot.

The repository is optimized for:

- Wrong-order prevention
- Duplicate-order prevention
- State integrity and restart recovery
- Loss limits and emergency-stop behavior
- Operational observability and recoverability

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
- `MODE=paper` and `MODE=test` use `BITHUMB_ENV_FILE_PAPER` when `BITHUMB_ENV_FILE` is not set.
- Healthcheck and live-operation commands must fail fast when the explicit env file is missing.

Example:

```bash
BITHUMB_ENV_FILE=.env uv run bithumb-bot health
```

Runtime artifacts must live outside the repository under env-injected runtime roots.

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

- All runtime roots must be injected through env: `ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, `BACKUP_ROOT`, and `ARCHIVE_ROOT`.
- Managed subtrees such as `run/<mode>`, `data/<mode>/*`, `logs/<mode>/*`, and `backup/<mode>/*` must be resolved through `PathManager`.
- `DB_PATH`, `RUN_LOCK_PATH`, `BACKUP_DIR`, and `SNAPSHOT_ROOT` are compatibility overrides only.
- In `MODE=live`, these overrides must still be absolute, repository-external, and mode-correct.
- Live helper scripts and deployment helpers should consult `PathManager` rather than inventing their own path scheme.
- Runtime artifacts belong under the env-injected runtime roots, not in the repository.

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
