# AGENTS.md

## Purpose

This repository is a safety-first Bithumb trading bot project.
When modifying this codebase, optimize for:

1. prevention of wrong orders
2. prevention of duplicate orders / state corruption
3. restart recovery and ledger consistency
4. loss limits and emergency stop behavior
5. operational observability, alerts, and recoverability

Profitability improvements are always secondary to execution safety and state integrity.

This file defines persistent project instructions for all patches.
Treat it as a repository-level operating contract, not as optional guidance.

---

## Repository intent

This repository is not yet in a “maximize profit” phase.
Current priority is late Stage 2 to early Stage 3 maturity:
recovery-aware execution, storage/path correctness, live-safe preflight, operator visibility, and operations-ready structure.
Any patch that increases strategy complexity while weakening these foundations is the wrong direction.

---

## Mandatory reading before any patch

Before making changes, read and follow these files:

- `docs/storage-layout.md`
- `docs/runtime-data-policy.md`
- `README.md`

If the patch touches path handling, runtime outputs, backups, logs, DB paths, or env loading, also inspect:

- `src/bithumb_bot/paths.py`
- `src/bithumb_bot/storage_io.py`
- `src/bithumb_bot/run_lock.py`

If the patch touches live execution, restart recovery, order lifecycle, or operator flows, inspect the relevant docs and tests before changing code.

---

## Project priorities

Use this priority order when making tradeoffs:

### P0 — must not regress
- wrong-order prevention
- duplicate-order prevention
- ledger/state consistency
- crash/restart recoverability
- live/paper separation
- safe live preflight and fail-fast checks
- emergency stop / halt safety
- single-instance safety
- no silent corruption

### P1
- operator visibility
- structured logging
- healthcheck quality
- ops commands
- recovery reporting
- backup and restore verification

### P2
- strategy tuning
- analytics
- performance optimization
- research convenience

Never accept a P2 improvement that weakens P0/P1 safety properties.

---

## Storage and path contract

This repository uses a strict storage contract.

### Core rule
Runtime outputs must not be written into the Git repository.

Do not write runtime artifacts to paths like:
- `./data/...`
- `./backups/...`
- `./tmp/...`
- repo-root `*.log`

The repository is for:
- code
- tests
- docs
- templates
- deployment definitions

Runtime artifacts belong in runtime roots injected by env.

### Required path policy
All path creation and path resolution must go through the shared path layer.

Use:
- `PathManager`
- `PathConfig`
- existing path helpers in `src/bithumb_bot/paths.py`

Do not:
- hardcode absolute runtime paths in code
- concatenate path strings manually for runtime artifacts
- introduce new direct path conventions outside the central path layer
- bypass PathManager for DB, logs, run lock, reports, raw/derived artifacts, backups, or snapshots

### Path principle
Path locations are configuration.
Path structure rules are code.

That means:
- env/config decides root locations
- code decides standardized subdirectories and filenames

---

## Environment separation rules

`paper` and `live` must remain fully separated.

They must never share:
- DB
- run lock
- pid
- runtime state
- logs
- reports
- backups
- snapshots
- audit evidence

Any patch that mixes `paper` and `live` storage is invalid.

For `MODE=live`:
- require repository-external absolute paths
- fail fast on relative paths
- fail fast on repo-internal paths
- fail fast on paths containing wrong environment segments such as `paper`

Do not weaken these guards.

---

## Runtime data classification rules

When introducing any new file output, first classify it explicitly into one of these buckets:

- `env/`
- `run/`
- `data/<mode>/raw/`
- `data/<mode>/derived/`
- `data/<mode>/trades/`
- `data/<mode>/reports/`
- `logs/<mode>/...`
- `backup/<mode>/...`
- `archive/<mode>/...`

If a new artifact is added, the patch must state what class it belongs to.

Examples:
- external API raw payload archive → `raw`
- feature snapshot / signal trace / tuning intermediate → `derived`
- orders / fills / balances / reconcile evidence → `trades`
- operator-readable summary JSON or report → `reports`
- runtime lock / pid / heartbeat / transient state pointer → `run`
- DB snapshot / redacted config snapshot / recovery snapshot → `backup`

Do not mix experimental outputs into the live trading ledger.
Do not store tuning/debug dumps in the same place as authoritative trade records.

---

## File format and mutation rules

### Prefer SQLite for
- stateful ledgers
- restart recovery state
- portfolio/order/fill/trade lifecycle state
- bot health tables
- other core recovery-critical tables

### Prefer JSONL append-only for
- order request/response events
- fill events
- balance snapshots
- reconcile summaries
- strategy decision evidence
- raw external response snapshots

### Never overwrite in place for live evidence
Do not use destructive overwrite semantics for:
- live order/fill evidence
- live balance snapshots
- audit evidence
- incident evidence
- strategy decision evidence tied to live actions

Use append-only or timestamped snapshot patterns instead.

### Atomic write rules
For non-append file writes, use existing atomic write helpers and preserve durability patterns already used in the repo.

---

## Logging and observability rules

Maintain the existing logging separation model.

Valid log kinds:
- app
- strategy
- orders
- fills
- errors
- audit

Do not collapse all logs into one undifferentiated sink if it harms operations.
Do not log secrets or full auth credentials.

Never log:
- API secret
- webhook secret
- full auth headers
- sensitive private payloads without redaction

Prefer structured, grep-friendly, incident-friendly logs.
When practical, preserve identifiers useful for correlation, such as:
- client order id
- exchange order id
- signal timestamp
- side
- state transition
- disable / block reason

---

## Live safety rules

Treat live-mode behavior as safety-critical.

Do not remove or weaken:
- live preflight checks
- safe default enforcement
- notifier requirements
- arming requirements for real orders
- slippage/spread protections
- max order / max daily loss / max daily order count guards
- kill switch / halt protections
- reconciliation on restart
- operator intervention requirements when consistency is unclear

Real-order flows must remain explicitly armed.
Dry-run and real-order behavior must remain clearly separated.

Any live patch must prefer fail-fast over ambiguous execution.

---

## Single-instance and runtime lock rules

This bot must not allow unsafe concurrent run loops.

Do not remove or weaken:
- run lock acquisition
- stale lock diagnostics
- mode-specific run lock separation
- Linux/WSL-only enforcement where required by locking semantics

New run-related code must respect the existing lock model and mode-specific run directories.

---

## Recovery and state integrity rules

State consistency is more important than convenience.

Any patch touching execution, order submission, fills, reconciliation, restart behavior, or DB writes must preserve:

- durable local intent recording
- correct order state transitions
- fill deduplication
- no overfill corruption
- no negative-balance corruption
- restart reconciliation behavior
- explicit handling of unknown/unrecoverable states

Do not “simplify” code in ways that remove recovery evidence or reduce the ability to explain what happened after a crash.

When uncertain, choose explicit stop / halt / recovery-required behavior over silent continuation.

---

## Strategy and research rules

Strategies are replaceable rules.
The live/runtime infrastructure is the stable socket they plug into.

When changing strategy code:
- keep strategy outputs inspectable
- keep validation/reporting paths separated from core trade ledger
- avoid coupling strategy experiments directly to live execution state
- preserve the ability to compare strategies through env/config selection

Strategy experimentation must not weaken operational safety.

---

## Env loading and configuration rules

Respect the repository’s explicit env-loading model.

Do not reintroduce implicit `.env` autoloading.
Keep explicit env selection behavior consistent with the bootstrap/config flow.

Prefer configuration injection over hardcoded behavior.

Backward-compatible overrides may exist, but do not expand override sprawl unnecessarily.
If introducing new env vars:
- document them in `.env.example` when appropriate
- keep naming consistent
- define whether they are paper-safe, live-required, or optional
- preserve fail-fast behavior for unsafe live omissions

---

## Deployment and script rules

Repository scripts and deployment helpers must also obey the same storage contract.

Shell scripts, systemd units, healthcheck helpers, backup scripts, and runtime inspection tools must not invent their own path scheme.
They should resolve managed paths through the canonical path interface wherever possible.

Do not patch only Python while leaving scripts inconsistent with the path policy.

---

## Testing expectations

After a patch, run targeted tests for changed areas and enough broader tests to catch contract regressions.

Minimum expectations:

### Standard test command
```bash
uv run pytest -q
```

### If environment/import issues appear
Use the repository-supported execution style rather than inventing ad hoc commands.
Prefer `uv run ...` entrypoints and the current package layout under `src/`.

### Always run relevant focused tests when touching:
- paths / storage contract
  - `tests/test_paths.py`
  - `tests/test_path_config_integration.py`
  - `tests/test_paths_cli.py`
  - `tests/test_db_path_resolution.py`
  - `tests/test_storage_io.py`
- live mode / preflight / live broker guards
  - `tests/test_live_preflight.py`
  - `tests/test_live_broker.py`
  - `tests/test_config_live_db_path_guard.py`
  - `tests/test_mode_validation.py`
  - `tests/test_order_rules_sync.py`
- recovery / restart / accounting integrity
  - `tests/test_fill_dedupe.py`
  - `tests/test_ledger_atomicity.py`
  - `tests/test_accounting_safety.py`
  - `tests/test_recovery_restart_regression.py`
  - `tests/test_recovery_recent_activity_interpretation.py`
  - `tests/test_trade_lifecycle.py`
- run lock / ops / observability
  - `tests/test_run_lock.py`
  - `tests/test_health_persistence.py`
  - `tests/test_operator_commands.py`
  - `tests/test_ops_report.py`
  - `tests/test_backup_sqlite_script.py`
  - `tests/test_sqlite_restore_verify_tool.py`

If you change behavior, update or add tests.
Do not ship behavior changes without test coverage when the area is safety-critical.

---

## Change-size discipline

Prefer small, reviewable patches.

Avoid mixing these in one patch unless necessary:
- path/storage refactor
- live execution logic changes
- strategy logic changes
- deployment/script changes
- observability/reporting changes

Separate structural safety changes from profitability experiments.

---

## Backward compatibility discipline

This project may still carry compatibility shims for older commands or overrides.
Do not remove them casually.

Before removing compatibility behavior:
- verify it is truly unused
- verify docs and scripts are aligned
- verify tests are updated
- explain the migration impact clearly

---

## Line ending and formatting rules

Respect `.gitattributes`.

Do not introduce unnecessary formatting churn.
Do not change EOL policy for `.sh`, `.py`, `.md`, `.yml`, `.yaml`.

Avoid unrelated reformatting in safety-critical patches.

---

## Patch output requirements

For each non-trivial patch, provide a concise summary that includes:

1. what changed
2. why it changed
3. which storage/path/runtime contract rules were relevant
4. whether any new artifact/output was introduced and how it was classified
5. what tests were run
6. any remaining risks or follow-up items

If a patch adds a new runtime artifact, explicitly state:
- classification
- mode separation behavior
- retention/append/snapshot semantics
- whether it is recovery-critical or diagnostic-only

---

## Things to avoid

Do not:
- write runtime files into the repo
- hardcode runtime paths
- weaken live preflight
- weaken notifier/live arming checks
- merge paper/live storage
- bypass central path helpers
- replace append-only evidence with overwrite behavior
- remove recovery evidence
- log secrets
- make broad formatting-only changes during safety patches
- trade safety for convenience

---

## In case of ambiguity

When requirements conflict, choose the safer interpretation.

Default preference order:
1. protect funds
2. protect state integrity
3. preserve recovery evidence
4. preserve operator visibility
5. preserve convenience
6. improve profitability

If a requested change appears to conflict with `docs/storage-layout.md` or `docs/runtime-data-policy.md`, follow the docs and keep the storage contract intact unless those docs are explicitly updated as part of the same task.
