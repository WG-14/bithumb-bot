# Storage Layout Contract

## Purpose

This document defines the storage contract for runtime data managed by the bot.
It exists to prevent wrong orders, duplicate orders, state corruption, and recoverability regressions.

The repository contains code, tests, docs, templates, and deployment definitions.
It does not contain runtime artifacts.

## Non-Negotiable Rules

- Do not write runtime artifacts into the Git repository.
- Do not write runtime artifacts to repo-relative paths such as `./data`, `./backups`, `./tmp`, or repo-root `*.log`.
- All path resolution must go through `PathManager`, `PathConfig`, and the helpers in `src/bithumb_bot/paths.py`.
- Paper and live data must remain fully separated.
- Live mode must fail fast on relative paths, repo-internal paths, and wrong-environment segments such as `paper`.
- Every new file output must be classified into exactly one storage bucket.

## Storage Buckets

The only valid storage buckets are:

- `env/`
- `run/`
- `data/<mode>/raw/`
- `data/<mode>/derived/`
- `data/<mode>/trades/`
- `data/<mode>/reports/`
- `logs/<mode>/...`
- `backup/<mode>/...`
- `archive/<mode>/...`

Classification examples:

- External API raw payload archive -> `raw`
- Feature snapshot, signal trace, or tuning intermediate -> `derived`
- Orders, fills, balances, or reconcile evidence -> `trades`
- Operator-readable summary JSON or report -> `reports`
- Runtime lock, PID, heartbeat, or transient state pointer -> `run`
- DB snapshot, redacted config snapshot, or recovery snapshot -> `backup`

## Required Layout

The storage contract is defined per managed root, not as one required shared tree.

Managed roots:

- `ENV_ROOT`
- `RUN_ROOT`
- `DATA_ROOT`
- `LOG_ROOT`
- `BACKUP_ROOT`
- `ARCHIVE_ROOT` when archive storage is enabled

Current live contract:

- Every managed live root must be explicitly set to an absolute path.
- Live managed roots must be repository-external.
- Live managed roots must not overlap.
- Live managed roots must not have parent/child relationships with one another.
- Managed roots themselves must stay mode-neutral; `paper`, `live`, and `dryrun` are not valid root path segments for the root directories.

Bucket structure is resolved relative to each managed root:

```text
ENV_ROOT/
  paper.env
  live.env

RUN_ROOT/
  paper/
    bithumb-bot.pid
    bithumb-bot.lock
    runtime_state.json
  live/
    bithumb-bot.pid
    bithumb-bot.lock
    runtime_state.json

DATA_ROOT/
  paper/
    raw/
    derived/
    trades/
      paper.sqlite
    reports/
  live/
    raw/
    derived/
    trades/
      live.sqlite
    reports/

LOG_ROOT/
  paper/
    app/
    strategy/
    orders/
    fills/
    errors/
    audit/
  live/
    app/
    strategy/
    orders/
    fills/
    errors/
    audit/

BACKUP_ROOT/
  paper/
    db/
    snapshots/
  live/
    db/
    snapshots/

ARCHIVE_ROOT/
  paper/
  live/
```

## Allowed Overrides

These env vars are compatibility overrides only, not new path conventions:

- `DB_PATH`
- `RUN_LOCK_PATH`
- `BACKUP_DIR`
- `SNAPSHOT_ROOT`

Rules for overrides:

- The override value must be an absolute path.
- Live mode requires a repository-external absolute path.
- Live mode must not use a path inside the repository.
- Live mode must not use a path that contains the wrong environment segment.
- The override must still map to the correct mode-specific storage bucket.

## Forbidden Patterns

Do not:

- Hardcode absolute runtime paths in code
- Concatenate runtime path strings manually
- Introduce direct path conventions outside the central path layer
- Bypass `PathManager` for DB, logs, run lock, reports, raw or derived artifacts, backups, or snapshots
- Share any storage between paper and live
- Overwrite live evidence in place

## Path Policy

Path locations are configuration. Path structure rules are code.

Path creation and path resolution must use the shared path layer:

- `PathManager`
- `PathConfig`
- `src/bithumb_bot/paths.py`

## Runtime Roots

Recommended live/runtime shape: choose separate absolute roots and pass them through the managed env vars.

Example:

```text
ENV_ROOT=/var/lib/bithumb-bot/env
RUN_ROOT=/var/lib/bithumb-bot/run
DATA_ROOT=/var/lib/bithumb-bot/data
LOG_ROOT=/var/log/bithumb-bot
BACKUP_ROOT=/var/backups/bithumb-bot
ARCHIVE_ROOT=/srv/bithumb-bot-archive
```

These examples are illustrative locations only. The contract is the managed-root separation and the mode-relative bucket structure, not a single required parent directory.

## File Placement Examples

### env

```text
ENV_ROOT/paper.env
ENV_ROOT/live.env
```

GitHub stores only `.env.example`. Real API keys, webhook secrets, and DB paths belong in runtime env files.

### run

```text
RUN_ROOT/live/bithumb-bot.lock
RUN_ROOT/live/bithumb-bot.pid
RUN_ROOT/live/runtime_state.json
```

The run lock must live under `RUN_ROOT/<mode>/`.
Do not invent a `data/locks/` or similar alternate lock path.

### trades

```text
DATA_ROOT/live/trades/live.sqlite
DATA_ROOT/live/trades/orders/orders_2026-03-30.jsonl
DATA_ROOT/live/trades/fills/fills_2026-03-30.jsonl
DATA_ROOT/live/trades/balance_snapshots/balance_snapshots_2026-03-30.jsonl
DATA_ROOT/live/trades/reconcile_events/reconcile_events_2026-03-30.jsonl
```

Use SQLite for stateful ledgers and JSONL append-only files for live evidence.

### reports

```text
DATA_ROOT/live/reports/ops_report/ops_report_2026-03-30.json
DATA_ROOT/live/reports/strategy_validation/strategy_validation_2026-03-30.json
DATA_ROOT/live/reports/recovery_report/recovery_report_2026-03-30.json
```

Reports are operator-readable outputs. Keep them separate from general logs.

### logs

```text
LOG_ROOT/live/app/app_2026-03-30.log
LOG_ROOT/live/strategy/strategy_2026-03-30.log
LOG_ROOT/live/orders/orders_2026-03-30.log
LOG_ROOT/live/errors/errors_2026-03-30.log
LOG_ROOT/live/audit/audit_2026-03-30.log
```

Keep log kinds separated. Valid log kinds are `app`, `strategy`, `orders`, `fills`, `errors`, and `audit`.

### backup

```text
BACKUP_ROOT/live/db/live.sqlite.20260330_120000.sqlite
BACKUP_ROOT/live/snapshots/runtime_snapshot_20260330_120000.tar.gz
```

Backups are mode-specific. Do not let paper and live share backup storage.

## Mode Separation

### paper

- Paper is for validation and simulation.
- Paper must never share DB, lock, logs, reports, backups, or snapshots with live.

### live

- Live is for real orders and recovery-critical evidence.
- Live requires explicit DB path configuration, notifier readiness, risk-limit configuration, and arming requirements.
- Live paths must be absolute, repository-external, and mode-correct.

## Storage Formats

### SQLite

Use SQLite for:

- Stateful ledgers
- Restart recovery state
- Portfolio, order, fill, and trade lifecycle state
- Bot health tables
- Other core recovery-critical tables

### JSONL append-only

Use JSONL append-only files for:

- Order request and response events
- Fill events
- Balance snapshots
- Reconcile summaries
- Strategy decision evidence
- Raw external response snapshots

Never overwrite live evidence in place.
Use append-only or timestamped snapshot patterns for live evidence, audit evidence, incident evidence, and strategy decision evidence tied to live actions.

## File Naming Rules

- Use KST-based dates when the operator workflow already depends on them.
- Use a clear timestamp format for snapshot-style files.
- Keep filenames unambiguous and mode-specific.
- Do not embed environment names in a way that can cause paper/live confusion.

## Backup and Retention

Backup priority:

1. Live DB
2. Redacted env snapshot
3. Reconcile, audit, and error evidence
4. Strategy and validation reports
5. Raw market cache

Retention guidance:

- DB snapshots: daily rotation with limited history
- Logs: recent hot retention with later archive movement
- Raw market cache: prune when safe
- Live trades, fills, and balances: keep as long as needed for recovery

## Path Contract Summary

The current codebase expects:

- Path resolution through `PathManager`
- Explicit env-root configuration for `ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, `BACKUP_ROOT`, and `ARCHIVE_ROOT`
- Live compatibility overrides to remain absolute, repository-external, and mode-correct
- Shared CLI and script helpers to consult managed paths instead of inventing new conventions

## Lot State Quantity Contract

`open_position_lots` is the persisted storage contract for the base lot-native
position rows.

Execution and reporting do not read final SELL authority directly from an
individual stored row. They materialize normalized authority from the persisted
lot-state rows together with reservation and dust interpretation.

Persisted schema-backed authority in `open_position_lots`:

- `qty_open`: stored row quantity for the current lot-state row.
- `executable_lot_count`: stored canonical executable lot authority for an `open_exposure` row.
- `dust_tracking_lot_count`: stored canonical operator-only residual lot authority for a `dust_tracking` row.
- `position_state`: stored lot-state classification. Current values are `open_exposure` and `dust_tracking`.
- `position_semantic_basis`: stored semantic basis and must remain `lot-native`.
- `lot_semantic_version`, `internal_lot_size`, `lot_min_qty`, `lot_qty_step`, `lot_min_notional_krw`, `lot_max_qty_decimals`, and `lot_rule_source_mode`: stored lot-rule metadata for interpretation and recovery.

Derived / interpreted outputs from the lot-state and dust interpretation layer:

- `open_lot_count`: interpreted executable exposure count derived from the stored lot-state row and surfaced for reporting / downstream logic.
- `raw_total_asset_qty`: interpreted broker-visible total remainder for the asset. It is useful for reconciliation and reporting, but it is not a stored `open_position_lots` column.
- `open_exposure_qty`: interpreted executable quantity derived from lot-native exposure for broker payloads, reporting, and compatibility.
- `dust_tracking_qty`: interpreted operator-only residual quantity derived from the dust-tracking lot state for evidence and reporting.
- `sellable_executable_qty`: interpreted sell-submit quantity derived from sellable executable lot count, not a persisted column.
- `sellable_executable_lot_count`, `reserved_exit_lot_count`, `exit_allowed`, and `exit_block_reason`: normalized execution/reporting authority fields materialized from persisted lot rows plus reservation and dust interpretation; they are not direct `open_position_lots` storage columns.

Practical routing rules:

- BUY fills create or refresh `open_exposure` lots.
- SELL matching consumes `open_exposure` lots only.
- `dust_tracking` lots are not sellable inventory and must not be counted as the basis for a normal SELL order.
- Harmless dust suppression is defined around the `dust_tracking` path, not the `open_exposure` path.
- Suppression behavior must avoid creating a normal SELL order, SELL event, or fresh client order ID for dust-only exits unless an operator explicitly clears the dust state.
- Reporting should surface the interpreted fields `open_lot_count`, `open_exposure_qty`, `dust_tracking_qty`, `sellable_executable_qty`, and `raw_total_asset_qty` together with the persisted lot-state fields so operators can explain the gap between broker-visible holdings and the sellable position base.
- Boundary rule: `qty_open < min_qty` may be reclassified to `dust_tracking`; `qty_open == min_qty` stays `open_exposure`.
- If a malformed `dust_tracking` lot appears above `min_qty`, it is still treated as operator evidence and remains excluded from normal SELL submission until an operator clears the inconsistency.
- Routing summary:
  - BUY creates or refreshes `open_exposure` lots.
- SELL lifecycle and real-order submission use normalized lot-native authority materialized from the persisted lot-state base contract, with `open_exposure_qty` materialized from that authority for the final broker payload.
  - `dust_tracking_qty` is operator-tracking evidence only and is excluded from normal SELL submission.
  - Harmless dust suppression is anchored to the `dust_tracking` path, not the `open_exposure` path.
