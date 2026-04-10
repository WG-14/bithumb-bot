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

Use the following runtime root structure:

```text
RUNTIME_ROOT/
  env/
    paper.env
    live.env

  run/
    paper/
      bithumb-bot.pid
      bithumb-bot.lock
      heartbeat.json
    live/
      bithumb-bot.pid
      bithumb-bot.lock
      heartbeat.json

  data/
    paper/
      raw/
        market/
        broker/
        snapshots/
      derived/
        indicators/
        features/
        validation/
      trades/
        paper.sqlite
        orders/
        fills/
        balances/
        reconcile/
      reports/
        ops/
        strategy/
        pnl/
    live/
      raw/
        market/
        broker/
        snapshots/
      derived/
        indicators/
        features/
        validation/
      trades/
        live.sqlite
        orders/
        fills/
        balances/
        reconcile/
      reports/
        ops/
        strategy/
        pnl/

  logs/
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

  backup/
    paper/
      db/
      configs/
      snapshots/
    live/
      db/
      configs/
      snapshots/

  archive/
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

Recommended runtime roots:

```text
/var/lib/bithumb-bot/
```

Alternative:

```text
/home/<run-user>/trading-bot-runtime/
```

The repository should refer to these roots through `RUNTIME_ROOT` and the managed env variables.

## File Placement Examples

### env

```text
RUNTIME_ROOT/env/paper.env
RUNTIME_ROOT/env/live.env
```

GitHub stores only `.env.example`. Real API keys, webhook secrets, and DB paths belong in runtime env files.

### run

```text
RUNTIME_ROOT/run/live/bithumb-bot.lock
RUNTIME_ROOT/run/live/bithumb-bot.pid
RUNTIME_ROOT/run/live/heartbeat.json
```

The run lock must live under `run/<mode>/`.
Do not invent a `data/locks/` or similar alternate lock path.

### trades

```text
RUNTIME_ROOT/data/live/trades/live.sqlite
RUNTIME_ROOT/data/live/trades/orders/orders_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/fills/fills_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/balances/balance_snapshots_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/reconcile/reconcile_2026-03-30.jsonl
```

Use SQLite for stateful ledgers and JSONL append-only files for live evidence.

### reports

```text
RUNTIME_ROOT/data/live/reports/ops/ops_report_2026-03-30T090000KST.txt
RUNTIME_ROOT/data/live/reports/strategy/strategy_report_2026-03-30.json
RUNTIME_ROOT/data/live/reports/market_catalog_diff/market_catalog_diff_2026-03-30.jsonl
```

Reports are operator-readable outputs. Keep them separate from general logs.

### logs

```text
RUNTIME_ROOT/logs/live/app/app_2026-03-30.log
RUNTIME_ROOT/logs/live/strategy/strategy_2026-03-30.log
RUNTIME_ROOT/logs/live/orders/orders_2026-03-30.log
RUNTIME_ROOT/logs/live/errors/error_2026-03-30.log
RUNTIME_ROOT/logs/live/audit/audit_2026-03-30.log
```

Keep log kinds separated. Valid log kinds are `app`, `strategy`, `orders`, `fills`, `errors`, and `audit`.

### backup

```text
RUNTIME_ROOT/backup/live/db/live.sqlite.20260330_120000.sqlite
RUNTIME_ROOT/backup/live/configs/live.env.20260330_120000.redacted
RUNTIME_ROOT/backup/live/snapshots/runtime_snapshot_20260330_120000.tar.gz
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

### dryrun

- If a dry-run mode exists, it must not share paper or live storage.
- Dry-run may use a separate layout only if it is explicitly isolated from both modes.

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

`open_position_lots.position_state` is part of the storage contract and must keep the following meaning stable:

- `open_lot_count`: canonical executable exposure authority for the position row.
- `dust_tracking_lot_count`: canonical operator-only residual authority for the position row.
- `raw_total_asset_qty`: broker-visible total remainder for the asset. This is useful for reconciliation and reporting, but it is derived / compatibility-oriented rather than the semantic authority.
- `open_exposure_qty`: derived executable quantity materialized from the lot-native open exposure state for broker payloads, reporting, and compatibility.
- `dust_tracking_qty`: derived operator-only residual quantity materialized from the dust-tracking lot state for evidence and reporting.
- `open_exposure`: real strategy exposure that may be sold normally.
- `dust_tracking`: operator-only residual tracking for harmless dust evidence.

Practical routing rules:

- BUY fills create or refresh `open_exposure` lots.
- SELL matching consumes `open_exposure` lots only.
- `dust_tracking` lots are not sellable inventory and must not be counted as the basis for a normal SELL order.
- Harmless dust suppression is defined around the `dust_tracking` path, not the `open_exposure` path.
- Suppression behavior must avoid creating a normal SELL order, SELL event, or fresh client order ID for dust-only exits unless an operator explicitly clears the dust state.
- Reporting must surface `open_lot_count`, `dust_tracking_lot_count`, `open_exposure_qty`, `dust_tracking_qty`, and `raw_total_asset_qty` together so operators can explain the gap between broker-visible holdings and the sellable position base.
- Boundary rule: `qty_open < min_qty` may be reclassified to `dust_tracking`; `qty_open == min_qty` stays `open_exposure`.
- If a malformed `dust_tracking` lot appears above `min_qty`, it is still treated as operator evidence and remains excluded from normal SELL submission until an operator clears the inconsistency.
- Routing summary:
  - BUY creates or refreshes `open_exposure` lots.
  - SELL lifecycle and real-order submission use lot-native exposure counts as the canonical state authority, with `open_exposure_qty` materialized from that state for the final broker payload.
  - `dust_tracking_qty` is operator-tracking evidence only and is excluded from normal SELL submission.
  - Harmless dust suppression is anchored to the `dust_tracking` path, not the `open_exposure` path.
