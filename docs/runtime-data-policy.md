# Runtime Data Policy

## Purpose

This document defines how runtime data is classified, stored, logged, backed up, and recovered.
It is a storage contract, not a narrative guide.

Its primary goal is safety:

- Prevent wrong orders
- Prevent duplicate orders and state corruption
- Preserve restart recovery
- Preserve audit evidence
- Keep paper and live fully separated

## Scope

This policy applies to:

- Explicit env files
- SQLite DBs
- Run lock, PID, and heartbeat files
- Logs
- Order, fill, balance, and reconcile evidence
- Healthcheck, ops-report, and incident snapshots
- Backup, snapshot, and archive outputs

Anything outside this scope is not a runtime artifact.

## Non-Negotiable Rules

- Do not write runtime artifacts into the repository.
- Do not use repo-relative runtime paths or invent new path conventions outside the managed path layer.
- Do not share storage between paper and live.
- Do not overwrite live evidence in place.
- Do not log secrets or full credentials.
- Do not weaken live-mode fail-fast behavior.

## Data Classes

### env

Use `env/` for explicit runtime env files.

Examples:

- `paper.env`
- `live.env`

The repository keeps only `.env.example`.

### run

Use `run/` for transient runtime state:

- PID files
- Run locks
- Heartbeats
- Temporary state pointers

### raw

Use `data/<mode>/raw/` for external payloads and raw snapshots:

- Market API payload archives
- Broker API payload archives
- Raw response captures
- Redaction-input evidence

### derived

Use `data/<mode>/derived/` for computed intermediates:

- Features
- Indicators
- Validation traces
- Signal traces
- Tuning intermediates

### trades

Use `data/<mode>/trades/` for recovery-critical lifecycle evidence:

- Orders
- Fills
- Balances
- Reconcile evidence
- Trade lifecycle state
- Stateful ledgers

### reports

Use `data/<mode>/reports/` for operator-readable outputs:

- Ops reports
- Strategy reports
- Recovery reports
- Validation summaries
- Incident summaries

### logs

Use `logs/<mode>/...` for kind-separated logs:

- app
- strategy
- orders
- fills
- errors
- audit

### backup

Use `backup/<mode>/...` for DB snapshots, redacted config snapshots, and recovery snapshots.

### archive

Use `archive/<mode>/...` for long-term retention when an output has been retired from active use.

## Storage Format Rules

### SQLite

Prefer SQLite for:

- Stateful ledgers
- Restart recovery state
- Portfolio, order, fill, and trade lifecycle state
- Bot health tables
- Other core recovery-critical tables

### JSONL append-only

Prefer JSONL append-only for:

- Order request and response events
- Fill events
- Balance snapshots
- Reconcile summaries
- Strategy decision evidence
- Raw external response snapshots

### Snapshot semantics

Use append-only or timestamped snapshots for:

- Live order evidence
- Live fill evidence
- Live balance snapshots
- Incident evidence
- Strategy decision evidence tied to live actions

Never replace recovery evidence with overwrite behavior.

## Logging Rules

Keep the existing log separation model.

Valid log kinds are:

- `app`
- `strategy`
- `orders`
- `fills`
- `errors`
- `audit`

Do not collapse all logs into one sink if that would reduce operational clarity.

Never log:

- API secrets
- Webhook secrets
- Full auth headers
- Sensitive private payloads without redaction

Prefer structured, grep-friendly, incident-friendly logs.
Preserve useful correlation identifiers where practical:

- Client order ID
- Exchange order ID
- Signal timestamp
- Side
- State transition
- Disable or block reason

## Backup Policy

Backups are mode-specific and must not be shared between paper and live.

Backup priority:

1. Live DB
2. Redacted env snapshot
3. Reconcile, audit, and error evidence
4. Strategy and validation reports
5. Raw market cache

Retention guidance:

- DB snapshots: daily rotation with limited history
- Logs: hot retention followed by archive movement
- Raw market cache: prune when safe
- Live trades, fills, and balances: retain for recovery and audit needs

## Path Rules

All path resolution must go through the shared path layer:

- `PathManager`
- `PathConfig`
- `src/bithumb_bot/paths.py`

Path locations are configuration. Path structure rules are code.

Managed root location rules:

- `ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, and `BACKUP_ROOT` are separate managed roots.
- `ARCHIVE_ROOT` is a separate managed root when archive storage is enabled.
- Bucket classification stays the same regardless of where the managed roots are mounted.
- In `MODE=paper`, unset `ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, and `BACKUP_ROOT` fall back under `XDG_STATE_HOME/bithumb-bot` or `~/.local/state/bithumb-bot`.
- In both modes, unset `ARCHIVE_ROOT` falls back to the default runtime root's `archive/` subtree.
- Live managed roots must be absolute paths.
- Live managed roots must be repository-external.
- Live managed roots must not overlap.
- Live managed roots must not have parent/child relationships with one another.
- Managed roots themselves must stay mode-neutral; mode scoping happens below the root in paths such as `RUN_ROOT/<mode>/...` and `DATA_ROOT/<mode>/...`.

Live mode requirements:

- Use absolute paths only
- Use repository-external paths only
- Fail fast on repo-internal paths
- Fail fast on paths containing the wrong environment segment such as `paper`

## Compatibility Overrides

The following env vars are the current documented compatibility override surfaces:

- `DB_PATH`
- `RUN_LOCK_PATH`
- `BACKUP_DIR`
- `SNAPSHOT_ROOT`

These documented compatibility overrides must remain compatible with the storage contract:

- They must resolve to the correct mode-specific bucket.
- They must be absolute paths.
- Live mode must keep them outside the repository.
- Live mode must not allow a paper-scoped path.

## Runtime Separation

### paper

- Paper is for validation and simulation.
- Paper storage must remain isolated from live storage.

### live

- Live is for real orders and recovery-critical evidence.
- Live requires explicit preflight checks, notifier readiness, live arming requirements, and mode-correct paths.
- Live failures must fail fast rather than continue ambiguously.

## Operator Evidence

Keep operator evidence separate from general logs.

Examples:

- Recovery reports
- Ops reports
- Incident snapshots
- Validation summaries
- Backup verification outputs

These outputs are diagnostic and recovery-critical, not disposable logs.

## Lot State Routing Rule

`open_position_lots` is the persisted storage contract for the base lot-native
position rows.

Execution and reporting do not treat a single stored row as the whole SELL
authority surface. They materialize normalized authority from the persisted
lot-state rows together with reservation and dust interpretation.

Persisted schema-backed fields:

- `qty_open`: stored row quantity for the current lot-state row.
- `executable_lot_count`: stored executable lot authority for an `open_exposure` row.
- `dust_tracking_lot_count`: stored residual lot authority for a `dust_tracking` row.
- `position_state`: stored lot-state classification. Current values are `open_exposure` and `dust_tracking`.
- `position_semantic_basis`: stored semantic basis and must remain `lot-native`.
- `lot_semantic_version`, `internal_lot_size`, `lot_min_qty`, `lot_qty_step`, `lot_min_notional_krw`, `lot_max_qty_decimals`, and `lot_rule_source_mode`: stored lot-rule metadata used during interpretation, recovery, and reporting.

Trigger-enforced storage invariants:

- `open_position_lots` is a trigger-protected storage contract, not only a reporting or interpretation convention.
- Negative `executable_lot_count` and `dust_tracking_lot_count` values are rejected.
- For lot-native rows with positive `qty_open`, `position_state='open_exposure'` requires positive `executable_lot_count` and zero `dust_tracking_lot_count`, while `position_state='dust_tracking'` requires zero `executable_lot_count` and positive `dust_tracking_lot_count`.
- For lot-native rows with positive `qty_open` and positive `internal_lot_size`, `qty_open` must match the active lot-count authority multiplied by `internal_lot_size`.
- Zero-qty rows must not retain lot authority; both lot-count columns must be zero when `qty_open <= 1e-12`.

Current schema-time legacy-row normalization:

- During schema setup, rows with missing or blank `position_semantic_basis` are backfilled to `lot-native`.
- Rows with missing or blank `position_state` are backfilled to `open_exposure`.
- Existing positive-qty rows are normalized so `open_exposure` rows have at least one `executable_lot_count` and `dust_tracking` rows have at least one `dust_tracking_lot_count` when legacy lot-count fields were blank or non-positive.
- This backfill keeps older rows compatible with the current storage contract without changing the normalized SELL authority boundary.

Derived / interpreted outputs from the lot-state and dust interpretation layer:

- `open_lot_count`: interpreted executable exposure count derived from the stored lot-state row.
- `raw_total_asset_qty`: interpreted broker-visible total remainder for the asset. It is a reconciliation and reporting value, not a stored `open_position_lots` column.
- `open_exposure_qty`: interpreted executable quantity materialized from lot-native open exposure for broker payloads and compatibility.
- `dust_tracking_qty`: interpreted operator-only residual quantity materialized from the dust-tracking lot state.
- `sellable_executable_qty`: interpreted sell-submit quantity derived from sellable executable lot count, not a persisted column.
- `sellable_executable_lot_count`, `reserved_exit_lot_count`, `exit_allowed`, and `exit_block_reason`: normalized execution/reporting authority fields materialized from persisted lot rows plus reservation and dust interpretation; they are not direct `open_position_lots` storage columns.
- `dust_operability_state` and `dust_operability_reason`: normalized tradeability projection fields. They explain whether preserved `dust_tracking` evidence is sub-minimum non-executable residue that may be treated as flat for new-entry policy, or whether operator review is still required.
- Storage-level checks and schema normalization are necessary inputs to that materialization, but they are not by themselves proof of final runtime SELL authority.

Practical routing rules:

- BUY fills create or refresh `open_exposure` lots.
- SELL matching consumes `open_exposure` lots only.
- `dust_tracking` lots are not sellable inventory and must not be counted as the basis for a normal SELL order.
- Harmless dust suppression is defined around the normalized dust operability projection, not around a stored row alone.
- A dust-only, sub-minimum, zero-executable state may be treated as flat for new-entry policy after recovery has otherwise converged. This preserves accounting evidence while allowing the experiment to continue.
- A `dust_tracking` row at or above the stored minimum quantity, or one whose minimum boundary is unavailable, remains non-operable and requires operator review.
- Suppression behavior must avoid creating a normal SELL order, SELL event, or fresh client order ID for dust-only exits unless an operator explicitly clears the dust state.
- Reporting should surface the interpreted fields `open_lot_count`, `open_exposure_qty`, `dust_tracking_qty`, `sellable_executable_qty`, and `raw_total_asset_qty` together with the persisted lot-state fields so operators can explain the gap between broker-visible holdings and the sellable position base.
- Boundary rule: `qty_open < min_qty` may be reclassified to `dust_tracking`; `qty_open == min_qty` stays `open_exposure`.
- If a malformed `dust_tracking` lot appears above `min_qty`, it is still treated as operator evidence and remains excluded from normal SELL submission until an operator clears the inconsistency.
- Routing summary:
  - BUY creates or refreshes `open_exposure` lots.
- SELL lifecycle and real-order submission use normalized lot-native authority materialized from the persisted lot-state base contract, with `open_exposure_qty` materialized for the final broker payload.
  - `dust_tracking_qty` is operator-tracking evidence only and is excluded from normal SELL submission.
  - Harmless dust suppression is anchored to the `dust_tracking` path, not the `open_exposure` path.
