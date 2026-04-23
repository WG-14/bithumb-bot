# AGENTS.md

## Purpose

This repository is a safety-first Bithumb trading bot project.
Optimize every change for:

1. prevention of wrong orders
2. prevention of duplicate orders and state corruption
3. restart recovery and ledger consistency
4. loss limits and emergency stop behavior
5. operational observability, alerts, and recoverability

Profitability is always secondary to execution safety and state integrity.
Treat this file as a binding repository-level operating contract.

## Environment Contract

- Production reference environment is AWS Linux.
- Linux runtime behavior is the source of truth for paths, shell commands, locking, process behavior, and operational validation.
- On Windows developer machines, the preferred local workspace is VS Code opened in WSL against a WSL-hosted repository.
- Native Windows execution may be used for convenience, but it is not the reference environment for runtime or deployment correctness.

## Mandatory Reading

Before making any patch, read and follow:

- `docs/storage-layout.md`
- `docs/runtime-data-policy.md`
- `README.md`

If the change touches path handling, runtime outputs, backups, logs, DB paths, or env loading, also inspect:

- `src/bithumb_bot/paths.py`
- `src/bithumb_bot/storage_io.py`
- `src/bithumb_bot/run_lock.py`

If the change touches live execution, restart recovery, order lifecycle, or operator flows, inspect the relevant docs and tests before changing code.

## Task Prompt Rules

- Do not repeat at length in task prompts the repository-wide rules already defined in AGENTS.md.
- Task prompts should contain only task-specific scope, execution steps, validation, and reporting.
- Prefer direct, execution-oriented, sequential instructions over explanatory wording.
- When a prompt would restate a repository rule, refer to AGENTS.md instead.
- Preserve the system’s intended operational meaning when following task prompts.
- Minimize unnecessary time use, token use, and avoid wasteful reruns.

## Change Planning

If a change touches multiple safety-critical axes at once, do not implement immediately.
Safety-critical axes include path/storage, live safety, recovery/state integrity, run lock, accounting, env loading, and deployment scripts.

First write a short execution plan that states:

- change order
- what can be grouped
- what must be split
- relevant focused tests
- whether a final full-suite run is needed

If a safe single-batch patch is inappropriate, split it into the minimum safe set of batches. Do not force unrelated safety work into one patch.

## Project Priorities

Use this priority order for tradeoffs:

### P0 - must not regress

- wrong-order prevention
- duplicate-order prevention
- ledger and state consistency
- crash and restart recoverability
- live and paper separation
- safe live preflight and fail-fast checks
- emergency stop and halt safety
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

Never accept a P2 improvement that weakens P0 or P1 safety properties.

## Storage, Path, and Runtime Roots

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

All path creation and path resolution must go through the shared path layer:

- `PathManager`
- `PathConfig`
- the path helpers in `src/bithumb_bot/paths.py`

Do not:

- hardcode absolute runtime paths in code
- concatenate path strings manually for runtime artifacts
- introduce new direct path conventions outside the central path layer
- bypass PathManager for DB, logs, run lock, reports, raw or derived artifacts, backups, or snapshots

Path locations are configuration. Path structure rules are code.

Before adding any new file output, classify it explicitly into one bucket:

- `env/`
- `run/`
- `data/<mode>/raw/`
- `data/<mode>/derived/`
- `data/<mode>/trades/`
- `data/<mode>/reports/`
- `logs/<mode>/...`
- `backup/<mode>/...`
- `archive/<mode>/...`

Examples:

- external API raw payload archive -> `raw`
- feature snapshot, signal trace, or tuning intermediate -> `derived`
- orders, fills, balances, or reconcile evidence -> `trades`
- operator-readable summary JSON or report -> `reports`
- runtime lock, pid, heartbeat, or transient state pointer -> `run`
- DB snapshot, redacted config snapshot, or recovery snapshot -> `backup`

## Environment Separation

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

If a dry-run mode exists, it must not share paper or live storage.

For `MODE=live`:

- require repository-external absolute paths
- fail fast on relative paths
- fail fast on repo-internal paths
- fail fast on paths containing the wrong environment segment such as `paper`

Do not weaken these guards.

## File Format and Mutation Rules

Prefer SQLite for:

- stateful ledgers
- restart recovery state
- portfolio, order, fill, and trade lifecycle state
- bot health tables
- other core recovery-critical tables

Prefer JSONL append-only for:

- order request and response events
- fill events
- balance snapshots
- reconcile summaries
- strategy decision evidence
- raw external response snapshots

Never overwrite in place for live evidence.
Use append-only or timestamped snapshot patterns for:

- live order and fill evidence
- live balance snapshots
- audit evidence
- incident evidence
- strategy decision evidence tied to live actions

For non-append file writes, use existing atomic write helpers and preserve the durability patterns already used in the repo.

## Logging and Observability

Maintain the existing logging separation model.

Valid log kinds are:

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
Preserve identifiers useful for correlation when practical, such as:

- client order id
- exchange order id
- signal timestamp
- side
- state transition
- disable or block reason

## Live Safety

Treat live-mode behavior as safety-critical.

Do not remove or weaken:

- live preflight checks
- safe default enforcement
- notifier requirements
- arming requirements for real orders
- slippage and spread protections
- max order, max daily loss, and max daily order count guards
- kill switch and halt protections
- reconciliation on restart
- operator intervention requirements when consistency is unclear

Real-order flows must remain explicitly armed.
Dry-run and real-order behavior must remain clearly separated.
Prefer fail-fast over ambiguous execution.

## Single-Instance and Run Lock

This bot must not allow unsafe concurrent run loops.

Do not remove or weaken:

- run lock acquisition
- stale lock diagnostics
- mode-specific run lock separation
- Linux or WSL-only enforcement where required by locking semantics

New run-related code must respect the existing lock model and mode-specific run directories.

## Recovery and State Integrity

State consistency is more important than convenience.

Any patch touching execution, order submission, fills, reconciliation, restart behavior, or DB writes must preserve:

- durable local intent recording
- correct order state transitions
- fill deduplication
- no overfill corruption
- no negative-balance corruption
- restart reconciliation behavior
- explicit handling of unknown or unrecoverable states

Do not simplify code in ways that remove recovery evidence or reduce the ability to explain what happened after a crash.

When uncertain, choose explicit stop, halt, or recovery-required behavior over silent continuation.

### Lot-Native State Semantics

Canonical executable position semantics must remain lot-native.

- Treat executable exposure as lot-based state, not arbitrary qty-first state.
- Keep dust or sub-lot remainder explicitly separated from executable exposure.
- Recovery, lifecycle, and risk logic must preserve the same lot-native meaning used by live execution.
- Qty values may be used for broker API interfacing, reporting, and compatibility, but they must not replace lot-native canonical meaning.
- Do not reintroduce qty-native executable state in new patches.

### Lot-Native Batch Contract

Practical live-operation target PASS is already baseline for the current lot-native work, but it is not the finish line for this batch.

- The direct goal of this batch is full lot-native declaration completion.
- Current contract PASS is only the starting point for that work.
- The remaining `decision_context` compatibility fallback / provenance must be removed.
- The remaining `reporting` truth-source / provenance primary layer must be removed.
- Improvements outside this goal are prohibited.

- When modifying lot-native-related logic, do not restore semantic authority to qty.
- Do not reopen SELL boundary authority work unless a contract test proves regression.
- At the SELL boundary, the canonical sellable lot count is the final authority.
- `dust_only_remainder`, `boundary_below_min`, and `no_executable_exit_lot` remain suppression outcomes, not submit failures.
- Do not mix `open_exposure` and `dust_tracking` into executable inventory.
- Do not make changes that break PASS under `docs/lot_native_contract.md`.
- Do not describe current contract PASS and full declaration PASS as the same milestone.
- Treat `decision_context` compatibility fallback / provenance residue as a direct closure target, not a later problem.
- Treat `reporting` truth-source / provenance residue as a direct closure target, not a later problem.
- Legacy or qty-first fallback in recovery, `decision_context`, and `reporting` must be treated as residue to classify or eliminate as semantic authority.
- Compatibility fields may remain only if they are clearly derived and non-authoritative.
- When touching `decision_context`, `reporting`, recovery, or lifecycle, also verify:
  - SELL authority still remains at canonical sellable lot count
  - a qty-only legacy row does not regain executable authority
  - `legacy_lot_metadata_missing` is not reintroduced into lifecycle semantic state
  - no legacy compatibility fallback authority or provenance remains in `decision_context`
  - no truth-source or provenance primary layer remains in `reporting`
- During implementation and review, ask:
  - Does this change merely preserve current PASS, or does it remove the reasons full declaration is still FAIL?
  - Does any legacy compatibility authority or provenance remain in `decision_context`?
  - Does any truth-source or provenance primary layer remain in `reporting`?
- Use document -> tests -> implementation order.
- Keep the order document, test contract, and implementation aligned, and do not expand beyond lot-native declaration scope.

## Strategy and Research

Strategies are replaceable rules.
The live and runtime infrastructure is the stable socket they plug into.

When changing strategy code:

- keep strategy outputs inspectable
- keep validation and reporting paths separated from the core trade ledger
- avoid coupling strategy experiments directly to live execution state
- preserve the ability to compare strategies through env and config selection

Strategy experimentation must not weaken operational safety.

## Env Loading and Configuration

Respect the repository's explicit env-loading model.

Do not reintroduce implicit `.env` autoloading.
Keep explicit env selection behavior consistent with the bootstrap and config flow.

Prefer configuration injection over hardcoded behavior.
Backward-compatible overrides may exist, but do not expand override sprawl unnecessarily.

If introducing new env vars:

- document them in `.env.example` when appropriate
- keep naming consistent
- define whether they are paper-safe, live-required, or optional
- preserve fail-fast behavior for unsafe live omissions

## Deployment and Script Rules

Repository scripts and deployment helpers must also obey the same storage contract.

Shell scripts, systemd units, healthcheck helpers, backup scripts, and runtime inspection tools must not invent their own path scheme.
They should resolve managed paths through the canonical path interface wherever possible.

Do not patch only Python while leaving scripts inconsistent with the path policy.

## Testing Expectations

After a patch, run targeted tests for changed areas and enough broader tests to catch contract regressions.

### Standard test command

```bash
uv run pytest -q
```

This is the project’s intended full-suite validation command.

### Test execution discipline

- `uv run pytest -q` must be treated as the final validation command.
- Run `uv run pytest -q` only after all requested patches are complete.
- The first full baseline command for a task that requires full validation must be `uv run pytest -q`.
- The final validation command for a task that requires full validation must be `uv run pytest -q`.
- During debugging, do not use full-suite reruns as the default loop.
- Use only narrower pytest invocations derived from actual failures from the most recent full run.
- Prefer the narrowest verification scope in this order:
  1. failing test function
  2. failing test file
  3. failure-specific `-k` expression
  4. closely related failure cluster
- Stay inside the current failure cluster until it is resolved or clearly blocked.
- Do not broaden scope without a concrete reason.
- Do not repeat the same command without a new hypothesis or a code change.
- Do not repeat the same full test command only by extending timeout.
- If the same verification runs longer than 90 seconds, stop repeating it and report the likely bottleneck, alternative validation commands, and residual risk.
- Minimize unnecessary time use, token use, and test reruns throughout the task.
- Preserve the system’s intended operational meaning when fixing failing tests.
- Do not change behavior just to satisfy tests if that would weaken safety, fail-close behavior, recovery correctness, exposure authority, reconciliation, or operator-facing reporting.
- If full completion remains possible, continue iterating with targeted tests and narrow fixes until the full suite reaches a clean pass under `uv run pytest -q`.
- Codex should continue the test-fix loop until `uv run pytest -q` passes cleanly, or until an external blocker makes further safe progress impossible.
- After resolving any of the following, rerun `uv run pytest -q`:
  - a full failing file
  - a shared helper used by multiple failing tests
  - an import, configuration, or path issue
  - a cross-cutting failure cluster
- Run the full suite only when it is actually needed, and only at the baseline and final validation points unless a shared failure cluster resolution justifies another full rerun.
- Localized changes such as small interface adjustments, logging improvements, report or output improvements, helper CLI additions or changes, and healthcheck-only changes may be validated with focused tests only if there is no broader regression risk. In those cases, the full suite may be skipped unless the task explicitly requires a clean pass under `uv run pytest -q`.

### Relevant focused tests

When touching these areas, run the relevant focused tests first:

- paths and storage contract
  - `tests/test_paths.py`
  - `tests/test_path_config_integration.py`
  - `tests/test_paths_cli.py`
  - `tests/test_db_path_resolution.py`
  - `tests/test_storage_io.py`
- live mode, preflight, and live broker guards
  - `tests/test_live_preflight.py`
  - `tests/test_live_broker.py`
  - `tests/test_config_live_db_path_guard.py`
  - `tests/test_mode_validation.py`
  - `tests/test_order_rules_sync.py`
- recovery, restart, and accounting integrity
  - `tests/test_fill_dedupe.py`
  - `tests/test_ledger_atomicity.py`
  - `tests/test_accounting_safety.py`
  - `tests/test_recovery_restart_regression.py`
  - `tests/test_recovery_recent_activity_interpretation.py`
  - `tests/test_trade_lifecycle.py`
- run lock, ops, and observability
  - `tests/test_run_lock.py`
  - `tests/test_health_persistence.py`
  - `tests/test_operator_commands.py`
  - `tests/test_ops_report.py`
  - `tests/test_backup_sqlite_script.py`
  - `tests/test_sqlite_restore_verify_tool.py`

If you change behavior, update or add tests.
Do not ship behavior changes without test coverage when the area is safety-critical.

## Patch Output Requirements

For every non-trivial patch, provide a concise summary that includes:

1. what changed
2. why it changed
3. which storage, path, and runtime contract rules were relevant
4. whether any new artifact or output was introduced and how it was classified
5. what tests were run
6. any remaining risks or follow-up items

Also include:

- changed files
- commands executed
- concise test result summary
- any blocked or deferred items
- any AWS deployment checks that still require human confirmation

If the full suite is skipped, explicitly state:

- why it was skipped
- which focused tests were run
- the residual risks

If a new runtime artifact is added, explicitly state:

- classification
- mode separation behavior
- retention, append, or snapshot semantics
- whether it is recovery-critical or diagnostic-only

## Change-Size Discipline

Prefer small, reviewable patches.

Avoid mixing these in one patch unless necessary:

- path and storage refactors
- live execution logic changes
- strategy logic changes
- deployment or script changes
- observability or reporting changes

## Backward Compatibility Discipline

Keep compatibility shims unless you have verified they are truly unused and the docs and tests are updated.

## Line Ending and Formatting Rules

Do not introduce unnecessary formatting churn.
Respect `.gitattributes`.
Do not change EOL policy for `.sh`, `.py`, `.md`, `.yml`, or `.yaml`.

## Things to Avoid

Do not:

- write runtime files into the repo
- hardcode runtime paths
- weaken live preflight
- weaken notifier or live arming checks
- merge paper and live storage
- bypass central path helpers
- replace append-only evidence with overwrite behavior
- remove recovery evidence
- log secrets
- make broad formatting-only changes during safety patches
- trade safety for convenience
- stop after partial debugging progress if a clean full-suite pass is still safely achievable
- use repeated full-suite reruns as a substitute for targeted diagnosis

## In Case of Ambiguity

When requirements conflict, choose the safer interpretation.

Default preference order:

1. protect funds
2. protect state integrity
3. preserve recovery evidence
4. preserve operator visibility
5. preserve convenience
6. improve profitability

If a requested change appears to conflict with `docs/storage-layout.md` or `docs/runtime-data-policy.md`, follow the docs and keep the storage contract intact unless those docs are explicitly updated as part of the same task.
