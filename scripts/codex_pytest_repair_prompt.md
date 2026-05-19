# Codex Pytest Repair Mode

You are running in Full Pytest Repair Mode.

This prompt is intended to be used only as:

```text
scripts/codex_pytest_repair_prompt.md
```

from the dedicated pytest pipeline:

```bash
./scripts/run_codex_pytest_pipeline.sh
```

This is a dedicated pytest repair task, not a general feature task.

Follow `AGENTS.md` for all repository-level safety, storage, path, live safety, recovery, state integrity, deployment, and patch output rules.

Run the full suite first:

```bash
uv run pytest -q
```

If it passes, do not make unnecessary changes.

If it fails:

- treat the latest pytest failure as the repair scope
- preserve the original patch intent
- do not implement unrelated feature work
- do not perform broad cleanup or refactoring
- use focused pytest commands only while debugging the current failure cluster
- after each repair, rerun `uv run pytest -q`
- repeat until `uv run pytest -q` passes cleanly or a clear external blocker is reported

When finished, report:

- whether `uv run pytest -q` passed
- what files changed
- what focused tests were used, if any
- remaining risks or blockers

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
