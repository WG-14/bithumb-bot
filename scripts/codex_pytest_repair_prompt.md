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

## Priority Order

This task has a strict priority order:

1. Preserve the existing system purpose, operational intent, and repository safety contracts.
2. Minimize wasted time, tokens, and redundant test execution.
3. Achieve a clean final pass with `uv run pytest -q`.

Priority 1 is absolute. Do not weaken fail-close behavior, live safety guards,
recovery correctness, state integrity, accounting correctness, path/storage
contracts, exposure authority, reconciliation, or operator-facing reporting
just to make tests pass faster.

If a test appears to conflict with the existing system purpose or safety
contracts, stop and report the conflict instead of weakening production behavior.

Follow `AGENTS.md` for all repository-level safety, storage, path, live safety, recovery, state integrity, deployment, and patch output rules.

Do not invoke `./scripts/run_codex_pytest_pipeline.sh` from inside Codex.
The wrapper has already invoked this prompt.

Do not run deployment, EC2 verification, live broker, notification, or remote operation scripts.
This task is limited to local pytest repair and local pytest validation.

Do not modify this request file, `scripts/codex_pytest_repair_prompt.md`, unless the latest pytest failure directly targets this file.
Do not modify pipeline scripts unless the latest pytest failure directly targets them.

Run the full suite first:

```bash
uv run pytest -q
```

If it passes, do not make unnecessary changes.

If it fails:

- first summarize all visible failures from the completed full-suite output
- group visible failures into repair clusters by likely shared cause
- identify whether each cluster is:
  - safety-contract related
  - shared/cross-cutting
  - localized
  - test-only expectation drift
  - externally blocked
- do not classify a cluster as test-only expectation drift until the existing
  production behavior and safety contract have been inspected and the mismatch
  is supported by visible failure evidence
- choose the next repair cluster using this priority:
  1. clusters that risk the existing system purpose or safety contracts
  2. clusters with a shared root cause affecting multiple failures
  3. localized clusters
- preserve the existing system behavior, operational intent, and repository safety contracts
- do not implement unrelated feature work
- do not perform broad cleanup or refactoring
- do not delete, skip, xfail, or loosen tests just to pass the suite
- use focused pytest commands while debugging each known failure cluster
- after each repair, rerun the narrowest focused pytest command that verifies that cluster
- do not rerun `uv run pytest -q` after each localized cluster while other known clusters from the same baseline remain
- continue through the known failure-cluster backlog with focused tests
- rerun `uv run pytest -q` only:
  - after all known clusters from the latest full-suite output have either focused verification or a clearly reported blocker
  - after a shared/cross-cutting or safety-critical fix when focused tests are insufficient
  - as the final validation command
- repeat the focused cluster loop until `uv run pytest -q` passes cleanly or a clear external blocker is reported

When finished, report:

- whether the final `uv run pytest -q` passed
- the visible failure clusters found from the baseline and any later full-suite runs
- what files changed
- what focused tests were used for each repaired cluster, if any
- whether any intermediate full-suite rerun was used and why
- remaining risks or blockers

## Testing Expectations

After a patch, run targeted tests for changed areas first, then broaden only when the change affects shared behavior or safety-critical contracts.

### Standard test command

```bash
uv run pytest -q
```

This is the project’s intended full-suite validation command.

### Test execution discipline

- The first full baseline command for this repair task must be:

  ```bash
  uv run pytest -q
  ```

- Let the baseline full suite complete whenever pytest can continue naturally.
- Do not use `-x`, `--maxfail=1`, or other fail-fast options for the baseline full-suite run.
- If pytest stops early because of collection errors, import errors, configuration errors, or an external blocker, treat the visible output as the current baseline and repair that blocker first.
- Before editing, summarize all visible failures from the latest full-suite output.
- Group visible failures into a repair-cluster backlog by likely shared cause.
- For each cluster, identify whether it is:
  - safety-contract related
  - shared/cross-cutting
  - localized
  - test-only expectation drift
  - externally blocked
- Do not classify a cluster as test-only expectation drift until the existing
  production behavior and safety contract have been inspected and the mismatch
  is supported by visible failure evidence.
- Choose the next repair cluster using this priority:
  1. clusters that risk the existing system purpose or safety contracts
  2. clusters with a shared root cause affecting multiple failures
  3. localized clusters
- After the baseline, do not use full-suite reruns as the default debugging loop.
- Use only narrower pytest invocations derived from actual failures from the latest full-suite output.
- Prefer the narrowest verification scope in this order:
  1. failing test function
  2. failing test file
  3. failure-specific `-k` expression
  4. closely related failure cluster
  5. focused cluster matrix covering files that share one likely root cause
- Stay inside the selected cluster from the known failure-cluster backlog until it is resolved or clearly blocked.
- After each repair, rerun the narrowest focused pytest command that verifies the selected cluster.
- After resolving a localized cluster, continue with the remaining known clusters from the same baseline instead of immediately rerunning the full suite.
- Do not rerun `uv run pytest -q` merely because one failing test function, one failing file, or one localized cluster has been resolved.
- Run an intermediate `uv run pytest -q` only when the completed patch has broad blast radius and focused tests are insufficient to validate the affected safety or contract area, such as:
  - shared production helper changes
  - shared test helper changes
  - import, packaging, configuration, or path resolution changes
  - storage path contract changes
  - live safety, broker guard, or preflight behavior changes
  - recovery, reconciliation, accounting, or state integrity changes
  - behavior used by multiple unrelated failing files
- Even for broad fixes, prefer the smallest focused cluster matrix first when it can validate the affected safety or contract area.
- Do not broaden scope without a concrete reason.
- Do not repeat the same command without a new hypothesis or a code change.
- Do not repeat the same full test command only by extending timeout.
- If the same verification runs longer than 90 seconds, stop repeating it and report:
  - the likely bottleneck
  - the narrower command used instead
  - the residual validation risk
- Minimize unnecessary time use, token use, and test reruns throughout the task.
- Preserve the system’s intended operational meaning when fixing failing tests.
- Do not change behavior just to satisfy tests if that would weaken safety, fail-close behavior, recovery correctness, exposure authority, reconciliation, accounting correctness, path/storage contracts, state integrity, or operator-facing reporting.
- Do not delete, skip, xfail, loosen assertions, or rewrite tests merely to make `uv run pytest -q` pass.
- If a test expectation appears wrong, stale, or inconsistent with repository safety contracts, change tests only when visible failure evidence proves the mismatch.
- When in doubt, preserve production safety and report the test/contract conflict instead of weakening production behavior.
- If safe completion remains possible, continue the focused cluster loop through the known failure-cluster backlog.
- After all known clusters from the latest full-suite output have either focused verification or a clearly reported blocker, run the final validation command only if safe completion remains possible:

  ```bash
  uv run pytest -q
  ```

- The final validation command for this repair task must be `uv run pytest -q`.
- If the final full suite passes, stop and report the result.
- If the final full suite fails, summarize the new visible failures, update the repair-cluster backlog, and return to the focused cluster repair loop.
- If further repair would require weakening the existing system purpose, safety contracts, or production behavior, stop and report the blocker instead of forcing a test pass.

### Relevant focused tests

When multiple visible failures point to the same safety or contract area, run the
smallest focused cluster matrix that covers the shared contract before changing
broader code.

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
Do not delete, skip, xfail, loosen assertions, or rewrite tests merely to make `uv run pytest -q` pass.
Only change tests when the visible failure evidence proves the test expectation is wrong, stale, or inconsistent with the repository safety contracts.

Do not change tests merely because production behavior is easier to preserve by weakening the assertion. When in doubt, preserve production safety and report the test/contract conflict.