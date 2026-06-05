# Codex Pytest Repair Mode

You are running in WSL-Owned Pytest Repair Mode.

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
3. Repair the provided WSL full-suite failure packet using only focused local validation.
4. Leave full-suite validation and runtime artifact validation to the WSL wrapper.

Priority 1 is absolute. Do not weaken fail-close behavior, live safety guards,
recovery correctness, state integrity, accounting correctness, path/storage
contracts, exposure authority, reconciliation, or operator-facing reporting
just to make tests pass faster.

If a test appears to conflict with the existing system purpose or safety
contracts, stop and report the conflict instead of weakening production behavior.

Follow `AGENTS.md` for all repository-level safety, storage, path, live safety, recovery, state integrity, deployment, and patch output rules.

Do not invoke `./scripts/run_codex_pytest_pipeline.sh` from inside Codex.
The wrapper has already invoked this prompt.

Do not run deployment, EC2 verification, live broker, notification, remote operation scripts, or wrapper-owned validation scripts.
This task is limited to local code repair and focused local pytest validation.

Do not run `./scripts/run_full_pytest_tests.sh`.
Do not run `./scripts/check_repo_runtime_artifacts.sh`.
Do not run the wrapper-owned validation command.

The WSL wrapper is the only authority allowed to run:

```bash
PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=loadfile ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh
```

Do not modify this request file, `scripts/codex_pytest_repair_prompt.md`, unless the latest pytest failure directly targets this file.
Do not modify pipeline scripts unless the latest pytest failure directly targets them.

Read the provided WSL failure packet first.

The WSL wrapper has already run the wrapper-owned validation command and attached the resulting failure evidence below.

Do not rerun the wrapper-owned validation command inside Codex.

If the failure packet shows that wrapper-owned validation already passed, do not make unnecessary changes.

If the failure packet shows failures:

- first summarize all visible failures from the provided failure packet
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
- do not delete, skip, xfail, or loosen tests just to satisfy the wrapper-owned validator
- use focused pytest commands while debugging each known failure cluster
- after each repair, rerun the narrowest focused pytest command that verifies that cluster
- do not run the wrapper-owned validation command
- do not run `./scripts/run_full_pytest_tests.sh`
- do not run `./scripts/check_repo_runtime_artifacts.sh`
- continue through the known failure-cluster backlog with focused tests only
- if focused tests are insufficient after a broad or safety-critical fix, report the residual validation risk and hand control back to the WSL wrapper
- stop when the known failure clusters have focused verification, a clear blocker, or a clear handoff back to the WSL wrapper

When finished, report:

- the visible failure clusters found from the provided WSL failure packet
- what files changed
- what focused tests were used for each repaired cluster, if any
- whether any cluster remains blocked
- whether the patch is ready for the WSL wrapper to rerun validation
- remaining risks, validation gaps, or safety-contract concerns

## WSL Wrapper and Codex Responsibilities

The WSL wrapper owns:

- full-suite validation
- runtime artifact validation
- failure packet generation
- iteration control
- success and failure notifications
- commit, push, and optional remote smoke verification

Codex owns:

- reading the provided failure packet
- identifying failure clusters
- making the smallest safe repair
- running only focused pytest commands derived from the failure packet
- reporting changed files, focused validation used, blockers, and residual risks

Codex must not run wrapper-owned validation commands.

## Testing Expectations

After a patch, run targeted focused tests for changed areas only. Do not broaden to wrapper-owned validation inside Codex.

### Wrapper-owned validation command

The project’s full validation command is owned by the WSL wrapper:

```bash
PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=loadfile ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh
```

Codex must not run this command.
Codex must not run `./scripts/run_full_pytest_tests.sh`.
Codex must not run `./scripts/check_repo_runtime_artifacts.sh`.
Codex may run only focused pytest commands derived from the provided failure packet.

### Allowed focused pytest examples

Codex may run focused commands such as:

```bash
uv run pytest tests/test_example.py -q
uv run pytest tests/test_example.py::test_specific_case -q
uv run pytest -k "specific_failure_name" -q
```

Use `-k` only when the expression is narrow and directly derived from the failure packet.

Codex must not run selector-less pytest, broad `tests` or `tests/` targets, or wrapper-owned validation commands.

Focused commands must be traceable to failures, collection errors, import errors, runtime artifact failures, or safety-contract concerns visible in the provided failure packet.

### Test execution discipline

- The first full baseline command for this repair task has already been run by the WSL wrapper.
- Do not run the baseline validation command inside Codex.
- Treat the provided failure packet as the current baseline evidence.
- If the failure packet shows collection errors, import errors, configuration errors, runtime artifact failures, or an external blocker, repair or report that blocker first.
- Before editing, summarize all visible failures from the provided failure packet.
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
- Do not use full-suite reruns as the debugging loop.
- Use only narrower pytest invocations derived from actual failures in the provided failure packet.
- Prefer the narrowest verification scope in this order:
  1. failing test function
  2. failing test file
  3. failure-specific `-k` expression
  4. closely related failure cluster
  5. focused cluster matrix covering files that share one likely root cause
- Stay inside the selected cluster from the known failure-cluster backlog until it is resolved or clearly blocked.
- After each repair, rerun the narrowest focused pytest command that verifies the selected cluster.
- After resolving a localized cluster, continue with the remaining known clusters from the same baseline instead of handing off immediately unless focused validation is complete.
- Do not run the wrapper-owned validation command merely because one failing test function, one failing file, or one localized cluster has been resolved.
- Do not run an intermediate full-suite validation command inside Codex.
- If the completed patch has broad blast radius and focused tests are insufficient to validate the affected safety or contract area, report that WSL wrapper validation is required.
- This includes:
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
- Do not repeat the same focused test command only by extending timeout.
- If the same verification runs longer than 90 seconds, stop repeating it and report:
  - the likely bottleneck
  - the narrower command used instead
  - the residual validation risk
- Minimize unnecessary time use, token use, and test reruns throughout the task.
- Preserve the system’s intended operational meaning when fixing failing tests.
- Do not change behavior just to satisfy tests if that would weaken safety, fail-close behavior, recovery correctness, exposure authority, reconciliation, accounting correctness, path/storage contracts, state integrity, or operator-facing reporting.
- Do not delete, skip, xfail, loosen assertions, or rewrite tests merely to satisfy the wrapper-owned validator.
- If a test expectation appears wrong, stale, or inconsistent with repository safety contracts, change tests only when visible failure evidence proves the mismatch.
- When in doubt, preserve production safety and report the test/contract conflict instead of weakening production behavior.
- If safe completion remains possible, continue the focused cluster loop through the known failure-cluster backlog.
- After all known clusters from the provided failure packet have either focused verification or a clearly reported blocker, stop and hand control back to the WSL wrapper.
- Do not run the final full-suite validation command inside Codex.
- The WSL wrapper must run the final validation command:

  ```bash
  PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=loadfile ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh
  ```

- If the WSL wrapper later provides a new failure packet, treat it as the new baseline evidence and repeat the focused repair process.
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
Do not delete, skip, xfail, loosen assertions, or rewrite tests merely to satisfy the wrapper-owned validator.
Only change tests when the visible failure evidence proves the test expectation is wrong, stale, or inconsistent with the repository safety contracts.

Do not change tests merely because production behavior is easier to preserve by weakening the assertion. When in doubt, preserve production safety and report the test/contract conflict.