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

## Purpose

Repair the provided WSL full-suite failure packet using the smallest safe patch
and focused local validation only.

Preserve the existing system purpose, operational intent, repository safety
contracts, wrapper/Codex responsibility boundary, and pytest repair constraints.

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

## Required Startup Sequence

Before editing any file during pytest repair mode, Codex must:

1. Read `AGENTS.md`.
2. Read any nested `AGENTS.md` that applies to files Codex may touch.
3. Read the WSL failure packet in the required packet-reading order.
4. Summarize all visible failures before making code or test changes.
5. Identify repair clusters and choose the first repair cluster according to the repair priority rules.

If `AGENTS.md` or a nested `AGENTS.md` conflicts with this prompt, Codex must
follow the stricter safety, storage, path, live-safety, recovery,
state-integrity, deployment, or patch-output rule.

Codex must not edit before completing this startup sequence unless the only task
is to report missing required evidence or repository instructions.

## Hard Prohibitions

Codex must not run, invoke, or indirectly trigger:

- `./scripts/run_codex_pytest_pipeline.sh`
- `./scripts/full_suite.sh`
- `./scripts/run_full_pytest_tests.sh`
- `./scripts/check_repo_runtime_artifacts.sh`
- the wrapper-owned validation command
- selector-less pytest, such as `uv run pytest -q`
- broad pytest targets, such as `uv run pytest tests`, `uv run pytest tests/`, or equivalent broad selectors
- deployment scripts
- EC2 verification scripts
- live broker scripts
- notification scripts
- remote operation scripts

The wrapper-owned validation command is:

```bash
PYTEST_XDIST_WORKERS=8 PYTEST_XDIST_DIST=worksteal ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh
```

The WSL wrapper is the only authority allowed to run that command.

If Codex believes a prohibited command is required, Codex must stop and report
the reason as a validation handoff or blocker instead of running it.

Do not modify this request file, `scripts/codex_pytest_repair_prompt.md`,
unless the latest failure packet, wrapper log, pytest failure, preflight failure,
collection/import/config error, or runtime artifact evidence directly targets this file.

Do not modify pipeline scripts unless the latest failure packet, wrapper log,
pytest failure, preflight failure, collection/import/config error, or runtime
artifact evidence directly targets those scripts.

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
- running only focused pytest commands allowed by the Focused Validation Scope
- reporting changed files, focused validation used, blockers, and residual risks

Respect the Hard Prohibitions.

## Required Failure Packet Reading Order

Codex must read the provided WSL failure packet before editing. Read packet
sections in this order:

1. `Packet Metadata`
2. `Failed Tests`
3. `Pytest Short Summary`
4. `Pytest Failure Sections`
5. `Failure Context Around Markers`
6. `Preflight Failure JSON`
7. `Preflight Failure Excerpt`
8. `First Collection Import Config Error Excerpt`
9. `Runtime Artifact Failure Excerpt`
10. `Diagnostic Runtime Artifact Check`
11. `Pytest Workspace Summary`
12. `Git Diff Stat`
13. `Git Diff Patch Excerpt`
14. `Repro Commands`
15. `Recent Full-Suite Log Tail`
16. `Required Behavior`

If a section says no matching evidence was extracted, Codex must not treat that
as proof that the failure type is absent.

Use the remaining packet sections to determine whether evidence is missing,
truncated, or genuinely not applicable.

If the packet references a full log path and excerpts are insufficient, Codex
may inspect the referenced full log file as evidence.

If the full log is unavailable, Codex must report an evidence gap instead of
guessing.

## Failure Triage and Repair Loop

The WSL wrapper has already run wrapper-owned validation and attached the
resulting failure evidence below.

If the failure packet shows that wrapper-owned validation already passed, do not
make unnecessary changes.

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
- respect the Hard Prohibitions
- continue through the known failure-cluster backlog with focused tests only
- if focused tests are insufficient after a broad or safety-critical fix, report the residual validation risk and hand control back to the WSL wrapper
- stop when the known failure clusters have focused verification, a clear blocker, or a clear handoff back to the WSL wrapper

Stay inside the selected cluster from the known failure-cluster backlog until it
is resolved or clearly blocked.

After resolving a localized cluster, continue with the remaining known clusters
from the same baseline instead of handing off immediately unless focused
validation is complete.

If the WSL wrapper later provides a new failure packet, treat it as the new
baseline evidence and repeat the focused repair process.

If further repair would require weakening the existing system purpose, safety
contracts, or production behavior, stop and report the blocker instead of
forcing a test pass.

## Testing Expectations

After a patch, run targeted focused tests for changed areas only. Do not broaden
to wrapper-owned validation inside Codex.

Codex may run only focused pytest commands allowed by the Focused Validation
Scope below.

The first full baseline command for this repair task has already been run by the
WSL wrapper. Treat the provided failure packet as the current baseline evidence.

If the failure packet shows collection errors, import errors, configuration
errors, runtime artifact failures, or an external blocker, repair or report that
blocker first.

If the failure packet contains a runtime artifact failure, Codex may repair the
underlying cause, such as removing accidental repo-local generated artifacts or
changing code that writes artifacts into the repository. Respect the Hard
Prohibitions.

Do not use full-suite reruns as the debugging loop. Do not repeat the same
command without a new hypothesis or a code change. Do not repeat the same
focused test command only by extending timeout.

If the same verification runs longer than 90 seconds, stop repeating it and
report:

- the likely bottleneck
- the narrower command used instead
- the residual validation risk

Minimize unnecessary time use, token use, and test reruns throughout the task.

If the completed patch has broad blast radius and focused tests are insufficient
to validate the affected safety or contract area, report that WSL wrapper
validation is required.

This includes:

- shared production helper changes
- shared test helper changes
- import, packaging, configuration, or path resolution changes
- storage path contract changes
- live safety, broker guard, or preflight behavior changes
- recovery, reconciliation, accounting, or state integrity changes
- behavior used by multiple unrelated failing files

After all known clusters from the provided failure packet have either focused
verification or a clearly reported blocker, stop and hand control back to the
WSL wrapper.

Respect the Hard Prohibitions.

## Focused Validation Scope

Codex may run focused commands such as:

```bash
uv run pytest tests/test_example.py -q
uv run pytest tests/test_example.py::test_specific_case -q
uv run pytest -k "specific_failure_name" -q
```

Use `-k` only when the expression is narrow and directly derived from the
failure packet.

If Codex cannot justify an added focused command using either packet evidence or
changed-code safety impact, Codex must not run it.

### A. Packet-derived Focused Validation

These commands are directly derived from visible failure evidence in the packet,
such as failed test selectors, collection errors, import errors, config errors,
or failure-specific names.

Prefer this order:

1. failing test function
2. failing test file
3. failure-specific `-k` expression
4. closely related failure cluster

### B. Changed-code Safety-area Validation

Codex may add a minimal focused safety-area matrix when the repair changes code
that directly affects a repository safety area, even if every test in that
matrix did not appear as a failure in the packet.

Allow this only when:

- the changed code directly affects the safety area
- the matrix stays small and focused
- each added command is explained in the final report
- the command does not become a broad `tests` or selector-less pytest run

Use this category for changes touching:

- path/storage contracts
- live mode, preflight, or broker guards
- recovery, reconciliation, accounting, or state integrity
- run lock, ops, or observability behavior
- shared helpers used by multiple failure clusters

## Suggested Safety-Area Focused Matrices

When multiple visible failures point to the same safety or contract area, run the
smallest focused cluster matrix that covers the shared contract before changing
broader code.

Do not run the entire list automatically. Select only the smallest subset that
directly covers:

- the visible failure cluster
- the changed production or test helper code
- the safety contract affected by the repair

When adding any test from this list that was not directly present in the failure
packet, Codex must explain why it is relevant in the final report.

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

Do not ship behavior changes without test coverage when the area is
safety-critical.

## Test Change Decision Rule

Codex may change tests only when visible packet evidence and repository behavior
show that the test expectation is wrong, stale, or inconsistent with the safety
contract.

Before changing a test, Codex must identify:

1. the current test expectation
2. the observed production behavior
3. the relevant safety or operational contract
4. why preserving production behavior is safer or more correct
5. why the test expectation should change instead of production code

Codex must not change tests merely because production code is harder to fix.

Codex must not weaken assertions, skip tests, xfail tests, or hide behavior with
unrealistic mocks.

When in doubt, preserve production safety and report the test/contract conflict.

## Insufficient Evidence Rule

If the failure packet is incomplete, contradictory, truncated, or missing
evidence required to choose a safe repair, Codex must not guess.

Codex should first inspect referenced local packet files or full-suite log paths
when available.

If required evidence is still unavailable, Codex must stop and report:

- what evidence is missing
- which cluster cannot be safely repaired
- what file or packet section would be needed
- whether WSL wrapper should regenerate the failure packet
- whether human review is required

Use final handoff status:

```text
BLOCKED_BY_INSUFFICIENT_EVIDENCE
```

## Required Final Report Format

When finished, Codex must report the following sections.

### 1. Visible Failure Clusters

For each cluster, include:

- cluster name
- visible evidence from the packet
- affected test selectors or files
- classification:
  - safety-contract related
  - shared/cross-cutting
  - localized
  - test-only expectation drift
  - externally blocked
- chosen repair priority
- whether the cluster was repaired, blocked, or handed off

### 2. Files Changed

List every changed file and the reason for changing it.

For each file, classify the change as:

- production behavior
- test expectation
- fixture/helper
- documentation/prompt/pipeline
- cleanup of generated artifacts

### 3. Focused Validation Performed

For every focused command Codex ran, include a table:

| Cluster | Command | Exit Code | Result | Packet-derived? | Why this command was allowed |
| ------- | ------- | --------: | ------ | --------------- | ---------------------------- |

`Packet-derived?` values:

- `yes` when the selector came directly from the failure packet
- `related safety-area` when the command was added because changed code directly affected a listed safety area

### 4. Commands Not Run

Codex must explicitly state that it did not run wrapper-owned validation
commands.

If Codex wanted to run a prohibited command, report why it was not run and what
handoff is needed.

### 5. Remaining Blockers and Risks

Report:

- unresolved clusters
- evidence gaps
- safety-contract concerns
- validation gaps
- whether WSL wrapper full-suite validation is required next

### 6. Handoff Decision

End with exactly one of:

- `READY_FOR_WSL_WRAPPER_VALIDATION`
- `BLOCKED_NEEDS_HUMAN_REVIEW`
- `BLOCKED_BY_INSUFFICIENT_EVIDENCE`
- `BLOCKED_BY_SAFETY_CONTRACT_CONFLICT`
