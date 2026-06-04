# Pre-Merge Validation Checklist

Run these repository-local checks before merging changes that touch config,
operator output, docs, templates, live safety, or runtime contracts.
The `safety-regression` GitHub Actions workflow runs the same targeted gate
commands after `uv sync --dev` and virtualenv activation.

```bash
python3 tools/check_text_hygiene.py
python3 tools/check_env_drift.py
python3 tools/generate_config_docs.py --check
python3 tools/generate_env_example.py --check
python3 -m pytest tests/test_text_hygiene.py tests/test_config_contract.py -q
python3 -m pytest tests/test_live_preflight.py::test_live_execution_contract_emits_safe_env_metadata_and_lints tests/test_live_preflight.py::test_live_execution_contract_log_emits_redacted_fingerprint -q
python3 -m pytest tests/test_operator_commands.py::test_cmd_signal_no_data_output_is_clean_and_actionable tests/test_operator_commands.py::test_cmd_explain_no_data_output_is_clean_and_actionable tests/test_operator_commands.py::test_cmd_status_missing_candle_output_is_clean_and_actionable -q
```

The default PR fast-suite gate is:

```bash
./scripts/run_fast_pr_tests.sh
```

It runs the static research runner marker/inventory policy check and then runs
pytest excluding `research_kernel`, `research_e2e`, `audit_e2e`,
`walk_forward_e2e`, `parallel_e2e`, `nightly`, `slow_research`, and
`memory_sensitive`, with duration reporting enabled. The fast script also parses
the reported durations and fails default-fast tests over the configured fast
threshold. The script creates a repository-external pytest workspace before
pytest starts and cleans it on success by default.

The dedicated research/nightly pytest suite is:

```bash
./scripts/run_research_nightly_tests.sh
```

This fast suite must not include full research matrices, complete-external audit
research runs, walk-forward E2E, serial/parallel real research comparisons, or
memory-sensitive checks. It must also avoid production research evaluators and
unbounded strategy/kernel tick loops; direct kernel tests in the fast suite must
stay bounded in-memory micro-kernel contracts. Run research E2E/nightly
validation through `scripts/run_research_nightly_tests.sh`, which includes
`research_kernel`, `research_e2e`, `audit_e2e`, `walk_forward_e2e`,
`parallel_e2e`, `nightly`, `slow_research`, and `memory_sensitive`, then checks
their workload budget before pytest and durations against
`tests/policy/research_e2e_inventory.json`.

The official full-suite pytest entrypoint is:

```bash
./scripts/run_full_pytest_tests.sh
```

Do not use raw selector-less `uv run pytest -q` as the default local or PR
validation path. Full-suite pytest validation must run through the dedicated
full pytest script or a later full pytest pipeline so pytest temporary files,
research artifacts, cleanup, and artifact summaries are handled consistently.

Pytest workspace controls:

- `BITHUMB_PYTEST_WORKSPACE_ROOT`: optional absolute repository-external root.
  Defaults to `/tmp/bithumb-bot-pytest-${USER:-user}`.
- `BITHUMB_PYTEST_RUN_ID`: optional run id. Defaults to a timestamp/PID value.
- `KEEP_BITHUMB_TEST_ARTIFACTS=1`: keep the run workspace and print its path
  and size summary.
- `BITHUMB_PYTEST_WORKSPACE_MAX_TOTAL_BYTES` and
  `BITHUMB_PYTEST_WORKSPACE_MAX_SINGLE_FILE_BYTES`: optional per-test workspace
  budgets enforced by the pytest fixture.

On WSL, local Linux, and CI, official runners keep pytest and generated
research/test evidence outside the repository. Successful runs clean the run
workspace by default. Failed tests, explicit keep-artifacts runs, and workspace
budget overages preserve the workspace and print a size summary. The full
runner prints the summary before successful cleanup. Preflight failures are
reported before pytest starts with the failed stage, workspace path, retained
workspace size, and a JSON report under the repo-external pytest workspace.
If a preflight fails and the retained workspace is zero bytes or only a few KB,
that normally means pytest did not start and only preflight diagnostics were
written.

## WSL full-suite disk regression check

Use the official full runner only:

```bash
df -h /
du -sh /tmp /tmp/bithumb-bot-pytest-* /tmp/pytest-of-$USER 2>/dev/null || true
./scripts/check_repo_runtime_artifacts.sh
./scripts/run_full_pytest_tests.sh
./scripts/check_repo_runtime_artifacts.sh
df -h /
du -sh /tmp /tmp/bithumb-bot-pytest-* /tmp/pytest-of-$USER 2>/dev/null || true
```

Do not use raw selector-less `uv run pytest -q` as the WSL full-suite disk
check path. The raw command bypasses the official repo-external workspace,
preflight labels, retained-workspace summaries, and cleanup policy.

Interpretation:

- `preflight failure before pytest starts`: fix the named preflight stage first.
  The full runner prints `pytest did not start`, the workspace path, retained
  size, and `preflight_failure.json`. A small zero-byte/KB retained workspace is
  expected when only preflight diagnostics exist.
- `pytest failure with retained workspace`: pytest started and failed. Inspect
  the retained repo-external workspace and largest-file summary before cleanup.
- `pytest success with workspace cleanup`: the full runner prints the final size
  summary and removes the run workspace unless `KEEP_BITHUMB_TEST_ARTIFACTS=1`
  is set.

On Windows, check the WSL distribution `ext4.vhdx` size before and after the
run from Windows Explorer or PowerShell. The VHDX may not shrink automatically
even after files are deleted inside WSL. Use `sync` after Linux-side cleanup,
then `sudo fstrim -av` when the WSL filesystem supports discard. Use
`wsl --shutdown` before Windows-side compaction. `compact vdisk` is appropriate
only after internal WSL free space has been reclaimed and no WSL distro is
running.

Solved criteria: repo artifact checks pass before and after, `/tmp` and the
official `/tmp/bithumb-bot-pytest-*` workspace do not retain unexpected large
files after a successful run, pytest success cleans the workspace, and any VHDX
growth is explained by known retained artifacts or normal sparse-file behavior.
Unresolved criteria: the repo artifact checker fails, a failed pytest workspace
contains unexplained large files, `/tmp/pytest-of-$USER` grows unexpectedly, or
`ext4.vhdx` keeps growing after WSL cleanup, `fstrim`, shutdown, and reviewed
compaction.

Research workload budgets are defined in
`tests/policy/research_workload_budget_policy.json` and enforced by
`scripts/check_research_workload_budget.py` before research/nightly and full
pytest. The preflight uses estimated tick events, audit stream rows, artifact
write count, hash payload bytes, artifact bytes, and artifact file count.
At runtime, experiment-scoped research artifacts share one
`ResearchArtifactContext` across `derived/research/<experiment_id>` and
`reports/research/<experiment_id>`, including reports, candidate journals,
candidate results/failures, return panels, statistical evidence, audit streams,
trace indexes, and trace manifests. `ArtifactBudgetExceeded` is a hard failure
and must not be swallowed as an audit observability warning. Family and
experiment JSONL registries are narrow append-only registry exemptions marked
with `budget_policy=registry_append_only_budget_exempt`.

The repo-local artifact checker rejects generated research/runtime outputs in
the repository, including `reports/research`, `derived/research`, `traces`,
`candidate_results`, `candidate_failures`, generated audit JSONL streams,
database files, `.tmp/pytest`, and `pytest-debug`. JSONL under `tests/fixtures`
or `examples` is allowed only as narrow static fixture/source material; generated
stream filenames such as `decisions.jsonl`, `equity.jsonl`,
`executions.jsonl`, and `candidate_events.jsonl` are forbidden there too unless
the checker is deliberately updated with a named fixture policy.

Selector-less full pytest is long-running/full validation and is not the
default PR check. Use `./scripts/run_full_pytest_tests.sh` or the dedicated
pytest pipeline for full-suite repair or final full validation when required.

`scripts/run_codex_pytest_pipeline.sh` is Codex full-pytest repair automation
that may commit, push, and perform EC2 smoke verification. It is not the
dedicated research/nightly pytest suite.

Required gate coverage:

- Text hygiene rejects BOM, Hangul, replacement characters, long question runs,
  and known mojibake fragments.
- Env drift rejects undeclared runtime env reads, undeclared `.env.example`
  keys, unverified docs/example drift, unsafe secret examples, unlabeled
  deprecated keys, and missing live-required examples.
- Config reference and `.env.example` stay verified against ConfigSpec.
- Live execution contract metadata includes config, docs, template, effective
  settings, env-file, provenance, approved-profile, managed-root, and runtime
  path fingerprints.
- Bithumb JWT auth warning budget is zero:
  `jwt.exceptions.InsecureKeyLengthWarning` is a test failure. Normal
  live-like tests must use centralized HS256-safe Bithumb test auth material.
  Short Bithumb secret literals are forbidden as normal test auth material and
  are allowed only in intentional negative tests that assert repo-owned
  rejection before PyJWT signing. The AST static regression test in
  `tests/test_bithumb_auth_material_policy.py` is the source of truth for that
  allowlist distinction; do not replace it with a grep-only policy.
- Operator-facing no-data diagnostics stay English, reason-coded, and
  action-oriented.
