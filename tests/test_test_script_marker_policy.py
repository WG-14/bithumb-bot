from __future__ import annotations

import re
import subprocess
from pathlib import Path

from tests.policy.research_runner_policy import DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS


CANONICAL_FAST_MARKER_EXPR = (
    "not research_kernel and not research_e2e and not audit_e2e and not walk_forward_e2e "
    "and not parallel_e2e and not nightly and not slow_research and not memory_sensitive"
)
CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR = (
    "research_kernel or research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e "
    "or nightly or slow_research or memory_sensitive"
)


def _shell_assignment(path: Path, name: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf'^{name}="([^"]+)"$', text, flags=re.MULTILINE)
    assert match is not None, f"{path} missing {name}"
    return match.group(1)


def _fast_excluded_markers(expr: str) -> set[str]:
    parts = expr.split(" and ")
    markers = []
    for part in parts:
        match = re.fullmatch(r"not ([a-zA-Z0-9_]+)", part)
        assert match is not None, f"unsupported fast marker expression term: {part}"
        markers.append(match.group(1))
    return set(markers)


def _nightly_included_markers(expr: str) -> set[str]:
    parts = expr.split(" or ")
    for part in parts:
        assert re.fullmatch(r"[a-zA-Z0-9_]+", part) is not None, f"unsupported nightly marker term: {part}"
    return set(parts)


def test_fast_and_diagnostic_scripts_use_canonical_marker_expressions() -> None:
    active_scripts = [
        Path("scripts/run_fast_pr_tests.sh"),
        Path("run_pytest_diagnostics.sh"),
        Path("run_remaining_test_results.sh"),
    ]

    for script in active_scripts:
        assert _shell_assignment(script, "FAST_MARKER_EXPR") == CANONICAL_FAST_MARKER_EXPR

    for script in [Path("run_pytest_diagnostics.sh"), Path("run_remaining_test_results.sh")]:
        assert _shell_assignment(script, "RESEARCH_NIGHTLY_MARKER_EXPR") == CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR


def test_research_nightly_script_uses_canonical_marker_expression() -> None:
    assert (
        _shell_assignment(Path("scripts/run_research_nightly_tests.sh"), "RESEARCH_NIGHTLY_MARKER_EXPR")
        == CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR
    )


def test_parallel_research_safety_script_uses_drift_proof_marker_expression() -> None:
    path = Path("scripts/run_parallel_research_safety_tests.sh")
    text = path.read_text(encoding="utf-8")

    assert _shell_assignment(path, "PARALLEL_RESEARCH_SAFETY_MARKER_EXPR") == "parallel_e2e or memory_sensitive"
    assert '-m "$PARALLEL_RESEARCH_SAFETY_MARKER_EXPR"' in text
    assert "tests/test_research_process_runtime.py" in text
    assert "scripts/check_research_test_policy.py" in text


def test_parallel_research_safety_script_runs_warning_as_error_and_runtime_artifact_check() -> None:
    text = Path("scripts/run_parallel_research_safety_tests.sh").read_text(encoding="utf-8")

    assert "-W error::DeprecationWarning" in text
    assert '-n "${PYTEST_XDIST_WORKERS:-2}"' in text
    assert '--dist="${PYTEST_XDIST_DIST:-worksteal}"' in text
    assert "./scripts/check_repo_runtime_artifacts.sh" in text


def test_full_runner_defaults_to_worksteal() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")

    assert 'PYTEST_XDIST_DIST:-worksteal' in text
    assert 'PYTEST_XDIST_DIST:-loadfile' not in text
    assert '--dist=worksteal' not in text
    assert 'echo "[PYTEST-XDIST] workers=${PYTEST_XDIST_WORKERS} dist=${pytest_dist}"' in text


def test_full_runner_logs_xdist_workers_and_dist() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")

    assert 'echo "[PYTEST-XDIST] workers=${PYTEST_XDIST_WORKERS} dist=${pytest_dist}"' in text
    assert 'pytest_args+=(-n "$PYTEST_XDIST_WORKERS" --dist="${pytest_dist}")' in text


def test_full_runner_allows_explicit_xdist_dist_override() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")

    assert 'pytest_dist="${PYTEST_XDIST_DIST:-worksteal}"' in text
    assert '--dist="${pytest_dist}"' in text
    assert '--dist=worksteal' not in text


def test_full_runner_skips_xdist_when_worker_count_is_empty_or_zero() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")

    guard_index = text.index('if [[ -n "${PYTEST_XDIST_WORKERS:-}" && "${PYTEST_XDIST_WORKERS:-0}" != "0" ]]; then')
    append_index = text.index('pytest_args+=(-n "$PYTEST_XDIST_WORKERS" --dist="${pytest_dist}")')
    end_index = text.index("\nfi", append_index)

    assert guard_index < append_index < end_index


def test_full_suite_wrapper_defaults_to_8_workers_and_worksteal() -> None:
    text = Path("scripts/full_suite.sh").read_text(encoding="utf-8")

    assert 'pytest_workers="${PYTEST_XDIST_WORKERS:-8}"' in text
    assert 'pytest_workers="${PYTEST_XDIST_WORKERS:-4}"' not in text
    assert 'pytest_dist="${PYTEST_XDIST_DIST:-worksteal}"' in text
    assert 'pytest_dist="${PYTEST_XDIST_DIST:-loadfile}"' not in text


def test_full_suite_wrapper_preserves_latest_log_pointer_for_failure_packets() -> None:
    text = Path("scripts/full_suite.sh").read_text(encoding="utf-8")

    assert 'latest_log_file="${WORK_DIR}/latest_full_suite_log"' in text
    assert 'printf \'%s\\n\' "${log_file}" > "${latest_log_file}"' in text


def test_full_suite_wrapper_preserves_pipe_status_for_tee() -> None:
    text = Path("scripts/full_suite.sh").read_text(encoding="utf-8")

    assert 'pytest_exit="${PIPESTATUS[0]}"' in text
    assert 'artifact_exit="${PIPESTATUS[0]}"' in text


def test_full_suite_wrapper_runs_artifact_check_only_after_pytest_success() -> None:
    text = Path("scripts/full_suite.sh").read_text(encoding="utf-8")

    success_guard_index = text.index('if [[ "${pytest_exit}" -eq 0 ]]; then')
    artifact_index = text.index("./scripts/check_repo_runtime_artifacts.sh", success_guard_index)
    else_index = text.index("\nelse", artifact_index)

    assert success_guard_index < artifact_index < else_index


def test_full_suite_wrapper_does_not_embed_local_review_log_path() -> None:
    text = Path("scripts/full_suite.sh").read_text(encoding="utf-8")

    assert ".local-review-logs" not in text
    assert ".local-review-logs/full_runner_worksteal.log" not in text


def test_codex_pipeline_delegates_full_suite_to_full_suite_wrapper() -> None:
    text = Path("scripts/run_codex_pytest_pipeline.sh").read_text(encoding="utf-8")

    assert 'FULL_SUITE_SCRIPT="${FULL_SUITE_SCRIPT:-${SCRIPT_DIR}/full_suite.sh}"' in text
    assert 'if "${FULL_SUITE_SCRIPT}"; then' in text


def test_codex_pipeline_does_not_inline_manual_full_runner_command() -> None:
    text = Path("scripts/run_codex_pytest_pipeline.sh").read_text(encoding="utf-8")

    assert ".local-review-logs/full_runner_worksteal.log" not in text
    assert "./scripts/run_full_pytest_tests.sh 2>&1 | tee" not in text
    assert (
        "PYTEST_XDIST_WORKERS=8 PYTEST_XDIST_DIST=worksteal ./scripts/run_full_pytest_tests.sh && "
        "./scripts/check_repo_runtime_artifacts.sh"
    ) not in text


def test_codex_pipeline_preserves_failure_packet_flow() -> None:
    text = Path("scripts/run_codex_pytest_pipeline.sh").read_text(encoding="utf-8")

    assert 'PACKET_SCRIPT="${PACKET_SCRIPT:-${SCRIPT_DIR}/make_failure_packet.sh}"' in text
    assert 'codex_input_file="$("${PACKET_SCRIPT}")"' in text


def test_codex_repair_prompt_uses_8_worker_worksteal_wrapper_command() -> None:
    text = Path("scripts/codex_pytest_repair_prompt.md").read_text(encoding="utf-8")

    assert "PYTEST_XDIST_WORKERS=8 PYTEST_XDIST_DIST=worksteal" in text
    assert "PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=worksteal" not in text


def test_codex_repair_prompt_preserves_wrapper_owned_validation_ban() -> None:
    text = Path("scripts/codex_pytest_repair_prompt.md").read_text(encoding="utf-8")

    assert "Do not run `./scripts/run_full_pytest_tests.sh`." in text
    assert "Do not run `./scripts/check_repo_runtime_artifacts.sh`." in text
    assert "Do not run `./scripts/full_suite.sh`." in text
    assert "Codex must not run this command." in text


def test_codex_repair_prompt_points_wrapper_to_full_suite() -> None:
    text = Path("scripts/codex_pytest_repair_prompt.md").read_text(encoding="utf-8")

    wrapper_index = text.index("The wrapper normally invokes validation through:")
    full_suite_index = text.index("./scripts/full_suite.sh", wrapper_index)
    codex_ban_index = text.index("Codex must not run `./scripts/full_suite.sh` directly.", full_suite_index)

    assert wrapper_index < full_suite_index < codex_ban_index


def test_no_authoritative_full_suite_command_uses_old_4_worker_or_loadfile_default() -> None:
    paths = [
        Path("scripts/codex_pytest_repair_prompt.md"),
        Path("scripts/full_suite.sh"),
        Path("scripts/make_failure_packet.sh"),
        Path("scripts/run_full_pytest_tests.sh"),
        Path("scripts/run_codex_pytest_pipeline.sh"),
        Path("docs/research-validation.md"),
    ]

    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert "PYTEST_XDIST_WORKERS=4 PYTEST_XDIST_DIST=worksteal ./scripts/run_full_pytest_tests.sh" not in text
        assert "PYTEST_XDIST_WORKERS:-4" not in text
        assert "PYTEST_XDIST_DIST:-loadfile" not in text
        if path != Path("docs/research-validation.md"):
            assert "PYTEST_XDIST_DIST=loadfile" not in text


def test_parallel_research_safety_runner_uses_parallel_marker_expression() -> None:
    path = Path("scripts/run_parallel_research_safety_tests.sh")
    text = path.read_text(encoding="utf-8")

    assert _shell_assignment(path, "PARALLEL_RESEARCH_SAFETY_MARKER_EXPR") == "parallel_e2e or memory_sensitive"
    assert '-m "$PARALLEL_RESEARCH_SAFETY_MARKER_EXPR"' in text
    assert 'PYTEST_XDIST_DIST:-worksteal' in text


def test_parallel_research_safety_runner_runs_process_runtime_tests_first() -> None:
    text = Path("scripts/run_parallel_research_safety_tests.sh").read_text(encoding="utf-8")

    runtime_index = text.index("tests/test_research_process_runtime.py")
    marker_index = text.index('-m "$PARALLEL_RESEARCH_SAFETY_MARKER_EXPR"')

    assert runtime_index < marker_index


def test_full_runner_does_not_replace_parallel_safety_runner() -> None:
    full_text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")
    safety_text = Path("scripts/run_parallel_research_safety_tests.sh").read_text(encoding="utf-8")

    assert "-m" not in full_text
    assert "PARALLEL_RESEARCH_SAFETY_MARKER_EXPR" in safety_text
    assert "tests/test_research_process_runtime.py" in safety_text


def test_worksteal_diagnostic_runner_exists() -> None:
    path = Path("scripts/run_xdist_worksteal_diagnostics.sh")

    assert path.exists()
    assert path.stat().st_mode & 0o111
    assert 'PYTEST_XDIST_DIST="${PYTEST_XDIST_DIST:-worksteal}"' in path.read_text(encoding="utf-8")


def test_worksteal_diagnostic_runner_fails_on_first_failed_iteration(tmp_path) -> None:
    script = tmp_path / "scripts" / "run_xdist_worksteal_diagnostics.sh"
    script.parent.mkdir()
    script.write_text(Path("scripts/run_xdist_worksteal_diagnostics.sh").read_text(encoding="utf-8"), encoding="utf-8")
    script.chmod(0o755)
    fake_runner = tmp_path / "scripts" / "run_full_pytest_tests.sh"
    fake_runner.write_text(
        "#!/usr/bin/env bash\n"
        "count_file=\"$PWD/count\"\n"
        "count=0\n"
        "if [[ -f \"$count_file\" ]]; then count=$(cat \"$count_file\"); fi\n"
        "count=$((count + 1))\n"
        "printf '%s' \"$count\" > \"$count_file\"\n"
        "exit 1\n",
        encoding="utf-8",
        newline="\n",
    )
    fake_runner.chmod(0o755)

    proc = subprocess.run(
        ["bash", str(script)],
        cwd=tmp_path,
        env={"PATH": "/usr/bin:/bin", "PYTEST_WORKSTEAL_DIAGNOSTIC_ITERATIONS": "3"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert (tmp_path / "count").read_text(encoding="utf-8") == "1"


def test_worksteal_diagnostic_runner_logs_iteration_number() -> None:
    text = Path("scripts/run_xdist_worksteal_diagnostics.sh").read_text(encoding="utf-8")

    assert "iteration=${iteration}/${iterations}" in text


def test_operator_tests_are_split_across_multiple_files() -> None:
    operator_files = sorted(Path("tests/operator").glob("test_*.py"))

    assert len(operator_files) >= 3
    assert Path("tests/test_operator_commands.py").exists()


def test_no_operator_test_file_exceeds_collection_limit() -> None:
    proc = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", "tests/operator", "tests/test_operator_commands.py"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    counts: dict[str, int] = {}
    for line in proc.stdout.splitlines():
        if "::test_" not in line:
            continue
        file_name = line.split("::", 1)[0]
        counts[file_name] = counts.get(file_name, 0) + 1
    assert counts
    assert max(counts.values()) <= 200
    assert counts.get("tests/test_operator_commands.py", 0) <= 50


def test_fast_pr_script_exports_fast_test_tier_before_pytest() -> None:
    text = Path("scripts/run_fast_pr_tests.sh").read_text(encoding="utf-8")

    export_index = text.index("export BITHUMB_TEST_TIER=fast")
    pytest_index = text.index("uv run pytest")

    assert export_index < pytest_index


def test_fast_and_nightly_scripts_run_policy_guards_before_pytest() -> None:
    for path in (Path("scripts/run_fast_pr_tests.sh"), Path("scripts/run_research_nightly_tests.sh")):
        text = path.read_text(encoding="utf-8")
        research_policy_index = text.index("uv run python scripts/check_research_test_policy.py")
        strategy_guard_index = text.index("uv run python scripts/check_strategy_pr_workload_guard.py")
        pytest_index = text.index("uv run pytest")

        assert research_policy_index < pytest_index
        assert strategy_guard_index < pytest_index


def test_research_nightly_and_full_scripts_run_workload_budget_before_pytest() -> None:
    for path in (Path("scripts/run_research_nightly_tests.sh"), Path("scripts/run_full_pytest_tests.sh")):
        text = path.read_text(encoding="utf-8")
        budget_index = text.index("uv run python scripts/check_research_workload_budget.py --suite")
        pytest_index = text.index("uv run pytest")

        assert budget_index < pytest_index


def test_default_fast_excluded_research_markers_match_script_expressions() -> None:
    expected = set(DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS)
    assert "research_kernel" in expected

    fast_expr = _shell_assignment(Path("scripts/run_fast_pr_tests.sh"), "FAST_MARKER_EXPR")
    nightly_expr = _shell_assignment(Path("scripts/run_research_nightly_tests.sh"), "RESEARCH_NIGHTLY_MARKER_EXPR")

    assert _fast_excluded_markers(fast_expr) == expected
    assert _nightly_included_markers(nightly_expr) == expected
