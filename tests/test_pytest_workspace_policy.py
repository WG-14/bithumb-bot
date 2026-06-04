from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from tests.support.test_workspace import TestRunWorkspace


def test_test_run_workspace_tracks_external_runtime_and_artifact_roots(tmp_path: Path) -> None:
    workspace = TestRunWorkspace.create(
        base_root=tmp_path,
        project_root=Path.cwd(),
        run_id="run-1",
        suite_name="fast",
        node_name="tests/example.py::test_case",
    )

    assert workspace.run_id == "run-1"
    assert workspace.suite_name == "fast"
    assert workspace.runtime_root == workspace.root / "runtime"
    assert workspace.artifact_root == workspace.root / "artifacts"
    assert workspace.retention_policy == "failed"
    assert workspace.max_total_bytes > 0
    assert workspace.max_single_file_bytes > 0
    assert workspace.keep_on_failure is True
    assert Path.cwd().resolve() not in workspace.root.resolve().parents


def test_test_run_workspace_reports_size_budget_status(tmp_path: Path) -> None:
    workspace = TestRunWorkspace.create(
        base_root=tmp_path,
        project_root=Path.cwd(),
        run_id="run-budget",
        suite_name="fast",
        node_name="tests/example.py::test_budget",
        max_total_bytes=8,
        max_single_file_bytes=4,
    )
    (workspace.artifact_root / "large.bin").write_bytes(b"12345")
    (workspace.runtime_root / "small.bin").write_bytes(b"1234")

    status = workspace.budget_status()

    assert status["ok"] is False
    assert status["total_bytes"] == 9
    assert status["largest_file_bytes"] == 5
    assert {item["reason"] for item in status["violations"]} == {
        "pytest_workspace_total_bytes_exceeded",
        "pytest_workspace_single_file_bytes_exceeded",
    }
    assert "budget_violation" in workspace.format_summary()


def test_pytest_workspace_wrapper_cleans_successful_workspace(tmp_path: Path) -> None:
    script = Path("scripts/lib/pytest_workspace.sh").resolve()
    workspace_root = tmp_path / "workspace"
    proc = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"source {script}; "
                "bithumb_pytest_setup_workspace fast; "
                "touch \"$PYTEST_DEBUG_TEMPROOT/proof.txt\"; "
                "kept=\"$BITHUMB_PYTEST_WORKSPACE\"; "
                "bithumb_pytest_cleanup_workspace 0; "
                "test ! -e \"$kept\""
            ),
        ],
        env={**os.environ, "BITHUMB_PYTEST_WORKSPACE_ROOT": str(workspace_root), "BITHUMB_PYTEST_RUN_ID": "run-clean"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "cleaned workspace" in proc.stdout


def test_pytest_workspace_wrapper_keeps_requested_artifacts(tmp_path: Path) -> None:
    script = Path("scripts/lib/pytest_workspace.sh").resolve()
    workspace_root = tmp_path / "workspace"
    proc = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"source {script}; "
                "bithumb_pytest_setup_workspace fast; "
                "touch \"$PYTEST_DEBUG_TEMPROOT/proof.txt\"; "
                "kept=\"$BITHUMB_PYTEST_WORKSPACE\"; "
                "bithumb_pytest_cleanup_workspace 0; "
                "test -e \"$kept/proof.txt\" -o -e \"$kept/pytest-debug/proof.txt\""
            ),
        ],
        env={
            **os.environ,
            "BITHUMB_PYTEST_WORKSPACE_ROOT": str(workspace_root),
            "BITHUMB_PYTEST_RUN_ID": "run-keep",
            "KEEP_BITHUMB_TEST_ARTIFACTS": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "keeping workspace" in proc.stdout


def test_pytest_workspace_wrapper_prints_success_summary_when_requested(tmp_path: Path) -> None:
    script = Path("scripts/lib/pytest_workspace.sh").resolve()
    workspace_root = tmp_path / "workspace"
    proc = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"source {script}; "
                "bithumb_pytest_setup_workspace full; "
                "touch \"$PYTEST_DEBUG_TEMPROOT/proof.txt\"; "
                "kept=\"$BITHUMB_PYTEST_WORKSPACE\"; "
                "bithumb_pytest_cleanup_workspace 0; "
                "test ! -e \"$kept\""
            ),
        ],
        env={
            **os.environ,
            "BITHUMB_PYTEST_WORKSPACE_ROOT": str(workspace_root),
            "BITHUMB_PYTEST_RUN_ID": "run-summary",
            "BITHUMB_PYTEST_SUMMARY_ON_SUCCESS": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "retained_size_bytes=" in proc.stdout
    assert "cleaned workspace" in proc.stdout


def test_pytest_workspace_wrapper_refuses_repo_local_workspace() -> None:
    script = Path("scripts/lib/pytest_workspace.sh").resolve()
    proc = subprocess.run(
        ["bash", "-c", f"source {script}; bithumb_pytest_setup_workspace fast"],
        env={**os.environ, "BITHUMB_PYTEST_WORKSPACE_ROOT": str(Path.cwd())},
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "refusing repo-local cleanup target" in proc.stderr


def test_official_runners_use_external_workspace_and_no_repo_local_basetemp() -> None:
    for path in (
        Path("scripts/run_fast_pr_tests.sh"),
        Path("scripts/run_research_nightly_tests.sh"),
        Path("scripts/run_full_pytest_tests.sh"),
        Path("scripts/run_parallel_research_safety_tests.sh"),
    ):
        text = path.read_text(encoding="utf-8")
        assert "scripts/lib/pytest_workspace.sh" in text
        assert "bithumb_pytest_setup_workspace" in text
        assert "bithumb_pytest_sanitize_unsafe_env" in text
        assert '--basetemp="$PWD/.tmp/pytest"' not in text
        assert ".tmp/pytest" not in text


def test_full_runner_requests_success_artifact_summary_before_cleanup() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")
    setup_index = text.index('bithumb_pytest_setup_workspace "full"')
    summary_index = text.index("export BITHUMB_PYTEST_SUMMARY_ON_SUCCESS=1")
    cleanup_index = text.index("bithumb_pytest_cleanup_workspace")

    assert setup_index < summary_index < cleanup_index


def test_pytest_workspace_preflight_failure_writes_external_report(tmp_path: Path) -> None:
    script = Path("scripts/lib/pytest_workspace.sh").resolve()
    workspace_root = tmp_path / "workspace"
    proc = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"source {script}; "
                "bithumb_pytest_setup_workspace full; "
                "if bithumb_pytest_run_preflight 'research test policy' false; then exit 99; fi; "
                "report=\"$BITHUMB_PYTEST_WORKSPACE/preflight_failure.json\"; "
                "test -f \"$report\"; "
                "python3 - \"$report\" <<'PY'\n"
                "import json, sys\n"
                "payload = json.load(open(sys.argv[1], encoding='utf-8'))\n"
                "assert payload['suite'] == 'full'\n"
                "assert payload['failed_stage'] == 'research test policy'\n"
                "assert payload['pytest_started'] is False\n"
                "assert payload['status'] == 'preflight_failed'\n"
                "assert payload['exit_code'] == 1\n"
                "assert payload['workspace_root']\n"
                "assert payload['retained_workspace_size_bytes'] >= 0\n"
                "PY\n"
            ),
        ],
        env={
            **os.environ,
            "BITHUMB_PYTEST_WORKSPACE_ROOT": str(workspace_root),
            "BITHUMB_PYTEST_RUN_ID": "run-preflight-fail",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    output = proc.stdout + proc.stderr
    assert "[PYTEST-PREFLIGHT] failed suite=full stage=research test policy exit_code=1" in output
    assert "[PYTEST-PREFLIGHT] pytest did not start" in output
    assert "preflight_failure.json" in output
    report = workspace_root / "full" / "run-preflight-fail" / "preflight_failure.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert Path(payload["workspace_root"]).resolve() == (workspace_root / "full" / "run-preflight-fail").resolve()
    assert Path.cwd().resolve() not in report.resolve().parents


def test_full_runner_uses_labeled_preflights_before_pytest_start() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")
    pythonpath_index = text.index('export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"')
    safety_index = text.index("bithumb_pytest_sanitize_unsafe_env")
    research_policy_index = text.index('bithumb_pytest_run_preflight "research test policy"')
    strategy_guard_index = text.index('bithumb_pytest_run_preflight "strategy PR workload guard"')
    budget_index = text.index('bithumb_pytest_run_preflight "research workload budget full"')
    started_index = text.index("bithumb_pytest_mark_pytest_started")
    pytest_index = text.index('uv run pytest "${pytest_args[@]}"')

    assert pythonpath_index < safety_index < research_policy_index < strategy_guard_index < budget_index < started_index < pytest_index


def test_official_runners_sanitize_unsafe_env_before_preflight_or_pytest() -> None:
    for path in (
        Path("scripts/run_full_pytest_tests.sh"),
        Path("scripts/run_fast_pr_tests.sh"),
        Path("scripts/run_research_nightly_tests.sh"),
        Path("scripts/run_parallel_research_safety_tests.sh"),
    ):
        text = path.read_text(encoding="utf-8")
        pythonpath_index = text.index('export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"')
        sanitizer_index = text.index("bithumb_pytest_sanitize_unsafe_env")
        command_indexes = [
            text.index(marker)
            for marker in (
                "bithumb_pytest_run_preflight",
                "uv run pytest",
                '"${pytest_cmd[@]}"',
            )
            if marker in text
        ]
        assert command_indexes, path
        assert pythonpath_index < sanitizer_index < min(command_indexes), path


def test_full_runner_sanitizes_notification_env_before_preflight_and_pytest(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "env-capture.txt"
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
{
  printf 'args=%s\\n' "$*"
  printf 'NOTIFIER_ENABLED=%s\\n' "${NOTIFIER_ENABLED-__unset__}"
  printf 'NTFY_TOPIC=%s\\n' "${NTFY_TOPIC-__unset__}"
  printf 'NOTIFIER_WEBHOOK_URL=%s\\n' "${NOTIFIER_WEBHOOK_URL-__unset__}"
  printf 'SLACK_WEBHOOK_URL=%s\\n' "${SLACK_WEBHOOK_URL-__unset__}"
  printf 'TELEGRAM_BOT_TOKEN=%s\\n' "${TELEGRAM_BOT_TOKEN-__unset__}"
  printf 'TELEGRAM_CHAT_ID=%s\\n' "${TELEGRAM_CHAT_ID-__unset__}"
  printf 'BITHUMB_API_KEY=%s\\n' "${BITHUMB_API_KEY-__unset__}"
  printf 'BITHUMB_API_SECRET=%s\\n' "${BITHUMB_API_SECRET-__unset__}"
  printf '%s\\n' '---'
} >> "$BITHUMB_CAPTURE_ENV"
exit 0
""",
        encoding="utf-8",
        newline="\n",
    )
    fake_uv.chmod(0o755)

    proc = subprocess.run(
        ["bash", "scripts/run_full_pytest_tests.sh"],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "BITHUMB_CAPTURE_ENV": str(capture),
            "BITHUMB_PYTEST_WORKSPACE_ROOT": str(tmp_path / "workspace"),
            "BITHUMB_PYTEST_RUN_ID": "run-env-safety",
            "BITHUMB_PYTEST_ALLOW_EXTERNAL_NOTIFICATIONS": "0",
            "NOTIFIER_ENABLED": "true",
            "NTFY_TOPIC": "real-topic",
            "NOTIFIER_WEBHOOK_URL": "https://example.invalid/generic",
            "SLACK_WEBHOOK_URL": "https://example.invalid/slack",
            "TELEGRAM_BOT_TOKEN": "real-token",
            "TELEGRAM_CHAT_ID": "real-chat",
            "BITHUMB_API_KEY": "real-api-key",
            "BITHUMB_API_SECRET": "real-api-secret",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "[PYTEST-SAFETY] unsafe inherited env disabled for full pytest runner" in proc.stdout
    captures = [block.strip().splitlines() for block in capture.read_text(encoding="utf-8").split("---") if block.strip()]
    assert len(captures) == 4
    assert any("args=run pytest -q" in block[0] for block in captures)
    for block in captures:
        values = dict(line.split("=", 1) for line in block[1:])
        assert values == {
            "NOTIFIER_ENABLED": "false",
            "NTFY_TOPIC": "__unset__",
            "NOTIFIER_WEBHOOK_URL": "__unset__",
            "SLACK_WEBHOOK_URL": "__unset__",
            "TELEGRAM_BOT_TOKEN": "__unset__",
            "TELEGRAM_CHAT_ID": "__unset__",
            "BITHUMB_API_KEY": "__unset__",
            "BITHUMB_API_SECRET": "__unset__",
        }


def test_full_runner_notification_opt_in_still_clears_broker_private_env(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "env-capture.txt"
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
{
  printf 'args=%s\\n' "$*"
  printf 'NOTIFIER_ENABLED=%s\\n' "${NOTIFIER_ENABLED-__unset__}"
  printf 'NTFY_TOPIC=%s\\n' "${NTFY_TOPIC-__unset__}"
  printf 'NOTIFIER_WEBHOOK_URL=%s\\n' "${NOTIFIER_WEBHOOK_URL-__unset__}"
  printf 'SLACK_WEBHOOK_URL=%s\\n' "${SLACK_WEBHOOK_URL-__unset__}"
  printf 'TELEGRAM_BOT_TOKEN=%s\\n' "${TELEGRAM_BOT_TOKEN-__unset__}"
  printf 'TELEGRAM_CHAT_ID=%s\\n' "${TELEGRAM_CHAT_ID-__unset__}"
  printf 'BITHUMB_API_KEY=%s\\n' "${BITHUMB_API_KEY-__unset__}"
  printf 'BITHUMB_API_SECRET=%s\\n' "${BITHUMB_API_SECRET-__unset__}"
  printf '%s\\n' '---'
} >> "$BITHUMB_CAPTURE_ENV"
exit 0
""",
        encoding="utf-8",
        newline="\n",
    )
    fake_uv.chmod(0o755)

    proc = subprocess.run(
        ["bash", "scripts/run_full_pytest_tests.sh"],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "BITHUMB_CAPTURE_ENV": str(capture),
            "BITHUMB_PYTEST_WORKSPACE_ROOT": str(tmp_path / "workspace"),
            "BITHUMB_PYTEST_RUN_ID": "run-env-safety-opt-in",
            "BITHUMB_PYTEST_ALLOW_EXTERNAL_NOTIFICATIONS": "1",
            "NOTIFIER_ENABLED": "true",
            "NTFY_TOPIC": "real-topic",
            "NOTIFIER_WEBHOOK_URL": "https://example.invalid/generic",
            "SLACK_WEBHOOK_URL": "https://example.invalid/slack",
            "TELEGRAM_BOT_TOKEN": "real-token",
            "TELEGRAM_CHAT_ID": "real-chat",
            "BITHUMB_API_KEY": "real-api-key",
            "BITHUMB_API_SECRET": "real-api-secret",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "broker-private env disabled for full pytest runner; external notification env allowed by explicit opt-in" in proc.stdout
    captures = [block.strip().splitlines() for block in capture.read_text(encoding="utf-8").split("---") if block.strip()]
    assert len(captures) == 4
    for block in captures:
        values = dict(line.split("=", 1) for line in block[1:])
        assert values == {
            "NOTIFIER_ENABLED": "true",
            "NTFY_TOPIC": "real-topic",
            "NOTIFIER_WEBHOOK_URL": "https://example.invalid/generic",
            "SLACK_WEBHOOK_URL": "https://example.invalid/slack",
            "TELEGRAM_BOT_TOKEN": "real-token",
            "TELEGRAM_CHAT_ID": "real-chat",
            "BITHUMB_API_KEY": "__unset__",
            "BITHUMB_API_SECRET": "__unset__",
        }


def test_full_runner_supports_optional_xdist_without_changing_serial_default() -> None:
    text = Path("scripts/run_full_pytest_tests.sh").read_text(encoding="utf-8")

    assert 'if [[ -n "${PYTEST_XDIST_WORKERS:-}" && "${PYTEST_XDIST_WORKERS:-0}" != "0" ]]' in text
    assert 'pytest_args+=(-n "$PYTEST_XDIST_WORKERS" --dist="${PYTEST_XDIST_DIST:-loadfile}")' in text
    assert "pytest_args=(-q)" in text


def test_wsl_full_suite_disk_runbook_uses_official_runner() -> None:
    text = Path("docs/pre-merge-checklist.md").read_text(encoding="utf-8")

    assert "WSL full-suite disk regression check" in text
    assert "./scripts/run_full_pytest_tests.sh" in text
    assert "df -h /" in text
    assert "du -sh /tmp /tmp/bithumb-bot-pytest-* /tmp/pytest-of-$USER" in text
    assert "./scripts/check_repo_runtime_artifacts.sh" in text
    assert "ext4.vhdx" in text
    assert "preflight failure before pytest starts" in text
    assert "pytest success with workspace cleanup" in text
    assert "fstrim" in text
    assert "compact vdisk" in text
    assert "Do not use raw selector-less `uv run pytest -q`" in text
