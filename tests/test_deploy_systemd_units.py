from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SYSTEMD_DIR = REPO_ROOT / "deploy/systemd"


def _read_unit(path: Path) -> ConfigParser:
    parser = ConfigParser()
    parser.optionxform = str
    parser.read(path, encoding="utf-8")
    return parser


def test_systemd_units_do_not_hardcode_workspace_path() -> None:
    for path in sorted(SYSTEMD_DIR.glob("*.service")) + sorted(SYSTEMD_DIR.glob("*.timer")):
        content = path.read_text(encoding="utf-8")
        assert "/workspace/bithumb-bot" not in content


def test_operational_scripts_do_not_hardcode_home_directory_repo_path() -> None:
    script_paths = [
        REPO_ROOT / "scripts" / "check_live_runtime.sh",
        REPO_ROOT / "scripts" / "collect_live_snapshot.sh",
        REPO_ROOT / "deploy" / "systemd" / "render_units.sh",
    ]
    for path in script_paths:
        content = path.read_text(encoding="utf-8")
        assert "/home/ec2-user/bithumb-bot" not in content


def test_services_use_consistent_explicit_env_source_rule() -> None:
    service_paths = [
        SYSTEMD_DIR / "bithumb-bot.service",
        SYSTEMD_DIR / "bithumb-bot-paper.service",
        SYSTEMD_DIR / "bithumb-bot-healthcheck.service",
        SYSTEMD_DIR / "bithumb-bot-backup.service",
    ]

    for path in service_paths:
        unit = _read_unit(path)
        service = unit["Service"]
        assert "Environment" in service
        assert "BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_" in service["Environment"]
        assert "MODE=" in service["Environment"]
        assert "RUN_ROOT=@BITHUMB_RUN_ROOT@" in service["Environment"]
        assert "DATA_ROOT=@BITHUMB_DATA_ROOT@" in service["Environment"]
        assert "LOG_ROOT=@BITHUMB_LOG_ROOT@" in service["Environment"]
        assert "BACKUP_ROOT=@BITHUMB_BACKUP_ROOT@" in service["Environment"]
        if path.name in {"bithumb-bot.service", "bithumb-bot-paper.service"}:
            assert "PYTHONUNBUFFERED=1" in service["Environment"]
        assert "EnvironmentFile" not in service


def test_live_and_paper_services_use_mode_specific_env_and_canonical_entrypoint() -> None:
    live = _read_unit(SYSTEMD_DIR / "bithumb-bot.service")
    paper = _read_unit(SYSTEMD_DIR / "bithumb-bot-paper.service")

    live_env = live["Service"]["Environment"]
    paper_env = paper["Service"]["Environment"]
    assert "MODE=live" in live_env
    assert "MODE=paper" in paper_env
    assert "BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_LIVE@" in live_env
    assert "BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_PAPER@" in paper_env
    assert "PYTHONUNBUFFERED=1" in live_env
    assert "PYTHONUNBUFFERED=1" in paper_env

    live_exec = live["Service"]["ExecStart"]
    paper_exec = paper["Service"]["ExecStart"]
    assert "@BITHUMB_UV_BIN@ run python -u -m bithumb_bot run" in live_exec
    assert "@BITHUMB_UV_BIN@ run python -u -m bithumb_bot run" in paper_exec
    assert "--mode live" in live_exec
    assert "--mode paper" in paper_exec

    assert live["Service"]["SyslogIdentifier"] != paper["Service"]["SyslogIdentifier"]


def test_backup_service_uses_bash_script_invocation_without_shell_string() -> None:
    backup = _read_unit(SYSTEMD_DIR / "bithumb-bot-backup.service")
    exec_start = backup["Service"]["ExecStart"]

    assert exec_start == "/usr/bin/env bash @BITHUMB_BOT_ROOT@/scripts/backup_sqlite.sh"
    assert "-lc" not in exec_start


def test_healthcheck_service_uses_templated_runtime_user_and_uv_binary() -> None:
    healthcheck = _read_unit(SYSTEMD_DIR / "bithumb-bot-healthcheck.service")
    service = healthcheck["Service"]

    assert service["Type"] == "oneshot"
    assert service["User"] == "@BITHUMB_RUN_USER@"
    assert service["WorkingDirectory"] == "@BITHUMB_BOT_ROOT@"
    assert "MODE=live" in service["Environment"]
    assert "BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_LIVE@" in service["Environment"]
    assert service["SyslogIdentifier"] == "bithumb-bot-healthcheck"
    assert service["ExecStart"] == "@BITHUMB_UV_BIN@ run python @BITHUMB_BOT_ROOT@/scripts/healthcheck.py"
    assert "/usr/bin/env uv" not in service["ExecStart"]
