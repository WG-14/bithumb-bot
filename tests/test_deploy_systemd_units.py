from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_service(path: Path) -> ConfigParser:
    parser = ConfigParser()
    parser.optionxform = str
    parser.read(path, encoding="utf-8")
    return parser


def test_live_and_paper_services_use_distinct_env_files_and_log_channels() -> None:
    live_path = REPO_ROOT / "deploy/systemd/bithumb-bot.service"
    paper_path = REPO_ROOT / "deploy/systemd/bithumb-bot-paper.service"

    live = _read_service(live_path)
    paper = _read_service(paper_path)

    live_env = live["Service"]["Environment"]
    paper_env = paper["Service"]["Environment"]
    assert "BITHUMB_ENV_FILE=" in live_env
    assert "BITHUMB_ENV_FILE=" in paper_env
    assert live_env != paper_env

    live_mode = live["Service"]["ExecStart"]
    paper_mode = paper["Service"]["ExecStart"]
    assert "--mode live" in live_mode
    assert "--mode paper" in paper_mode

    assert live["Service"]["SyslogIdentifier"] != paper["Service"]["SyslogIdentifier"]
