from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def test_config_fail_fast_when_live_mode_missing_db_path() -> None:
    env = dict(os.environ)
    env["MODE"] = "live"
    env.pop("DB_PATH", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "live env 파일에 DB_PATH를 명시하라" in (proc.stderr + proc.stdout)


def test_config_keeps_paper_default_db_path_when_unset() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env.pop("DB_PATH", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.DB_PATH)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    expected = (Path.cwd() / "data" / "bithumb_1m.sqlite").resolve()
    assert Path(proc.stdout.strip()) == expected


def test_config_live_fee_rate_estimate_defaults_when_unset() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env.pop("LIVE_FEE_RATE_ESTIMATE", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.LIVE_FEE_RATE_ESTIMATE)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0025


def test_config_live_fee_rate_estimate_supports_env_override() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["LIVE_FEE_RATE_ESTIMATE"] = "0.0031"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.LIVE_FEE_RATE_ESTIMATE)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0031
