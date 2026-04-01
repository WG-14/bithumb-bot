from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def _apply_root_env(env: dict[str, str], tmp_path: Path) -> None:
    env["ENV_ROOT"] = str((tmp_path / "env").resolve())
    env["RUN_ROOT"] = str((tmp_path / "run").resolve())
    env["DATA_ROOT"] = str((tmp_path / "data").resolve())
    env["LOG_ROOT"] = str((tmp_path / "logs").resolve())
    env["BACKUP_ROOT"] = str((tmp_path / "backup").resolve())


def test_config_fail_fast_when_live_mode_missing_db_path(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["MODE"] = "live"
    _apply_root_env(env, tmp_path)
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


def test_config_keeps_paper_default_db_path_when_unset(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    _apply_root_env(env, tmp_path)
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
    assert Path(proc.stdout.strip()) == Path(env["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite"


def test_config_market_accepts_canonical_market_env() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["MARKET"] = "KRW-BTC"
    env.pop("PAIR", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.PAIR)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "KRW-BTC"


def test_config_live_market_accepts_canonical_market_env(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["MODE"] = "live"
    _apply_root_env(env, tmp_path)
    env["DB_PATH"] = str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve())
    env["MARKET"] = "KRW-BTC"
    env.pop("PAIR", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.PAIR)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "KRW-BTC"


def test_config_market_accepts_legacy_pair_alias() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env.pop("MARKET", None)
    env["PAIR"] = "BTC_KRW"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.PAIR)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "KRW-BTC"


def test_config_rejects_dryrun_mode_at_startup() -> None:
    env = dict(os.environ)
    env["MODE"] = "dryrun"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "invalid MODE='dryrun'" in (proc.stderr + proc.stdout)


def test_config_live_market_rejects_bare_symbol_pair_alias(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["MODE"] = "live"
    _apply_root_env(env, tmp_path)
    env["DB_PATH"] = str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve())
    env["PAIR"] = "BTC"
    env.pop("MARKET", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "invalid PAIR format for MODE=live" in (proc.stderr + proc.stdout)


def test_config_live_market_rejects_legacy_pair_alias(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["MODE"] = "live"
    _apply_root_env(env, tmp_path)
    env["DB_PATH"] = str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve())
    env["PAIR"] = "BTC_KRW"
    env.pop("MARKET", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "invalid PAIR format for MODE=live" in (proc.stderr + proc.stdout)


def test_config_market_prefers_market_over_pair_when_both_match() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["MARKET"] = "KRW-BTC"
    env["PAIR"] = "BTC_KRW"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.PAIR)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "KRW-BTC"


def test_config_market_rejects_flipped_legacy_alias_against_canonical_pair() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["MARKET"] = "KRW_BTC"
    env["PAIR"] = "KRW-BTC"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "invalid MARKET format" in (proc.stderr + proc.stdout)


def test_config_market_rejects_invalid_market_format_at_startup() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["MARKET"] = "KRW/BTC"
    env.pop("PAIR", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "invalid MARKET format" in (proc.stderr + proc.stdout)


def test_config_market_rejects_market_pair_mismatch() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["MARKET"] = "KRW-ETH"
    env["PAIR"] = "BTC_KRW"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "MARKET and PAIR resolve to different canonical markets" in (proc.stderr + proc.stdout)


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


def test_config_live_fee_rate_estimate_falls_back_to_legacy_fee_rate() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["FEE_RATE"] = "0.0017"
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
    assert float(proc.stdout.strip()) == 0.0017


def test_config_paper_fee_rate_reuses_live_estimate_by_default() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["LIVE_FEE_RATE_ESTIMATE"] = "0.0031"
    env.pop("PAPER_FEE_RATE", None)
    env.pop("PAPER_FEE_RATE_ESTIMATE", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.PAPER_FEE_RATE)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0031


def test_config_paper_fee_rate_supports_env_override() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["LIVE_FEE_RATE_ESTIMATE"] = "0.0031"
    env["PAPER_FEE_RATE"] = "0.0018"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.PAPER_FEE_RATE)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0018


def test_config_paper_fee_rate_falls_back_to_legacy_fee_rate() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["FEE_RATE"] = "0.0044"
    env.pop("PAPER_FEE_RATE", None)
    env.pop("PAPER_FEE_RATE_ESTIMATE", None)
    env.pop("LIVE_FEE_RATE_ESTIMATE", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.PAPER_FEE_RATE)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0044


def test_config_strategy_name_defaults_to_filtered_sma() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env.pop("STRATEGY_NAME", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_NAME)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "sma_with_filter"


def test_config_strategy_name_supports_legacy_override_to_sma_cross() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["STRATEGY_NAME"] = "sma_cross"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_NAME)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "sma_cross"


def test_config_strategy_name_normalizes_case_and_whitespace() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["STRATEGY_NAME"] = "  SMA_WITH_FILTER  "
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_NAME)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "sma_with_filter"


def test_config_entry_edge_buffer_ratio_defaults_when_unset() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env.pop("ENTRY_EDGE_BUFFER_RATIO", None)
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.ENTRY_EDGE_BUFFER_RATIO)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0005


def test_config_entry_edge_buffer_ratio_supports_env_override() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["ENTRY_EDGE_BUFFER_RATIO"] = "0.0013"
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.ENTRY_EDGE_BUFFER_RATIO)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0013


def test_config_strategy_min_expected_edge_ratio_defaults_and_override() -> None:
    env_default = dict(os.environ)
    env_default["MODE"] = "paper"
    env_default.pop("STRATEGY_MIN_EXPECTED_EDGE_RATIO", None)
    env_default["PYTHONPATH"] = "src"
    proc_default = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO)",
        ],
        env=env_default,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc_default.returncode == 0
    assert float(proc_default.stdout.strip()) == 0.0

    env_override = dict(os.environ)
    env_override["MODE"] = "paper"
    env_override["STRATEGY_MIN_EXPECTED_EDGE_RATIO"] = "0.0021"
    env_override["PYTHONPATH"] = "src"
    proc_override = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO)",
        ],
        env=env_override,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc_override.returncode == 0
    assert float(proc_override.stdout.strip()) == 0.0021


def test_config_strategy_exit_min_take_profit_ratio_defaults_and_override() -> None:
    env_default = dict(os.environ)
    env_default["MODE"] = "paper"
    env_default.pop("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", None)
    env_default["PYTHONPATH"] = "src"
    proc_default = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO)",
        ],
        env=env_default,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc_default.returncode == 0
    assert float(proc_default.stdout.strip()) == 0.0

    env_override = dict(os.environ)
    env_override["MODE"] = "paper"
    env_override["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"] = "0.0042"
    env_override["PYTHONPATH"] = "src"
    proc_override = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO)",
        ],
        env=env_override,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc_override.returncode == 0
    assert float(proc_override.stdout.strip()) == 0.0042


def test_config_float_env_blank_value_falls_back_to_default() -> None:
    env = dict(os.environ)
    env["MODE"] = "paper"
    env["ENTRY_EDGE_BUFFER_RATIO"] = ""
    env["PYTHONPATH"] = "src"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; print(c.settings.ENTRY_EDGE_BUFFER_RATIO)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert float(proc.stdout.strip()) == 0.0005
