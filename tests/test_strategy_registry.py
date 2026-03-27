from __future__ import annotations

from pathlib import Path
import os

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import compute_signal
from bithumb_bot.strategy import create_strategy, list_strategies


def test_registry_default_strategy_available() -> None:
    assert "sma_cross" in list_strategies()


def test_compute_signal_uses_default_strategy_name_from_settings(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_strategy_name = settings.STRATEGY_NAME
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "strategy_default.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_cross")

    conn = ensure_db()
    base_ts = 1_700_000_000_000
    try:
        for idx, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0]):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = compute_signal(conn, 2, 3)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy_name)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is not None
    assert result["signal"] in {"BUY", "SELL", "HOLD"}
    assert result["strategy"] == "sma_cross"
    assert "reason" in result


def test_registry_rejects_unknown_strategy_name() -> None:
    with pytest.raises(ValueError, match="unknown strategy"):
        create_strategy("does_not_exist")


def test_engine_no_direct_sma_import() -> None:
    engine_source = Path("src/bithumb_bot/engine.py").read_text()
    assert "from .strategy.sma import" not in engine_source
