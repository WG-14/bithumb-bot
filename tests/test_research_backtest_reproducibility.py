from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from bithumb_bot.paths import PathManager
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import run_research_backtest


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE candles(
                ts INTEGER PRIMARY KEY,
                pair TEXT,
                interval TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            closes = [100, 99, 98, 99, 101, 103, 102, 100, 98, 97, 99, 102, 104, 103]
            for index, close in enumerate(closes):
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close * 1.01, close * 0.99, close),
                )
        conn.commit()
    finally:
        conn.close()


def _manifest() -> dict[str, object]:
    return {
        "experiment_id": "deterministic_sma",
        "hypothesis": "SMA candidate remains deterministic across repeated research runs.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "unit_candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
        },
    }


def test_same_manifest_and_dataset_produce_same_content_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_manifest())

    first = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert first["content_hash"] == second["content_hash"]
    assert first["candidates"][0]["candidate_profile_hash"] == second["candidates"][0]["candidate_profile_hash"]
    assert first["candidates"][0]["regime_classifier_version"] == "market_regime_v2"
    assert first["candidates"][0]["market_regime_bucket_performance"]
    assert first["candidates"][0]["market_regime_coverage"]
    assert "regime_gate_result" in first["candidates"][0]
    assert Path(first["artifact_paths"]["report_path"]).exists()


def test_sma_backtest_attaches_entry_and_exit_regime_snapshots() -> None:
    candles = tuple(
        Candle(
            ts=1_700_000_000_000 + index * 60_000,
            open=float(close),
            high=float(close) * 1.02,
            low=float(close) * 0.98,
            close=float(close),
            volume=float(100 + index * 10),
        )
        for index, close in enumerate([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0, "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    closed = [trade for trade in result.trades if trade["side"] == "SELL"]
    assert closed
    assert closed[0]["entry_regime"]
    assert closed[0]["exit_regime"]
    assert isinstance(closed[0]["entry_regime_snapshot"], dict)
    assert isinstance(closed[0]["exit_regime_snapshot"], dict)
    assert result.regime_performance
    assert result.regime_coverage
