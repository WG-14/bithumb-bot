from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.forward_diagnostics import (
    ForwardDiagnosticsResult,
    run_forward_diagnostics,
    run_forward_diagnostics_on_snapshot,
)
import bithumb_bot.research.forward_diagnostics as forward_diagnostics


ROOT = Path(__file__).resolve().parents[1]


def _snapshot() -> DatasetSnapshot:
    candles = tuple(
        Candle(ts=index, open=100 + index, high=102 + index, low=99 + index, close=101 + index, volume=10 + index)
        for index in range(30)
    )
    return DatasetSnapshot(
        snapshot_id="snapshot",
        source="test",
        market="BTC_KRW",
        interval="1m",
        split_name="train",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=candles,
    )


def test_forward_diagnostics_uses_dataset_snapshot_loader(monkeypatch) -> None:
    calls: list[object] = []

    def fake_loader(*, db_path, manifest, split_name):
        calls.append((db_path, manifest, split_name))
        return _snapshot()

    monkeypatch.setattr(forward_diagnostics, "load_dataset_split", fake_loader)
    manifest = SimpleNamespace(experiment_id="exp")

    payload = run_forward_diagnostics(
        manifest=manifest,
        db_path="/tmp/test.sqlite",
        split_name="train",
        feature_names=("rolling_return",),
        horizon_steps=(1,),
        bucket_method="quantile:10",
        min_bucket_count=1,
    )

    assert calls == [("/tmp/test.sqlite", manifest, "train")]
    assert payload["split_name"] == "train"


def test_forward_diagnostics_core_accepts_dataset_snapshot_without_db() -> None:
    result = run_forward_diagnostics_on_snapshot(
        snapshot=_snapshot(),
        feature_names=("rolling_return",),
        horizon_steps=(1,),
        bucket_method="quantile:10",
        min_bucket_count=1,
    )

    assert isinstance(result, ForwardDiagnosticsResult)
    assert result.sample_count > 0


def test_forward_diagnostics_files_do_not_import_sqlite3() -> None:
    for relative in (
        "src/bithumb_bot/research/forward_diagnostics.py",
        "src/bithumb_bot/research/forward_diagnostics_cli.py",
    ):
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "import sqlite3" not in source
        assert "sqlite3.connect" not in source


def test_forward_diagnostics_files_do_not_query_candles_table_directly() -> None:
    for relative in (
        "src/bithumb_bot/research/forward_diagnostics.py",
        "src/bithumb_bot/research/forward_diagnostics_cli.py",
    ):
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "SELECT ts, open, high, low, close, volume" not in source
        assert "FROM candles" not in source
