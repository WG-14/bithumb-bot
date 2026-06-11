from __future__ import annotations

import inspect
from pathlib import Path

from bithumb_bot.paths import PathManager
from bithumb_bot.research.dataset_snapshot import Candle, DatasetQualityReport, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange, parse_manifest
from bithumb_bot.research import execution_plan, validation_protocol
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_backtest_reproducibility import _manifest


def _snapshot(split_name: str) -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id=f"snapshot_{split_name}",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name=split_name,
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=(
            Candle(ts=1_700_000_000_000, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
            Candle(ts=1_700_000_060_000, open=101.0, high=102.0, low=100.0, close=101.0, volume=1.0),
        ),
    )


def _quality_report(split_name: str) -> DatasetQualityReport:
    return DatasetQualityReport(
        {
            "split_name": split_name,
            "content_hash": f"sha256:quality-{split_name}",
            "quality_gate_status": "PASS",
            "quality_gate_reasons": [],
        }
    )


def _manager(tmp_path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def test_build_research_work_units_reuse_precomputed_dataset_hashes(tmp_path, monkeypatch) -> None:
    payload = _manifest()
    payload["parameter_space"]["SMA_SHORT"] = [2, 3, 4, 5, 6]
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": [0.0, 1.0],
        "latency_ms": 0,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "market_order_extra_cost_bps": 0.0,
    }
    manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    call_count = 0
    original = DatasetSnapshot.content_hash

    def counted(self):
        nonlocal call_count
        call_count += 1
        return original(self)

    monkeypatch.setattr(DatasetSnapshot, "content_hash", counted)

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        progress_callback=lambda event: None,
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    assert len(result.candidates) == 5
    assert call_count <= len(snapshots)


def test_build_research_work_unit_does_not_call_snapshot_content_hash() -> None:
    source = inspect.getsource(execution_plan.build_research_work_unit)

    assert ".content_hash(" not in source
