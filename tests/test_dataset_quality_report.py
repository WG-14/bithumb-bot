from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.db_core import ensure_db
from bithumb_bot.orderbook_depth_store import build_orderbook_depth_snapshot, upsert_orderbook_depth_snapshot
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot, build_dataset_quality_report, load_dataset_split
from bithumb_bot.research.experiment_manifest import DateRange, ManifestValidationError, parse_manifest
from bithumb_bot.research.promotion_gate import PromotionGateError, promote_candidate
from bithumb_bot.research.validation_protocol import run_research_backtest


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _create_db(path: Path, *, bad_row: tuple[int, float, float, float, float, float] | None = None) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE candles(
                ts INTEGER NOT NULL,
                pair TEXT NOT NULL,
                interval TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            )
            """
        )
        for day in ("2023-01-01", "2023-01-02"):
            for minute in range(24 * 60):
                close = 100.0 + float(minute % 10)
                row = (_ts(day, minute), close, close + 1.0, close - 1.0, close, 1.0)
                if bad_row is not None and day == "2023-01-01" and minute == bad_row[0]:
                    row = (_ts(day, minute), *bad_row[1:])
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        conn.commit()
    finally:
        conn.close()


def _manifest(interval: str = "1m"):
    return parse_manifest(
        {
            "experiment_id": "quality_unit",
            "hypothesis": "Dataset quality is explicit research evidence.",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": interval,
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "unit_snapshot",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 99,
                "min_profit_factor": 0.1,
                "oos_return_must_be_positive": False,
                "parameter_stability_required": False,
                "final_holdout_required_for_promotion": False,
            },
        }
    )


def _manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def test_dataset_quality_report_passes_complete_valid_candles(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    report = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert report.quality_gate_status == "PASS"
    assert report.payload["expected_candle_count"] == 1440
    assert report.payload["actual_candle_count"] == 1440
    assert report.payload["present_expected_bucket_count"] == 1440
    assert report.payload["coverage_pct"] == 100.0
    assert report.content_hash.startswith("sha256:")
    limitations = report.payload["limitations"]
    assert report.payload["depth_available"] is False
    assert limitations["execution_reference_price"] == "configured_by_execution_timing_policy"
    assert limitations["available_execution_reference_sources"] == [
        "candle_ohlcv",
        "top_of_book_if_requested",
    ]
    assert limitations["intra_candle_policy"] == "configured_by_execution_timing_policy"
    assert limitations["top_of_book_is_full_depth"] is False


def test_dataset_quality_depth_available_only_when_depth_rows_exist(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    without_depth = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
    assert without_depth.payload["depth_available"] is False
    assert without_depth.payload["l2_depth_rows_available"] is False
    assert without_depth.payload["l2_depth_complete_snapshots_available"] is False
    assert without_depth.payload["depth_evidence_available"] is False
    assert without_depth.payload["limitations"]["orderbook_depth_available"] is False

    conn = ensure_db(str(db_path))
    try:
        upsert_orderbook_depth_snapshot(
            conn,
            build_orderbook_depth_snapshot(
                ts=_ts("2023-01-01", 10),
                pair="KRW-BTC",
                bid_levels=[(100.0, 1.0)],
                ask_levels=[(101.0, 1.0)],
                source="bithumb_public_v1_orderbook",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    with_depth = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
    assert with_depth.payload["depth_available"] is True
    assert with_depth.payload["l2_depth_rows_available"] is True
    assert with_depth.payload["l2_depth_complete_snapshots_available"] is True
    assert with_depth.payload["depth_evidence_available"] is True
    assert with_depth.payload["limitations"]["orderbook_depth_available"] is True
    assert with_depth.payload["limitations"]["top_of_book_is_full_depth"] is False


def test_dataset_quality_report_detects_missing_candles_deterministically(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM candles WHERE ts IN (?, ?)", (_ts("2023-01-01", 10), _ts("2023-01-01", 11)))
        conn.commit()
    finally:
        conn.close()
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    first = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
    second = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert first.quality_gate_status == "FAIL"
    assert first.payload["missing_bucket_count"] == 2
    assert first.payload["missing_bucket_ranges"][0]["bucket_count"] == 2
    assert first.payload["quality_gate_reasons"] == ["missing_candles", "interval_mismatch"]
    assert first.content_hash == second.content_hash


def test_dataset_quality_coverage_uses_present_expected_buckets_not_raw_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM candles WHERE ts=?", (_ts("2023-01-01", 10),))
        conn.execute(
            """
            INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, 'KRW-BTC', '1m', 100.0, 101.0, 99.0, 100.0, 1.0)
            """,
            (_ts("2023-01-01", 0) + 30_000,),
        )
        conn.commit()
    finally:
        conn.close()
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    report = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert report.payload["actual_candle_count"] == 1440
    assert report.payload["present_expected_bucket_count"] == 1439
    assert report.payload["unexpected_bucket_count"] == 1
    assert report.payload["coverage_pct"] == round(1439 / 1440 * 100.0, 8)
    assert report.payload["coverage_pct"] < 100.0


def test_dataset_quality_coverage_never_exceeds_100_with_duplicate_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, 'KRW-BTC', '1m', 100.0, 101.0, 99.0, 100.0, 1.0)
            """,
            (_ts("2023-01-01", 0),),
        )
        conn.commit()
    finally:
        conn.close()
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    report = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert report.payload["actual_candle_count"] == 1441
    assert report.payload["present_expected_bucket_count"] == 1440
    assert report.payload["coverage_pct"] == 100.0
    assert "duplicate_candle_keys" in report.payload["quality_gate_reasons"]


def test_dataset_quality_report_detects_ohlc_non_positive_and_negative_volume(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path, bad_row=(4, 100.0, 99.0, 101.0, 0.0, -1.0))
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest(), split_name="train")

    report = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert report.quality_gate_status == "FAIL"
    assert report.payload["ohlc_violation_count"] == 1
    assert report.payload["non_positive_price_count"] == 1
    assert report.payload["negative_volume_count"] == 1
    assert "ohlc_invariant_violation" in report.payload["quality_gate_reasons"]
    assert "non_positive_price" in report.payload["quality_gate_reasons"]
    assert "negative_volume" in report.payload["quality_gate_reasons"]


def test_dataset_quality_report_rejects_unknown_interval(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    snapshot = load_dataset_split(db_path=db_path, manifest=_manifest("1h"), split_name="train")

    with pytest.raises(ManifestValidationError, match="unsupported dataset interval"):
        build_dataset_quality_report(db_path=db_path, snapshot=snapshot)


def test_dataset_quality_long_range_missing_diagnostics_are_bounded_and_deterministic(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    snapshot = DatasetSnapshot(
        snapshot_id="long",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2024-01-01"),
        candles=(
            Candle(ts=_ts("2023-01-01", 0), open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
        ),
    )

    first = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
    second = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert first.payload["expected_candle_count"] == 527040
    assert first.payload["actual_candle_count"] == 1
    assert first.payload["present_expected_bucket_count"] == 1
    assert len(first.payload["missing_bucket_sample"]) == 20
    assert len(first.payload["missing_bucket_ranges"]) <= 20
    assert first.content_hash == second.content_hash


def test_research_report_surfaces_quality_and_promotion_refuses_failed_quality(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "quality.sqlite"
    _create_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM candles WHERE ts=?", (_ts("2023-01-01", 10),))
        conn.commit()
    finally:
        conn.close()
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest()

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-07T00:00:00+00:00",
    )

    assert report["dataset_quality_gate_status"] == "FAIL"
    assert report["dataset_quality_hash"].startswith("sha256:")
    assert report["dataset_splits"]["train"]["quality_hash"].startswith("sha256:")
    assert report["candidates"][0]["acceptance_gate_result"] == "FAIL"
    assert any(reason.startswith("dataset_quality_train_") for reason in report["candidates"][0]["gate_fail_reasons"])
    with pytest.raises(PromotionGateError, match="standalone_backtest_not_full_validation"):
        promote_candidate(
            experiment_id="quality_unit",
            candidate_id=report["candidates"][0]["parameter_candidate_id"],
            manager=manager,
        )
