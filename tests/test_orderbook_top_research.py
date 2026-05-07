from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.orderbook_top_store import build_orderbook_top_snapshot, upsert_orderbook_top_snapshot
from bithumb_bot.paths import PathManager
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot, build_dataset_quality_report, load_dataset_split
from bithumb_bot.research.experiment_manifest import DateRange, ManifestValidationError, parse_manifest
from bithumb_bot.research.validation_protocol import ResearchValidationError, run_research_backtest


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _manifest(*, top_of_book: dict[str, object] | None = None, strategy_name: str = "sma_with_filter"):
    dataset: dict[str, object] = {
        "source": "sqlite_candles",
        "snapshot_id": "quotes_unit",
        "train": {"start": "2023-01-01", "end": "2023-01-01"},
        "validation": {"start": "2023-01-02", "end": "2023-01-02"},
    }
    if top_of_book is not None:
        dataset["top_of_book"] = top_of_book
    return parse_manifest(
        {
            "experiment_id": "quotes_unit",
            "hypothesis": "Top-of-book quote joins are explicit research evidence.",
            "strategy_name": strategy_name,
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": dataset,
            "parameter_space": {"SMA_SHORT": [1], "SMA_LONG": [2]},
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


def _create_candle_db(path: Path) -> None:
    conn = ensure_db(str(path))
    try:
        for day in ("2023-01-01", "2023-01-02"):
            pattern = [100.0, 99.0, 101.0, 98.0]
            for minute in range(24 * 60):
                close = pattern[minute % len(pattern)]
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, minute), close, close + 1.0, close - 1.0, close),
                )
        conn.commit()
    finally:
        conn.close()


def _manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def test_top_of_book_join_uses_nearest_snapshot_with_deterministic_tie_break(tmp_path: Path) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    conn = ensure_db(str(db_path))
    try:
        candle_ts = _ts("2023-01-01", 0)
        for quote_ts, bid, ask in (
            (candle_ts + 1_000, 100.0, 101.0),
            (candle_ts - 1_000, 99.0, 100.0),
        ):
            upsert_orderbook_top_snapshot(
                conn,
                build_orderbook_top_snapshot(
                    ts=quote_ts,
                    pair="KRW-BTC",
                    bid_price=bid,
                    ask_price=ask,
                    source="bithumb_public_v1_orderbook",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    snapshot = load_dataset_split(
        db_path=db_path,
        manifest=_manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "join_tolerance_ms": 3000}),
        split_name="train",
    )

    assert snapshot.top_of_book_quotes[0] is not None
    assert snapshot.top_of_book_quotes[0].ts == candle_ts - 1_000
    assert snapshot.top_of_book_quotes[0].bid_price == 99.0


def test_quote_coverage_fields_are_deterministic_when_top_of_book_requested(tmp_path: Path) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    manifest = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "join_tolerance_ms": 3000})
    snapshot = load_dataset_split(db_path=db_path, manifest=manifest, split_name="train")

    first = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
    second = build_dataset_quality_report(db_path=db_path, snapshot=snapshot)

    assert first.payload["top_of_book_requested"] is True
    assert first.payload["top_of_book_required"] is False
    assert first.payload["top_of_book_join_tolerance_ms"] == 3000
    assert first.payload["top_of_book_expected_signal_count"] == 1440
    assert first.payload["top_of_book_joined_count"] == 0
    assert first.payload["top_of_book_missing_count"] == 1440
    assert first.payload["top_of_book_missing_sample"] == [_ts("2023-01-01", minute) for minute in range(20)]
    assert first.payload["top_of_book_gate_status"] == "WARN"
    assert first.quality_gate_status == "PASS"
    assert first.content_hash == second.content_hash


def test_required_top_of_book_missing_fails_dataset_quality_and_candidate_gate(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    manifest = _manifest(
        top_of_book={
            "source": "sqlite_orderbook_top_snapshots",
            "required": True,
            "join_tolerance_ms": 3000,
        }
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    assert report["dataset_quality_gate_status"] == "FAIL"
    assert "dataset_quality_train_top_of_book_missing" in report["candidates"][0]["gate_fail_reasons"]
    assert report["dataset_quality_reports"]["train"]["top_of_book_gate_status"] == "FAIL"


def test_research_backtest_metadata_includes_joined_top_of_book(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    conn = ensure_db(str(db_path))
    try:
        for day in ("2023-01-01", "2023-01-02"):
            for minute in range(24 * 60):
                upsert_orderbook_top_snapshot(
                    conn,
                    build_orderbook_top_snapshot(
                        ts=_ts(day, minute),
                        pair="KRW-BTC",
                        bid_price=99.0,
                        ask_price=101.0,
                        source="bithumb_public_v1_orderbook",
                    ),
                )
        conn.commit()
    finally:
        conn.close()
    manifest = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "join_tolerance_ms": 3000})

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    metadata = report["candidates"][0]["scenario_results"][0]["train_execution_metadata"]
    assert metadata
    assert metadata[0]["best_bid"] == 99.0
    assert metadata[0]["best_ask"] == 101.0
    assert metadata[0]["spread_bps"] == 200.0
    assert report["dataset_quality_reports"]["train"]["top_of_book_gate_status"] == "PASS"
    assert report["data_limitations"]["top_of_book_available"] is True
    assert report["data_limitations"]["orderbook_depth_available"] is False


def test_strategy_requiring_top_of_book_fails_closed_when_manifest_lacks_it(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)

    with pytest.raises(ResearchValidationError, match="research_data_requirement_top_of_book_missing"):
        run_research_backtest(
            manifest=_manifest(strategy_name="top_of_book_required_test"),
            db_path=db_path,
            manager=_manager(tmp_path, monkeypatch),
        )


def test_sma_backtest_execution_metadata_includes_joined_quote_fields() -> None:
    candles = (
        Candle(ts=1, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
        Candle(ts=2, open=101.0, high=102.0, low=100.0, close=101.0, volume=1.0),
        Candle(ts=3, open=99.0, high=100.0, low=98.0, close=99.0, volume=1.0),
        Candle(ts=4, open=102.0, high=103.0, low=101.0, close=102.0, volume=1.0),
        Candle(ts=5, open=98.0, high=99.0, low=97.0, close=98.0, volume=1.0),
    )
    dataset = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=candles,
        top_of_book_quotes=(
            None,
            None,
            None,
            build_dataset_quote(candle_ts=4, bid=100.0, ask=102.0),
            build_dataset_quote(candle_ts=5, bid=97.0, ask=99.0),
        ),
        top_of_book_requested=True,
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert len(result.trades) == 2
    assert result.trades[0]["execution"]["best_bid"] == 100.0
    assert result.trades[0]["execution"]["best_ask"] == 102.0
    assert result.trades[0]["execution"]["spread_bps"] == pytest.approx((2.0 / 101.0) * 10_000.0)
    assert result.trades[1]["execution"]["best_bid"] == 97.0


def test_candle_only_execution_metadata_records_no_quote_data() -> None:
    dataset = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=(
            Candle(ts=1, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
            Candle(ts=2, open=101.0, high=102.0, low=100.0, close=101.0, volume=1.0),
            Candle(ts=3, open=99.0, high=100.0, low=98.0, close=99.0, volume=1.0),
            Candle(ts=4, open=102.0, high=103.0, low=101.0, close=102.0, volume=1.0),
        ),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.trades[0]["execution"]["best_bid"] is None
    assert result.trades[0]["execution"]["best_ask"] is None
    assert result.trades[0]["execution"]["spread_bps"] is None
    assert result.trades[0]["execution"]["intra_candle_policy"] == "close_price_only_no_intracandle_path"


def test_manifest_rejects_unknown_dataset_and_top_of_book_fields() -> None:
    payload = _manifest().raw
    payload["dataset"]["unexpected"] = True
    with pytest.raises(ManifestValidationError, match="dataset unsupported fields"):
        parse_manifest(payload)

    payload = _manifest().raw
    payload["dataset"]["top_of_book"] = {"source": "sqlite_orderbook_top_snapshots", "unknown": True}
    with pytest.raises(ManifestValidationError, match="dataset.top_of_book unsupported fields"):
        parse_manifest(payload)


def build_dataset_quote(*, candle_ts: int, bid: float, ask: float):
    from bithumb_bot.research.dataset_snapshot import TopOfBookQuote

    mid = (bid + ask) / 2.0
    return TopOfBookQuote(
        ts=candle_ts,
        pair="KRW-BTC",
        bid_price=bid,
        ask_price=ask,
        spread_bps=((ask - bid) / mid) * 10_000.0,
        source="bithumb_public_v1_orderbook",
        matched_candle_ts=candle_ts,
        age_ms=0,
    )
