from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.orderbook_top_store import build_orderbook_top_snapshot, upsert_orderbook_top_snapshot
from bithumb_bot.paths import PathManager
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot, build_dataset_quality_report, load_dataset_split
from bithumb_bot.research.execution_model import StressExecutionModel
from bithumb_bot.research.execution_timing import first_quote_after_or_equal
from bithumb_bot.research.experiment_manifest import DateRange, ExecutionTimingPolicy, ManifestValidationError, parse_manifest
from bithumb_bot.research.strategy_registry import TEST_TOP_OF_BOOK_REQUIRED_STRATEGY
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
    assert report["best_candidate_id"] is None
    assert report["gate_result"] == "FAIL"
    assert "dataset_quality_train_top_of_book_missing" in report["candidates"][0]["gate_fail_reasons"]
    assert report["dataset_quality_reports"]["train"]["top_of_book_gate_status"] == "FAIL"
    assert report["top_of_book_quality_summary"]["gate_status"] == "FAIL"
    assert report["top_of_book_quality_summary"]["fail_closed"] is True


def test_optional_top_of_book_missing_is_visible_warning_not_candidate_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    manifest = _manifest(
        top_of_book={
            "source": "sqlite_orderbook_top_snapshots",
            "required": False,
            "missing_policy": "warn",
            "join_tolerance_ms": 3000,
        }
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    summary = report["top_of_book_quality_summary"]
    assert report["dataset_quality_gate_status"] == "PASS"
    assert report["dataset_quality_reports"]["train"]["top_of_book_gate_status"] == "WARN"
    assert "top_of_book_optional_coverage_warning" in report["warnings"]
    assert "top_of_book_optional_coverage_warning" in candidate["warnings"]
    assert "dataset_quality_train_top_of_book_missing" not in candidate["gate_fail_reasons"]
    assert summary["gate_status"] == "WARN"
    assert summary["coverage_pct"] == 0.0
    assert summary["missing_quote_count"] == 2880
    assert [item["split_name"] for item in summary["affected_splits"]] == ["train", "validation"]
    assert summary["next_action"] == (
        "collect orderbook top snapshots with sync-orderbook-top, rerun research-backtest, "
        "and verify top_of_book_coverage_pct"
    )


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
    assert metadata[0]["reference_price"] in {98.0, 99.0, 100.0, 101.0, 102.0}
    assert report["dataset_quality_reports"]["train"]["top_of_book_gate_status"] == "PASS"
    assert report["data_limitations"]["top_of_book_available"] is True
    assert report["data_limitations"]["orderbook_depth_available"] is False
    assert report["data_limitations"]["execution_reference_price"] == "candle_close_legacy"


def test_strategy_requiring_top_of_book_fails_closed_when_manifest_lacks_it(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)

    with pytest.raises(ResearchValidationError, match="research_data_requirement_top_of_book_missing"):
        run_research_backtest(
            manifest=_manifest(strategy_name=TEST_TOP_OF_BOOK_REQUIRED_STRATEGY),
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


def test_orderbook_policy_uses_first_quote_after_decision_not_nearest_before() -> None:
    base_ts = 1_700_000_000_000
    signal_start = base_ts + 4 * 60_000
    decision_ts = signal_start + 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        quotes=(
            build_dataset_quote(candle_ts=decision_ts - 100, bid=70.0, ask=71.0),
            build_dataset_quote(candle_ts=decision_ts + 200, bid=90.0, ask=120.0),
        ),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    assert execution["quote_ts"] == decision_ts + 200
    assert execution["fill_reference_price"] == 120.0
    assert execution["fill_reference_source"] == "first_orderbook_after_decision"


def test_quote_lookup_preserves_first_after_or_equal_tie_break() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        quotes=(
            build_dataset_quote(candle_ts=base_ts + 10_000, bid=99.0, ask=101.0),
            build_dataset_quote(candle_ts=base_ts + 5_000, bid=98.0, ask=102.0),
            build_dataset_quote(candle_ts=base_ts + 5_000, bid=97.0, ask=103.0),
        ),
    )

    quote = first_quote_after_or_equal(dataset=dataset, target_ts=base_ts + 5_000, max_wait_ms=10_000)

    assert quote is not None
    assert quote.ts == base_ts + 5_000
    assert quote.ask_price == 102.0


def test_latency_changes_fill_reference_quote() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        quotes=(
            build_dataset_quote(candle_ts=decision_ts, bid=90.0, ask=100.0),
            build_dataset_quote(candle_ts=decision_ts + 800, bid=110.0, ask=130.0),
        ),
    )
    policy = ExecutionTimingPolicy(
        fill_reference_policy="latency_adjusted_orderbook",
        max_quote_wait_ms=2000,
        allow_same_candle_close_fill=False,
        source="test",
    )

    fast = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=StressExecutionModel(fee_rate=0.0, slippage_bps=0.0, latency_ms=0, seed=1),
        execution_timing_policy=policy,
    )
    slow = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=StressExecutionModel(fee_rate=0.0, slippage_bps=0.0, latency_ms=700, seed=1),
        execution_timing_policy=policy,
    )

    assert fast.trades[0]["execution"]["submit_ts_assumption"] == decision_ts
    assert fast.trades[0]["execution"]["quote_ts"] == decision_ts
    assert fast.trades[0]["execution"]["fill_reference_price"] == 100.0
    assert slow.trades[0]["execution"]["submit_ts_assumption"] == decision_ts + 700
    assert slow.trades[0]["execution"]["quote_ts"] == decision_ts + 800
    assert slow.trades[0]["execution"]["fill_reference_price"] == 130.0
    assert slow.trades[0]["execution"]["latency_applied_to_reference"] is True
    assert slow.trades[0]["execution"]["latency_reference_policy_warning"] is None


def test_latency_adjusted_orderbook_loads_quote_after_latency_plus_wait(tmp_path: Path) -> None:
    db_path = tmp_path / "quotes.sqlite"
    conn = ensure_db(str(db_path))
    base_ts = _ts("2023-01-01", 0)
    try:
        closes = [100.0, 90.0, 100.0, 80.0, 100.0]
        for index, close in enumerate(closes):
            conn.execute(
                """
                INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                """,
                (base_ts + index * 60_000, close, close + 1.0, close - 1.0, close),
            )
        signal_close_ts = base_ts + 5 * 60_000
        late_quote_ts = signal_close_ts + 10_000
        upsert_orderbook_top_snapshot(
            conn,
            build_orderbook_top_snapshot(
                ts=late_quote_ts,
                pair="KRW-BTC",
                bid_price=120.0,
                ask_price=140.0,
                source="bithumb_public_v1_orderbook",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    payload = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "missing_policy": "warn"}).raw
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": 0.0,
        "slippage_bps": 0.0,
        "latency_ms": 7000,
    }
    payload["execution_timing"] = {
        "fill_reference_policy": "latency_adjusted_orderbook",
        "max_quote_wait_ms": 5000,
        "missing_quote_policy": "fail",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(payload)

    snapshot = load_dataset_split(db_path=db_path, manifest=manifest, split_name="train")
    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=StressExecutionModel(fee_rate=0.0, slippage_bps=0.0, latency_ms=7000, seed=1),
        execution_timing_policy=manifest.execution_timing,
    )

    assert snapshot.top_of_book_event_quotes[-1].ts == late_quote_ts
    execution = result.trades[0]["execution"]
    assert execution["submit_ts_assumption"] == signal_close_ts + 7000
    assert execution["quote_ts"] == late_quote_ts
    assert execution["quote_age_ms"] == 3000
    assert execution["fill_reference_price"] == 140.0


def test_top_of_book_model_uses_ask_for_buy_bid_for_sell() -> None:
    base_ts = 1_700_000_000_000
    quotes = []
    for index in (4, 5, 6):
        decision_ts = base_ts + index * 60_000 + 60_000
        quotes.append(build_dataset_quote(candle_ts=decision_ts, bid=80.0, ask=120.0))
    dataset = _signal_dataset(base_ts=base_ts, quotes=tuple(quotes), closes=(100, 90, 100, 80, 100, 80, 100, 130))

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    buy = next(trade for trade in result.trades if trade["side"] == "BUY")
    sell = next(trade for trade in result.trades if trade["side"] == "SELL")
    assert buy["execution"]["fill_reference_price"] == 120.0
    assert buy["price"] == 120.0
    assert sell["execution"]["fill_reference_price"] == 80.0
    assert sell["price"] == 80.0


def test_execution_metadata_includes_reference_source_and_quote_age() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        quotes=(build_dataset_quote(candle_ts=decision_ts + 250, bid=90.0, ask=120.0),),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    for key in (
        "fill_reference_source",
        "fill_reference_ts",
        "decision_ts",
        "submit_ts_assumption",
        "execution_reality_level",
        "quote_age_ms",
        "quote_source",
    ):
        assert key in execution
    assert execution["quote_age_ms"] == 250
    assert execution["top_of_book_is_full_depth"] is False


def test_trade_top_level_records_signal_and_fill_timestamps_separately() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(base_ts=base_ts, quotes=())

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    trade = result.trades[0]
    assert trade["ts"] == base_ts + 4 * 60_000
    assert trade["event_ts_role"] == "signal_ts_legacy"
    assert trade["signal_ts"] == base_ts + 4 * 60_000
    assert trade["decision_ts"] == base_ts + 5 * 60_000
    assert trade["submit_ts_assumption"] == base_ts + 5 * 60_000
    assert trade["fill_ts"] == base_ts + 5 * 60_000
    assert trade["fill_reference_ts"] == base_ts + 5 * 60_000
    assert trade["fill_ts"] != trade["signal_ts"]


def test_next_open_fill_does_not_affect_signal_candle_close_equity() -> None:
    base_ts = 1_700_000_000_000
    closes = [100.0, 90.0, 100.0, 80.0, 100.0, 1.0]
    opens = [100.0, 90.0, 100.0, 80.0, 100.0, 1.0]
    dataset = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=tuple(
            Candle(
                ts=base_ts + index * 60_000,
                open=opens[index],
                high=max(opens[index], close) + 1.0,
                low=min(opens[index], close) - 1.0,
                close=close,
                volume=1.0,
            )
            for index, close in enumerate(closes)
        ),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    assert result.trades[0]["execution"]["fill_reference_ts"] == base_ts + 5 * 60_000
    assert result.metrics.max_drawdown_pct == 0.0


def test_latency_fill_after_next_candle_close_does_not_update_position_early() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        closes=(100, 90, 100, 80, 100, 1, 100, 130),
        quotes=(build_dataset_quote(candle_ts=decision_ts + 90_000, bid=100.0, ask=100.0),),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=120_000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    buy = result.trades[0]
    assert buy["fill_reference_ts"] == decision_ts + 90_000
    assert buy["portfolio_applied"] is True
    assert buy["record_type"] == "portfolio_trade"
    assert buy["is_execution_filled"] is True
    assert buy["is_portfolio_applied_trade"] is True
    assert buy["is_effective_trade"] is True
    assert buy["portfolio_application_status"] == "applied"
    assert buy["portfolio_effective_ts"] == decision_ts + 90_000
    assert result.metrics.max_drawdown_pct == 0.0


def test_pending_buy_fill_does_not_enable_sell_before_fill_ts() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        closes=(100, 90, 100, 80, 100, 70, 100, 130),
        quotes=(build_dataset_quote(candle_ts=decision_ts + 90_000, bid=95.0, ask=100.0),),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=120_000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    sell_before_buy_fill = [
        trade
        for trade in result.trades
        if trade["side"] == "SELL" and int(trade["decision_ts"]) < decision_ts + 90_000
    ]
    assert sell_before_buy_fill == []


def test_delayed_fill_crossing_mark_boundary_is_pending_not_early_applied() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        closes=(100, 90, 100, 80, 100, 1),
        quotes=(build_dataset_quote(candle_ts=decision_ts + 90_000, bid=100.0, ask=100.0),),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=120_000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    assert result.trades[0]["portfolio_applied"] is False
    assert result.trades[0]["record_type"] == "pending_execution"
    assert result.trades[0]["is_execution_filled"] is True
    assert result.trades[0]["is_portfolio_applied_trade"] is False
    assert result.trades[0]["is_effective_trade"] is False
    assert result.trades[0]["is_filled_trade"] is False
    assert result.trades[0]["portfolio_application_status"] == "pending"
    assert result.trades[0]["pending_execution_at_end"] is True
    assert result.trades[0]["pending_execution_after_dataset_end"] is True
    assert result.trades[0]["asset_qty"] == 0.0
    assert result.metrics.return_pct == 0.0
    assert result.metrics.max_drawdown_pct == 0.0
    assert result.execution_event_summary["pending_execution_at_end_count"] == 1
    assert result.execution_event_summary["pending_execution_after_dataset_end_count"] == 1
    assert result.execution_event_summary["execution_event_timeline_incomplete"] is True
    assert all(row.trade_count == 0 for row in result.regime_coverage)


def test_stress_latency_non_latency_policy_is_flagged_or_failed(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    raw = _manifest().raw
    raw["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [0.0],
        "latency_ms": [500],
    }
    raw["execution_timing"] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "candle_next_open",
    }
    manifest = parse_manifest(raw)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    summary = candidate["execution_reality_summary"]
    execution = candidate["scenario_results"][0]["validation_execution_metadata"][0]
    assert execution["latency_applied_to_reference"] is False
    assert execution["latency_applied_to_submit_ts"] is True
    assert execution["latency_applied_to_fill_reference"] is False
    assert execution["latency_reference_policy_warning"] == "execution_latency_not_applied_to_reference_policy"
    assert summary["execution_reality_gate_status"] == "FAIL"
    assert "execution_latency_not_applied_to_reference_policy" in candidate["gate_fail_reasons"]


def test_latency_submit_and_reference_application_are_reported_separately() -> None:
    base_ts = 1_700_000_000_000
    decision_ts = base_ts + 5 * 60_000
    dataset = _signal_dataset(
        base_ts=base_ts,
        quotes=(
            build_dataset_quote(candle_ts=decision_ts + 500, bid=90.0, ask=100.0),
            build_dataset_quote(candle_ts=decision_ts + 1_000, bid=95.0, ask=105.0),
        ),
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=StressExecutionModel(fee_rate=0.0, slippage_bps=0.0, latency_ms=900, seed=1),
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="latency_adjusted_orderbook",
            max_quote_wait_ms=2_000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    assert execution["submit_ts_assumption"] == decision_ts + 900
    assert execution["quote_ts"] == decision_ts + 1_000
    assert execution["latency_applied_to_submit_ts"] is True
    assert execution["latency_applied_to_fill_reference"] is True
    assert execution["latency_applied_to_reference"] is True


def test_missing_quote_policy_fail_fails_candidate(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    raw = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "missing_policy": "warn"}).raw
    raw["execution_timing"] = {
        "fill_reference_policy": "first_orderbook_after_decision",
        "max_quote_wait_ms": 1000,
        "missing_quote_policy": "fail",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(raw)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    execution = candidate["scenario_results"][0]["validation_execution_metadata"][0]
    assert report["gate_result"] == "FAIL"
    assert execution["fill_status"] == "failed"
    assert execution["execution_reference_failure_reason"] == "missing_quote_failed"
    assert "quote_after_decision_signal_coverage_below_threshold" in candidate["gate_fail_reasons"]


def test_missing_quote_policy_skip_records_skip_not_failed_fill() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(base_ts=base_ts, quotes=())

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            missing_quote_policy="skip",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    assert execution["fill_status"] == "skipped"
    assert execution["execution_reference_failure_reason"] == "missing_quote_skipped"
    assert execution["filled_qty"] == 0.0
    assert result.trades[0]["record_type"] == "skipped_execution"
    assert result.trades[0]["is_filled_trade"] is False
    assert result.trades[0]["is_execution_filled"] is False
    assert result.trades[0]["is_portfolio_applied_trade"] is False
    assert result.trades[0]["portfolio_application_status"] == "not_applicable"
    assert result.trades[0]["is_skipped_execution"] is True
    assert result.trades[0]["asset_qty"] == 0.0


def test_skipped_execution_attempt_is_not_counted_as_filled_trade() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(base_ts=base_ts, quotes=())

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            missing_quote_policy="skip",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    assert result.metrics.trade_count == 0
    assert all(row.trade_count == 0 for row in result.regime_coverage)
    assert all(row.trade_count == 0 for row in result.regime_performance)


def test_failed_execution_has_not_applicable_portfolio_application_status() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(base_ts=base_ts, quotes=())

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            missing_quote_policy="fail",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    trade = result.trades[0]
    assert trade["record_type"] == "failed_execution"
    assert trade["is_failed_execution"] is True
    assert trade["is_execution_filled"] is False
    assert trade["is_portfolio_applied_trade"] is False
    assert trade["portfolio_application_status"] == "not_applicable"


def test_missing_quote_policy_warn_records_warning_without_promotion_grade_pass(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    raw = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "missing_policy": "warn"}).raw
    raw["execution_timing"] = {
        "fill_reference_policy": "first_orderbook_after_decision",
        "max_quote_wait_ms": 1000,
        "missing_quote_policy": "warn",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "top_of_book_after_decision",
    }
    manifest = parse_manifest(raw)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    execution = candidate["scenario_results"][0]["validation_execution_metadata"][0]
    summary = report["signal_quote_coverage_summary"]
    assert execution["fill_status"] == "skipped_with_warning"
    assert execution["execution_reference_failure_reason"] == "missing_quote_warning"
    assert "missing_quote_warning" in report["warnings"]
    assert summary["signal_event_count"] > 0
    assert summary["skipped_execution_signal_count"] == summary["signal_event_count"]
    assert report["gate_result"] == "FAIL"
    assert "quote_after_decision_signal_coverage_below_threshold" in candidate["gate_fail_reasons"]


def test_report_separates_execution_attempt_count_from_closed_trade_count(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    raw = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "missing_policy": "warn"}).raw
    raw["execution_timing"] = {
        "fill_reference_policy": "first_orderbook_after_decision",
        "max_quote_wait_ms": 1000,
        "missing_quote_policy": "skip",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(raw)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    summary = report["candidates"][0]["execution_reality_summary"]
    assert summary["execution_attempt_count"] > 0
    assert summary["skipped_execution_count"] == summary["execution_attempt_count"]
    assert summary["execution_filled_count"] == 0
    assert summary["filled_execution_count"] == 0
    assert summary["portfolio_applied_trade_count"] == 0
    assert summary["pending_execution_count"] == 0
    assert summary["closed_trade_count"] == 0


def test_regime_coverage_does_not_treat_skipped_execution_as_filled_trade() -> None:
    base_ts = 1_700_000_000_000
    dataset = _signal_dataset(base_ts=base_ts, quotes=())

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            missing_quote_policy="skip",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    composite = [row for row in result.regime_coverage if row.dimension == "composite_regime"]
    assert composite
    assert all(row.trade_count == 0 for row in composite)


def test_signal_event_quote_coverage_reported(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quotes.sqlite"
    _create_candle_db(db_path)
    conn = ensure_db(str(db_path))
    try:
        upsert_orderbook_top_snapshot(
            conn,
            build_orderbook_top_snapshot(
                ts=_ts("2023-01-01", 6),
                pair="KRW-BTC",
                bid_price=90.0,
                ask_price=110.0,
                source="bithumb_public_v1_orderbook",
            ),
        )
        upsert_orderbook_top_snapshot(
            conn,
            build_orderbook_top_snapshot(
                ts=_ts("2023-01-02", 6),
                pair="KRW-BTC",
                bid_price=90.0,
                ask_price=110.0,
                source="bithumb_public_v1_orderbook",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    raw = _manifest(top_of_book={"source": "sqlite_orderbook_top_snapshots", "missing_policy": "warn"}).raw
    raw["execution_timing"] = {
        "fill_reference_policy": "first_orderbook_after_decision",
        "max_quote_wait_ms": 1000,
        "missing_quote_policy": "warn",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(raw)

    first = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=_manager(tmp_path, monkeypatch),
        generated_at="2026-05-07T00:00:00+00:00",
    )

    summary = first["signal_quote_coverage_summary"]
    assert summary["signal_event_count"] > 0
    assert "fillable_signal_event_count" in summary
    assert "missing_quote_on_signal_count" in summary
    assert "quote_after_decision_coverage_pct" in summary
    assert first["content_hash"] == second["content_hash"]


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


def _signal_dataset(
    *,
    base_ts: int,
    quotes,
    closes=(100, 90, 100, 80, 100, 130),
) -> DatasetSnapshot:
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=float(close),
            high=float(close) + 1.0,
            low=float(close) - 1.0,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    return DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=candles,
        top_of_book_event_quotes=tuple(quotes),
        top_of_book_requested=True,
    )
