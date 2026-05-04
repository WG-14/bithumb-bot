from __future__ import annotations

import json

import pytest

from bithumb_bot.app import cmd_report, main
from bithumb_bot.db_core import ensure_db, record_strategy_decision
from bithumb_bot.config import settings
from bithumb_bot.execution_quality import (
    ExecutionQualityThresholds,
    build_execution_quality_record,
    latency_ms,
    side_aware_slippage_bps,
    summarize_execution_quality,
)
from bithumb_bot.execution import apply_fill_and_trade
from bithumb_bot.oms import add_fill, create_order, record_submit_attempt


def test_fill_slippage_bps_calculated_from_submit_reference_price(tmp_path) -> None:
    db_path = tmp_path / "execution_quality_calc.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_exec_buy",
            submit_attempt_id="attempt_exec_buy",
            side="BUY",
            qty_req=0.01,
            price=None,
            status="NEW",
            ts_ms=1_700_000_000_000,
            conn=conn,
        )
        record_submit_attempt(
            conn=conn,
            client_order_id="o_exec_buy",
            submit_attempt_id="attempt_exec_buy",
            symbol="BTC_KRW",
            side="BUY",
            qty=0.01,
            price=100_000_000.0,
            submit_ts=1_700_000_000_100,
            payload_fingerprint="hash",
            broker_response_summary="ok",
            submission_reason_code="confirmed_success",
            exception_class=None,
            timeout_flag=False,
            submit_evidence=None,
            exchange_order_id_obtained=True,
            order_status="NEW",
        )
        add_fill(
            client_order_id="o_exec_buy",
            fill_id="f_exec_buy",
            fill_ts=1_700_000_000_200,
            price=100_100_000.0,
            qty=0.01,
            fee=100.0,
            conn=conn,
        )

        row = conn.execute(
            "SELECT reference_price, slippage_bps FROM fills WHERE client_order_id='o_exec_buy'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert float(row["reference_price"]) == 100_000_000.0
    assert abs(float(row["slippage_bps"]) - 10.0) < 1e-9


def test_fill_slippage_handles_missing_reference_price_safely(tmp_path) -> None:
    db_path = tmp_path / "execution_quality_missing_ref.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_exec_missing",
            side="SELL",
            qty_req=0.02,
            price=None,
            status="NEW",
            ts_ms=1_700_000_100_000,
            conn=conn,
        )
        add_fill(
            client_order_id="o_exec_missing",
            fill_id="f_exec_missing",
            fill_ts=1_700_000_100_100,
            price=99_900_000.0,
            qty=0.02,
            fee=0.0,
            conn=conn,
        )

        row = conn.execute(
            "SELECT reference_price, slippage_bps FROM fills WHERE client_order_id='o_exec_missing'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["reference_price"] is None
    assert row["slippage_bps"] is None


def test_report_prints_execution_quality_aggregate(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "execution_quality_report.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))

    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_exec_seed_buy",
            side="BUY",
            qty_req=0.03,
            price=None,
            status="NEW",
            ts_ms=1_700_000_199_900,
            conn=conn,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="o_exec_seed_buy",
            side="BUY",
            fill_id="f_exec_seed_buy",
            fill_ts=1_700_000_199_950,
            price=1_000.0,
            qty=0.03,
            fee=0.0,
        )
        create_order(
            client_order_id="o_exec_report",
            submit_attempt_id="attempt_exec_report",
            side="SELL",
            qty_req=0.03,
            price=None,
            status="NEW",
            ts_ms=1_700_000_200_000,
            conn=conn,
        )
        record_submit_attempt(
            conn=conn,
            client_order_id="o_exec_report",
            submit_attempt_id="attempt_exec_report",
            symbol="BTC_KRW",
            side="SELL",
            qty=0.03,
            price=101_000_000.0,
            submit_ts=1_700_000_200_050,
            payload_fingerprint="hash-report",
            broker_response_summary="ok",
            submission_reason_code="confirmed_success",
            exception_class=None,
            timeout_flag=False,
            submit_evidence=None,
            exchange_order_id_obtained=True,
            order_status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="o_exec_report",
            side="SELL",
            fill_id="f_exec_report",
            fill_ts=1_700_000_200_100,
            price=100_899_000.0,
            qty=0.03,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    cmd_report(days=3650)
    out = capsys.readouterr().out

    assert "[EXECUTION-QUALITY]" in out
    assert "fills=2 measured=1" in out
    assert "avg_slippage_bps=10.000" in out


def test_side_aware_slippage_and_top_of_book_math() -> None:
    assert side_aware_slippage_bps(side="BUY", reference_price=100.0, fill_price=101.0) == pytest.approx(100.0)
    assert side_aware_slippage_bps(side="SELL", reference_price=100.0, fill_price=99.0) == pytest.approx(100.0)
    assert side_aware_slippage_bps(side="BUY", reference_price=0.0, fill_price=101.0) is None
    assert side_aware_slippage_bps(side="SELL", reference_price=None, fill_price=99.0) is None


def test_latency_calculation_handles_missing_and_ordering() -> None:
    assert latency_ms(start_ms=1000, end_ms=1250) == 250
    assert latency_ms(start_ms=1000, end_ms=1600) == 600
    assert latency_ms(start_ms=None, end_ms=1600) is None
    assert latency_ms(start_ms=1600, end_ms=1000) is None


def _seed_quality_order(
    conn,
    *,
    client_order_id: str,
    side: str = "BUY",
    order_type: str = "market",
    decision_price: float = 100.0,
    submit_reference: float = 100.0,
    fill_prices: tuple[float, ...] = (101.0,),
    fill_qtys: tuple[float, ...] = (0.5,),
    qty_req: float = 1.0,
    request_ts: int = 1_700_000_000_100,
    response_ts: int = 1_700_000_000_180,
) -> int:
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY" if side == "BUY" else "SELL",
        reason="test",
        candle_ts=1_699_999_940_000,
        market_price=decision_price,
        context={
            "top_of_book": {
                "best_bid": 99.0,
                "best_ask": 100.0,
            }
        },
    )
    create_order(
        client_order_id=client_order_id,
        submit_attempt_id=f"{client_order_id}_attempt",
        symbol="KRW-BTC",
        mode="live",
        side=side,
        qty_req=qty_req,
        price=None,
        strategy_name="sma_with_filter",
        entry_decision_id=decision_id if side == "BUY" else None,
        exit_decision_id=decision_id if side == "SELL" else None,
        order_type=order_type,
        status="NEW",
        ts_ms=1_700_000_000_050,
        conn=conn,
    )
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=f"{client_order_id}_attempt",
        symbol="KRW-BTC",
        side=side,
        qty=qty_req,
        price=submit_reference,
        submit_ts=1_700_000_000_060,
        payload_fingerprint=f"{client_order_id}_hash",
        broker_response_summary="ok",
        submission_reason_code="confirmed_success",
        exception_class=None,
        timeout_flag=False,
        submit_evidence=json.dumps(
            {
                "request_ts": request_ts,
                "response_ts": response_ts,
                "top_of_book": {"best_bid": 99.0, "best_ask": 100.0},
            },
            sort_keys=True,
        ),
        exchange_order_id_obtained=True,
        order_status="NEW",
        order_type=order_type,
    )
    for index, (price, qty) in enumerate(zip(fill_prices, fill_qtys, strict=True), start=1):
        add_fill(
            client_order_id=client_order_id,
            fill_id=f"{client_order_id}_fill_{index}",
            fill_ts=1_700_000_000_200 + index,
            price=price,
            qty=qty,
            fee=0.01,
            conn=conn,
        )
    return decision_id


def test_order_level_execution_quality_aggregates_fills_and_latency(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-order.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_multi",
            fill_prices=(100.0, 102.0),
            fill_qtys=(0.25, 0.75),
            qty_req=1.0,
        )
        record = build_execution_quality_record(conn, client_order_id="quality_multi", backtest_assumed_slippage_bps=250.0)
    finally:
        conn.close()

    assert record is not None
    assert record.avg_fill_price == pytest.approx(101.5)
    assert record.filled_qty == pytest.approx(1.0)
    assert record.fill_ratio == pytest.approx(1.0)
    assert record.partial_fill_flag is False
    assert record.unfilled_flag is False
    assert record.response_latency_ms == 80
    assert record.first_fill_latency_ms == 101
    assert record.full_fill_latency_ms == 102
    assert record.slippage_vs_signal_bps == pytest.approx(150.0)
    assert record.slippage_vs_submit_ref_bps == pytest.approx(150.0)
    assert record.slippage_vs_best_quote_bps == pytest.approx(150.0)
    assert record.quality_status == "within_model"


def test_order_level_execution_quality_partial_and_unfilled(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-partial.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_partial",
            fill_prices=(101.0,),
            fill_qtys=(0.25,),
            qty_req=1.0,
        )
        _seed_quality_order(
            conn,
            client_order_id="quality_unfilled",
            fill_prices=(),
            fill_qtys=(),
            qty_req=1.0,
        )
        partial = build_execution_quality_record(conn, client_order_id="quality_partial")
        unfilled = build_execution_quality_record(conn, client_order_id="quality_unfilled")
    finally:
        conn.close()

    assert partial is not None
    assert partial.fill_ratio == pytest.approx(0.25)
    assert partial.partial_fill_flag is True
    assert unfilled is not None
    assert unfilled.unfilled_flag is True
    assert unfilled.quality_status == "insufficient_evidence"
    assert "unfilled" in unfilled.quality_reason


def test_manifest_comparison_summary_classifies_breaches(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-manifest.sqlite"))
    try:
        _seed_quality_order(conn, client_order_id="quality_inside", fill_prices=(100.1,), fill_qtys=(1.0,))
        _seed_quality_order(conn, client_order_id="quality_breach", fill_prices=(101.0,), fill_qtys=(1.0,))
        inside = build_execution_quality_record(conn, client_order_id="quality_inside", backtest_assumed_slippage_bps=20.0)
        breach = build_execution_quality_record(conn, client_order_id="quality_breach", backtest_assumed_slippage_bps=20.0)
    finally:
        conn.close()

    assert inside is not None
    assert breach is not None
    summary = summarize_execution_quality(
        [inside],
        thresholds=ExecutionQualityThresholds(min_sample=1, max_model_breach_rate=0.5),
        backtest_slippage_bps_max=20.0,
    )
    assert summary["quality_gate_status"] == "PASS"
    summary = summarize_execution_quality(
        [inside, breach],
        thresholds=ExecutionQualityThresholds(min_sample=1, max_model_breach_rate=0.1),
        backtest_slippage_bps_max=20.0,
    )
    assert summary["model_breach_count"] == 1
    assert summary["quality_gate_status"] == "FAIL"


def test_execution_quality_report_cli_no_records(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "execution-quality-empty.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "LIVE_EXECUTION_QUALITY_MIN_SAMPLE", 30)
    ensure_db(str(db_path)).close()

    rc = main(["execution-quality-report", "--limit", "10"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "sample_count=0" in out
    assert "quality_gate_status=INSUFFICIENT_EVIDENCE" in out


def test_execution_quality_report_cli_json_and_persistence(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "execution-quality-cli.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "LIVE_EXECUTION_QUALITY_MIN_SAMPLE", 1)
    conn = ensure_db(str(db_path))
    try:
        _seed_quality_order(conn, client_order_id="quality_cli", fill_prices=(101.0,), fill_qtys=(1.0,))
        conn.commit()
    finally:
        conn.close()

    rc = main(["execution-quality-report", "--limit", "10", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["sample_count"] == 1
    assert payload["quality_gate_status"] == "FAIL"

    conn = ensure_db(str(db_path))
    try:
        row = conn.execute(
            "SELECT client_order_id, slippage_vs_signal_bps FROM execution_quality_events WHERE client_order_id='quality_cli'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert float(row["slippage_vs_signal_bps"]) == pytest.approx(100.0)
