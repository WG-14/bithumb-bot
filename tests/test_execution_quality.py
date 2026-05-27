from __future__ import annotations

import json
import sqlite3

import pytest

from bithumb_bot.cli.main import main
from bithumb_bot.operator_commands import cmd_report
from bithumb_bot.db_core import ensure_db, record_strategy_decision
from bithumb_bot.config import settings
from bithumb_bot.execution_quality import (
    ExecutionQualityThresholds,
    assess_execution_remainder,
    build_execution_quality_record,
    format_execution_quality_text,
    latency_ms,
    refresh_execution_quality_records,
    side_aware_slippage_bps,
    summarize_execution_quality,
)
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.order_semantics import classify_order_semantics
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


def test_canonical_order_semantics_side_aware() -> None:
    buy_price = classify_order_semantics(
        raw_order_type="price",
        side="BUY",
        exchange="bithumb",
        submit_contract_kind="market_buy_notional",
    )
    assert buy_price.canonical_execution_kind == "market_buy_quote_notional"
    assert buy_price.semantic_evidence_quality == "current_verified"
    assert buy_price.market_equivalent is True
    assert buy_price.legacy_unknown is False

    legacy_buy_price = classify_order_semantics(raw_order_type="price", side="BUY")
    assert legacy_buy_price.canonical_execution_kind == "market_buy_quote_notional"
    assert legacy_buy_price.semantic_evidence_quality == "legacy_unverified"
    assert legacy_buy_price.market_equivalent is True
    assert legacy_buy_price.unsupported_unknown is False

    conflicting_buy_price = classify_order_semantics(
        raw_order_type="price",
        side="BUY",
        exchange="bithumb",
        submit_contract_kind="limit_qty_price",
    )
    assert conflicting_buy_price.canonical_execution_kind == "unsupported_unknown"
    assert conflicting_buy_price.semantic_evidence_quality == "conflicting"
    assert conflicting_buy_price.market_equivalent is False
    assert conflicting_buy_price.unsupported_unknown is True

    sell_market = classify_order_semantics(raw_order_type="market", side="SELL")
    assert sell_market.canonical_execution_kind == "market_sell_base_qty"
    assert sell_market.market_equivalent is True

    limit = classify_order_semantics(raw_order_type="limit", side="BUY")
    assert limit.canonical_execution_kind == "limit_qty_price"
    assert limit.limit_equivalent is True

    legacy = classify_order_semantics(raw_order_type=None, side="BUY")
    assert legacy.canonical_execution_kind == "legacy_unknown"
    assert legacy.legacy_unknown is True

    invalid_buy_market = classify_order_semantics(raw_order_type="market", side="BUY", exchange="bithumb")
    assert invalid_buy_market.canonical_execution_kind == "unsupported_unknown"
    assert invalid_buy_market.semantic_evidence_quality == "conflicting"
    assert invalid_buy_market.market_equivalent is False

    unsupported = classify_order_semantics(raw_order_type="post_only", side="BUY")
    assert unsupported.canonical_execution_kind == "unsupported_unknown"
    assert unsupported.unsupported_unknown is True


def test_execution_remainder_materiality_rules() -> None:
    tiny = assess_execution_remainder(
        requested_qty=0.0006,
        filled_qty=0.00059988,
        remaining_qty=0.00000012,
        reference_price=100_000_000.0,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert tiny.is_material_remaining is False
    assert tiny.materiality_reason == "remaining_qty_below_qty_step"

    material = assess_execution_remainder(
        requested_qty=0.002,
        filled_qty=0.0005,
        remaining_qty=0.0015,
        reference_price=100_000_000.0,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert material.is_material_remaining is True

    one_step = assess_execution_remainder(
        requested_qty=0.0011,
        filled_qty=0.0010,
        remaining_qty=0.0001,
        reference_price=100_000_000.0,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert one_step.is_material_remaining is True
    assert one_step.materiality_reason == "material_executable_remaining_qty"

    below_min_qty = assess_execution_remainder(
        requested_qty=0.001,
        filled_qty=0.00095,
        remaining_qty=0.00005,
        reference_price=100_000_000.0,
        qty_step=None,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert below_min_qty.is_material_remaining is False
    assert below_min_qty.materiality_reason == "remaining_qty_below_effective_min_trade_qty"

    below_min_notional = assess_execution_remainder(
        requested_qty=0.001,
        filled_qty=0.0008,
        remaining_qty=0.0002,
        reference_price=10_000_000.0,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert below_min_notional.is_material_remaining is False
    assert below_min_notional.materiality_reason == "remaining_notional_below_min_notional_krw"

    missing_reference = assess_execution_remainder(
        requested_qty=0.001,
        filled_qty=0.0008,
        remaining_qty=0.0002,
        reference_price=None,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert missing_reference.is_material_remaining is True
    assert missing_reference.materiality_reason == "material_executable_remaining_qty_notional_unknown"

    unfilled = assess_execution_remainder(
        requested_qty=0.001,
        filled_qty=0.0,
        remaining_qty=0.001,
        reference_price=100_000_000.0,
        qty_step=0.0001,
        effective_min_trade_qty=0.0001,
        min_notional_krw=5000.0,
    )
    assert unfilled.is_material_remaining is True


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
    submit_contract_kind: str | None = None,
    exchange_submit_notional_krw: float | None = None,
    request_ts: int = 1_700_000_000_100,
    response_ts: int = 1_700_000_000_180,
    execution_reality_contract: dict[str, object] | None = None,
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
    submit_evidence: dict[str, object] = {
        "exchange": "bithumb",
        "submit_contract_kind": submit_contract_kind,
        "exchange_submit_notional_krw": exchange_submit_notional_krw,
        "request_ts": request_ts,
        "response_ts": response_ts,
        "top_of_book": {"best_bid": 99.0, "best_ask": 100.0},
    }
    if execution_reality_contract is not None:
        submit_evidence["execution_reality_contract"] = execution_reality_contract
        submit_evidence["execution_contract_hash"] = execution_reality_contract["execution_contract_hash"]
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
        submit_evidence=json.dumps(submit_evidence, sort_keys=True),
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


def _quality_execution_contract(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "fill_reference_policy": "paper_top_of_book",
        "missing_quote_policy": "fail",
        "quote_source": "test_quote",
        "top_of_book_required": True,
        "top_of_book_is_full_depth": False,
        "latency_model": {"type": "immediate_top_of_book", "latency_ms": 0},
        "partial_fill_model": {"type": "immediate_top_of_book", "partial_fill_rate": 0.0},
        "order_failure_model": {"type": "immediate_top_of_book", "order_failure_rate": 0.0},
        "fee_source": "paper_runtime_settings",
        "slippage_source": "paper_runtime_settings",
        "calibration_required": False,
        "execution_reality_level": "paper_immediate_top_of_book",
    }
    kwargs.update(overrides)
    return build_execution_reality_contract(**kwargs)


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


def test_execution_quality_record_extracts_execution_contract_hash_from_submit_evidence(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-contract.sqlite"))
    contract = _quality_execution_contract()
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_contract",
            fill_prices=(100.0,),
            fill_qtys=(1.0,),
            execution_reality_contract=contract,
        )
        record = build_execution_quality_record(conn, client_order_id="quality_contract")
    finally:
        conn.close()

    assert record is not None
    assert record.execution_contract_hash == contract["execution_contract_hash"]
    assert record.execution_reality_contract == contract
    assert record.execution_contract_hash_valid is True
    assert record.execution_contract_mismatch_reason is None


def test_execution_quality_summary_reports_single_execution_contract_hash(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-contract-summary.sqlite"))
    contract = _quality_execution_contract()
    try:
        records = []
        for client_order_id in ("quality_contract_a", "quality_contract_b"):
            _seed_quality_order(
                conn,
                client_order_id=client_order_id,
                fill_prices=(100.0,),
                fill_qtys=(1.0,),
                execution_reality_contract=contract,
            )
            records.append(build_execution_quality_record(conn, client_order_id=client_order_id))
    finally:
        conn.close()

    summary = summarize_execution_quality(
        [record for record in records if record is not None],
        thresholds=ExecutionQualityThresholds(min_sample=1),
    )

    assert summary["execution_contract_hash"] == contract["execution_contract_hash"]
    assert summary["execution_contract_hash_present"] is True
    assert summary["mixed_execution_contract_hashes"] is False


def test_execution_quality_summary_flags_mixed_execution_contract_hashes(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-contract-mixed.sqlite"))
    first_contract = _quality_execution_contract()
    second_contract = _quality_execution_contract(
        latency_model={"type": "immediate_top_of_book", "latency_ms": 1}
    )
    try:
        records = []
        for client_order_id, contract in (
            ("quality_contract_first", first_contract),
            ("quality_contract_second", second_contract),
        ):
            _seed_quality_order(
                conn,
                client_order_id=client_order_id,
                fill_prices=(100.0,),
                fill_qtys=(1.0,),
                execution_reality_contract=contract,
            )
            records.append(build_execution_quality_record(conn, client_order_id=client_order_id))
    finally:
        conn.close()

    summary = summarize_execution_quality(
        [record for record in records if record is not None],
        thresholds=ExecutionQualityThresholds(min_sample=1),
    )

    assert summary["execution_contract_hash"] is None
    assert summary["mixed_execution_contract_hashes"] is True
    assert summary["quality_gate_status"] == "FAIL"
    assert summary["primary_issue"] == "mixed_execution_contract_hashes"


def test_execution_quality_historical_submit_timestamp_fallback_remains_explicit(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-fallback.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_historical",
            request_ts=None,
            response_ts=None,
            fill_prices=(100.0,),
            fill_qtys=(1.0,),
        )
        confirmation = conn.execute(
            """
            SELECT event_ts
            FROM order_events
            WHERE client_order_id='quality_historical' AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        record = build_execution_quality_record(conn, client_order_id="quality_historical")
    finally:
        conn.close()

    assert record is not None
    assert confirmation is not None
    assert record.submit_sent_ts_ms == int(confirmation["event_ts"])
    assert record.submit_response_ts_ms == int(confirmation["event_ts"])
    assert record.response_latency_ms == 0


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


def test_summary_reports_market_limit_cost_latency_and_fill_comparison(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-order-type.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_market_fast_costly",
            side="SELL",
            order_type="MaRkEt",
            decision_price=100.0,
            fill_prices=(99.0,),
            fill_qtys=(1.0,),
            qty_req=1.0,
            request_ts=1_700_000_000_100,
            response_ts=1_700_000_000_120,
        )
        _seed_quality_order(
            conn,
            client_order_id="quality_limit_partial",
            order_type="LIMIT",
            decision_price=100.0,
            fill_prices=(100.1,),
            fill_qtys=(0.5,),
            qty_req=1.0,
            request_ts=1_700_000_000_000,
            response_ts=1_700_000_000_020,
        )
        _seed_quality_order(
            conn,
            client_order_id="quality_limit_unfilled",
            order_type="limit",
            decision_price=100.0,
            fill_prices=(),
            fill_qtys=(),
            qty_req=1.0,
            request_ts=1_700_000_000_000,
            response_ts=1_700_000_000_020,
        )
        records = [
            build_execution_quality_record(conn, client_order_id="quality_market_fast_costly"),
            build_execution_quality_record(conn, client_order_id="quality_limit_partial"),
            build_execution_quality_record(conn, client_order_id="quality_limit_unfilled"),
        ]
    finally:
        conn.close()

    assert all(record is not None for record in records)
    summary = summarize_execution_quality(
        [record for record in records if record is not None],
        thresholds=ExecutionQualityThresholds(min_sample=1),
    )

    assert summary["market_order_count"] == 1
    assert summary["limit_order_count"] == 2
    assert summary["market_p90_slippage_bps"] == pytest.approx(100.0)
    assert summary["limit_p90_slippage_bps"] == pytest.approx(10.0)
    assert summary["market_p95_submit_to_fill_ms"] == pytest.approx(101.0)
    assert summary["limit_p95_submit_to_fill_ms"] == pytest.approx(201.0)
    assert summary["limit_partial_fill_rate"] == pytest.approx(0.5)
    assert summary["limit_unfilled_rate"] == pytest.approx(0.5)
    assert summary["order_type_cost_delta"] == "market_fills_faster_but_costs_more"

    text = format_execution_quality_text(summary)
    assert "market_p90_slippage_bps=100" in text
    assert "limit_p95_submit_to_fill_ms=201" in text
    assert "order_type_cost_delta=market_fills_faster_but_costs_more" in text


def test_summary_order_type_comparison_handles_one_side_only_and_unknown_type(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-one-side.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_market_only",
            side="SELL",
            order_type="market",
            fill_prices=(100.2,),
            fill_qtys=(1.0,),
        )
        _seed_quality_order(conn, client_order_id="quality_unknown_type", order_type="post_only", fill_prices=(100.1,), fill_qtys=(1.0,))
        market_only = build_execution_quality_record(conn, client_order_id="quality_market_only")
        unknown = build_execution_quality_record(conn, client_order_id="quality_unknown_type")
    finally:
        conn.close()

    assert market_only is not None
    assert unknown is not None
    summary = summarize_execution_quality(
        [market_only, unknown],
        thresholds=ExecutionQualityThresholds(min_sample=1),
    )

    assert summary["market_order_count"] == 1
    assert summary["limit_order_count"] == 0
    assert summary["unknown_order_type_count"] == 1
    assert summary["limit_p90_slippage_bps"] is None
    assert summary["order_type_cost_delta"] == "one_order_type_only"
    assert "limit_p90_slippage_bps=NA" in format_execution_quality_text(summary)


def test_summary_treats_buy_price_as_market_equivalent_not_unknown(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-price-market.sqlite"))
    try:
        records = []
        for index in range(72):
            client_order_id = f"quality_price_{index}"
            _seed_quality_order(
                conn,
                client_order_id=client_order_id,
                side="BUY",
                order_type="price",
                fill_prices=(100.0,),
                fill_qtys=(1.0,),
            )
            records.append(build_execution_quality_record(conn, client_order_id=client_order_id))
        for index in range(61):
            client_order_id = f"quality_market_{index}"
            _seed_quality_order(
                conn,
                client_order_id=client_order_id,
                side="SELL",
                order_type="market",
                fill_prices=(100.0,),
                fill_qtys=(1.0,),
            )
            records.append(build_execution_quality_record(conn, client_order_id=client_order_id))
        for index in range(49):
            client_order_id = f"quality_legacy_{index}"
            _seed_quality_order(
                conn,
                client_order_id=client_order_id,
                side="BUY",
                order_type=None,
                fill_prices=(100.0,),
                fill_qtys=(1.0,),
            )
            records.append(build_execution_quality_record(conn, client_order_id=client_order_id))
    finally:
        conn.close()

    assert all(record is not None for record in records)
    summary = summarize_execution_quality(
        [record for record in records if record is not None],
        thresholds=ExecutionQualityThresholds(min_sample=1),
    )

    assert summary["market_equivalent_order_count"] == 133
    assert summary["market_order_count"] == 133
    assert summary["verified_market_equivalent_order_count"] == 61
    assert summary["unverified_market_equivalent_order_count"] == 72
    assert summary["market_buy_quote_order_count"] == 72
    assert summary["market_sell_base_order_count"] == 61
    assert summary["legacy_unknown_order_type_count"] == 49
    assert summary["unsupported_unknown_order_type_count"] == 0
    assert summary["unknown_order_type_count"] == 49
    assert summary["order_type_cost_delta"] == "one_order_type_only"


def test_execution_quality_summary_separates_legacy_unverified_market_equivalent(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-unverified-market.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_legacy_buy_price",
            side="BUY",
            order_type="price",
            submit_contract_kind=None,
            fill_prices=(100.0,),
            fill_qtys=(1.0,),
        )
        record = build_execution_quality_record(conn, client_order_id="quality_legacy_buy_price")
    finally:
        conn.close()

    assert record is not None
    assert record.canonical_execution_kind == "market_buy_quote_notional"
    assert record.semantic_evidence_quality == "legacy_unverified"
    assert record.market_equivalent is True
    assert record.legacy_unknown_order_type is False
    assert record.unsupported_unknown_order_type is False

    summary = summarize_execution_quality([record], thresholds=ExecutionQualityThresholds(min_sample=1))
    assert summary["market_equivalent_order_count"] == 1
    assert summary["verified_market_equivalent_order_count"] == 0
    assert summary["unverified_market_equivalent_order_count"] == 1
    assert summary["unknown_order_type_count"] == 0


def test_material_remainder_controls_quality_gate_and_preserves_raw_flags(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "execution-quality-material-remainder.sqlite"))
    try:
        _seed_quality_order(
            conn,
            client_order_id="quality_tiny_residue",
            order_type="price",
            submit_contract_kind="market_buy_notional",
            exchange_submit_notional_krw=60_000.0,
            decision_price=100_000_000.0,
            submit_reference=100_000_000.0,
            fill_prices=(100_000_000.0,),
            fill_qtys=(0.00059988,),
            qty_req=0.0006,
        )
        conn.execute(
            """
            UPDATE orders
            SET qty_step=0.0001, effective_min_trade_qty=0.0001, min_notional_krw=5000.0
            WHERE client_order_id='quality_tiny_residue'
            """
        )
        record = build_execution_quality_record(conn, client_order_id="quality_tiny_residue")
    finally:
        conn.close()

    assert record is not None
    assert record.partial_fill_flag is True
    assert record.material_partial_fill_flag is False
    assert record.exchange_submit_notional_krw == pytest.approx(60_000.0)
    assert record.exchange_spent_quote_krw == pytest.approx(59_988.0)
    assert record.exchange_remaining_quote_krw == pytest.approx(12.0)
    assert record.exchange_fill_completion_ratio == pytest.approx(0.9998)
    assert record.internal_target_remaining_qty == pytest.approx(0.00000012)
    assert record.internal_target_residue_material is False
    assert record.remaining_qty_materiality_reason == "exchange_quote_budget_remaining_below_min_notional_krw"
    assert record.quality_status == "within_model"

    summary = summarize_execution_quality([record], thresholds=ExecutionQualityThresholds(min_sample=1))
    assert summary["raw_partial_fill_rate"] == pytest.approx(1.0)
    assert summary["partial_fill_rate"] == pytest.approx(0.0)
    assert summary["quality_gate_status"] == "PASS"


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
        _seed_quality_order(conn, client_order_id="quality_cli", side="SELL", fill_prices=(99.0,), fill_qtys=(1.0,))
        conn.commit()
    finally:
        conn.close()

    rc = main(["execution-quality-report", "--limit", "10", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["sample_count"] == 1
    assert "median_slippage_vs_signal_bps" in payload
    assert "market_p90_slippage_bps" in payload
    assert "limit_p90_slippage_bps" in payload
    assert payload["order_type_cost_delta"] == "one_order_type_only"
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


def test_execution_quality_schema_and_refresh_are_idempotent(tmp_path) -> None:
    db_path = tmp_path / "execution-quality-idempotent.sqlite"
    conn = ensure_db(str(db_path))
    try:
        schema_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='execution_quality_events'"
        ).fetchone()
        assert schema_row is not None
        schema_sql = str(schema_row["sql"])
        assert "client_order_id TEXT NOT NULL UNIQUE" in schema_sql
        assert "created_ts INTEGER NOT NULL" in schema_sql
        assert "updated_ts INTEGER NOT NULL" in schema_sql

        _seed_quality_order(conn, client_order_id="quality_idempotent", fill_prices=(100.0,), fill_qtys=(1.0,))
        refresh_execution_quality_records(conn, limit=10)
        refresh_execution_quality_records(conn, limit=10)
        count = conn.execute(
            "SELECT COUNT(*) AS count FROM execution_quality_events WHERE client_order_id='quality_idempotent'"
        ).fetchone()
    finally:
        conn.close()

    assert count is not None
    assert int(count["count"]) == 1


def test_execution_quality_old_schema_is_upgraded_and_report_runs(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "execution-quality-old-schema.sqlite"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.execute(
            """
            CREATE TABLE execution_quality_events (
                client_order_id TEXT,
                order_type TEXT,
                filled_qty REAL NOT NULL DEFAULT 0,
                requested_qty REAL,
                quality_status TEXT NOT NULL DEFAULT 'insufficient_evidence',
                quality_reason TEXT NOT NULL DEFAULT 'legacy_schema'
            )
            """
        )
        raw.commit()
    finally:
        raw.close()

    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "LIVE_EXECUTION_QUALITY_MIN_SAMPLE", 1)
    conn = ensure_db(str(db_path))
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(execution_quality_events)").fetchall()}
        for column in (
            "canonical_execution_kind",
            "semantic_evidence_quality",
            "market_equivalent",
            "legacy_unknown_order_type",
            "unsupported_unknown_order_type",
            "remaining_notional_krw",
            "qty_step",
            "effective_min_trade_qty",
            "min_notional_krw",
            "material_partial_fill_flag",
            "material_unfilled_flag",
            "remaining_qty_materiality_reason",
            "exchange_submit_notional_krw",
            "exchange_fill_completion_ratio",
        ):
            assert column in columns
        _seed_quality_order(
            conn,
            client_order_id="quality_old_schema",
            side="SELL",
            order_type="market",
            fill_prices=(100.0,),
            fill_qtys=(1.0,),
        )
        conn.commit()
    finally:
        conn.close()

    rc = main(["execution-quality-report", "--limit", "10", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["sample_count"] == 1
    assert payload["market_equivalent_order_count"] == 1
