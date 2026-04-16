from __future__ import annotations

from bithumb_bot.app import cmd_report
from bithumb_bot.db_core import ensure_db
from bithumb_bot.config import settings
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
