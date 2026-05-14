from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from bithumb_bot.broker import paper
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio
from bithumb_bot.execution_quality import build_execution_quality_record
from bithumb_bot.public_api_orderbook import BestQuote


def _set(attr: str, value):
    old = getattr(settings, attr)
    object.__setattr__(settings, attr, value)
    return old


def _configure_stress(
    tmp_path: Path,
    *,
    db_name: str,
    failure_rate: float = 0.0,
    partial_rate: float = 0.0,
    partial_fraction: float = 0.5,
    seed: str = "123",
):
    return {
        "DB_PATH": _set("DB_PATH", str(tmp_path / db_name)),
        "PAPER_EXECUTION_MODEL": _set("PAPER_EXECUTION_MODEL", "stress"),
        "PAPER_EXECUTION_STRESS_SEED": _set("PAPER_EXECUTION_STRESS_SEED", seed),
        "PAPER_EXECUTION_LATENCY_MS": _set("PAPER_EXECUTION_LATENCY_MS", 250),
        "PAPER_EXECUTION_PARTIAL_FILL_RATE": _set("PAPER_EXECUTION_PARTIAL_FILL_RATE", partial_rate),
        "PAPER_EXECUTION_PARTIAL_FILL_FRACTION": _set("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", partial_fraction),
        "PAPER_EXECUTION_ORDER_FAILURE_RATE": _set("PAPER_EXECUTION_ORDER_FAILURE_RATE", failure_rate),
        "SLIPPAGE_BPS": _set("SLIPPAGE_BPS", 0.0),
        "MAX_ORDER_KRW": _set("MAX_ORDER_KRW", 0.0),
        "PAPER_FEE_RATE": _set("PAPER_FEE_RATE", 0.0),
        "BUY_FRACTION": _set("BUY_FRACTION", 1.0),
        "MAX_ORDERBOOK_SPREAD_BPS": _set("MAX_ORDERBOOK_SPREAD_BPS", 100.0),
    }


def _configure_immediate(tmp_path: Path, *, db_name: str):
    values = _configure_stress(tmp_path, db_name=db_name)
    _set("PAPER_EXECUTION_MODEL", "immediate")
    return values


def _restore(values: dict[str, object]) -> None:
    for key, value in values.items():
        _set(key, value)


def _prepare_buy(monkeypatch) -> None:
    monkeypatch.setattr(
        paper,
        "fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=100.0),
    )
    conn = ensure_db()
    set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
    conn.close()


def _prepare_sell_lot(monkeypatch, *, qty: float = 0.0002, lot_count: int = 2) -> None:
    monkeypatch.setattr(
        paper,
        "fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=100.0),
    )
    conn = ensure_db()
    set_portfolio(conn, cash_krw=1_000_000, asset_qty=float(qty))
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            lot_semantic_version,
            internal_lot_size,
            lot_min_qty,
            lot_qty_step,
            lot_min_notional_krw,
            lot_max_qty_decimals,
            lot_rule_source_mode,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_open",
            1,
            100.0,
            float(qty),
            int(lot_count),
            0,
            1,
            float(qty) / float(lot_count),
            float(qty) / float(lot_count),
            float(qty) / float(lot_count),
            0.0,
            8,
            "test",
            "lot-native",
            "open_exposure",
        ),
    )
    conn.commit()
    conn.close()


def _latest_stress_evidence(conn):
    row = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE submit_phase='paper_execution'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    return json.loads(str(row["submit_evidence"]))


def test_paper_immediate_execution_records_execution_contract_evidence(tmp_path: Path, monkeypatch):
    old = _configure_immediate(tmp_path, db_name="immediate_contract.sqlite")
    try:
        _prepare_buy(monkeypatch)

        trade = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        assert trade is not None
        conn = ensure_db()
        evidence = _latest_stress_evidence(conn)
        row = conn.execute(
            "SELECT client_order_id FROM order_events WHERE submit_phase='paper_execution' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        record = build_execution_quality_record(conn, client_order_id=str(row["client_order_id"]))
        conn.close()

        assert evidence["execution_model_name"] == "immediate_top_of_book"
        assert evidence["fill_status"] == "filled"
        assert evidence["execution_contract_hash"].startswith("sha256:")
        assert evidence["execution_reality_contract"]["execution_contract_hash"] == evidence["execution_contract_hash"]
        assert record is not None
        assert record.execution_contract_hash == evidence["execution_contract_hash"]
        assert record.execution_reality_contract == evidence["execution_reality_contract"]
        assert record.execution_contract_hash_valid is True
    finally:
        _restore(old)


def test_paper_stress_execution_records_execution_contract_evidence(tmp_path: Path, monkeypatch):
    old = _configure_stress(tmp_path, db_name="stress_contract.sqlite")
    try:
        _prepare_buy(monkeypatch)

        trade = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        assert trade is not None
        conn = ensure_db()
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert evidence["execution_model_name"] == "stress"
        assert evidence["fill_status"] == "filled"
        assert evidence["execution_contract_hash"].startswith("sha256:")
        assert evidence["execution_reality_contract"]["execution_contract_hash"] == evidence["execution_contract_hash"]
    finally:
        _restore(old)


def test_stress_failure_records_failed_order_without_fill_or_trade(tmp_path: Path, monkeypatch):
    old = _configure_stress(tmp_path, db_name="stress_failure.sqlite", failure_rate=1.0)
    try:
        _prepare_buy(monkeypatch)

        trade = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        assert trade is None
        conn = ensure_db()
        order = conn.execute(
            "SELECT status, qty_req, qty_filled FROM orders ORDER BY id DESC LIMIT 1"
        ).fetchone()
        fill_count = conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"]
        trade_count = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        dedup_count = conn.execute("SELECT COUNT(*) AS n FROM order_intent_dedup").fetchone()["n"]
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert order is not None
        assert order["status"] == "FAILED"
        assert float(order["qty_req"]) > 0.0
        assert float(order["qty_filled"]) == pytest.approx(0.0)
        assert fill_count == 0
        assert trade_count == 0
        assert dedup_count == 0
        assert evidence["fill_status"] == "failed"
        assert evidence["filled_qty"] == pytest.approx(0.0)
        assert evidence["execution_contract_hash"].startswith("sha256:")
        assert evidence["execution_reality_contract"]["execution_contract_hash"] == evidence["execution_contract_hash"]
    finally:
        _restore(old)


def test_stress_partial_fill_keeps_order_open_and_dedup_claimed(tmp_path: Path, monkeypatch):
    old = _configure_stress(
        tmp_path,
        db_name="stress_partial.sqlite",
        partial_rate=1.0,
        partial_fraction=0.5,
    )
    try:
        _prepare_buy(monkeypatch)

        trade = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        assert trade is not None
        conn = ensure_db()
        order = conn.execute(
            "SELECT client_order_id, status, qty_req, qty_filled FROM orders ORDER BY id DESC LIMIT 1"
        ).fetchone()
        fill = conn.execute("SELECT qty FROM fills ORDER BY id DESC LIMIT 1").fetchone()
        trade_row = conn.execute("SELECT qty FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        dedup = conn.execute("SELECT order_status FROM order_intent_dedup").fetchone()
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert order is not None
        assert order["status"] == "PARTIAL"
        assert float(order["qty_filled"]) == pytest.approx(float(order["qty_req"]) * 0.5)
        assert float(order["qty_filled"]) < float(order["qty_req"])
        assert fill is not None
        assert float(fill["qty"]) == pytest.approx(float(order["qty_filled"]))
        assert trade_row is not None
        assert float(trade_row["qty"]) == pytest.approx(float(order["qty_filled"]))
        assert dedup is not None
        assert dedup["order_status"] == "PARTIAL"
        assert evidence["fill_status"] == "partial"
        assert evidence["remaining_qty"] > 0.0
        assert evidence["execution_contract_hash"].startswith("sha256:")
    finally:
        _restore(old)


def test_stress_execution_is_deterministic_across_isolated_dbs(tmp_path: Path, monkeypatch):
    observed: list[dict[str, object]] = []
    old = _configure_stress(
        tmp_path,
        db_name="stress_replay_1.sqlite",
        partial_rate=1.0,
        partial_fraction=0.25,
        seed="777",
    )
    try:
        for index in range(2):
            _set("DB_PATH", str(tmp_path / f"stress_replay_{index}.sqlite"))
            _prepare_buy(monkeypatch)
            trade = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)
            assert trade is not None
            conn = ensure_db()
            evidence = _latest_stress_evidence(conn)
            conn.close()
            observed.append(
                {
                    "fill_status": evidence["fill_status"],
                    "filled_qty": evidence["filled_qty"],
                    "remaining_qty": evidence["remaining_qty"],
                    "execution_model_params_hash": evidence["execution_model_params_hash"],
                    "derived_seed_hash": evidence["derived_seed_hash"],
                    "execution_contract_hash": evidence["execution_contract_hash"],
                }
            )
    finally:
        _restore(old)

    assert observed[0] == observed[1]


def test_partial_stress_order_blocks_duplicate_intent(tmp_path: Path, monkeypatch):
    old = _configure_stress(
        tmp_path,
        db_name="stress_duplicate.sqlite",
        partial_rate=1.0,
        partial_fraction=0.5,
    )
    try:
        _prepare_buy(monkeypatch)

        first = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)
        second = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        assert first is not None
        assert second is None
        conn = ensure_db()
        order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
        dedup = conn.execute("SELECT order_status FROM order_intent_dedup").fetchone()
        conn.close()

        assert order_count == 1
        assert dedup is not None
        assert dedup["order_status"] == "PARTIAL"
    finally:
        _restore(old)


def test_partial_stress_order_blocks_new_intent_with_different_timestamp(
    tmp_path: Path,
    monkeypatch,
    caplog,
):
    old = _configure_stress(
        tmp_path,
        db_name="stress_unresolved_gate.sqlite",
        partial_rate=1.0,
        partial_fraction=0.5,
    )
    try:
        _prepare_buy(monkeypatch)

        first = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        caplog.set_level(logging.INFO, logger="bithumb_bot.run")
        second = paper.paper_execute("SELL", ts=1_700_000_060_000, price=100.0)

        assert first is not None
        assert second is None
        conn = ensure_db()
        order = conn.execute("SELECT status FROM orders ORDER BY id DESC LIMIT 1").fetchone()
        order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
        fill_count = conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"]
        trade_count = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        dedup = conn.execute("SELECT order_status FROM order_intent_dedup").fetchone()
        conn.close()

        assert order is not None
        assert order["status"] == "PARTIAL"
        assert order_count == 1
        assert fill_count == 1
        assert trade_count == 1
        assert dedup is not None
        assert dedup["order_status"] == "PARTIAL"
        assert any(
            "[SKIP] unresolved order gate" in record.getMessage()
            and "reason_code=UNRESOLVED_OPEN_ORDER_PRESENT" in record.getMessage()
            for record in caplog.records
        )
    finally:
        _restore(old)


def test_unresolved_paper_order_gate_runs_before_orderbook_fetch(
    tmp_path: Path,
    monkeypatch,
    caplog,
):
    old = _configure_stress(
        tmp_path,
        db_name="stress_unresolved_gate_before_orderbook.sqlite",
        partial_rate=1.0,
        partial_fraction=0.5,
    )
    try:
        fetch_calls = {"count": 0}

        def _fetch_once(_market: str):
            fetch_calls["count"] += 1
            if fetch_calls["count"] > 1:
                raise AssertionError("orderbook fetch should not run while an unresolved order is open")
            return BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=100.0)

        monkeypatch.setattr(paper, "fetch_orderbook_top", _fetch_once)
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        first = paper.paper_execute("BUY", ts=1_700_000_000_000, price=100.0)

        caplog.set_level(logging.INFO, logger="bithumb_bot.run")
        second = paper.paper_execute("SELL", ts=1_700_000_060_000, price=100.0)

        assert first is not None
        assert second is None
        assert fetch_calls["count"] == 1

        conn = ensure_db()
        order = conn.execute("SELECT status FROM orders ORDER BY id DESC LIMIT 1").fetchone()
        order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
        fill_count = conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"]
        trade_count = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        dedup = conn.execute("SELECT order_status FROM order_intent_dedup").fetchone()
        conn.close()

        assert order is not None
        assert order["status"] == "PARTIAL"
        assert order_count == 1
        assert fill_count == 1
        assert trade_count == 1
        assert dedup is not None
        assert dedup["order_status"] == "PARTIAL"
        assert any(
            "[SKIP] unresolved order gate" in record.getMessage()
            and "reason_code=UNRESOLVED_OPEN_ORDER_PRESENT" in record.getMessage()
            for record in caplog.records
        )
    finally:
        _restore(old)


def test_invalid_paper_execution_model_fails_closed():
    old = _set("PAPER_EXECUTION_MODEL", "stres")
    try:
        with pytest.raises(ValueError, match="PAPER_EXECUTION_MODEL"):
            paper._validate_paper_execution_config()
    finally:
        _set("PAPER_EXECUTION_MODEL", old)


@pytest.mark.parametrize(
    ("setting_name", "invalid_value"),
    [
        ("PAPER_EXECUTION_PARTIAL_FILL_RATE", -0.1),
        ("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", 1.1),
        ("PAPER_EXECUTION_ORDER_FAILURE_RATE", 2.0),
    ],
)
def test_invalid_paper_stress_rate_fails_closed(setting_name: str, invalid_value: float):
    old_model = _set("PAPER_EXECUTION_MODEL", "stress")
    old_value = _set(setting_name, invalid_value)
    try:
        with pytest.raises(ValueError, match=setting_name):
            paper._validate_paper_execution_config()
    finally:
        _set(setting_name, old_value)
        _set("PAPER_EXECUTION_MODEL", old_model)


@pytest.mark.parametrize("invalid_fraction", [0.0, 1.0])
def test_invalid_paper_partial_fill_fraction_boundary_fails_closed(invalid_fraction: float):
    old_model = _set("PAPER_EXECUTION_MODEL", "stress")
    old_fraction = _set("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", invalid_fraction)
    try:
        with pytest.raises(ValueError, match="PAPER_EXECUTION_PARTIAL_FILL_FRACTION"):
            paper._validate_paper_execution_config()
    finally:
        _set("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", old_fraction)
        _set("PAPER_EXECUTION_MODEL", old_model)


def test_valid_paper_partial_fill_fraction_passes_stress_validation():
    old_model = _set("PAPER_EXECUTION_MODEL", "stress")
    old_fraction = _set("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", 0.5)
    try:
        paper._validate_paper_execution_config()
    finally:
        _set("PAPER_EXECUTION_PARTIAL_FILL_FRACTION", old_fraction)
        _set("PAPER_EXECUTION_MODEL", old_model)


def test_invalid_paper_stress_latency_fails_closed():
    old_model = _set("PAPER_EXECUTION_MODEL", "stress")
    old_latency = _set("PAPER_EXECUTION_LATENCY_MS", -1)
    try:
        with pytest.raises(ValueError, match="PAPER_EXECUTION_LATENCY_MS"):
            paper._validate_paper_execution_config()
    finally:
        _set("PAPER_EXECUTION_LATENCY_MS", old_latency)
        _set("PAPER_EXECUTION_MODEL", old_model)


def test_stress_partial_sell_keeps_order_partial_and_applies_only_filled_qty(
    tmp_path: Path,
    monkeypatch,
):
    old = _configure_stress(
        tmp_path,
        db_name="stress_partial_sell.sqlite",
        partial_rate=1.0,
        partial_fraction=0.5,
    )
    try:
        _prepare_sell_lot(monkeypatch, qty=0.0002, lot_count=2)

        trade = paper.paper_execute("SELL", ts=1_700_000_000_000, price=100.0)

        assert trade is not None
        assert trade["side"] == "SELL"
        conn = ensure_db()
        order = conn.execute(
            "SELECT status, qty_req, qty_filled FROM orders ORDER BY id DESC LIMIT 1"
        ).fetchone()
        fill = conn.execute("SELECT qty FROM fills ORDER BY id DESC LIMIT 1").fetchone()
        trade_row = conn.execute("SELECT qty FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        remaining_lot_qty = conn.execute(
            """
            SELECT COALESCE(SUM(qty_open), 0.0) AS qty
            FROM open_position_lots
            WHERE position_state='open_exposure'
            """
        ).fetchone()["qty"]
        dedup = conn.execute("SELECT order_status FROM order_intent_dedup").fetchone()
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert order is not None
        assert order["status"] == "PARTIAL"
        assert float(order["qty_filled"]) == pytest.approx(float(order["qty_req"]) * 0.5)
        assert float(order["qty_filled"]) < float(order["qty_req"])
        assert fill is not None
        assert float(fill["qty"]) == pytest.approx(float(order["qty_filled"]))
        assert trade_row is not None
        assert float(trade_row["qty"]) == pytest.approx(float(order["qty_filled"]))
        assert float(remaining_lot_qty) == pytest.approx(0.0001)
        assert dedup is not None
        assert dedup["order_status"] == "PARTIAL"
        assert evidence["fill_status"] == "partial"
        assert evidence["remaining_qty"] > 0.0
    finally:
        _restore(old)


def test_stress_failed_sell_does_not_change_portfolio_lots_fills_or_trades(
    tmp_path: Path,
    monkeypatch,
):
    old = _configure_stress(
        tmp_path,
        db_name="stress_failed_sell.sqlite",
        failure_rate=1.0,
    )
    try:
        _prepare_sell_lot(monkeypatch, qty=0.0002, lot_count=2)
        conn = ensure_db()
        before = conn.execute(
            """
            SELECT cash_krw, asset_qty
            FROM portfolio
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()
        before_lot_qty = conn.execute(
            """
            SELECT COALESCE(SUM(qty_open), 0.0) AS qty
            FROM open_position_lots
            WHERE position_state='open_exposure'
            """
        ).fetchone()["qty"]
        conn.close()

        trade = paper.paper_execute("SELL", ts=1_700_000_000_000, price=100.0)

        assert trade is None
        conn = ensure_db()
        after = conn.execute(
            """
            SELECT cash_krw, asset_qty
            FROM portfolio
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()
        after_lot_qty = conn.execute(
            """
            SELECT COALESCE(SUM(qty_open), 0.0) AS qty
            FROM open_position_lots
            WHERE position_state='open_exposure'
            """
        ).fetchone()["qty"]
        order = conn.execute("SELECT status, qty_filled FROM orders ORDER BY id DESC LIMIT 1").fetchone()
        fill_count = conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"]
        trade_count = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        dedup_count = conn.execute("SELECT COUNT(*) AS n FROM order_intent_dedup").fetchone()["n"]
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert before is not None
        assert after is not None
        assert float(after["cash_krw"]) == pytest.approx(float(before["cash_krw"]))
        assert float(after["asset_qty"]) == pytest.approx(float(before["asset_qty"]))
        assert float(after_lot_qty) == pytest.approx(float(before_lot_qty))
        assert order is not None
        assert order["status"] == "FAILED"
        assert float(order["qty_filled"]) == pytest.approx(0.0)
        assert fill_count == 0
        assert trade_count == 0
        assert dedup_count == 0
        assert evidence["fill_status"] == "failed"
    finally:
        _restore(old)


def test_stress_latency_shifts_fill_ts_and_is_persisted_in_evidence(
    tmp_path: Path,
    monkeypatch,
):
    old = _configure_stress(
        tmp_path,
        db_name="stress_latency.sqlite",
        partial_rate=0.0,
        failure_rate=0.0,
    )
    try:
        _prepare_buy(monkeypatch)
        signal_ts = 1_700_000_000_000

        trade = paper.paper_execute("BUY", ts=signal_ts, price=100.0)

        assert trade is not None
        conn = ensure_db()
        fill = conn.execute("SELECT fill_ts FROM fills ORDER BY id DESC LIMIT 1").fetchone()
        trade_row = conn.execute("SELECT ts FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        evidence = _latest_stress_evidence(conn)
        conn.close()

        assert fill is not None
        assert trade_row is not None
        assert fill["fill_ts"] == signal_ts + 250
        assert trade_row["ts"] == signal_ts + 250
        assert evidence["latency_ms"] == 250
        assert evidence["execution_model_name"] == "stress"
        assert evidence["execution_model_version"] == "research_stress_v1"
        assert evidence["execution_model_params_hash"]
        assert evidence["derived_seed_hash"]
    finally:
        _restore(old)
