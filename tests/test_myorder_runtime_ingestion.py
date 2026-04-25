from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.myorder_events import normalize_myorder_event_payload
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.oms import create_order


pytestmark = pytest.mark.fast_regression


@pytest.fixture(autouse=True)
def _restore_start_cash():
    original = settings.START_CASH_KRW
    original_mode = settings.MODE
    object.__setattr__(settings, "START_CASH_KRW", 50_000_000.0)
    try:
        yield
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original)
        object.__setattr__(settings, "MODE", original_mode)


def _official_trade_payload(
    *,
    client_order_id: str,
    order_uuid: str,
    trade_uuid: str,
    ask_bid: str = "bid",
    order_type: str = "limit",
    price: str = "100000000",
    volume: str = "0.1",
    executed_volume: str = "0.1",
    remaining_volume: str = "0.1",
    timestamp: int = 1710000000000,
    trade_timestamp: int = 1710000000123,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "myOrder",
        "ask_bid": ask_bid,
        "order_type": order_type,
        "state": "trade",
        "uuid": order_uuid,
        "trade_uuid": trade_uuid,
        "client_order_id": client_order_id,
        "price": price,
        "volume": volume,
        "executed_volume": executed_volume,
        "remaining_volume": remaining_volume,
        "executed_funds": str(float(price) * float(executed_volume)),
        "trade_timestamp": trade_timestamp,
        "order_timestamp": timestamp - 1000,
        "timestamp": timestamp,
    }
    if extra:
        payload.update(extra)
    return payload


def _incident_runtime_payload() -> dict[str, object]:
    fixture_path = (
        Path(__file__).resolve().parent / "fixtures" / "bithumb" / "live_paid_fee_single_fill_buy_2026_04_24.json"
    )
    fixture = json.loads(fixture_path.read_text())
    trade = dict(fixture["trade"])
    return _official_trade_payload(
        client_order_id="cid-incident-paid-fee",
        order_uuid="ex-incident-paid-fee",
        trade_uuid=str(trade["uuid"]),
        price=str(trade["price"]),
        volume=str(trade["volume"]),
        executed_volume=str(trade["volume"]),
        extra={
            "paid_fee": str(fixture["order_fee_fields"]["paid_fee"]),
            "executed_funds": str(trade["funds"]),
        },
    )


def test_myorder_runtime_ingestion_applies_fill_and_status(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_ingest.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-1",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-entry-1",
                order_uuid="ex-1",
                trade_uuid="fill-1",
                extra={"fee": "50.0"},
            ),
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT exchange_order_id, status, qty_filled FROM orders WHERE client_order_id='cid-entry-1'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fill_id, qty, price, fee FROM fills WHERE client_order_id='cid-entry-1'"
        ).fetchone()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert order_row["exchange_order_id"] == "ex-1"
    assert order_row["status"] == "PARTIAL"
    assert order_row["qty_filled"] == pytest.approx(0.1)
    assert fill_row["fill_id"] == "fill-1"
    assert fill_row["qty"] == pytest.approx(0.1)
    assert fill_row["price"] == pytest.approx(100_000_000.0)
    assert fill_row["fee"] == pytest.approx(50.0)
    assert int(stream_row["applied"]) == 1
    assert stream_row["applied_status"] == "applied"


def test_myorder_runtime_ingestion_dedupes_repeated_event(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_duplicate.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-2",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        payload = {
            **_official_trade_payload(
                client_order_id="cid-entry-2",
                order_uuid="ex-2",
                trade_uuid="fill-2",
                volume="0.2",
                executed_volume="0.2",
                remaining_volume="0",
                timestamp=1710000001000,
                trade_timestamp=1710000001123,
                extra={"fee": "100.0"},
            )
        }
        first = BithumbBroker.ingest_myorder_event_runtime(conn, payload=payload)
        second = BithumbBroker.ingest_myorder_event_runtime(conn, payload=payload)
        conn.commit()

        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-2'"
        ).fetchone()["cnt"]
        event_count = conn.execute("SELECT COUNT(*) AS cnt FROM private_stream_events").fetchone()["cnt"]
    finally:
        conn.close()

    assert first.accepted is True
    assert first.applied is True
    assert second.accepted is False
    assert second.action == "duplicate_event"
    assert fill_count == 1
    assert event_count == 1


def test_myorder_runtime_ingestion_records_unmatched_event_without_applying(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_unmatched.sqlite"))
    try:
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-missing",
                order_uuid="ex-missing",
                trade_uuid="fill-missing",
                timestamp=1710000002000,
                trade_timestamp=1710000002123,
            ),
        )
        conn.commit()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is False
    assert result.action == "no_local_order_match"
    assert int(stream_row["applied"]) == 0
    assert stream_row["applied_status"] == "no_local_order_match"


def test_myorder_runtime_ingestion_missing_fee_records_pending_observation_without_ledger_apply(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_fee_pending.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-missing-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-entry-missing-fee",
                order_uuid="ex-missing-fee",
                trade_uuid="fill-missing-fee",
                timestamp=1710000003000,
                trade_timestamp=1710000003123,
            ),
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()["cnt"]
        trade_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fill_id, fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                   fee_provenance, fee_validation_reason, fee_validation_checks, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-missing-fee'
            """
        ).fetchone()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert result.action == "recovery_required_fee_pending"
    assert result.status == "RECOVERY_REQUIRED"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee-pending" in str(order_row["last_error"])
    assert fill_count == 0
    assert trade_count == 0
    assert observation is not None
    assert observation["fill_id"] == "fill-missing-fee"
    assert observation["fee"] is None
    assert observation["fee_status"] == "missing"
    assert observation["fee_source"] == "missing"
    assert observation["fee_confidence"] == "invalid"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "myorder_private_stream_fee_pending"
    assert observation["fee_provenance"] == "missing_fee_field"
    assert observation["fee_validation_reason"] == "missing_fee_field"
    assert "missing_fee_field" in str(observation["parse_warnings"])
    assert int(stream_row["applied"]) == 1
    assert stream_row["applied_status"] == "recovery_required_fee_pending"


def test_myorder_runtime_ingestion_paid_fee_candidate_is_not_accounting_complete(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_paid_fee_candidate.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-paid-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-entry-paid-fee",
                order_uuid="ex-paid-fee",
                trade_uuid="fill-paid-fee",
                timestamp=1710000003500,
                trade_timestamp=1710000003623,
                extra={"paid_fee": "50.0", "reserved_fee": "50.0", "remaining_fee": "0"},
            ),
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-paid-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-paid-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                   fee_provenance, fee_validation_reason, fee_validation_checks, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-paid-fee'
            """
        ).fetchone()
    finally:
        conn.close()

    assert result.action == "recovery_required_fee_pending"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee_status=order_level_candidate" in str(order_row["last_error"])
    assert fill_count == 0
    assert observation["fee"] == pytest.approx(50.0)
    assert observation["fee_status"] == "order_level_candidate"
    assert observation["fee_source"] == "order_level_paid_fee"
    assert observation["fee_confidence"] == "ambiguous"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "myorder_private_stream_fee_pending"
    assert observation["fee_provenance"] == "order_level_paid_fee_unvalidated"
    assert observation["fee_validation_reason"] == "expected_fee_rate_mismatch"
    assert '"expected_fee_rate_match": false' in str(observation["fee_validation_checks"]).lower()
    assert "order_level_fee_candidate:paid_fee" in str(observation["parse_warnings"])


def test_myorder_runtime_ingestion_validated_single_fill_paid_fee_applies_ledger(tmp_path) -> None:
    original_fee_rate = settings.LIVE_FEE_RATE_ESTIMATE
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    conn = ensure_db(str(tmp_path / "myorder_validated_paid_fee.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-incident-paid-fee",
            side="BUY",
            qty_req=0.00059999,
            price=116_110_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(conn, payload=_incident_runtime_payload())
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-incident-paid-fee'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fill_id, qty, price, fee FROM fills WHERE client_order_id='cid-incident-paid-fee'"
        ).fetchone()
        observation_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM broker_fill_observations WHERE client_order_id='cid-incident-paid-fee'"
        ).fetchone()
    finally:
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", original_fee_rate)
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert result.action != "recovery_required_fee_pending"
    assert order_row["status"] == "PARTIAL"
    assert order_row["qty_filled"] == pytest.approx(0.00059999)
    assert order_row["last_error"] is None
    assert fill_row["fill_id"] == "C0101000000983750807"
    assert fill_row["qty"] == pytest.approx(0.00059999)
    assert fill_row["price"] == pytest.approx(116_110_000.0)
    assert fill_row["fee"] == pytest.approx(27.86)
    assert observation_count["cnt"] == 0


def test_myorder_runtime_ingestion_material_zero_fee_marks_recovery_required(tmp_path) -> None:
    object.__setattr__(settings, "MODE", "live")
    conn = ensure_db(str(tmp_path / "myorder_recovery_required.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-live-zero-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-entry-live-zero-fee",
                order_uuid="ex-live-zero-fee",
                trade_uuid="fill-live-zero-fee",
                timestamp=1710000003000,
                trade_timestamp=1710000003123,
                extra={"fee": "0"},
            ),
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-live-zero-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-live-zero-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fee, fee_status, accounting_status, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-live-zero-fee'
            """
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert result.action == "recovery_required_fee_pending"
    assert result.status == "RECOVERY_REQUIRED"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee_status=zero_reported" in str(order_row["last_error"])
    assert fill_count == 0
    assert observation is not None
    assert observation["fee"] == pytest.approx(0.0)
    assert observation["fee_status"] == "zero_reported"
    assert observation["accounting_status"] == "fee_pending"
    assert "zero_fee_field:fee" in str(observation["parse_warnings"])


def test_myorder_trade_uses_volume_as_per_trade_qty_and_trade_timestamp(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_partial_volume.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-partial-volume",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        first = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-partial-volume",
                order_uuid="ex-partial-volume",
                trade_uuid="trade-partial-a",
                volume="0.1",
                executed_volume="0.1",
                remaining_volume="0.1",
                timestamp=1710000010000,
                trade_timestamp=1710000010123,
                extra={"fee": "50.0"},
            ),
        )
        second = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-partial-volume",
                order_uuid="ex-partial-volume",
                trade_uuid="trade-partial-b",
                volume="0.1",
                executed_volume="0.2",
                remaining_volume="0",
                timestamp=1710000011000,
                trade_timestamp=1710000011123,
                extra={"fee": "50.0"},
            ),
        )
        conn.commit()
        order = conn.execute(
            "SELECT qty_filled FROM orders WHERE client_order_id='cid-partial-volume'"
        ).fetchone()
        fills = conn.execute(
            """
            SELECT fill_id, qty, fill_ts
            FROM fills
            WHERE client_order_id='cid-partial-volume'
            ORDER BY fill_ts
            """
        ).fetchall()
    finally:
        conn.close()

    assert first.action == "applied"
    assert second.action == "applied"
    assert order["qty_filled"] == pytest.approx(0.2)
    assert [row["fill_id"] for row in fills] == ["trade-partial-a", "trade-partial-b"]
    assert [row["qty"] for row in fills] == pytest.approx([0.1, 0.1])
    assert [row["fill_ts"] for row in fills] == [1710000010123, 1710000011123]


def test_myorder_done_without_trade_uuid_is_order_state_only(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_done_order_state.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-done-state",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        trade_result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-done-state",
                order_uuid="ex-done-state",
                trade_uuid="trade-before-done",
                volume="0.2",
                executed_volume="0.2",
                remaining_volume="0",
                timestamp=1710000020000,
                trade_timestamp=1710000020123,
                extra={"fee": "100.0"},
            ),
        )
        done_result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "ask_bid": "bid",
                "order_type": "limit",
                "state": "done",
                "uuid": "ex-done-state",
                "client_order_id": "cid-done-state",
                "price": "100000000",
                "volume": "0.2",
                "executed_volume": "0.2",
                "remaining_volume": "0",
                "executed_funds": "20000000",
                "paid_fee": "100.0",
                "reserved_fee": "100.0",
                "remaining_fee": "0",
                "order_timestamp": 1710000020000,
                "timestamp": 1710000021000,
            },
        )
        conn.commit()
        order = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='cid-done-state'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-done-state'"
        ).fetchone()["cnt"]
    finally:
        conn.close()

    assert trade_result.action == "applied"
    assert done_result.action == "applied"
    assert order["status"] == "FILLED"
    assert order["qty_filled"] == pytest.approx(0.2)
    assert fill_count == 1


def test_myorder_done_with_unaccounted_executed_volume_requires_recovery(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_done_gap.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-done-gap",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "ask_bid": "bid",
                "order_type": "limit",
                "state": "done",
                "uuid": "ex-done-gap",
                "client_order_id": "cid-done-gap",
                "price": "100000000",
                "volume": "0.2",
                "executed_volume": "0.2",
                "remaining_volume": "0",
                "executed_funds": "20000000",
                "paid_fee": "100.0",
                "reserved_fee": "100.0",
                "remaining_fee": "0",
                "order_timestamp": 1710000020000,
                "timestamp": 1710000021000,
            },
        )
        conn.commit()
        order = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-done-gap'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-done-gap'"
        ).fetchone()["cnt"]
    finally:
        conn.close()

    assert result.action == "recovery_required_unaccounted_terminal_volume"
    assert order["status"] == "RECOVERY_REQUIRED"
    assert order["qty_filled"] == pytest.approx(0.0)
    assert "unaccounted executed volume" in str(order["last_error"])
    assert fill_count == 0


def test_myorder_normalization_prefers_official_identity_side_order_type_and_timestamps() -> None:
    buy = normalize_myorder_event_payload(
        _official_trade_payload(
            client_order_id="cid-buy",
            order_uuid="order-buy",
            trade_uuid="trade-buy",
            ask_bid="bid",
            order_type="price",
            volume="0.01",
            executed_volume="0.03",
            remaining_volume="0.07",
            timestamp=1710000030000,
            trade_timestamp=1710000030123,
            extra={"trade_id": "legacy-trade-id", "fill_id": "legacy-fill-id"},
        )
    )
    sell = normalize_myorder_event_payload(
        _official_trade_payload(
            client_order_id="cid-sell",
            order_uuid="order-sell",
            trade_uuid="trade-sell",
            ask_bid="ask",
            order_type="market",
            timestamp=1710000040000,
            trade_timestamp=1710000040123,
        )
    )

    assert buy.side == "BUY"
    assert buy.order_type == "price"
    assert buy.exchange_order_id == "order-buy"
    assert buy.trade_uuid == "trade-buy"
    assert buy.fill_id == "trade-buy"
    assert buy.qty == pytest.approx(0.01)
    assert buy.executed_volume == pytest.approx(0.03)
    assert buy.fill_ts_ms == 1710000030123
    assert buy.event_ts_ms == 1710000030000
    assert sell.side == "SELL"
    assert sell.order_type == "market"


def test_myorder_paid_fee_fields_remain_fee_pending_fill_observation(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_paid_fee_order_level.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-paid-fee-fields",
            side="SELL",
            qty_req=0.1,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload=_official_trade_payload(
                client_order_id="cid-paid-fee-fields",
                order_uuid="ex-paid-fee-fields",
                trade_uuid="trade-paid-fee-fields",
                ask_bid="ask",
                order_type="market",
                extra={"paid_fee": "50.0", "reserved_fee": "50.0", "remaining_fee": "0"},
            ),
        )
        conn.commit()
        observation = conn.execute(
            """
            SELECT fill_id, side, fee, fee_status, accounting_status, raw_payload
            FROM broker_fill_observations
            WHERE client_order_id='cid-paid-fee-fields'
            """
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-paid-fee-fields'"
        ).fetchone()["cnt"]
    finally:
        conn.close()

    assert result.action == "recovery_required_fee_pending"
    assert fill_count == 0
    assert observation["fill_id"] == "trade-paid-fee-fields"
    assert observation["side"] == "SELL"
    assert observation["fee"] == pytest.approx(50.0)
    assert observation["fee_status"] == "order_level_candidate"
    assert observation["accounting_status"] == "fee_pending"
    assert "paid_fee" in str(observation["raw_payload"])


def test_myorder_stream_and_rest_fill_contract_parity(monkeypatch) -> None:
    _configure_payload = {
        "uuid": "ex-parity",
        "client_order_id": "cid-parity",
        "state": "done",
        "side": "bid",
        "price": "100000000",
        "volume": "0.2",
        "remaining_volume": "0",
        "executed_volume": "0.2",
        "executed_funds": "20000000",
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:02+00:00",
        "trades": [
            {
                "uuid": "trade-parity-a",
                "price": "100000000",
                "volume": "0.1",
                "fee": "50.0",
                "created_at": "2024-01-01T00:00:01+00:00",
            },
            {
                "uuid": "trade-parity-b",
                "price": "100000000",
                "volume": "0.1",
                "fee": "50.0",
                "created_at": "2024-01-01T00:00:02+00:00",
            },
        ],
    }
    broker = BithumbBroker()
    broker.dry_run = False
    monkeypatch.setattr(broker, "_get_private", lambda endpoint, params, retry_safe=False: _configure_payload)

    rest_fills = broker.get_fills(client_order_id="cid-parity", exchange_order_id="ex-parity")
    stream_events = [
        normalize_myorder_event_payload(
            _official_trade_payload(
                client_order_id="cid-parity",
                order_uuid="ex-parity",
                trade_uuid="trade-parity-a",
                volume="0.1",
                executed_volume="0.1",
                remaining_volume="0.1",
                timestamp=1704067201500,
                trade_timestamp=1704067201000,
                extra={"fee": "50.0"},
            )
        ),
        normalize_myorder_event_payload(
            _official_trade_payload(
                client_order_id="cid-parity",
                order_uuid="ex-parity",
                trade_uuid="trade-parity-b",
                volume="0.1",
                executed_volume="0.2",
                remaining_volume="0",
                timestamp=1704067202500,
                trade_timestamp=1704067202000,
                extra={"fee": "50.0"},
            )
        ),
    ]

    assert [
        (fill.client_order_id, fill.exchange_order_id, fill.fill_id, fill.fill_ts, fill.price, fill.qty, fill.fee, fill.fee_status)
        for fill in rest_fills
    ] == [
        (event.client_order_id, event.exchange_order_id, event.fill_id, event.fill_ts_ms, event.price, event.qty, event.fee, event.fee_status)
        for event in stream_events
    ]
