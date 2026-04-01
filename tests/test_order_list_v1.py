from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.bithumb import BithumbPrivateAPI
from bithumb_bot.broker.order_list_v1 import build_order_list_params, parse_v1_order_list_row


def test_build_order_list_params_accepts_uuids_only() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"])
    assert params == {
        "uuids": ["uuid-1", "uuid-2"],
        "page": 1,
        "order_by": "desc",
    }


def test_build_order_list_params_accepts_client_order_ids_only() -> None:
    params = build_order_list_params(client_order_ids=["cid-1", "cid-2"], state="done", page=2, order_by="asc")
    assert params == {
        "client_order_ids": ["cid-1", "cid-2"],
        "state": "done",
        "page": 2,
        "order_by": "asc",
    }


def test_build_order_list_params_rejects_missing_identifiers() -> None:
    with pytest.raises(ValueError, match="requires uuids or client_order_ids"):
        build_order_list_params()


def test_build_order_list_params_rejects_identifier_length_over_limit() -> None:
    with pytest.raises(ValueError, match="allows at most 100 items"):
        build_order_list_params(uuids=[f"uuid-{idx}" for idx in range(101)])


def test_build_order_list_params_rejects_invalid_state() -> None:
    with pytest.raises(ValueError, match="state must be one of"):
        build_order_list_params(uuids=["uuid-1"], state="unknown")


def test_build_order_list_params_rejects_zero_page() -> None:
    with pytest.raises(ValueError, match="page must be between 1"):
        build_order_list_params(uuids=["uuid-1"], page=0)


def test_build_order_list_params_rejects_invalid_order_by() -> None:
    with pytest.raises(ValueError, match="order_by must be one of"):
        build_order_list_params(uuids=["uuid-1"], order_by="latest")


def test_build_order_list_params_query_string_remains_query_hash_compatible() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"], state="wait", page=3, order_by="desc")
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=3&order_by=desc&uuids[]=uuid-1&uuids[]=uuid-2&state=wait"
    claims = BithumbPrivateAPI._query_hash_claims(params)
    assert claims["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_build_order_list_params_client_order_ids_array_is_consistent_for_auth_hash() -> None:
    params = build_order_list_params(client_order_ids=["cid-1", "cid-2"], state="done")
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=1&order_by=desc&client_order_ids[]=cid-1&client_order_ids[]=cid-2&state=done"
    claims = BithumbPrivateAPI._query_hash_claims(params)
    assert claims["query_hash"]


def _v1_orders_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "uuid": "order-1",
        "client_order_id": "cid-1",
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "limit",
        "state": "wait",
        "price": "150000000",
        "volume": "0.02",
        "remaining_volume": "0.01",
        "executed_volume": "0.01",
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:01:00+00:00",
    }
    row.update(overrides)
    return row


def test_parse_v1_order_list_row_success() -> None:
    parsed = parse_v1_order_list_row(_v1_orders_row())
    assert parsed.uuid == "order-1"
    assert parsed.client_order_id == "cid-1"
    assert parsed.side == "BUY"
    assert parsed.executed_funds is None


def test_parse_v1_order_list_row_accepts_optional_executed_funds() -> None:
    parsed = parse_v1_order_list_row(_v1_orders_row(executed_funds="3000000"))
    assert parsed.executed_funds == pytest.approx(3_000_000.0)


def test_parse_v1_order_list_row_accepts_client_order_id_when_uuid_missing() -> None:
    parsed = parse_v1_order_list_row(_v1_orders_row(uuid=""))
    assert parsed.uuid == ""
    assert parsed.client_order_id == "cid-1"


def test_parse_v1_order_list_row_rejects_missing_both_identifiers() -> None:
    with pytest.raises(BrokerRejectError, match="missing both uuid and client_order_id"):
        parse_v1_order_list_row(_v1_orders_row(uuid="", client_order_id=""))


def test_parse_v1_order_list_row_rejects_invalid_state() -> None:
    with pytest.raises(BrokerRejectError, match="unknown state"):
        parse_v1_order_list_row(_v1_orders_row(state="mystery"))


def test_parse_v1_order_list_row_rejects_invalid_numeric() -> None:
    with pytest.raises(BrokerRejectError, match="invalid numeric field 'executed_volume'"):
        parse_v1_order_list_row(_v1_orders_row(executed_volume="bad-number"))


def test_parse_v1_order_list_row_rejects_invalid_timestamp() -> None:
    with pytest.raises(BrokerRejectError, match="invalid timestamp field 'updated_at'"):
        parse_v1_order_list_row(_v1_orders_row(updated_at="not-a-timestamp"))
