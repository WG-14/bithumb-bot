from __future__ import annotations

import hashlib

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.bithumb import BithumbBroker, BithumbPrivateAPI
from bithumb_bot.broker.order_list_v1 import build_order_list_params, parse_v1_order_list_row
from bithumb_bot.config import settings


def _configure_live() -> None:
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")


@pytest.fixture(autouse=True)
def _stub_canonical_market(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")



def test_v1_orders_builder_accepts_documented_identifier_query_only() -> None:
    params = build_order_list_params(
        uuids=[" ex-1 ", "ex-2"],
        client_order_ids=["cid_1", "cid-2"],
        state="DONE",
        page=3,
        order_by="ASC",
    )

    assert params == {
        "uuids": ["ex-1", "ex-2"],
        "client_order_ids": ["cid_1", "cid-2"],
        "state": "done",
        "page": 3,
        "order_by": "asc",
    }


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"market": "KRW-BTC", "state": "wait", "limit": 100}, TypeError),
        ({"state": "wait", "limit": 100}, ValueError),
        ({"market": "KRW-BTC", "state": "done"}, TypeError),
    ],
)
def test_v1_orders_builder_rejects_legacy_market_state_limit_scan_shape(
    kwargs: dict[str, object],
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        build_order_list_params(**kwargs)


def test_v1_orders_parser_rejects_row_without_identifiers() -> None:
    with pytest.raises(BrokerRejectError, match="missing both uuid and client_order_id"):
        parse_v1_order_list_row(
            {
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "state": "wait",
                "price": "100",
                "volume": "0.1",
                "remaining_volume": "0.1",
                "executed_volume": "0",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
            }
        )


def test_v1_orders_parser_rejects_unknown_state() -> None:
    with pytest.raises(BrokerRejectError, match="unknown state"):
        parse_v1_order_list_row(
            {
                "uuid": "ex-1",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "state": "halted",
                "price": "100",
                "volume": "0.1",
                "remaining_volume": "0.1",
                "executed_volume": "0",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
            }
        )


def test_v1_orders_query_hash_serialization_for_identifier_arrays_is_stable() -> None:
    payload = {
        "uuids": ["ex-1", "ex-2"],
        "client_order_ids": ["cid-1", "cid-2"],
        "state": "done",
        "page": 1,
        "order_by": "desc",
    }

    canonical = BithumbPrivateAPI._canonical_payload_for_query_hash(payload)
    assert canonical == (
        "uuids[]=ex-1&uuids[]=ex-2&client_order_ids[]=cid-1&client_order_ids[]=cid-2"
        "&state=done&page=1&order_by=desc"
    )
    claims = BithumbPrivateAPI._query_hash_from_canonical_payload(canonical)
    assert claims["query_hash"] == hashlib.sha512(canonical.encode("utf-8")).hexdigest()


def test_v1_orders_query_hash_serialization_uses_bracket_array_for_single_identifier_kind() -> None:
    payload = {
        "page": 1,
        "order_by": "desc",
        "client_order_ids": ["cid-1", "cid-2"],
        "state": "wait",
        "limit": 2,
    }
    canonical = BithumbPrivateAPI._canonical_payload_for_query_hash(payload)
    assert canonical == "page=1&order_by=desc&client_order_ids[]=cid-1&client_order_ids[]=cid-2&state=wait&limit=2"


def test_v1_orders_builder_rejects_invalid_client_order_id_format() -> None:
    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        build_order_list_params(client_order_ids=["invalid id"])


def test_get_recent_orders_rejects_broad_scan_without_identifiers() -> None:
    _configure_live()
    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="broad /v1/orders market/state scans are disabled"):
        broker.get_recent_orders(limit=10)


def test_get_recent_orders_fails_fast_on_contract_violation_row(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint: str, params: dict[str, object], retry_safe: bool = False):
        assert endpoint == "/v1/orders"
        assert params.get("uuids") == ["ex-1"]
        if params.get("state") == "wait":
            return [
                {
                    "uuid": "ex-1",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "UNKNOWN",  # contract violation
                    "price": "100",
                    "volume": "0.1",
                    "remaining_volume": "0.1",
                    "executed_volume": "0",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "updated_at": "2024-01-01T00:00:00+00:00",
                }
            ]
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    with pytest.raises(BrokerRejectError, match="/v1/orders schema mismatch: unknown state"):
        broker.get_recent_orders(limit=5, exchange_order_ids=["ex-1"])
