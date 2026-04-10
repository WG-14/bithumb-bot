from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.bithumb import BithumbPrivateAPI
from bithumb_bot.broker.order_list_v1 import (
    build_order_list_params,
    build_recovery_order_list_params,
    parse_v1_order_list_row,
)


def test_build_order_list_params_accepts_uuids_only() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"])
    assert params == {
        "uuids": ["uuid-1", "uuid-2"],
        "page": 1,
        "order_by": "desc",
    }


def test_build_order_list_params_accepts_client_order_ids_only() -> None:
    params = build_order_list_params(
        client_order_ids=["cid-1", "cid-2"],
        state="done",
        page=2,
        order_by="asc",
        limit=10,
    )
    assert params == {
        "client_order_ids": ["cid-1", "cid-2"],
        "state": "done",
        "page": 2,
        "order_by": "asc",
        "limit": 10,
    }


def test_build_order_list_params_rejects_missing_identifiers() -> None:
    with pytest.raises(ValueError, match="requires uuids or client_order_ids"):
        build_order_list_params()


def test_build_order_list_params_rejects_identifier_length_over_limit() -> None:
    with pytest.raises(ValueError, match="allows at most 30 items"):
        build_order_list_params(uuids=[f"uuid-{idx}" for idx in range(31)])


def test_build_order_list_params_accepts_identifier_length_limit() -> None:
    params = build_order_list_params(uuids=[f"uuid-{idx}" for idx in range(30)])
    assert params["uuids"] == [f"uuid-{idx}" for idx in range(30)]


def test_build_order_list_params_rejects_identifier_length_over_limit_for_client_ids() -> None:
    with pytest.raises(ValueError, match="allows at most 30 items"):
        build_order_list_params(client_order_ids=[f"cid-{idx}" for idx in range(31)])


def test_build_order_list_params_accepts_client_identifier_length_limit() -> None:
    params = build_order_list_params(client_order_ids=[f"cid-{idx}" for idx in range(30)])
    assert params["client_order_ids"] == [f"cid-{idx}" for idx in range(30)]


def test_build_order_list_params_rejects_invalid_state() -> None:
    with pytest.raises(ValueError, match="state must be one of"):
        build_order_list_params(uuids=["uuid-1"], state="unknown")


def test_build_order_list_params_rejects_zero_page() -> None:
    with pytest.raises(ValueError, match="page must be between 1"):
        build_order_list_params(uuids=["uuid-1"], page=0)


def test_build_order_list_params_rejects_invalid_order_by() -> None:
    with pytest.raises(ValueError, match="order_by must be one of"):
        build_order_list_params(uuids=["uuid-1"], order_by="latest")


def test_build_order_list_params_accepts_broad_recovery_scan_with_market_and_states() -> None:
    params = build_recovery_order_list_params(
        market="KRW-BTC",
        states=["wait", "done"],
        limit=10,
    )

    assert params == {
        "page": 1,
        "order_by": "desc",
        "market": "KRW-BTC",
        "states": ["wait", "done"],
        "limit": 10,
    }


def test_build_order_list_params_rejects_state_and_states_together() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        build_order_list_params(
            market="KRW-BTC",
            state="wait",
            states=["done"],
            allow_broad_scan=True,
        )


def test_build_order_list_params_rejects_watch_mixed_with_general_states() -> None:
    with pytest.raises(ValueError, match="must not mix watch"):
        build_order_list_params(
            market="KRW-BTC",
            states=["wait", "watch"],
            allow_broad_scan=True,
        )


def test_build_order_list_params_accepts_watch_only_recovery_scan() -> None:
    params = build_recovery_order_list_params(
        market="KRW-BTC",
        states=["watch"],
        page=2,
    )

    assert params["states"] == ["watch"]
    assert params["market"] == "KRW-BTC"
    assert params["page"] == 2


def test_build_recovery_order_list_params_keeps_general_lookup_identifier_scoped() -> None:
    with pytest.raises(ValueError, match="use build_recovery_order_list_params"):
        build_order_list_params(market="KRW-BTC", state="wait")


def test_build_order_list_params_rejects_out_of_range_limit() -> None:
    with pytest.raises(ValueError, match="limit must be between 1 and 100"):
        build_order_list_params(uuids=["uuid-1"], limit=0)


def test_build_order_list_params_query_string_remains_query_hash_compatible() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"], state="wait", page=3, order_by="desc")
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=3&order_by=desc&uuids[]=uuid-1&uuids[]=uuid-2&state=wait"
    claims = BithumbPrivateAPI._query_hash_claims(params)
    assert claims["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_build_order_list_params_client_order_ids_array_is_consistent_for_auth_hash() -> None:
    params = build_order_list_params(client_order_ids=["cid-1", "cid-2"], state="done", limit=5)
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=1&order_by=desc&client_order_ids[]=cid-1&client_order_ids[]=cid-2&state=done&limit=5"
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


def test_parse_v1_order_list_row_tolerates_missing_volume_with_units_alias() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            volume="",
            remaining_volume="",
            executed_volume="",
            units="0.02",
            units_remaining="0.01",
            filled_volume="0.01",
        )
    )
    assert parsed.volume == pytest.approx(0.02)
    assert parsed.remaining_volume == pytest.approx(0.01)
    assert parsed.executed_volume == pytest.approx(0.01)


def test_parse_v1_order_list_row_tolerates_missing_volume_when_derivable() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            volume="",
            remaining_volume="0.01",
            executed_volume="0.01",
        )
    )
    assert parsed.volume == pytest.approx(0.02)
    assert parsed.remaining_volume == pytest.approx(0.01)
    assert parsed.executed_volume == pytest.approx(0.01)




def _aws_observed_done_row_missing_updated_at(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "uuid": "aws-done-1",
        "client_order_id": "live_1712230310689_buy_abcd1234",
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "price",
        "state": "done",
        "price": "149000000",
        "volume": "",
        "remaining_volume": "",
        "executed_volume": "0.01",
        "created_at": "2024-04-04T13:45:10+09:00",
        "updated_at": "",
        "executed_funds": "1490000",
        "fee": "745",
    }
    row.update(overrides)
    return row


def test_parse_v1_order_list_row_aws_done_shape_missing_updated_at_is_tolerated() -> None:
    parsed = parse_v1_order_list_row(_aws_observed_done_row_missing_updated_at())

    assert parsed.uuid == "aws-done-1"
    assert parsed.client_order_id == "live_1712230310689_buy_abcd1234"
    assert parsed.state == "done"
    assert parsed.executed_volume == pytest.approx(0.01)
    assert parsed.volume == pytest.approx(0.01)
    assert parsed.paid_fee == pytest.approx(745.0)
    assert parsed.updated_ts == parsed.created_ts
    assert "updated_at:derived_from_created_at" in parsed.degraded_fields

def test_parse_v1_order_list_row_done_tolerates_missing_volume_and_remaining() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            volume="",
            remaining_volume="",
            executed_volume="0.02",
        )
    )
    assert parsed.volume == pytest.approx(0.02)
    assert parsed.remaining_volume == pytest.approx(0.0)
    assert "volume:derived_from_executed_volume" in parsed.degraded_fields


def test_parse_v1_order_list_row_done_tolerates_missing_volume_from_executed_funds() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            volume="",
            remaining_volume="",
            executed_volume="",
            executed_funds="2980000",
            price="149000000",
        )
    )
    assert parsed.volume == pytest.approx(0.02)
    assert parsed.executed_volume == pytest.approx(0.02)
    assert parsed.remaining_volume == pytest.approx(0.0)
    assert "volume:derived_from_executed_funds" in parsed.degraded_fields


@pytest.mark.parametrize(
    ("fee_payload", "expected"),
    [
        ({}, None),
        ({"paid_fee": ""}, None),
        ({"paid_fee": None}, None),
        ({"paid_fee": "0"}, 0.0),
        ({"reserved_fee": "12.5"}, 12.5),
        ({"remaining_fee": "1.2"}, 1.2),
    ],
)
def test_parse_v1_order_list_row_fee_variants_are_tolerated(fee_payload, expected) -> None:
    parsed = parse_v1_order_list_row(_v1_orders_row(**fee_payload))
    if expected is None:
        assert parsed.paid_fee is None
    else:
        assert parsed.paid_fee == pytest.approx(expected)


def test_parse_v1_order_list_row_executed_funds_forward_compatibility() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            volume="",
            remaining_volume="",
            executed_volume="",
            executed_funds="1490000",
            avg_price="149000000",
        )
    )
    assert parsed.executed_funds == pytest.approx(1_490_000.0)
    assert parsed.volume == pytest.approx(0.01)


def test_parse_v1_order_list_row_done_uses_avg_price_when_price_missing() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            price="",
            avg_price="149000000",
            executed_volume="0.01",
            executed_funds="1490000",
        )
    )

    assert parsed.price == pytest.approx(149_000_000.0)
    assert parsed.price_missing is False
    assert parsed.price_source == "avg_price"
    assert "price:derived_from_avg_price" in parsed.degraded_fields


def test_parse_v1_order_list_row_done_derives_price_from_executed_funds_and_volume() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            price="",
            avg_price="",
            executed_volume="0.01",
            executed_funds="1490000",
        )
    )

    assert parsed.price == pytest.approx(149_000_000.0)
    assert parsed.price_missing is False
    assert parsed.price_source == "executed_funds/executed_volume"
    assert "price:derived_from_executed_funds_over_executed_volume" in parsed.degraded_fields


def test_parse_v1_order_list_row_done_allows_terminal_confirmation_only_when_price_missing() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            price="",
            avg_price="",
            executed_volume="",
            executed_funds="",
            volume="",
            remaining_volume="",
        )
    )

    assert parsed.price is None
    assert parsed.price_missing is True
    assert parsed.price_source == "terminal_confirmation_only"
    assert "price:missing_terminal_confirmation_only" in parsed.degraded_fields


def test_parse_v1_order_list_row_tolerates_missing_updated_at_with_created_at_fallback() -> None:
    parsed = parse_v1_order_list_row(_v1_orders_row(state="done", updated_at=""))

    assert parsed.created_ts == 1704067200000
    assert parsed.updated_ts == 1704067200000
    assert "updated_at:derived_from_created_at" in parsed.degraded_fields


def test_parse_v1_order_list_row_tolerates_missing_created_and_updated_with_ordered_at() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            created_at="",
            updated_at="",
            ordered_at="2024-01-01T00:02:00+00:00",
        )
    )

    assert parsed.created_ts == 1704067320000
    assert parsed.updated_ts == 1704067320000
    assert "created_at:derived_from_ordered_at" in parsed.degraded_fields
    assert "updated_at:derived_from_ordered_at" in parsed.degraded_fields


def test_parse_v1_order_list_row_tolerates_missing_order_timestamps_with_trade_timestamp_fallback() -> None:
    parsed = parse_v1_order_list_row(
        _v1_orders_row(
            state="done",
            created_at="",
            updated_at="",
            ordered_at="",
            trades=[{"created_at": "2024-01-01T00:03:00+00:00"}],
        )
    )

    assert parsed.created_ts == 1704067380000
    assert parsed.updated_ts == 1704067380000
    assert "created_at:derived_from_updated_at" in parsed.degraded_fields
    assert "updated_at:derived_from_trade_timestamp" in parsed.degraded_fields
