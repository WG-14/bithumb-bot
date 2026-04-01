from __future__ import annotations

from decimal import Decimal

import pytest

from bithumb_bot.broker.accounts_v1 import PairBalances, parse_accounts_response, select_pair_balances, to_broker_balance
from bithumb_bot.broker.base import BrokerRejectError


def test_parse_accounts_response_is_strict_and_pure():
    parsed = parse_accounts_response(
        [
            {"currency": "krw", "balance": "1000", "locked": "10"},
            {"currency": "btc", "balance": "0.2", "locked": "0.05"},
        ]
    )

    assert parsed.row_count == 2
    assert len(parsed.rows) == 2
    assert parsed.balances["KRW"] == (Decimal("1000"), Decimal("10"))
    assert parsed.balances["BTC"] == (Decimal("0.2"), Decimal("0.05"))


def test_parse_accounts_response_schema_error_surfaces_broker_reject():
    with pytest.raises(BrokerRejectError, match="schema mismatch: expected array payload"):
        parse_accounts_response({"currency": "KRW"})


def test_parse_accounts_response_rejects_non_object_row() -> None:
    with pytest.raises(BrokerRejectError, match=r"/v1/accounts\[0\] schema mismatch: expected object row"):
        parse_accounts_response(["bad-row"])


def test_parse_accounts_response_rejects_missing_currency() -> None:
    with pytest.raises(BrokerRejectError, match="missing required text field 'currency'"):
        parse_accounts_response([{"balance": "1", "locked": "0"}])


@pytest.mark.parametrize("field,bad", [("balance", "abc"), ("locked", "-1")])
def test_parse_accounts_response_rejects_invalid_required_numeric_fields(field: str, bad: str) -> None:
    row = {"currency": "KRW", "balance": "1", "locked": "0"}
    row[field] = bad
    with pytest.raises(BrokerRejectError, match=rf"(invalid|negative) numeric field '{field}'"):
        parse_accounts_response([row])


def test_parse_accounts_response_rejects_duplicate_currency_row() -> None:
    with pytest.raises(BrokerRejectError, match="duplicate currency row 'KRW'"):
        parse_accounts_response(
            [
                {"currency": "KRW", "balance": "1", "locked": "0"},
                {"currency": "krw", "balance": "2", "locked": "0"},
            ]
        )


def test_parse_accounts_response_accepts_documented_optional_fields() -> None:
    parsed = parse_accounts_response(
        [
            {
                "currency": "btc",
                "balance": "0.2",
                "locked": "0.05",
                "avg_buy_price": "123",
                "avg_buy_price_modified": "true",
                "unit_currency": "krw",
            }
        ]
    )
    row = parsed.rows[0]
    assert row.avg_buy_price == Decimal("123")
    assert row.avg_buy_price_modified is True
    assert row.unit_currency == "KRW"


def test_parse_accounts_response_optional_fields_can_be_absent() -> None:
    parsed = parse_accounts_response([{"currency": "BTC", "balance": "0.2", "locked": "0.05"}])
    row = parsed.rows[0]
    assert row.avg_buy_price is None
    assert row.avg_buy_price_modified is None
    assert row.unit_currency is None


def test_select_pair_balances_requires_pair_currencies():
    parsed = parse_accounts_response([{"currency": "BTC", "balance": "0.2", "locked": "0.05"}])

    with pytest.raises(BrokerRejectError, match="missing quote currency row 'KRW'"):
        select_pair_balances(parsed, order_currency="BTC", payment_currency="KRW")


def test_to_broker_balance_maps_values():
    mapped = to_broker_balance(
        PairBalances(
            cash_balance=Decimal("1000"),
            cash_locked=Decimal("10"),
            asset_balance=Decimal("0.2"),
            asset_locked=Decimal("0.05"),
        )
    )

    assert mapped.cash_available == 1000.0
    assert mapped.cash_locked == 10.0
    assert mapped.asset_available == 0.2
    assert mapped.asset_locked == 0.05


def test_select_pair_balances_uses_decimal_totals_without_float_error():
    parsed = parse_accounts_response([
        {"currency": "KRW", "balance": "0.1", "locked": "0.2"},
        {"currency": "BTC", "balance": "0.00000001", "locked": "0.00000002"},
    ])

    pair = select_pair_balances(parsed, order_currency="BTC", payment_currency="KRW")

    assert pair.cash_total == Decimal("0.3")
    assert pair.asset_total == Decimal("0.00000003")
