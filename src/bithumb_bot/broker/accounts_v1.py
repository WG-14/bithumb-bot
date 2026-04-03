from __future__ import annotations

"""Helpers for /v1/accounts REST snapshot parsing.

This module validates Bithumb private REST `/v1/accounts` payloads used by
`BithumbBroker.get_balance()`. It does not implement MyAsset(WebSocket)
streaming balances.
"""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .base import BrokerBalance, BrokerRejectError


class AccountsSchemaMismatchError(BrokerRejectError):
    """`/v1/accounts` response row/payload does not match documented schema."""


class AccountsRequiredCurrencyMissingError(BrokerRejectError):
    """Required base/quote currency row is missing for requested market."""


@dataclass(frozen=True)
class AccountRow:
    currency: str
    balance: Decimal
    locked: Decimal
    avg_buy_price: Decimal | None = None
    avg_buy_price_modified: bool | None = None
    unit_currency: str | None = None


@dataclass(frozen=True)
class ParsedAccounts:
    rows: tuple[AccountRow, ...]
    balances: dict[str, tuple[Decimal, Decimal]]
    row_count: int
    currencies: tuple[str, ...]
    duplicate_currencies: tuple[str, ...]


@dataclass(frozen=True)
class PairBalances:
    cash_balance: Decimal
    cash_locked: Decimal
    asset_balance: Decimal
    asset_locked: Decimal

    @property
    def cash_total(self) -> Decimal:
        return self.cash_balance + self.cash_locked

    @property
    def asset_total(self) -> Decimal:
        return self.asset_balance + self.asset_locked


def _required_non_negative_decimal(payload: dict[str, object], key: str, *, context: str) -> Decimal:
    raw = payload.get(key)
    if raw in (None, ""):
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: missing required numeric field '{key}'")
    try:
        parsed = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
    if not parsed.is_finite():
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
    if parsed < 0:
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: negative numeric field '{key}'={raw}")
    return parsed


def _optional_decimal(payload: dict[str, object], key: str, *, context: str) -> Decimal | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        parsed = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: invalid optional numeric field '{key}'={raw}") from exc
    if not parsed.is_finite():
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: non-finite optional numeric field '{key}'={raw}")
    if parsed < 0:
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: negative optional numeric field '{key}'={raw}")
    return parsed


def _optional_bool(payload: dict[str, object], key: str, *, context: str) -> bool | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    if isinstance(raw, bool):
        return raw
    token = str(raw).strip().lower()
    if token in {"true", "1"}:
        return True
    if token in {"false", "0"}:
        return False
    raise AccountsSchemaMismatchError(f"{context} schema mismatch: invalid optional bool field '{key}'={raw}")


def parse_accounts_response(data: object) -> ParsedAccounts:
    context = "/v1/accounts"
    if not isinstance(data, list):
        raise AccountsSchemaMismatchError(f"{context} schema mismatch: expected array payload, got {type(data).__name__}")

    balances: dict[str, tuple[Decimal, Decimal]] = {}
    rows: list[AccountRow] = []
    currencies_in_order: list[str] = []
    duplicate_currencies: list[str] = []
    for index, row in enumerate(data):
        row_context = f"{context}[{index}]"
        if not isinstance(row, dict):
            raise AccountsSchemaMismatchError(f"{row_context} schema mismatch: expected object row, got {type(row).__name__}")

        raw_currency = row.get("currency")
        currency = str(raw_currency).strip().upper() if raw_currency is not None else ""
        if not currency:
            raise AccountsSchemaMismatchError(f"{row_context} schema mismatch: missing required text field 'currency'")
        if currency in balances:
            duplicate_currencies.append(currency)
            raise AccountsSchemaMismatchError(f"{row_context} schema mismatch: duplicate currency row '{currency}'")

        balance = _required_non_negative_decimal(row, "balance", context=row_context)
        locked = _required_non_negative_decimal(row, "locked", context=row_context)
        avg_buy_price = _optional_decimal(row, "avg_buy_price", context=row_context)
        avg_buy_price_modified = _optional_bool(row, "avg_buy_price_modified", context=row_context)
        unit_currency_raw = row.get("unit_currency")
        unit_currency = str(unit_currency_raw).strip().upper() if unit_currency_raw not in (None, "") else None
        balances[currency] = (balance, locked)
        currencies_in_order.append(currency)
        rows.append(
            AccountRow(
                currency=currency,
                balance=balance,
                locked=locked,
                avg_buy_price=avg_buy_price,
                avg_buy_price_modified=avg_buy_price_modified,
                unit_currency=unit_currency,
            )
        )

    return ParsedAccounts(
        rows=tuple(rows),
        balances=balances,
        row_count=len(data),
        currencies=tuple(sorted(set(currencies_in_order))),
        duplicate_currencies=tuple(sorted(set(duplicate_currencies))),
    )


def select_pair_balances(
    accounts: ParsedAccounts,
    *,
    order_currency: str,
    payment_currency: str,
    allow_missing_base: bool = False,
) -> PairBalances:
    quote_currency = payment_currency.strip().upper()
    base_currency = order_currency.strip().upper()
    if quote_currency not in accounts.balances:
        raise AccountsRequiredCurrencyMissingError(f"/v1/accounts schema mismatch: missing quote currency row '{quote_currency}'")

    cash_balance, cash_locked = accounts.balances[quote_currency]
    if base_currency in accounts.balances:
        asset_balance, asset_locked = accounts.balances[base_currency]
    elif allow_missing_base:
        asset_balance = Decimal("0")
        asset_locked = Decimal("0")
    else:
        raise AccountsRequiredCurrencyMissingError(f"/v1/accounts schema mismatch: missing base currency row '{base_currency}'")

    return PairBalances(
        cash_balance=cash_balance,
        cash_locked=cash_locked,
        asset_balance=asset_balance,
        asset_locked=asset_locked,
    )


def to_broker_balance(pair_balances: PairBalances) -> BrokerBalance:
    return BrokerBalance(
        cash_available=float(pair_balances.cash_balance),
        cash_locked=float(pair_balances.cash_locked),
        asset_available=float(pair_balances.asset_balance),
        asset_locked=float(pair_balances.asset_locked),
    )
