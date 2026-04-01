from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .base import BrokerBalance, BrokerRejectError


@dataclass(frozen=True)
class ParsedAccounts:
    balances: dict[str, tuple[Decimal, Decimal]]
    row_count: int
    currencies: tuple[str, ...]
    duplicate_currencies: tuple[str, ...]


def _required_non_negative_decimal(payload: dict[str, object], key: str, *, context: str) -> Decimal:
    raw = payload.get(key)
    if raw in (None, ""):
        raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field '{key}'")
    try:
        parsed = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise BrokerRejectError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
    if not parsed.is_finite():
        raise BrokerRejectError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
    if parsed < 0:
        raise BrokerRejectError(f"{context} schema mismatch: negative numeric field '{key}'={raw}")
    return parsed


def parse_accounts_response(data: object) -> ParsedAccounts:
    context = "/v1/accounts"
    if not isinstance(data, list):
        raise BrokerRejectError(f"{context} schema mismatch: expected array payload, got {type(data).__name__}")

    balances: dict[str, tuple[Decimal, Decimal]] = {}
    currencies_in_order: list[str] = []
    for index, row in enumerate(data):
        row_context = f"{context}[{index}]"
        if not isinstance(row, dict):
            raise BrokerRejectError(f"{row_context} schema mismatch: expected object row, got {type(row).__name__}")

        raw_currency = row.get("currency")
        currency = str(raw_currency).strip().upper() if raw_currency is not None else ""
        if not currency:
            raise BrokerRejectError(f"{row_context} schema mismatch: missing required text field 'currency'")
        if currency in balances:
            raise BrokerRejectError(f"{row_context} schema mismatch: duplicate currency row '{currency}'")

        balance = _required_non_negative_decimal(row, "balance", context=row_context)
        locked = _required_non_negative_decimal(row, "locked", context=row_context)
        balances[currency] = (balance, locked)
        currencies_in_order.append(currency)

    duplicate_currencies = sorted({token for token in currencies_in_order if currencies_in_order.count(token) > 1})
    return ParsedAccounts(
        balances=balances,
        row_count=len(data),
        currencies=tuple(sorted(set(currencies_in_order))),
        duplicate_currencies=tuple(duplicate_currencies),
    )


def select_pair_balances(
    accounts: ParsedAccounts,
    *,
    order_currency: str,
    payment_currency: str,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    quote_currency = payment_currency.strip().upper()
    base_currency = order_currency.strip().upper()
    missing_required_currencies: list[str] = []
    if quote_currency not in accounts.balances:
        missing_required_currencies.append(quote_currency)
    if base_currency not in accounts.balances:
        missing_required_currencies.append(base_currency)

    if missing_required_currencies:
        if quote_currency in missing_required_currencies:
            raise BrokerRejectError(f"/v1/accounts schema mismatch: missing quote currency row '{quote_currency}'")
        raise BrokerRejectError(f"/v1/accounts schema mismatch: missing base currency row '{base_currency}'")

    cash_balance, cash_locked = accounts.balances[quote_currency]
    asset_balance, asset_locked = accounts.balances[base_currency]
    return cash_balance, cash_locked, asset_balance, asset_locked


def to_broker_balance(*, cash_balance: Decimal, cash_locked: Decimal, asset_balance: Decimal, asset_locked: Decimal) -> BrokerBalance:
    return BrokerBalance(
        cash_available=float(cash_balance),
        cash_locked=float(cash_locked),
        asset_available=float(asset_balance),
        asset_locked=float(asset_locked),
    )
