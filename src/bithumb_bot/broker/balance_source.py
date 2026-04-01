from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from ..config import settings
from .accounts_v1 import AccountsRequiredCurrencyMissingError
from .base import BrokerBalance


@dataclass(frozen=True)
class BalanceSnapshot:
    source_id: str
    observed_ts_ms: int
    balance: BrokerBalance


class BalanceSource(Protocol):
    def fetch_snapshot(self) -> BalanceSnapshot:
        ...


def fetch_balance_snapshot(broker: object) -> BalanceSnapshot:
    fetcher = getattr(broker, "get_balance_snapshot", None)
    if callable(fetcher):
        snapshot = fetcher()
        if isinstance(snapshot, BalanceSnapshot):
            return snapshot

    balance_getter = getattr(broker, "get_balance", None)
    if not callable(balance_getter):
        raise AttributeError("broker does not provide get_balance/get_balance_snapshot")
    balance = balance_getter()
    if not isinstance(balance, BrokerBalance):
        raise TypeError("broker.get_balance() returned non-BrokerBalance payload")
    return BalanceSnapshot(
        source_id="legacy_balance_api",
        observed_ts_ms=0,
        balance=balance,
    )


class DryRunBalanceSource:
    def fetch_snapshot(self) -> BalanceSnapshot:
        return BalanceSnapshot(
            source_id="dry_run_static",
            observed_ts_ms=0,
            balance=BrokerBalance(
                cash_available=settings.START_CASH_KRW,
                cash_locked=0.0,
                asset_available=0.0,
                asset_locked=0.0,
            ),
        )


class AccountsV1BalanceSource:
    SOURCE_ID = "accounts_v1_rest_snapshot"

    def __init__(
        self,
        *,
        fetch_accounts_raw: Callable[[], object],
        order_currency: str,
        payment_currency: str,
        now_ms: Callable[[], int],
        parse_accounts_response: Callable[[object], object],
        select_pair_balances: Callable[..., object],
        to_broker_balance: Callable[[object], BrokerBalance],
    ) -> None:
        self._fetch_accounts_raw = fetch_accounts_raw
        self._order_currency = str(order_currency).strip().upper()
        self._payment_currency = str(payment_currency).strip().upper()
        self._now_ms = now_ms
        self._parse_accounts_response = parse_accounts_response
        self._select_pair_balances = select_pair_balances
        self._to_broker_balance = to_broker_balance
        self._validation_diag: dict[str, object] = {
            "reason": "not_checked",
            "row_count": 0,
            "currencies": [],
            "missing_required_currencies": [],
            "duplicate_currencies": [],
            "last_success_reason": None,
            "last_failure_reason": None,
            "source": self.SOURCE_ID,
            "last_observed_ts_ms": None,
        }

    def get_validation_diagnostics(self) -> dict[str, object]:
        return dict(self._validation_diag)

    @staticmethod
    def classify_validation_reason(exc: Exception) -> str:
        if isinstance(exc, AccountsRequiredCurrencyMissingError):
            return "required currency missing"
        detail = str(exc).lower()
        if "duplicate currency row" in detail:
            return "duplicate currency"
        return "schema mismatch"

    def fetch_snapshot(self) -> BalanceSnapshot:
        response = self._fetch_accounts_raw()
        row_count = len(response) if isinstance(response, list) else 0
        currencies: list[str] = []
        if isinstance(response, list):
            for row in response:
                if not isinstance(row, dict):
                    continue
                token = str(row.get("currency") or "").strip().upper()
                if token:
                    currencies.append(token)

        parsed_accounts = None
        observed_ts_ms = self._now_ms()
        try:
            parsed_accounts = self._parse_accounts_response(response)
            pair_balances = self._select_pair_balances(
                parsed_accounts,
                order_currency=self._order_currency,
                payment_currency=self._payment_currency,
            )
        except Exception as exc:
            reason = self.classify_validation_reason(exc)
            missing_required_currencies: list[str] = []
            error_text = str(exc)
            if "missing quote currency row '" in error_text:
                missing_required_currencies.append(self._payment_currency)
            if "missing base currency row '" in error_text:
                missing_required_currencies.append(self._order_currency)
            duplicate_currencies = (
                list(parsed_accounts.duplicate_currencies)
                if parsed_accounts is not None
                else sorted({token for token in currencies if currencies.count(token) > 1})
            )
            self._validation_diag = {
                "reason": reason,
                "row_count": row_count,
                "currencies": sorted(set(currencies)),
                "missing_required_currencies": missing_required_currencies,
                "duplicate_currencies": duplicate_currencies,
                "last_success_reason": self._validation_diag.get("last_success_reason"),
                "last_failure_reason": reason,
                "source": self.SOURCE_ID,
                "last_observed_ts_ms": observed_ts_ms,
            }
            raise

        self._validation_diag = {
            "reason": "ok",
            "row_count": row_count,
            "currencies": sorted(parsed_accounts.balances.keys()) if parsed_accounts is not None else [],
            "missing_required_currencies": [],
            "duplicate_currencies": list(parsed_accounts.duplicate_currencies) if parsed_accounts is not None else [],
            "last_success_reason": "ok",
            "last_failure_reason": self._validation_diag.get("last_failure_reason"),
            "source": self.SOURCE_ID,
            "last_observed_ts_ms": observed_ts_ms,
        }
        return BalanceSnapshot(
            source_id=self.SOURCE_ID,
            observed_ts_ms=observed_ts_ms,
            balance=self._to_broker_balance(pair_balances),
        )
