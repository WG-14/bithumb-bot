from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Protocol
from ..config import prepare_db_path_for_connection, settings
from ..dust import dust_qty_gap_tolerance
from ..position_state_snapshot import build_canonical_position_snapshot
from .accounts_v1 import AccountsRequiredCurrencyMissingError
from .base import BrokerBalance, BrokerSchemaError, BrokerTemporaryError


@dataclass(frozen=True)
class BalanceSnapshot:
    source_id: str
    observed_ts_ms: int
    asset_ts_ms: int
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
        asset_ts_ms=0,
        balance=balance,
    )


class DryRunBalanceSource:
    def fetch_snapshot(self) -> BalanceSnapshot:
        return BalanceSnapshot(
            source_id="dry_run_static",
            observed_ts_ms=0,
            asset_ts_ms=0,
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
        evaluate_flat_start_safety: Callable[[], tuple[bool, str]] | None = None,
    ) -> None:
        self._fetch_accounts_raw = fetch_accounts_raw
        self._order_currency = str(order_currency).strip().upper()
        self._payment_currency = str(payment_currency).strip().upper()
        self._now_ms = now_ms
        self._parse_accounts_response = parse_accounts_response
        self._select_pair_balances = select_pair_balances
        self._to_broker_balance = to_broker_balance
        self._evaluate_flat_start_safety = evaluate_flat_start_safety or _default_flat_start_safety_check
        self._allow_missing_base = bool(
            str(settings.MODE).strip().lower() == "live"
            and bool(settings.LIVE_DRY_RUN)
            and not bool(settings.LIVE_REAL_ORDER_ARMED)
        )
        self._allow_missing_base_on_flat_start = bool(
            str(settings.MODE).strip().lower() == "live"
            and not bool(settings.LIVE_DRY_RUN)
        )
        self._flat_start_allowed = False
        self._flat_start_reason = "not_checked"
        self._execution_mode = (
            "live_dry_run_unarmed" if self._allow_missing_base else "live_real_order_path"
        )
        self._base_missing_policy = (
            "allow_zero_position_start_in_dry_run"
            if self._allow_missing_base
            else (
                "allow_flat_start_when_no_open_or_unresolved_exposure"
                if self._allow_missing_base_on_flat_start
                else "block_when_base_currency_row_missing"
            )
        )
        self._validation_diag: dict[str, object] = {
            "reason": "not_checked",
            "failure_category": "none",
            "row_count": 0,
            "currencies": [],
            "missing_required_currencies": [],
            "duplicate_currencies": [],
            "execution_mode": self._execution_mode,
            "quote_currency": self._payment_currency,
            "base_currency": self._order_currency,
            "base_currency_missing_policy": self._base_missing_policy,
            "allow_missing_base_currency": self._allow_missing_base,
            "flat_start_allowed": self._flat_start_allowed,
            "flat_start_reason": self._flat_start_reason,
            "preflight_outcome": "not_checked",
            "last_success_reason": None,
            "last_failure_reason": None,
            "source": self.SOURCE_ID,
            "last_observed_ts_ms": None,
            "last_asset_ts_ms": None,
            "last_success_ts_ms": None,
            "last_failure_ts_ms": None,
            "stale": False,
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

    @staticmethod
    def classify_failure_category(exc: Exception) -> str:
        if isinstance(exc, BrokerSchemaError):
            return "schema_mismatch"
        if isinstance(exc, BrokerTemporaryError):
            return "transport_failure"
        detail = str(exc).lower()
        if "auth" in detail or "apikey" in detail or "unauthorized" in detail:
            return "auth_failure"
        return "unknown_failure"

    def fetch_snapshot(self) -> BalanceSnapshot:
        observed_ts_ms = self._now_ms()
        allow_missing_base = self._allow_missing_base
        if self._allow_missing_base:
            self._flat_start_allowed = False
            self._flat_start_reason = "dry_run_unarmed_allowance"
        else:
            self._flat_start_allowed = False
            self._flat_start_reason = "not_applicable"
        try:
            response = self._fetch_accounts_raw()
        except Exception as exc:
            reason = str(exc).strip() or type(exc).__name__
            self._validation_diag = {
                **self._validation_diag,
                "reason": reason,
                "failure_category": self.classify_failure_category(exc),
                "flat_start_allowed": self._flat_start_allowed,
                "flat_start_reason": self._flat_start_reason,
                "preflight_outcome": "fail_transport_or_schema_unavailable",
                "last_failure_reason": reason,
                "last_failure_ts_ms": observed_ts_ms,
                "last_observed_ts_ms": observed_ts_ms,
                "stale": bool(self._validation_diag.get("last_success_ts_ms")),
            }
            raise
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
        try:
            parsed_accounts = self._parse_accounts_response(response)
            if (
                self._allow_missing_base_on_flat_start
                and self._order_currency not in parsed_accounts.balances
            ):
                self._flat_start_allowed, self._flat_start_reason = self._evaluate_flat_start_safety()
                allow_missing_base = self._flat_start_allowed
            pair_balances = self._select_pair_balances(
                parsed_accounts,
                order_currency=self._order_currency,
                payment_currency=self._payment_currency,
                allow_missing_base=allow_missing_base,
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
                "failure_category": self.classify_failure_category(exc),
                "row_count": row_count,
                "currencies": sorted(set(currencies)),
                "missing_required_currencies": missing_required_currencies,
                "duplicate_currencies": duplicate_currencies,
                "execution_mode": self._execution_mode,
                "quote_currency": self._payment_currency,
                "base_currency": self._order_currency,
                "base_currency_missing_policy": self._base_missing_policy,
                "allow_missing_base_currency": allow_missing_base,
                "flat_start_allowed": self._flat_start_allowed,
                "flat_start_reason": self._flat_start_reason,
                "preflight_outcome": "fail_real_order_blocked",
                "last_success_reason": self._validation_diag.get("last_success_reason"),
                "last_failure_reason": reason,
                "source": self.SOURCE_ID,
                "last_observed_ts_ms": observed_ts_ms,
                "last_asset_ts_ms": self._validation_diag.get("last_asset_ts_ms"),
                "last_success_ts_ms": self._validation_diag.get("last_success_ts_ms"),
                "last_failure_ts_ms": observed_ts_ms,
                "stale": bool(self._validation_diag.get("last_success_ts_ms")),
            }
            raise

        base_row_missing_allowed = bool(
            allow_missing_base
            and parsed_accounts is not None
            and self._order_currency not in parsed_accounts.balances
        )
        self._validation_diag = {
            "reason": "ok",
            "failure_category": "none",
            "row_count": row_count,
            "currencies": sorted(parsed_accounts.balances.keys()) if parsed_accounts is not None else [],
            "missing_required_currencies": [],
            "duplicate_currencies": list(parsed_accounts.duplicate_currencies) if parsed_accounts is not None else [],
            "execution_mode": self._execution_mode,
            "quote_currency": self._payment_currency,
            "base_currency": self._order_currency,
            "base_currency_missing_policy": self._base_missing_policy,
            "allow_missing_base_currency": allow_missing_base,
            "flat_start_allowed": self._flat_start_allowed,
            "flat_start_reason": self._flat_start_reason,
            "preflight_outcome": (
                "pass_no_position_allowed" if base_row_missing_allowed else "pass"
            ),
            "last_success_reason": "ok",
            "last_failure_reason": self._validation_diag.get("last_failure_reason"),
            "source": self.SOURCE_ID,
            "last_observed_ts_ms": observed_ts_ms,
            "last_asset_ts_ms": observed_ts_ms,
            "last_success_ts_ms": observed_ts_ms,
            "last_failure_ts_ms": self._validation_diag.get("last_failure_ts_ms"),
            "stale": False,
        }
        return BalanceSnapshot(
            source_id=self.SOURCE_ID,
            observed_ts_ms=observed_ts_ms,
            asset_ts_ms=observed_ts_ms,
            balance=self._to_broker_balance(pair_balances),
        )


def _default_flat_start_safety_check() -> tuple[bool, str]:
    from .. import runtime_state

    try:
        db_path = prepare_db_path_for_connection(settings.DB_PATH, mode=settings.MODE)
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except Exception as exc:
        detail = str(exc).strip() or type(exc).__name__
        return False, f"flat_start_local_state_unavailable({type(exc).__name__}: {detail})"

    try:
        unresolved_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            """
        ).fetchone()
        unresolved_count = int(unresolved_row["cnt"] if unresolved_row else 0)
        if unresolved_count > 0:
            return False, f"local_unresolved_or_open_orders={unresolved_count}"

        portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
        asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
        if abs(asset_qty) > 1e-12:
            snapshot = build_canonical_position_snapshot(
                conn,
                metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
                pair=settings.PAIR,
                portfolio_asset_qty=asset_qty,
            )
            dust_context = snapshot.dust_context
            exposure = snapshot.position_state.normalized_exposure
            if exposure.harmless_dust_effective_flat:
                return True, f"flat_start_effective_flat({dust_context.compact_summary})"
            if dust_context.classification.present:
                return False, f"flat_start_requires_operator_review({dust_context.compact_summary})"
            try:
                from . import order_rules

                rules = order_rules.get_effective_order_rules(settings.PAIR).rules
                min_qty = float(rules.min_qty)
                min_notional_krw = float(rules.min_notional_krw)
                qty_gap_tolerance = dust_qty_gap_tolerance(
                    min_qty=min_qty,
                    default_abs_tolerance=1e-12,
                )
                local_only_summary = (
                    "state=blocking_dust "
                    f"broker_qty={0.0:.8f} "
                    f"local_qty={asset_qty:.8f} "
                    f"delta_qty={-asset_qty:.8f} "
                    f"min_qty={min_qty:.8f} "
                    f"min_notional_krw={min_notional_krw:.1f} "
                    f"qty_gap_tolerance={qty_gap_tolerance:.8f} "
                    "qty_gap_small=1 "
                    "qty_below_min(broker=0 local=1) "
                    "notional_below_min(broker=0 local=0) "
                    "broker_local_match=0 "
                    "operator_action=manual_review_before_resume "
                    "new_orders_allowed=0 "
                    "resume_allowed=0 "
                    "treat_as_flat=0"
                )
                return False, f"flat_start_requires_operator_review({local_only_summary})"
            except Exception:
                return False, f"flat_start_requires_operator_review(local_position_present={asset_qty:.12f})"
    finally:
        conn.close()
    return True, "flat_start_safe"
