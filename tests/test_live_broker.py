from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError
from bithumb_bot.broker import live as live_module
from bithumb_bot.broker.live_submission_execution import (
    reconcile_apply_fills_and_refresh,
    submit_live_order_and_confirm,
)
from bithumb_bot.broker import order_rules
from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker.live import (
    _submit_contract_fields,
    adjust_buy_order_qty_for_dust_safety,
    adjust_sell_order_qty_for_dust_safety,
    live_execute_signal,
    SellDustGuardError,
    normalize_order_qty,
    validate_order,
    validate_pretrade,
)
from bithumb_bot.broker.live_submit_orchestrator import (
    LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
    StandardSubmitPipelineRequest,
    run_standard_submit_pipeline,
)
from bithumb_bot.broker.order_submit import plan_place_order
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.execution_models import OrderIntent
from bithumb_bot.dust import build_position_state_model
from bithumb_bot.lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from bithumb_bot.lifecycle import apply_fill_lifecycle
from bithumb_bot.lot_model import build_market_lot_rules, lot_count_to_qty
from bithumb_bot.oms import payload_fingerprint
from bithumb_bot.order_sizing import BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS, BuyExecutionAuthority
from bithumb_bot.db_core import ensure_db, init_portfolio, record_strategy_decision, set_portfolio_breakdown
from bithumb_bot.reason_codes import (
    DUST_RESIDUAL_SUPPRESSED,
    DUST_RESIDUAL_UNSELLABLE,
    EXIT_PARTIAL_LEFT_DUST,
    MANUAL_DUST_REVIEW_REQUIRED,
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
)
from bithumb_bot.recovery import cancel_open_orders_with_broker, reconcile_with_broker, recover_order_with_exchange_id
from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.public_api_orderbook import BestQuote
from tests.fakes import FakeMarketData


pytestmark = pytest.mark.slow_integration


class _FakeBroker:
    def __init__(self) -> None:
        self.place_order_calls = 0
        self._last_qty = 0.01
        self._last_side = "BUY"
        self._last_price = None
        self._last_client_order_id = "live_1000_buy"

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        self.place_order_calls += 1
        self._last_client_order_id = client_order_id
        self._last_side = side
        self._last_qty = qty
        self._last_price = price
        return BrokerOrder(client_order_id, "ex1", side, "NEW", price, qty, 0.0, 1, 1)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise NotImplementedError

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex1",
            self._last_side,
            "FILLED",
            self._last_price,
            self._last_qty,
            self._last_qty,
            1,
            1,
        )

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if client_order_id:
            return [
                BrokerFill(
                    client_order_id=client_order_id,
                    fill_id="f1",
                    fill_ts=1000,
                    price=100000000.0,
                    qty=self._last_qty,
                    fee=10.0,
                )
            ]
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=float(settings.START_CASH_KRW),
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []


class _DustOnlyBalanceBroker(_FakeBroker):
    def __init__(self, *, asset_available: float) -> None:
        super().__init__()
        self._asset_available = float(asset_available)

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=float(settings.START_CASH_KRW),
            cash_locked=0.0,
            asset_available=self._asset_available,
            asset_locked=0.0,
        )

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-dust-only", "SELL", "CANCELED", 0.0, 0.0, 0.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []


def _record_test_order(conn, *, client_order_id: str, side: str, qty_req: float, ts_ms: int) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty_req=qty_req,
        submit_attempt_id=f"attempt_{client_order_id}",
        price=None,
        ts_ms=ts_ms,
        status="NEW",
    )


def _stub_live_reference_quote(monkeypatch, *, price: float = 100000000.0) -> None:
    monkeypatch.setattr(
        live_module,
        "_load_live_reference_quote",
        lambda **kwargs: {
            "bid": float(price) - 1000.0,
            "ask": float(price),
            "reference_price": float(price),
            "reference_ts_epoch_sec": 1700000000.0,
            "reference_source": "test_stub",
        },
    )


def _stub_live_effective_order_rules(monkeypatch) -> None:
    monkeypatch.setattr(
        live_module,
        "_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=order_rules.DerivedOrderConstraints(
                order_types=("price",),
                bid_types=("price",),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
            )
        ),
    )


def _stub_submit_plan_quote(monkeypatch, *, price: float = 100000000.0) -> None:
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.fetch_orderbook_top",
        lambda _market: BestQuote(
            market="KRW-BTC",
            bid_price=float(price) - 1000.0,
            ask_price=float(price),
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.validated_best_quote_ask_price",
        lambda _quote, requested_market: float(price),
    )


def _standard_submit_request(
    *,
    conn,
    submit_plan,
    effective_rules,
    client_order_id: str = "live_submit_plan_test",
    side: str = "BUY",
    qty: float = 0.01,
    reference_price: float = 100000000.0,
) -> StandardSubmitPipelineRequest:
    return StandardSubmitPipelineRequest(
        conn=conn,
        submit_plan=submit_plan,
        signal=side,
        client_order_id=client_order_id,
        submit_attempt_id=f"{client_order_id}:attempt",
        side=side,
        order_qty=float(qty),
        position_qty=float(qty),
        qty=float(qty),
        ts=1000,
        intent_key=f"{client_order_id}:intent",
        market_price=float(reference_price),
        raw_total_asset_qty=0.0,
        open_exposure_qty=0.0,
        dust_tracking_qty=0.0,
        effective_rules=effective_rules,
        submit_qty_source="test.submit_qty_source",
        position_state_source="test.position_state_source",
        reference_price=float(reference_price),
        top_of_book_summary={"ask": float(reference_price), "bid": float(reference_price) - 1000.0},
        strategy_name="test_strategy",
        decision_id=None,
        decision_reason="test",
        exit_rule_name=None,
        order_type=("price" if side == "BUY" else "market"),
        contract_profile=LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
        payload_hash=payload_fingerprint(
            {
                "client_order_id": client_order_id,
                "side": side,
                "qty": float(qty),
                "ts": 1000,
            }
        ),
        internal_lot_size=float(qty),
        effective_min_trade_qty=float(qty),
        qty_step=float(qty),
        min_notional_krw=5000.0,
        intended_lot_count=1,
        executable_lot_count=1,
        final_intended_qty=float(qty),
        final_submitted_qty=float(qty),
        decision_reason_code="test_reason",
        submit_truth_source_fields={},
        submit_observability_fields={},
        sell_observability={},
    )


class _CommitCheckingBroker(_FakeBroker):
    def __init__(self, *, db_path: str) -> None:
        super().__init__()
        self._db_path = db_path

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        conn = ensure_db(self._db_path)
        try:
            row = conn.execute(
                "SELECT status FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            assert row is not None
            assert row["status"] == "PENDING_SUBMIT"
            intent = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='intent_created'",
                (client_order_id,),
            ).fetchone()
            assert intent is not None

            event = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_started'",
                (client_order_id,),
            ).fetchone()
            assert event is not None
            preflight = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_attempt_preflight'",
                (client_order_id,),
            ).fetchone()
            assert preflight is not None
        finally:
            conn.close()

        return super().place_order(
            client_order_id=client_order_id,
            side=side,
            qty=qty,
            price=price,
            submit_plan=submit_plan,
        )


class _BuyPriceNoneContractCapturingBroker(_FakeBroker):
    def __init__(self) -> None:
        super().__init__()
        self.captured_live_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        assert submit_plan is not None
        assert isinstance(submit_plan.buy_price_none_submit_contract, order_rules.BuyPriceNoneSubmitContract)
        self.captured_live_submit_contract = submit_plan.buy_price_none_submit_contract
        return super().place_order(
            client_order_id=client_order_id,
            side=side,
            qty=qty,
            price=price,
            submit_plan=submit_plan,
        )


class _TimeoutBroker(_FakeBroker):
    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        raise BrokerTemporaryError("timeout")


class _FailingSubmitBroker(_FakeBroker):
    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        raise RuntimeError("exchange rejected")


class _TransportErrorBroker(_FakeBroker):
    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        raise BrokerTemporaryError("connection reset by peer")


class _CanceledBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex1",
            self._last_side,
            "CANCELED",
            self._last_price,
            self._last_qty,
            0.0,
            1,
            1,
        )


def _seed_sell_retry_authority_state(*, db_path: str) -> int:
    conn = ensure_db(db_path)
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.00049193,
            asset_locked=0.0,
        )
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
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
        )
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
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_200,
            strategy_name="dust_exit_test",
            signal="SELL",
            reason="partial_take_profit",
            candle_ts=1_700_000_000_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0004,
                "raw_total_asset_qty": 0.00049193,
                "open_exposure_qty": 0.0004,
                "dust_tracking_qty": 0.00009193,
                "open_lot_count": 1,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 1,
                "sellable_executable_qty": 0.0004,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0004,
                "submit_lot_count": 1,
                "submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "sell_qty_basis_qty": 0.0004,
                "sell_qty_basis_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "exit_allowed": True,
                "exit_block_reason": "none",
                "terminal_state": "open_exposure",
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0004,
                        "raw_total_asset_qty": 0.00049193,
                        "open_exposure_qty": 0.0004,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
                        "sellable_executable_qty": 0.0004,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0004,
                    }
                },
            },
        )
        conn.commit()
        return decision_id
    finally:
        conn.close()


class _StrayBroker(_FakeBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [BrokerOrder("", "stray1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _NoExchangeIdBroker(_FakeBroker):
    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
        submit_plan=None,
    ) -> BrokerOrder:
        self.place_order_calls += 1
        self._last_client_order_id = client_order_id
        self._last_side = side
        self._last_qty = qty
        self._last_price = price
        return BrokerOrder(client_order_id, None, side, "NEW", price, qty, 0.0, 1, 1)


class _InvalidFeeAggregateBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if not client_order_id:
            return []
        return [
            BrokerFill(
                client_order_id=client_order_id,
                fill_id="agg_bad_fee_1",
                fill_ts=1000,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id=exchange_order_id or "ex1",
            ),
            BrokerFill(
                client_order_id=client_order_id,
                fill_id="agg_bad_fee_2",
                fill_ts=1001,
                price=100000000.0,
                qty=0.01,
                fee=float("nan"),
                exchange_order_id=exchange_order_id or "ex1",
            ),
        ]


class _CancelOpenOrdersBroker(_FakeBroker):
    def __init__(self) -> None:
        super().__init__()
        self.canceled: list[tuple[str, str | None]] = []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1),
            BrokerOrder("", "stray1", "SELL", "PARTIAL", 110.0, 0.2, 0.05, 1, 1),
        ]

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCELED", None, 0.0, 0.0, 1, 1)


class _CancelFailureBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id == "stray1":
            raise RuntimeError("cancel failed")
        return super().cancel_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


class _CancelAcceptedBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCEL_REQUESTED", None, 0.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id == "ex1":
            return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCELED", 100.0, 0.1, 0.0, 1, 2)
        return BrokerOrder(client_order_id, exchange_order_id or "stray1", "SELL", "NEW", 110.0, 0.2, 0.05, 1, 2)


class _CancelIdentifierMismatchBroker(_CancelOpenOrdersBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [BrokerOrder("live_1000_buy", "remote_mismatch_exid", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _CancelAcceptedUnknownStatusBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCEL_REQUESTED", None, 0.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 2)


class _StrictRecoveryBroker(_FakeBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id is None:
            raise AssertionError("unsafe get_order called without exchange_order_id")
        return super().get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


class _RecentActivityBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if exchange_order_id != "ex_recent_known":
            return []
        return [
            BrokerFill(
                client_order_id=client_order_id or "",
                fill_id="recent_fill_1",
                fill_ts=1002,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_recent_known",
            )
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="",
                exchange_order_id="ex_recent_known",
                side="BUY",
                status="FILLED",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.01,
                created_ts=1001,
                updated_ts=1002,
            )
        ]

class _UnmatchedRecentFillBroker(_FakeBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []


class _ConflictingRecoverySourcesBroker(_FakeBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_conflict", "BUY", "NEW", 100.0, 0.01, 0.0, 1001, 1002)
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_conflict", "BUY", "FILLED", 100.0, 0.01, 0.01, 1001, 1003)
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="recent_fill_conflict",
                fill_ts=1003,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_conflict",
            )
        ]


class _OpenOrderPreferredBroker(_FakeBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_precedence", "BUY", "NEW", 100.0, 0.02, 0.0, 2001, 2002)
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_precedence", "BUY", "PARTIAL", 100.0, 0.02, 0.01, 2001, 2003)
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []


class _BalanceMismatchBroker(_FakeBroker):
    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=900000.0,
            cash_locked=20000.0,
            asset_available=0.2,
            asset_locked=0.1,
        )


class _SubmitUnknownRecentFillBroker(_StrictRecoveryBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="ambiguous_missing_exid",
                fill_id="recent_submit_unknown_fill",
                fill_ts=1003,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_submit_unknown_fill",
            )
        ]


class _SubmitUnknownRecentOrderBroker(_StrictRecoveryBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="ambiguous_missing_exid",
                exchange_order_id="ex_submit_unknown_order",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.0,
                created_ts=1001,
                updated_ts=1002,
            )
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []




@pytest.fixture(autouse=True)
def _fake_market_data(monkeypatch):
    fake = FakeMarketData()
    from bithumb_bot.broker import live as live_module

    monkeypatch.setattr(live_module, "fetch_orderbook_top", fake.fetch_orderbook_top)
    return fake

@pytest.fixture(autouse=True)
def _reset_pretrade_guards():
    from bithumb_bot.broker import order_rules as _order_rules

    old_values = {
        "MODE": settings.MODE,
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "BUY_FRACTION": settings.BUY_FRACTION,
        "FEE_RATE": settings.FEE_RATE,
        "LIVE_FEE_RATE_ESTIMATE": settings.LIVE_FEE_RATE_ESTIMATE,
        "MAX_ORDERBOOK_SPREAD_BPS": settings.MAX_ORDERBOOK_SPREAD_BPS,
        "MAX_MARKET_SLIPPAGE_BPS": settings.MAX_MARKET_SLIPPAGE_BPS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "PRETRADE_BALANCE_BUFFER_BPS": settings.PRETRADE_BALANCE_BUFFER_BPS,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "KILL_SWITCH": settings.KILL_SWITCH,
        "MAX_OPEN_ORDER_AGE_SEC": settings.MAX_OPEN_ORDER_AGE_SEC,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS,
        "LIVE_PRICE_REFERENCE_MAX_AGE_SEC": settings.LIVE_PRICE_REFERENCE_MAX_AGE_SEC,
        "LIVE_FILL_FEE_STRICT_MODE": settings.LIVE_FILL_FEE_STRICT_MODE,
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW,
        "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW,
        "PAIR": settings.PAIR,
    }

    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "FEE_RATE", 0.0004)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "PAIR", old_values["PAIR"])
    object.__setattr__(settings, "MODE", old_values["MODE"])
    _order_rules._cached_rules.clear()

    yield

    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    object.__setattr__(settings, "MODE", "paper")
    _order_rules._cached_rules.clear()


def test_bithumb_broker_dry_run(monkeypatch):
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")
    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="LIVE_DRY_RUN=true"):
        broker.place_order(
            client_order_id="a",
            side="BUY",
            qty=0.1,
            price=None,
        )


def test_broker_auth_runtime_diagnostics_redact_secret_values(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "live-key-visible-length-only")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "super-secret-value-should-not-appear")
    object.__setattr__(settings, "BITHUMB_WS_MYASSET_ENABLED", False)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    diag = broker.get_auth_runtime_diagnostics(
        caller="test",
        env_summary={"source_key": "BITHUMB_ENV_FILE", "env_file": "/runtime/live.env", "loaded": True},
    )

    assert diag["api_key_present"] is True
    assert diag["api_key_length"] == len("live-key-visible-length-only")
    assert diag["api_secret_present"] is True
    assert diag["api_secret_length"] == len("super-secret-value-should-not-appear")
    assert "super-secret-value-should-not-appear" not in json.dumps(diag, ensure_ascii=False)
    assert diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert diag["chance_auth"]["query_hash_included"] is True
    assert diag["accounts_auth"]["endpoint"] == "/v1/accounts"
    assert diag["accounts_auth"]["query_hash_included"] is False
    assert diag["order_submit_auth"]["endpoint"] == "/v2/orders"
    assert diag["order_submit_auth"]["auth_branch"] == "order_submit_json_query_hash"
    assert diag["order_submit_auth"]["submit_path"] == "canonical_v2_orders_json_content"
    assert diag["order_submit_auth"]["submit_dispatch_authority"] == "validated_place_order_flow"
    assert diag["order_submit_auth"]["query_hash_included"] is True


def test_build_broker_with_auth_diagnostics_reuses_same_private_auth_preview_for_health_and_run(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "live-key-visible-length-only")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "super-secret-value-should-not-appear")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    health_broker, health_diag = build_broker_with_auth_diagnostics(
        caller="cmd_health",
        env_summary={"source_key": "BITHUMB_ENV_FILE_LIVE", "loaded": True},
        broker_factory=BithumbBroker,
    )
    run_broker, run_diag = build_broker_with_auth_diagnostics(
        caller="run_loop",
        env_summary={"source_key": "BITHUMB_ENV_FILE_LIVE", "loaded": True},
        broker_factory=BithumbBroker,
    )

    assert isinstance(health_broker, BithumbBroker)
    assert isinstance(run_broker, BithumbBroker)
    assert health_diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert run_diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert health_diag["chance_auth"]["query_hash_included"] is True
    assert run_diag["chance_auth"]["query_hash_included"] is True
    assert health_diag["accounts_auth"]["endpoint"] == "/v1/accounts"
    assert run_diag["accounts_auth"]["endpoint"] == "/v1/accounts"
    assert health_diag["order_submit_auth"]["submit_path"] == "canonical_v2_orders_json_content"
    assert run_diag["order_submit_auth"]["submit_dispatch_authority"] == "validated_place_order_flow"


def test_validate_pretrade_price_protection_buy_within_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="BUY",
        qty=0.001,
        market_price=100.5,
    )


def test_validate_pretrade_price_protection_buy_beyond_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 40.0)

    with pytest.raises(ValueError, match="price protection blocked BUY"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_sell_within_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="SELL",
        qty=0.001,
        market_price=100.5,
    )


def test_validate_pretrade_price_protection_sell_beyond_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 40.0)

    with pytest.raises(ValueError, match="price protection blocked SELL"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="SELL",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_blocks_missing_reference_price(monkeypatch) -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    monkeypatch.setattr("bithumb_bot.broker.live.fetch_orderbook_top", lambda _pair: (_ for _ in ()).throw(RuntimeError("no data")))

    with pytest.raises(ValueError, match="reference price unavailable"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_uses_fresh_quote_age_not_last_candle_age() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 5)
    object.__setattr__(settings, "MODE", "paper")
    runtime_state.set_last_candle_age_sec(30.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="BUY",
        qty=0.001,
        market_price=100.5,
    )

    runtime_state.set_last_candle_age_sec(None)


def test_validate_pretrade_price_protection_blocks_stale_quote_timestamp() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 5)

    with pytest.raises(ValueError, match="reference price stale"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
            reference_bid=100.0,
            reference_ask=101.0,
            reference_ts_epoch_sec=time.time() - 30.0,
            reference_source="test_quote",
        )


def test_validate_pretrade_rejects_crossed_quote() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 1.0)
    with pytest.raises(ValueError, match="invalid orderbook top: market="):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
            reference_bid=101.0,
            reference_ask=100.0,
            reference_source="test_quote",
        )


def test_validate_pretrade_rejects_non_positive_quote() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 1.0)
    with pytest.raises(ValueError, match="invalid orderbook top: market="):
        validate_pretrade(
            broker=_FakeBroker(),
            side="SELL",
            qty=0.001,
            market_price=100.5,
            reference_bid=0.0,
            reference_ask=101.0,
            reference_source="test_quote",
        )


def test_live_duplicate_intent_after_cancel_is_skipped_by_submit_dedup(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "retry_after_cancel.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_a", "attempt_b"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    broker = _CanceledBroker()
    first = live_execute_signal(broker, "BUY", 1000, 100000000.0)
    second = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert first is None
    assert second is None
    assert broker.place_order_calls == 1

    conn = ensure_db(str(tmp_path / "retry_after_cancel.sqlite"))
    try:
        rows = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1700000000000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='BUY' AND intent_ts=1000
            """,
            (settings.PAIR,),
        ).fetchone()
    finally:
        conn.close()

    assert len(rows) == 1
    assert rows[0]["status"] == "CANCELED"
    assert rows[0]["submit_attempt_id"] == "attempt_a"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == rows[0]["client_order_id"]
    assert dedup_row["order_status"] == "CANCELED"
    assert any("event=order_intent_dedup_skip" in msg for msg in notifications)


def test_live_failed_before_send_releases_dedup_for_same_intent_retry(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "terminal_resubmit_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_fail", "attempt_retry"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))

    failed_trade = live_execute_signal(_FailingSubmitBroker(), "BUY", 1000, 100000000.0)
    retried_trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)

    assert failed_trade is None
    assert retried_trade is not None

    conn = ensure_db(str(tmp_path / "terminal_resubmit_guard.sqlite"))
    try:
        rows = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1700000000000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='BUY' AND intent_ts=1000
            """,
            (settings.PAIR,),
        ).fetchone()
    finally:
        conn.close()

    assert len(rows) == 2
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["submit_attempt_id"] == "attempt_fail"
    assert rows[1]["status"] == "FILLED"
    assert rows[1]["submit_attempt_id"] == "attempt_retry"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == rows[1]["client_order_id"]
    assert dedup_row["order_status"] == "FILLED"


def test_live_sell_duplicate_intent_after_cancel_keeps_lot_native_authority(monkeypatch, tmp_path):
    db_path = str(tmp_path / "sell_retry_after_cancel.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    decision_id = _seed_sell_retry_authority_state(db_path=db_path)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_a", "attempt_b"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))

    runtime_state.record_reconcile_result(
        success=True,
        metadata={"dust_residual_present": 0, "dust_effective_flat": 0, "dust_policy_reason": "none"},
    )

    broker = _CanceledBroker()
    first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    second = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert first is None
    assert second is None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)
    assert broker._last_qty != pytest.approx(0.00049193)

    conn = ensure_db(db_path)
    try:
        order_row = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1000_sell_%'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='SELL' AND intent_ts=1000
            """,
            (settings.PAIR,),
        ).fetchone()
        submit_attempt = conn.execute(
            """
            SELECT submit_evidence
            FROM order_events
            WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert order_row is not None
    assert order_row["status"] == "CANCELED"
    assert order_row["submit_attempt_id"] == "attempt_a"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == order_row["client_order_id"]
    assert dedup_row["order_status"] == "CANCELED"
    assert submit_attempt is not None
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["sell_submit_lot_count"] == 1
    assert submit_evidence["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["observed_submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)


def test_live_sell_failed_before_send_retry_keeps_lot_native_authority(monkeypatch, tmp_path):
    db_path = str(tmp_path / "sell_terminal_resubmit_guard.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    decision_id = _seed_sell_retry_authority_state(db_path=db_path)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_fail", "attempt_retry"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))

    runtime_state.record_reconcile_result(
        success=True,
        metadata={"dust_residual_present": 0, "dust_effective_flat": 0, "dust_policy_reason": "none"},
    )

    failed_trade = live_execute_signal(
        _FailingSubmitBroker(),
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    retry_broker = _FakeBroker()
    retried_trade = live_execute_signal(
        retry_broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert failed_trade is None
    assert retried_trade is not None
    assert retry_broker._last_qty == pytest.approx(0.0004)
    assert retry_broker._last_qty != pytest.approx(0.00049193)

    conn = ensure_db(db_path)
    try:
        rows = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1000_sell_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='SELL' AND intent_ts=1000
            """,
            (settings.PAIR,),
        ).fetchone()
        submit_attempt = conn.execute(
            """
            SELECT submit_evidence
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (rows[1]["client_order_id"],),
        ).fetchone()
    finally:
        conn.close()

    assert len(rows) == 2
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["submit_attempt_id"] == "attempt_fail"
    assert rows[1]["status"] == "FILLED"
    assert rows[1]["submit_attempt_id"] == "attempt_retry"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == rows[1]["client_order_id"]
    assert dedup_row["order_status"] == "FILLED"
    assert submit_attempt is not None
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["sell_submit_lot_count"] == 1
    assert submit_evidence["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["observed_submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)

def test_live_submit_unknown_unresolved_blocks_and_persists_reason(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_gate.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    conn = ensure_db(str(tmp_path / "submit_unknown_gate.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('unknown_open',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,999,999,'submit timeout')
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "submit_unknown_gate.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=SUBMIT_UNKNOWN_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg and "reason_code=RISKY_ORDER_BLOCK" in msg and "submit_attempt_id=-" in msg for msg in notifications)
    assert any("event=order_submit_blocked" in msg and "decision_id=-" in msg for msg in notifications)
    assert any("reason_detail_code=SUBMIT_UNKNOWN_PRESENT" in msg for msg in notifications)


def test_live_open_order_guard_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "open_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    conn = ensure_db(str(tmp_path / "open_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open','ex_open','NEW','BUY',NULL,0.01,0,999,999,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)
    assert trade is None




def test_live_runtime_halt_blocks_new_order_submission(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "runtime_halt.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    runtime_state.enter_halt(
        reason_code="MANUAL_PAUSE",
        reason="manual operator pause",
        unresolved=False,
    )

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0
    assert any("event=order_submit_blocked" in msg and "status=HALTED" in msg and "timestamp=" in msg for msg in notifications)
    assert any("reason_detail_code=submission_halt" in msg for msg in notifications)

def test_live_kill_switch_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "kill_switch.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "KILL_SWITCH", True)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_daily_loss_limit_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "daily_loss.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 1000.0)

    conn = ensure_db(str(tmp_path / "daily_loss.sqlite"))
    conn.execute("INSERT INTO daily_risk(day_kst, start_equity) VALUES ('1970-01-01', 1010000.0)")
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_recovery_required_order_blocks_new_order(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    conn = ensure_db(str(tmp_path / "recovery_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('needs_recovery',NULL,'RECOVERY_REQUIRED','BUY',NULL,0.01,0,999,999,'manual recovery required')
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "recovery_guard.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=RECOVERY_REQUIRED_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg for msg in notifications)


def test_live_unresolved_open_order_blocks_and_persists_reason(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "open_guard_persisted.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    now_ms = int(time.time() * 1000)
    conn = ensure_db(str(tmp_path / "open_guard_persisted.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open','ex_open','NEW','BUY',NULL,0.01,0,?,?,NULL)
        """,
        (now_ms, now_ms),
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "open_guard_persisted.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=UNRESOLVED_OPEN_ORDER_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg for msg in notifications)


def test_live_stale_unresolved_open_order_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "stale_open_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    conn = ensure_db(str(tmp_path / "stale_open_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('stale_open','ex_open','NEW','BUY',NULL,0.01,0,1,1,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 10_000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_submit_intent_is_committed_before_remote_submit(tmp_path):
    db_path = str(tmp_path / "pre_submit_commit.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    trade = live_execute_signal(_CommitCheckingBroker(db_path=db_path), "BUY", 1000, 100000000.0)

    assert trade is not None


def test_extension_invariant_live_submit_requires_persisted_local_intent_and_preflight(tmp_path):
    db_path = str(tmp_path / "submit_invariant.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    trade = live_execute_signal(_CommitCheckingBroker(db_path=db_path), "BUY", 1000, 100000000.0)

    assert trade is not None


def test_live_success_persists_submit_attempt_record(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_success.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    _stub_live_reference_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)
    trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "submit_success.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, submit_evidence, exchange_order_id_obtained, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, submit_evidence, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert preflight is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert submit_attempt["price"] is None or float(submit_attempt["price"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "exchange_order_id=ex1" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] is None
    assert submit_attempt["timeout_flag"] == 0
    assert submit_attempt["exchange_order_id_obtained"] == 1
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["symbol"] == settings.PAIR
    assert submit_evidence["side"] == "BUY"
    assert float(submit_evidence["intended_qty"]) > 0
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_evidence["contract_profile"] == LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE
    assert submit_evidence["submit_phase"] == "confirmation"
    assert submit_evidence["execution_state"] == "broker_response_received"
    assert submit_evidence["execution_trace_id"] == row["client_order_id"]
    assert submit_evidence["submit_plan_id"] == f"{row['client_order_id']}:plan"
    assert submit_evidence["signed_request_id"] == f"{row['client_order_id']}:signed_request"
    assert submit_evidence["submission_id"] == f"{row['client_order_id']}:submission"
    assert submit_evidence["confirmation_id"] == f"{row['client_order_id']}:confirmation"
    assert submit_evidence["submit_mode"] == settings.MODE
    assert submit_evidence["exchange_order_type"] == "price"
    assert submit_evidence["exchange_submit_field"] == "price"
    assert submit_evidence["submit_contract_kind"] == "market_buy_notional"
    assert submit_evidence["buy_price_none_allowed"] is True
    assert submit_evidence["buy_price_none_decision_outcome"] == "pass"
    assert submit_evidence["buy_price_none_decision_basis"] == "raw"
    assert submit_evidence["buy_price_none_alias_used"] is False
    assert submit_evidence["buy_price_none_alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert submit_evidence["buy_price_none_block_reason"] == ""
    assert submit_evidence["buy_price_none_support_source"] == "bid_types"
    assert submit_evidence["buy_price_none_raw_supported_types"] == ["price"]
    assert submit_evidence["buy_price_none_resolved_order_type"] == "price"
    assert submit_evidence["internal_executable_qty"] == pytest.approx(float(submit_evidence["normalized_qty"]))
    assert submit_evidence["exchange_submit_notional_krw"] is None
    assert submit_evidence["submit_failure_category"] == "none"
    assert submit_evidence["request_ts"] is not None
    assert submit_evidence["response_ts"] is not None
    assert int(submit_evidence["response_ts"]) >= int(submit_evidence["request_ts"])
    preflight_evidence = json.loads(str(preflight["submit_evidence"]))
    assert preflight_evidence["exchange_order_type"] == "price"
    assert preflight_evidence["exchange_submit_field"] == "price"
    assert preflight_evidence["submit_contract_kind"] == "market_buy_notional"
    assert preflight_evidence["contract_profile"] == LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE
    assert preflight_evidence["submit_phase"] == "planning"
    assert preflight_evidence["execution_state"] == "validated_pre_submit"
    assert preflight_evidence["execution_trace_id"] == row["client_order_id"]
    assert preflight_evidence["submit_plan_id"] == f"{row['client_order_id']}:plan"
    assert preflight_evidence["signed_request_id"] == f"{row['client_order_id']}:signed_request"
    assert preflight_evidence["submission_id"] == f"{row['client_order_id']}:submission"
    assert preflight_evidence["confirmation_id"] == f"{row['client_order_id']}:confirmation"
    assert preflight_evidence["buy_price_none_allowed"] is True
    assert preflight_evidence["buy_price_none_decision_outcome"] == "pass"
    assert preflight_evidence["buy_price_none_decision_basis"] == "raw"
    assert preflight_evidence["buy_price_none_alias_used"] is False
    assert preflight_evidence["buy_price_none_alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert preflight_evidence["buy_price_none_block_reason"] == ""
    assert preflight_evidence["buy_price_none_support_source"] == "bid_types"
    assert preflight_evidence["buy_price_none_raw_supported_types"] == ["price"]
    assert preflight_evidence["buy_price_none_resolved_order_type"] == "price"
    assert preflight_evidence["internal_executable_qty"] == pytest.approx(float(preflight_evidence["normalized_qty"]))
    assert preflight_evidence["exchange_submit_notional_krw"] is None
    assert preflight_evidence["submit_failure_category"] == "none"
    assert preflight_evidence["request_ts"] is None
    assert preflight_evidence["response_ts"] is None
    assert preflight["submit_attempt_id"] == submit_attempt["submit_attempt_id"]
    assert preflight["symbol"] == settings.PAIR
    assert preflight["side"] == "BUY"
    assert float(preflight["qty"]) > 0
    assert preflight["price"] is None or float(preflight["price"]) > 0
    assert int(preflight["submit_ts"]) == 1000
    assert preflight["order_status"] == "PENDING_SUBMIT"
    expected_fp = payload_fingerprint(
        {
            "client_order_id": row["client_order_id"],
            "submit_attempt_id": submit_attempt["submit_attempt_id"],
            "symbol": settings.PAIR,
            "side": "BUY",
            "qty": float(submit_attempt["qty"]),
            "price": (float(preflight["price"]) if preflight["price"] is not None else None),
            "submit_ts": 1000,
        }
    )
    assert preflight["payload_fingerprint"] == expected_fp
    assert submit_attempt["payload_fingerprint"] == expected_fp


def test_live_timeout_marks_submit_unknown(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    _stub_live_reference_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_TimeoutBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_unknown.sqlite"))
    row = conn.execute("SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1").fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, submit_evidence, exchange_order_id_obtained, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, submit_ts, payload_fingerprint, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert "submit unknown" in str(row["last_error"])
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert submit_attempt["price"] is None or float(submit_attempt["price"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "submit_exception=BrokerTemporaryError" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] == "BrokerTemporaryError"
    assert submit_attempt["submission_reason_code"] == "sent_but_response_timeout"
    assert submit_attempt["timeout_flag"] == 1
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert submit_attempt["order_status"] == "SUBMIT_UNKNOWN"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_evidence["error_class"] == "BrokerTemporaryError"
    assert "timeout" in str(submit_evidence["error_summary"])
    assert int(submit_evidence["response_ts"]) >= int(submit_evidence["request_ts"])
    assert preflight is not None
    assert preflight["submit_attempt_id"] == submit_attempt["submit_attempt_id"]
    assert preflight["order_status"] == "PENDING_SUBMIT"
    assert preflight["payload_fingerprint"] == submit_attempt["payload_fingerprint"]
    assert transition is not None
    assert transition["order_status"] == "SUBMIT_UNKNOWN"
    assert "from=PENDING_SUBMIT" in str(transition["message"])
    assert "to=SUBMIT_UNKNOWN" in str(transition["message"])
    assert any("event=order_submit_started" in msg for msg in notifications)
    assert any("event=order_submit_unknown" in msg and "reason_code=SUBMIT_TIMEOUT" in msg and "state_to=SUBMIT_UNKNOWN" in msg for msg in notifications)


def test_live_execute_signal_buy_price_none_preflight_and_submit_use_same_contract(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_price_none_shared_contract.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr(
        live_module,
        "_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.fetch_orderbook_top",
        lambda _market: BestQuote(market="KRW-BTC", bid_price=99_900_000.0, ask_price=100_000_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )

    broker = _BuyPriceNoneContractCapturingBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "buy_price_none_shared_contract.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    preflight_evidence = json.loads(str(preflight["submit_evidence"]))
    contract_keys = (
        "chance_validation_order_type",
        "chance_supported_order_types",
        "buy_price_none_allowed",
        "buy_price_none_decision_outcome",
        "buy_price_none_decision_basis",
        "buy_price_none_alias_used",
        "buy_price_none_alias_policy",
        "buy_price_none_block_reason",
        "buy_price_none_support_source",
        "buy_price_none_raw_supported_types",
        "buy_price_none_resolved_order_type",
        "exchange_submit_field",
        "exchange_order_type",
        "market",
        "order_side",
    )
    expected_contract = {key: preflight_evidence[key] for key in contract_keys}
    assert expected_contract == {key: submit_evidence[key] for key in contract_keys}
    assert broker.captured_live_submit_contract is not None
    assert expected_contract == {
        key: order_rules.serialize_buy_price_none_submit_contract(
            broker.captured_live_submit_contract,
            market=settings.PAIR,
            order_side="BUY",
        )[key]
        for key in contract_keys
    }


def test_bithumb_broker_buy_price_none_accepts_matching_live_submit_contract(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)

    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _market: BestQuote(market="KRW-BTC", bid_price=99_900_000.0, ask_price=100_000_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )

    captured: dict[str, object] = {}

    def _fake_post_private(_endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict[str, object]:
        captured["retry_safe"] = retry_safe
        captured["payload"] = dict(payload)
        return {"status": "0000", "data": {"order_id": "ex-live-contract", "client_order_id": payload["client_order_id"]}}

    broker = BithumbBroker()
    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    expected_contract = order_rules.build_buy_price_none_submit_contract(
        rules=resolved_rules,
        resolution=order_rules.resolve_buy_price_none_resolution(rules=resolved_rules),
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-match",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.0008,
            price=None,
            created_ts=1000,
            submit_contract=expected_contract,
            market_price_hint=100_000_000.0,
            trace_id="cid-match",
        ),
        rules=resolved_rules,
        skip_qty_revalidation=True,
    )
    order = broker.place_order(
        client_order_id="cid-match",
        side="BUY",
        qty=0.0008,
        price=None,
        submit_plan=submit_plan,
    )

    assert order.exchange_order_id == "ex-live-contract"
    assert captured["retry_safe"] is False
    assert captured["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": expected_contract.exchange_order_type,
        "price": "80000",
        "client_order_id": "cid-match",
    }


def test_bithumb_broker_buy_price_none_blocks_market_alias_without_explicit_support(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")

    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        bid_types=("market",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=5000.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=10.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    dispatch_attempted = False

    def _unexpected_post_private(_endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict[str, object]:
        nonlocal dispatch_attempted
        dispatch_attempted = True
        return {"status": "0000", "data": {"order_id": "should-not-dispatch", "client_order_id": payload["client_order_id"]}}

    monkeypatch.setattr(broker, "_post_private", _unexpected_post_private)
    submit_contract = order_rules.build_buy_price_none_submit_contract(
        rules=resolved_rules,
        resolution=order_rules.resolve_buy_price_none_resolution(rules=resolved_rules),
    )
    with pytest.raises(BrokerRejectError, match="BUY price=None before submit") as excinfo:
        plan_place_order(
            broker,
            intent=OrderIntent(
                client_order_id="cid-market-alias-blocked",
                market="KRW-BTC",
                side="BUY",
                normalized_side="bid",
                qty=0.001,
                price=None,
                created_ts=1000,
                submit_contract=submit_contract,
                market_price_hint=100_000_000.0,
                trace_id="cid-market-alias-blocked",
            ),
            rules=resolved_rules,
            skip_qty_revalidation=True,
        )

    assert "reason=buy_price_none_requires_explicit_price_support" in str(excinfo.value)
    assert "raw_supported_types=['market']" in str(excinfo.value)
    assert dispatch_attempted is False


def test_bithumb_broker_buy_price_none_uses_same_contract_object_for_validation_routing_and_diagnostics(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)

    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    captured: dict[str, object] = {}

    def _fake_post_private(_endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict[str, object]:
        captured["payload"] = dict(payload)
        return {"status": "0000", "data": {"order_id": "ex-reused-contract", "client_order_id": payload["client_order_id"]}}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _market: BestQuote(market="KRW-BTC", bid_price=99_900_000.0, ask_price=100_000_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )
    resolution = order_rules.BuyPriceNoneResolution(
        allowed=True,
        resolved_order_type="price",
        decision_basis="sentinel_basis",
        alias_used=False,
        alias_policy="sentinel_alias_policy",
        block_reason="",
        raw_supported_types=("price",),
        support_source="bid_types",
    )
    submit_contract = order_rules.BuyPriceNoneSubmitContract(
        resolution=resolution,
        chance_validation_order_type="price",
        chance_supported_order_types=("price",),
        exchange_submit_field="price",
        exchange_order_type="price",
    )
    validated: dict[str, object] = {}
    original_validate = order_rules.validate_buy_price_none_submit_contract
    original_validate_contract = order_rules.validate_buy_price_none_order_chance_contract

    def _capture_validate(*, submit_contract: order_rules.BuyPriceNoneSubmitContract) -> None:
        validated["contract"] = submit_contract
        original_validate(submit_contract=submit_contract)

    def _capture_validate_contract(
        *,
        rules: order_rules.DerivedOrderConstraints,
        submit_contract: order_rules.BuyPriceNoneSubmitContract,
    ) -> None:
        validated["chance_contract"] = submit_contract
        original_validate_contract(rules=rules, submit_contract=submit_contract)

    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.validate_buy_price_none_submit_contract",
        _capture_validate,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.validate_buy_price_none_order_chance_contract",
        _capture_validate_contract,
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-reused-contract",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.0008,
            price=None,
            created_ts=1000,
            submit_contract=submit_contract,
            market_price_hint=100_000_000.0,
            trace_id="cid-reused-contract",
        ),
        rules=resolved_rules,
        skip_qty_revalidation=True,
    )
    order = broker.place_order(
        client_order_id="cid-reused-contract",
        side="BUY",
        qty=0.0008,
        price=None,
        submit_plan=submit_plan,
    )
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=resolved_rules,
        submit_contract=submit_contract,
    )

    assert order.exchange_order_id == "ex-reused-contract"
    assert validated["contract"] is submit_contract
    assert validated["chance_contract"] is submit_contract
    assert captured["payload"]["order_type"] == submit_contract.exchange_order_type
    assert order.submit_contract_context is not None
    assert order.submit_contract_context["buy_price_none_decision_basis"] == "sentinel_basis"
    assert order.submit_contract_context["buy_price_none_alias_policy"] == "sentinel_alias_policy"
    assert diagnostic_fields["decision_basis"] == "sentinel_basis"
    assert diagnostic_fields["alias_policy"] == "sentinel_alias_policy"


def test_bithumb_broker_rejects_missing_live_submit_plan_before_dispatch(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")

    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    dispatch_attempted = False

    def _unexpected_post_private(_endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict[str, object]:
        nonlocal dispatch_attempted
        dispatch_attempted = True
        return {"status": "0000", "data": {"order_id": "should-not-dispatch", "client_order_id": payload["client_order_id"]}}

    monkeypatch.setattr(broker, "_post_private", _unexpected_post_private)

    with pytest.raises(BrokerRejectError, match="explicit SubmitPlan"):
        broker.place_order(client_order_id="cid-missing", side="BUY", qty=0.001, price=None)

    assert dispatch_attempted is False


def test_bithumb_broker_rejects_mismatched_submit_plan_inputs_before_dispatch(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")

    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-planned",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.001,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=resolved_rules),
            market_price_hint=100000000.0,
            trace_id="cid-planned",
        ),
        rules=resolved_rules,
        skip_qty_revalidation=True,
    )

    with pytest.raises(BrokerRejectError, match="submit_plan client_order_id mismatch"):
        broker.place_order(
            client_order_id="cid-requested",
            side="BUY",
            qty=0.001,
            price=None,
            submit_plan=submit_plan,
        )


def test_buy_price_none_diagnostics_reuse_submit_contract_fields() -> None:
    resolved_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )

    resolution = order_rules.resolve_buy_price_none_resolution(rules=resolved_rules)
    submit_contract = order_rules.build_buy_price_none_submit_contract(
        rules=resolved_rules,
        resolution=resolution,
    )
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=resolved_rules,
        submit_contract=submit_contract,
    )
    submit_context = order_rules.serialize_buy_price_none_submit_contract(submit_contract)

    assert diagnostic_fields["raw_buy_supported_types"] == submit_context["buy_price_none_raw_supported_types"]
    assert diagnostic_fields["support_source"] == submit_context["buy_price_none_support_source"]
    assert diagnostic_fields["resolved_order_type"] == submit_context["buy_price_none_resolved_order_type"]
    assert diagnostic_fields["submit_field"] == submit_context["exchange_submit_field"]
    assert diagnostic_fields["allowed"] == submit_context["buy_price_none_allowed"]
    assert diagnostic_fields["decision_outcome"] == submit_context["buy_price_none_decision_outcome"]
    assert diagnostic_fields["decision_basis"] == submit_context["buy_price_none_decision_basis"]
    assert diagnostic_fields["alias_used"] == submit_context["buy_price_none_alias_used"]
    assert diagnostic_fields["alias_policy"] == submit_context["buy_price_none_alias_policy"]
    assert diagnostic_fields["block_reason"] == "-"


def test_buy_price_none_order_chance_contract_rejects_validation_type_drift() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )
    submit_contract = order_rules.BuyPriceNoneSubmitContract(
        resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
        chance_validation_order_type="market",
        chance_supported_order_types=("price",),
        exchange_submit_field="price",
        exchange_order_type="price",
    )

    with pytest.raises(BrokerRejectError, match="chance validation mismatch"):
        order_rules.validate_buy_price_none_order_chance_contract(
            rules=rules,
            submit_contract=submit_contract,
        )


def test_submit_evidence_exposes_generic_buy_price_none_contract_fields() -> None:
    contract = order_rules.BuyPriceNoneSubmitContract(
        resolution=order_rules.BuyPriceNoneResolution(
            allowed=True,
            resolved_order_type="price",
            decision_basis="raw",
            alias_used=False,
            alias_policy=order_rules.BUY_PRICE_NONE_ALIAS_POLICY,
            block_reason="",
            raw_supported_types=("price",),
            support_source="bid_types",
        ),
        chance_validation_order_type="price",
        chance_supported_order_types=("price",),
        exchange_submit_field="price",
        exchange_order_type="price",
    ).with_execution_fields(
        exchange_submit_notional_krw=120000.0,
        internal_executable_qty=0.0012,
    )

    fields = _submit_contract_fields(
        side="BUY",
        order_type="price",
        normalized_qty=0.0012,
        contract_context=contract,
    )

    assert fields["raw_buy_supported_types"] == ["price"]
    assert fields["support_source"] == "bid_types"
    assert fields["decision_basis"] == "raw"
    assert fields["alias_used"] is False
    assert fields["alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert fields["block_reason"] == ""
    assert fields["resolved_order_type"] == "price"
    assert fields["resolved_contract"] == contract.resolved_contract
    assert fields["contract_id"] == contract.contract_id
    assert fields["submit_field"] == "price"
    assert fields["buy_price_none_raw_supported_types"] == fields["raw_buy_supported_types"]
    assert fields["buy_price_none_support_source"] == fields["support_source"]
    assert fields["buy_price_none_decision_basis"] == fields["decision_basis"]
    assert fields["buy_price_none_alias_used"] == fields["alias_used"]
    assert fields["buy_price_none_alias_policy"] == fields["alias_policy"]
    assert fields["buy_price_none_resolved_order_type"] == fields["resolved_order_type"]


def test_submit_contract_fields_do_not_infer_market_buy_policy_without_planned_context() -> None:
    fields = _submit_contract_fields(
        side="BUY",
        order_type="price",
        normalized_qty=0.0012,
        contract_context=None,
    )

    assert fields["submit_contract_kind"] == "-"
    assert fields["exchange_submit_field"] == "volume"
    assert fields["buy_price_none_alias_policy"] is None


@pytest.mark.fast_regression
def test_live_execute_signal_buy_reject_does_not_classify_market_notional_submit_as_qty_step_mismatch(tmp_path):
    class _RejectingBuyBroker(_FakeBroker):
        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
            submit_plan=None,
        ) -> BrokerOrder:
            raise BrokerRejectError(f"{side} qty does not match qty_step: qty={qty} qty_step=0.0001")

    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_notional_reject.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    trade = live_execute_signal(_RejectingBuyBroker(), "BUY", 1000, 100_000_000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "buy_notional_reject.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row["status"] == "FAILED"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    preflight_evidence = json.loads(str(preflight["submit_evidence"]))
    assert submit_evidence["exchange_order_type"] == "price"
    assert submit_evidence["exchange_submit_field"] == "price"
    assert submit_evidence["submit_contract_kind"] == "market_buy_notional"
    assert submit_evidence["submit_failure_category"] == "broker_reject"
    assert "qty_step_mismatch" not in json.dumps(submit_evidence, sort_keys=True)
    assert "sell_failure_category" not in submit_evidence
    assert preflight_evidence["exchange_order_type"] == "price"
    assert preflight_evidence["exchange_submit_field"] == "price"
    assert preflight_evidence["submit_contract_kind"] == "market_buy_notional"
    assert "qty_step_mismatch" not in json.dumps(preflight_evidence, sort_keys=True)


@pytest.mark.fast_regression
def test_live_execute_signal_buy_chance_order_type_reject_is_not_qty_step_mismatch(monkeypatch, tmp_path):
    class _RejectingBuyBroker(_FakeBroker):
        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
            submit_plan=None,
        ) -> BrokerOrder:
            raise BrokerRejectError(
                "/v1/orders/chance rejected order type before submit: "
                "order_type=price supported=['limit', 'market']"
            )

    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_chance_order_type_reject.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    _stub_submit_plan_quote(monkeypatch)
    monkeypatch.setattr(
        live_module,
        "_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=order_rules.DerivedOrderConstraints(
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=("limit", "market"),
                order_sides=("bid", "ask"),
                bid_types=("market",),
                ask_types=("limit", "market"),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
            )
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=("limit", "market"),
                order_sides=("bid", "ask"),
                bid_types=("market",),
                ask_types=("limit", "market"),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )

    trade = live_execute_signal(_RejectingBuyBroker(), "BUY", 1000, 100_000_000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "buy_chance_order_type_reject.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row["status"] == "FAILED"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert preflight is None
    assert submit_evidence["submit_phase"] == "planning"
    assert submit_evidence["buy_price_none_allowed"] is False
    assert submit_evidence["buy_price_none_decision_outcome"] == "block"
    assert submit_evidence["buy_price_none_decision_basis"] == "raw"
    assert submit_evidence["buy_price_none_alias_used"] is False
    assert submit_evidence["buy_price_none_alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert submit_evidence["buy_price_none_block_reason"] == "buy_price_none_requires_explicit_price_support"
    assert submit_evidence["buy_price_none_support_source"] == "bid_types"
    assert submit_evidence["buy_price_none_raw_supported_types"] == ["market"]
    assert submit_evidence["submit_failure_category"] == "broker_reject"
    assert "qty_step_mismatch" not in json.dumps(submit_evidence, sort_keys=True)


def test_live_submit_error_marks_failed_and_records_submit_started(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_failed.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    _stub_live_reference_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_FailingSubmitBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_failed.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    started_event = conn.execute(
        "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_started'",
        (row["client_order_id"],),
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, exchange_order_id_obtained, order_status, submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FAILED"
    assert "submit failed" in str(row["last_error"])
    assert started_event is not None
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "submit_exception=RuntimeError" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] == "RuntimeError"
    assert submit_attempt["submission_reason_code"] == "failed_before_send"
    assert submit_attempt["timeout_flag"] == 0
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert submit_attempt["order_status"] == "FAILED"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["submit_phase"] == "submission"
    assert submit_evidence["execution_state"] == "dispatch_attempted"
    assert transition is not None
    assert transition["order_status"] == "FAILED"
    assert "from=PENDING_SUBMIT" in str(transition["message"])
    assert "to=FAILED" in str(transition["message"])
    assert any("event=order_submit_started" in msg for msg in notifications)
    assert any("event=order_submit_failed" in msg for msg in notifications)


def test_live_planning_failure_is_recorded_before_broker_dispatch(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_planning_failed.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    _stub_live_reference_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)

    def _unexpected_request_construction(**_kwargs):
        raise AssertionError("live submit request must not be constructed before planning succeeds")

    monkeypatch.setattr(
        "bithumb_bot.broker.live_submission_execution.StandardSubmitPipelineRequest",
        _unexpected_request_construction,
    )

    monkeypatch.setattr(
        "bithumb_bot.broker.live_submission_execution.build_live_submit_plan",
        lambda **kwargs: (_ for _ in ()).throw(BrokerRejectError("planner contract mismatch")),
    )

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)
    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "submit_planning_failed.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, last_error, local_intent_state FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, submission_reason_code, exception_class, order_status, submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    started_event = conn.execute(
        "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_started'",
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row["status"] == "FAILED"
    assert row["local_intent_state"] == "PLAN_REJECTED"
    assert "submit planning failed" in str(row["last_error"])
    assert submit_attempt is not None
    assert submit_attempt["submission_reason_code"] == "failed_before_send"
    assert submit_attempt["exception_class"] == "BrokerRejectError"
    assert submit_attempt["order_status"] == "FAILED"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["submit_phase"] == "planning"
    assert submit_evidence["execution_state"] == "planning_failed"
    assert submit_evidence["execution_trace_id"] == row["client_order_id"]
    assert submit_evidence["submit_plan_id"] == f"{row['client_order_id']}:plan"
    assert submit_evidence["signed_request_id"] == f"{row['client_order_id']}:signed_request"
    assert submit_evidence["submission_id"] == f"{row['client_order_id']}:submission"
    assert submit_evidence["confirmation_id"] == f"{row['client_order_id']}:confirmation"
    assert submit_evidence["error_summary"] == "planner contract mismatch"
    assert started_event is None


def test_run_standard_submit_pipeline_rejects_missing_explicit_submit_plan(tmp_path):
    db_path = tmp_path / "missing_submit_plan.sqlite"
    conn = ensure_db(str(db_path))
    try:
        request = _standard_submit_request(
            conn=conn,
            submit_plan=None,
            effective_rules=order_rules.DerivedOrderConstraints(
                market_id="KRW-BTC",
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                order_types=("price", "market", "limit"),
                bid_types=("price",),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
            ),
        )
        broker = _FakeBroker()

        order = run_standard_submit_pipeline(broker=broker, request=request)

        assert order is None
        assert broker.place_order_calls == 0
        row = conn.execute(
            "SELECT status, local_intent_state, last_error FROM orders WHERE client_order_id=?",
            (request.client_order_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "FAILED"
        assert row["local_intent_state"] == "PLAN_REJECTED"
        assert "explicit submit_plan" in str(row["last_error"])
    finally:
        conn.close()


def test_run_standard_submit_pipeline_dispatches_valid_explicit_submit_plan(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    db_path = tmp_path / "explicit_submit_plan.sqlite"
    conn = ensure_db(str(db_path))
    broker = _CommitCheckingBroker(db_path=str(db_path))
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="live_submit_plan_explicit",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.01,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
            market_price_hint=100000000.0,
            trace_id="live_submit_plan_explicit",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    request = _standard_submit_request(
        conn=conn,
        submit_plan=submit_plan,
        effective_rules=rules,
        client_order_id="live_submit_plan_explicit",
    )

    try:
        order = run_standard_submit_pipeline(broker=broker, request=request)

        assert order is not None
        assert broker.place_order_calls == 1
        assert order.client_order_id == "live_submit_plan_explicit"
    finally:
        conn.close()


def test_run_standard_submit_pipeline_rejects_non_planning_submit_plan_metadata(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    db_path = tmp_path / "explicit_submit_plan_bad_phase.sqlite"
    conn = ensure_db(str(db_path))
    broker = _CommitCheckingBroker(db_path=str(db_path))
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    submit_plan = replace(
        plan_place_order(
            broker,
            intent=OrderIntent(
                client_order_id="live_submit_plan_bad_phase",
                market="KRW-BTC",
                side="BUY",
                normalized_side="bid",
                qty=0.01,
                price=None,
                created_ts=1000,
                submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
                market_price_hint=100000000.0,
                trace_id="live_submit_plan_bad_phase",
            ),
            rules=rules,
            skip_qty_revalidation=True,
        ),
        phase_identity="signed_request",
    )
    request = _standard_submit_request(
        conn=conn,
        submit_plan=submit_plan,
        effective_rules=rules,
        client_order_id="live_submit_plan_bad_phase",
    )

    try:
        order = run_standard_submit_pipeline(broker=broker, request=request)

        assert order is None
        assert broker.place_order_calls == 0
        row = conn.execute(
            "SELECT status, local_intent_state, last_error FROM orders WHERE client_order_id=?",
            (request.client_order_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "FAILED"
        assert row["local_intent_state"] == "PLAN_REJECTED"
        assert "phase identity invalid" in str(row["last_error"])
    finally:
        conn.close()


def test_run_standard_submit_pipeline_rejects_unknown_contract_profile(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    db_path = tmp_path / "explicit_submit_plan_bad_contract.sqlite"
    conn = ensure_db(str(db_path))
    broker = _CommitCheckingBroker(db_path=str(db_path))
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="live_submit_plan_bad_contract",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.01,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
            market_price_hint=100000000.0,
            trace_id="live_submit_plan_bad_contract",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    request = replace(
        _standard_submit_request(
            conn=conn,
            submit_plan=submit_plan,
            effective_rules=rules,
            client_order_id="live_submit_plan_bad_contract",
        ),
        contract_profile="legacy_bool_combo",
    )

    try:
        order = run_standard_submit_pipeline(broker=broker, request=request)

        assert order is None
        assert broker.place_order_calls == 0
        row = conn.execute(
            "SELECT status, local_intent_state, last_error FROM orders WHERE client_order_id=?",
            (request.client_order_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "FAILED"
        assert row["local_intent_state"] == "PLAN_REJECTED"
        assert "contract profile invalid" in str(row["last_error"])
    finally:
        conn.close()


def test_live_submit_phase_progression_is_queryable(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_phase_progression.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    _stub_live_reference_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)

    trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "submit_phase_progression.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    phases = conn.execute(
        """
        SELECT
            event_type,
            submit_phase,
            submit_plan_id,
            signed_request_id,
            submission_id,
            confirmation_id,
            broker_response_summary,
            submit_evidence
        FROM order_events
        WHERE client_order_id=?
          AND event_type IN (
              'submit_attempt_preflight',
              'submit_attempt_signed',
              'submit_started',
              'submit_attempt_recorded',
              'submit_attempt_application'
          )
        ORDER BY id ASC
        """,
        (row["client_order_id"],),
    ).fetchall()
    conn.close()

    assert [str(phase["event_type"]) for phase in phases] == [
        "submit_started",
        "submit_attempt_preflight",
        "submit_attempt_signed",
        "submit_attempt_recorded",
        "submit_attempt_application",
    ]
    assert phases[1]["submit_phase"] == "planning"
    assert phases[2]["submit_phase"] == "signed_request"
    assert phases[3]["submit_phase"] == "confirmation"
    assert phases[4]["submit_phase"] == "application"
    assert phases[1]["submit_plan_id"] == f"{row['client_order_id']}:plan"
    assert phases[1]["signed_request_id"] == f"{row['client_order_id']}:signed_request"
    assert phases[1]["submission_id"] == f"{row['client_order_id']}:submission"
    assert phases[1]["confirmation_id"] == f"{row['client_order_id']}:confirmation"
    preflight_evidence = json.loads(str(phases[1]["submit_evidence"]))
    signed_evidence = json.loads(str(phases[2]["submit_evidence"]))
    confirmation_evidence = json.loads(str(phases[3]["submit_evidence"]))
    application_evidence = json.loads(str(phases[4]["submit_evidence"]))
    assert preflight_evidence["submit_phase"] == "planning"
    assert signed_evidence["submit_phase"] == "signed_request"
    assert confirmation_evidence["submit_phase"] == "confirmation"
    assert application_evidence["submit_phase"] == "application"
    assert application_evidence["execution_state"] == "application_completed"


def test_run_standard_submit_pipeline_uses_submit_plan_order_type_over_request_copy(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    db_path = tmp_path / "submit_plan_order_type_authority.sqlite"
    conn = ensure_db(str(db_path))
    broker = _CommitCheckingBroker(db_path=str(db_path))
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="live_submit_plan_order_type_authority",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.01,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
            market_price_hint=100000000.0,
            trace_id="live_submit_plan_order_type_authority",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    request = replace(
        _standard_submit_request(
            conn=conn,
            submit_plan=submit_plan,
            effective_rules=rules,
            client_order_id="live_submit_plan_order_type_authority",
        ),
        order_type="limit",
    )

    try:
        order = run_standard_submit_pipeline(broker=broker, request=request)

        assert order is not None
        row = conn.execute(
            "SELECT order_type FROM orders WHERE client_order_id=?",
            (request.client_order_id,),
        ).fetchone()
        preflight = conn.execute(
            """
            SELECT submit_evidence
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_preflight'
            ORDER BY id DESC
            LIMIT 1
            """,
            (request.client_order_id,),
        ).fetchone()
        recorded = conn.execute(
            """
            SELECT submit_evidence
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (request.client_order_id,),
        ).fetchone()

        assert row is not None
        assert row["order_type"] == "price"
        preflight_evidence = json.loads(str(preflight["submit_evidence"]))
        recorded_evidence = json.loads(str(recorded["submit_evidence"]))
        assert preflight_evidence["order_type"] == "price"
        assert preflight_evidence["exchange_order_type"] == "price"
        assert recorded_evidence["order_type"] == "price"
        assert recorded_evidence["exchange_order_type"] == "price"
    finally:
        conn.close()


def test_submit_live_order_and_confirm_does_not_run_reconcile_application(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    db_path = tmp_path / "submit_stage_only.sqlite"
    conn = ensure_db(str(db_path))
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )

    class _NoReconcileBroker(_FakeBroker):
        def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None):
            raise AssertionError("submit-and-confirm stage must not fetch fills")

    broker = _NoReconcileBroker()
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="live_submit_stage_only",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.01,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
            market_price_hint=100000000.0,
            trace_id="live_submit_stage_only",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    request = _standard_submit_request(
        conn=conn,
        submit_plan=submit_plan,
        effective_rules=rules,
        client_order_id="live_submit_stage_only",
    )

    try:
        submission = submit_live_order_and_confirm(
            broker=broker,
            request=request,
            intent_key=request.intent_key,
            strategy_name=request.strategy_name,
            decision_id=request.decision_id,
            decision_reason=request.decision_reason,
            exit_rule_name=request.exit_rule_name,
        )

        assert submission is not None
        assert submission.client_order_id == request.client_order_id
        assert submission.exchange_order_id == "ex1"
        assert broker.place_order_calls == 1
    finally:
        conn.close()


def test_reconcile_apply_fills_and_refresh_runs_against_confirmed_submission(monkeypatch, tmp_path):
    _stub_live_reference_quote(monkeypatch)
    object.__setattr__(settings, "START_CASH_KRW", 2000000.0)
    db_path = tmp_path / "reconcile_stage.sqlite"
    conn = ensure_db(str(db_path))
    broker = _FakeBroker()
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("price", "market", "limit"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="live_reconcile_stage",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=0.01,
            price=None,
            created_ts=1000,
            submit_contract=order_rules.build_buy_price_none_submit_contract(rules=rules),
            market_price_hint=100000000.0,
            trace_id="live_reconcile_stage",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    request = _standard_submit_request(
        conn=conn,
        submit_plan=submit_plan,
        effective_rules=rules,
        client_order_id="live_reconcile_stage",
    )

    try:
        submission = submit_live_order_and_confirm(
            broker=broker,
            request=request,
            intent_key=request.intent_key,
            strategy_name=request.strategy_name,
            decision_id=request.decision_id,
            decision_reason=request.decision_reason,
            exit_rule_name=request.exit_rule_name,
        )
        assert submission is not None

        trade = reconcile_apply_fills_and_refresh(
            live_module,
            broker=broker,
            submission=submission,
        )

        assert trade is not None
        row = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (request.client_order_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "FILLED"
    finally:
        conn.close()


def test_each_submit_attempt_records_exactly_one_classification(tmp_path):
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    db_success = str(tmp_path / "classify_success.sqlite")
    object.__setattr__(settings, "DB_PATH", db_success)
    assert live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0) is not None

    db_failed = str(tmp_path / "classify_failed.sqlite")
    object.__setattr__(settings, "DB_PATH", db_failed)
    assert live_execute_signal(_FailingSubmitBroker(), "BUY", 1001, 100000000.0) is None

    db_unknown = str(tmp_path / "classify_unknown.sqlite")
    object.__setattr__(settings, "DB_PATH", db_unknown)
    assert live_execute_signal(_TimeoutBroker(), "BUY", 1002, 100000000.0) is None

    for db_path in (db_success, db_failed, db_unknown):
        conn = ensure_db(db_path)
        row = conn.execute(
            "SELECT client_order_id, status FROM orders WHERE client_order_id LIKE 'live_%_buy_%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        attempts = conn.execute(
            """
            SELECT order_status
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            """,
            (row["client_order_id"],),
        ).fetchall()
        conn.close()

        assert len(attempts) == 1
        assert attempts[0]["order_status"] in {"NEW", "FAILED", "SUBMIT_UNKNOWN"}
        assert row["status"] in {"NEW", "FILLED", "FAILED", "SUBMIT_UNKNOWN"}


def test_live_submit_without_exchange_id_marks_submit_unknown(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "missing_exchange_id.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_NoExchangeIdBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "missing_exchange_id.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, exchange_order_id, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, order_status, broker_response_summary, submission_reason_code, timeout_flag, submit_evidence, exchange_order_id_obtained
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert row["exchange_order_id"] is None
    assert "classification=SUBMIT_UNKNOWN" in str(row["last_error"])
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["order_status"] == "SUBMIT_UNKNOWN"
    assert "exchange_order_id=-" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["submission_reason_code"] == "ambiguous_response"
    assert submit_attempt["timeout_flag"] == 0
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["error_summary"] == "missing exchange_order_id"
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert any("event=order_submit_unknown" in msg and "reason_code=SUBMIT_TIMEOUT" in msg for msg in notifications)


def test_live_execute_signal_marks_recovery_required_when_strict_fill_fee_blocks_aggregate(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "strict_fee_block.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_InvalidFeeAggregateBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "strict_fee_block.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    application_event = conn.execute(
        """
        SELECT submit_phase, order_status, submission_reason_code, submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_application'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "material fee validation blocked fill aggregation" in str(row["last_error"])
    assert application_event is not None
    assert application_event["submit_phase"] == "application"
    assert application_event["order_status"] == "RECOVERY_REQUIRED"
    assert application_event["submission_reason_code"] == "application_failed"
    application_evidence = json.loads(str(application_event["submit_evidence"]))
    assert application_evidence["submit_phase"] == "application"
    assert application_evidence["execution_state"] == "application_failed"
    assert any("event=recovery_required_transition" in msg for msg in notifications)


def test_submit_evidence_handles_unavailable_optional_fields(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_evidence_optional.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    _stub_submit_plan_quote(monkeypatch)

    def _raise_orderbook(_symbol: str) -> tuple[float, float]:
        raise RuntimeError("orderbook offline")

    monkeypatch.setattr("bithumb_bot.broker.live.fetch_orderbook_top", _raise_orderbook)

    trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "submit_evidence_optional.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert evidence["reference_price"] is None
    assert evidence["top_of_book"]["error"].startswith("market=KRW-BTC side=UNKNOWN RuntimeError:")


def test_reconcile_updates_portfolio(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "reconcile.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "reconcile.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_FakeBroker())

    conn = ensure_db(str(tmp_path / "reconcile.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    p = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "FILLED"
    assert float(p["asset_qty"]) == 0.01


def test_reconcile_records_stray_remote_open_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "stray.sqlite"))
    reconcile_with_broker(_StrayBroker())

    conn = ensure_db(str(tmp_path / "stray.sqlite"))
    row = conn.execute("SELECT status, exchange_order_id, side FROM orders WHERE client_order_id='remote_stray1'").fetchone()
    conn.close()

    assert row is None

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECONCILE_OK"


def test_cancel_open_orders_cancels_remote_and_updates_local(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_open_orders.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_open_orders.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _CancelOpenOrdersBroker()
    summary = cancel_open_orders_with_broker(broker)

    conn = ensure_db(str(tmp_path / "cancel_open_orders.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert summary["remote_open_count"] == 2
    assert summary["canceled_count"] == 1
    assert summary["matched_local_count"] == 1
    assert summary["stray_canceled_count"] == 0
    assert summary["failed_count"] == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_reports_cancel_failures(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_failure.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_failure.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelFailureBroker())

    assert summary["remote_open_count"] == 2
    assert summary["canceled_count"] == 1
    assert summary["failed_count"] == 0
    assert len(summary["error_messages"]) == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_tracks_cancel_acceptance_and_confirmation(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_acceptance.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_acceptance.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelAcceptedBroker())
    conn = ensure_db(str(tmp_path / "cancel_acceptance.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert summary["cancel_accepted_count"] == 1
    assert summary["canceled_count"] == 1
    assert summary["cancel_confirm_pending_count"] == 0
    assert summary["stray_canceled_count"] == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_does_not_bind_by_client_order_id_when_exchange_id_mismatch(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelIdentifierMismatchBroker())

    conn = ensure_db(str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "NEW"
    assert summary["failed_count"] == 1
    assert any("identifier mismatch" in str(message) for message in summary["error_messages"])


def test_cancel_open_orders_escalates_when_post_cancel_status_is_unresolved(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_unresolved_status.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_unresolved_status.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelAcceptedUnknownStatusBroker())

    conn = ensure_db(str(tmp_path / "cancel_unresolved_status.sqlite"))
    row = conn.execute("SELECT status, last_error FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "manual recovery required" in str(row["last_error"])
    assert summary["failed_count"] == 1
    assert any("final status unresolved" in str(message) for message in summary["error_messages"])


def test_reconcile_submit_unknown_without_exchange_id_marks_recovery_required_and_continues(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_required.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "recovery_required.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_2000_buy','ex2','NEW','BUY',NULL,0.01,0,2000,2000,NULL)
        """
    )
    conn.commit()
    conn.close()

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.recovery.notify", lambda msg: notifications.append(msg))

    reconcile_with_broker(_StrictRecoveryBroker())

    conn = ensure_db(str(tmp_path / "recovery_required.sqlite"))
    ambiguous = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id='ambiguous_missing_exid' AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    reconciled = conn.execute("SELECT status FROM orders WHERE client_order_id='live_2000_buy'").fetchone()
    conn.close()

    assert ambiguous is not None
    assert ambiguous["status"] == "RECOVERY_REQUIRED"
    assert ambiguous["exchange_order_id"] is None
    assert "manual recovery required" in str(ambiguous["last_error"])
    assert transition is not None
    assert transition["order_status"] == "RECOVERY_REQUIRED"
    assert "to=RECOVERY_REQUIRED" in str(transition["message"])
    assert "to=RECOVERY_REQUIRED" in str(transition["message"])
    assert reconciled is not None
    assert reconciled["status"] == "FILLED"
    recovery_alerts = [
        msg for msg in notifications
        if "event=recovery_required_transition" in msg and "reason_code=WEAK_ORDER_CORRELATION" in msg
    ]
    assert recovery_alerts
    assert any("symbol=" in msg for msg in recovery_alerts)
    assert any("exchange_order_id=-" in msg for msg in recovery_alerts)
    assert any("operator_next_action=review submit ambiguity and recover order with exchange_order_id" in msg for msg in recovery_alerts)
    assert any("operator_hint_command=uv run python bot.py recover-order --client-order-id ambiguous_missing_exid --exchange-order-id <exchange_order_id>" in msg for msg in recovery_alerts)
    assert any(
        "event=reconcile_status_change" in msg and "client_order_id=live_2000_buy" in msg
        for msg in notifications
    )


def test_reconcile_submit_unknown_without_exchange_id_ambiguous_remote_fill_escalates(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert float(row["qty_filled"]) == pytest.approx(0.0)
    assert fill is None


def test_reconcile_submit_unknown_recent_fill_path_keeps_manual_recovery_required(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert fill is None


def test_reconcile_submit_unknown_without_exchange_id_weak_order_correlation_escalates(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_order.sqlite"))
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None


def test_reconcile_submit_unknown_recent_order_path_escalates_to_recovery_required(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])


def test_reconcile_recovers_known_local_order_from_recent_activity(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recent_known.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "recent_known.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_3000_buy','ex_recent_known','NEW','BUY',NULL,0.01,0,3000,3000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_RecentActivityBroker())

    conn = ensure_db(str(tmp_path / "recent_known.sqlite"))
    row = conn.execute(
        "SELECT status, qty_filled FROM orders WHERE client_order_id='live_3000_buy'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='live_3000_buy'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == 0.01
    assert fill is not None

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECENT_FILL_APPLIED"


def test_reconcile_unmatched_recent_activity_creates_recovery_required_record(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recent_unmatched.sqlite"))

    reconcile_with_broker(_UnmatchedRecentFillBroker())

    conn = ensure_db(str(tmp_path / "recent_unmatched.sqlite"))
    row = conn.execute(
        """
        SELECT client_order_id, status, exchange_order_id, last_error
        FROM orders
        WHERE exchange_order_id='stray_recent_ex'
        """
    ).fetchone()
    conn.close()

    assert row is None


def test_manual_recover_order_attaches_exchange_order_id_and_applies_fills(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "manual_recover.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "manual_recover.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('manual_1',NULL,'RECOVERY_REQUIRED','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    recover_order_with_exchange_id(_FakeBroker(), client_order_id="manual_1", exchange_order_id="ex_manual_fill")

    conn = ensure_db(str(tmp_path / "manual_recover.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='manual_1'"
    ).fetchone()
    fill = conn.execute("SELECT fill_id FROM fills WHERE client_order_id='manual_1'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex_manual_fill"
    assert float(row["qty_filled"]) == 0.01
    assert fill is not None


def test_reconcile_conflicting_sources_halts_conservatively(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "source_conflict.sqlite"))
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.recovery.notify", lambda msg: notifications.append(msg))

    reconcile_with_broker(_ConflictingRecoverySourcesBroker())

    state = runtime_state.snapshot()
    assert state.halt_new_orders_blocked is False
    assert state.last_reconcile_reason_code == "RECONCILE_OK"
    assert not any("event=reconcile_source_conflict" in msg for msg in notifications)


def test_reconcile_precedence_prefers_open_orders_over_recent_orders(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "source_precedence.sqlite"))

    reconcile_with_broker(_OpenOrderPreferredBroker())

    conn = ensure_db(str(tmp_path / "source_precedence.sqlite"))
    row = conn.execute(
        "SELECT status FROM orders WHERE exchange_order_id='ex_precedence'"
    ).fetchone()
    conn.close()

    assert row is None

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECONCILE_OK"


def test_reconcile_records_balance_split_mismatch_metadata(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "balance_mismatch.sqlite"))

    conn = ensure_db(str(tmp_path / "balance_mismatch.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1000000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_BalanceMismatchBroker())

    state = runtime_state.snapshot()
    assert state.last_reconcile_metadata is not None
    payload = json.loads(state.last_reconcile_metadata)
    assert int(payload.get("balance_split_mismatch_count", 0)) >= 1
    assert "cash_available" in str(payload.get("balance_split_mismatch_summary", ""))
    assert payload.get("balance_source") in {"legacy_balance_api", "accounts_v1_rest_snapshot", "dry_run_static"}
    assert int(payload.get("balance_observed_ts_ms", 0)) >= 0


def test_validate_order_rejects_invalid_qty():
    try:
        validate_order(signal="BUY", side="BUY", qty=0.0, market_price=100.0)
    except ValueError as e:
        assert "invalid order qty" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_live_invalid_market_price_rejected_before_submit(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "invalid_market.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    broker = _FakeBroker()

    trade = live_execute_signal(broker, "BUY", 1000, 0.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_insufficient_available_balance_rejected(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "insufficient_balance.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "BUY_FRACTION", 0.99)
    object.__setattr__(settings, "FEE_RATE", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.001)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 10.0)

    broker = _FakeBroker()
    monkeypatch.setattr(
        broker,
        "get_balance",
        lambda: BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_validate_pretrade_buy_uses_live_fee_rate_estimate_not_fee_rate() -> None:
    object.__setattr__(settings, "FEE_RATE", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.10)
    broker = _FakeBroker()
    broker.get_balance = lambda: BrokerBalance(  # type: ignore[method-assign]
        cash_available=109.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )

    with pytest.raises(ValueError, match="insufficient available cash"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


def test_validate_pretrade_buy_becomes_more_conservative_when_live_fee_increases() -> None:
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "FEE_RATE", 0.0)
    broker = _FakeBroker()
    broker.get_balance = lambda: BrokerBalance(  # type: ignore[method-assign]
        cash_available=105.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )

    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.01)
    validate_pretrade(
        broker=broker,
        side="BUY",
        qty=1.0,
        market_price=100.0,
        reference_bid=99.9,
        reference_ask=100.1,
    )

    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.06)
    with pytest.raises(ValueError, match="insufficient available cash"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


def test_validate_pretrade_rejects_dry_run_balance_source_in_live_real_order() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    broker = _FakeBroker()
    broker.get_balance_snapshot = lambda: BalanceSnapshot(  # type: ignore[attr-defined]
        source_id="dry_run_static",
        observed_ts_ms=0,
        asset_ts_ms=0,
        balance=BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

    with pytest.raises(ValueError, match="invalid live balance source: dry_run_static"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


def test_validate_pretrade_accepts_accounts_snapshot_balance_source(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "min_notional_krw": 0.0,
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "max_qty_decimals": 8,
                    },
                )(),
            },
        )(),
    )
    broker = _FakeBroker()
    broker.get_balance_snapshot = lambda: BalanceSnapshot(  # type: ignore[attr-defined]
        source_id="accounts_v1_rest_snapshot",
        observed_ts_ms=int(time.time() * 1000),
        asset_ts_ms=int(time.time() * 1000),
        balance=BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

    validate_pretrade(
        broker=broker,
        side="BUY",
        qty=1.0,
        market_price=100.0,
        reference_bid=99.9,
        reference_ask=100.1,
    )


def test_live_excessive_spread_rejected_before_submit(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "spread_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 5.0)

    from bithumb_bot.broker import live as live_module

    monkeypatch.setattr(
        live_module,
        "fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0),
    )

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100.5)

    assert trade is None
    assert broker.place_order_calls == 0


def test_normalize_order_qty_floors_to_step_and_decimals():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.01)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 3)

    normalized = normalize_order_qty(qty=1.2399, market_price=100.0)

    assert normalized == pytest.approx(1.23)


def test_normalize_order_qty_rejects_when_collapses_to_zero():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.01)

    with pytest.raises(ValueError, match="normalized order qty is non-positive"):
        normalize_order_qty(qty=0.009, market_price=100.0)


def test_normalize_order_qty_does_not_apply_notional_guard():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.1)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 100.0)

    normalized = normalize_order_qty(qty=1.09, market_price=99.0)
    assert normalized == pytest.approx(1.0)


def test_normalize_order_qty_avoids_float_boundary_drift():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    normalized = normalize_order_qty(qty=0.0003, market_price=100_000_000.0)

    assert normalized == pytest.approx(0.0003)


@pytest.mark.fast_regression
def test_adjust_buy_order_qty_for_dust_safety_floors_to_sellable_qty():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="would not leave an executable exit lot"):
        adjust_buy_order_qty_for_dust_safety(qty=0.00019193, market_price=100_000_000.0)


@pytest.mark.fast_regression
def test_adjust_buy_order_qty_for_dust_safety_rejects_when_no_sellable_qty_remains():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="dust-safe entry qty unavailable"):
        adjust_buy_order_qty_for_dust_safety(qty=0.00009193, market_price=100_000_000.0)


@pytest.mark.fast_regression
@pytest.mark.parametrize(
    ("qty", "expected_qty"),
    [
        (0.0001, 0.0001),
        (0.00019999, 0.0001),
    ],
    ids=["already_sellable_qty", "floors_to_sellable_qty_without_leaving_entry_dust"],
)
def test_adjust_buy_order_qty_for_dust_safety_preserves_only_sellable_entry_qty(qty, expected_qty):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="would not leave an executable exit lot"):
        adjust_buy_order_qty_for_dust_safety(qty=qty, market_price=100_000_000.0)


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_dust_unsafe_entry_qty_before_submit(tmp_path):
    original = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": float(settings.START_CASH_KRW),
        "BUY_FRACTION": float(settings.BUY_FRACTION),
        "MAX_ORDER_KRW": float(settings.MAX_ORDER_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "MAX_ORDERBOOK_SPREAD_BPS": float(settings.MAX_ORDERBOOK_SPREAD_BPS),
        "MAX_MARKET_SLIPPAGE_BPS": float(settings.MAX_MARKET_SLIPPAGE_BPS),
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS),
    }
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_dust_unsafe.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 19_193.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "buy_dust_unsafe.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=19_193.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    try:
        broker = _FakeBroker()
        trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

        assert trade is None
        assert broker.place_order_calls == 0
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_blocks_when_step_floor_would_leave_unsellable_dust():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.00019192, market_price=100_000_000.0)

    details = exc.value.details
    assert details["normalized_qty"] == pytest.approx(0.0001)
    assert details["remainder_qty"] == pytest.approx(0.00009192)
    assert details["dust_scope"] == "remainder_after_sell"
    assert "guard_action=block_sell_remainder_dust" in str(details["summary"])


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_blocks_when_broker_precision_still_leaves_dust():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.000191939, market_price=100_000_000.0)

    details = exc.value.details
    assert details["normalized_qty"] == pytest.approx(0.0001)
    assert details["remainder_qty"] == pytest.approx(0.000091939)
    assert details["broker_full_qty"] == pytest.approx(0.00019193)
    assert details["dust_scope"] == "remainder_after_sell"


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_exposes_remainder_dust_guard_details():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.000191939, market_price=100_000_000.0)

    details = exc.value.details
    assert details["state"] == EXIT_PARTIAL_LEFT_DUST
    assert details["operator_action"] == MANUAL_DUST_REVIEW_REQUIRED
    assert details["dust_scope"] == "remainder_after_sell"
    assert details["qty_below_min"] == 1
    assert details["notional_below_min"] == 0
    assert details["remainder_qty"] == pytest.approx(0.000091939)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_treats_sub_min_residual_as_dust_before_submit(tmp_path):
    original = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": float(settings.START_CASH_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "MAX_ORDERBOOK_SPREAD_BPS": float(settings.MAX_ORDERBOOK_SPREAD_BPS),
        "MAX_MARKET_SLIPPAGE_BPS": float(settings.MAX_MARKET_SLIPPAGE_BPS),
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS),
    }
    db_path = str(tmp_path / "sell_sub_min_residual.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00000001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(db_path)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009997,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00009997, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009997,
            "raw_total_asset_qty": 0.00009997,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.00009997,
            "open_exposure_qty": 0.00009997,
            "dust_tracking_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009997,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009997,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "terminal_state": "dust_only",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    try:
        broker = _FakeBroker()
        trade = live_execute_signal(
            broker,
            "SELL",
            1000,
            100_000_000.0,
            strategy_name="dust_exit_test",
            decision_id=decision_id,
            decision_reason="partial_take_profit",
            exit_rule_name="exit_signal",
        )

        assert trade is None
        assert broker.place_order_calls == 0

        conn = ensure_db(db_path)
        order_row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM orders
            WHERE side='SELL'
            """
        ).fetchone()
        suppression_row = conn.execute(
            """
            SELECT reason_code, summary, context_json
            FROM order_suppressions
            WHERE reason_code=?
            ORDER BY updated_ts DESC
            LIMIT 1
            """,
            (DUST_RESIDUAL_SUPPRESSED,),
        ).fetchone()
        conn.close()

        assert order_row is not None
        assert order_row["n"] == 0
        assert suppression_row is not None
        assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
        assert "decision_suppressed:exit_suppressed_by_quantity_rule" in str(suppression_row["summary"])
        assert "exit_non_executable_reason=" in str(suppression_row["summary"])
        assert any(
            token in str(suppression_row["summary"])
            for token in ("exit_non_executable_reason=dust_only_remainder", "exit_non_executable_reason=no_executable_exit_lot")
        )
        suppression_context = json.loads(str(suppression_row["context_json"]))
        assert suppression_context["reason_code"] == DUST_RESIDUAL_SUPPRESSED
        assert suppression_context["exit_non_executable_reason"] in {"dust_only_remainder", "no_executable_exit_lot"}
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_exit_intent_true_but_quantity_blocked_suppresses_without_order_submit(tmp_path):
    original = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "MAX_ORDERBOOK_SPREAD_BPS": settings.MAX_ORDERBOOK_SPREAD_BPS,
        "MAX_MARKET_SLIPPAGE_BPS": settings.MAX_MARKET_SLIPPAGE_BPS,
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS,
    }
    db_path = str(tmp_path / "sell_exit_intent_quantity_blocked.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(db_path)
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="exit_intent_blocked_test",
        signal="SELL",
        reason="exit_signal_present",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009997,
            "raw_total_asset_qty": 0.00009997,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.00009997,
            "open_exposure_qty": 0.00009997,
            "dust_tracking_qty": 0.0,
            "exit": {
                "intent": {
                    "intent": "exit_open_exposure",
                    "requires_execution_sizing": True,
                }
            },
            "position_state": {
                "normalized_exposure": {
                    "raw_total_asset_qty": 0.00009997,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009997,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "terminal_state": "dust_only",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    recorded: dict[str, object] = {}
    original_recorder = live_module._record_sell_no_executable_exit_suppression
    try:
        live_module._record_sell_no_executable_exit_suppression = lambda **kwargs: recorded.update(kwargs) or True
        trade = live_execute_signal(
            broker,
            "SELL",
            1000,
            100_000_000.0,
            strategy_name="exit_intent_blocked_test",
            decision_id=decision_id,
            decision_reason="exit_signal_present",
            exit_rule_name="exit_signal",
        )

        assert trade is None
        assert broker.place_order_calls == 0

        conn = ensure_db(db_path)
        sell_order_count = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE side='SELL'").fetchone()["n"]
        conn.close()

        assert sell_order_count == 0
        assert recorded["strategy_name"] == "exit_intent_blocked_test"
        assert recorded["decision_id"] == decision_id
        assert recorded["decision_reason"] == "exit_signal_present"
        assert recorded["exit_rule_name"] == "exit_signal"
        assert float(recorded["market_price"]) == pytest.approx(100_000_000.0)
        exit_sizing = recorded["exit_sizing"]
        assert exit_sizing is not None
        assert exit_sizing.allowed is False
        assert exit_sizing.executable_lot_count == 0
        assert exit_sizing.executable_qty == pytest.approx(0.0)
        canonical_sell = recorded["canonical_sell"]
        assert canonical_sell.sellable_executable_lot_count == 0
        assert canonical_sell.exit_allowed is False
        assert canonical_sell.submit_qty_source == "position_state.normalized_exposure.sellable_executable_lot_count"
    finally:
        live_module._record_sell_no_executable_exit_suppression = original_recorder
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_fee_created_residue_keeps_raw_holdings_non_executable_and_skips_submit(
    monkeypatch,
    tmp_path,
):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_fee_residue.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    fee_residue_qty = 0.00005
    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "fee_deduction_after_sell",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": fee_residue_qty,
        "dust_local_qty": fee_residue_qty,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 0.0,
        "dust_latest_price": 100_000_000.0,
        "dust_broker_notional_krw": 5_000.0,
        "dust_local_notional_krw": 5_000.0,
        "dust_broker_qty_is_dust": 1,
        "dust_local_qty_is_dust": 1,
        "dust_broker_notional_is_dust": 0,
        "dust_local_notional_is_dust": 0,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_fee_residue.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=fee_residue_qty,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    normalized = build_position_state_model(
        raw_qty_open=fee_residue_qty,
        metadata_raw=metadata,
        raw_total_asset_qty=fee_residue_qty,
        open_exposure_qty=0.0,
        dust_tracking_qty=fee_residue_qty,
        open_lot_count=0,
        dust_tracking_lot_count=1,
        market_price=100_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="fee_residue_cleanup",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(str(tmp_path / "sell_fee_residue.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert normalized.raw_total_asset_qty == pytest.approx(fee_residue_qty)
    assert normalized.open_exposure_qty == pytest.approx(0.0)
    assert normalized.dust_tracking_qty == pytest.approx(fee_residue_qty)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.exit_allowed is False
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert normalized.terminal_state == "dust_only"
    assert trade is None
    assert broker.place_order_calls == 0
    assert order_count == 0
    assert event_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    context = json.loads(str(suppression_row["context_json"]))
    assert context["raw_total_asset_qty"] == pytest.approx(fee_residue_qty)
    assert context["sell_submit_lot_count"] == 0
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_min_notional_only_residue_keeps_raw_holdings_non_executable_and_skips_submit(
    monkeypatch,
    tmp_path,
):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_min_notional_only.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    min_notional_residue_qty = 0.0001
    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "notional_below_min_after_sell",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": min_notional_residue_qty,
        "dust_local_qty": min_notional_residue_qty,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 5_000.0,
        "dust_latest_price": 40_000_000.0,
        "dust_broker_notional_krw": 4_000.0,
        "dust_local_notional_krw": 4_000.0,
        "dust_broker_qty_is_dust": 0,
        "dust_local_qty_is_dust": 0,
        "dust_broker_notional_is_dust": 1,
        "dust_local_notional_is_dust": 1,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_min_notional_only.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=min_notional_residue_qty,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    normalized = build_position_state_model(
        raw_qty_open=min_notional_residue_qty,
        metadata_raw=metadata,
        raw_total_asset_qty=min_notional_residue_qty,
        open_exposure_qty=0.0,
        dust_tracking_qty=min_notional_residue_qty,
        open_lot_count=0,
        dust_tracking_lot_count=1,
        market_price=40_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        40_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="min_notional_residue_cleanup",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(str(tmp_path / "sell_min_notional_only.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert normalized.raw_total_asset_qty == pytest.approx(min_notional_residue_qty)
    assert normalized.open_exposure_qty == pytest.approx(0.0)
    assert normalized.dust_tracking_qty == pytest.approx(min_notional_residue_qty)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.exit_allowed is False
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert normalized.terminal_state == "dust_only"
    assert trade is None
    assert broker.place_order_calls == 0
    assert order_count == 0
    assert event_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    context = json.loads(str(suppression_row["context_json"]))
    assert context["raw_total_asset_qty"] == pytest.approx(min_notional_residue_qty)
    assert context["sell_submit_lot_count"] == 0
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_authority_boundary_live_execute_signal_sell_dust_only_raw_holdings_do_not_restore_submit_authority(
    monkeypatch,
    tmp_path,
):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_only_live_submit_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    residue_qty = 0.0001
    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "notional_below_min_after_sell",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": residue_qty,
            "dust_local_qty": residue_qty,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 40_000_000.0,
            "dust_broker_notional_krw": 4_000.0,
            "dust_local_notional_krw": 4_000.0,
            "dust_broker_qty_is_dust": 0,
            "dust_local_qty_is_dust": 0,
            "dust_broker_notional_is_dust": 1,
            "dust_local_notional_is_dust": 1,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "sell_dust_only_live_submit_authority.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=residue_qty,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        40_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="dust_only raw holdings must stay non-executable",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(str(tmp_path / "sell_dust_only_live_submit_authority.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    suppression_row = conn.execute(
        """
        SELECT reason_code, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert trade is None
    assert broker.place_order_calls == 0
    assert order_count is not None
    assert int(order_count["n"]) == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    suppression_context = json.loads(str(suppression_row["context_json"]))
    assert suppression_context["raw_total_asset_qty"] == pytest.approx(residue_qty)
    assert suppression_context["sell_dust_tracking_qty"] == pytest.approx(residue_qty)
    assert suppression_context["sell_submit_lot_count"] == 0
    assert suppression_context["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert suppression_context["submit_payload_qty"] == pytest.approx(0.0)
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_snaps_tiny_min_qty_boundary_upward():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    adjusted = adjust_sell_order_qty_for_dust_safety(qty=0.00009999, market_price=100_000_000.0)

    assert adjusted == pytest.approx(0.0001)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_keeps_exact_min_qty_executable():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    adjusted = adjust_sell_order_qty_for_dust_safety(qty=0.0001, market_price=100_000_000.0)

    assert adjusted == pytest.approx(0.0001)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_executable_sell_submit_rejects_observational_qty_preview_source() -> None:
    with pytest.raises(ValueError, match="requires canonical lot-native SELL authority"):
        live_module._require_canonical_sell_submit_lot_source(
            submit_qty_source="observation.sell_qty_preview",
            context="live SELL submit",
        )


@pytest.mark.fast_regression
def test_canonical_sell_submit_source_returns_typed_lot_native_authority_value() -> None:
    source = live_module._require_canonical_sell_submit_lot_source(
        submit_qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
        context="live SELL submit",
    )

    assert source.value == "position_state.normalized_exposure.sellable_executable_lot_count"


# SELL authority-boundary enforcement for observational qty paths.

@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_sell_dust_unsellable_rejects_observational_qty_authority(monkeypatch, tmp_path) -> None:
    db_path = str(tmp_path / "sell_dust_unsellable_boundary.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    conn = ensure_db(db_path)
    try:
        with pytest.raises(ValueError, match="requires canonical lot-native SELL authority"):
            live_module._record_sell_dust_unsellable(
                conn=conn,
                state=runtime_state.snapshot(),
                ts=100,
                market_price=100_000_000.0,
                canonical_sell=live_module._CanonicalSellExecutionView(
                    sellable_executable_lot_count=0,
                    sellable_executable_qty=0.0004,
                    exit_allowed=False,
                    exit_block_reason="no_executable_exit_lot",
                    submit_qty_source="observation.sell_qty_preview",
                    position_state_source="observation.sell_qty_preview",
                ),
                diagnostic_qty=live_module._SellDiagnosticQtyView(
                    observed_position_qty=0.0004,
                    observed_position_qty_source="observation.sell_qty_preview",
                    raw_total_asset_qty=0.0004,
                    open_exposure_qty=0.0,
                    dust_tracking_qty=0.0004,
                ),
                strategy_name="authority_boundary_test",
                decision_id=None,
                decision_reason="non canonical source",
                exit_rule_name="exit_signal",
                decision_observability={
                    "base_signal": "SELL",
                    "final_signal": "SELL",
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0004,
                    "raw_total_asset_qty": 0.0004,
                    "exit_block_reason": "no_executable_exit_lot",
                },
            )
    finally:
        conn.close()


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_harmless_dust_suppression_rejects_observational_qty_authority(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "harmless_dust_authority_boundary.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    conn = ensure_db(db_path)
    try:
        state = type(
            "_State",
            (),
            {
                "last_reconcile_metadata": json.dumps(
                    {
                        "dust_classification": "harmless_dust",
                        "dust_residual_present": 1,
                        "dust_residual_allow_resume": 1,
                        "dust_effective_flat": 1,
                    }
                )
            },
        )()
        with pytest.raises(ValueError, match="requires canonical lot-native SELL authority"):
            live_module._record_harmless_dust_exit_suppression(
                conn=conn,
                state=state,
                signal="SELL",
                side="SELL",
                requested_qty=0.0004,
                market_price=100_000_000.0,
                normalized_qty=0.0004,
                submit_qty_source="observation.sell_qty_preview",
                position_state_source="observation.sell_qty_preview",
                strategy_name="authority_boundary_test",
                decision_id=None,
                decision_reason="non canonical source",
                exit_rule_name="exit_signal",
            )
    finally:
        conn.close()


def test_live_execute_signal_buy_does_not_floor_market_buy_spend_via_qty_step(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_qty_step.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="test",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "market_buy_qty_step.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.0,
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": 0.0,
                "has_executable_exposure": False,
                "has_any_position_residue": False,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0, decision_id=decision_id, decision_reason="sma golden cross")

    assert trade is not None
    assert broker.place_order_calls == 1
    # 20,000 / 100,000,000 = 0.0002. If qty-step flooring leaked into market BUY,
    # this would collapse to 0.0001 and halve the KRW notional (regression).
    assert broker._last_qty == pytest.approx(0.0002)


# BUY execution handoff and market submit sizing.

@pytest.mark.fast_regression
def test_live_execute_signal_buy_passes_typed_buy_authority_to_sizing(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    captured: dict[str, object] = {}

    def _capture_buy_execution_sizing(**kwargs):
        captured["authority"] = kwargs.get("authority")
        return SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="entry.intent_lot_count",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
            buy_authority=kwargs.get("authority"),
        )

    monkeypatch.setattr(live_module, "build_buy_execution_sizing", _capture_buy_execution_sizing)

    conn = ensure_db(str(tmp_path / "market_buy_authority.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "entry_allowed_truth_source": "position_state.normalized_exposure.entry_allowed",
                "effective_flat": True,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.0,
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": 0.0,
                "has_executable_exposure": False,
                "has_any_position_residue": False,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "BUY",
        1000,
        100_000_000.0,
        decision_id=decision_id,
        decision_reason="sma golden cross",
    )

    assert trade is not None
    assert isinstance(captured["authority"], BuyExecutionAuthority)
    assert captured["authority"] == BuyExecutionAuthority(
        entry_allowed=True,
        entry_allowed_truth_source="context.entry_allowed",
    )


@pytest.mark.fast_regression
def test_live_execute_signal_buy_logs_structured_entry_sizing_diagnostics(tmp_path, monkeypatch, caplog):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_log.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    def _blocked_buy_execution_sizing(**kwargs):
        return SimpleNamespace(
            allowed=False,
            block_reason=BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS,
            decision_reason_code=BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS,
            budget_krw=4000.0,
            requested_qty=0.0002,
            executable_qty=0.0,
            internal_lot_size=0.0004,
            intended_lot_count=0,
            executable_lot_count=0,
            qty_source="entry.intent_budget_exchange_constraints",
            effective_min_trade_qty=0.0003,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            non_executable_reason=BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS,
            buy_authority=kwargs.get("authority"),
            internal_lot_is_exchange_inflated=True,
            internal_lot_would_block_buy=True,
        )

    monkeypatch.setattr(live_module, "build_buy_execution_sizing", _blocked_buy_execution_sizing)

    conn = ensure_db(str(tmp_path / "market_buy_log.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "entry_allowed_truth_source": "context.entry_allowed",
                "effective_flat": True,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.0,
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": 0.0,
                "has_executable_exposure": False,
                "has_any_position_residue": False,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        trade = live_execute_signal(
            broker,
            "BUY",
            1000,
            100_000_000.0,
            decision_id=decision_id,
            decision_reason="sma golden cross",
        )

    assert trade is None
    assert broker.place_order_calls == 0
    assert "[ORDER_SKIP] entry sizing blocked" in caplog.text
    assert f"reason={BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS}" in caplog.text
    assert f"decision_reason_code={BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS}" in caplog.text
    assert "budget_krw=4000.0" in caplog.text
    assert "requested_qty=" in caplog.text
    assert "internal_lot_size=" in caplog.text
    assert "effective_min_trade_qty=" in caplog.text
    assert "min_qty=" in caplog.text
    assert "qty_step=" in caplog.text
    assert "min_notional_krw=5000.0" in caplog.text
    assert "internal_lot_is_exchange_inflated=1" in caplog.text
    assert "internal_lot_would_block_buy=1" in caplog.text


@pytest.mark.fast_regression
def test_live_execute_signal_buy_adjusts_dust_creating_entry_qty(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_dust_safe.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 19_193.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "market_buy_dust_safe.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_when_dust_safe_adjustment_collapses_to_zero(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_dust_blocked.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 9_193.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0


    conn = ensure_db(str(tmp_path / "market_buy_dust_blocked.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_dust_unsafe_entry_is_blocked_in_sizing_before_live_guard(
    tmp_path, monkeypatch, caplog
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_sizing_dust_block.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 19_193.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 10.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    def _unexpected_live_guard(**kwargs):
        raise AssertionError("live BUY dust guard should not run for normal dust-unsafe sizing block")

    monkeypatch.setattr(live_module, "adjust_buy_order_qty_for_dust_safety", _unexpected_live_guard)

    broker = _FakeBroker()
    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0
    assert "[ORDER_SKIP] entry sizing blocked" in caplog.text
    assert "reason=no_executable_exit_lot" in caplog.text


@pytest.mark.fast_regression
def test_live_execute_signal_buy_fallback_dust_guard_logs_unexpected_invariant_mismatch(
    tmp_path, monkeypatch, caplog
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_dust_fallback.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    def _allowed_buy_execution_sizing(**kwargs):
        return SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="entry.intent_budget_exchange_constraints",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5_000.0,
            non_executable_reason="executable",
            buy_authority=kwargs.get("authority"),
            internal_lot_is_exchange_inflated=False,
            internal_lot_would_block_buy=False,
        )

    def _fallback_guard(*, qty: float, market_price: float) -> float:
        raise ValueError(
            "dust-safe entry qty would not leave an executable exit lot: "
            "normalized_qty=0.000200000000 effective_min_trade_qty=0.000300000000 "
            "reason=no_executable_exit_lot"
        )

    monkeypatch.setattr(live_module, "build_buy_execution_sizing", _allowed_buy_execution_sizing)
    monkeypatch.setattr(live_module, "adjust_buy_order_qty_for_dust_safety", _fallback_guard)

    broker = _FakeBroker()
    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0
    assert "[ORDER_SKIP] buy dust guard fallback blocked" in caplog.text
    assert "fallback_invariant_mismatch=1" in caplog.text


@pytest.mark.fast_regression
def test_live_execute_signal_buy_records_intent_when_harmless_dust_is_effective_flat(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_harmless_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="test",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "market_buy_harmless_dust.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.00009193,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1_699_999_940_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": True,
                "has_executable_exposure": False,
                "has_any_position_residue": True,
                "has_non_executable_residue": True,
                "has_dust_only_remainder": True,
                "normalized_exposure_qty": 0.00009629,
                "raw_qty_open": 0.00009629,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009629,
                "position_gate": {
                    "entry_allowed": True,
                    "effective_flat_due_to_harmless_dust": True,
                    "normalized_exposure_active": True,
                    "has_executable_exposure": False,
                    "has_any_position_residue": True,
                    "has_non_executable_residue": True,
                    "has_dust_only_remainder": True,
                    "raw_qty_open": 0.00009629,
                },
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": True,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "normalized_exposure_qty": 0.00009629,
                        "raw_qty_open": 0.00009629,
                        "open_exposure_qty": 0.00009629,
                        "dust_tracking_qty": 0.00009629,
                    }
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "BUY",
        1_700_000_000_000,
        100_000_000.0,
        decision_id=decision_id,
        decision_reason="sma golden cross",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_side == "BUY"
    assert broker._last_qty > 0

    conn = ensure_db(str(tmp_path / "market_buy_harmless_dust.sqlite"))
    order_row = conn.execute(
        """
        SELECT client_order_id, side, status, qty_req
        FROM orders
        WHERE client_order_id LIKE 'live_1700000000000_buy_%'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert order_row is not None
    intent_row = conn.execute(
        """
        SELECT event_type, side, qty, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_row["client_order_id"],),
    ).fetchone()
    submit_attempt_row = conn.execute(
        """
        SELECT event_type, side, qty, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert order_row["side"] == "BUY"
    assert order_row["status"] in {"NEW", "FILLED"}
    assert float(order_row["qty_req"]) > 0
    assert intent_row is not None
    assert intent_row["side"] == "BUY"
    assert float(intent_row["qty"]) > 0
    assert submit_attempt_row is not None
    assert submit_attempt_row["side"] == "BUY"
    assert float(submit_attempt_row["qty"]) > 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_defensively_for_blocking_dust_mismatch(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_blocking_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "blocking_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 0,
            "dust_broker_qty": 0.000099,
            "dust_local_qty": 0.000010,
            "dust_delta_qty": 0.000089,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 40_000_000.0,
            "dust_broker_notional_krw": 3_960.0,
            "dust_local_notional_krw": 400.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 1,
            "dust_local_notional_is_dust": 1,
            "dust_broker_local_match": 0,
            "dust_residual_summary": (
                "classification=blocking_dust harmless_dust=0 broker_local_match=0 "
                "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "market_buy_blocking_dust.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.000099,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "market_buy_blocking_dust.sqlite"))
    order_count = conn.execute(
        "SELECT COUNT(*) AS n FROM orders WHERE side='BUY'"
    ).fetchone()["n"]
    intent_count = conn.execute(
        "SELECT COUNT(*) AS n FROM order_events WHERE event_type='intent_created'"
    ).fetchone()["n"]
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert intent_count == 0
    assert submit_attempt_count == 0


def test_live_execute_signal_sell_blocks_when_qty_step_floor_would_leave_unsellable_dust(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00019192,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    dust_rows = conn.execute(
        """
        SELECT reason_code
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    latest_order = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    event_row = conn.execute(
        """
        SELECT reason_code, requested_qty, normalized_qty, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert len(dust_rows) == 0
    assert latest_order is None
    assert event_row is None


# Authority boundary regression suite.


@pytest.mark.fast_regression
def test_authority_boundary_live_execute_signal_sell_records_end_to_end_authority_flow_through_fill_and_reinterpretation(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00029193,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0002,
                "raw_total_asset_qty": 0.00029193,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0002,
                "open_exposure_qty": 0.0002,
                "dust_tracking_qty": 0.00009193,
                "open_lot_count": 2,
                "dust_tracking_lot_count": 1,
                "sellable_executable_lot_count": 2,
                "sellable_executable_qty": 0.0002,
                "has_executable_exposure": True,
                "has_any_position_residue": True,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
                "submit_lot_count": 2,
                "submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "sell_qty_basis_qty": 0.0002,
                "sell_qty_basis_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "exit_allowed": True,
                "exit_block_reason": "none",
                "terminal_state": "open_exposure",
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0002,
                        "raw_total_asset_qty": 0.00029193,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0002,
                        "open_exposure_qty": 0.0002,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 2,
                        "dust_tracking_lot_count": 1,
                        "sellable_executable_lot_count": 2,
                        "sellable_executable_qty": 0.0002,
                        "has_executable_exposure": True,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": False,
                        "has_dust_only_remainder": False,
                        "submit_lot_count": 2,
                        "position_state_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                        "sell_qty_basis_qty": 0.0002,
                        "sell_qty_basis_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                        "exit_allowed": True,
                        "exit_block_reason": "none",
                        "terminal_state": "open_exposure",
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0002)
    assert broker._last_price is None

    conn = ensure_db(str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    conn.close()

    assert order_row is not None
    assert float(order_row["qty_req"]) == pytest.approx(0.0002)
    assert float(order_row["qty_filled"]) == pytest.approx(0.0002)
    assert order_row["status"] == "FILLED"
    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0002)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0002)
    assert submit_evidence["normalized_qty"] == pytest.approx(0.0002)
    assert submit_evidence["submit_lot_count"] == 2
    assert submit_evidence["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert submit_evidence["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert submit_evidence["position_state_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["position_state_source_truth_source"] == "derived:sellable_executable_lot_count"
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00029193)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.0002)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.00009193)

    assert lot_snapshot.lot_definition is not None
    assert lot_snapshot.lot_definition.internal_lot_size is not None
    post_fill_open_lot_count = int(
        round(float(lot_snapshot.executable_open_exposure_qty) / float(lot_snapshot.lot_definition.internal_lot_size))
    )

    normalized = build_position_state_model(
        raw_qty_open=float(lot_snapshot.executable_open_exposure_qty),
        metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
        raw_total_asset_qty=float(lot_snapshot.raw_total_asset_qty),
        open_exposure_qty=float(lot_snapshot.executable_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        open_lot_count=post_fill_open_lot_count,
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        market_price=100_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0002)
    assert lot_snapshot.executable_open_exposure_qty == pytest.approx(0.0)
    assert post_fill_open_lot_count == 0
    assert lot_snapshot.dust_tracking_lot_count == 1
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.00009193)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.exit_allowed is False
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert normalized.terminal_state == "dust_only"


@pytest.mark.fast_regression
def test_live_execute_signal_runtime_path_preserves_authority_sequence_through_fill_reinterpretation(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "runtime_authority_sequence.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="entry_sizing.budget",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "runtime_authority_sequence.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="runtime_authority_sequence",
        signal="BUY",
        reason="sma_golden_cross",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "BUY",
            "final_signal": "BUY",
            "entry_allowed": True,
            "effective_flat": True,
            "raw_qty_open": 0.0,
            "raw_total_asset_qty": 0.0,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.0,
            "has_executable_exposure": False,
            "has_any_position_residue": False,
            "has_non_executable_residue": False,
            "has_dust_only_remainder": False,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.0,
                    "effective_flat": True,
                    "entry_allowed": True,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                    "has_executable_exposure": False,
                    "has_any_position_residue": False,
                    "has_non_executable_residue": False,
                    "has_dust_only_remainder": False,
                    "terminal_state": "flat",
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 1,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "BUY",
        1000,
        100_000_000.0,
        strategy_name="runtime_authority_sequence",
        decision_id=decision_id,
        decision_reason="sma_golden_cross",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0002)

    conn = ensure_db(str(tmp_path / "runtime_authority_sequence.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    conn.close()

    assert submit_attempt is not None
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["side"] == "BUY"
    assert submit_evidence["order_qty"] == pytest.approx(0.0002)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0002)
    assert submit_evidence["intended_qty"] == pytest.approx(0.0002)
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.0)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.0)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.0)

    normalized = build_position_state_model(
        raw_qty_open=float(lot_snapshot.raw_open_exposure_qty),
        metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
        raw_total_asset_qty=float(lot_snapshot.raw_total_asset_qty),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        market_price=100_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    assert lot_snapshot.open_lot_count == 0
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_lot_count == 1
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.0002)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.has_dust_only_remainder is True
    assert normalized.exit_allowed is False
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert normalized.terminal_state == "dust_only"


@pytest.mark.fast_regression
def test_authority_trace_buy_flow_records_authority_sequence_through_fill_and_reinterpretation(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_authority_trace.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="entry_sizing.budget",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "buy_authority_trace.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="buy_authority_trace",
        signal="BUY",
        reason="sma_golden_cross",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "BUY",
            "final_signal": "BUY",
            "entry_allowed": True,
            "effective_flat": True,
            "raw_qty_open": 0.0,
            "raw_total_asset_qty": 0.0,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.0,
            "has_executable_exposure": False,
            "has_any_position_residue": False,
            "has_non_executable_residue": False,
            "has_dust_only_remainder": False,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.0,
                    "effective_flat": True,
                    "entry_allowed": True,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                    "has_executable_exposure": False,
                    "has_any_position_residue": False,
                    "has_non_executable_residue": False,
                    "has_dust_only_remainder": False,
                    "terminal_state": "flat",
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 1,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "BUY",
        1000,
        100_000_000.0,
        strategy_name="buy_authority_trace",
        decision_id=decision_id,
        decision_reason="sma_golden_cross",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_side == "BUY"
    assert broker._last_qty == pytest.approx(0.0002)

    conn = ensure_db(str(tmp_path / "buy_authority_trace.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    conn.close()

    assert submit_attempt is not None
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["side"] == "BUY"
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.0)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.0)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.0)
    assert submit_evidence["intended_qty"] == pytest.approx(0.0002)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0002)
    assert submit_evidence["order_qty"] == pytest.approx(0.0002)

    normalized = build_position_state_model(
        raw_qty_open=float(lot_snapshot.raw_open_exposure_qty),
        metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
        raw_total_asset_qty=float(lot_snapshot.raw_total_asset_qty),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        market_price=100_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    assert lot_snapshot.open_lot_count == 0
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_lot_count == 1
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.0002)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.has_dust_only_remainder is True
    assert normalized.exit_allowed is False
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert normalized.terminal_state == "dust_only"


@pytest.mark.fast_regression
def test_authority_boundary_live_execute_signal_sell_does_not_sum_open_exposure_and_dust_tracking_for_submission(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_sum_regression.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_sum_regression.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00049193,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0004,
                "raw_total_asset_qty": 0.00049193,
                "open_exposure_qty": 0.0004,
                "dust_tracking_qty": 0.00009193,
                "open_lot_count": 1,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 1,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0004,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0004,
                        "raw_total_asset_qty": 0.00049193,
                        "open_exposure_qty": 0.0004,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0004,
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)

    conn = ensure_db(str(tmp_path / "sell_sum_regression.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0004)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["observed_position_qty"] == pytest.approx(0.0004)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_open_exposure_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_dust_tracking_qty"] == pytest.approx(0.00009193)
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00049193)
    assert submit_evidence["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["observed_sell_qty_basis_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["sell_qty_boundary_kind"] == "none"
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_authority_boundary_live_execute_signal_sell_uses_exit_sizing_executable_qty_for_final_submit_payload(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "build_sell_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            requested_qty=0.0004,
            executable_qty=0.0004,
            internal_lot_size=0.0004,
            intended_lot_count=1,
            executable_lot_count=1,
            qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00049193,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.00049193,
            "open_exposure_qty": 0.0004,
            "dust_tracking_qty": 0.00009193,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 1,
            "reserved_exit_lot_count": 0,
            "sellable_executable_qty": 0.00049193,
            "sellable_executable_lot_count": 1,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0004,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.00049193,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.00009193,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.00049193,
                    "sellable_executable_lot_count": 1,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": True,
                    "normalized_exposure_qty": 0.0004,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)
    assert broker._last_qty != pytest.approx(0.00049193)

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0004)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["observed_sell_qty_basis_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["sell_open_exposure_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_dust_tracking_qty"] == pytest.approx(0.00009193)
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_authority_boundary_live_execute_signal_sell_handoff_uses_canonical_normalized_lot_state_over_stale_qty_candidates(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_handoff_canonical_normalized.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    captured: dict[str, object] = {}

    def _capture_sell_execution_sizing(**kwargs):
        authority = kwargs["authority"]
        captured["authority"] = authority
        lot_qty = 0.0004
        executable_qty = float(authority.sellable_executable_lot_count) * lot_qty
        return SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            requested_qty=executable_qty,
            executable_qty=executable_qty,
            internal_lot_size=lot_qty,
            intended_lot_count=int(authority.sellable_executable_lot_count),
            executable_lot_count=int(authority.sellable_executable_lot_count),
            qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        )

    monkeypatch.setattr(live_module, "build_sell_execution_sizing", _capture_sell_execution_sizing)

    conn = ensure_db(str(tmp_path / "sell_handoff_canonical_normalized.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0036,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="sell_handoff_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "raw_qty_open": 0.0036,
            "raw_total_asset_qty": 0.0036,
            "open_exposure_qty": 0.0036,
            "sellable_executable_qty": 0.0036,
            "sellable_executable_lot_count": 9,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0036,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0036,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.0032,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 8,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0004,
                    "sellable_executable_lot_count": 1,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "exit_allowed": True,
                    "exit_block_reason": "none",
                    "normalized_exposure_active": True,
                    "normalized_exposure_qty": 0.0004,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="sell_handoff_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    authority = captured["authority"]
    assert authority.sellable_executable_lot_count == 1
    assert authority.exit_allowed is True
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)
    assert broker._last_qty != pytest.approx(0.0036)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_live_execute_signal_sell_ignores_exit_sizing_qty_source_shadow(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_exit_sizing_shadow_source.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "build_sell_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            requested_qty=0.0004,
            executable_qty=0.0004,
            internal_lot_size=0.0004,
            intended_lot_count=1,
            executable_lot_count=1,
            qty_source="position_state.raw_total_asset_qty",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_shadow_source.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00049193,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.00049193,
            "open_exposure_qty": 0.0004,
            "dust_tracking_qty": 0.00009193,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 1,
            "reserved_exit_lot_count": 0,
            "sellable_executable_qty": 0.00049193,
            "sellable_executable_lot_count": 1,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0004,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.00049193,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.00009193,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.00049193,
                    "sellable_executable_lot_count": 1,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": True,
                    "normalized_exposure_qty": 0.0004,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_shadow_source.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert submit_evidence["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)

@pytest.mark.fast_regression
def test_live_execute_signal_sell_uses_open_exposure_only_when_dust_tracking_coexists(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00021999,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00012, 3, 0, "lot-native", "open_exposure"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009999, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.00021999,
                "open_exposure_qty": 0.00012,
                "dust_tracking_qty": 0.00009999,
                "open_lot_count": 3,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 3,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0,
                        "raw_total_asset_qty": 0.00021999,
                        "open_exposure_qty": 0.00012,
                        "dust_tracking_qty": 0.00009999,
                        "open_lot_count": 3,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 3,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": False,
                        "normalized_exposure_qty": 0.0,
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.00012)

    conn = ensure_db(str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    intent_row = conn.execute(
        """
        SELECT event_type, qty, submit_attempt_id
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_row is not None
    assert float(order_row["qty_req"]) == pytest.approx(0.00012)
    assert float(order_row["qty_filled"]) == pytest.approx(0.00012)
    assert order_row["status"] == "FILLED"
    assert intent_row is not None
    assert float(intent_row["qty"]) == pytest.approx(0.00012)
    assert intent_row["submit_attempt_id"]
    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.00012)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.00012)
    assert submit_evidence["observed_position_qty"] == pytest.approx(0.00012)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.00012)
    assert submit_evidence["normalized_qty"] == pytest.approx(0.00012)
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00021999)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.00012)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.00009999)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_snaps_tiny_open_exposure_boundary_upward_with_harmless_dust(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 5)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00019192,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00009999, 0, 1, "lot-native", "dust_tracking"),
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009999,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.00009999,
            "dust_tracking_qty": 0.00009193,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.00009999,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                    "open_exposure_qty": 0.00009999,
                    "dust_tracking_qty": 0.00009193,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009999,
            "dust_local_qty": 0.00009999,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_999.0,
            "dust_local_notional_krw": 9_999.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    suppression_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        """
    ).fetchone()["n"]
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert suppression_count == 1
    assert order_row is None
    assert submit_attempt is None


@pytest.mark.fast_regression
def test_live_execute_signal_sell_classifies_qty_step_mismatch_broker_reject(monkeypatch, tmp_path):
    class _RejectingBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0002, asset_locked=0.0)

        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract: order_rules.BuyPriceNoneSubmitContract | None = None,
            submit_plan=None,
        ):
            raise BrokerRejectError(f"{side} qty does not match qty_step: qty={qty} qty_step=0.0001")

    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_qty_step_reject.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_qty_step_reject.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0002,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0002,
            "raw_total_asset_qty": 0.0002,
            "open_exposure_qty": 0.0002,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 1,
            "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
            "position_state_source": "context.raw_qty_open",
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0002,
                    "raw_total_asset_qty": 0.0002,
                    "open_exposure_qty": 0.0002,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 1,
                    "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                    "position_state_source": "context.raw_qty_open",
                    "entry_allowed": False,
                    "effective_flat": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    trade = live_execute_signal(
        _RejectingBroker(),
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None

    conn = ensure_db(str(tmp_path / "sell_qty_step_reject.sqlite"))
    submit_row = conn.execute(
        """
        SELECT submission_reason_code, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert submit_row is None


def test_sell_failure_category_prefers_boundary_kind_over_unsafe_mismatch():
    category = live_module._classify_sell_failure_category(
        reason_code=DUST_RESIDUAL_UNSELLABLE,
        dust_details={
            "sell_qty_boundary_kind": "min_qty",
            "qty_below_min": 0,
            "normalized_below_min": 0,
            "notional_below_min": 0,
            "dust_qty_gap_small": 1,
            "summary": "sell_qty_boundary_kind=min_qty dust_qty_gap_small=1",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN


def test_sell_failure_category_prefers_qty_step_boundary_kind():
    category = live_module._classify_sell_failure_category(
        reason_code=DUST_RESIDUAL_UNSELLABLE,
        dust_details={
            "sell_qty_boundary_kind": "qty_step",
            "summary": "sell_qty_boundary_kind=qty_step qty_step=0.0001",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH


@pytest.mark.fast_regression
def test_live_execute_signal_sell_blocks_when_only_dust_tracking_remains(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_tracking_only.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_tracking_only.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0,
            "raw_total_asset_qty": 0.00009193,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.00009193,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009193,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009193,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 1,
            "dust_classification": "blocking_dust",
            "dust_effective_flat": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "classification=blocking_dust harmless_dust=0 effective_flat=0",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_tracking_only.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    order_row = conn.execute(
        """
        SELECT client_order_id, status, side, qty_req
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_row = conn.execute(
        """
        SELECT reason_code, context_json, summary
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    dust_event_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert order_row is None
    assert submit_row is None
    assert dust_event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_sell_blocks_when_broker_precision_still_leaves_unsellable_dust(tmp_path):
    notifications: list[str] = []
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_precision_block.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_precision_block.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.000191939,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_precision_block.sqlite"))
    row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert row is None
    assert len(notifications) == 0
    monkeypatch.undo()


@pytest.mark.fast_regression
def test_live_execute_signal_sell_dust_unsellable_records_operational_event_and_dedups(tmp_path):
    notifications: list[str] = []
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_unsellable.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_unsellable.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()

    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_unsellable.sqlite"))
    order_row = conn.execute(
        """
        SELECT client_order_id, status, side, qty_req, decision_reason, exit_rule_name
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    event_rows = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, summary, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()[0]
    conn.close()

    assert order_row is None
    assert len(event_rows) == 1
    assert int(submit_attempt_count) == 0
    assert event_rows[0]["reason_code"] == DUST_RESIDUAL_UNSELLABLE
    assert int(event_rows[0]["seen_count"]) == 2
    assert event_rows[0]["dust_state"] == EXIT_PARTIAL_LEFT_DUST
    assert event_rows[0]["dust_action"] == MANUAL_DUST_REVIEW_REQUIRED
    assert "position_qty=0.000090000000" in str(event_rows[0]["summary"])
    assert "normalized_qty=0.000000000000" in str(event_rows[0]["summary"])
    assert "detail=boundary_below_min" in str(event_rows[0]["summary"])
    suppression_context = json.loads(str(event_rows[0]["context_json"]))
    assert suppression_context["raw_total_asset_qty"] == pytest.approx(0.00009)
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert suppression_context["terminal_state"] in {"dust_only", "non_executable_position"}
    assert suppression_context["exit_block_reason"] in {"dust_only_remainder", "no_position"}
    assert "decision_truth_sources" in suppression_context
    assert suppression_context["entry_allowed_truth_source"] == "-"
    assert suppression_context["effective_flat_truth_source"] == "-"
    assert suppression_context["observed_position_qty"] == pytest.approx(0.00009)
    assert suppression_context["submit_payload_qty"] == pytest.approx(0.0)
    assert suppression_context["normalized_qty"] == pytest.approx(0.0)
    assert EXIT_PARTIAL_LEFT_DUST in str(event_rows[0]["summary"])
    assert MANUAL_DUST_REVIEW_REQUIRED in str(event_rows[0]["summary"])
    assert len(notifications) == 2
    assert all("event=decision_suppressed" in msg for msg in notifications)
    assert all("dust_state=blocking_dust" in msg for msg in notifications)
    assert all("dust_action=manual_review_before_resume" in msg for msg in notifications)
    assert all("dust_new_orders_allowed=0" in msg for msg in notifications)
    assert all("dust_resume_allowed=0" in msg for msg in notifications)
    monkeypatch.undo()


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_partial_exit_dust_residue_stays_suppressed_after_reconcile(
    tmp_path,
):
    db_path = str(tmp_path / "sell_partial_exit_dust_reconcile.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    base_ts = 1_700_002_000_000
    buy_qty = 0.00085
    dust_qty = 0.00005
    partial_exit_qty = 0.0008

    conn = ensure_db(db_path)
    _record_test_order(conn, client_order_id="partial_exit_entry", side="BUY", qty_req=buy_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="partial_exit_entry",
        side="BUY",
        fill_id="fill_partial_exit_entry",
        fill_ts=base_ts,
        price=100_000_000.0,
        qty=buy_qty,
        fee=0.0,
        strategy_name="dust_exit_test",
        entry_decision_id=1001,
    )
    _record_test_order(
        conn,
        client_order_id="partial_exit_to_dust",
        side="SELL",
        qty_req=partial_exit_qty,
        ts_ms=base_ts + 60_000,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="partial_exit_to_dust",
        side="SELL",
        fill_id="fill_partial_exit_to_dust",
        fill_ts=base_ts + 60_000,
        price=101_000_000.0,
        qty=partial_exit_qty,
        fee=0.0,
        strategy_name="dust_exit_test",
        entry_decision_id=1001,
        exit_decision_id=1002,
        exit_reason="trim_to_dust",
        exit_rule_name="partial_trim",
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_DustOnlyBalanceBroker(asset_available=dust_qty))

    conn = ensure_db(db_path)
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    conn.close()
    normalized = build_position_state_model(
        raw_qty_open=dust_qty,
        metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
        raw_total_asset_qty=max(dust_qty, float(lot_snapshot.raw_total_asset_qty)),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        market_price=100_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    ).normalized_exposure

    broker = _DustOnlyBalanceBroker(asset_available=dust_qty)
    trade_first = live_execute_signal(
        broker,
        "SELL",
        base_ts + 120_000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        base_ts + 180_000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert lot_snapshot.open_lot_count == 0
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_lot_count == 1
    assert lot_snapshot.dust_tracking_qty == pytest.approx(dust_qty)
    assert normalized.sellable_executable_lot_count == 0
    assert normalized.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.has_executable_exposure is False
    assert normalized.exit_allowed is False
    assert normalized.terminal_state == "dust_only"
    assert normalized.exit_block_reason == "dust_only_remainder"
    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0

@pytest.mark.fast_regression
def test_live_execute_signal_sell_suppresses_harmless_dust_exit_without_order_row(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr(
        "bithumb_bot.broker.live._submit_attempt_id",
        lambda: (_ for _ in ()).throw(AssertionError("client_order_id must not be generated")),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.live._client_order_id",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("client_order_id must not be generated")),
    )
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert event_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert int(suppression_row["seen_count"]) == 2
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["dust_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["sell_failure_category"] == "dust_suppression"
    assert context["sell_failure_detail"] == "dust_suppression"
    assert context["requested_qty"] == pytest.approx(0.0)
    assert context["normalized_qty"] == pytest.approx(0.0)
    assert context["observed_position_qty"] == pytest.approx(0.0)
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["entry_allowed_truth_source"] == "-"
    assert context["effective_flat_truth_source"] == "-"
    assert context["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert context["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert any("event=decision_suppressed" in msg for msg in notifications)
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_falls_back_to_harmless_dust_suppression_when_sell_guard_raises(
    monkeypatch,
    tmp_path,
):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    original_suppression = live_module._record_harmless_dust_exit_suppression
    suppression_calls = {"count": 0}

    def _suppression_with_one_miss(*args, **kwargs):
        suppression_calls["count"] += 1
        if suppression_calls["count"] == 1:
            return False
        return original_suppression(*args, **kwargs)

    monkeypatch.setattr(
        live_module,
        "_record_harmless_dust_exit_suppression",
        _suppression_with_one_miss,
    )

    broker = _FakeBroker()
    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0
    assert suppression_calls["count"] >= 2

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert event_count == 0
    assert submit_attempt_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert int(suppression_row["seen_count"]) == 1
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["requested_qty"] == pytest.approx(0.0)
    assert context["normalized_qty"] == pytest.approx(0.0)
    assert context["entry_allowed_truth_source"] == "-"
    assert context["effective_flat_truth_source"] == "-"
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert context["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert any("event=decision_suppressed" in msg for msg in notifications)
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_no_executable_exit_suppresses_before_broker_submit(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_no_executable_exit.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "flatten_not_recent",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": 0.00009629,
        "dust_local_qty": 0.00009629,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 5_000.0,
        "dust_latest_price": 100_000_000.0,
        "dust_broker_notional_krw": 9_629.0,
        "dust_local_notional_krw": 9_629.0,
        "dust_broker_qty_is_dust": 1,
        "dust_local_qty_is_dust": 1,
        "dust_broker_notional_is_dust": 0,
        "dust_local_notional_is_dust": 0,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_no_executable_exit.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009629,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_001_200_000,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_001_140_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009629,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.00009629,
            "sell_submit_lot_count": 0,
            "sell_submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009629,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009629,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "terminal_state": "dust_only",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1200,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_no_executable_exit.sqlite"))
    submit_attempt_recorded_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()["n"]
    order_events = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt_recorded_count == 0
    assert order_events == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert "suppression_outcome=execution_suppressed" in str(suppression_row["summary"])
    assert "decision_suppressed:exit_suppressed_by_quantity_rule" in str(suppression_row["summary"])
    suppression_context = json.loads(str(suppression_row["context_json"]))
    assert suppression_context["suppression_outcome"] == "execution_suppressed"
    assert suppression_context["sell_submit_lot_count"] == 0
    assert suppression_context["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert suppression_context["exit_non_executable_reason"] == "dust_only_remainder"
    assert suppression_context["exit_sizing_block_reason"] == "dust_only_remainder"
    assert any("suppression_outcome=execution_suppressed" in msg for msg in notifications)
    assert any("event=decision_suppressed" in msg for msg in notifications)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_ignores_stale_recorded_sellable_qty_without_current_lot_state(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})

    conn = ensure_db(str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_001_300_000,
        strategy_name="stale_sell_context",
        signal="SELL",
        reason="stale sellable qty",
        candle_ts=1_700_001_240_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "raw_qty_open": 0.0002,
            "raw_total_asset_qty": 0.0002,
            "open_exposure_qty": 0.0002,
            "dust_tracking_qty": 0.0,
            "sellable_executable_qty": 0.0002,
            "sell_submit_lot_count": 2,
            "exit_allowed": True,
            "exit_block_reason": "none",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0002,
                    "raw_total_asset_qty": 0.0002,
                    "open_exposure_qty": 0.0002,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 2,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0002,
                    "sellable_executable_lot_count": 2,
                    "exit_allowed": True,
                    "exit_block_reason": "none",
                    "terminal_state": "open",
                    "normalized_exposure_qty": 0.0002,
                    "normalized_exposure_active": True,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1300,
        100_000_000.0,
        strategy_name="stale_sell_context",
        decision_id=decision_id,
        decision_reason="stale sellable qty",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    order_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM orders
        WHERE side='SELL'
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, context_json
        FROM order_suppressions
        WHERE strategy_name='stale_sell_context' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert suppression_row is None


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_boundary_does_not_override_canonical_authority_snapshot(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_boundary_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})

    conn = ensure_db(str(tmp_path / "sell_boundary_authority.sqlite"))
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0008,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    canonical_snapshot = replace(
        live_module.build_normalized_exposure(
            raw_qty_open=0.0,
            dust_context={"dust_residual_present": 0},
            raw_total_asset_qty=0.0,
            open_exposure_qty=0.0,
            dust_tracking_qty=0.0,
            reserved_exit_qty=0.0,
            open_lot_count=0,
            dust_tracking_lot_count=0,
            market_price=100_000_000.0,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            max_qty_decimals=8,
            exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        ),
        open_exposure_qty=0.0002,
        open_lot_count=2,
        sellable_executable_qty=0.0002,
        sellable_executable_lot_count=2,
        exit_allowed=True,
        exit_block_reason="none",
        terminal_state="open_exposure",
        normalized_exposure_active=True,
        has_executable_exposure=True,
        has_any_position_residue=True,
        has_non_executable_residue=False,
        has_dust_only_remainder=False,
        normalized_exposure_qty=0.0002,
    )
    monkeypatch.setattr("bithumb_bot.broker.live.build_normalized_exposure", lambda **_kwargs: canonical_snapshot)

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1400,
        100_000_000.0,
        strategy_name="boundary_authority_test",
        decision_reason="canonical snapshot",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_reserved_exit_state_submits_only_unreserved_lots(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_reserved_exit_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})
    lot_rules = build_market_lot_rules(
        market_id="BTC_KRW",
        market_price=100_000_000.0,
        rules=SimpleNamespace(
            min_qty=float(settings.LIVE_MIN_ORDER_QTY),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        ),
        source_mode="derived",
    )
    buy_qty = lot_count_to_qty(lot_count=3, lot_size=lot_rules.lot_size)
    reserved_exit_qty = lot_count_to_qty(lot_count=1, lot_size=lot_rules.lot_size)
    base_ts = int(time.time() * 1000)

    conn = ensure_db(str(tmp_path / "sell_reserved_exit_authority.sqlite"))
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=buy_qty,
        asset_locked=0.0,
    )
    record_order_if_missing(
        conn,
        client_order_id="entry_reserved",
        side="BUY",
        qty_req=buy_qty,
        symbol=settings.PAIR,
        price=100_000_000.0,
        ts_ms=base_ts,
        status="NEW",
    )
    apply_fill_lifecycle(
        conn,
        side="BUY",
        pair=settings.PAIR,
        trade_id=1,
        client_order_id="entry_reserved",
        fill_id="fill_entry_reserved",
        fill_ts=base_ts + 100,
        price=100_000_000.0,
        qty=buy_qty,
        fee=0.0,
        strategy_name="reserved_exit_test",
        entry_decision_id=501,
    )
    record_order_if_missing(
        conn,
        client_order_id="reserved_exit",
        side="SELL",
        qty_req=reserved_exit_qty,
        symbol=settings.PAIR,
        price=100_000_000.0,
        ts_ms=base_ts + 200,
        status="NEW",
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        base_ts + 300,
        100_000_000.0,
        strategy_name="reserved_exit_test",
        decision_reason="trim_after_partial_exit",
        exit_rule_name="exit_signal",
    )
    conn = ensure_db(str(tmp_path / "sell_reserved_exit_authority.sqlite"))
    attempted_order = conn.execute(
        """
        SELECT qty_req, status
        FROM orders
        WHERE client_order_id LIKE 'live_%' AND side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert trade is None
    assert broker.place_order_calls == 0
    assert attempted_order is not None
    assert attempted_order["status"] == "FAILED"
    assert float(attempted_order["qty_req"]) == pytest.approx(buy_qty - reserved_exit_qty)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_reserved_exit_pending_remains_explicit_no_submit_outcome(
    monkeypatch, tmp_path, caplog
):
    db_path = str(tmp_path / "sell_reserved_exit_pending.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})
    lot_rules = build_market_lot_rules(
        market_id="BTC_KRW",
        market_price=100_000_000.0,
        rules=SimpleNamespace(
            min_qty=float(settings.LIVE_MIN_ORDER_QTY),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        ),
        source_mode="derived",
    )
    buy_qty = lot_count_to_qty(lot_count=1, lot_size=lot_rules.lot_size)
    base_ts = int(time.time() * 1000)

    conn = ensure_db(db_path)
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=buy_qty,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_reserved_full",
            base_ts,
            100_000_000.0,
            buy_qty,
            1,
            0,
            "lot-native",
            "open_exposure",
        ),
    )
    record_order_if_missing(
        conn,
        client_order_id="reserved_exit_full",
        side="SELL",
        qty_req=buy_qty,
        symbol=settings.PAIR,
        price=100_000_000.0,
        ts_ms=base_ts + 200,
        status="NEW",
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=base_ts + 250,
        strategy_name="reserved_exit_pending_test",
        signal="SELL",
        reason="trim_after_partial_exit",
        candle_ts=base_ts + 150,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": buy_qty,
            "raw_total_asset_qty": buy_qty,
            "open_exposure_qty": buy_qty,
            "dust_tracking_qty": 0.0,
            "sellable_executable_lot_count": 0,
            "sellable_executable_qty": 0.0,
            "reserved_exit_qty": buy_qty,
            "reserved_exit_lot_count": 1,
            "exit_allowed": False,
            "exit_block_reason": "reserved_for_open_sell_orders",
            "terminal_state": "reserved_exit_pending",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": buy_qty,
                    "raw_total_asset_qty": buy_qty,
                    "open_exposure_qty": buy_qty,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_qty": buy_qty,
                    "reserved_exit_lot_count": 1,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "reserved_for_open_sell_orders",
                    "terminal_state": "reserved_exit_pending",
                    "normalized_exposure_qty": buy_qty,
                    "normalized_exposure_active": True,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )

    before_live_sell_orders = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM orders
        WHERE client_order_id LIKE 'live_%' AND side='SELL'
        """
    ).fetchone()["n"]
    before_submit_attempt_recorded = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()["n"]
    before_submit_events = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    before_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    before_reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    conn.commit()
    conn.close()

    assert before_reserved_exit_qty == pytest.approx(buy_qty)

    broker = _DustOnlyBalanceBroker(asset_available=buy_qty)
    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        trade = live_execute_signal(
            broker,
            "SELL",
            base_ts + 300,
            100_000_000.0,
            strategy_name="reserved_exit_pending_test",
            decision_id=decision_id,
            decision_reason="trim_after_partial_exit",
            exit_rule_name="exit_signal",
        )

    conn = ensure_db(db_path)
    after_live_sell_orders = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM orders
        WHERE client_order_id LIKE 'live_%' AND side='SELL'
        """
    ).fetchone()["n"]
    after_submit_attempt_recorded = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()["n"]
    after_submit_events = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE strategy_name='reserved_exit_pending_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    after_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    after_reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    conn.close()

    assert trade is None
    assert broker.place_order_calls == 0
    assert after_live_sell_orders == before_live_sell_orders
    assert after_submit_attempt_recorded == before_submit_attempt_recorded
    assert after_submit_events == before_submit_events
    assert after_reserved_exit_qty == pytest.approx(before_reserved_exit_qty)
    assert after_snapshot.open_lot_count == before_snapshot.open_lot_count
    assert after_snapshot.dust_tracking_lot_count == before_snapshot.dust_tracking_lot_count
    assert suppression_row is None
    assert "exit inventory already reserved" in caplog.text
    assert "reason=reserved_for_open_sell_orders" in caplog.text
    assert "reserved_exit_qty=" in caplog.text


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
@pytest.mark.parametrize(
    ("state_name", "asset_available", "lot_row", "reconcile_metadata", "expected_stage_calls", "expected_place_orders", "expected_submit_attempts"),
    [
        (
            "open_exposure",
            0.0002,
            (0.0002, 2, 0, "open_exposure"),
            {"dust_residual_present": 0},
            ["position_state", "intent", "feasibility", "execution"],
            1,
            1,
        ),
        (
            "flat",
            0.0,
            None,
            {"dust_residual_present": 0},
            ["position_state", "intent"],
            0,
            0,
        ),
        (
            "dust_only",
            0.00009193,
            (0.00009193, 0, 1, "dust_tracking"),
            {
                "dust_residual_present": 1,
                "dust_classification": "blocking_dust",
                "dust_effective_flat": 0,
                "dust_policy_reason": "dangerous_dust_operator_review_required",
                "dust_residual_summary": "classification=blocking_dust harmless_dust=0 effective_flat=0",
            },
            ["position_state", "intent"],
            0,
            0,
        ),
    ],
)
def test_live_execute_signal_sell_runtime_flow_varies_by_normalized_state(
    monkeypatch,
    tmp_path,
    state_name,
    asset_available,
    lot_row,
    reconcile_metadata,
    expected_stage_calls,
    expected_place_orders,
    expected_submit_attempts,
):
    db_path = str(tmp_path / f"flow_{state_name}.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata=reconcile_metadata)

    conn = ensure_db(db_path)
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=asset_available,
        asset_locked=0.0,
    )
    if lot_row is not None:
        qty_open, executable_lot_count, dust_tracking_lot_count, position_state = lot_row
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
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                1,
                f"entry_{state_name}",
                1_700_000_000_000,
                100_000_000.0,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                "lot-native",
                position_state,
            ),
        )
    conn.commit()
    conn.close()

    stage_calls: list[str] = []

    original_position_state = live_module._determine_live_execution_position_state
    original_intent = live_module._determine_live_execution_intent
    original_feasibility = live_module._evaluate_live_execution_feasibility
    original_execution = live_module._execute_live_submission_and_application

    def _record_position_state(*args, **kwargs):
        stage_calls.append("position_state")
        return original_position_state(*args, **kwargs)

    def _record_intent(*args, **kwargs):
        stage_calls.append("intent")
        return original_intent(*args, **kwargs)

    def _record_feasibility(*args, **kwargs):
        stage_calls.append("feasibility")
        return original_feasibility(*args, **kwargs)

    def _record_execution(*args, **kwargs):
        stage_calls.append("execution")
        return original_execution(*args, **kwargs)

    monkeypatch.setattr(live_module, "_determine_live_execution_position_state", _record_position_state)
    monkeypatch.setattr(live_module, "_determine_live_execution_intent", _record_intent)
    monkeypatch.setattr(live_module, "_evaluate_live_execution_feasibility", _record_feasibility)
    monkeypatch.setattr(live_module, "_execute_live_submission_and_application", _record_execution)

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name=f"flow_{state_name}",
        decision_reason=f"flow_{state_name}",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(db_path)
    live_sell_order_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM orders
        WHERE client_order_id LIKE 'live_%' AND side='SELL'
        """
    ).fetchone()["n"]
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()["n"]
    conn.close()

    assert stage_calls == expected_stage_calls
    assert broker.place_order_calls == expected_place_orders
    assert submit_attempt_count == expected_submit_attempts
    assert live_sell_order_count == expected_submit_attempts
    if state_name == "open_exposure":
        assert trade is not None
    else:
        assert trade is None


@pytest.mark.fast_regression
def test_live_execute_signal_records_signed_request_phase_event(monkeypatch, tmp_path):
    db_path = str(tmp_path / "signed_request_phase.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})

    conn = ensure_db(db_path)
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0002,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_signed_phase",
            1_700_000_000_000,
            100_000_000.0,
            0.0002,
            2,
            0,
            "lot-native",
            "open_exposure",
        ),
    )
    conn.commit()
    conn.close()

    trade = live_execute_signal(
        _FakeBroker(),
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="signed_phase_test",
        decision_reason="signed_phase_test",
        exit_rule_name="exit_signal",
    )

    assert trade is not None

    conn = ensure_db(db_path)
    signed_event = conn.execute(
        """
        SELECT event_type, submission_reason_code, submit_evidence
        FROM order_events
        WHERE event_type='submit_attempt_signed'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert signed_event is not None
    assert str(signed_event["submission_reason_code"]) == "signed_request_prepared"
    signed_evidence = json.loads(str(signed_event["submit_evidence"]))
    assert signed_evidence["submit_phase"] == "signed_request"
    assert signed_evidence["execution_state"] == "signed_request_prepared"
    assert signed_evidence["signed_request_id"].endswith(":signed_request")


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_reserved_exit_stops_before_execution_stage(monkeypatch, tmp_path):
    db_path = str(tmp_path / "reserved_exit_flow.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})
    lot_rules = build_market_lot_rules(
        market_id="BTC_KRW",
        market_price=100_000_000.0,
        rules=SimpleNamespace(
            min_qty=float(settings.LIVE_MIN_ORDER_QTY),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        ),
        source_mode="derived",
    )
    buy_qty = lot_count_to_qty(lot_count=1, lot_size=lot_rules.lot_size)
    base_ts = int(time.time() * 1000)

    conn = ensure_db(db_path)
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=buy_qty,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_reserved_full",
            base_ts,
            100_000_000.0,
            buy_qty,
            1,
            0,
            "lot-native",
            "open_exposure",
        ),
    )
    record_order_if_missing(
        conn,
        client_order_id="reserved_exit_full",
        side="SELL",
        qty_req=buy_qty,
        symbol=settings.PAIR,
        price=100_000_000.0,
        ts_ms=base_ts + 200,
        status="NEW",
    )
    conn.commit()
    conn.close()

    execution_calls = {"count": 0}
    original_execution = live_module._execute_live_submission_and_application

    def _record_execution(*args, **kwargs):
        execution_calls["count"] += 1
        return original_execution(*args, **kwargs)

    monkeypatch.setattr(live_module, "_execute_live_submission_and_application", _record_execution)

    broker = _DustOnlyBalanceBroker(asset_available=buy_qty)
    trade = live_execute_signal(
        broker,
        "SELL",
        base_ts + 300,
        100_000_000.0,
        strategy_name="reserved_exit_flow",
        decision_reason="reserved exit blocks",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(db_path)
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    conn.close()

    assert trade is None
    assert broker.place_order_calls == 0
    assert execution_calls["count"] == 0
    assert submit_attempt_count == 0


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_orderability_constraint_stops_before_execution_stage(monkeypatch, tmp_path):
    db_path = str(tmp_path / "sell_orderability_flow.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})

    conn = ensure_db(db_path)
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0002,
        asset_locked=0.0,
    )
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
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )
    conn.commit()
    conn.close()

    execution_calls = {"count": 0}
    original_execution = live_module._execute_live_submission_and_application

    def _record_execution(*args, **kwargs):
        execution_calls["count"] += 1
        return original_execution(*args, **kwargs)

    def _block_on_step(*, qty: float, market_price: float) -> float:
        raise SellDustGuardError(
            "order qty below minimum after step normalization",
            details={
                "state": "blocking_dust",
                "operator_action": "manual_review_before_resume",
                "position_qty": float(qty),
                "normalized_qty": 0.0,
                "requested_qty": float(qty),
                "min_qty": 0.0001,
                "sell_notional_krw": 0.0,
                "min_notional_krw": 0.0,
                "qty_below_min": 1,
                "normalized_non_positive": 1,
                "normalized_below_min": 1,
                "notional_below_min": 0,
                "qty_step": 0.0001,
                "max_qty_decimals": 8,
                "dust_scope": "position_qty",
                "dust_signature": "flow-test-orderability",
                "notify_dust_state": "blocking_dust",
                "notify_dust_action": "manual_review_before_resume",
                "new_orders_allowed": 0,
                "resume_allowed": 0,
                "treat_as_flat": 0,
            },
        )

    monkeypatch.setattr(live_module, "_execute_live_submission_and_application", _record_execution)
    monkeypatch.setattr(live_module, "adjust_sell_order_qty_for_dust_safety", _block_on_step)

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="orderability_flow",
        decision_reason="step constraint",
        exit_rule_name="exit_signal",
    )

    conn = ensure_db(db_path)
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert trade is None
    assert broker.place_order_calls == 0
    assert execution_calls["count"] == 0
    assert submit_attempt_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_UNSELLABLE

def test_bithumb_broker_defensively_rejects_sell_qty_below_executable_threshold(monkeypatch):
    original = {
        "LIVE_DRY_RUN": bool(settings.LIVE_DRY_RUN),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
    }
    try:
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)

        broker = BithumbBroker()
        monkeypatch.setattr(broker, "_market", lambda: "KRW-BTC")
        monkeypatch.setattr(
            live_module,
            "get_effective_order_rules",
            lambda _pair: SimpleNamespace(
                rules=SimpleNamespace(
                    market_id="KRW-BTC",
                    bid_min_total_krw=5000.0,
                    ask_min_total_krw=5000.0,
                    bid_price_unit=1.0,
                    ask_price_unit=1.0,
                    order_types=("limit", "price", "market"),
                    order_sides=("ask", "bid"),
                    bid_fee=0.0025,
                    ask_fee=0.0025,
                    maker_bid_fee=0.0020,
                    maker_ask_fee=0.0020,
                    min_qty=0.0001,
                    qty_step=0.0001,
                    min_notional_krw=5000.0,
                    max_qty_decimals=8,
                )
            ),
        )

        monkeypatch.setattr(
            broker,
            "_post_private",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("broker should reject before HTTP")),
        )

        with pytest.raises(BrokerRejectError, match="explicit SubmitPlan"):
            broker.place_order(client_order_id="cid-defensive", side="SELL", qty=0.00005, price=None)
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_sell_no_executable_exit_suppression_keeps_observational_position_qty_non_authoritative(
    monkeypatch,
    tmp_path,
):
    db_path = str(tmp_path / "sell-no-executable-exit-suppression.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    conn = ensure_db(db_path)
    try:
        recorded = live_module._record_sell_no_executable_exit_suppression(
            conn=conn,
            state=runtime_state.snapshot(),
            ts=100,
            market_price=100_000_000.0,
            canonical_sell=live_module._CanonicalSellExecutionView(
                sellable_executable_lot_count=0,
                sellable_executable_qty=0.0,
                exit_allowed=False,
                exit_block_reason="no_executable_exit_lot",
                submit_qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
                position_state_source="position_state.normalized_exposure.sellable_executable_lot_count",
            ),
            diagnostic_qty=live_module._SellDiagnosticQtyView(
                observed_position_qty=0.0004,
                observed_position_qty_source="observation.sell_qty_preview",
                raw_total_asset_qty=0.0004,
                open_exposure_qty=0.0,
                dust_tracking_qty=0.0004,
            ),
            strategy_name="suppression_test",
            decision_id=None,
            decision_reason="no executable lots",
            exit_rule_name="exit_signal",
            decision_observability={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": 0.0004,
                "raw_total_asset_qty": 0.0004,
                "exit_block_reason": "no_executable_exit_lot",
                "sell_submit_lot_count": 0,
            },
            exit_sizing=SimpleNamespace(
                allowed=False,
                block_reason="no_executable_exit_lot",
                decision_reason_code="exit_suppressed_by_quantity_rule",
                intended_lot_count=0,
                executable_lot_count=0,
                executable_qty=0.0,
                internal_lot_size=0.0001,
                effective_min_trade_qty=0.0001,
                min_qty=0.0001,
                min_notional_krw=5000.0,
            ),
        )
        conn.commit()
        suppression_row = conn.execute(
            """
            SELECT context_json
            FROM order_suppressions
            WHERE strategy_name='suppression_test'
            ORDER BY updated_ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert recorded is True
    assert suppression_row is not None
    suppression_context = json.loads(str(suppression_row["context_json"]))
    assert suppression_context["observed_position_qty"] == pytest.approx(0.0004)
    assert suppression_context["sell_open_exposure_qty"] == pytest.approx(0.0)
    assert suppression_context["observed_sell_qty_basis_qty"] == pytest.approx(0.0)
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert suppression_context["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_sell_dust_error_path_keeps_canonical_authority_separate_from_observational_qty(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_error_path.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda _msg: None)

    conn = ensure_db()
    try:
        init_portfolio(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET asset_qty=?, asset_available=?, cash_krw=?, cash_available=?
            WHERE id=1
            """,
            (0.00049193, 0.00049193, 1_000_000.0, 1_000_000.0),
        )
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
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_200,
            strategy_name="sell_dust_error_path",
            signal="SELL",
            reason="partial_take_profit",
            candle_ts=1_700_000_000_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0004,
                        "raw_total_asset_qty": 0.00049193,
                        "open_exposure_qty": 0.0004,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
                        "sellable_executable_qty": 0.0004,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "exit_allowed": True,
                        "exit_block_reason": "none",
                        "terminal_state": "open_exposure",
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0004,
                    }
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    dust_details = {
        "state": EXIT_PARTIAL_LEFT_DUST,
        "operator_action": MANUAL_DUST_REVIEW_REQUIRED,
        "position_qty": 0.0004,
        "normalized_qty": 0.0,
        "min_qty": 0.0001,
        "sell_notional_krw": 40_000.0,
        "min_notional_krw": 0.0,
        "qty_below_min": 0,
        "normalized_non_positive": 1,
        "normalized_below_min": 0,
        "notional_below_min": 0,
        "qty_step": 0.0001,
        "max_qty_decimals": 8,
        "dust_signature": "synthetic_pretrade_error_path",
        "summary": "synthetic dust guard failure for authority separation test",
    }

    captured: dict[str, object] = {}

    def _capture_sell_dust_unsellable(**kwargs):
        captured["canonical_sell"] = kwargs["canonical_sell"]
        captured["diagnostic_qty"] = kwargs["diagnostic_qty"]
        return True

    monkeypatch.setattr(live_module, "_record_harmless_dust_exit_suppression", lambda **_kwargs: False)
    monkeypatch.setattr(
        live_module,
        "adjust_sell_order_qty_for_dust_safety",
        lambda **_kwargs: (_ for _ in ()).throw(SellDustGuardError("blocked", details=dust_details)),
    )
    monkeypatch.setattr(live_module, "_record_sell_dust_unsellable", _capture_sell_dust_unsellable)

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="sell_dust_error_path",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0
    canonical_sell = captured["canonical_sell"]
    diagnostic_qty = captured["diagnostic_qty"]
    assert canonical_sell.submit_qty_source == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert canonical_sell.sellable_executable_qty == pytest.approx(0.0004)
    assert diagnostic_qty.observed_position_qty == pytest.approx(0.00049193)
    assert diagnostic_qty.raw_total_asset_qty == pytest.approx(0.00049193)
    assert diagnostic_qty.observed_position_qty > canonical_sell.sellable_executable_qty


@pytest.mark.fast_regression
def test_harmless_dust_exit_suppression_blocks_sell_path_even_without_sub_min_qty_preview(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_state_only.sqlite"))
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "flatten_not_recent",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": 0.00009193,
        "dust_local_qty": 0.00009193,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 5_000.0,
        "dust_latest_price": 100_000_000.0,
        "dust_broker_notional_krw": 9_193.0,
        "dust_local_notional_krw": 9_193.0,
        "dust_broker_qty_is_dust": 1,
        "dust_local_qty_is_dust": 1,
        "dust_broker_notional_is_dust": 0,
        "dust_local_notional_is_dust": 0,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_state_only.sqlite"))
    try:
        state = type("_State", (), {"last_reconcile_metadata": json.dumps(metadata)})()
        suppressed = live_module._record_harmless_dust_exit_suppression(
            conn=conn,
            state=state,
            signal="SELL",
            side="SELL",
            requested_qty=0.0002,
            market_price=100_000_000.0,
            normalized_qty=0.0002,
            submit_qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
            position_state_source="position_state.normalized_exposure.sellable_executable_lot_count",
            strategy_name="dust_exit_test",
            decision_id=77,
            decision_reason="partial_take_profit",
            exit_rule_name="exit_signal",
        )
        conn.commit()

        order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
        suppression_row = conn.execute(
            """
            SELECT reason_code, dust_state, dust_action, summary, context_json
            FROM order_suppressions
            WHERE strategy_name='dust_exit_test' AND signal='SELL'
            """
        ).fetchone()
    finally:
        conn.close()

    assert suppressed is True
    assert order_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    assert "suppression_scope=harmless_dust_effective_flat" in suppression_row["summary"]
    assert "effective_flat_due_to_harmless_dust=1" in suppression_row["summary"]
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert len(notifications) == 1
    assert "reason_code=DUST_RESIDUAL_SUPPRESSED" in notifications[0]


@pytest.mark.fast_regression
def test_live_execute_signal_sell_with_dust_and_unresolved_open_order_still_records_dust_evidence(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_with_open.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    now_ms = int(time.time() * 1000)
    conn = ensure_db(str(tmp_path / "sell_dust_with_open.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open_sell','ex_open_sell','NEW','SELL',100000000.0,0.00009,0,?,?,NULL)
        """,
        (now_ms, now_ms),
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        3000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_with_open.sqlite"))
    dust_blocked = conn.execute(
        """
        SELECT reason_code, summary
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    dust_rows = conn.execute(
        """
        SELECT reason_code
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    conn.close()

    assert dust_blocked is not None
    assert dust_blocked["reason_code"] == DUST_RESIDUAL_UNSELLABLE
    assert EXIT_PARTIAL_LEFT_DUST in str(dust_blocked["summary"])
    assert MANUAL_DUST_REVIEW_REQUIRED in str(dust_blocked["summary"])
    assert len(dust_rows) == 1
    assert any("event=decision_suppressed" in msg for msg in notifications)


def test_validate_pretrade_applies_side_specific_min_total():
    from bithumb_bot.broker import live as live_module
    from bithumb_bot.broker import order_rules

    original_pair = settings.PAIR
    object.__setattr__(settings, "PAIR", "KRW-BTC")

    class _BalanceOnlyBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1_000_000.0, asset_available=100.0, cash_locked=0.0, asset_locked=0.0)

    broker = _BalanceOnlyBroker()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=5100.0,
                ask_min_total_krw=5000.0,
                min_notional_krw=7000.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={},
        ),
    )
    monkeypatch.setattr(
        live_module,
        "_load_live_reference_quote",
        lambda **_kwargs: {
            "bid": 99_999_000.0,
            "ask": 100_000_000.0,
            "reference_price": 100_000_000.0,
            "reference_ts_epoch_sec": 1_700_000_000.0,
            "reference_source": "test",
        },
    )
    _stub_submit_plan_quote(monkeypatch)
    monkeypatch.setattr(
        live_module,
        "_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=5100.0,
                ask_min_total_krw=5000.0,
                min_notional_krw=7000.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={},
        ),
    )
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    try:
        with pytest.raises(ValueError, match=r"order notional below minimum \(BUY\): 5050.00 < 5100.00"):
            validate_pretrade(broker=broker, side="BUY", qty=50.5, market_price=100.0)

        validate_pretrade(broker=broker, side="SELL", qty=50.5, market_price=100.0)
    finally:
        object.__setattr__(settings, "PAIR", original_pair)
        monkeypatch.undo()

def test_live_submit_attempt_reason_codes_cover_ambiguous_paths(tmp_path, monkeypatch):
    from bithumb_bot.broker import order_rules
    import bithumb_bot.order_sizing as order_sizing

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "bootstrap_live_state.sqlite"))
    runtime_state.enable_trading()
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                min_notional_krw=0.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={"source": "test"},
        ),
    )
    monkeypatch.setattr(
        order_sizing,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                min_notional_krw=0.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={"source": "test"},
        ),
    )
    monkeypatch.setattr(
        live_module,
        "_load_live_reference_quote",
        lambda **_kwargs: {
            "bid": 99_999_000.0,
            "ask": 100_000_000.0,
            "reference_price": 100_000_000.0,
            "reference_ts_epoch_sec": 1_700_000_000.0,
            "reference_source": "test",
        },
    )
    _stub_submit_plan_quote(monkeypatch)
    _stub_live_effective_order_rules(monkeypatch)

    scenarios = [
        ("success", _FakeBroker(), 1100, "confirmed_success"),
        ("failed_before_send", _FailingSubmitBroker(), 1101, "failed_before_send"),
        ("timeout", _TimeoutBroker(), 1102, "sent_but_response_timeout"),
        ("transport", _TransportErrorBroker(), 1103, "sent_but_transport_error"),
        ("ambiguous", _NoExchangeIdBroker(), 1104, "ambiguous_response"),
    ]

    for name, broker, ts, expected_reason_code in scenarios:
        db_path = str(tmp_path / f"submit_reason_{name}.sqlite")
        object.__setattr__(settings, "DB_PATH", db_path)

        live_execute_signal(broker, "BUY", ts, 100000000.0)

        conn = ensure_db(db_path)
        row = conn.execute(
            "SELECT client_order_id FROM orders WHERE client_order_id LIKE ? ORDER BY id DESC LIMIT 1",
            (f"%_{ts}_buy_%",),
        ).fetchone()
        assert row is not None
        attempt = conn.execute(
            """
            SELECT submission_reason_code, submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (row["client_order_id"],),
        ).fetchone()
        conn.close()

        assert attempt is not None
        assert attempt["submission_reason_code"] == expected_reason_code
        assert attempt["submit_attempt_id"]
        assert attempt["symbol"] == settings.PAIR
        assert attempt["side"] == "BUY"
        assert float(attempt["qty"]) > 0
        assert attempt["price"] is None or float(attempt["price"]) > 0
        assert int(attempt["submit_ts"]) == ts
        assert attempt["payload_fingerprint"]


class _JournaledReconcileBroker:
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "NEW", None, 0.0, 0.0, 0, 0)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

    def get_read_journal_summary(self) -> dict[str, str]:
        return {
            "/v1/accounts": "{'path': '/v1/accounts', 'status': '0000', 'row_count': 1}",
            "/v1/orders(open_orders)": "{'path': '/v1/orders(open_orders)', 'status': '0000', 'row_count': 0}",
        }


def test_reconcile_persists_broker_read_journal_metadata(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "reconcile_journal.sqlite"))

    reconcile_with_broker(_JournaledReconcileBroker())

    state = runtime_state.snapshot()
    assert state.last_reconcile_metadata is not None
    payload = json.loads(state.last_reconcile_metadata)
    assert "broker_read_journal" in payload
    assert "/v1/accounts" in str(payload["broker_read_journal"])
