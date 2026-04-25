from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import (
    BrokerBalance,
    BrokerFill,
    BrokerIdentifierMismatchError,
    BrokerOrder,
)
from bithumb_bot.config import settings
from bithumb_bot.db_core import compute_accounting_replay, ensure_db, get_portfolio_breakdown
from bithumb_bot.dust import build_dust_display_context, build_position_state_model
from bithumb_bot.engine import (
    evaluate_startup_safety_gate,
    maybe_clear_stale_initial_reconcile_halt,
    run_loop,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from bithumb_bot.oms import set_exchange_order_id, set_status
from bithumb_bot.order_sizing import SellExecutionAuthority, build_sell_execution_sizing
from bithumb_bot.recovery import (
    RecoveryDisposition,
    RecoveryProgressState,
    classify_recovery_outcome,
    reconcile_with_broker,
)
import bithumb_bot.recovery as recovery_module
from tests.test_failsafe import _set_live_runtime_paths


pytestmark = pytest.mark.slow_integration


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    db_path = tmp_path / "restart_regression.sqlite"

    monkeypatch.setenv("DB_PATH", str(db_path))
    _set_live_runtime_paths(monkeypatch, base_dir=tmp_path.resolve())
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")

    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)

    yield db_path

    object.__setattr__(settings, "DB_PATH", old_db_path)
    object.__setattr__(settings, "MODE", old_mode)


class _NoopBroker:
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-noop", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

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

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)


class _ClientOnlyLookupBroker(_NoopBroker):
    def __init__(self) -> None:
        self.calls: list[tuple[str | None, str | None]] = []

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.calls.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id or "ex-client-only", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)


class _IdentifierMismatchLookupBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise BrokerIdentifierMismatchError("identifier mismatch during myorder lookup")


class _RecentFillBroker(_NoopBroker):
    def __init__(self, *, status: str = "FILLED") -> None:
        self.status = status

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-partial", "BUY", self.status, 100.0, 1.0, 1.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="fill-rest",
                fill_ts=220,
                price=100.0,
                qty=0.6,
                fee=0.0,
                exchange_order_id="ex-partial",
            )
        ]


class _RecentSellZeroFeeBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-zero-fee", "SELL", "FILLED", 100000000.0, 0.1, 0.1, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id=str(client_order_id or ""),
                fill_id="fill-zero-fee",
                fill_ts=220,
                price=100000000.0,
                qty=0.1,
                fee=0.0,
                exchange_order_id=str(exchange_order_id or "ex-zero-fee"),
            )
        ]


class _AggregateDuplicateFillBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-dup", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 2)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-dup:aggregate:201",
                fill_ts=201,
                price=100.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-dup",
            )
        ]


class _TerminalFillReplayBroker(_NoopBroker):
    def __init__(self, *, balance: BrokerBalance | None = None) -> None:
        self._balance = balance or BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-filled-replay:aggregate:201",
                fill_ts=201,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id="ex-filled-replay",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        return self._balance


class _FilledFlatReplayBroker(_NoopBroker):
    def __init__(self, *, balance: BrokerBalance) -> None:
        self._balance = balance

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-flat-buy:aggregate:301",
                fill_ts=301,
                price=100.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-flat-buy",
            ),
            BrokerFill(
                client_order_id="",
                fill_id="ex-flat-sell:aggregate:302",
                fill_ts=302,
                price=110.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-flat-sell",
            ),
        ]

    def get_balance(self) -> BrokerBalance:
        return self._balance


class _DustBalanceBroker(_NoopBroker):
    def __init__(self, *, asset_available: float, asset_locked: float = 0.0) -> None:
        self._balance = BrokerBalance(
            cash_available=1000.0,
            cash_locked=0.0,
            asset_available=asset_available,
            asset_locked=asset_locked,
        )

    def get_balance(self) -> BrokerBalance:
        return self._balance


class _RecentSellMissingPriceBroker(_NoopBroker):
    def __init__(self, *, repeat_count: int = 1) -> None:
        self._repeat_count = max(1, int(repeat_count))

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        fills: list[BrokerFill] = []
        for idx in range(self._repeat_count):
            fills.append(
                BrokerFill(
                    client_order_id="",
                    fill_id=f"ex-sell-missing-price-{idx}",
                    fill_ts=300 + idx,
                    price=0.0,
                    qty=1.0,
                    fee=0.0,
                    exchange_order_id="ex-reconcile-sell",
                )
            )
        return fills


class _RecentSellValidPriceBroker(_NoopBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-sell-valid-price",
                fill_ts=330,
                price=110.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-reconcile-sell",
            )
        ]


class _FailAfterWriteBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-write-fail", "BUY", "PARTIAL", 100.0, 1.0, 0.4, 1, 2)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id=client_order_id or "",
                fill_id="write-before-fail",
                fill_ts=200,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id=exchange_order_id or "ex-write-fail",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        raise RuntimeError("boom after ledger write")


class _SubmitUnknownRecentFillBroker(_NoopBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="submit_timeout_restart",
                fill_id="submit_unknown_fill",
                fill_ts=300,
                price=100.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-submit-unknown-fill",
            )
        ]


class _SubmitUnknownMissingFeeRecentFillBroker(_NoopBroker):
    def __init__(self) -> None:
        self.parse_modes: list[str] = []

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        parse_mode: str = "strict",
    ) -> list[BrokerFill]:
        self.parse_modes.append(parse_mode)
        if parse_mode == "strict":
            raise RuntimeError("strict fill parsing should not be used for restart observation")
        return [
            BrokerFill(
                client_order_id="submit_timeout_restart",
                fill_id="submit_unknown_missing_fee_fill",
                fill_ts=300,
                price=100.0,
                qty=1.0,
                fee=None,
                exchange_order_id="ex-submit-unknown-missing-fee-fill",
                fee_status="missing",
                parse_warnings=("missing_fee_field",),
                raw={"uuid": "submit_unknown_missing_fee_fill", "price": "100", "volume": "1"},
            )
        ]


class _KnownOrderLevelFeeCandidateRecentFillBroker(_NoopBroker):
    def __init__(self) -> None:
        self.parse_modes: list[str] = []

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        parse_mode: str = "strict",
    ) -> list[BrokerFill]:
        self.parse_modes.append(parse_mode)
        if parse_mode == "strict":
            raise RuntimeError("strict fill parsing should not be used for restart observation")
        return [
            BrokerFill(
                client_order_id="known_fee_candidate",
                fill_id="known_order_level_fee_candidate_fill",
                fill_ts=300,
                price=100_000_000.0,
                qty=0.001,
                fee=50.0,
                exchange_order_id="ex-known-fee-candidate",
                fee_status="order_level_candidate",
                parse_warnings=("missing_fee_field", "order_level_fee_candidate:paid_fee"),
                raw={
                    "trade": {"uuid": "known_order_level_fee_candidate_fill", "price": "100000000", "volume": "0.001"},
                    "order_fee_fields": {"paid_fee": "50.0"},
                },
            )
        ]


class _SubmitUnknownRecentOrderBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-order",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            )
        ]


class _SubmitUnknownRecentOrderNoExchangeBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id=None,
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            )
        ]


class _SubmitUnknownStrongCorrelationBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-strong",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            )
        ]


class _SubmitUnknownWeakMetadataCorrelationBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-weak",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=0.5,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            )
        ]

class _SubmitUnknownMultipleStrongCandidatesBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-candidate-1",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            ),
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-candidate-2",
                side="BUY",
                status="FAILED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=251,
                updated_ts=261,
            ),
        ]


class _SubmitUnknownIncompatibleCorrelationBroker(_NoopBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-bad-status",
                side="BUY",
                status="PENDING_SUBMIT",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
            ),
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-bad-side",
                side="SELL",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=251,
                updated_ts=261,
            ),
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-bad-qty",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=2.0,
                qty_filled=0.0,
                created_ts=252,
                updated_ts=262,
            ),
        ]


def _insert_submit_timeout_attempt_metadata(*, conn, client_order_id: str, submit_attempt_id: str, qty: float = 1.0) -> None:
    conn.execute(
        """
        UPDATE orders
        SET submit_attempt_id=?
        WHERE client_order_id=?
        """,
        (submit_attempt_id, client_order_id),
    )
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id, event_type, event_ts, order_status, qty, side, submit_attempt_id, timeout_flag
        ) VALUES (?, 'submit_attempt_preflight', 101, 'PENDING_SUBMIT', ?, 'BUY', ?, 0)
        """,
        (client_order_id, float(qty), submit_attempt_id),
    )
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id, event_type, event_ts, order_status, qty, side, submit_attempt_id, timeout_flag
        ) VALUES (?, 'submit_attempt_recorded', 102, 'SUBMIT_UNKNOWN', ?, 'BUY', ?, 1)
        """,
        (client_order_id, float(qty), submit_attempt_id),
    )



def test_recovery_classification_marks_recent_fill_as_auto_recoverable_candidate():
    classification = classify_recovery_outcome(
        reason_code="RECENT_FILL_APPLIED",
        metadata={"recent_fill_applied": 1, "submit_unknown_unresolved": 0},
        source_conflicts=[],
    )

    assert classification.disposition == RecoveryDisposition.AUTO_RECOVERABLE_CANDIDATE
    assert classification.progress_state == RecoveryProgressState.CANDIDATE_IDENTIFIED


def test_recovery_classification_keeps_ambiguous_evidence_manual_recovery_required():
    classification = classify_recovery_outcome(
        reason_code="SUBMIT_UNKNOWN_UNRESOLVED",
        metadata={"recent_fill_applied": 0, "submit_unknown_unresolved": 1},
        source_conflicts=[],
    )

    assert classification.disposition == RecoveryDisposition.MANUAL_RECOVERY_REQUIRED
    assert classification.progress_state == RecoveryProgressState.MANUAL_INTERVENTION_REQUIRED


def test_recovery_classification_source_conflict_remains_hard_stop():
    classification = classify_recovery_outcome(
        reason_code="SOURCE_CONFLICT_HALT",
        metadata={"recent_fill_applied": 1, "submit_unknown_unresolved": 0},
        source_conflicts=["exchange_order_id=abc conflicting status"],
    )

    assert classification.disposition == RecoveryDisposition.HARD_STOP
    assert classification.progress_state == RecoveryProgressState.HALTED

class _CancelRaceBroker(_NoopBroker):
    def __init__(self) -> None:
        self.remote_status = "NEW"

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        if self.remote_status == "CANCELED":
            return []
        return [BrokerOrder("", "ex-cancel-race", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)]

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.remote_status = "CANCELED"
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cancel-race", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cancel-race", "BUY", self.remote_status, 100.0, 1.0, 0.0, 1, 1)


class _ApiErrorBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise RuntimeError("broker api unavailable")



def _resolved_order_rules(*, min_qty: float, min_notional_krw: float):
    rules = type(
        "_Rules",
        (),
        {
            "min_qty": float(min_qty),
            "min_notional_krw": float(min_notional_krw),
        },
    )
    return type("_Resolved", (), {"rules": rules})()


def _latest_reconcile_metadata() -> dict[str, object]:
    state = runtime_state.snapshot()
    if state.last_reconcile_metadata is None:
        return {}
    return json.loads(str(state.last_reconcile_metadata))


def _load_reconciled_position_authority(*, db_path: Path):
    metadata = _latest_reconcile_metadata()
    dust_context = build_dust_display_context(metadata)
    conn = ensure_db(str(db_path))
    try:
        portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
        asset_qty = float(portfolio_row["asset_qty"] or 0.0) if portfolio_row is not None else 0.0
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    finally:
        conn.close()

    return build_position_state_model(
        raw_qty_open=asset_qty,
        metadata_raw=metadata,
        raw_total_asset_qty=max(
            asset_qty,
            float(lot_snapshot.raw_total_asset_qty),
            float(dust_context.raw_holdings.broker_qty),
        ),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        reserved_exit_qty=float(reserved_exit_qty),
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
    ).normalized_exposure


def _seed_dust_state(*, db_path: Path, asset_qty: float, close_price: float) -> None:
    conn = ensure_db(str(db_path))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000.0, ?, 1000.0, 0.0, ?, 0.0)
            """,
            (asset_qty, asset_qty),
        )
        conn.execute(
            """
            INSERT INTO candles(pair, interval, ts, open, high, low, close, volume)
            VALUES ('KRW-BTC', '1m', 1, ?, ?, ?, ?, 1.0)
            """,
            (close_price, close_price, close_price, close_price),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_trade_residue_state(
    *,
    db_path: Path,
    entry_client_order_id: str,
    exit_client_order_id: str,
    buy_qty: float,
    sell_qty: float,
    buy_price: float,
    sell_price: float,
    buy_fee: float,
    sell_fee: float,
    base_ts: int,
) -> None:
    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id=entry_client_order_id,
            side="BUY",
            qty_req=buy_qty,
            price=buy_price,
            ts_ms=base_ts,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id=entry_client_order_id,
            side="BUY",
            fill_id=f"{entry_client_order_id}_fill",
            fill_ts=base_ts,
            price=buy_price,
            qty=buy_qty,
            fee=buy_fee,
            strategy_name="sma_with_filter",
            entry_decision_id=501,
        )
        record_order_if_missing(
            conn,
            client_order_id=exit_client_order_id,
            side="SELL",
            qty_req=sell_qty,
            price=sell_price,
            ts_ms=base_ts + 60_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id=exit_client_order_id,
            side="SELL",
            fill_id=f"{exit_client_order_id}_fill",
            fill_ts=base_ts + 60_000,
            price=sell_price,
            qty=sell_qty,
            fee=sell_fee,
            strategy_name="sma_with_filter",
            entry_decision_id=501,
            exit_decision_id=502,
            exit_reason="trim_to_dust",
            exit_rule_name="partial_trim",
        )
        conn.commit()
    finally:
        conn.close()


def _seed_reserved_exit_authority_state(
    *,
    db_path: Path,
    asset_qty: float,
    executable_lot_count: int,
    reserved_sell_qty: float,
    sell_status: str,
    base_ts: int,
) -> None:
    conn = ensure_db(str(db_path))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000.0, ?, 1000.0, 0.0, ?, 0.0)
            """,
            (asset_qty, asset_qty),
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
                f"reserved_seed_{base_ts}",
                base_ts,
                40_000_000.0,
                asset_qty,
                executable_lot_count,
                0,
                "lot-native",
                "open_exposure",
            ),
        )
        record_order_if_missing(
            conn,
            client_order_id=f"reserved_sell_{base_ts}",
            side="SELL",
            qty_req=reserved_sell_qty,
            price=41_000_000.0,
            ts_ms=base_ts + 60_000,
            status=sell_status,
        )
        conn.commit()
    finally:
        conn.close()


def test_reconcile_marks_equal_dust_with_recent_partial_flatten_as_resume_safe(isolated_db, monkeypatch):
    _seed_dust_state(db_path=isolated_db, asset_qty=0.00009629, close_price=40_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (True, "flatten_recent(age_sec=10.0,trigger=kill-switch)"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.00009629))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 1
    assert metadata["dust_policy_reason"] == "matched_harmless_dust_resume_allowed"
    assert metadata["dust_classification"] == "harmless_dust"
    assert int(metadata["dust_partial_flatten_recent"]) == 1


@pytest.mark.lot_native_regression_gate
def test_reserved_exit_authority_from_unresolved_sell_orders_fully_suppresses_new_sell_sizing(
    isolated_db,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    _seed_reserved_exit_authority_state(
        db_path=isolated_db,
        asset_qty=0.0008,
        executable_lot_count=2,
        reserved_sell_qty=0.0008,
        sell_status="NEW",
        base_ts=1_700_002_090_000,
    )

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    finally:
        conn.close()

    sizing = build_sell_execution_sizing(
        pair=settings.PAIR,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            # Reserved exit is canonical SELL-submit authority: unresolved SELL
            # orders reduce the lot count passed into execution sizing.
            sellable_executable_lot_count=authority.sellable_executable_lot_count,
            exit_allowed=authority.exit_allowed,
            exit_block_reason=authority.exit_block_reason,
        ),
    )

    assert lot_snapshot.open_lot_count == 2
    assert reserved_exit_qty == pytest.approx(0.0008)
    assert authority.reserved_exit_qty == pytest.approx(0.0008)
    assert authority.reserved_exit_lot_count > 0
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "reserved_for_open_sell_orders"
    assert authority.terminal_state == "reserved_exit_pending"
    assert sizing.allowed is False
    assert sizing.requested_qty == pytest.approx(0.0)
    assert sizing.executable_qty == pytest.approx(0.0)
    assert sizing.block_reason == "reserved_for_open_sell_orders"


@pytest.mark.lot_native_regression_gate
def test_reserved_exit_authority_from_unresolved_sell_orders_allows_only_unreserved_sell_remainder(
    isolated_db,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    _seed_reserved_exit_authority_state(
        db_path=isolated_db,
        asset_qty=0.0012,
        executable_lot_count=3,
        reserved_sell_qty=0.0004,
        sell_status="PARTIAL",
        base_ts=1_700_002_095_000,
    )

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    finally:
        conn.close()

    sizing = build_sell_execution_sizing(
        pair=settings.PAIR,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=authority.sellable_executable_lot_count,
            exit_allowed=authority.exit_allowed,
            exit_block_reason=authority.exit_block_reason,
        ),
    )

    assert reserved_exit_qty == pytest.approx(0.0004)
    assert authority.open_lot_count == 3
    assert authority.reserved_exit_qty == pytest.approx(0.0004)
    assert authority.reserved_exit_lot_count == 1
    assert authority.sellable_executable_lot_count == 2
    assert authority.sellable_executable_qty == pytest.approx(0.0008)
    assert authority.exit_allowed is True
    assert authority.exit_block_reason == "none"
    assert authority.terminal_state == "open_exposure"
    assert sizing.allowed is True
    assert sizing.intended_lot_count == 2
    assert sizing.executable_lot_count == 2
    assert sizing.requested_qty == pytest.approx(0.0008)
    assert sizing.executable_qty == pytest.approx(0.0008)


def test_reconcile_preserves_fee_residue_as_dust_only_restart_authority(isolated_db, monkeypatch):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    fee_residue_qty = 0.00005
    _seed_trade_residue_state(
        db_path=isolated_db,
        entry_client_order_id="fee_residue_entry",
        exit_client_order_id="fee_residue_exit",
        buy_qty=0.00085,
        sell_qty=0.0008,
        buy_price=40_000_000.0,
        sell_price=41_000_000.0,
        buy_fee=0.0,
        sell_fee=0.0,
        base_ts=1_700_002_100_000,
    )
    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=fee_residue_qty))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert authority.open_lot_count == 0
    assert authority.dust_tracking_lot_count == 1
    assert authority.raw_total_asset_qty == pytest.approx(fee_residue_qty)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.dust_tracking_qty == pytest.approx(fee_residue_qty)
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.has_executable_exposure is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "dust_only_remainder"
    assert authority.terminal_state == "dust_only"
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(fee_residue_qty)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(fee_residue_qty)


@pytest.mark.lot_native_regression_gate
def test_restart_reconcile_residual_account_asset_closed_strategy_exposure_stays_non_executable(
    isolated_db,
    monkeypatch,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    residual_qty = 0.00005
    _seed_trade_residue_state(
        db_path=isolated_db,
        entry_client_order_id="restart_closed_exposure_entry",
        exit_client_order_id="restart_closed_exposure_exit",
        buy_qty=0.00085,
        sell_qty=0.0008,
        buy_price=40_000_000.0,
        sell_price=41_000_000.0,
        buy_fee=0.0,
        sell_fee=0.0,
        base_ts=1_700_002_150_000,
    )
    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=residual_qty))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    authority_surface = authority.as_dict()
    interpretation = build_position_state_model(
        raw_qty_open=authority.raw_total_asset_qty,
        metadata_raw=_latest_reconcile_metadata(),
        raw_total_asset_qty=authority.raw_total_asset_qty,
        open_exposure_qty=authority.open_exposure_qty,
        dust_tracking_qty=authority.dust_tracking_qty,
        reserved_exit_qty=authority.reserved_exit_qty,
        open_lot_count=authority.open_lot_count,
        dust_tracking_lot_count=authority.dust_tracking_lot_count,
    ).state_interpretation
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        sell_order_count = int(
            conn.execute("SELECT COUNT(*) FROM orders WHERE side='SELL'").fetchone()[0]
        )
        submit_attempt_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_events WHERE event_type='submit_attempt_recorded'"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    assert authority.raw_total_asset_qty == pytest.approx(residual_qty)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.open_lot_count == 0
    assert authority.dust_tracking_qty == pytest.approx(residual_qty)
    assert authority.dust_tracking_lot_count == 1
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.has_executable_exposure is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "dust_only_remainder"
    assert authority.terminal_state == "dust_only"
    assert authority_surface["holding_authority_state"] == "dust_only"
    assert authority_surface["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert interpretation.exit_submit_expected is False
    assert interpretation.operator_outcome == "tracked_unsellable_residual"
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(residual_qty)
    assert sell_order_count == 1
    assert submit_attempt_count == 0


@pytest.mark.lot_native_regression_gate
def test_restart_reconcile_keeps_open_exposure_dust_tracking_and_recovery_required_distinct(
    isolated_db,
    monkeypatch,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(isolated_db))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000.0, ?, 1000.0, 0.0, ?, 0.0)
            """,
            (0.00045, 0.00045),
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
            (settings.PAIR, 1, "restart_open_exposure", 1_700_002_300_000, 40_000_000.0, 0.0004, 4, 0, "lot-native", "open_exposure"),
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
            (settings.PAIR, 2, "restart_dust_tracking", 1_700_002_360_000, 40_000_000.0, 0.00005, 0, 1, "lot-native", "dust_tracking"),
        )
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    startup_blocker_before = evaluate_startup_safety_gate()

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.00045))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        order_row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    startup_blocker_after = evaluate_startup_safety_gate()

    assert startup_blocker_before is not None
    assert "submit_unknown_orders=1" in startup_blocker_before
    assert authority.raw_total_asset_qty == pytest.approx(0.00045)
    assert authority.open_exposure_qty == pytest.approx(0.0004)
    assert authority.dust_tracking_qty == pytest.approx(0.00005)
    assert authority.open_lot_count == 4
    assert authority.dust_tracking_lot_count == 1
    assert authority.sellable_executable_lot_count == 4
    assert authority.sellable_executable_qty == pytest.approx(0.0004)
    assert authority.exit_allowed is True
    assert authority.terminal_state == "open_exposure"
    assert authority.recovery_blocked is True
    assert authority.recovery_required_count == 1
    assert authority.recovery_block_reason == "recovery_required_present"
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(0.00045)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0004)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.00005)
    assert order_row is not None
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["exchange_order_id"] is None
    assert "manual recovery required" in str(order_row["last_error"])
    assert startup_blocker_after is not None
    assert "recovery_required_orders=1" in startup_blocker_after


@pytest.mark.lot_native_regression_gate
def test_restart_reconcile_keeps_large_holdings_without_lot_metadata_blocked_not_dust(
    isolated_db,
    monkeypatch,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(isolated_db))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000.0, ?, 1000.0, 0.0, ?, 0.0)
            """,
            (0.0008, 0.0008),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.0008))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()
    gate_reason = evaluate_startup_safety_gate()

    assert authority.raw_total_asset_qty == pytest.approx(0.0008)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.dust_tracking_qty == pytest.approx(0.0)
    assert authority.open_lot_count == 0
    assert authority.dust_tracking_lot_count == 0
    assert authority.sellable_executable_lot_count == 0
    assert authority.has_executable_exposure is False
    assert authority.has_dust_only_remainder is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "legacy_lot_metadata_missing"
    assert authority.terminal_state == "non_executable_position"
    assert authority.authority_gap_reason == "authority_missing_recovery_required"
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(0.0)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.0)
    assert gate_reason is not None
    assert "position_authority_gap=authority_missing_recovery_required" in gate_reason


@pytest.mark.lot_native_regression_gate
def test_restart_reconcile_incomplete_lot_authority_keeps_qty_only_holdings_fail_closed_and_blocked(
    isolated_db,
    monkeypatch,
):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(isolated_db))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000.0, ?, 1000.0, 0.0, ?, 0.0)
            """,
            (0.0008, 0.0008),
        )
        record_order_if_missing(
            conn,
            client_order_id="missing_lot_authority_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="RECOVERY_REQUIRED",
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.0008))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    authority_surface = authority.as_dict()
    interpretation = build_position_state_model(
        raw_qty_open=authority.raw_total_asset_qty,
        metadata_raw=_latest_reconcile_metadata(),
        raw_total_asset_qty=authority.raw_total_asset_qty,
        open_exposure_qty=authority.open_exposure_qty,
        dust_tracking_qty=authority.dust_tracking_qty,
        reserved_exit_qty=authority.reserved_exit_qty,
        open_lot_count=authority.open_lot_count,
        dust_tracking_lot_count=authority.dust_tracking_lot_count,
    ).state_interpretation
    gate_reason = evaluate_startup_safety_gate()
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        sell_order_count = int(conn.execute("SELECT COUNT(*) FROM orders WHERE side='SELL'").fetchone()[0])
        submit_attempt_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_events WHERE event_type='submit_attempt_recorded'"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    assert authority.raw_total_asset_qty == pytest.approx(0.0008)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.dust_tracking_qty == pytest.approx(0.0)
    assert authority.open_lot_count == 0
    assert authority.dust_tracking_lot_count == 0
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.has_executable_exposure is False
    assert authority.has_dust_only_remainder is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "legacy_lot_metadata_missing"
    assert authority.terminal_state == "non_executable_position"
    assert authority.authority_gap_reason == "authority_missing_recovery_required"
    assert authority.recovery_blocked is True
    assert authority.recovery_required_count == 1
    assert authority.recovery_block_reason == "recovery_required_present"
    assert authority_surface["holding_authority_state"] == "non_executable_position"
    assert authority_surface["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert interpretation.exit_submit_expected is False
    assert "manual recovery is required" in interpretation.operator_message
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(0.0)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(0.0)
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason
    assert sell_order_count == 0
    assert submit_attempt_count == 0


def test_reconcile_preserves_rounding_residue_as_dust_only_restart_authority(isolated_db, monkeypatch):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    rounded_residue_qty = 0.00001
    _seed_trade_residue_state(
        db_path=isolated_db,
        entry_client_order_id="rounding_residue_entry",
        exit_client_order_id="rounding_residue_exit",
        buy_qty=0.00081,
        sell_qty=0.0008,
        buy_price=40_000_000.0,
        sell_price=41_000_000.0,
        buy_fee=0.0,
        sell_fee=0.0,
        base_ts=1_700_002_200_000,
    )
    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=rounded_residue_qty))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert authority.open_lot_count == 0
    assert authority.dust_tracking_lot_count == 1
    assert authority.raw_total_asset_qty == pytest.approx(rounded_residue_qty)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.dust_tracking_qty == pytest.approx(rounded_residue_qty)
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.has_executable_exposure is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "dust_only_remainder"
    assert authority.terminal_state == "dust_only"
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(rounded_residue_qty)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(rounded_residue_qty)


def test_reconcile_classifies_reviewed_below_min_residual_lot_as_dust_only_restart_authority(isolated_db, monkeypatch):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    reviewed_residual_qty = 0.00009997
    _seed_trade_residue_state(
        db_path=isolated_db,
        entry_client_order_id="reviewed_residual_entry",
        exit_client_order_id="reviewed_residual_exit",
        buy_qty=0.00089997,
        sell_qty=0.0008,
        buy_price=40_000_000.0,
        sell_price=41_000_000.0,
        buy_fee=0.0,
        sell_fee=0.0,
        base_ts=1_700_002_250_000,
    )
    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=0.0),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=reviewed_residual_qty))

    authority = _load_reconciled_position_authority(db_path=isolated_db)
    conn = ensure_db(str(isolated_db))
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert authority.raw_total_asset_qty == pytest.approx(reviewed_residual_qty)
    assert authority.open_exposure_qty == pytest.approx(0.0)
    assert authority.dust_tracking_qty == pytest.approx(reviewed_residual_qty)
    assert authority.open_lot_count == 0
    assert authority.dust_tracking_lot_count == 1
    assert authority.sellable_executable_lot_count == 0
    assert authority.sellable_executable_qty == pytest.approx(0.0)
    assert authority.has_executable_exposure is False
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "dust_only_remainder"
    assert authority.terminal_state == "dust_only"
    assert lot_snapshot.raw_total_asset_qty == pytest.approx(reviewed_residual_qty)
    assert lot_snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert lot_snapshot.dust_tracking_qty == pytest.approx(reviewed_residual_qty)


def test_reconcile_marks_equal_dust_without_recent_flatten_as_resume_safe_when_notional_is_also_dust(
    isolated_db,
    monkeypatch,
):
    _seed_dust_state(db_path=isolated_db, asset_qty=0.00009629, close_price=40_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (False, "flatten_not_recent"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.00009629))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 1
    assert metadata["dust_policy_reason"] == "matched_harmless_dust_resume_allowed"
    assert metadata["dust_classification"] == "harmless_dust"
    assert int(metadata["dust_partial_flatten_recent"]) == 0


def test_reconcile_blocks_local_only_dust_gap_without_broker_match(isolated_db, monkeypatch):
    _seed_dust_state(db_path=isolated_db, asset_qty=0.00009629, close_price=40_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (False, "flatten_not_recent"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.0))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 0
    assert metadata["dust_policy_reason"] == "dangerous_dust_operator_review_required"
    assert metadata["dust_classification"] == "blocking_dust"
    assert "classification=blocking_dust" in str(metadata["dust_residual_summary"])


@pytest.mark.parametrize(
    ("local_qty", "broker_qty"),
    [
        (0.00009629, 0.0),
        (0.0, 0.00009629),
    ],
    ids=["local_only_dust", "broker_only_dust"],
)
def test_reconcile_blocks_one_sided_dust_gap_without_broker_local_match(
    isolated_db,
    monkeypatch,
    local_qty,
    broker_qty,
):
    _seed_dust_state(db_path=isolated_db, asset_qty=local_qty, close_price=40_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (False, "flatten_not_recent"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=broker_qty))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 0
    assert metadata["dust_policy_reason"] == "dangerous_dust_operator_review_required"
    assert metadata["dust_classification"] == "blocking_dust"
    summary = str(metadata["dust_residual_summary"])
    assert "classification=blocking_dust" in summary
    assert f"broker_qty={broker_qty:.8f}" in summary
    assert f"local_qty={local_qty:.8f}" in summary
    assert f"delta={(broker_qty - local_qty):.8f}" in summary


def test_reconcile_blocks_matched_dust_when_broker_local_gap_exceeds_tolerance(isolated_db, monkeypatch):
    _seed_dust_state(db_path=isolated_db, asset_qty=0.00001, close_price=40_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (False, "flatten_not_recent"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.000099))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 0
    assert metadata["dust_policy_reason"] == "dangerous_dust_operator_review_required"
    assert metadata["dust_classification"] == "blocking_dust"

    assert int(metadata["dust_qty_gap_small"]) == 0
    summary = str(metadata["dust_residual_summary"])
    assert "classification=blocking_dust" in summary
    assert "broker_qty=0.00009900" in summary
    assert "local_qty=0.00001000" in summary


def test_reconcile_blocks_qty_dust_when_notional_is_still_tradeable(isolated_db, monkeypatch):
    _seed_dust_state(db_path=isolated_db, asset_qty=0.00009629, close_price=100_000_000.0)

    monkeypatch.setattr(
        recovery_module,
        "get_effective_order_rules",
        lambda _pair: _resolved_order_rules(min_qty=0.0001, min_notional_krw=5000.0),
    )
    monkeypatch.setattr(
        recovery_module,
        "_is_partial_flatten_recent",
        lambda *, now_sec: (False, "flatten_not_recent"),
    )

    reconcile_with_broker(_DustBalanceBroker(asset_available=0.00009629))

    metadata = _latest_reconcile_metadata()
    assert int(metadata["dust_residual_present"]) == 1
    assert int(metadata["dust_residual_allow_resume"]) == 1
    assert metadata["dust_policy_reason"] == "matched_harmless_dust_resume_allowed"
    assert metadata["dust_classification"] == "harmless_dust"
    assert "classification=harmless_dust" in str(metadata["dust_residual_summary"])


def test_restart_after_submit_immediate_exit_keeps_gate_blocked(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert reason is not None
    assert "unresolved_open_orders=1" in reason
    assert state.unresolved_open_order_count == 1
    assert state.startup_gate_reason == reason


def test_startup_gate_explicitly_blocks_pending_submit_order(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="pending_submit_blocker",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()

    assert reason is not None
    assert "pending_submit_orders=1" in reason


def test_startup_gate_explicitly_blocks_submit_unknown_order(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_unknown_blocker",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()

    assert reason is not None
    assert "submit_unknown_orders=1" in reason


def test_submit_timeout_then_restart_moves_to_recovery_required_and_stays_blocked(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_NoopBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert reason is not None
    assert "recovery_required_orders=1" in reason
    assert state.unresolved_open_order_count == 1


def test_submit_unknown_ambiguous_remote_fill_on_restart_escalates(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert float(row["qty_filled"]) == pytest.approx(0.0)
    gate_reason = evaluate_startup_safety_gate()
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason


def test_submit_unknown_recent_fill_restart_path_escalates_instead_of_silent_resolution(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()[0]
    finally:
        conn.close()

    state = runtime_state.snapshot()
    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert fill_count == 0
    gate_reason = evaluate_startup_safety_gate()
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason
    assert state.unresolved_open_order_count == 1


def test_submit_unknown_timeout_metadata_recent_order_without_exchange_resolves_on_restart(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderNoExchangeBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error, qty_filled FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    gate_reason = evaluate_startup_safety_gate()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert row["exchange_order_id"] is None
    assert float(row["qty_filled"]) == pytest.approx(0.0)
    assert autolink is not None
    assert "outcome=order_only" in str(autolink["message"])
    assert gate_reason is None


def test_submit_unknown_timeout_metadata_recent_fill_only_resolves_on_restart(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error, qty_filled FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    gate_reason = evaluate_startup_safety_gate()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-submit-unknown-fill"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert autolink is not None
    assert "outcome=fill_only" in str(autolink["message"])
    assert gate_reason is None


def test_submit_unknown_recent_fill_missing_fee_applies_principal_with_pending_fee(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    broker = _SubmitUnknownMissingFeeRecentFillBroker()
    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error, qty_filled FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fee, fee_accounting_status FROM fills WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        observation = conn.execute(
            """
            SELECT fill_id, fee, fee_status, accounting_status, source, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='submit_timeout_restart'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    state = runtime_state.snapshot()
    metadata = json.loads(str(state.last_reconcile_metadata))
    gate_reason = evaluate_startup_safety_gate()

    assert broker.parse_modes
    assert set(broker.parse_modes) == {"salvage"}
    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-submit-unknown-missing-fee-fill"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert row["last_error"] is None
    assert fill_count["cnt"] == 1
    assert fill_row["fee"] == pytest.approx(0.0)
    assert fill_row["fee_accounting_status"] == "principal_applied_fee_pending"
    assert observation is not None
    assert observation["fill_id"] == "submit_unknown_missing_fee_fill"
    assert observation["fee"] is None
    assert observation["fee_status"] == "missing"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "reconcile_recent_activity_fee_pending"
    assert "missing_fee_field" in str(observation["parse_warnings"])
    assert state.last_reconcile_reason_code == "FILL_FEE_PENDING_RECOVERY_REQUIRED"
    assert metadata["observed_fill_count"] == 1
    assert metadata["fee_pending_fill_count"] == 1
    assert metadata["fee_pending_auto_recovering"] == 1
    assert metadata["fee_pending_latest_fee_status"] == "missing"
    assert gate_reason is None


def test_startup_gate_fee_pending_auto_recovery_clears_persistent_process_pause(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="startup_fee_pending",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=100,
            status="ACCOUNTING_PENDING",
        )
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, accounting_status, source, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                101,
                "startup_fee_pending",
                "ex-startup-fee-pending",
                "startup-fee-fill",
                101,
                "BUY",
                100_000_000.0,
                0.001,
                None,
                "missing",
                "fee_pending",
                "test_startup_gate_fee_pending",
                '{"fixture":"startup_fee_pending"}',
            ),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="FILL_FEE_PENDING_RECOVERY_REQUIRED",
        metadata={
            "fee_pending_auto_recovering": 1,
            "fee_pending_fill_count": 1,
            "fee_pending_latest_fee_status": "missing",
            "fee_pending_latest_fill_id": "startup-fee-fill",
            "balance_split_mismatch_count": 0,
        },
        now_epoch_sec=0.0,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="startup safety gate",
        reason_code="STARTUP_SAFETY_GATE",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    cleared = maybe_clear_stale_initial_reconcile_halt()
    state = runtime_state.snapshot()

    assert cleared is True
    assert state.trading_enabled is True
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert state.resume_gate_blocked is True
    assert "fee_pending_auto_recovering=1" in str(state.resume_gate_reason)


def test_known_recent_fill_order_level_fee_candidate_stays_observation_until_fee_confirmed(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="known_fee_candidate",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("known_fee_candidate", "ex-known-fee-candidate", conn=conn)
        conn.commit()
    finally:
        conn.close()

    broker = _KnownOrderLevelFeeCandidateRecentFillBroker()
    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        order = conn.execute(
            "SELECT status, last_error, qty_filled FROM orders WHERE client_order_id='known_fee_candidate'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fee, fee_accounting_status FROM fills WHERE client_order_id='known_fee_candidate'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='known_fee_candidate'"
        ).fetchone()
        trade_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE client_order_id='known_fee_candidate'"
        ).fetchone()
        adjustment_count = conn.execute("SELECT COUNT(*) AS cnt FROM external_cash_adjustments").fetchone()
        observation = conn.execute(
            """
            SELECT fill_id, fee, fee_status, accounting_status, source, parse_warnings, raw_payload
            FROM broker_fill_observations
            WHERE client_order_id='known_fee_candidate'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        replay = compute_accounting_replay(conn)
    finally:
        conn.close()

    state = runtime_state.snapshot()
    metadata = json.loads(str(state.last_reconcile_metadata))
    gate_reason = evaluate_startup_safety_gate()

    assert broker.parse_modes
    assert set(broker.parse_modes) == {"salvage"}
    assert order is not None
    assert order["status"] == "FILLED"
    assert float(order["qty_filled"]) == pytest.approx(0.001)
    assert order["last_error"] is None
    assert fill_count["cnt"] == 1
    assert fill_row["fee"] == pytest.approx(0.0)
    assert fill_row["fee_accounting_status"] == "principal_applied_fee_pending"
    assert trade_count["cnt"] == 1
    assert adjustment_count["cnt"] == 0
    assert observation is not None
    assert observation["fill_id"] == "known_order_level_fee_candidate_fill"
    assert observation["fee"] == pytest.approx(50.0)
    assert observation["fee_status"] == "order_level_candidate"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "reconcile_recent_activity_fee_pending"
    assert "order_level_fee_candidate:paid_fee" in str(observation["parse_warnings"])
    assert "paid_fee" in str(observation["raw_payload"])
    assert state.last_reconcile_reason_code == "FILL_FEE_PENDING_RECOVERY_REQUIRED"
    assert metadata["fee_pending_auto_recovering"] == 1
    assert metadata["fee_pending_latest_fee_status"] == "order_level_candidate"
    assert replay["broker_fill_observation_count"] >= 1
    assert replay["broker_fill_fee_pending_count"] >= 1
    assert replay["broker_fill_fee_candidate_order_level_count"] >= 1
    assert replay["broker_fill_missing_fee_count"] == 0
    assert "broker_fill_observations" in replay["omitted_event_families"]
    assert gate_reason is None


def test_submit_unknown_weak_order_correlation_on_restart_escalates(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    gate_reason = evaluate_startup_safety_gate()
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason


def test_submit_unknown_recent_order_restart_path_escalates_to_manual_recovery(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    state = runtime_state.snapshot()
    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    gate_reason = evaluate_startup_safety_gate()
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason
    assert state.unresolved_open_order_count == 1


def test_ambiguous_submit_persists_across_restart_until_reconcile_runs(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    startup_blocker = evaluate_startup_safety_gate()
    state_before = runtime_state.snapshot()

    assert startup_blocker is not None
    assert "submit_unknown_orders=1" in startup_blocker
    assert state_before.startup_gate_reason == startup_blocker

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    startup_blocker_after_reconcile = evaluate_startup_safety_gate()
    state_after = runtime_state.snapshot()
    assert startup_blocker_after_reconcile is not None
    assert "recovery_required_orders=1" in startup_blocker_after_reconcile
    assert state_after.unresolved_open_order_count == 1



@pytest.mark.lot_native_regression_gate
def test_restart_after_partial_fill_applies_recent_fill_and_clears_gate(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="partial_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("partial_crash", "ex-partial", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="partial_crash",
            side="BUY",
            fill_id="fill-part",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("partial_crash", "PARTIAL", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='partial_crash'"
        ).fetchone()
        fills = conn.execute("SELECT COUNT(*) AS c FROM fills WHERE client_order_id='partial_crash'").fetchone()[0]
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()
    authority = _load_reconciled_position_authority(db_path=isolated_db)

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fills == 2
    assert reason is None
    assert state.unresolved_open_order_count == 0
    assert authority.open_lot_count > 0
    assert authority.reserved_exit_lot_count == 0
    assert authority.sellable_executable_lot_count == authority.open_lot_count
    assert authority.exit_allowed is True
    assert authority.exit_block_reason == "none"




def test_submit_unknown_timeout_metadata_strong_correlation_resolves_on_restart(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownStrongCorrelationBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    gate_reason = evaluate_startup_safety_gate()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert row["exchange_order_id"] == "ex-submit-unknown-strong"
    assert autolink is not None
    assert "outcome=order_only" in str(autolink["message"])
    assert gate_reason is None


@pytest.mark.lot_native_regression_gate
def test_reconcile_recent_fill_replay_preserves_filled_terminal_state(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="filled_replay_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("filled_replay_restart", "ex-filled-replay", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="filled_replay_restart",
            side="BUY",
            fill_id="fill-existing-partial",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("filled_replay_restart", "FILLED", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_TerminalFillReplayBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='filled_replay_restart'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS c FROM fills WHERE client_order_id='filled_replay_restart'"
        ).fetchone()["c"]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(0.4)
    assert fill_count == 1
    authority = _load_reconciled_position_authority(db_path=isolated_db)
    assert authority.open_lot_count > 0
    assert authority.reserved_exit_lot_count == 0
    assert authority.sellable_executable_lot_count == authority.open_lot_count
    assert authority.exit_allowed is True
    assert authority.exit_block_reason == "none"


def test_reconcile_filled_buy_and_flattened_sell_remains_idempotent(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="flat_buy",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("flat_buy", "ex-flat-buy", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="flat_buy",
            side="BUY",
            fill_id="flat-buy-fill",
            fill_ts=110,
            price=100.0,
            qty=1.0,
            fee=0.0,
        )
        set_status("flat_buy", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="flat_sell",
            side="SELL",
            qty_req=1.0,
            price=110.0,
            ts_ms=200,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("flat_sell", "ex-flat-sell", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="flat_sell",
            side="SELL",
            fill_id="flat-sell-fill",
            fill_ts=210,
            price=110.0,
            qty=1.0,
            fee=0.0,
        )
        set_status("flat_sell", "FILLED", conn=conn)
        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        conn.commit()
    finally:
        conn.close()

    balance = BrokerBalance(
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_available,
        asset_locked=asset_locked,
    )
    broker = _FilledFlatReplayBroker(balance=balance)

    reconcile_with_broker(broker)
    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        rows = conn.execute(
            "SELECT client_order_id, status, qty_filled FROM orders WHERE client_order_id IN ('flat_buy', 'flat_sell') ORDER BY client_order_id"
        ).fetchall()
        buy_fill_count = conn.execute(
            "SELECT COUNT(*) AS c FROM fills WHERE client_order_id='flat_buy'"
        ).fetchone()["c"]
        sell_fill_count = conn.execute(
            "SELECT COUNT(*) AS c FROM fills WHERE client_order_id='flat_sell'"
        ).fetchone()["c"]
    finally:
        conn.close()

    assert [row["client_order_id"] for row in rows] == ["flat_buy", "flat_sell"]
    assert [row["status"] for row in rows] == ["FILLED", "FILLED"]
    assert [float(row["qty_filled"]) for row in rows] == pytest.approx([1.0, 1.0])
    assert buy_fill_count == 1
    assert sell_fill_count == 1
    assert evaluate_startup_safety_gate() is None


def test_reconcile_repeated_fill_sources_apply_only_once(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="dup_fill_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("dup_fill_restart", "ex-dup", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_AggregateDuplicateFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='dup_fill_restart'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS c FROM fills WHERE client_order_id='dup_fill_restart'"
        ).fetchone()["c"]
        trade_count = conn.execute(
            "SELECT COUNT(*) AS c FROM trades"
        ).fetchone()["c"]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fill_count == 1
    assert trade_count == 1


def test_reconcile_recent_sell_missing_price_blocks_ledger_and_sets_recovery_required(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_sell_missing_price",
            side="SELL",
            qty_req=1.0,
            price=110.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_sell_missing_price", "ex-reconcile-sell", conn=conn)
        record_order_if_missing(
            conn,
            client_order_id="seed_buy",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=80,
            status="FILLED",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="seed_buy",
            side="BUY",
            fill_id="seed-buy-fill",
            fill_ts=90,
            price=100.0,
            qty=1.0,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_RecentSellMissingPriceBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='reconcile_sell_missing_price'"
        ).fetchone()
        bad_trade_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE note LIKE 'reconcile recent%' AND price <= 0"
        ).fetchone()[0]
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_sell_missing_price'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert float(row["qty_filled"]) == pytest.approx(0.0)
    assert "missing/invalid execution price" in str(row["last_error"])
    assert bad_trade_count == 0
    assert fill_count == 0


def test_reconcile_recent_sell_valid_price_applies_and_flattens_position(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_sell_valid_price",
            side="SELL",
            qty_req=1.0,
            price=110.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_sell_valid_price", "ex-reconcile-sell", conn=conn)
        record_order_if_missing(
            conn,
            client_order_id="seed_buy_valid",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=80,
            status="FILLED",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="seed_buy_valid",
            side="BUY",
            fill_id="seed-buy-fill-valid",
            fill_ts=90,
            price=100.0,
            qty=1.0,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_RecentSellValidPriceBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='reconcile_sell_valid_price'"
        ).fetchone()
        trade_row = conn.execute(
            "SELECT price, asset_after, cash_after FROM trades WHERE note LIKE 'reconcile%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert trade_row is not None
    assert float(trade_row["price"]) == pytest.approx(110.0)
    assert float(trade_row["asset_after"]) == pytest.approx(0.0)
    assert float(trade_row["cash_after"]) > 0.0


def test_reconcile_repeated_recent_sell_missing_price_pattern_never_writes_zero_price_trade(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_sell_missing_price_repeat",
            side="SELL",
            qty_req=1.0,
            price=110.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_sell_missing_price_repeat", "ex-reconcile-sell", conn=conn)
        record_order_if_missing(
            conn,
            client_order_id="seed_buy_repeat",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=80,
            status="FILLED",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="seed_buy_repeat",
            side="BUY",
            fill_id="seed-buy-repeat",
            fill_ts=90,
            price=100.0,
            qty=1.0,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_RecentSellMissingPriceBroker(repeat_count=3))
    reconcile_with_broker(_RecentSellMissingPriceBroker(repeat_count=3))

    conn = ensure_db(str(isolated_db))
    try:
        zero_price_reconcile_trades = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE note LIKE 'reconcile recent%' AND price <= 0"
        ).fetchone()[0]
    finally:
        conn.close()

    assert zero_price_reconcile_trades == 0


def test_reconcile_recent_sell_zero_fee_blocks_ledger_and_sets_recovery_required(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_sell_zero_fee",
            side="SELL",
            qty_req=0.1,
            price=100000000.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_sell_zero_fee", "ex-zero-fee", conn=conn)
        record_order_if_missing(
            conn,
            client_order_id="seed_buy_zero_fee",
            side="BUY",
            qty_req=0.1,
            price=100.0,
            ts_ms=80,
            status="FILLED",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="seed_buy_zero_fee",
            side="BUY",
            fill_id="seed-buy-zero-fee",
            fill_ts=90,
            price=100.0,
            qty=0.1,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    object.__setattr__(settings, "MODE", "live")
    reconcile_with_broker(_RecentSellZeroFeeBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='reconcile_sell_zero_fee'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fee, fee_accounting_status FROM fills WHERE client_order_id='reconcile_sell_zero_fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_sell_zero_fee'"
        ).fetchone()[0]
        trade_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE note LIKE 'reconcile%' AND client_order_id='reconcile_sell_zero_fee'"
        ).fetchone()[0]
    finally:
        conn.close()

    state = runtime_state.snapshot()
    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(0.1)
    assert row["last_error"] is None
    assert fill_count == 1
    assert fill_row["fee"] == pytest.approx(0.0)
    assert fill_row["fee_accounting_status"] == "fee_validation_blocked"
    assert trade_count == 1
    assert state.last_reconcile_reason_code == "FILL_FEE_PENDING_RECOVERY_REQUIRED"
    assert "fee_validation_blocked=" in str(evaluate_startup_safety_gate())


def test_reconcile_failure_after_local_write_records_original_error_without_locked_db(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="write_fail_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("write_fail_restart", "ex-write-fail", conn=conn)
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="boom after ledger write"):
        reconcile_with_broker(_FailAfterWriteBroker())

    state = runtime_state.snapshot()
    assert state.last_reconcile_status == "error"
    assert state.last_reconcile_reason_code == "RECONCILE_FAILED"
    assert "boom after ledger write" in str(state.last_reconcile_error)


def test_submit_unknown_timeout_metadata_weak_correlation_stays_recovery_required(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownWeakMetadataCorrelationBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    gate_reason = evaluate_startup_safety_gate()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason


def test_submit_unknown_timeout_metadata_multiple_strong_candidates_stays_recovery_required(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownMultipleStrongCandidatesBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert autolink is not None
    assert "outcome=ambiguous" in str(autolink["message"])


def test_submit_unknown_timeout_metadata_no_candidate_records_insufficient_evidence(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_NoopBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert autolink is not None
    assert "outcome=insufficient_evidence" in str(autolink["message"])


def test_submit_unknown_timeout_metadata_incompatible_status_qty_side_rejected(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        _insert_submit_timeout_attempt_metadata(
            conn=conn,
            client_order_id="submit_timeout_restart",
            submit_attempt_id="attempt_timeout_meta",
            qty=1.0,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SubmitUnknownIncompatibleCorrelationBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        autolink = conn.execute(
            """
            SELECT message
            FROM order_events
            WHERE client_order_id='submit_timeout_restart' AND event_type='reconcile_submit_unknown_autolink'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert autolink is not None
    assert "outcome=insufficient_evidence" in str(autolink["message"])


def test_reconcile_uses_client_order_id_lookup_when_exchange_order_id_is_missing(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="client_only_lookup",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        conn.commit()
    finally:
        conn.close()

    broker = _ClientOnlyLookupBroker()
    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='client_only_lookup'"
        ).fetchone()
    finally:
        conn.close()

    assert broker.calls == [("client_only_lookup", None)]
    assert row is not None
    assert row["status"] == "CANCELED"
    assert row["exchange_order_id"] == "ex-client-only"


def test_reconcile_identifier_mismatch_marks_recovery_required_with_reason(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="lookup_identifier_mismatch",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_IdentifierMismatchLookupBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, last_error FROM orders WHERE client_order_id='lookup_identifier_mismatch'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "identifier_mismatch" in str(row["last_error"])


def test_restarted_ambiguous_order_blocks_new_submit_until_resolved(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_NoopBroker())

    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    run_loop(5, 20)

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert reason is not None
    assert "recovery_required_orders=1" in reason
    assert state.trading_enabled is False
    assert live_execute_calls["n"] == 0

def test_submit_success_then_crash_restart_blocks_new_submit_attempt(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_success_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("submit_success_crash", "ex-submit-success", conn=conn)
        conn.commit()
    finally:
        conn.close()

    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    run_loop(5, 20)

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()
    assert reason is not None
    assert "unresolved_open_orders=1" in reason
    assert state.trading_enabled is False
    assert live_execute_calls["n"] == 0



@pytest.mark.lot_native_regression_gate
def test_restart_during_cancel_request_reconciles_to_canceled(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="cancel_race",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("cancel_race", "ex-cancel-race", conn=conn)
        conn.commit()
    finally:
        conn.close()

    broker = _CancelRaceBroker()
    broker.cancel_order(client_order_id="cancel_race", exchange_order_id="ex-cancel-race")

    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        status = conn.execute("SELECT status FROM orders WHERE client_order_id='cancel_race'").fetchone()[0]
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    authority = _load_reconciled_position_authority(db_path=isolated_db)
    assert status == "CANCELED"
    assert reason is None
    assert authority.open_lot_count == 0
    assert authority.reserved_exit_lot_count == 0
    assert authority.sellable_executable_lot_count == 0
    assert authority.exit_allowed is False
    assert authority.exit_block_reason == "no_position"



@pytest.mark.lot_native_regression_gate
def test_restart_mid_reconcile_rolls_back_then_retries_cleanly(isolated_db, monkeypatch):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PARTIAL",
        )
        set_exchange_order_id("reconcile_restart", "ex-partial", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="reconcile_restart",
            side="BUY",
            fill_id="fill-existing",
            fill_ts=110,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    original_set_portfolio_breakdown = recovery_module.set_portfolio_breakdown

    def _crash_once(*args, **kwargs):
        raise RuntimeError("crash during reconcile")

    monkeypatch.setattr("bithumb_bot.recovery.set_portfolio_breakdown", _crash_once)
    monkeypatch.setattr("bithumb_bot.recovery.runtime_state.record_reconcile_result", lambda **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.recovery.runtime_state.refresh_open_order_health", lambda **_kwargs: None)
    with pytest.raises(RuntimeError, match="crash during reconcile"):
        reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        fills_after_crash = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_restart'").fetchone()[0]
    finally:
        conn.close()

    assert fills_after_crash == 1
    authority_after_crash = _load_reconciled_position_authority(db_path=isolated_db)
    assert authority_after_crash.open_lot_count > 0
    assert authority_after_crash.reserved_exit_lot_count == 0
    assert authority_after_crash.sellable_executable_lot_count == authority_after_crash.open_lot_count
    assert authority_after_crash.exit_allowed is True
    assert authority_after_crash.exit_block_reason == "none"

    monkeypatch.setattr("bithumb_bot.recovery.set_portfolio_breakdown", original_set_portfolio_breakdown)
    reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='reconcile_restart'"
        ).fetchone()
        fills_after_retry = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_restart'").fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fills_after_retry == 2
    assert evaluate_startup_safety_gate() is None
    authority_after_retry = _load_reconciled_position_authority(db_path=isolated_db)
    assert authority_after_retry.open_lot_count > 0
    assert authority_after_retry.reserved_exit_lot_count == 0
    assert authority_after_retry.sellable_executable_lot_count == authority_after_retry.open_lot_count
    assert authority_after_retry.exit_allowed is True
    assert authority_after_retry.exit_block_reason == "none"


def test_reconcile_success_auto_clears_stale_initial_reconcile_halt(isolated_db):
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)
    try:
        runtime_state.disable_trading_until(
            float("inf"),
            reason=(
                "initial reconcile failed (BrokerRejectError): "
                "bithumb private /info/orders rejected with http status=400"
            ),
            reason_code="INITIAL_RECONCILE_FAILED",
            halt_new_orders_blocked=True,
            unresolved=True,
        )

        reconcile_with_broker(_NoopBroker())

        state = runtime_state.snapshot()
        assert state.trading_enabled is False
        assert state.halt_new_orders_blocked is False
        assert state.halt_state_unresolved is False
        assert state.halt_reason_code is None
        assert state.last_disable_reason is None
        assert state.last_reconcile_status == "ok"
        assert state.last_reconcile_reason_code == "RECONCILE_OK"
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original_cash)


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_reconcile_does_not_clear_halt_from_qty_only_holdings_without_lot_native_exposure(isolated_db):
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)
    try:
        conn = ensure_db(str(isolated_db))
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO portfolio(
                    id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
                ) VALUES (1, 1000.0, 1.0, 1000.0, 0.0, 1.0, 0.0)
                """
            )
            conn.commit()
        finally:
            conn.close()

        runtime_state.disable_trading_until(
            float("inf"),
            reason="post-trade reconcile halt",
            reason_code="POST_TRADE_RECONCILE_REQUIRED",
            halt_new_orders_blocked=True,
            unresolved=True,
        )

        reconcile_with_broker(_DustBalanceBroker(asset_available=1.0))

        state = runtime_state.snapshot()
        conn = ensure_db(str(isolated_db))
        try:
            lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        finally:
            conn.close()
        assert state.halt_new_orders_blocked is True
        assert state.halt_state_unresolved is True
        assert state.halt_reason_code == "POST_TRADE_RECONCILE_REQUIRED"
        assert lot_snapshot.open_lot_count == 0
        assert lot_snapshot.executable_open_exposure_qty == pytest.approx(0.0)
        assert lot_snapshot.exit_non_executable_reason == "no_executable_open_lots"
        authority = _load_reconciled_position_authority(db_path=isolated_db)
        assert authority.open_lot_count == 0
        assert authority.reserved_exit_lot_count == 0
        assert authority.sellable_executable_lot_count == 0
        assert authority.has_dust_only_remainder is False
        assert authority.exit_allowed is False
        assert authority.exit_block_reason == "legacy_lot_metadata_missing"
        assert authority.terminal_state == "non_executable_position"
        assert authority.authority_gap_reason == "authority_missing_recovery_required"
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original_cash)


def test_reconcile_recent_fill_success_clears_prior_locked_post_trade_halt(isolated_db):
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)
    try:
        conn = ensure_db(str(isolated_db))
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO portfolio(
                    id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
                ) VALUES (1, 1000.0, 1.0, 900.0, 0.0, 1.0, 0.0)
                """
            )
            record_order_if_missing(
                conn,
                client_order_id="locked_halt_replay",
                side="SELL",
                qty_req=1.0,
                price=100.0,
                ts_ms=100,
                status="PARTIAL",
            )
            set_exchange_order_id("locked_halt_replay", "ex-partial", conn=conn)
            apply_fill_and_trade(
                conn,
                client_order_id="locked_halt_replay",
                side="SELL",
                fill_id="fill-existing",
                fill_ts=110,
                price=100.0,
                qty=0.4,
                fee=0.0,
            )
            conn.commit()
        finally:
            conn.close()

        runtime_state.disable_trading_until(
            float("inf"),
            reason="reconcile failed (OperationalError): database is locked",
            reason_code="POST_TRADE_RECONCILE_FAILED",
            halt_new_orders_blocked=True,
            unresolved=True,
        )

        class _SellRecentFillBroker(_NoopBroker):
            def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
                return BrokerOrder(client_order_id, exchange_order_id or "ex-partial", "SELL", "FILLED", 100.0, 1.0, 1.0, 1, 1)

            def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
                return [
                    BrokerFill(
                        client_order_id="",
                        fill_id="fill-rest",
                        fill_ts=220,
                        price=100.0,
                        qty=0.6,
                        fee=0.0,
                        exchange_order_id="ex-partial",
                    )
                ]

        reconcile_with_broker(_SellRecentFillBroker())

        state = runtime_state.snapshot()
        assert state.halt_new_orders_blocked is False
        assert state.halt_state_unresolved is False
        assert state.halt_reason_code is None
        assert state.last_reconcile_status == "ok"
        assert state.last_reconcile_reason_code == "RECENT_FILL_APPLIED"

        conn = ensure_db(str(isolated_db))
        try:
            row = conn.execute(
                "SELECT status, qty_filled FROM orders WHERE client_order_id='locked_halt_replay'"
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row["status"] == "FILLED"
        assert float(row["qty_filled"]) == pytest.approx(1.0)
        assert evaluate_startup_safety_gate() is None
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original_cash)


def test_restart_reconcile_api_exception_halts_and_prevents_resume(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_api_exception",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_api_exception", "ex-api-down", conn=conn)
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: _ApiErrorBroker())
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "INITIAL_RECONCILE_FAILED"
    assert live_execute_calls["n"] == 0


def _patch_single_tick_run_loop(monkeypatch) -> None:
    monkeypatch.setattr("bithumb_bot.config.notifier_is_configured", lambda: True)
    monkeypatch.setattr("bithumb_bot.config.validate_market_preflight", lambda _cfg: None)
    _set_live_runtime_paths(monkeypatch, base_dir=Path(settings.DB_PATH).resolve().parent)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC", 30)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 900)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")

    object.__setattr__(settings, "MAX_ORDER_KRW", 100000)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)

    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 1)
    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda conn, s, l: {
            "ts": 1000,
            "last_close": 100.0,
            "curr_s": 1.0,
            "curr_l": 0.5,
            "signal": "BUY",
        },
    )
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: object())
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )

    ticks = iter([10.0, 11.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(ticks, 11.0))

    sleeps = {"n": 0}

    def _sleep(_sec: float):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)


def test_restart_with_risky_state_does_not_resume_trading_loop(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="restart_block",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="RECOVERY_REQUIRED",
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.startup_gate_reason is not None
    assert "recovery_required_orders=1" in state.startup_gate_reason
    assert live_execute_calls["n"] == 0


def test_restart_while_persisted_halted_does_not_resume_trading_loop(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    runtime_state.enter_halt(
        reason_code="MANUAL_HALT",
        reason="operator requested stop",
        unresolved=True,
    )

    reconcile_calls = {"n": 0}
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.recovery.reconcile_with_broker",
        lambda _broker: reconcile_calls.__setitem__("n", reconcile_calls["n"] + 1),
        raising=False,
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "MANUAL_HALT"
    assert reconcile_calls["n"] == 0
    assert live_execute_calls["n"] == 0


def test_restart_startup_proceeds_when_reconcile_clears_risky_state(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="restart_clear",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("restart_clear", "ex-restart-clear", conn=conn)
        conn.commit()
    finally:
        conn.close()

    class _ResolveToCanceledBroker(_NoopBroker):
        def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
            return BrokerOrder(client_order_id, exchange_order_id or "ex-restart-clear", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)

    monkeypatch.setattr(
        "bithumb_bot.recovery.reconcile_with_broker",
        lambda _broker: reconcile_with_broker(_ResolveToCanceledBroker()),
        raising=False,
    )
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.startup_gate_reason is None
    assert state.trading_enabled is True
