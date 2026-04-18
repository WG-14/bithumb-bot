from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from . import runtime_state
from .config import settings
from .db_core import ensure_db
from .decision_context import resolve_canonical_position_exposure_snapshot

if False:  # pragma: no cover
    from .broker.base import Broker


@dataclass(frozen=True)
class SignalExecutionRequest:
    signal: str
    ts: int
    market_price: float
    strategy_name: str | None = None
    decision_id: int | None = None
    decision_reason: str | None = None
    exit_rule_name: str | None = None
    decision_context: dict[str, object] | None = None


class SignalExecutionService(Protocol):
    def execute(self, request: SignalExecutionRequest) -> dict | None: ...


def paper_execute(
    signal: str,
    ts: int,
    market_price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
) -> dict | None:
    from .broker.paper import paper_execute as _paper_execute

    return _paper_execute(
        signal,
        ts,
        market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )


def live_execute_signal(
    broker: "Broker",
    signal: str,
    ts: int,
    market_price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
) -> dict | None:
    from .broker.live import live_execute_signal as _live_execute_signal

    return _live_execute_signal(
        broker,
        signal,
        ts,
        market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )


def record_harmless_dust_exit_suppression(**kwargs) -> bool:
    from .broker.live import record_harmless_dust_exit_suppression as _record_harmless_dust_exit_suppression

    return _record_harmless_dust_exit_suppression(**kwargs)


def _canonical_harmless_dust_sell_preview(decision_context: dict[str, object] | None) -> dict[str, float | str] | None:
    if not isinstance(decision_context, dict):
        return None

    canonical_exposure = resolve_canonical_position_exposure_snapshot(decision_context)
    if bool(canonical_exposure.exit_allowed):
        return None
    if int(canonical_exposure.sellable_executable_lot_count) > 0:
        return None

    exit_block_reason = str(canonical_exposure.exit_block_reason or "").strip()
    if exit_block_reason not in {"dust_only_remainder", "no_executable_exit_lot"}:
        return None

    requested_qty = max(0.0, float(canonical_exposure.raw_total_asset_qty))
    if requested_qty <= 1e-12:
        return None

    return {
        "requested_qty": requested_qty,
        "normalized_qty": max(0.0, float(canonical_exposure.sellable_executable_qty)),
        "raw_total_asset_qty": requested_qty,
        "open_exposure_qty": max(0.0, float(canonical_exposure.open_exposure_qty)),
        "dust_tracking_qty": max(0.0, float(canonical_exposure.dust_tracking_qty)),
        "submit_qty_source": "position_state.normalized_exposure.sellable_executable_lot_count",
    }


@dataclass(frozen=True)
class PaperSignalExecutionService:
    executor: Callable[..., dict | None]

    def execute(self, request: SignalExecutionRequest) -> dict | None:
        try:
            return self.executor(
                request.signal,
                request.ts,
                request.market_price,
                strategy_name=request.strategy_name,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            return self.executor(request.signal, request.ts, request.market_price)


@dataclass(frozen=True)
class LiveSignalExecutionService:
    broker: "Broker"
    executor: Callable[..., dict | None]
    harmless_dust_recorder: Callable[..., bool]

    def execute(self, request: SignalExecutionRequest) -> dict | None:
        harmless_dust_preview = None
        if request.signal == "SELL":
            harmless_dust_preview = _canonical_harmless_dust_sell_preview(request.decision_context)
        if harmless_dust_preview is not None:
            suppression_conn = ensure_db()
            try:
                if self.harmless_dust_recorder(
                    conn=suppression_conn,
                    state=runtime_state.snapshot(),
                    signal=request.signal,
                    side="SELL",
                    requested_qty=float(harmless_dust_preview["requested_qty"]),
                    market_price=float(request.market_price),
                    normalized_qty=float(harmless_dust_preview["normalized_qty"]),
                    strategy_name=request.strategy_name or settings.STRATEGY_NAME,
                    decision_id=request.decision_id,
                    decision_reason=request.decision_reason,
                    exit_rule_name=request.exit_rule_name,
                    submit_qty_source=str(harmless_dust_preview["submit_qty_source"]),
                    position_state_source=str(harmless_dust_preview["submit_qty_source"]),
                    raw_total_asset_qty=float(harmless_dust_preview["raw_total_asset_qty"]),
                    open_exposure_qty=float(harmless_dust_preview["open_exposure_qty"]),
                    dust_tracking_qty=float(harmless_dust_preview["dust_tracking_qty"]),
                ):
                    suppression_conn.commit()
                    return None
            finally:
                suppression_conn.close()
        try:
            return self.executor(
                self.broker,
                request.signal,
                request.ts,
                request.market_price,
                strategy_name=request.strategy_name,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            return self.executor(self.broker, request.signal, request.ts, request.market_price)


def build_signal_execution_service(
    *,
    mode: str,
    broker: "Broker | None" = None,
    paper_executor: Callable[..., dict | None] = paper_execute,
    live_executor: Callable[..., dict | None] = live_execute_signal,
    harmless_dust_recorder: Callable[..., bool] = record_harmless_dust_exit_suppression,
) -> SignalExecutionService | None:
    if mode == "paper":
        return PaperSignalExecutionService(executor=paper_executor)
    if mode == "live" and broker is not None:
        return LiveSignalExecutionService(
            broker=broker,
            executor=live_executor,
            harmless_dust_recorder=harmless_dust_recorder,
        )
    return None
