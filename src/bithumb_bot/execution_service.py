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


@dataclass(frozen=True)
class ResidualSellCandidate:
    qty: float
    notional: float | None
    source: str
    classes: tuple[str, ...]
    exchange_sellable: bool
    allowed_by_policy: bool
    requires_final_pre_submit_proof: bool


@dataclass(frozen=True)
class ResidualSellPreSubmitProof:
    passed: bool
    reasons: tuple[str, ...]


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


def _dict_value(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def build_residual_sell_candidate(decision_context: dict[str, object] | None) -> ResidualSellCandidate | None:
    if not isinstance(decision_context, dict):
        return None
    residual_mode = str(decision_context.get("residual_inventory_mode") or "block")
    residual_state = str(decision_context.get("residual_inventory_state") or "")
    residual_inventory = _dict_value(decision_context.get("residual_inventory"))
    residual_candidate = _dict_value(decision_context.get("residual_sell_candidate"))
    if residual_mode != "track" or residual_state != "RESIDUAL_INVENTORY_TRACKED":
        return None
    if residual_candidate:
        return ResidualSellCandidate(
            qty=float(residual_candidate.get("qty") or 0.0),
            notional=(
                None if residual_candidate.get("notional") is None else float(residual_candidate.get("notional") or 0.0)
            ),
            source=str(residual_candidate.get("source") or "residual_inventory"),
            classes=tuple(str(item) for item in (residual_candidate.get("classes") or [])),
            exchange_sellable=bool(residual_candidate.get("exchange_sellable")),
            allowed_by_policy=bool(residual_candidate.get("allowed_by_policy")),
            requires_final_pre_submit_proof=bool(residual_candidate.get("requires_final_pre_submit_proof")),
        )
    if not bool(residual_inventory.get("exchange_sellable")):
        return None
    qty = float(residual_inventory.get("residual_qty") or 0.0)
    if qty <= 1e-12:
        return None
    return ResidualSellCandidate(
        qty=qty,
        notional=(
            None
            if residual_inventory.get("residual_notional_krw") is None
            else float(residual_inventory.get("residual_notional_krw") or 0.0)
        ),
        source="residual_inventory",
        classes=tuple(str(item) for item in (residual_inventory.get("residual_classes") or [])),
        exchange_sellable=True,
        allowed_by_policy=True,
        requires_final_pre_submit_proof=True,
    )


def build_residual_sell_presubmit_proof(decision_context: dict[str, object] | None) -> ResidualSellPreSubmitProof:
    reasons: list[str] = []
    if not isinstance(decision_context, dict):
        return ResidualSellPreSubmitProof(passed=False, reasons=("missing_decision_context",))
    candidate = build_residual_sell_candidate(decision_context)
    if candidate is None:
        reasons.append("missing_residual_sell_candidate")
    if not bool(decision_context.get("residual_inventory_policy_allows_sell")):
        reasons.append("residual_sell_policy_blocked")
    if not bool(decision_context.get("projection_converged")):
        reasons.append("projection_not_converged")
    if not bool(decision_context.get("accounting_projection_ok")):
        reasons.append("accounting_projection_not_ok")
    if int(decision_context.get("open_order_count") or 0) > 0:
        reasons.append("open_order_count_nonzero")
    if int(decision_context.get("recovery_required_count") or 0) > 0:
        reasons.append("recovery_required_count_nonzero")
    broker_evidence = _dict_value(decision_context.get("broker_position_evidence"))
    if not bool(broker_evidence.get("broker_qty_known")):
        reasons.append("broker_qty_unknown")
    if bool(broker_evidence.get("balance_source_stale")):
        reasons.append("broker_evidence_stale")
    if candidate is not None and float(broker_evidence.get("broker_qty") or 0.0) + 1e-12 < float(candidate.qty):
        reasons.append("broker_qty_below_candidate_qty")
    return ResidualSellPreSubmitProof(passed=not reasons, reasons=tuple(reasons))


def _canonical_harmless_dust_sell_preview(decision_context: dict[str, object] | None) -> dict[str, float | str] | None:
    if not isinstance(decision_context, dict):
        return None
    if build_residual_sell_candidate(decision_context) is not None:
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
