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


@dataclass(frozen=True)
class ExecutionDecisionSummary:
    raw_signal: str
    final_signal: str
    final_action: str
    submit_expected: bool
    pre_submit_proof_status: str
    block_reason: str
    strategy_sell_candidate: dict[str, object] | None
    residual_sell_candidate: dict[str, object] | None
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    tracked_residual_exposure_krw: float | None
    buy_delta_krw: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "raw_signal": self.raw_signal,
            "final_signal": self.final_signal,
            "final_action": self.final_action,
            "submit_expected": bool(self.submit_expected),
            "pre_submit_proof_status": self.pre_submit_proof_status,
            "block_reason": self.block_reason,
            "strategy_sell_candidate": (
                None if self.strategy_sell_candidate is None else dict(self.strategy_sell_candidate)
            ),
            "residual_sell_candidate": (
                None if self.residual_sell_candidate is None else dict(self.residual_sell_candidate)
            ),
            "target_exposure_krw": self.target_exposure_krw,
            "current_effective_exposure_krw": self.current_effective_exposure_krw,
            "tracked_residual_exposure_krw": self.tracked_residual_exposure_krw,
            "buy_delta_krw": self.buy_delta_krw,
        }


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
    if int(decision_context.get("unresolved_open_order_count") or 0) > 0:
        reasons.append("unresolved_open_order_count_nonzero")
    if int(decision_context.get("recovery_required_count") or 0) > 0:
        reasons.append("recovery_required_count_nonzero")
    if int(decision_context.get("submit_unknown_count") or 0) > 0:
        reasons.append("submit_unknown_count_nonzero")
    broker_evidence = _dict_value(decision_context.get("broker_position_evidence"))
    if not bool(broker_evidence.get("broker_qty_known")):
        reasons.append("broker_qty_unknown")
    if bool(broker_evidence.get("balance_source_stale")):
        reasons.append("broker_evidence_stale")
    if candidate is not None and float(broker_evidence.get("broker_qty") or 0.0) + 1e-12 < float(candidate.qty):
        reasons.append("broker_qty_below_candidate_qty")
    return ResidualSellPreSubmitProof(passed=not reasons, reasons=tuple(reasons))


def _first_block_reason(*values: object, default: str = "none") -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "none":
            return text
    return default


def _strategy_sell_candidate(decision_context: dict[str, object]) -> dict[str, object] | None:
    exposure = resolve_canonical_position_exposure_snapshot(decision_context)
    if int(exposure.sellable_executable_lot_count) <= 0 or not bool(exposure.exit_allowed):
        return None
    return {
        "source": "lot_native_strategy_position",
        "authority": "position_state.normalized_exposure.sellable_executable_lot_count",
        "sellable_executable_lot_count": int(exposure.sellable_executable_lot_count),
        "sellable_executable_qty": float(exposure.sellable_executable_qty),
    }


def _residual_block_reason(
    *,
    decision_context: dict[str, object],
    proof: ResidualSellPreSubmitProof | None,
) -> str:
    if proof is not None and proof.reasons:
        return str(proof.reasons[0])
    residual_inventory = _dict_value(decision_context.get("residual_inventory"))
    classes = {str(item) for item in (residual_inventory.get("residual_classes") or [])}
    if not bool(residual_inventory.get("exchange_sellable")):
        if "TRUE_DUST" in classes:
            return "below_min_qty_or_min_notional"
        return "residual_not_exchange_sellable"
    return _first_block_reason(
        decision_context.get("exit_block_reason"),
        decision_context.get("block_reason"),
        decision_context.get("reason"),
        default="residual_policy_blocked",
    )


def build_execution_decision_summary(
    *,
    decision_context: dict[str, object] | None,
    readiness_payload: dict[str, object] | None = None,
    raw_signal: str | None = None,
    final_signal: str | None = None,
    final_reason: str | None = None,
) -> ExecutionDecisionSummary:
    payload: dict[str, object] = dict(decision_context or {})
    if isinstance(readiness_payload, dict):
        for key in (
            "residual_inventory_mode",
            "residual_inventory_state",
            "residual_inventory_policy_allows_run",
            "residual_inventory_policy_allows_buy",
            "residual_inventory_policy_allows_sell",
            "residual_inventory",
            "residual_sell_candidate",
            "projection_converged",
            "projection_convergence",
            "open_order_count",
            "unresolved_open_order_count",
            "recovery_required_count",
            "submit_unknown_count",
            "broker_position_evidence",
            "total_effective_exposure_qty",
            "total_effective_exposure_notional_krw",
            "residual_inventory_notional_krw",
        ):
            if key in readiness_payload:
                payload[key] = readiness_payload[key]
        payload["accounting_projection_ok"] = bool(readiness_payload.get("projection_converged"))

    raw = str(raw_signal or payload.get("raw_signal") or payload.get("base_signal") or payload.get("signal") or "HOLD").upper()
    final = str(final_signal or payload.get("final_signal") or payload.get("signal") or "HOLD").upper()
    strategy_candidate = _strategy_sell_candidate(payload)
    residual_candidate = build_residual_sell_candidate(payload)
    residual_candidate_dict = None if residual_candidate is None else {
        "qty": float(residual_candidate.qty),
        "notional": residual_candidate.notional,
        "source": residual_candidate.source,
        "classes": list(residual_candidate.classes),
        "exchange_sellable": bool(residual_candidate.exchange_sellable),
        "allowed_by_policy": bool(residual_candidate.allowed_by_policy),
        "requires_final_pre_submit_proof": bool(residual_candidate.requires_final_pre_submit_proof),
    }

    target_exposure_krw = None
    current_effective_exposure_krw = None
    tracked_residual_exposure_krw = None
    buy_delta_krw = None
    if raw == "BUY":
        target_exposure_krw = max(0.0, float(getattr(settings, "MAX_ORDER_KRW", 0.0) or 0.0))
        current_effective_exposure_krw = (
            None
            if payload.get("total_effective_exposure_notional_krw") is None
            else max(0.0, float(payload.get("total_effective_exposure_notional_krw") or 0.0))
        )
        tracked_residual_exposure_krw = (
            None
            if payload.get("residual_inventory_notional_krw") is None
            else max(0.0, float(payload.get("residual_inventory_notional_krw") or 0.0))
        )
        if current_effective_exposure_krw is not None:
            buy_delta_krw = max(0.0, float(target_exposure_krw) - float(current_effective_exposure_krw))

    proof: ResidualSellPreSubmitProof | None = None
    if raw == "SELL" and residual_candidate is not None:
        proof = build_residual_sell_presubmit_proof(payload)

    if raw == "BUY":
        if not bool(payload.get("residual_inventory_policy_allows_run", True)):
            action = "BLOCK_RECOVERY"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(payload.get("residual_inventory_state"), final_reason, default="recovery_blocked")
        elif final == "BUY":
            action = "ENTER_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none"
        elif buy_delta_krw is not None and buy_delta_krw <= 0.0:
            action = "HOLD_TARGET_ALREADY_COVERED"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "tracked_residual_exposure_covers_target"
        else:
            action = "BLOCK_ORDER_RULE" if final == "HOLD" else "STRATEGY_HOLD"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("entry_block_reason"), payload.get("block_reason"))
    elif raw == "SELL":
        if strategy_candidate is not None and final == "SELL":
            action = "EXIT_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none"
        elif residual_candidate is not None:
            action = "CLOSE_RESIDUAL_CANDIDATE" if proof is not None and proof.passed else "BLOCK_UNRESOLVED_RESIDUAL"
            # Telemetry-only in this patch: residual closeout has a candidate
            # and proof, but live submission remains disabled until explicitly
            # enabled by a separate policy change.
            submit_expected = False
            proof_status = "passed_telemetry_only" if proof is not None and proof.passed else "failed"
            block_reason = "residual_live_submit_disabled" if proof is not None and proof.passed else _residual_block_reason(decision_context=payload, proof=proof)
        elif str(payload.get("residual_inventory_state") or "") == "RESIDUAL_INVENTORY_UNRESOLVED":
            action = "BLOCK_UNRESOLVED_RESIDUAL"
            submit_expected = False
            proof_status = "failed"
            block_reason = _first_block_reason(payload.get("residual_inventory_state"), payload.get("exit_block_reason"))
        elif bool(payload.get("has_dust_only_remainder")):
            action = "HOLD_TRACKED_DUST"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _residual_block_reason(decision_context=payload, proof=None)
        elif final == "HOLD":
            action = "STRATEGY_HOLD"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("exit_block_reason"), payload.get("block_reason"))
        else:
            action = "BLOCK_ORDER_RULE"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("exit_block_reason"), payload.get("block_reason"))
    else:
        action = "STRATEGY_HOLD"
        submit_expected = False
        proof_status = "not_required"
        block_reason = _first_block_reason(final_reason, payload.get("block_reason"))

    return ExecutionDecisionSummary(
        raw_signal=raw,
        final_signal=final,
        final_action=action,
        submit_expected=submit_expected,
        pre_submit_proof_status=proof_status,
        block_reason=block_reason,
        strategy_sell_candidate=strategy_candidate,
        residual_sell_candidate=residual_candidate_dict,
        target_exposure_krw=target_exposure_krw,
        current_effective_exposure_krw=current_effective_exposure_krw,
        tracked_residual_exposure_krw=tracked_residual_exposure_krw,
        buy_delta_krw=buy_delta_krw,
    )


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
