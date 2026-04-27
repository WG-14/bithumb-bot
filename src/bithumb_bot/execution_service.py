from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from . import runtime_state
from .config import settings
from .db_core import ensure_db
from .decision_context import resolve_canonical_position_exposure_snapshot
from .oms import build_order_intent_key

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
class ExecutionSubmitPlan:
    side: str
    source: str
    authority: str
    final_action: str
    qty: float | None
    notional_krw: float | None
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    delta_krw: float | None
    submit_expected: bool
    pre_submit_proof_status: str
    block_reason: str
    idempotency_key: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "side": self.side,
            "source": self.source,
            "authority": self.authority,
            "final_action": self.final_action,
            "qty": self.qty,
            "notional_krw": self.notional_krw,
            "target_exposure_krw": self.target_exposure_krw,
            "current_effective_exposure_krw": self.current_effective_exposure_krw,
            "delta_krw": self.delta_krw,
            "submit_expected": bool(self.submit_expected),
            "pre_submit_proof_status": self.pre_submit_proof_status,
            "block_reason": self.block_reason,
            "idempotency_key": self.idempotency_key,
        }


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
    residual_live_sell_mode: str
    residual_buy_sizing_mode: str
    residual_submit_plan: dict[str, object] | None
    buy_submit_plan: dict[str, object] | None

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
            "residual_live_sell_mode": self.residual_live_sell_mode,
            "residual_buy_sizing_mode": self.residual_buy_sizing_mode,
            "residual_submit_plan": (
                None if self.residual_submit_plan is None else dict(self.residual_submit_plan)
            ),
            "buy_submit_plan": None if self.buy_submit_plan is None else dict(self.buy_submit_plan),
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
    execution_submit_plan: dict[str, object] | None = None,
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
        execution_submit_plan=execution_submit_plan,
    )


def _residual_live_sell_mode() -> str:
    mode = str(getattr(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry") or "telemetry").strip().lower()
    return mode if mode in {"telemetry", "dry_run", "enabled"} else "telemetry"


def _residual_buy_sizing_mode() -> str:
    mode = str(getattr(settings, "RESIDUAL_BUY_SIZING_MODE", "telemetry") or "telemetry").strip().lower()
    return mode if mode in {"off", "telemetry", "delta"} else "telemetry"


def _residual_intent_ts(payload: dict[str, object]) -> int:
    for key in ("ts", "candle_ts", "signal_ts", "decision_ts"):
        try:
            value = payload.get(key)
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return 0


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
    else:
        if not bool(candidate.allowed_by_policy):
            reasons.append("candidate_policy_blocked")
        if not bool(candidate.requires_final_pre_submit_proof):
            reasons.append("candidate_final_pre_submit_proof_not_required")
    if not bool(decision_context.get("residual_inventory_policy_allows_sell")):
        reasons.append("residual_sell_policy_blocked")
    if not bool(decision_context.get("projection_converged")):
        reasons.append("projection_not_converged")
    projection = _dict_value(decision_context.get("projection_convergence"))
    if projection and not bool(projection.get("converged")):
        reasons.append("projection_not_converged")
    if not bool(decision_context.get("accounting_projection_ok")):
        reasons.append(
            "missing_accounting_projection_ok"
            if "accounting_projection_ok" not in decision_context
            else "accounting_projection_not_ok"
        )
    if int(decision_context.get("open_order_count") or 0) > 0:
        reasons.append("open_order_count_nonzero")
    if int(decision_context.get("unresolved_open_order_count") or 0) > 0:
        reasons.append("unresolved_open_order_count_nonzero")
    if int(decision_context.get("recovery_required_count") or 0) > 0:
        reasons.append("recovery_required_count_nonzero")
    if int(decision_context.get("submit_unknown_count") or 0) > 0:
        reasons.append("submit_unknown_count_nonzero")
    broker_evidence = _dict_value(decision_context.get("broker_position_evidence"))
    locked_qty = (
        decision_context.get("locked_qty")
        if "locked_qty" in decision_context
        else decision_context.get("residual_proof_locked_qty", broker_evidence.get("asset_locked"))
    )
    if locked_qty is None:
        reasons.append("missing_locked_qty")
    elif float(locked_qty or 0.0) > 1e-12:
        reasons.append("locked_qty_nonzero")
    if bool(decision_context.get("active_fee_accounting_blocker")):
        reasons.append("active_fee_accounting_blocker")
    if not bool(broker_evidence.get("broker_qty_known")):
        reasons.append("broker_qty_unknown")
    if bool(broker_evidence.get("balance_source_stale")):
        reasons.append("broker_evidence_stale")
    if candidate is not None and float(broker_evidence.get("broker_qty") or 0.0) + 1e-12 < float(candidate.qty):
        reasons.append("broker_qty_below_candidate_qty")
    min_qty = decision_context.get("min_qty", decision_context.get("residual_proof_min_qty"))
    min_notional = decision_context.get(
        "min_notional_krw", decision_context.get("residual_proof_min_notional_krw")
    )
    if min_qty is None:
        reasons.append("missing_min_qty")
    elif candidate is not None and float(candidate.qty) + 1e-12 < float(min_qty):
        reasons.append("qty_below_min_qty")
    if min_notional is None:
        reasons.append("missing_min_notional")
    elif candidate is not None and (
        candidate.notional is None or float(candidate.notional) + 1e-9 < float(min_notional)
    ):
        reasons.append("notional_below_min_notional")
    if not str(decision_context.get("idempotency_scope") or "").strip():
        reasons.append("missing_idempotency_scope")
    return ResidualSellPreSubmitProof(passed=not reasons, reasons=tuple(dict.fromkeys(reasons)))


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
            "residual_proof_min_qty",
            "residual_proof_min_notional_krw",
            "residual_proof_locked_qty",
            "active_fee_accounting_blocker",
            "accounting_projection_ok",
            "idempotency_scope",
        ):
            if key in readiness_payload:
                payload[key] = readiness_payload[key]

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

    residual_live_sell_mode = _residual_live_sell_mode()
    residual_buy_sizing_mode = _residual_buy_sizing_mode()
    residual_submit_plan: dict[str, object] | None = None
    buy_submit_plan: dict[str, object] | None = None

    if raw == "BUY":
        if not bool(payload.get("residual_inventory_policy_allows_run", True)):
            action = "BLOCK_RECOVERY"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(payload.get("residual_inventory_state"), final_reason, default="recovery_blocked")
        elif (
            residual_buy_sizing_mode == "delta"
            and buy_delta_krw is not None
            and buy_delta_krw <= 0.0
        ):
            action = "HOLD_TARGET_ALREADY_COVERED"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "tracked_residual_exposure_covers_target"
        elif (
            residual_buy_sizing_mode == "delta"
            and buy_delta_krw is not None
            and 0.0 < buy_delta_krw < float(payload.get("min_notional_krw", payload.get("residual_proof_min_notional_krw", 0.0)) or 0.0)
        ):
            action = "BLOCK_ORDER_RULE"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "buy_delta_below_min_notional"
        elif final == "BUY":
            action = "ENTER_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none" if residual_buy_sizing_mode != "telemetry" else "residual_buy_sizing_mode_telemetry"
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
        if buy_delta_krw is not None:
            delta_for_plan = (
                buy_delta_krw
                if residual_buy_sizing_mode == "delta"
                else target_exposure_krw
            )
            buy_submit_plan = ExecutionSubmitPlan(
                side="BUY",
                source="strategy_position",
                authority=(
                    "residual_inventory_delta"
                    if residual_buy_sizing_mode == "delta"
                    else "configured_strategy_order_size"
                ),
                final_action=action,
                qty=(None if delta_for_plan is None else float(delta_for_plan) / float(payload.get("market_price") or 1.0)),
                notional_krw=delta_for_plan,
                target_exposure_krw=target_exposure_krw,
                current_effective_exposure_krw=current_effective_exposure_krw,
                delta_krw=buy_delta_krw,
                submit_expected=submit_expected,
                pre_submit_proof_status=proof_status,
                block_reason=block_reason,
                idempotency_key=None,
            ).as_dict()
    elif raw == "SELL":
        if strategy_candidate is not None and final == "SELL":
            action = "EXIT_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none"
        elif residual_candidate is not None:
            action = "CLOSE_RESIDUAL_CANDIDATE" if proof is not None and proof.passed else "BLOCK_UNRESOLVED_RESIDUAL"
            proof_status = "passed" if proof is not None and proof.passed else "failed"
            if proof is not None and proof.passed:
                submit_expected = bool(
                    residual_live_sell_mode == "enabled"
                    and bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
                    and not bool(getattr(settings, "LIVE_DRY_RUN", True))
                )
                block_reason = (
                    "none"
                    if submit_expected
                    else (
                        "residual_live_sell_mode_telemetry"
                        if residual_live_sell_mode == "telemetry"
                        else (
                            "residual_live_sell_mode_dry_run"
                            if residual_live_sell_mode == "dry_run"
                            else "residual_live_sell_not_armed"
                        )
                    )
                )
            else:
                submit_expected = False
                block_reason = _residual_block_reason(decision_context=payload, proof=proof)
            residual_intent_key = build_order_intent_key(
                symbol=str(settings.PAIR),
                side="SELL",
                strategy_context="residual_inventory_policy",
                intent_ts=_residual_intent_ts(payload),
                intent_type="residual_close",
                qty=float(residual_candidate.qty),
            )
            residual_submit_plan = ExecutionSubmitPlan(
                side="SELL",
                source="residual_inventory",
                authority="residual_inventory_policy",
                final_action=action,
                qty=float(residual_candidate.qty),
                notional_krw=residual_candidate.notional,
                target_exposure_krw=None,
                current_effective_exposure_krw=None,
                delta_krw=None,
                submit_expected=submit_expected,
                pre_submit_proof_status=proof_status,
                block_reason=block_reason,
                idempotency_key=residual_intent_key,
            ).as_dict()
            residual_submit_plan.update(
                {
                    "intent_type": "residual_close",
                    "strategy_context": "residual_inventory_policy",
                    "would_submit_pipeline": "standard",
                    "would_intent_key": residual_intent_key,
                    "would_client_order_id_shape": "live_<ts>_sell_<submit_attempt_id>",
                    "would_order_type": "market",
                    "would_source": "residual_inventory",
                    "would_authority": "residual_inventory_policy",
                    "would_submit_side": "SELL",
                    "would_submit_qty": float(residual_candidate.qty),
                }
            )
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
        residual_live_sell_mode=residual_live_sell_mode,
        residual_buy_sizing_mode=residual_buy_sizing_mode,
        residual_submit_plan=residual_submit_plan,
        buy_submit_plan=buy_submit_plan,
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
        execution_decision = (
            dict(request.decision_context.get("execution_decision"))
            if isinstance(request.decision_context, dict)
            and isinstance(request.decision_context.get("execution_decision"), dict)
            else {}
        )
        residual_plan = (
            dict(execution_decision.get("residual_submit_plan"))
            if isinstance(execution_decision.get("residual_submit_plan"), dict)
            else {}
        )
        if (
            request.signal == "SELL"
            and residual_plan
            and str(residual_plan.get("source")) == "residual_inventory"
        ):
            if str(residual_plan.get("block_reason") or "none") != "none":
                return None
            if not bool(residual_plan.get("submit_expected")):
                return None
            if _residual_live_sell_mode() != "enabled":
                return None
            if bool(settings.LIVE_DRY_RUN) or not bool(settings.LIVE_REAL_ORDER_ARMED):
                return None
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
                    execution_submit_plan=residual_plan,
                )
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                return {
                    "status": "blocked",
                    "reason": "executor_missing_execution_submit_plan_support",
                    "side": "SELL",
                    "source": "residual_inventory",
                    "authority": "residual_inventory_policy",
                }
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
