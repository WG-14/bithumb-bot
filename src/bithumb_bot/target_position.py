from __future__ import annotations

import math
from dataclasses import dataclass


TARGET_ENGINE_MODE_SHADOW = "shadow"
TARGET_ENGINE_MODE_TARGET_DELTA = "target_delta"
TARGET_STATE_PERSISTENCE_NOT_PERSISTED = "not_yet_persisted"
TARGET_STATE_PERSISTENCE_PERSISTED = "persisted"
TARGET_STATE_PERSISTENCE_MISSING = "missing"


@dataclass(frozen=True)
class TargetPositionSettings:
    execution_engine: str = "lot_native"
    shadow_enabled: bool = False
    target_exposure_krw: float | None = None
    max_order_krw: float = 0.0
    hold_policy: str = "maintain_previous_target"


@dataclass(frozen=True)
class TargetPositionState:
    pair: str
    target_exposure_krw: float
    target_qty: float
    last_signal: str
    last_decision_id: int | None
    last_reference_price: float
    updated_ts: int


@dataclass(frozen=True)
class TargetPositionDecision:
    engine_mode: str
    raw_signal: str
    previous_target_exposure_krw: float | None
    new_target_exposure_krw: float | None
    hold_policy: str
    state_persistence: str
    reference_price: float | None
    current_qty: float | None
    current_exposure_krw: float | None
    target_qty: float | None
    delta_qty: float | None
    delta_notional_krw: float | None
    delta_side: str
    would_submit: bool
    submit_qty: float | None
    submit_notional_krw: float | None
    block_reason: str
    position_truth_state: str
    dust_classification: str
    order_rule_min_qty: float | None
    order_rule_min_notional_krw: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "target_engine_mode": self.engine_mode,
            "target_raw_signal": self.raw_signal,
            "target_previous_exposure_krw": self.previous_target_exposure_krw,
            "target_new_exposure_krw": self.new_target_exposure_krw,
            "target_hold_policy": self.hold_policy,
            "target_state_persistence": self.state_persistence,
            "target_reference_price": self.reference_price,
            "target_current_qty": self.current_qty,
            "target_current_exposure_krw": self.current_exposure_krw,
            "target_qty": self.target_qty,
            "target_delta_qty": self.delta_qty,
            "target_delta_notional_krw": self.delta_notional_krw,
            "target_delta_side": self.delta_side,
            "target_would_submit": bool(self.would_submit),
            "target_submit_qty": self.submit_qty,
            "target_submit_notional_krw": self.submit_notional_krw,
            "target_block_reason": self.block_reason,
            "target_position_truth_state": self.position_truth_state,
            "target_dust_classification": self.dust_classification,
            "target_order_rule_min_qty": self.order_rule_min_qty,
            "target_order_rule_min_notional_krw": self.order_rule_min_notional_krw,
        }


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _as_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _dict_value(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _configured_target_exposure(settings: TargetPositionSettings) -> float:
    explicit = _as_float(settings.target_exposure_krw)
    if explicit is not None and explicit >= 0.0:
        return float(explicit)
    return max(0.0, float(settings.max_order_krw or 0.0))


def _first_truth_blocker(payload: dict[str, object], broker_evidence: dict[str, object]) -> str | None:
    if not bool(broker_evidence.get("broker_qty_known")):
        return "broker_qty_unknown"
    if bool(broker_evidence.get("balance_source_stale")):
        return "balance_snapshot_stale"
    if (
        "broker_portfolio_converged" in payload
        and not bool(payload.get("broker_portfolio_converged"))
    ):
        return "broker_local_not_converged"
    if (
        "dust_broker_local_match" in payload
        and not bool(payload.get("dust_broker_local_match"))
    ):
        return "broker_local_not_converged"
    raw_holdings = _dict_value(payload.get("raw_holdings"))
    if "broker_local_match" in raw_holdings and not bool(raw_holdings.get("broker_local_match")):
        return "broker_local_not_converged"
    projection = _dict_value(payload.get("projection_convergence"))
    if projection and not bool(projection.get("converged")):
        return "projection_not_converged"
    if "projection_converged" in payload and not bool(payload.get("projection_converged")):
        return "projection_not_converged"
    if _as_int(payload.get("open_order_count")) > 0:
        return "open_order_count_nonzero"
    if _as_int(payload.get("unresolved_open_order_count")) > 0:
        return "unresolved_open_order_count_nonzero"
    if _as_int(payload.get("recovery_required_count")) > 0:
        return "recovery_required_count_nonzero"
    if _as_int(payload.get("submit_unknown_count")) > 0:
        return "submit_unknown_count_nonzero"
    if bool(payload.get("active_fee_accounting_blocker")):
        return "active_fee_accounting_blocker"
    if "accounting_projection_ok" in payload and not bool(payload.get("accounting_projection_ok")):
        return "accounting_projection_not_ok"
    return None


def build_target_position_decision(
    *,
    raw_signal: str,
    previous_target_exposure_krw: float | None,
    current_position_snapshot: dict[str, object] | None,
    readiness_payload: dict[str, object] | None,
    order_rules: dict[str, object] | None,
    reference_price: float | None,
    settings: TargetPositionSettings,
) -> TargetPositionDecision:
    payload = dict(readiness_payload or {})
    current_snapshot = dict(current_position_snapshot or {})
    rules = dict(order_rules or {})
    broker_evidence = _dict_value(payload.get("broker_position_evidence"))
    if current_snapshot:
        broker_evidence = {**broker_evidence, **current_snapshot}

    signal = str(raw_signal or "HOLD").upper()
    price = _as_float(reference_price)
    min_qty = _as_float(rules.get("min_qty"))
    if min_qty is None:
        min_qty = _as_float(payload.get("min_qty", payload.get("residual_proof_min_qty")))
    min_notional = _as_float(rules.get("min_notional_krw"))
    if min_notional is None:
        min_notional = _as_float(
            payload.get("min_notional_krw", payload.get("residual_proof_min_notional_krw"))
        )

    engine_mode = (
        TARGET_ENGINE_MODE_TARGET_DELTA
        if str(settings.execution_engine or "lot_native").strip().lower() == TARGET_ENGINE_MODE_TARGET_DELTA
        and not bool(settings.shadow_enabled)
        else TARGET_ENGINE_MODE_SHADOW
    )
    state_persistence = (
        TARGET_STATE_PERSISTENCE_PERSISTED
        if previous_target_exposure_krw is not None or engine_mode == TARGET_ENGINE_MODE_TARGET_DELTA
        else TARGET_STATE_PERSISTENCE_NOT_PERSISTED
    )

    base = {
        "engine_mode": engine_mode,
        "raw_signal": signal,
        "previous_target_exposure_krw": previous_target_exposure_krw,
        "hold_policy": str(settings.hold_policy or "maintain_previous_target"),
        "state_persistence": state_persistence,
        "reference_price": price,
        "current_qty": None,
        "current_exposure_krw": None,
        "target_qty": None,
        "delta_qty": None,
        "delta_notional_krw": None,
        "delta_side": "NONE",
        "would_submit": False,
        "submit_qty": None,
        "submit_notional_krw": None,
        "position_truth_state": "blocked",
        "dust_classification": "unknown",
        "order_rule_min_qty": min_qty,
        "order_rule_min_notional_krw": min_notional,
    }

    def _decision(**overrides: object) -> TargetPositionDecision:
        return TargetPositionDecision(**{**base, **overrides})

    if signal == "BUY":
        target_exposure = _configured_target_exposure(settings)
    elif signal == "SELL":
        target_exposure = 0.0
    elif signal == "HOLD":
        if previous_target_exposure_krw is None:
            return _decision(
                new_target_exposure_krw=None,
                state_persistence=(
                    TARGET_STATE_PERSISTENCE_MISSING
                    if engine_mode == TARGET_ENGINE_MODE_TARGET_DELTA
                    else TARGET_STATE_PERSISTENCE_NOT_PERSISTED
                ),
                block_reason="missing_persistent_target_state",
                dust_classification="hold_target_unknown",
            )
        target_exposure = max(0.0, float(previous_target_exposure_krw))
    else:
        target_exposure = None

    if target_exposure is None:
        return _decision(
            new_target_exposure_krw=None,
            block_reason="unsupported_signal",
        )
    if price is None or price <= 0.0:
        return _decision(
            new_target_exposure_krw=target_exposure,
            block_reason="missing_reference_price",
        )
    if min_qty is None:
        return _decision(
            new_target_exposure_krw=target_exposure,
            block_reason="missing_order_rule_min_qty",
        )
    if min_notional is None:
        return _decision(
            new_target_exposure_krw=target_exposure,
            block_reason="missing_order_rule_min_notional_krw",
        )

    truth_blocker = _first_truth_blocker(payload, broker_evidence)
    current_qty = _as_float(broker_evidence.get("broker_qty"))
    if current_qty is None:
        current_qty = _as_float(current_snapshot.get("current_qty"))
    if truth_blocker is not None:
        return _decision(
            new_target_exposure_krw=target_exposure,
            current_qty=current_qty,
            current_exposure_krw=(None if current_qty is None else max(0.0, current_qty) * price),
            block_reason=truth_blocker,
            position_truth_state="blocked",
        )
    if current_qty is None:
        return _decision(
            new_target_exposure_krw=target_exposure,
            block_reason="broker_qty_unknown",
            position_truth_state="blocked",
        )

    current_qty = max(0.0, float(current_qty))
    current_exposure = current_qty * price
    target_qty = max(0.0, float(target_exposure) / price)
    delta_qty = target_qty - current_qty
    delta_notional = delta_qty * price
    abs_delta_qty = abs(delta_qty)
    abs_delta_notional = abs(delta_notional)

    if delta_qty > 1e-12:
        delta_side = "BUY"
    elif delta_qty < -1e-12:
        delta_side = "SELL"
    else:
        delta_side = "NONE"

    if delta_side == "NONE" or abs_delta_qty + 1e-12 < float(min_qty) or abs_delta_notional + 1e-9 < float(min_notional):
        return _decision(
            new_target_exposure_krw=target_exposure,
            current_qty=current_qty,
            current_exposure_krw=current_exposure,
            target_qty=target_qty,
            delta_qty=delta_qty,
            delta_notional_krw=delta_notional,
            delta_side="NONE",
            submit_qty=0.0,
            submit_notional_krw=0.0,
            block_reason="delta_below_exchange_min",
            position_truth_state="converged",
            dust_classification="true_dust",
        )

    return _decision(
        new_target_exposure_krw=target_exposure,
        current_qty=current_qty,
        current_exposure_krw=current_exposure,
        target_qty=target_qty,
        delta_qty=delta_qty,
        delta_notional_krw=delta_notional,
        delta_side=delta_side,
        would_submit=True,
        submit_qty=abs_delta_qty,
        submit_notional_krw=abs_delta_notional,
        block_reason="none",
        position_truth_state="converged",
        dust_classification="executable_delta",
    )
