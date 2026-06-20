from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .config import LiveModeValidationError, Settings, validate_market_preflight
from .db_core import assert_current_schema
from .oms import OPEN_ORDER_STATUSES
from .operator_smoke_preflight import validate_operator_smoke_cli_guard
from .risk_direction_gates import evaluate_risk_direction_gates
from .runtime_readiness import compute_runtime_readiness_snapshot


_QTY_EPSILON = 1e-12


class LivePipelineSmokePreflightError(ValueError):
    pass


@dataclass(frozen=True)
class LivePipelineSmokeReadiness:
    broker_qty: float
    portfolio_qty: float
    projected_total_qty: float
    open_order_count: int
    submit_unknown_count: int
    recovery_required_count: int
    fee_pending_count: int
    active_fee_accounting_blocker: bool
    broker_qty_known: bool
    balance_source_stale: bool
    projection_converged: bool

    @property
    def converged(self) -> bool:
        return bool(
            self.broker_qty_known
            and not self.balance_source_stale
            and self.projection_converged
            and abs(self.broker_qty - self.portfolio_qty) <= _QTY_EPSILON
            and abs(self.broker_qty - self.projected_total_qty) <= _QTY_EPSILON
        )

    @property
    def flat(self) -> bool:
        return self.converged and abs(self.broker_qty) <= _QTY_EPSILON

    @property
    def in_position(self) -> bool:
        return self.converged and self.broker_qty > _QTY_EPSILON

    def as_dict(self) -> dict[str, object]:
        return {
            "broker_qty": float(self.broker_qty),
            "portfolio_qty": float(self.portfolio_qty),
            "projected_total_qty": float(self.projected_total_qty),
            "open_order_count": int(self.open_order_count),
            "submit_unknown_count": int(self.submit_unknown_count),
            "recovery_required_count": int(self.recovery_required_count),
            "fee_pending_count": int(self.fee_pending_count),
            "active_fee_accounting_blocker": bool(self.active_fee_accounting_blocker),
            "broker_qty_known": bool(self.broker_qty_known),
            "balance_source_stale": bool(self.balance_source_stale),
            "projection_converged": bool(self.projection_converged),
            "converged": bool(self.converged),
            "flat": bool(self.flat),
            "in_position": bool(self.in_position),
        }


def readiness_from_snapshot(snapshot: Any) -> LivePipelineSmokeReadiness:
    evidence = dict(getattr(snapshot, "broker_position_evidence", {}) or {})
    projection = dict(getattr(snapshot, "projection_convergence", {}) or {})
    return LivePipelineSmokeReadiness(
        broker_qty=float(evidence.get("broker_qty") or 0.0),
        portfolio_qty=float(projection.get("portfolio_qty") or 0.0),
        projected_total_qty=float(projection.get("projected_total_qty") or 0.0),
        open_order_count=int(getattr(snapshot, "open_order_count", 0) or 0),
        submit_unknown_count=int(getattr(snapshot, "submit_unknown_count", 0) or 0),
        recovery_required_count=int(getattr(snapshot, "recovery_required_count", 0) or 0),
        fee_pending_count=int(getattr(snapshot, "fee_pending_count", 0) or 0),
        active_fee_accounting_blocker=bool(getattr(snapshot, "active_fee_accounting_blocker", False)),
        broker_qty_known=bool(evidence.get("broker_qty_known")),
        balance_source_stale=bool(evidence.get("balance_source_stale")),
        projection_converged=bool(projection.get("converged")),
    )


def open_local_order_count(conn: Any) -> int:
    placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM orders WHERE status IN ({placeholders})",
        tuple(OPEN_ORDER_STATUSES),
    ).fetchone()
    return int(row["cnt"] if hasattr(row, "keys") else row[0])


def open_broker_order_count(broker: Any, *, market: str) -> int:
    if broker is None:
        return 0
    if hasattr(broker, "get_recent_orders_for_recovery"):
        orders = broker.get_recent_orders_for_recovery(market=str(market), limit=30)
    elif hasattr(broker, "get_open_orders"):
        orders = broker.get_open_orders()
    else:
        orders = []
    return sum(
        1
        for order in orders
        if str(getattr(order, "status", "") or "").strip().upper() in OPEN_ORDER_STATUSES
    )


def validate_live_pipeline_smoke_start_preflight(
    *,
    cfg: Settings,
    conn: Any,
    broker: Any,
    market: str,
    readiness_builder: Callable[[Any], Any] = compute_runtime_readiness_snapshot,
    market_preflight: Callable[[Settings], Any] = validate_market_preflight,
    cli_guard: Callable[[Settings], Any] = validate_operator_smoke_cli_guard,
    schema_validator: Callable[[Any], Any] = assert_current_schema,
) -> LivePipelineSmokeReadiness:
    if str(cfg.MODE).strip().lower() != "live":
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_requires_live_mode")
    if bool(cfg.LIVE_DRY_RUN):
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_requires_live_dry_run_false")
    if not bool(cfg.LIVE_REAL_ORDER_ARMED):
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_requires_live_real_order_armed")
    if bool(cfg.KILL_SWITCH):
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_blocked_by_kill_switch")
    if str(getattr(cfg, "EXECUTION_ENGINE", "") or "").strip().lower() != "target_delta":
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_requires_execution_engine_target_delta")
    if str(market or "").strip().upper() != str(cfg.PAIR or "").strip().upper():
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_market_mismatch_with_settings_pair")
    try:
        cli_guard(cfg)
        schema_validator(conn)
        market_preflight(cfg)
    except (LiveModeValidationError, Exception) as exc:
        raise LivePipelineSmokePreflightError(f"live_pipeline_smoke_preflight_failed:{exc}") from exc

    local_open = open_local_order_count(conn)
    if local_open > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_open_local_order")
    broker_open = open_broker_order_count(broker, market=market)
    if broker_open > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_open_broker_order")

    readiness = readiness_from_snapshot(readiness_builder(conn))
    if readiness.submit_unknown_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_submit_unknown_present")
    if readiness.recovery_required_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_recovery_required_present")
    if readiness.fee_pending_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_fee_pending_present")
    if readiness.active_fee_accounting_blocker:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_active_fee_accounting_blocker")
    if not readiness.broker_qty_known or readiness.balance_source_stale:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_broker_qty_evidence_missing_or_stale")
    if not readiness.projection_converged:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_projection_non_converged")
    if not readiness.converged:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_broker_local_projection_mismatch")
    if not readiness.flat:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_start_requires_flat")
    return readiness


def validate_live_pipeline_smoke_step_readiness(
    readiness: LivePipelineSmokeReadiness,
    *,
    expected_side: str,
    requested_qty: float | None = None,
    terminal_flat_authority: bool = False,
) -> None:
    if readiness.open_order_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_step_open_order")
    if readiness.submit_unknown_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_step_submit_unknown")
    if readiness.recovery_required_count > 0:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_step_recovery_required")
    if not readiness.converged:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_step_projection_mismatch")
    side = str(expected_side or "").upper()
    direction_gate = evaluate_risk_direction_gates(
        fee_pending=bool(readiness.fee_pending_count > 0 or readiness.active_fee_accounting_blocker),
        side=side,
        broker_qty=float(readiness.broker_qty) if readiness.broker_qty_known else None,
        requested_qty=(
            float(requested_qty)
            if requested_qty is not None
            else (float(readiness.broker_qty) if side == "SELL" else None)
        ),
        terminal_flat_authority=bool(terminal_flat_authority),
        risk_reducing_authority=False,
        open_order_count=int(readiness.open_order_count),
        submit_unknown_count=int(readiness.submit_unknown_count),
        recovery_required_count=int(readiness.recovery_required_count),
    )
    if readiness.fee_pending_count > 0 or readiness.active_fee_accounting_blocker:
        if side == "BUY" or not direction_gate.terminal_flat_closeout_allowed:
            raise LivePipelineSmokePreflightError(str(direction_gate.reason_code))
    if side == "BUY" and not readiness.flat:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_buy_requires_flat")
    if side == "SELL" and not readiness.in_position:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_sell_requires_position")
