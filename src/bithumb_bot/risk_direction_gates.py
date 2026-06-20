from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskDirectionGateResult:
    exposure_increase_allowed: bool
    hold_allowed: bool
    risk_reducing_sell_allowed: bool
    terminal_flat_closeout_allowed: bool
    strategy_new_cycle_allowed: bool
    reason_code: str

    def as_dict(self) -> dict[str, object]:
        return {
            "exposure_increase_allowed": bool(self.exposure_increase_allowed),
            "hold_allowed": bool(self.hold_allowed),
            "risk_reducing_sell_allowed": bool(self.risk_reducing_sell_allowed),
            "terminal_flat_closeout_allowed": bool(self.terminal_flat_closeout_allowed),
            "strategy_new_cycle_allowed": bool(self.strategy_new_cycle_allowed),
            "reason_code": self.reason_code,
        }


def evaluate_risk_direction_gates(
    *,
    fee_pending: bool,
    side: str,
    broker_qty: float | None,
    requested_qty: float | None,
    terminal_flat_authority: bool = False,
    risk_reducing_authority: bool = False,
    open_order_count: int = 0,
    submit_unknown_count: int = 0,
    recovery_required_count: int = 0,
) -> RiskDirectionGateResult:
    side_upper = str(side or "").strip().upper()
    broker_known = broker_qty is not None
    broker_value = max(0.0, float(broker_qty or 0.0))
    requested_value = max(0.0, float(requested_qty or 0.0))
    clean_order_state = (
        int(open_order_count or 0) == 0
        and int(submit_unknown_count or 0) == 0
        and int(recovery_required_count or 0) == 0
    )
    close_qty_covered = broker_known and broker_value > 0.0 and requested_value <= broker_value + 1e-12
    terminal_allowed = bool(
        side_upper == "SELL"
        and terminal_flat_authority
        and close_qty_covered
        and clean_order_state
    )
    risk_reducing_allowed = bool(
        side_upper == "SELL"
        and risk_reducing_authority
        and close_qty_covered
        and clean_order_state
    )
    if fee_pending:
        exposure_allowed = False
        new_cycle_allowed = False
        reason = "fee_pending_blocks_exposure_increase"
        if side_upper == "SELL" and (terminal_allowed or risk_reducing_allowed):
            reason = "fee_pending_allows_authorized_risk_reduction"
        elif side_upper == "SELL":
            reason = "fee_pending_blocks_unauthorized_sell"
    else:
        exposure_allowed = side_upper != "SELL"
        new_cycle_allowed = True
        reason = "allowed"
    return RiskDirectionGateResult(
        exposure_increase_allowed=exposure_allowed,
        hold_allowed=True,
        risk_reducing_sell_allowed=risk_reducing_allowed,
        terminal_flat_closeout_allowed=terminal_allowed,
        strategy_new_cycle_allowed=new_cycle_allowed,
        reason_code=reason,
    )


__all__ = ["RiskDirectionGateResult", "evaluate_risk_direction_gates"]
