from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ResidualDispositionName = Literal[
    "NONE",
    "EXECUTABLE_EXPOSURE",
    "TRACKED_NON_EXECUTABLE",
    "CLOSEOUT_EXECUTABLE",
    "BLOCKING_INCONSISTENT",
    "AUTHORITY_REPAIR_REQUIRED",
]


@dataclass(frozen=True)
class ResidualDisposition:
    disposition: ResidualDispositionName
    exchange_sellable: bool
    run_allowed: bool
    buy_allowed: bool
    sell_allowed: bool
    flatten_allowed: bool
    repair_required: bool
    reason_codes: tuple[str, ...]
    quantity_rule_authority: str
    broker_qty: float | None
    portfolio_qty: float | None
    projected_qty: float | None
    min_qty: float | None = None
    min_qty_source: str = "unknown"
    qty_step: float | None = None
    qty_step_source: str = "unknown"
    min_notional_krw: float | None = None
    min_notional_source: str = "unknown"
    max_qty_decimals: int | None = None
    max_qty_decimals_source: str = "unknown"
    qty_step_authority_level: str = "unknown"
    quantity_rule_source_mode: str = "unknown"
    quantity_contract_complete: bool = False
    operator_action_required: bool = False
    recommended_action: str = "none"
    manual_exchange_action_required: bool = False
    broker_local_projection_state: str = "unknown"

    def as_dict(self) -> dict[str, object]:
        return {
            "disposition": self.disposition,
            "exchange_sellable": bool(self.exchange_sellable),
            "run_allowed": bool(self.run_allowed),
            "buy_allowed": bool(self.buy_allowed),
            "sell_allowed": bool(self.sell_allowed),
            "flatten_allowed": bool(self.flatten_allowed),
            "repair_required": bool(self.repair_required),
            "reason_codes": list(self.reason_codes),
            "reason_code": self.reason_codes[0] if self.reason_codes else "none",
            "quantity_rule_authority": self.quantity_rule_authority,
            "broker_qty": self.broker_qty,
            "portfolio_qty": self.portfolio_qty,
            "projected_qty": self.projected_qty,
            "min_qty": self.min_qty,
            "min_qty_source": self.min_qty_source,
            "qty_step": self.qty_step,
            "qty_step_source": self.qty_step_source,
            "min_notional_krw": self.min_notional_krw,
            "min_notional_source": self.min_notional_source,
            "max_qty_decimals": self.max_qty_decimals,
            "max_qty_decimals_source": self.max_qty_decimals_source,
            "qty_step_authority_level": self.qty_step_authority_level,
            "quantity_rule_source_mode": self.quantity_rule_source_mode,
            "quantity_contract_complete": bool(self.quantity_contract_complete),
            "operator_action_required": bool(self.operator_action_required),
            "recommended_action": self.recommended_action,
            "manual_exchange_action_required": bool(self.manual_exchange_action_required),
            "broker_local_projection_state": self.broker_local_projection_state,
        }


def quantity_provenance_from_lot_definition(lot_definition: Any | None) -> dict[str, object]:
    if lot_definition is None:
        return {
            "min_qty": None,
            "min_qty_source": "unknown",
            "qty_step": None,
            "qty_step_source": "unknown",
            "min_notional_krw": None,
            "min_notional_source": "unknown",
            "max_qty_decimals": None,
            "max_qty_decimals_source": "unknown",
            "qty_step_authority_level": "unknown",
            "quantity_rule_source_mode": "unknown",
            "quantity_contract_complete": False,
        }
    source_mode = str(getattr(lot_definition, "source_mode", "") or "unknown")
    if source_mode in {"ledger", "exchange", "merged", "orders_chance", "chance_doc"}:
        authority = "exchange_hard" if source_mode != "ledger" else "persisted_exchange_snapshot"
        source = "persisted_exchange_snapshot" if source_mode == "ledger" else "exchange"
        complete = True
    elif source_mode == "local_fallback":
        authority = "local_fallback"
        source = "local_fallback"
        complete = True
    else:
        authority = "unknown"
        source = "unknown"
        complete = False
    return {
        "min_qty": getattr(lot_definition, "min_qty", None),
        "min_qty_source": source,
        "qty_step": getattr(lot_definition, "qty_step", None),
        "qty_step_source": source,
        "min_notional_krw": getattr(lot_definition, "min_notional_krw", None),
        "min_notional_source": source,
        "max_qty_decimals": getattr(lot_definition, "max_qty_decimals", None),
        "max_qty_decimals_source": source,
        "qty_step_authority_level": authority,
        "quantity_rule_source_mode": source,
        "quantity_contract_complete": bool(complete),
    }


def build_residual_disposition(
    *,
    residual_inventory: Any,
    residual_inventory_state: str,
    residual_policy_allows_run: bool,
    residual_policy_allows_buy: bool,
    residual_policy_allows_sell: bool,
    position_state: Any,
    authority_assessment: dict[str, object],
    projection_convergence: dict[str, object],
    broker_position_evidence: dict[str, object],
    lot_definition: Any | None,
    open_order_count: int,
    submit_unknown_count: int,
    recovery_required_count: int,
) -> ResidualDisposition:
    provenance = quantity_provenance_from_lot_definition(lot_definition)
    broker_qty = (
        float(broker_position_evidence.get("broker_qty") or 0.0)
        if bool(broker_position_evidence.get("broker_qty_known"))
        else None
    )
    portfolio_qty = float(projection_convergence.get("portfolio_qty") or 0.0)
    projected_qty = float(projection_convergence.get("projected_total_qty") or 0.0)
    converged = bool(
        broker_qty is not None
        and abs(float(broker_qty) - portfolio_qty) <= 1e-12
        and abs(projected_qty - portfolio_qty) <= 1e-12
        and bool(projection_convergence.get("converged"))
    )
    broker_local_projection_state = "converged" if converged else "mismatch_or_unknown"
    exchange_sellable = bool(getattr(residual_inventory, "exchange_sellable", False))
    residual_qty = float(getattr(residual_inventory, "residual_qty", 0.0) or 0.0)
    normalized = getattr(position_state, "normalized_exposure", None)
    has_executable = bool(getattr(normalized, "has_executable_exposure", False))
    sellable_lots = int(getattr(normalized, "sellable_executable_lot_count", 0) or 0)
    repair_keys = (
        "needs_correction",
        "needs_residual_normalization",
        "needs_portfolio_projection_repair",
        "needs_full_projection_rebuild",
    )
    base = {
        "quantity_rule_authority": str(provenance["qty_step_authority_level"]),
        "broker_qty": broker_qty,
        "portfolio_qty": portfolio_qty,
        "projected_qty": projected_qty,
        "broker_local_projection_state": broker_local_projection_state,
        **provenance,
    }
    if any(bool(authority_assessment.get(key)) for key in repair_keys):
        return ResidualDisposition(
            disposition="AUTHORITY_REPAIR_REQUIRED",
            exchange_sellable=exchange_sellable,
            run_allowed=False,
            buy_allowed=False,
            sell_allowed=False,
            flatten_allowed=False,
            repair_required=True,
            reason_codes=("authority_repair_required",),
            operator_action_required=True,
            recommended_action="review_position_authority_evidence",
            manual_exchange_action_required=False,
            **base,
        )
    known_broker_projection_mismatch = bool(
        broker_qty is not None and residual_qty > 1e-12 and not converged
    )
    if open_order_count > 0 or submit_unknown_count > 0 or recovery_required_count > 0 or (
        known_broker_projection_mismatch
    ):
        return ResidualDisposition(
            disposition="BLOCKING_INCONSISTENT",
            exchange_sellable=exchange_sellable,
            run_allowed=False,
            buy_allowed=False,
            sell_allowed=False,
            flatten_allowed=False,
            repair_required=True,
            reason_codes=("residual_state_inconsistent",),
            operator_action_required=True,
            recommended_action="review_recovery_report",
            manual_exchange_action_required=False,
            **base,
        )
    if str(residual_inventory_state) == "RESIDUAL_INVENTORY_TRACKED" and residual_qty > 1e-12:
        if exchange_sellable:
            return ResidualDisposition(
                disposition="CLOSEOUT_EXECUTABLE",
                exchange_sellable=True,
                run_allowed=bool(residual_policy_allows_run),
                buy_allowed=bool(residual_policy_allows_buy),
                sell_allowed=bool(residual_policy_allows_sell),
                flatten_allowed=True,
                repair_required=False,
                reason_codes=("sellable_residual_closeout_candidate",),
                operator_action_required=True,
                recommended_action="flatten-position",
                manual_exchange_action_required=False,
                **base,
            )
        return ResidualDisposition(
            disposition="TRACKED_NON_EXECUTABLE",
            exchange_sellable=False,
            run_allowed=bool(residual_policy_allows_run),
            buy_allowed=bool(residual_policy_allows_buy),
            sell_allowed=False,
            flatten_allowed=False,
            repair_required=False,
            reason_codes=("sub_min_qty_residual_tracked",),
            operator_action_required=False,
            recommended_action="none",
            manual_exchange_action_required=False,
            **base,
        )
    if has_executable or sellable_lots > 0:
        return ResidualDisposition(
            disposition="EXECUTABLE_EXPOSURE",
            exchange_sellable=exchange_sellable,
            run_allowed=bool(residual_policy_allows_run),
            buy_allowed=False,
            sell_allowed=True,
            flatten_allowed=True,
            repair_required=False,
            reason_codes=("executable_exposure_present",),
            operator_action_required=False,
            recommended_action="none",
            manual_exchange_action_required=False,
            **base,
        )
    return ResidualDisposition(
        disposition="NONE",
        exchange_sellable=False,
        run_allowed=bool(residual_policy_allows_run),
        buy_allowed=bool(residual_policy_allows_buy),
        sell_allowed=False,
        flatten_allowed=False,
        repair_required=False,
        reason_codes=("none",),
        operator_action_required=False,
        recommended_action="none",
        manual_exchange_action_required=False,
        **base,
    )
