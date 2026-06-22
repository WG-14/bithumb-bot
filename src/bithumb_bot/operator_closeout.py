from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR

from .broker.order_payloads import build_order_payload_from_plan
from .execution_models import OrderIntent
from .quantity_contract import (
    ExchangeQuantityContract,
    should_enforce_qty_step_as_hard_rule,
)

COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT = "operator_clean_account_closeout"
COMMAND_INTENT_STRATEGY_EXIT = "strategy_exit"
COMMAND_INTENT_OPERATOR_RISK_REDUCTION = "operator_risk_reduction"
COMMAND_INTENT_EMERGENCY_FLATTEN = "emergency_flatten"
REASON_BROKER_CONFIRMED_RESIDUAL_CLOSEOUT = "broker_confirmed_residual_closeout"
REASON_FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL = "full_closeout_would_leave_residual"
REASON_QUANTITY_STEP_AUTHORITY_UNKNOWN = "quantity_step_authority_unknown_or_fallback"
REASON_QUANTITY_CONTRACT_INCOMPLETE = "quantity_contract_incomplete"
REASON_EXCHANGE_RULE_BLOCK = "exchange_rule_block"
REASON_LOCAL_POLICY_BLOCK = "local_policy_block"
RECOMMENDED_MANUAL_CLOSEOUT = "manual_exchange_closeout_or_rule_update"
RECOMMENDED_REVIEW_LOCAL_FALLBACK = "review_local_quantity_fallback"
_QTY_EPS = 1e-12


@dataclass(frozen=True)
class OperatorCleanCloseoutContract:
    status: str
    command_intent: str
    market: str
    side: str
    dry_run: bool
    broker_asset_available: float
    raw_total_asset_qty: float
    planned_sell_qty: float
    estimated_residual_qty: float
    estimated_residual_notional_krw: float
    clean_account_after_sell: bool
    quantity_authority: dict[str, object]
    closeout_allowed: bool
    block_reason: str | None
    recommended_action: str | None
    submit_payload_preview: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "command_intent": self.command_intent,
            "market": self.market,
            "side": self.side,
            "dry_run": bool(self.dry_run),
            "broker_asset_available": float(self.broker_asset_available),
            "raw_total_asset_qty": float(self.raw_total_asset_qty),
            "planned_sell_qty": float(self.planned_sell_qty),
            "estimated_residual_qty": float(self.estimated_residual_qty),
            "estimated_residual_notional_krw": float(self.estimated_residual_notional_krw),
            "clean_account_after_sell": bool(self.clean_account_after_sell),
            "quantity_authority": dict(self.quantity_authority),
            "closeout_allowed": bool(self.closeout_allowed),
            "block_reason": self.block_reason,
            "recommended_action": self.recommended_action,
            "submit_payload_preview": dict(self.submit_payload_preview),
        }


def _decimal_from_number(value: object) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError(f"invalid non-finite quantity: {value}")
    return parsed


def _floor_to_max_decimals(*, qty: float, max_qty_decimals: int) -> float:
    parsed = max(Decimal("0"), _decimal_from_number(qty))
    if int(max_qty_decimals) > 0:
        parsed = parsed.quantize(Decimal("1").scaleb(-int(max_qty_decimals)), rounding=ROUND_FLOOR)
    return float(parsed)


def _qty_matches_step(*, qty: float, qty_step: float) -> bool:
    step = _decimal_from_number(qty_step)
    if step <= 0:
        return True
    parsed_qty = _decimal_from_number(qty)
    stepped = (parsed_qty / step).to_integral_value(rounding=ROUND_FLOOR) * step
    return abs(float(parsed_qty - stepped)) <= _QTY_EPS


def _metrics(*, raw_total_asset_qty: float, planned_sell_qty: float, market_price: float) -> dict[str, object]:
    raw_qty = max(0.0, float(raw_total_asset_qty))
    planned_qty = max(0.0, float(planned_sell_qty))
    residual_qty = max(0.0, raw_qty - planned_qty)
    if residual_qty <= _QTY_EPS:
        residual_qty = 0.0
    else:
        residual_qty = round(residual_qty, 12)
    return {
        "raw_total_asset_qty": float(raw_qty),
        "planned_sell_qty": float(planned_qty),
        "estimated_residual_qty": float(residual_qty),
        "estimated_residual_notional_krw": float(residual_qty * float(market_price)),
        "clean_account_after_sell": bool(residual_qty <= _QTY_EPS),
    }


def _blocked_contract(
    *,
    market: str,
    dry_run: bool,
    broker_asset_available: float,
    raw_total_asset_qty: float,
    planned_sell_qty: float,
    market_price: float,
    quantity_contract: ExchangeQuantityContract,
    block_reason: str,
    recommended_action: str | None,
    submit_payload_preview: dict[str, object] | None = None,
) -> OperatorCleanCloseoutContract:
    closeout_metrics = _metrics(
        raw_total_asset_qty=raw_total_asset_qty,
        planned_sell_qty=planned_sell_qty,
        market_price=market_price,
    )
    return OperatorCleanCloseoutContract(
        status="blocked",
        command_intent=COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
        market=market,
        side="SELL",
        dry_run=bool(dry_run),
        broker_asset_available=float(broker_asset_available),
        raw_total_asset_qty=float(raw_total_asset_qty),
        planned_sell_qty=float(planned_sell_qty),
        estimated_residual_qty=float(closeout_metrics["estimated_residual_qty"]),
        estimated_residual_notional_krw=float(closeout_metrics["estimated_residual_notional_krw"]),
        clean_account_after_sell=False,
        quantity_authority=quantity_contract.as_dict(),
        closeout_allowed=False,
        block_reason=block_reason,
        recommended_action=recommended_action,
        submit_payload_preview=submit_payload_preview or {},
    )


def build_operator_clean_closeout_contract(
    *,
    broker,
    market: str,
    raw_total_asset_qty: float,
    broker_asset_available: float,
    market_price: float,
    quantity_contract: ExchangeQuantityContract,
    dry_run: bool,
    plan_place_order_fn,
    rules,
    client_order_id: str,
) -> OperatorCleanCloseoutContract:
    raw_qty = max(0.0, float(raw_total_asset_qty))
    broker_qty = max(0.0, float(broker_asset_available))
    tolerance = max(_QTY_EPS, float(quantity_contract.min_qty or 0.0) * 1e-6)
    if abs(raw_qty - broker_qty) > tolerance:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=0.0,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason="broker_residual_does_not_match_local_evidence",
            recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
        )
    if quantity_contract.qty_step_authority_level == "unknown":
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=0.0,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_QUANTITY_STEP_AUTHORITY_UNKNOWN,
            recommended_action=(
                quantity_contract.quantity_contract_recommended_action
                or RECOMMENDED_MANUAL_CLOSEOUT
            ),
        )
    if not quantity_contract.quantity_contract_complete:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=0.0,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_QUANTITY_CONTRACT_INCOMPLETE,
            recommended_action=(
                quantity_contract.quantity_contract_recommended_action
                or RECOMMENDED_MANUAL_CLOSEOUT
            ),
        )

    planned_qty = _floor_to_max_decimals(
        qty=broker_qty,
        max_qty_decimals=quantity_contract.max_qty_decimals,
    )
    if planned_qty <= 0.0:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
            recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
        )
    if quantity_contract.min_qty > 0 and planned_qty + _QTY_EPS < quantity_contract.min_qty:
        local_fallback = quantity_contract.qty_step_authority_level == "local_fallback"
        exchange_authority = quantity_contract.qty_step_authority_level in {
            "exchange_hard",
            "persisted_exchange_snapshot",
        }
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_LOCAL_POLICY_BLOCK
            if local_fallback
            else REASON_EXCHANGE_RULE_BLOCK
            if exchange_authority
            else (
                "order qty below minimum: "
                f"{planned_qty:.12f} < {quantity_contract.min_qty:.12f}"
            ),
            recommended_action=RECOMMENDED_REVIEW_LOCAL_FALLBACK
            if local_fallback
            else RECOMMENDED_MANUAL_CLOSEOUT,
        )
    if quantity_contract.min_notional_krw > 0 and (planned_qty * float(market_price)) + _QTY_EPS < quantity_contract.min_notional_krw:
        local_fallback = quantity_contract.qty_step_authority_level == "local_fallback"
        exchange_authority = quantity_contract.qty_step_authority_level in {
            "exchange_hard",
            "persisted_exchange_snapshot",
        }
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_LOCAL_POLICY_BLOCK
            if local_fallback
            else REASON_EXCHANGE_RULE_BLOCK
            if exchange_authority
            else (
                "order notional below minimum (SELL): "
                f"{(planned_qty * float(market_price)):.2f} < {quantity_contract.min_notional_krw:.2f}"
            ),
            recommended_action=RECOMMENDED_REVIEW_LOCAL_FALLBACK
            if local_fallback
            else RECOMMENDED_MANUAL_CLOSEOUT,
        )
    if should_enforce_qty_step_as_hard_rule(
        quantity_contract,
        command_intent=COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
    ):
        step = float(quantity_contract.exchange_qty_step or 0.0)
        if step > 0 and not _qty_matches_step(qty=planned_qty, qty_step=step):
            step_planned_qty = float(
                (_decimal_from_number(planned_qty) / _decimal_from_number(step)).to_integral_value(rounding=ROUND_FLOOR)
                * _decimal_from_number(step)
            )
            return _blocked_contract(
                market=market,
                dry_run=dry_run,
                broker_asset_available=broker_qty,
                raw_total_asset_qty=raw_qty,
                planned_sell_qty=step_planned_qty,
                market_price=market_price,
                quantity_contract=quantity_contract,
                block_reason=REASON_FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
                recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
            )

    submit_payload_preview: dict[str, object]
    try:
        plan = plan_place_order_fn(
            broker,
            intent=OrderIntent(
                client_order_id=client_order_id,
                market=market,
                side="SELL",
                normalized_side="ask",
                qty=float(planned_qty),
                price=None,
                created_ts=0,
                market_price_hint=float(market_price),
                trace_id=client_order_id,
            ),
            rules=rules,
            skip_qty_revalidation=True,
        )
        payload_plan = build_order_payload_from_plan(plan=plan)
        payload = dict(payload_plan.payload)
        submit_payload_preview = {
            "submit_plan_requested_qty": float(plan.requested_qty),
            "submit_plan_submitted_qty": float(plan.submitted_qty),
            "exchange_submit_volume": payload.get("volume"),
            "payload_volume": payload.get("volume"),
            "payload_order_type": payload.get("order_type"),
            "payload_side": payload.get("side"),
            "payload_market": payload.get("market"),
            "skip_qty_revalidation": True,
            "payload_build_status": "built",
        }
    except Exception as exc:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=f"payload_build_failed:{type(exc).__name__}",
            recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
            submit_payload_preview={"payload_build_status": "failed", "error": str(exc)},
        )

    closeout_metrics = _metrics(
        raw_total_asset_qty=raw_qty,
        planned_sell_qty=planned_qty,
        market_price=market_price,
    )
    clean = bool(closeout_metrics["clean_account_after_sell"])
    if not clean:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
            recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
            submit_payload_preview=submit_payload_preview,
        )
    payload_volume = submit_payload_preview.get("payload_volume")
    if payload_volume is None or abs(float(payload_volume) - planned_qty) > _QTY_EPS:
        return _blocked_contract(
            market=market,
            dry_run=dry_run,
            broker_asset_available=broker_qty,
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=planned_qty,
            market_price=market_price,
            quantity_contract=quantity_contract,
            block_reason=REASON_FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
            recommended_action=RECOMMENDED_MANUAL_CLOSEOUT,
            submit_payload_preview=submit_payload_preview,
        )

    return OperatorCleanCloseoutContract(
        status="dry_run" if dry_run else "planned",
        command_intent=COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
        market=market,
        side="SELL",
        dry_run=bool(dry_run),
        broker_asset_available=broker_qty,
        raw_total_asset_qty=raw_qty,
        planned_sell_qty=planned_qty,
        estimated_residual_qty=float(closeout_metrics["estimated_residual_qty"]),
        estimated_residual_notional_krw=float(closeout_metrics["estimated_residual_notional_krw"]),
        clean_account_after_sell=True,
        quantity_authority=quantity_contract.as_dict(),
        closeout_allowed=True,
        block_reason=None,
        recommended_action=quantity_contract.quantity_contract_recommended_action,
        submit_payload_preview=submit_payload_preview,
    )


def validate_clean_closeout_contract_for_submit(
    contract: OperatorCleanCloseoutContract,
    *,
    submitted_qty: float,
    broker_asset_available: float,
) -> None:
    if not contract.closeout_allowed or not contract.clean_account_after_sell:
        raise ValueError("operator clean closeout contract is blocked")
    if contract.quantity_authority.get("qty_step_authority_level") == "unknown":
        raise ValueError("operator clean closeout quantity step authority unknown")
    if not bool(contract.quantity_authority.get("quantity_contract_complete")):
        raise ValueError("operator clean closeout quantity contract incomplete")
    if abs(float(contract.planned_sell_qty) - float(submitted_qty)) > _QTY_EPS:
        raise ValueError(
            "operator clean closeout submit qty mismatch: "
            f"planned={contract.planned_sell_qty:.12f} submitted={float(submitted_qty):.12f}"
        )
    if float(contract.estimated_residual_qty) > _QTY_EPS:
        raise ValueError("operator clean closeout contract would leave residual")
    if float(broker_asset_available) + _QTY_EPS < float(contract.planned_sell_qty):
        raise ValueError(
            "operator clean closeout broker balance changed before submit: "
            f"available={float(broker_asset_available):.12f} planned={contract.planned_sell_qty:.12f}"
        )
    payload_volume = contract.submit_payload_preview.get("payload_volume")
    if payload_volume is None or abs(float(payload_volume) - float(contract.planned_sell_qty)) > _QTY_EPS:
        raise ValueError("operator clean closeout payload volume does not match planned qty")
