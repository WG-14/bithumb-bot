from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .broker.live_submission_execution import submit_live_order_and_confirm
from .broker.live_submit_orchestrator import StandardSubmitPipelineRequest
from .broker.live_submit_planning import build_live_submit_plan
from .config import runtime_code_provenance, settings
from .execution_authority import execution_authority_from_payload, require_authority_operation
from .execution_order_rules import resolve_execution_order_rules
from .oms import OPEN_ORDER_STATUSES, build_client_order_id, build_order_intent_key, payload_fingerprint
from .operator_smoke_authority import (
    OPERATOR_SMOKE_MAX_NOTIONAL_KRW,
    load_operator_smoke_authority,
)
from .operator_smoke_preflight import validate_operator_smoke_preflight


SMOKE_BUY_CONFIRMATION_TOKEN = "LIVE_SMOKE_BUY_50000"
OPERATOR_SMOKE_STRATEGY_NAME = "operator_execution_smoke"
OPERATOR_SMOKE_ORIGIN = "operator_smoke"


class OperatorSmokeError(ValueError):
    pass


@dataclass(frozen=True)
class SmokeBuyPlan:
    market: str
    side: str
    krw: float
    strategy_name: str
    strategy_instance_id: str
    origin: str
    run_id: str

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "side": self.side,
            "krw": float(self.krw),
            "strategy_name": self.strategy_name,
            "strategy_instance_id": self.strategy_instance_id,
            "origin": self.origin,
            "run_id": self.run_id,
        }


def build_smoke_buy_plan(*, market: str, krw: float, run_id: str | None = None) -> SmokeBuyPlan:
    smoke_run_id = str(run_id or uuid.uuid4().hex[:12])
    return SmokeBuyPlan(
        market=str(market or "").strip().upper(),
        side="BUY",
        krw=float(krw),
        strategy_name=OPERATOR_SMOKE_STRATEGY_NAME,
        strategy_instance_id=f"{OPERATOR_SMOKE_STRATEGY_NAME}:{smoke_run_id}",
        origin=OPERATOR_SMOKE_ORIGIN,
        run_id=smoke_run_id,
    )


def validate_smoke_buy_request(
    *,
    mode: str,
    live_real_order_armed: bool,
    kill_switch: bool,
    krw: float,
    confirm: str | None,
) -> None:
    if str(mode or "").strip().lower() != "live":
        raise OperatorSmokeError("smoke_buy_requires_live_mode")
    if not bool(live_real_order_armed):
        raise OperatorSmokeError("smoke_buy_requires_live_real_order_armed")
    if bool(kill_switch):
        raise OperatorSmokeError("smoke_buy_blocked_by_kill_switch")
    if str(confirm or "") != SMOKE_BUY_CONFIRMATION_TOKEN:
        raise OperatorSmokeError("smoke_buy_requires_confirmation_token")
    if float(krw) <= 0.0:
        raise OperatorSmokeError("smoke_buy_krw_must_be_positive")
    if float(krw) > OPERATOR_SMOKE_MAX_NOTIONAL_KRW:
        raise OperatorSmokeError("smoke_buy_krw_above_50000_cap")


def _open_local_order_count(conn: Any) -> int:
    placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM orders WHERE status IN ({placeholders})",
        tuple(OPEN_ORDER_STATUSES),
    ).fetchone()
    return int(row["cnt"] if hasattr(row, "keys") else row[0])


def _broker_open_order_count(broker: Any) -> int:
    return len(list(broker.get_open_orders()))


def _format_balances(balance: Any) -> dict[str, float]:
    return {
        "cash_available": float(getattr(balance, "cash_available", 0.0) or 0.0),
        "cash_locked": float(getattr(balance, "cash_locked", 0.0) or 0.0),
        "asset_available": float(getattr(balance, "asset_available", 0.0) or 0.0),
        "asset_locked": float(getattr(balance, "asset_locked", 0.0) or 0.0),
    }


def execute_smoke_buy(
    *,
    conn: Any,
    broker: Any,
    krw: float,
    market: str,
    confirm: str,
    authority_path: str | None = None,
    reference_price: float | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    validate_smoke_buy_request(
        mode=str(settings.MODE),
        live_real_order_armed=bool(settings.LIVE_REAL_ORDER_ARMED),
        kill_switch=bool(settings.KILL_SWITCH),
        krw=float(krw),
        confirm=confirm,
    )
    if bool(settings.LIVE_DRY_RUN):
        raise OperatorSmokeError("smoke_buy_rejects_live_dry_run")
    if str(market or "").strip().upper() != str(settings.PAIR or "").strip().upper():
        raise OperatorSmokeError("smoke_buy_market_mismatch_with_settings_pair")
    if authority_path is None or not str(authority_path).strip():
        raise OperatorSmokeError("smoke_buy_requires_authority_path")
    validate_operator_smoke_preflight(
        cfg=settings,
        conn=conn,
        market=str(market),
    )
    code_commit_sha = str(runtime_code_provenance().get("commit_sha") or "unavailable")
    authority = load_operator_smoke_authority(authority_path)
    command_authority = execution_authority_from_payload(authority.payload)
    require_authority_operation(command_authority, "operator_smoke_buy")
    authority.verify(
        now=datetime.now(timezone.utc),
        side="BUY",
        notional_krw=float(krw),
        market=str(market),
        db_path=str(settings.DB_PATH),
        account_key=str(settings.BITHUMB_API_KEY),
        code_commit_sha=code_commit_sha,
    )

    if _open_local_order_count(conn) > 0:
        raise OperatorSmokeError("smoke_buy_blocked_by_unresolved_local_orders")
    if _broker_open_order_count(broker) > 0:
        raise OperatorSmokeError("smoke_buy_blocked_by_open_broker_orders")

    before_balance = _format_balances(broker.get_balance())
    ts = int(now_ms if now_ms is not None else time.time() * 1000)
    plan_identity = build_smoke_buy_plan(market=market, krw=float(krw))
    client_order_id = build_client_order_id(
        mode="live",
        side="buy",
        intent_ts=ts,
        submit_attempt_id=plan_identity.run_id,
    )
    submit_attempt_id = f"{client_order_id}:submit:{plan_identity.run_id}"
    rules = resolve_execution_order_rules(market=market).as_order_rules()
    min_notional = float(rules.get("min_notional_krw") or 0.0)
    if min_notional > 0.0 and float(krw) < min_notional:
        raise OperatorSmokeError("smoke_buy_below_min_notional")
    if reference_price is None:
        raise OperatorSmokeError("smoke_buy_reference_price_required")
    reference_price = float(reference_price)
    if not math.isfinite(reference_price) or reference_price <= 0.0:
        raise OperatorSmokeError("smoke_buy_reference_price_must_be_positive_finite")
    order_qty = float(krw) / reference_price
    intent_key = build_order_intent_key(
        symbol=market,
        side="BUY",
        strategy_context=plan_identity.strategy_instance_id,
        intent_ts=ts,
        intent_type=OPERATOR_SMOKE_ORIGIN,
        qty=float(order_qty),
    )
    submit_plan = build_live_submit_plan(
        broker=broker,
        client_order_id=client_order_id,
        side="BUY",
        qty=float(order_qty),
        ts=ts,
        effective_rules=type("SmokeRules", (), rules)(),
        reference_price=float(reference_price),
        market=str(market),
    )
    request = StandardSubmitPipelineRequest(
        conn=conn,
        submit_plan=submit_plan,
        signal="BUY",
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side="BUY",
        order_qty=float(order_qty),
        position_qty=0.0,
        qty=float(submit_plan.submitted_qty),
        ts=ts,
        intent_key=intent_key,
        market_price=float(reference_price),
        raw_total_asset_qty=0.0,
        open_exposure_qty=0.0,
        dust_tracking_qty=0.0,
        effective_rules=submit_plan.rules,
        submit_qty_source=str(submit_plan.submit_qty_authority),
        position_state_source="operator_smoke_preflight",
        reference_price=float(reference_price),
        top_of_book_summary=None,
        strategy_name=plan_identity.strategy_name,
        strategy_instance_id=plan_identity.strategy_instance_id,
        decision_id=None,
        decision_reason=OPERATOR_SMOKE_ORIGIN,
        exit_rule_name=None,
        order_type=str(submit_plan.exchange_order_type),
        contract_profile=str(settings.LIVE_SUBMIT_CONTRACT_PROFILE),
        payload_hash=payload_fingerprint({"client_order_id": client_order_id, "origin": OPERATOR_SMOKE_ORIGIN}),
        internal_lot_size=float(submit_plan.internal_lot_qty),
        effective_min_trade_qty=float(rules.get("min_qty") or 0.0),
        qty_step=float(rules.get("qty_step") or 0.0),
        min_notional_krw=min_notional,
        intended_lot_count=int(getattr(submit_plan.qty_split, "lot_count", 0) or 0),
        executable_lot_count=int(getattr(submit_plan.qty_split, "lot_count", 0) or 0),
        final_intended_qty=float(order_qty),
        final_submitted_qty=float(submit_plan.submitted_qty),
        decision_reason_code=OPERATOR_SMOKE_ORIGIN,
        submit_truth_source_fields={"origin": OPERATOR_SMOKE_ORIGIN},
        submit_observability_fields=plan_identity.as_dict(),
        sell_observability={},
    )
    authority.consume(
        consumed_at=datetime.now(timezone.utc),
        side="BUY",
        notional_krw=float(krw),
        market=str(market),
        db_path=str(settings.DB_PATH),
        account_key=str(settings.BITHUMB_API_KEY),
        code_commit_sha=code_commit_sha,
    )
    submission = submit_live_order_and_confirm(
        broker=broker,
        request=request,
        intent_key=intent_key,
        strategy_name=plan_identity.strategy_name,
        decision_id=None,
        decision_reason=OPERATOR_SMOKE_ORIGIN,
        exit_rule_name=None,
    )
    after_balance = _format_balances(broker.get_balance())
    payload = {
        "status": "submitted" if submission is not None else "blocked_or_no_submission",
        "identity": plan_identity.as_dict(),
        "client_order_id": client_order_id,
        "before_balance": before_balance,
        "after_balance": after_balance,
        "diagnostics": {
            "krw": float(krw),
            "market": market,
            "origin": OPERATOR_SMOKE_ORIGIN,
            "promotion_evidence": False,
            "approved_profile_evidence": False,
        },
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return payload
