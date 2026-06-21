from __future__ import annotations

import json
import math
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Callable

from .config import runtime_code_provenance, settings
from .db_core import ensure_db, record_strategy_decision
from .decision_equivalence import sha256_prefixed
from .execution_order_rules import ExecutionOrderRules, resolve_execution_order_rules
from .execution_service import ExecutionDecisionSummary, ExecutionSubmitPlan, build_signal_execution_service
from .quantity_contracts import build_quantity_semantics
from .live_pipeline_smoke_authority import (
    LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
    LIVE_PIPELINE_SMOKE_CYCLES,
    LIVE_PIPELINE_SMOKE_DEFAULT_MAX_NOTIONAL_KRW,
    LIVE_PIPELINE_SMOKE_MAX_ORDERS,
    build_live_pipeline_smoke_plan_payload,
    load_live_pipeline_smoke_authority,
)
from .live_pipeline_smoke_preflight import (
    LivePipelineSmokePreflightError,
    LivePipelineSmokeReadiness,
    readiness_from_snapshot,
    validate_live_pipeline_smoke_start_preflight,
    validate_live_pipeline_smoke_step_readiness,
)
from .runtime_readiness import compute_runtime_readiness_snapshot
from .runtime_data_access import select_latest_closed_candle
from .runtime.execution_coordinator import ExecutionCoordinator, build_signal_execution_request
from .runtime.live_order_settlement import LiveOrderSettlementWrapper
from .runtime.live_pipeline_smoke_decision import (
    OPERATOR_LIVE_PIPELINE_SMOKE_STRATEGY_NAME,
    LivePipelineSmokeDecisionProvider,
)
from .utils_time import parse_interval_sec


class LivePipelineSmokeError(ValueError):
    pass


@dataclass(frozen=True)
class MarketReference:
    price: float
    source: str
    ts: int | None = None
    bid_price: float | None = None
    ask_price: float | None = None


@dataclass
class LivePipelineSmokeExecutionService:
    broker: Any
    fail_at_step: int | None = None
    submissions: list[dict[str, Any]] = field(default_factory=list)

    def execute(self, request: Any) -> dict[str, Any] | None:
        if self.fail_at_step is not None and len(self.submissions) == int(self.fail_at_step):
            raise RuntimeError("fake_broker_submit_failure")
        summary = request.execution_decision_summary
        plan = summary.typed_target_submit_plan() if isinstance(summary, ExecutionDecisionSummary) else None
        if plan is None:
            return None
        side = str(plan.side).upper()
        qty = float(plan.qty or 0.0)
        client_order_id = f"lps_{int(request.ts)}_{side.lower()}_{len(self.submissions) + 1}"
        exchange_order_id = f"ex_lps_{len(self.submissions) + 1}"
        if hasattr(self.broker, "apply_fill"):
            self.broker.apply_fill(side=side, qty=qty)
        submission = {
            "status": "submitted",
            "client_order_id": client_order_id,
            "exchange_order_id": exchange_order_id,
            "side": side,
            "filled_qty": qty,
            "submit_qty": qty,
            "decision_id": request.decision_id,
        }
        self.submissions.append(submission)
        return submission


def validate_live_pipeline_smoke_request(
    *,
    apply: bool,
    yes: bool,
    cycles: int,
    max_orders: int,
    max_notional_krw: float,
    authority_path: str | None,
    confirm: str | None,
    mode: str | None = None,
) -> None:
    live = str(mode if mode is not None else settings.MODE).strip().lower() == "live"
    if live and int(cycles) != LIVE_PIPELINE_SMOKE_CYCLES:
        raise LivePipelineSmokeError("live_pipeline_smoke_live_cycles_must_be_5")
    if int(max_orders) != int(cycles) * 2:
        raise LivePipelineSmokeError("live_pipeline_smoke_max_orders_must_equal_cycles_x2")
    if live and int(max_orders) > LIVE_PIPELINE_SMOKE_MAX_ORDERS:
        raise LivePipelineSmokeError("live_pipeline_smoke_live_max_orders_above_10")
    if float(max_notional_krw) <= 0.0:
        raise LivePipelineSmokeError("live_pipeline_smoke_max_notional_must_be_positive")
    if apply:
        if not yes:
            raise LivePipelineSmokeError("live_pipeline_smoke_apply_requires_yes")
        if not str(authority_path or "").strip():
            raise LivePipelineSmokeError("live_pipeline_smoke_apply_requires_authority_path")
        if str(confirm or "") != LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN:
            raise LivePipelineSmokeError("live_pipeline_smoke_apply_requires_confirmation_token")


def build_live_pipeline_smoke_plan(
    *,
    cycles: int,
    max_orders: int,
    max_notional_krw: float,
    market: str | None = None,
) -> dict[str, Any]:
    validate_live_pipeline_smoke_request(
        apply=False,
        yes=False,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
        authority_path=None,
        confirm=None,
    )
    return build_live_pipeline_smoke_plan_payload(
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
        market=str(market or settings.PAIR),
    )


def _readiness_from_broker(broker: Any) -> LivePipelineSmokeReadiness:
    qty = float(getattr(broker, "qty", 0.0) if hasattr(broker, "qty") else 0.0)
    return LivePipelineSmokeReadiness(
        broker_qty=qty,
        portfolio_qty=qty,
        projected_total_qty=qty,
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
        fee_pending_count=0,
        active_fee_accounting_blocker=False,
        broker_qty_known=True,
        balance_source_stale=False,
        projection_converged=True,
    )


def _positive_finite(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0.0:
        return None
    return parsed


def _row_value(row: Any, key: str, index: int) -> Any:
    if row is None:
        return None
    if hasattr(row, "keys"):
        return row[key]
    return row[index]


def _is_closed_candle(*, candle_ts_ms: int, now_ms: int, interval_sec: int) -> bool:
    interval_ms = max(1, int(interval_sec)) * 1000
    close_guard_ms = max(2_000, min(30_000, interval_ms // 20))
    return int(now_ms) >= int(candle_ts_ms) + interval_ms + close_guard_ms


def _resolve_market_reference(conn: Any, *, market: str, now_ms: int) -> MarketReference:
    top_row = conn.execute(
        """
        SELECT ts, bid_price, ask_price
        FROM orderbook_top_snapshots
        WHERE pair=?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (market,),
    ).fetchone()
    if top_row is not None:
        bid = _positive_finite(_row_value(top_row, "bid_price", 1))
        ask = _positive_finite(_row_value(top_row, "ask_price", 2))
        if bid is not None and ask is not None and ask >= bid:
            return MarketReference(
                price=(bid + ask) / 2.0,
                source="orderbook_top_mid",
                ts=int(_row_value(top_row, "ts", 0)),
                bid_price=bid,
                ask_price=ask,
            )

    interval = str(getattr(settings, "INTERVAL", "1m") or "1m")
    interval_sec = parse_interval_sec(interval)
    candle_row, _incomplete_ts = select_latest_closed_candle(
        conn,
        pair=market,
        interval=interval,
        interval_sec=interval_sec,
        now_ms=now_ms,
        is_closed_candle=_is_closed_candle,
    )
    if candle_row is not None:
        close = _positive_finite(_row_value(candle_row, "close", 1))
        if close is not None:
            return MarketReference(
                price=close,
                source="latest_closed_candle",
                ts=int(_row_value(candle_row, "ts", 0)),
            )
    raise LivePipelineSmokePreflightError("live_pipeline_smoke_market_reference_unavailable")


def _validate_smoke_order_rules(
    *,
    rules: ExecutionOrderRules,
    market: str,
    settings_pair: str,
    side: str,
    qty: float,
    notional_krw: float,
) -> None:
    if str(market or "").strip().upper() != str(settings_pair or "").strip().upper():
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_market_mismatch_with_settings_pair")
    if str(rules.market or "").strip().upper() != str(market or "").strip().upper():
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_order_rules_market_mismatch")
    if rules.min_qty is None or rules.min_notional_krw is None:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_order_rules_missing_required_fields")
    if bool(rules.stale):
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_order_rules_stale")
    if _positive_finite(qty) is None:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_non_positive_qty")
    if _positive_finite(notional_krw) is None:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_non_positive_notional")
    if float(qty) + 1e-12 < float(rules.min_qty):
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_qty_below_min_qty")
    side_upper = str(side or "").upper()
    side_min_total = rules.bid_min_total_krw if side_upper == "BUY" else rules.ask_min_total_krw
    min_notional = max(float(rules.min_notional_krw), float(side_min_total or 0.0))
    if float(notional_krw) + 1e-9 < min_notional:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_notional_below_min_notional")


def _validate_smoke_roundtrip_notional_buffer(
    *,
    rules: ExecutionOrderRules,
    reference_price: float,
    max_notional_krw: float,
    safety_buffer: float = 1.05,
) -> None:
    if rules.min_qty is None or rules.min_notional_krw is None:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_order_rules_missing_required_fields")
    reference = _positive_finite(reference_price)
    if reference is None:
        raise LivePipelineSmokePreflightError("live_pipeline_smoke_market_reference_unavailable")
    min_qty_notional = float(reference) * float(rules.min_qty) * float(safety_buffer)
    required_roundtrip_notional = max(float(rules.min_notional_krw), min_qty_notional)
    if float(max_notional_krw) + 1e-9 < required_roundtrip_notional:
        raise LivePipelineSmokePreflightError(
            "live_pipeline_smoke_max_notional_below_sellable_roundtrip_minimum"
        )


_SMOKE_REPAIR_TABLES = (
    "manual_flat_accounting_repairs",
    "fee_pending_accounting_repairs",
    "fee_gap_accounting_repairs",
    "position_authority_repairs",
    "external_position_adjustments",
)


def _table_count(conn: Any, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()
    return int(row["cnt"] if hasattr(row, "keys") else row[0])


def _repair_event_counters(conn: Any) -> dict[str, int]:
    return {table: _table_count(conn, table) for table in _SMOKE_REPAIR_TABLES}


def _repair_event_delta(before: Mapping[str, int], after: Mapping[str, int]) -> dict[str, int]:
    return {
        key: max(0, int(after.get(key, 0)) - int(before.get(key, 0)))
        for key in _SMOKE_REPAIR_TABLES
    }


def _repair_event_delta_total(delta: Mapping[str, int]) -> int:
    return sum(int(value or 0) for value in delta.values())


def _smoke_risk_policy_hash() -> str:
    return sha256_prefixed({"risk_policy": "operator_live_pipeline_smoke_minimal_allow"})


def _smoke_strategy_risk_profile_hash() -> str:
    return sha256_prefixed({"strategy": OPERATOR_LIVE_PIPELINE_SMOKE_STRATEGY_NAME})


def _smoke_pre_submit_risk_proof(*, submit_plan_hash: str) -> dict[str, object]:
    proof_input = {
        "authority": "operator_live_pipeline_smoke",
        "submit_plan_hash": str(submit_plan_hash),
    }
    policy_hash = _smoke_risk_policy_hash()
    profile_hash = _smoke_strategy_risk_profile_hash()
    return {
        "pre_submit_risk_required": True,
        "pre_submit_risk_decision": {
            "evaluation_point": "pre_submit",
            "status": "ALLOW",
            "reason_code": "OPERATOR_LIVE_PIPELINE_SMOKE_AUTHORIZED",
            "reason": "operator authorized bounded live pipeline smoke",
            "allowed_actions": ["submit"],
        },
        "pre_submit_risk_status": "ALLOW",
        "pre_submit_risk_decision_hash": sha256_prefixed({**proof_input, "decision": "ALLOW"}),
        "pre_submit_risk_policy_hash": policy_hash,
        "effective_pre_submit_risk_policy_hash": policy_hash,
        "pre_submit_risk_input_hash": sha256_prefixed({**proof_input, "input": "smoke"}),
        "pre_submit_risk_evidence_hash": sha256_prefixed({**proof_input, "evidence": "smoke"}),
        "pre_submit_risk_plan_hash": str(submit_plan_hash),
        "pre_submit_risk_reason_code": "OPERATOR_LIVE_PIPELINE_SMOKE_AUTHORIZED",
        "pre_submit_risk_state_source": "operator_live_pipeline_smoke_preflight",
        "risk_policy_source": "operator_live_pipeline_smoke_authority",
        "pre_submit_risk_policy_composition_rule": "operator_bounded_smoke_only",
        "strategy_risk_profile_hashes": [profile_hash],
    }


def _summary_for_step(
    *,
    side: str,
    target_exposure_krw: float,
    current_exposure_krw: float,
    qty: float,
    notional_krw: float,
    market: str,
    market_reference: MarketReference,
    order_rules: ExecutionOrderRules,
    context: dict[str, object],
) -> ExecutionDecisionSummary:
    order_rule_payload = order_rules.as_order_rules()
    policy_hash = _smoke_risk_policy_hash()
    profile_hash = _smoke_strategy_risk_profile_hash()
    extra_payload = {
        "portfolio_target_authoritative": True,
        "portfolio_target_hash": "sha256:live_pipeline_smoke_portfolio_target",
        "allocation_decision_hash": "sha256:live_pipeline_smoke_allocation",
        "allocator_config_hash": "sha256:live_pipeline_smoke_allocator",
        "strategy_contribution_hash": "sha256:live_pipeline_smoke_contribution",
        "runtime_pair": str(market),
        "authoritative_pair": str(market),
        "operator_live_pipeline_smoke": True,
        "operator_authorization": "live_pipeline_smoke_authority",
        "execution_mode": "live_pipeline_smoke",
        "candle_checkpoint_authority": "smoke_step_checkpoint",
        "market_reference_source": market_reference.source,
        "market_reference_price": float(market_reference.price),
        "market_reference_ts": market_reference.ts,
        "market_reference_bid_price": market_reference.bid_price,
        "market_reference_ask_price": market_reference.ask_price,
        "normal_h74_strategy_performance_authority": False,
        "normal_strategy_gate_modified": False,
        "strategy_performance_gate": {
            "enabled": True,
            "allowed": False,
            "blocked": True,
            "reason_code": "operator_live_pipeline_smoke_bypasses_strategy_performance_gate",
            "reason": "operator smoke is not ordinary strategy performance authority",
            "scope": "operator_live_pipeline_smoke_only",
        },
        "strategy_performance_gate_status": "blocked",
        "strategy_performance_gate_blocked": True,
        "strategy_performance_gate_enforced": False,
        "strategy_performance_gate_would_block_if_armed": True,
        "strategy_performance_gate_reason_code": (
            "operator_live_pipeline_smoke_bypasses_strategy_performance_gate"
        ),
        "strategy_performance_gate_reason": (
            "operator smoke is not ordinary strategy performance authority"
        ),
        "effective_pre_submit_risk_policy_hash": policy_hash,
        "risk_policy_source": "operator_live_pipeline_smoke_authority",
        "strategy_risk_profile_hashes": [profile_hash],
        **order_rule_payload,
    }
    base_plan = ExecutionSubmitPlan(
        side=str(side).upper(),
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=float(qty),
        notional_krw=float(notional_krw),
        target_exposure_krw=float(target_exposure_krw),
        current_effective_exposure_krw=float(current_exposure_krw),
        delta_krw=float(notional_krw),
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key=f"{context['run_id']}:{context['step_index']}:{side}",
        pair=str(market),
        scope_key_hash="sha256:live_pipeline_smoke_scope",
        portfolio_target_hash="sha256:live_pipeline_smoke_portfolio_target",
        submit_authority_policy_hash="sha256:live_pipeline_smoke_submit_policy",
        extra_payload=extra_payload,
    )
    plan = replace(
        base_plan,
        extra_payload={
            **extra_payload,
            **_smoke_pre_submit_risk_proof(submit_plan_hash=base_plan.content_hash()),
        },
    )
    return ExecutionDecisionSummary(
        raw_signal=str(side).upper(),
        final_signal=str(side).upper(),
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=float(target_exposure_krw),
        current_effective_exposure_krw=float(current_exposure_krw),
        tracked_residual_exposure_krw=None,
        buy_delta_krw=float(notional_krw) if str(side).upper() == "BUY" else None,
        residual_live_sell_mode="telemetry",
        residual_buy_sizing_mode="telemetry",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision={
            "target_new_exposure_krw": float(target_exposure_krw),
            "target_current_exposure_krw": float(current_exposure_krw),
            "target_delta_notional_krw": float(notional_krw),
            "target_delta_side": str(side).upper(),
            "target_qty": float(qty if side == "BUY" else 0.0),
            "target_reference_price": float(market_reference.price),
            "market_reference_source": market_reference.source,
            "target_origin": "operator_live_pipeline_smoke",
            "target_adoption_reason": "operator_authorized_pipeline_smoke",
        },
        target_submit_plan=plan,
        signal_flow={"primary_block_layer": "none", "primary_block_reason": "none"},
    )


def _record_smoke_decision(
    conn: Any,
    *,
    ts: int,
    side: str,
    market_price: float,
    context: dict[str, object],
) -> int:
    return record_strategy_decision(
        conn,
        decision_ts=ts,
        strategy_name=OPERATOR_LIVE_PIPELINE_SMOKE_STRATEGY_NAME,
        signal=str(side).upper(),
        reason="operator_authorized_pipeline_smoke",
        candle_ts=ts,
        market_price=float(market_price),
        confidence=1.0,
        context=context,
        strategy_decision_projection_type="operator_live_pipeline_smoke",
        strategy_decisions_authority="operator_authorized_pipeline_smoke",
    )


def run_live_pipeline_smoke(
    *,
    conn: Any,
    broker: Any,
    cycles: int = LIVE_PIPELINE_SMOKE_CYCLES,
    max_orders: int = LIVE_PIPELINE_SMOKE_MAX_ORDERS,
    max_notional_krw: float = LIVE_PIPELINE_SMOKE_DEFAULT_MAX_NOTIONAL_KRW,
    yes: bool = False,
    authority_path: str | None = None,
    confirm: str | None = None,
    execution_service: Any | None = None,
    readiness_provider: Callable[[], LivePipelineSmokeReadiness] | None = None,
    post_trade_reconcile: Callable[[], Any] | None = None,
    settlement_coordinator: Callable[[Mapping[str, Any]], Any] | None = None,
    run_id: str | None = None,
    market: str | None = None,
) -> dict[str, Any]:
    validate_live_pipeline_smoke_request(
        apply=True,
        yes=yes,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
        authority_path=authority_path,
        confirm=confirm,
    )
    market = str(market or settings.PAIR).strip().upper()
    code_commit_sha = str(runtime_code_provenance().get("commit_sha") or "unavailable")
    authority = load_live_pipeline_smoke_authority(str(authority_path))
    authority.verify(
        now=datetime.now(timezone.utc),
        market=market,
        db_path=str(settings.DB_PATH),
        account_key=str(settings.BITHUMB_API_KEY),
        code_commit_sha=code_commit_sha,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
    )
    smoke_run_id = str(run_id or f"lps_{uuid.uuid4().hex[:12]}")
    try:
        validate_live_pipeline_smoke_start_preflight(
            cfg=settings,
            conn=conn,
            broker=broker,
            market=market,
        )
        authority.consume(
            consumed_at=datetime.now(timezone.utc),
            market=market,
            db_path=str(settings.DB_PATH),
            account_key=str(settings.BITHUMB_API_KEY),
            code_commit_sha=code_commit_sha,
            cycles=cycles,
            max_orders=max_orders,
            max_notional_krw=max_notional_krw,
            run_id=smoke_run_id,
        )
    except Exception as exc:
        return _failure_payload(
            run_id=smoke_run_id,
            reason=str(exc),
            step=0,
            round_index=1,
            orders_submitted=0,
        )

    provider = LivePipelineSmokeDecisionProvider(
        run_id=smoke_run_id,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
    )
    coordinator = ExecutionCoordinator(execution_engine_name="target_delta")
    service = execution_service or build_signal_execution_service(mode="live", broker=broker)
    if service is None:
        raise LivePipelineSmokeError("live_pipeline_smoke_execution_service_unavailable")
    readiness = readiness_provider or (lambda: readiness_from_snapshot(compute_runtime_readiness_snapshot(conn)))
    reconcile = post_trade_reconcile or (lambda: None)
    live_settlement = settlement_coordinator or LiveOrderSettlementWrapper(
        broker=broker,
        db_factory=lambda: ensure_db(str(settings.DB_PATH)),
        reconcile_with_broker=lambda _broker: reconcile(),
    )
    rounds: list[dict[str, Any]] = []
    flat_round: dict[str, Any] = {}
    orders_submitted = 0
    buy_submitted = 0
    sell_submitted = 0
    manual_intervention_required = False
    market_reference_sources: set[str] = set()
    repair_counters_before = _repair_event_counters(conn)

    for step in range(max_orders):
        try:
            current = readiness()
            side = provider.next_side(current)
            if side == "STOP":
                break
            validate_live_pipeline_smoke_step_readiness(
                current,
                expected_side=side,
                requested_qty=(float(current.broker_qty) if side == "SELL" else None),
                terminal_flat_authority=(side == "SELL"),
            )
            ts = int(time.time() * 1000) + step
            market_reference = _resolve_market_reference(conn, market=market, now_ms=ts)
        except Exception as exc:
            return _failure_payload(
                run_id=smoke_run_id,
                reason=str(exc),
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted,
            )
        context = provider.context_for_step(side=side)
        context["market_reference_source"] = market_reference.source
        context["market_reference_price"] = float(market_reference.price)
        context["market_reference_ts"] = market_reference.ts
        market_reference_sources.add(market_reference.source)
        current_exposure = float(current.broker_qty) * float(market_reference.price)
        target_exposure = provider.target_exposure_krw_for_side(side)
        qty = (
            float(max_notional_krw) / float(market_reference.price)
            if side == "BUY"
            else float(current.broker_qty)
        )
        notional = float(max_notional_krw) if side == "BUY" else max(0.0, float(current_exposure))
        try:
            order_rules = resolve_execution_order_rules(market=market)
            if side == "BUY":
                _validate_smoke_roundtrip_notional_buffer(
                    rules=order_rules,
                    reference_price=float(market_reference.price),
                    max_notional_krw=float(max_notional_krw),
                )
            _validate_smoke_order_rules(
                rules=order_rules,
                market=market,
                settings_pair=str(settings.PAIR),
                side=side,
                qty=qty,
                notional_krw=notional,
            )
        except Exception as exc:
            return _failure_payload(
                run_id=smoke_run_id,
                reason=str(exc),
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted,
            )
        summary = _summary_for_step(
            side=side,
            target_exposure_krw=target_exposure,
            current_exposure_krw=current_exposure,
            qty=qty,
            notional_krw=notional,
            market=market,
            market_reference=market_reference,
            order_rules=order_rules,
            context=context,
        )
        context["execution_decision"] = summary.as_dict()
        decision_id = _record_smoke_decision(
            conn,
            ts=ts,
            side=side,
            market_price=market_reference.price,
            context=context,
        )
        conn.commit()
        def _submit_invoker(
            *,
            _side: str = side,
            _ts: int = ts,
            _decision_id: int = decision_id,
            _summary: ExecutionDecisionSummary = summary,
            _context: dict[str, object] = context,
            _market_reference: MarketReference = market_reference,
        ) -> Any:
            return service.execute(
                build_signal_execution_request(
                    signal=_side,
                    ts=_ts,
                    market_price=float(_market_reference.price),
                    strategy_name=OPERATOR_LIVE_PIPELINE_SMOKE_STRATEGY_NAME,
                    decision_id=_decision_id,
                    decision_reason="operator_authorized_pipeline_smoke",
                    exit_rule_name=None,
                    execution_decision_summary=_summary,
                    decision_context=_context,
                    execution_plan_bundle=None,
                )
            )

        quantity_semantics = build_quantity_semantics(
            broker_position_qty=(float(current.broker_qty) if side == "SELL" else float(qty)),
            exchange_min_qty=float(order_rules.min_qty or 0.0),
            strategy_internal_lot_size=(
                float(getattr(settings, "LIVE_INTERNAL_LOT_SIZE", 0.0) or 0.0) or None
            ),
            target_delta_closeout_authorized=(side == "SELL"),
            terminal_closeout_covered_qty=(float(current.broker_qty) if side == "SELL" else float(qty)),
        ).as_dict()

        result = coordinator.execute_cycle(
            candle_ts=ts,
            decision_id=decision_id,
            signal=side,
            market_price=float(market_reference.price),
            strategy_name=OPERATOR_LIVE_PIPELINE_SMOKE_STRATEGY_NAME,
            decision_reason="operator_authorized_pipeline_smoke",
            decision_context=context,
            execution_decision_summary=summary,
            execution_plan_bundle=None,
            execution_service=None,
            submit_invoker=_submit_invoker,
            post_trade_reconcile=None,
            settlement_coordinator=live_settlement,
            settlement_required=True,
            input_hash=None,
        )
        trade = dict(result.trade or {}) if isinstance(result.trade, Mapping) else {}
        try:
            filled_qty = float(trade.get("filled_qty") or 0.0)
        except (TypeError, ValueError):
            filled_qty = 0.0
        if (
            not result.submitted
            or result.halt_transition
            or not trade
            or not str(trade.get("client_order_id") or "").strip()
            or str(trade.get("side") or "").upper() != str(side).upper()
            or filled_qty <= 0.0
        ):
            return _failure_payload(
                run_id=smoke_run_id,
                reason=result.planning_status,
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted,
            )
        settlement_result = dict(result.settlement_result or {})
        manual_intervention_required = manual_intervention_required or bool(
            settlement_result.get("operator_action_required")
        )
        if not settlement_result or not bool(settlement_result.get("settled")):
            return _failure_payload(
                run_id=smoke_run_id,
                reason=str(settlement_result.get("reason_code") or "live_pipeline_smoke_order_not_settled"),
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted + 1,
                settlement_result=settlement_result,
                failed_trade=trade,
                failed_side=side,
            )
        after = readiness()
        try:
            next_side = "SELL" if side == "BUY" else "BUY"
            validate_live_pipeline_smoke_step_readiness(
                after,
                expected_side=next_side,
                requested_qty=(float(after.broker_qty) if next_side == "SELL" else None),
                terminal_flat_authority=(next_side == "SELL"),
            )
        except LivePipelineSmokePreflightError as exc:
            return _failure_payload(
                run_id=smoke_run_id,
                reason=str(exc),
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted + 1,
            )
        evidence = {
            "decision_id": decision_id,
            "client_order_id": trade.get("client_order_id"),
            "exchange_order_id": trade.get("exchange_order_id"),
            "submitted": True,
            "post_trade_reconciled": bool(result.post_trade_reconciled),
            "settlement_status": settlement_result.get("reason_code"),
            "settlement_result": settlement_result,
            "fee_state": settlement_result.get("fee_state"),
            "projection_converged_after_settlement": bool(after.projection_converged),
            "broker_qty_after": float(after.broker_qty),
            "portfolio_qty_after": float(after.portfolio_qty),
            "projected_total_qty_after": float(after.projected_total_qty),
            "repair_events_created_during_step": _repair_event_delta_total(
                _repair_event_delta(repair_counters_before, _repair_event_counters(conn))
            ),
            "manual_intervention_required": bool(settlement_result.get("operator_action_required")),
            "quantity_semantics": quantity_semantics,
            "filled_qty": filled_qty,
            "market_reference_source": market_reference.source,
            "market_reference_price": float(market_reference.price),
        }
        if not bool(settlement_result.get("settled")):
            return _failure_payload(
                run_id=smoke_run_id,
                reason="live_pipeline_smoke_order_not_settled",
                step=step,
                round_index=(step // 2) + 1,
                orders_submitted=orders_submitted + 1,
                settlement_result=settlement_result,
                failed_trade=trade,
                failed_side=side,
            )
        orders_submitted += 1
        if side == "BUY":
            if not after.in_position:
                return _failure_payload(run_id=smoke_run_id, reason="live_pipeline_smoke_buy_did_not_create_position", step=step, round_index=(step // 2) + 1, orders_submitted=orders_submitted)
            buy_submitted += 1
            flat_round = {"round": (step // 2) + 1, "buy": evidence}
        else:
            if not after.flat:
                return _failure_payload(run_id=smoke_run_id, reason="live_pipeline_smoke_sell_did_not_end_flat", step=step, round_index=(step // 2) + 1, orders_submitted=orders_submitted)
            sell_submitted += 1
            evidence["flat_after_sell"] = True
            flat_round["sell"] = evidence
            rounds.append(flat_round)
            flat_round = {}
        if bool(settlement_result.get("settled")):
            provider.mark_step_complete()

    final = readiness()
    repair_counters_after = _repair_event_counters(conn)
    repair_delta = _repair_event_delta(repair_counters_before, repair_counters_after)
    repair_events_created = _repair_event_delta_total(repair_delta)
    final_flat = bool(
        final.flat
        and abs(float(final.broker_qty)) <= 1e-12
        and abs(float(final.portfolio_qty)) <= 1e-12
        and abs(float(final.projected_total_qty)) <= 1e-12
    )
    if (
        orders_submitted != max_orders
        or buy_submitted != cycles
        or sell_submitted != cycles
        or not final_flat
        or repair_events_created != 0
        or manual_intervention_required
    ):
        return _failure_payload(
            run_id=smoke_run_id,
            reason="live_pipeline_smoke_final_completion_criteria_failed",
            step=provider.step_index,
            round_index=(provider.step_index // 2) + 1,
            orders_submitted=orders_submitted,
        )
    return {
        "status": "passed",
        "execution_mode": "live_pipeline_smoke",
        "run_id": smoke_run_id,
        "cycles_requested": int(cycles),
        "orders_expected": int(max_orders),
        "orders_submitted": int(orders_submitted),
        "buy_submitted": int(buy_submitted),
        "sell_submitted": int(sell_submitted),
        "repair_events_created_during_run": int(repair_events_created),
        "manual_intervention_required": bool(manual_intervention_required),
        "repair_event_delta": repair_delta,
        "rounds": rounds,
        "final": {
            "broker_qty": float(final.broker_qty),
            "portfolio_qty": float(final.portfolio_qty),
            "projected_total_qty": float(final.projected_total_qty),
            "open_order_count": int(final.open_order_count),
            "submit_unknown_count": int(final.submit_unknown_count),
            "recovery_required_count": int(final.recovery_required_count),
        },
        "execution_mode_metadata": {
            "execution_mode": "live_pipeline_smoke",
            "candle_checkpoint_authority": "smoke_step_checkpoint",
            "market_reference_source": (
                next(iter(market_reference_sources))
                if len(market_reference_sources) == 1
                else "mixed"
            ),
            "market_reference_sources": sorted(market_reference_sources),
            "normal_h74_strategy_performance_authority": False,
        },
    }


def _failure_payload(
    *,
    run_id: str,
    reason: str,
    step: int,
    round_index: int,
    orders_submitted: int,
    settlement_result: Mapping[str, Any] | None = None,
    failed_trade: Mapping[str, Any] | None = None,
    failed_side: str | None = None,
) -> dict[str, Any]:
    payload = {
        "status": "failed",
        "execution_mode": "live_pipeline_smoke",
        "run_id": run_id,
        "reason": str(reason),
        "failed_step": int(step),
        "failed_round": int(round_index),
        "orders_submitted": int(orders_submitted),
        "next_operator_action": "inspect health/audit and use flatten-position if exposure remains",
    }
    if settlement_result is not None:
        settlement_payload = dict(settlement_result)
        payload["settlement_result"] = settlement_payload
        payload["failed_client_order_id"] = (
            str((failed_trade or {}).get("client_order_id") or settlement_payload.get("client_order_id") or "")
            or None
        )
        payload["failed_exchange_order_id"] = (
            str((failed_trade or {}).get("exchange_order_id") or settlement_payload.get("exchange_order_id") or "")
            or None
        )
        payload["failed_side"] = (
            str(failed_side or (failed_trade or {}).get("side") or "").upper() or None
        )
    return payload


def cmd_live_pipeline_smoke(
    *,
    plan: bool,
    apply: bool,
    yes: bool,
    cycles: int,
    max_orders: int,
    max_notional_krw: float,
    authority_path: str | None = None,
    confirm: str | None = None,
    json_output: bool = False,
) -> dict[str, Any]:
    if plan == apply:
        raise LivePipelineSmokeError("live_pipeline_smoke_requires_exactly_one_of_plan_or_apply")
    if plan:
        payload = build_live_pipeline_smoke_plan(
            cycles=cycles,
            max_orders=max_orders,
            max_notional_krw=max_notional_krw,
        )
    else:
        from .broker.bithumb import build_broker_with_auth_diagnostics

        conn = ensure_db()
        try:
            broker, _auth_diag = build_broker_with_auth_diagnostics(caller="live_pipeline_smoke")
            from .recovery import reconcile_with_broker

            payload = run_live_pipeline_smoke(
                conn=conn,
                broker=broker,
                cycles=cycles,
                max_orders=max_orders,
                max_notional_krw=max_notional_krw,
                yes=yes,
                authority_path=authority_path,
                confirm=confirm,
                post_trade_reconcile=lambda: reconcile_with_broker(broker),
            )
        finally:
            conn.close()
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return payload


def cmd_live_pipeline_smoke_authority(
    *,
    out: str,
    cycles: int,
    max_orders: int,
    max_notional_krw: float,
    expires_min: int,
) -> dict[str, Any]:
    from .live_pipeline_smoke_authority import write_live_pipeline_smoke_authority

    validate_live_pipeline_smoke_request(
        apply=False,
        yes=False,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
        authority_path=None,
        confirm=None,
    )
    payload = write_live_pipeline_smoke_authority(
        out,
        cycles=cycles,
        max_orders=max_orders,
        max_notional_krw=max_notional_krw,
        expires_min=expires_min,
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return payload
