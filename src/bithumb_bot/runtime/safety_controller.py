from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, MutableSequence

from ..dust import build_dust_display_context
from ..reason_codes import CANCEL_FAILURE, POSITION_LOSS_LIMIT, RISKY_ORDER_BLOCK
from ..risk import (
    RISK_STATE_MISMATCH,
    daily_loss_reason_code_from_reason,
    evaluate_daily_loss_breach,
    evaluate_position_loss_breach,
)
from .cleanup_revalidation import CleanupRevalidationResult
from .lifecycle_artifacts import SafetyDecision, StateTransitionResult
from .operator_event_composer import (
    OperatorEventComposer,
    recommended_operator_commands,
)


@dataclass(frozen=True)
class HaltReason:
    code: str
    detail: str


@dataclass(frozen=True)
class CleanupResult:
    halt_reason: HaltReason
    canceled_ok: bool
    unresolved: bool
    decision: SafetyDecision
    cleanup_revalidation: CleanupRevalidationResult | None = None


@dataclass(frozen=True)
class RuntimeSafetyResult:
    blocked: bool
    cycle_id: str | None = None
    safety_decision: SafetyDecision | None = None
    state_transition_hash: str | None = None
    notification_event_hashes: tuple[str, ...] = ()
    notification_events: tuple[Mapping[str, object], ...] = ()
    notification_messages: tuple[str, ...] = ()
    last_open_order_reconcile_at: float | None = None
    last_market_runtime_check_at: float | None = None


@dataclass(frozen=True)
class SafetyController:
    symbol: str
    state_snapshot: Callable[[], object]
    enter_halt: Callable[..., None]
    resume_evaluator: Callable[[], tuple[bool, list[object]]]
    latest_order_identifiers: Callable[[], tuple[str | None, str | None]]
    count_open_orders: Callable[[], int]
    position_summary: Callable[[], str]
    cancel_open_orders_with_broker: Callable[[object], Mapping[str, object]]
    record_cancel_open_orders_result: Callable[..., None]
    flatten_position: Callable[..., Mapping[str, object]]
    record_flatten_position_result: Callable[..., None]
    exposure_snapshot: Callable[[int], tuple[bool, bool]]
    cleanup_revalidator: Callable[..., CleanupRevalidationResult]
    now_ms: Callable[[], int]
    live_dry_run: Callable[[], bool]
    legacy_cancel_open_orders: Callable[[object, str], bool] | None = None

    def evaluate_halt(
        self,
        reason: HaltReason,
        *,
        unresolved: bool = False,
        attempt_flatten: bool = False,
    ) -> SafetyDecision:
        halt_state = self.state_snapshot()
        startup_gate_order_unresolved = (
            reason.code == "STARTUP_SAFETY_GATE"
            and bool(unresolved)
            and "unresolved_open_orders=0" not in reason.detail
            and "unresolved_open_orders=" in reason.detail
        )
        _resume_allowed, resume_blockers = self.resume_evaluator()
        latest_client_order_id, latest_exchange_order_id = self.latest_order_identifiers()
        operator_action_required = bool(getattr(halt_state, "halt_operator_action_required", False)) or bool(
            startup_gate_order_unresolved
        )
        open_order_count = self.count_open_orders()
        position_summary = self.position_summary()
        recommended_commands = recommended_operator_commands(
            reason_code=reason.code,
            startup_gate=False,
            recovery_required=False,
            unresolved_count=int(getattr(halt_state, "unresolved_open_order_count", 0) or 0),
        )
        primary_blocker_code = getattr(resume_blockers[0], "code", "-") if resume_blockers else "-"
        force_resume_allowed = bool(resume_blockers) and all(bool(getattr(b, "overridable", False)) for b in resume_blockers)
        blocker_summary = (
            f"total={len(resume_blockers)} "
            f"non_overridable={sum(1 for b in resume_blockers if not bool(getattr(b, 'overridable', False)))} "
            f"overridable={sum(1 for b in resume_blockers if bool(getattr(b, 'overridable', False)))}"
        )
        event = OperatorEventComposer(self.symbol).trading_halted_event(
            reason_code=reason.code,
            reason=reason.detail,
            unresolved=unresolved,
            operator_action_required=operator_action_required,
            force_resume_allowed=force_resume_allowed,
            open_orders_present=bool(getattr(halt_state, "halt_open_orders_present", False))
            or bool(startup_gate_order_unresolved),
            position_present=bool(getattr(halt_state, "halt_position_present", False)),
            unresolved_order_count=int(getattr(halt_state, "unresolved_open_order_count", 0) or 0),
            primary_blocker_code=primary_blocker_code,
            blocker_summary=blocker_summary,
            halt_policy_stage=getattr(halt_state, "halt_policy_stage", None),
            block_new_orders=bool(getattr(halt_state, "halt_policy_block_new_orders", False)),
            attempt_cancel_open_orders=bool(getattr(halt_state, "halt_policy_attempt_cancel_open_orders", False)),
            auto_liquidate_positions=bool(getattr(halt_state, "halt_policy_auto_liquidate_positions", False)),
            latest_client_order_id=latest_client_order_id,
            latest_exchange_order_id=latest_exchange_order_id,
            open_order_count=open_order_count,
            position_summary=position_summary,
            recommended_commands=recommended_commands,
        )
        transition = StateTransitionResult(
            status="pending",
            reason_code=reason.code,
            state_from="READY",
            state_to="HALTED",
            applied=False,
        )
        return SafetyDecision(
            action="HALT",
            reason_code=reason.code,
            reason=reason.detail,
            unresolved=unresolved,
            attempt_flatten=attempt_flatten,
            state_transition=transition,
            operator_event=event,
            evidence={"resume_blocker_count": len(resume_blockers)},
        )

    def apply(self, decision: SafetyDecision) -> StateTransitionResult:
        if decision.action != "HALT":
            return StateTransitionResult(
                status="not_applied",
                reason_code=decision.reason_code,
                state_from=None,
                state_to=None,
                applied=False,
                evidence=decision.as_dict(),
            )
        self.enter_halt(
            reason_code=decision.reason_code,
            reason=decision.reason,
            unresolved=decision.unresolved,
            attempt_flatten=decision.attempt_flatten,
        )
        return StateTransitionResult(
            status="applied",
            reason_code=decision.reason_code,
            state_from="READY",
            state_to="HALTED",
            applied=True,
            evidence=decision.as_dict(),
        )

    def _attach_cleanup_evidence(self, decision: SafetyDecision, cleanup: CleanupResult) -> SafetyDecision:
        evidence = {
            **dict(decision.evidence),
            "cleanup_result": cleanup.decision.as_dict(),
        }
        if cleanup.cleanup_revalidation is not None:
            evidence["cleanup_revalidation"] = cleanup.cleanup_revalidation.as_dict()
        return SafetyDecision(
            action=decision.action,
            reason_code=decision.reason_code,
            reason=decision.reason,
            unresolved=decision.unresolved,
            attempt_flatten=decision.attempt_flatten,
            state_transition=decision.state_transition,
            operator_event=decision.operator_event,
            input_hash=decision.input_hash,
            evidence_hash=None,
            decision_hash=None,
            evidence=evidence,
        )

    def evaluate_market_runtime(
        self,
        *,
        settings_obj: object,
        now_epoch_sec: float,
        last_market_runtime_check_at: float | None,
        validate_market_runtime: Callable[[object], None],
        validation_error_type: type[BaseException],
    ) -> RuntimeSafetyResult:
        check_interval = float(getattr(settings_obj, "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC"))
        if check_interval < 0:
            decision = self.evaluate_halt(
                HaltReason(
                    "MARKET_RUNTIME_POLICY_INVALID",
                    "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC must be >= 0",
                ),
                unresolved=False,
            )
            return self._blocked_result(
                "halt:market_runtime_policy_invalid",
                decision,
                last_open_order_reconcile_at=None,
                notification_events=([decision.operator_event] if decision.operator_event else []),
            )
        should_check = getattr(settings_obj, "MODE", None) == "live" and check_interval > 0 and (
            last_market_runtime_check_at is None
            or (now_epoch_sec - last_market_runtime_check_at) >= check_interval
        )
        if not should_check:
            return RuntimeSafetyResult(
                blocked=False,
                last_market_runtime_check_at=last_market_runtime_check_at,
            )
        try:
            validate_market_runtime(settings_obj)
        except validation_error_type as exc:
            decision = self.evaluate_halt(
                HaltReason(
                    "MARKET_RUNTIME_CONTRACT_FAILED",
                    f"market runtime contract failed: {exc}",
                ),
                unresolved=False,
            )
            return self._blocked_result(
                "halt:market_runtime_contract_failed",
                decision,
                last_open_order_reconcile_at=None,
                notification_events=([decision.operator_event] if decision.operator_event else []),
            )
        return RuntimeSafetyResult(
            blocked=False,
            last_market_runtime_check_at=now_epoch_sec,
        )

    def evaluate_runtime_safety(
        self,
        *,
        settings_obj: object,
        broker: object | None,
        now_epoch_sec: float,
        last_close: float,
        last_open_order_reconcile_at: float | None,
        portfolio_cash_qty_with_position_state: Callable[..., tuple[float, float, object, object]],
        db_factory: Callable[[], object],
        open_order_snapshot: Callable[[int], tuple[int, float | None]],
        mark_open_orders_recovery_required: Callable[[str, int], int],
        reconcile_with_broker: Callable[[object], None],
    ) -> RuntimeSafetyResult:
        if getattr(settings_obj, "MODE", None) != "live" or broker is None:
            return RuntimeSafetyResult(blocked=False, last_open_order_reconcile_at=last_open_order_reconcile_at)

        notification_events: list[Mapping[str, object]] = []
        notification_messages: list[str] = []
        if bool(getattr(settings_obj, "KILL_SWITCH", False)):
            cleanup = self.attempt_cleanup_with_optional_flatten(
                broker,
                reason_code="KILL_SWITCH",
                reason_detail="KILL_SWITCH=ON",
                cancel_trigger="kill-switch",
                flatten_trigger="kill-switch",
                attempt_flatten=bool(getattr(settings_obj, "KILL_SWITCH_LIQUIDATE", False)),
                notification_events=notification_events,
                notification_messages=notification_messages,
            )
            decision = self._attach_cleanup_evidence(self.evaluate_halt(
                cleanup.halt_reason,
                unresolved=cleanup.unresolved,
                attempt_flatten=bool(getattr(settings_obj, "KILL_SWITCH_LIQUIDATE", False)),
            ), cleanup)
            return self._blocked_result(
                "halt:kill_switch",
                decision,
                last_open_order_reconcile_at,
                notification_events=notification_events,
                notification_messages=notification_messages,
            )

        portfolio_cash, portfolio_qty, position_state, lot_definition = portfolio_cash_qty_with_position_state(
            pair=getattr(settings_obj, "PAIR")
        )
        conn = db_factory()
        try:
            if position_state is not None:
                dust_context = build_dust_display_context(self.state_snapshot().last_reconcile_metadata)
                blocked, reason = evaluate_daily_loss_breach(
                    conn,
                    ts_ms=int(now_epoch_sec * 1000),
                    cash=portfolio_cash,
                    qty=portfolio_qty,
                    price=float(last_close),
                    broker=broker,
                    mark_price_source="closed_candle",
                    evaluation_origin="run_loop_daily_halt",
                )
                if blocked:
                    reason_code = daily_loss_reason_code_from_reason(reason)
                    if reason_code == RISK_STATE_MISMATCH:
                        decision = self.evaluate_halt(HaltReason(RISK_STATE_MISMATCH, reason), unresolved=True)
                    else:
                        cleanup = self.attempt_cleanup_with_optional_flatten(
                            broker,
                            reason_code="DAILY_LOSS_LIMIT",
                            reason_detail=reason,
                            cancel_trigger="daily-loss-halt",
                            flatten_trigger="daily-loss-halt",
                            attempt_flatten=True,
                            notification_events=notification_events,
                            notification_messages=notification_messages,
                        )
                        decision = self._attach_cleanup_evidence(
                            self.evaluate_halt(cleanup.halt_reason, unresolved=cleanup.unresolved),
                            cleanup,
                        )
                    return self._blocked_result(
                        "halt:daily_loss",
                        decision,
                        last_open_order_reconcile_at,
                        notification_events=notification_events,
                        notification_messages=notification_messages,
                    )

                position_loss_qty = float(position_state.normalized_exposure.open_exposure_qty)
                dust_view = position_state.normalized_exposure.dust_operator_view
                min_position_loss_qty = max(
                    0.0,
                    float(0.0 if lot_definition is None else lot_definition.min_qty),
                    float(getattr(settings_obj, "LIVE_MIN_ORDER_QTY", 0.0) or 0.0),
                )
                if (
                    bool(dust_context.classification.present)
                    and bool(dust_view.resume_allowed)
                    and min_position_loss_qty > 0.0
                    and 0.0 < float(portfolio_qty) < min_position_loss_qty
                ):
                    position_loss_qty = 0.0

                blocked, reason = evaluate_position_loss_breach(
                    conn,
                    qty=position_loss_qty,
                    price=float(last_close),
                )
                if blocked:
                    cleanup = self.attempt_cleanup_with_optional_flatten(
                        broker,
                        reason_code=POSITION_LOSS_LIMIT,
                        reason_detail=reason,
                        cancel_trigger="position-loss-halt",
                        flatten_trigger="position-loss-halt",
                        attempt_flatten=True,
                        notification_events=notification_events,
                        notification_messages=notification_messages,
                    )
                    decision = self._attach_cleanup_evidence(
                        self.evaluate_halt(cleanup.halt_reason, unresolved=cleanup.unresolved),
                        cleanup,
                    )
                    return self._blocked_result(
                        "halt:position_loss",
                        decision,
                        last_open_order_reconcile_at,
                        notification_events=notification_events,
                        notification_messages=notification_messages,
                    )
        finally:
            conn.close()

        open_count, oldest_open_age_sec = open_order_snapshot(int(now_epoch_sec * 1000))
        next_reconcile_at = last_open_order_reconcile_at
        if open_count > 0:
            min_reconcile_sec = max(1, int(getattr(settings_obj, "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC")))
            if next_reconcile_at is None or (now_epoch_sec - next_reconcile_at) >= min_reconcile_sec:
                try:
                    reconcile_with_broker(broker)
                    next_reconcile_at = now_epoch_sec
                except Exception as exc:
                    decision = self.evaluate_halt(
                        HaltReason(
                            "PERIODIC_RECONCILE_FAILED",
                            f"periodic reconcile failed ({type(exc).__name__}): {exc}",
                        ),
                        unresolved=True,
                    )
                    return self._blocked_result("halt:periodic_reconcile_failed", decision, next_reconcile_at)

            open_count, oldest_open_age_sec = open_order_snapshot(int(now_epoch_sec * 1000))
            if open_count > 0 and oldest_open_age_sec is not None:
                max_age_sec = max(1, int(getattr(settings_obj, "MAX_OPEN_ORDER_AGE_SEC")))
                if oldest_open_age_sec > max_age_sec:
                    reason = (
                        "stale unresolved open order detected: "
                        f"age={oldest_open_age_sec:.1f}s > {max_age_sec}s"
                    )
                    marked = mark_open_orders_recovery_required(reason, int(now_epoch_sec * 1000))
                    latest_client_order_id, latest_exchange_order_id = self.latest_order_identifiers()
                    event = OperatorEventComposer(self.symbol).stale_open_order_recovery_required_event(
                        reason=reason,
                        marked_count=marked,
                        latest_client_order_id=latest_client_order_id,
                        latest_exchange_order_id=latest_exchange_order_id,
                        open_order_count=self.count_open_orders(),
                        unresolved_order_count=self.state_snapshot().unresolved_open_order_count,
                        position_summary=self.position_summary(),
                    )
                    notification_events.append(event)
                    canceled_ok = self.attempt_open_order_cancellation(
                        broker,
                        trigger="stale-open-order-halt",
                        notification_events=notification_events,
                        notification_messages=notification_messages,
                    )
                    halt_detail = (
                        f"{reason}; marked={marked} recovery_required; "
                        + ("emergency cancellation attempted" if canceled_ok else "emergency cancellation failed")
                    )
                    unresolved = True
                    if not canceled_ok:
                        revalidation = self.cleanup_revalidator(
                            broker,
                            trigger="stale-open-order-halt",
                        )
                        halt_detail += f"; {revalidation.detail}"
                        unresolved = not revalidation.safe
                    decision = self.evaluate_halt(HaltReason("STALE_OPEN_ORDER", halt_detail), unresolved=unresolved)
                    return self._blocked_result(
                        "halt:stale_open_order",
                        decision,
                        next_reconcile_at,
                        notification_event_hashes=(str(event.get("event_hash")),),
                        notification_events=notification_events,
                        notification_messages=notification_messages,
                    )

            if open_count > 0:
                event = OperatorEventComposer(self.symbol).open_order_blocked_event(
                    reason_code=RISKY_ORDER_BLOCK,
                    reason="unresolved open order exists; skip new order placement",
                )
                return RuntimeSafetyResult(
                    blocked=True,
                    cycle_id="skip:open_order_blocked",
                    notification_event_hashes=(str(event.get("event_hash")),),
                    notification_events=(event,),
                    last_open_order_reconcile_at=next_reconcile_at,
                )

        return RuntimeSafetyResult(blocked=False, last_open_order_reconcile_at=next_reconcile_at)

    def _blocked_result(
        self,
        cycle_id: str,
        decision: SafetyDecision,
        last_open_order_reconcile_at: float | None,
        notification_event_hashes: tuple[str, ...] = (),
        notification_events: MutableSequence[Mapping[str, object]] | None = None,
        notification_messages: MutableSequence[str] | None = None,
    ) -> RuntimeSafetyResult:
        payload = decision.as_dict()
        transition = payload.get("state_transition")
        transition_hash = transition.get("decision_hash") if isinstance(transition, dict) else None
        event_hashes = tuple(payload.get("operator_event_hashes") or ()) + tuple(notification_event_hashes)
        return RuntimeSafetyResult(
            blocked=True,
            cycle_id=cycle_id,
            safety_decision=decision,
            state_transition_hash=str(transition_hash) if transition_hash else None,
            notification_event_hashes=tuple(str(item) for item in event_hashes),
            notification_events=tuple(notification_events or ()),
            notification_messages=tuple(notification_messages or ()),
            last_open_order_reconcile_at=last_open_order_reconcile_at,
        )

    def attempt_open_order_cancellation(
        self,
        broker: object,
        trigger: str,
        *,
        notification_events: MutableSequence[Mapping[str, object]] | None = None,
        notification_messages: MutableSequence[str] | None = None,
    ) -> bool:
        events = notification_events
        messages = notification_messages
        if self.legacy_cancel_open_orders is not None:
            ok = bool(self.legacy_cancel_open_orders(broker, trigger))
            summary = {
                "remote_open_count": 0,
                "canceled_count": 0,
                "failed_count": 0 if ok else 1,
                "stray_messages": [],
                "error_messages": [],
            }
            status = "ok" if ok else "partial"
            event = OperatorEventComposer(self.symbol).cancel_open_orders_result_event(
                trigger=trigger,
                remote_open_count=0,
                canceled_count=0,
                failed_count=0 if ok else 1,
                status=status,
            )
            if events is not None:
                events.append(event)
            self.record_cancel_open_orders_result(trigger=trigger, status=status, summary=summary)
            return ok
        try:
            summary = self.cancel_open_orders_with_broker(broker)
        except Exception as exc:
            self.record_cancel_open_orders_result(
                trigger=trigger,
                status="error",
                summary={"error": f"{type(exc).__name__}: {exc}"},
            )
            if events is not None:
                events.append(
                    OperatorEventComposer(self.symbol).panic_cleanup_event(
                    reason_code=CANCEL_FAILURE,
                    status="cancel_open_orders_error",
                    trigger=trigger,
                    cancel_detail_code="CANCEL_OPEN_ORDERS_ERROR",
                    error_type=type(exc).__name__,
                    reason=str(exc),
                )
                )
            return False

        remote_open_count = int(summary["remote_open_count"])
        canceled_count = int(summary["canceled_count"])
        failed_count = int(summary["failed_count"])
        status = "partial" if failed_count > 0 else "ok"
        event = OperatorEventComposer(self.symbol).cancel_open_orders_result_event(
            trigger=trigger,
            remote_open_count=remote_open_count,
            canceled_count=canceled_count,
            failed_count=failed_count,
            status=status,
        )
        if events is not None:
            events.append(event)
        for message in summary.get("stray_messages", []):
            if messages is not None:
                messages.append(str(message))
        for message in summary.get("error_messages", []):
            if messages is not None:
                messages.append(str(message))
        self.record_cancel_open_orders_result(trigger=trigger, status=status, summary=summary)
        if failed_count > 0:
            if events is not None:
                events.append(
                    OperatorEventComposer(self.symbol).panic_cleanup_event(
                    reason_code=CANCEL_FAILURE,
                    status="cancel_open_orders_incomplete",
                    trigger=trigger,
                    cancel_detail_code="CANCEL_OPEN_ORDERS_INCOMPLETE",
                    failed_count=failed_count,
                )
                )
            return False
        return True

    def attempt_cleanup_with_optional_flatten(
        self,
        broker: object,
        *,
        reason_code: str,
        reason_detail: str,
        cancel_trigger: str,
        flatten_trigger: str,
        attempt_flatten: bool,
        notification_events: MutableSequence[Mapping[str, object]] | None = None,
        notification_messages: MutableSequence[str] | None = None,
    ) -> CleanupResult:
        initial_open_orders_present, initial_position_present = self.exposure_snapshot(self.now_ms())
        canceled_ok = self.attempt_open_order_cancellation(
            broker,
            trigger=cancel_trigger,
            notification_events=notification_events,
            notification_messages=notification_messages,
        )
        flatten_outcome: Mapping[str, object] | None = None
        if attempt_flatten and canceled_ok:
            flatten_outcome = self.flatten_position(
                broker=broker,
                dry_run=self.live_dry_run(),
                trigger=flatten_trigger,
            )
            flatten_status = str(flatten_outcome.get("status") or "-")
        elif attempt_flatten:
            flatten_status = "skipped_cancel_failed"
        else:
            flatten_status = "skipped"

        if flatten_status in {"skipped", "skipped_cancel_failed"}:
            self.record_flatten_position_result(
                status=flatten_status,
                summary={
                    "status": flatten_status,
                    "attempted": int(bool(attempt_flatten)),
                    "cancel_ok": int(bool(canceled_ok)),
                    "reason_code": reason_code,
                    "reason_detail": reason_detail,
                    "trigger": flatten_trigger,
                },
            )

        detail_parts = [
            reason_detail,
            "emergency cancellation attempted" if canceled_ok else "emergency cancellation failed",
            f"flatten_status={flatten_status}",
        ]
        flatten_failed = flatten_status == "failed"
        if flatten_failed and flatten_outcome is not None:
            detail_parts.append(f"flatten_error={str(flatten_outcome.get('error') or '-')}")

        cleanup_uncertain = (not canceled_ok) or flatten_failed
        if cleanup_uncertain:
            revalidation = self.cleanup_revalidator(
                broker,
                trigger=flatten_trigger,
            )
            detail_parts.append(revalidation.detail)
            unresolved = not revalidation.safe
        else:
            revalidation = None
            post_open_orders_present, post_position_present = self.exposure_snapshot(self.now_ms())
            if post_open_orders_present or post_position_present:
                detail_parts.append(
                    "risk_open_exposure_remains("
                    f"open_orders={1 if post_open_orders_present else 0},"
                    f"position={1 if post_position_present else 0})"
                )
            unresolved = post_open_orders_present or post_position_present

        if initial_open_orders_present or initial_position_present:
            detail_parts.append(
                "cleanup_started_with_exposure("
                f"open_orders={1 if initial_open_orders_present else 0},"
                f"position={1 if initial_position_present else 0})"
            )
        halt_reason = HaltReason(reason_code, "; ".join(detail_parts))
        decision = SafetyDecision(
            action="HALT",
            reason_code=reason_code,
            reason=halt_reason.detail,
            unresolved=unresolved,
            attempt_flatten=attempt_flatten,
            evidence={
                "canceled_ok": bool(canceled_ok),
                "flatten_status": flatten_status,
                "cleanup_uncertain": bool(cleanup_uncertain),
                "cleanup_revalidation": (
                    revalidation.as_dict() if revalidation is not None else None
                ),
            },
        )
        return CleanupResult(
            halt_reason=halt_reason,
            canceled_ok=canceled_ok,
            unresolved=unresolved,
            decision=decision,
            cleanup_revalidation=revalidation,
        )


__all__ = [
    "CleanupResult",
    "HaltReason",
    "RuntimeSafetyResult",
    "SafetyController",
]
