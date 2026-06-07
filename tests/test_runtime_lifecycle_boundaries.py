from __future__ import annotations

import inspect
import ast
import textwrap
from dataclasses import dataclass

from bithumb_bot import engine
from bithumb_bot import runtime_state
from bithumb_bot.runtime import runner
from bithumb_bot.runtime.app_container import build_runtime_dependency_manifest, create_default_runtime_app
from bithumb_bot.runtime.public_api import RuntimeHealthQuery, RuntimeResumeQuery
from bithumb_bot.runtime.execution_coordinator import ExecutionCoordinator
from bithumb_bot.runtime.cleanup_revalidation import CleanupRevalidationResult, CleanupRevalidationService
from bithumb_bot.runtime.execution_coordinator import ExecutionCycleResult
from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact, SafetyDecision
from bithumb_bot.runtime.notification_adapter import NotificationAdapter
from bithumb_bot.runtime.operator_event_composer import OperatorEventComposer
from bithumb_bot.runtime.recovery_controller import RecoveryController, ReconcileClearEvidence
from bithumb_bot.runtime.safety_controller import HaltReason, RuntimeSafetyResult, SafetyController
from bithumb_bot.runtime.startup_controller import StartupController
from bithumb_bot.runtime.state_store import RuntimeStateStore


@dataclass
class _State:
    halt_new_orders_blocked: bool = False
    halt_state_unresolved: bool = False
    halt_reason_code: str | None = None
    last_disable_reason: str | None = None
    halt_operator_action_required: bool = False
    halt_open_orders_present: bool = False
    halt_position_present: bool = False
    unresolved_open_order_count: int = 0
    recovery_required_count: int = 0
    last_reconcile_status: str | None = None
    last_reconcile_reason_code: str | None = None


class _Notifications:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []
        self.messages: list[str] = []

    def send_event(self, event_name: str, **fields: object) -> None:
        self.events.append((event_name, fields))

    def send_message(self, message: str) -> None:
        self.messages.append(message)


def _cleanup_revalidation_safe() -> CleanupRevalidationResult:
    return CleanupRevalidationResult(
        safe=True,
        detail="cleanup_revalidation(trigger=unit) attempts=1/1 broker_confirms_no_open_orders_and_no_position",
        attempts=1,
        open_orders_present=False,
        position_present=False,
    )


def _safety_controller(
    *,
    state: object | None = None,
    mutations: list[str] | None = None,
    cleanup_revalidator=None,
) -> SafetyController:
    state_obj = state or _State()
    mutation_log = mutations if mutations is not None else []
    return SafetyController(
        symbol="BTC_KRW",
        state_snapshot=lambda: state_obj,
        enter_halt=lambda **_kwargs: mutation_log.append("halt"),
        resume_evaluator=lambda: (False, []),
        latest_order_identifiers=lambda: (None, None),
        count_open_orders=lambda: 0,
        position_summary=lambda: "flat",
        cancel_open_orders_with_broker=lambda _broker: {
            "remote_open_count": 0,
            "canceled_count": 0,
            "failed_count": 0,
        },
        record_cancel_open_orders_result=lambda **_kwargs: None,
        flatten_position=lambda **_kwargs: {"status": "skipped"},
        record_flatten_position_result=lambda **_kwargs: None,
        exposure_snapshot=lambda _now_ms: (False, False),
        cleanup_revalidator=cleanup_revalidator or (lambda *_args, **_kwargs: _cleanup_revalidation_safe()),
        now_ms=lambda: 1,
        live_dry_run=lambda: True,
    )


def test_engine_is_explicit_entrypoint_or_facade() -> None:
    source = inspect.getsource(engine)
    assert "sys.modules[__name__]" not in source
    assert hasattr(engine, "run_loop")


def test_engine_does_not_reexport_runtime_private_helpers() -> None:
    exported = set(getattr(engine, "__all__", ()))
    assert "_attempt_open_order_cancellation" not in exported
    assert "_revalidate_cleanup_state_after_failure" not in exported
    assert "maybe_clear_stale_initial_reconcile_halt" not in exported
    assert all(not name.startswith("_") for name in exported)


def test_engine_imports_only_runner_or_public_facade() -> None:
    source = inspect.getsource(engine)
    tree = ast.parse(textwrap.dedent(source))
    imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules = {node.module for node in imports}
    assert modules <= {"__future__", "config", "runtime.app_container"}
    for node in imports:
        if node.module == "runtime.app_container":
            assert [alias.name for alias in node.names] == ["create_default_runtime_app"]
        if node.module == "config":
            assert [alias.name for alias in node.names] == ["settings"]
    assert "create_default_runtime_app(settings).runner.run_forever()" in source
    assert "_attempt_open_order_cancellation" not in source
    assert "_revalidate_cleanup_state_after_failure" not in source


def test_runner_does_not_compose_operator_notifications() -> None:
    source = inspect.getsource(runner)
    assert "safety_event(" not in source
    assert "operator_next_action" not in source
    assert "operator_hint_command" not in source
    assert "operator_compact_summary" not in source
    assert "operator_recommended_commands" not in source
    assert "globals()[" not in source


def test_runner_only_delegates_to_runtime_cycle_pipeline() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    assert "prepare_runtime_start(" in source
    assert "RuntimeCyclePipeline" in source
    assert ".run_once(" in source
    assert ".decide_cycle(" not in source
    assert ".evaluate_runtime_safety(" not in source
    assert ".execute_cycle(" not in source
    assert ".evaluate_closed_candle(" not in source
    for forbidden in {
        "c.market_sync(",
        "c.db_factory(",
        "c.candle_reader(",
        "c.closed_candle_selector(",
        "conn.execute(",
        "\"SELECT ",
        "RuntimeDecisionGateway().decide_bundle(",
        "record_strategy_decision(",
        "run_loop_execution_planner(",
        "execution_service.execute(",
        "BrokerError",
        "evaluate_daily_loss_breach(",
        "evaluate_position_loss_breach(",
    }:
        assert forbidden not in source
    tree = ast.parse(textwrap.dedent(source))
    direct_composer_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "OperatorEventComposer"
    ]
    assert direct_composer_calls == []


def test_runner_does_not_own_cycle_stage_calls() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    for forbidden in {
        "c.market_sync(",
        "c.db_factory(",
        "c.candle_reader(",
        "c.closed_candle_selector(",
        ".evaluate_runtime_safety(",
        ".decide_cycle(",
        ".execute_cycle(",
        "conn.execute(",
        "\"SELECT ",
        "execution_submit_plan_json",
        "pre_submit_fields =",
    }:
        assert forbidden not in source


def test_runner_does_not_query_execution_plan_for_artifact() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    assert "SELECT execution_submit_plan_json FROM execution_plan" not in source
    assert "json.loads(str(row[\"execution_submit_plan_json\"]" not in source
    assert "pre_submit_fields = {" not in source


def test_runner_does_not_own_startup_gate_branches() -> None:
    source = inspect.getsource(runner.Runner._prepare_runtime_start)
    assert "prepare_runtime_start" in source
    assert "_startup_gate_allows_process_auto_recovery" not in source
    assert "startup_gate_blocked" not in source


def test_runner_does_not_shadow_controller_owned_reason_code_sets() -> None:
    source = inspect.getsource(runner)
    assert "SAFE_CLEARABLE_RECONCILE_HALT_REASON_CODES =" not in source
    assert "NON_CLEARING_RECONCILE_REASON_CODES =" not in source
    assert "class HaltReason" not in source


def test_runner_does_not_call_runtime_state_halt_mutators_directly() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    assert "runtime_state.enter_halt(" not in source
    assert "runtime_state.disable_trading_until(" not in source
    assert "runtime_state.mark_processed_candle(" not in source


def test_runner_does_not_directly_submit_orders_or_reconcile_post_trade() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    assert "execution_service.execute(" not in source
    assert "build_signal_execution_request(" not in source
    assert "except BrokerError" not in source
    assert "POST_TRADE_RECONCILE_FAILED" not in source
    assert "LIVE_EXECUTION_FAILED" not in source
    assert "LIVE_EXECUTION_BROKER_ERROR" not in source


def test_runner_module_does_not_import_or_reference_submit_boundary_symbols() -> None:
    source = inspect.getsource(runner)
    for forbidden in {
        "build_signal_execution_",
        "compat_request_builder",
        "getattr(_execution_coordinator",
        "build_signal_execution_request",
        "live_execute_signal",
        "paper_execute",
        "build_signal_execution_service",
        "record_harmless_dust_exit_suppression",
        "from ..execution_service import",
        "from bithumb_bot.execution_service import",
    }:
        assert forbidden not in source


def test_runner_does_not_own_safety_policy_branches() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    for forbidden in {
        "KILL_SWITCH",
        "KILL_SWITCH_LIQUIDATE",
        "DAILY_LOSS_LIMIT",
        "POSITION_LOSS_LIMIT",
        "MAX_OPEN_ORDER_AGE_SEC",
        "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC",
        "stale unresolved open order",
        "emergency cancellation",
    }:
        assert forbidden not in source


def test_runner_does_not_own_cleanup_revalidation_policy() -> None:
    source = inspect.getsource(runner)
    assert "def _revalidate_cleanup_state_after_failure" not in source
    assert "get_open_orders(" not in source
    assert "get_balance(" not in source
    assert "broker_confirms_no_open_orders_and_no_position" not in source


def test_runner_applies_safety_decision_after_evaluation() -> None:
    from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline

    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    assert ".evaluate_runtime_safety(" in source
    assert "_apply_runtime_safety_decision(c.safety_controller, safety_result)" in source
    assert "_apply_runtime_safety_decision(c.safety_controller, market_safety_result)" in source


def test_execution_coordinator_execute_cycle_is_used_by_runner() -> None:
    from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline

    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    assert ".execute_cycle(" in source
    assert ".resolve_submit_expectation(" not in source
    assert ".target_delta_submit_expected(" not in source


def test_runtime_recovery_gate_does_not_call_side_effect_clearers_in_prepare_phase() -> None:
    from bithumb_bot.runtime_recovery_gate import RuntimeRecoveryGateService

    source = inspect.getsource(RuntimeRecoveryGateService.prepare_resume_gate)
    assert "clearer" not in source
    assert "evaluate_and_apply" not in source


def test_runtime_recovery_gate_compat_clearers_are_not_main_path_side_effects() -> None:
    from bithumb_bot.runtime_recovery_gate import RuntimeRecoveryGateService

    calls: list[str] = []
    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        state_snapshot=lambda: _State(),
        stale_initial_reconcile_halt_clearer=lambda: calls.append("initial") or True,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: calls.append("broker") or True,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: calls.append("risk") or True,
    )

    preparation = service.prepare_resume_gate()

    assert calls == []
    assert preparation.initial_reconcile_halt_cleared is False
    assert preparation.live_execution_broker_halt_cleared is False
    assert preparation.risk_state_mismatch_halt_cleared is False


def test_startup_controller_constructor_has_no_side_effect_clearer_dependencies() -> None:
    signature = inspect.signature(StartupController)
    assert "stale_initial_reconcile_clearer" not in signature.parameters
    assert "stale_live_execution_broker_clearer" not in signature.parameters


def test_startup_controller_does_not_call_side_effect_stale_clearers_in_evaluate_phase() -> None:
    source = inspect.getsource(StartupController.evaluate_persisted_halt)
    assert "stale_initial_reconcile_clearer" not in source
    assert "stale_live_execution_broker_clearer" not in source


def test_startup_controller_prepare_uses_clearance_artifacts_only() -> None:
    source = inspect.getsource(StartupController.prepare_runtime_start)
    assert "recovery_clearance_evaluators" in source
    assert "recovery_clearance_applier" in source
    assert "stale_initial_reconcile_clearer" not in source
    assert "stale_live_execution_broker_clearer" not in source


def test_runner_does_not_inject_stale_clearers_into_startup_controller() -> None:
    source = inspect.getsource(runner._startup_controller)
    assert "stale_initial_reconcile_clearer" not in source
    assert "stale_live_execution_broker_clearer" not in source


def test_recovery_controller_evaluate_phase_has_no_state_apply() -> None:
    source = inspect.getsource(RecoveryController.evaluate_clearance)
    assert "enable_trading" not in source
    assert "disable_trading_until" not in source
    assert "set_resume_gate" not in source


def test_recovery_clearance_public_wrappers_are_side_effect_free() -> None:
    forbidden = {
        "refresh_open_order_health",
        "disable_trading_until",
        "enable_trading",
        "set_resume_gate",
        "evaluate_and_apply",
        "apply_clearance",
    }
    for func in (
        runner.evaluate_initial_reconcile_halt_clearance,
        runner.evaluate_live_execution_broker_halt_clearance,
        runner.evaluate_risk_state_mismatch_halt_clearance,
    ):
        source = inspect.getsource(func)
        tree = ast.parse(source)
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        called.update(
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        )
        assert forbidden.isdisjoint(called)


def test_runtime_state_disable_trading_until_does_not_query_exposure_sources() -> None:
    source = inspect.getsource(runtime_state.disable_trading_until)
    assert "ensure_db(" not in source
    assert "summarize_position_lots" not in source
    assert "build_position_state_model" not in source


def test_runtime_state_store_owns_state_application_boundary() -> None:
    source = inspect.getsource(RuntimeStateStore)
    assert "def snapshot" in source
    assert "def persist" in source
    assert "def apply_transition" in source
    assert "def pause_until" in source
    assert "def enable" in source
    assert "def set_resume_gate" in source
    assert "HaltStateProjector" in source


def test_runtime_cycle_artifact_is_recorded_for_halt_recovery_and_execution_failure() -> None:
    source = inspect.getsource(runner)
    from bithumb_bot.runtime import cycle_pipeline

    pipeline_source = inspect.getsource(cycle_pipeline)
    assert '"recovery:initial_reconcile_clear"' in source
    assert '"recovery:live_execution_broker_clear"' in source
    assert '"recovery:risk_state_mismatch_clear"' in source
    assert "safety_decision_hash=decision.as_dict()" in pipeline_source
    assert "state_transition_hash=decision.as_dict()" in pipeline_source
    assert "notification_event_hashes=" in pipeline_source
    assert "execution_result.planning_status" in pipeline_source


def test_startup_controller_prepare_runtime_start_statuses() -> None:
    blocked_state = _State(halt_new_orders_blocked=True, halt_reason_code="HALTED")
    controller = StartupController(
        symbol="BTC_KRW",
        startup_gate_evaluator=lambda: None,
        state_snapshot=lambda: blocked_state,
        latest_order_identifiers=lambda: (None, None),
        count_open_orders=lambda: 0,
        position_summary=lambda: "flat",
        recommended_commands=lambda **_kwargs: ["resume"],
        auto_recovery_allowed=lambda **_kwargs: False,
    )
    assert controller.prepare_runtime_start(live_mode=False).status == "BLOCKED"

    state = _State()
    ready = StartupController(
        symbol="BTC_KRW",
        startup_gate_evaluator=lambda: None,
        state_snapshot=lambda: state,
        latest_order_identifiers=lambda: (None, None),
        count_open_orders=lambda: 0,
        position_summary=lambda: "flat",
        recommended_commands=lambda **_kwargs: ["resume"],
        auto_recovery_allowed=lambda **_kwargs: False,
    )
    assert ready.prepare_runtime_start(live_mode=False).status == "READY"

    degraded = StartupController(
        symbol="BTC_KRW",
        startup_gate_evaluator=lambda: "fee_pending_auto_recovering=1",
        state_snapshot=lambda: state,
        latest_order_identifiers=lambda: (None, None),
        count_open_orders=lambda: 0,
        position_summary=lambda: "flat",
        recommended_commands=lambda **_kwargs: ["resume"],
        auto_recovery_allowed=lambda **_kwargs: True,
    )
    assert degraded.prepare_runtime_start(live_mode=False).status == "DEGRADED_RECOVERY_CONTINUE"


def test_recovery_controller_evaluate_and_apply_split() -> None:
    mutations: list[str] = []
    state = _State(
        halt_new_orders_blocked=True,
        halt_state_unresolved=True,
        halt_reason_code="INITIAL_RECONCILE_FAILED",
        last_reconcile_status="ok",
        last_reconcile_reason_code="RECONCILE_OK",
    )
    controller = RecoveryController(
        state_snapshot=lambda: state,
        refresh_open_order_health=lambda: None,
        startup_gate_evaluator=lambda: None,
        reconcile_clear_evidence=lambda _state: ReconcileClearEvidence(False, False, 0, True),
        risk_state_clear_allowed=lambda **_kwargs: False,
        enable_trading=lambda: mutations.append("enable"),
        disable_trading_until=lambda *args, **kwargs: mutations.append("disable"),
        set_resume_gate=lambda **kwargs: mutations.append("resume_gate"),
    )

    clearance = controller.evaluate_clearance(
        snapshot=state,
        startup_gate_reason=None,
        clearance_type="initial_reconcile",
    )
    assert clearance.allowed is True
    assert mutations == []

    transition = controller.apply_clearance(clearance)
    assert transition.applied is True
    assert mutations == ["disable", "resume_gate"]


def test_safety_controller_decision_creation_is_separate_from_notification_send() -> None:
    notifications = _Notifications()
    mutations: list[str] = []
    state = _State()
    controller = SafetyController(
        symbol="BTC_KRW",
        state_snapshot=lambda: state,
        enter_halt=lambda **_kwargs: mutations.append("halt"),
        resume_evaluator=lambda: (False, []),
        latest_order_identifiers=lambda: (None, None),
        count_open_orders=lambda: 0,
        position_summary=lambda: "flat",
        cancel_open_orders_with_broker=lambda _broker: {},
        record_cancel_open_orders_result=lambda **_kwargs: None,
        flatten_position=lambda **_kwargs: {},
        record_flatten_position_result=lambda **_kwargs: None,
        exposure_snapshot=lambda _now_ms: (False, False),
        cleanup_revalidator=lambda *_args, **_kwargs: _cleanup_revalidation_safe(),
        now_ms=lambda: 1,
        live_dry_run=lambda: True,
    )

    decision = controller.evaluate_halt(HaltReason("TEST", "detail"), unresolved=True)
    assert decision.as_dict()["decision_hash"].startswith("sha256:")
    assert notifications.events == []
    assert mutations == []

    controller.apply(decision)
    assert notifications.events == []
    assert mutations == ["halt"]


def test_safety_controller_live_runtime_evaluate_phase_has_no_state_apply() -> None:
    source = inspect.getsource(SafetyController.evaluate_runtime_safety)
    assert "halt_trading(" not in source
    assert "enter_halt(" not in source
    assert "disable_trading_until(" not in source

    class _Settings:
        MODE = "live"
        KILL_SWITCH = True
        KILL_SWITCH_LIQUIDATE = False
        PAIR = "BTC_KRW"

    mutations: list[str] = []
    controller = _safety_controller(mutations=mutations)
    result = controller.evaluate_runtime_safety(
        settings_obj=_Settings(),
        broker=object(),
        now_epoch_sec=1.0,
        last_close=100.0,
        last_open_order_reconcile_at=None,
        portfolio_cash_qty_with_position_state=lambda **_kwargs: (0.0, 0.0, None, None),
        db_factory=lambda: None,
        open_order_snapshot=lambda _now_ms: (0, None),
        mark_open_orders_recovery_required=lambda _reason, _now_ms: 0,
        reconcile_with_broker=lambda _broker: None,
    )
    assert result.blocked is True
    assert result.safety_decision is not None
    assert mutations == []


def test_safety_controller_apply_phase_owns_halt_state_transition() -> None:
    mutations: list[str] = []
    controller = _safety_controller(mutations=mutations)
    decision = controller.evaluate_halt(HaltReason("UNIT", "detail"), unresolved=True)
    transition = controller.apply(decision)
    assert transition.applied is True
    assert transition.reason_code == "UNIT"
    assert mutations == ["halt"]


def test_runtime_safety_notification_deduplicates_decision_event() -> None:
    notifications = _Notifications()
    event = OperatorEventComposer("BTC_KRW").trading_halted_event(
        reason_code="UNIT",
        reason="detail",
        unresolved=True,
        operator_action_required=True,
    )
    decision = SafetyDecision(
        action="HALT",
        reason_code="UNIT",
        reason="detail",
        operator_event=event,
    )
    result = RuntimeSafetyResult(
        blocked=True,
        safety_decision=decision,
        notification_events=(event,),
    )

    runner._dispatch_runtime_safety_notifications(NotificationAdapter(notifications), result)

    assert len(notifications.events) == 1
    assert notifications.events[0][0] == "trading_halted"


def test_market_runtime_halt_sends_single_operator_event() -> None:
    notifications = _Notifications()
    mutations: list[str] = []

    class _Settings:
        MODE = "live"
        MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC = -1

    controller = _safety_controller(mutations=mutations)
    result = controller.evaluate_market_runtime(
        settings_obj=_Settings(),
        now_epoch_sec=10.0,
        last_market_runtime_check_at=None,
        validate_market_runtime=lambda _settings: None,
        validation_error_type=ValueError,
    )
    assert mutations == []
    result = runner._apply_runtime_safety_decision(controller, result)
    runner._dispatch_runtime_safety_notifications(NotificationAdapter(notifications), result)

    assert mutations == ["halt"]
    assert [name for name, _fields in notifications.events] == ["trading_halted"]


def test_safety_decision_operator_event_hash_uses_event_hash() -> None:
    event = {"event_type": "unit", "event_hash": "sha256:event-authority", "payload": "x"}
    decision = SafetyDecision(
        action="HALT",
        reason_code="UNIT",
        reason="detail",
        operator_event=event,
    )
    assert decision.as_dict()["operator_event_hashes"] == ["sha256:event-authority"]

    fallback = SafetyDecision(
        action="HALT",
        reason_code="UNIT",
        reason="detail",
        operator_event={"event_type": "unit", "payload": "x"},
    ).as_dict()["operator_event_hashes"][0]
    assert fallback.startswith("sha256:")
    assert fallback == SafetyDecision(
        action="HALT",
        reason_code="UNIT",
        reason="detail",
        operator_event={"payload": "x", "event_type": "unit"},
    ).as_dict()["operator_event_hashes"][0]


def test_cleanup_revalidation_service_reports_safe_when_broker_confirms_flat() -> None:
    calls: list[str] = []

    class _Balance:
        asset_available = 0.0
        asset_locked = 0.0

    class _Broker:
        def get_open_orders(self, **_kwargs):
            calls.append("open_orders")
            return []

        def get_balance(self):
            calls.append("balance")
            return _Balance()

    service = CleanupRevalidationService(
        reconcile_with_broker=lambda _broker: calls.append("reconcile"),
        open_order_identifiers=lambda: (["client-1"], ["exchange-1"]),
        max_attempts=2,
    )

    result = service.evaluate(_Broker(), trigger="unit")

    assert result.safe is True
    assert result.open_orders_present is False
    assert result.position_present is False
    assert result.as_dict()["decision_hash"].startswith("sha256:")
    assert calls == ["reconcile", "open_orders", "balance"]


def test_cleanup_revalidation_service_fails_closed_on_unknown_broker_state() -> None:
    class _Broker:
        def get_open_orders(self, **_kwargs):
            raise RuntimeError("open orders unknown")

        def get_balance(self):
            raise RuntimeError("balance unknown")

    service = CleanupRevalidationService(
        reconcile_with_broker=lambda _broker: None,
        open_order_identifiers=lambda: (["client-1"], ["exchange-1"]),
        max_attempts=2,
    )

    result = service.evaluate(_Broker(), trigger="unit")

    assert result.safe is False
    assert result.open_orders_present is None
    assert result.position_present is None
    assert "open_orders_present=unknown" in result.detail
    assert "position_present=unknown" in result.detail
    assert result.errors


def test_safety_controller_apply_does_not_send_notifications() -> None:
    source = inspect.getsource(SafetyController.apply)
    assert "send_event" not in source
    assert "send_message" not in source
    assert "NotificationAdapter" not in source
    assert "notification_sender" not in inspect.getsource(SafetyController)


def test_operator_event_composer_owns_operator_action_fields() -> None:
    source = inspect.getsource(SafetyController)
    assert "operator_next_action" not in source
    assert "operator_hint_command" not in source
    assert "operator_compact_summary" not in source
    assert "operator_recommended_commands" not in source

    event = OperatorEventComposer("BTC_KRW").trading_halted_event(
        reason_code="TEST",
        reason="detail",
        unresolved=True,
        operator_action_required=True,
        open_orders_present=True,
        position_present=False,
        recommended_commands=["uv run python bot.py recovery-report"],
    )
    assert event["event_type"] == "trading_halted"
    assert event["operator_next_action"]
    assert event["operator_hint_command"]
    assert event["operator_compact_summary"]
    assert event["operator_recommended_commands"]
    assert event["event_hash"].startswith("sha256:")


def test_runner_does_not_create_market_runtime_halt_reason_codes() -> None:
    source = inspect.getsource(runner.Runner.run_one_cycle)
    assert "MARKET_RUNTIME_POLICY_INVALID" not in source
    assert "MARKET_RUNTIME_CONTRACT_FAILED" not in source


def test_default_runtime_app_exposes_explicit_container_interface() -> None:
    app = create_default_runtime_app()
    assert app.db_factory is not None
    assert app.clock is not None
    assert app.scheduler is not None
    assert app.decision_coordinator is not None
    assert app.execution_coordinator is not None
    assert app.safety_controller is not None
    assert app.startup_controller is not None
    assert app.runtime_dependency_manifest_provider is not None
    assert hasattr(app.runner, "run_one_cycle")


def test_runtime_dependency_manifest_hash_changes_with_wiring() -> None:
    class _Settings:
        MODE = "live"
        LIVE_DRY_RUN = True
        LIVE_REAL_ORDER_ARMED = False
        EXECUTION_ENGINE = "lot_native"
        PAIR = "KRW-BTC"
        INTERVAL = "1m"
        STRATEGY_NAME = "safe_hold"
        MAX_ORDER_KRW = 1
        MAX_DAILY_LOSS_KRW = 1
        MAX_DAILY_ORDER_COUNT = 1

    def broker_a():
        return object()

    def broker_b():
        return object()

    common = dict(
        settings_obj=_Settings(),
        decision_gateway=object,
        execution_service_factory=object,
        notification_service=_Notifications(),
        flatten_service=object,
        clock=lambda: 1.0,
        scheduler=object(),
        runtime_strategy_set_manifest_hash="sha256:strategy",
    )
    a = build_runtime_dependency_manifest(broker_factory=broker_a, **common).as_dict()
    b = build_runtime_dependency_manifest(broker_factory=broker_b, **common).as_dict()
    assert a["runtime_dependency_manifest_hash"] != b["runtime_dependency_manifest_hash"]


def test_read_only_runtime_queries_do_not_mutate_state() -> None:
    state = runtime_state.RuntimeState(halt_new_orders_blocked=True, halt_reason_code="HALTED")
    mutations: list[str] = []
    health = RuntimeHealthQuery(state_snapshot=lambda: state).get_status()

    class _Gate:
        def resume_eligibility(self):
            return False, []

    allowed, blockers = RuntimeResumeQuery(lambda: _Gate()).evaluate_eligibility()

    assert health["halt_reason_code"] == "HALTED"
    assert allowed is False
    assert blockers == []
    assert mutations == []


def test_operator_event_composer_has_no_runtime_state_side_effects() -> None:
    source = inspect.getsource(OperatorEventComposer)
    assert "runtime_state" not in source
    event = OperatorEventComposer("BTC_KRW").startup_gate_blocked_event(
        reason_code="STARTUP_BLOCKED",
        reason="blocked",
        unresolved_order_count=1,
        position_may_remain=True,
    )
    assert event["event_type"] == "startup_gate_blocked"
    assert event["event_hash"].startswith("sha256:")
    assert "operator_recommended_commands" in event


def test_notification_adapter_sends_only_already_composed_events() -> None:
    notifications = _Notifications()
    event = OperatorEventComposer("BTC_KRW").recovery_required_event(
        reason_code="TEST",
        reason="already composed",
    )
    NotificationAdapter(notifications).send_event(event)
    assert notifications.events == [("recovery_required", {"schema_version": 1, "alert_kind": "recovery_required", "symbol": "BTC_KRW", "reason_code": "TEST", "reason": "already composed"})]


def test_execution_coordinator_owns_submit_checkpoint_and_post_trade_reconcile() -> None:
    coordinator = ExecutionCoordinator("lot_native")
    assert coordinator.execute_cycle(
        candle_ts=1,
        decision_id=None,
        execution_decision_summary=object(),
    ).planning_status == "decision_persistence_failed"
    assert coordinator.execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=None,
    ).planning_status == "execution_summary_missing"

    class _Summary:
        submit_expected = True

    result = coordinator.execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=lambda: None,
        post_trade_reconcile=lambda: None,
    )
    assert result.submitted is True
    assert result.post_trade_reconciled is True


def test_runtime_cycle_artifact_hashes_required_paths() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="submit",
        candle_ts=1,
        startup_state="READY",
        readiness_hash="sha256:ready",
        strategy_decision_hash="sha256:strategy",
        execution_plan_bundle_hash="sha256:plan",
        strategy_risk_decision_hash="sha256:strategy-risk",
        strategy_risk_policy_hash="sha256:strategy-policy",
        strategy_risk_input_hash="sha256:strategy-input",
        strategy_risk_evidence_hash="sha256:strategy-evidence",
        strategy_risk_state_source="runtime_db_strategy_instance_ledger",
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_decision_hash="sha256:portfolio-risk",
        portfolio_risk_policy_hash="sha256:portfolio-policy",
        portfolio_risk_input_hash="sha256:portfolio-input",
        portfolio_risk_evidence_hash="sha256:portfolio-evidence",
        portfolio_risk_state_source="portfolio_allocator_target",
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
        pre_submit_risk_decision_hash="sha256:pre-submit-risk",
        pre_submit_risk_policy_hash="sha256:pre-submit-policy",
        pre_submit_risk_input_hash="sha256:pre-submit-input",
        pre_submit_risk_evidence_hash="sha256:pre-submit-evidence",
        pre_submit_risk_plan_hash="sha256:plan",
        pre_submit_risk_state_source="runtime_db_broker",
        pre_submit_risk_status="ALLOW",
        pre_submit_risk_reason_code="OK",
        execution_result_hash="sha256:execution",
        safety_decision_hash="sha256:safety",
        recovery_decision_hash="sha256:recovery",
        state_transition_hash="sha256:state",
        notification_event_hashes=["sha256:event"],
    )
    payload = artifact.as_dict()
    assert payload["artifact_type"] == "runtime_cycle_artifact"
    assert payload["execution_result_hash"] == "sha256:execution"
    assert payload["strategy_risk_decision_hash"] == "sha256:strategy-risk"
    assert payload["portfolio_risk_decision_hash"] == "sha256:portfolio-risk"
    assert payload["pre_submit_risk_decision_hash"] == "sha256:pre-submit-risk"
    assert payload["pre_submit_risk_plan_hash"] == "sha256:plan"
    assert payload["decision_hash"].startswith("sha256:")


def test_runtime_cycle_artifact_records_execution_result_hash_on_submit() -> None:
    result = ExecutionCycleResult(
        candle_ts=1,
        decision_id=1,
        planning_status="submitted",
        submit_expected=True,
        submitted=True,
        post_trade_reconciled=True,
        mark_processed_allowed=True,
    )
    artifact = RuntimeCycleArtifact(
        cycle_id="checkpoint:processed",
        candle_ts=1,
        execution_result_hash=result.as_dict()["decision_hash"],
    )
    assert artifact.as_dict()["execution_result_hash"] == result.as_dict()["decision_hash"]
    assert artifact.as_dict()["evidence_hash"].startswith("sha256:")


def test_runtime_cycle_artifact_records_execution_result_hash_on_submit_blocked() -> None:
    result = ExecutionCycleResult(
        candle_ts=1,
        decision_id=1,
        planning_status="submit_blocked",
        submit_expected=False,
        submitted=False,
        post_trade_reconciled=False,
        mark_processed_allowed=True,
    )
    artifact = RuntimeCycleArtifact(
        cycle_id="checkpoint:processed",
        candle_ts=1,
        execution_result_hash=result.as_dict()["decision_hash"],
    )
    assert artifact.as_dict()["execution_result_hash"] == result.as_dict()["decision_hash"]


def test_runtime_cycle_artifact_records_execution_result_hash_on_execution_halt() -> None:
    result = ExecutionCycleResult(
        candle_ts=1,
        decision_id=1,
        planning_status="live_execution_failed",
        submit_expected=True,
        submitted=False,
        post_trade_reconciled=False,
        mark_processed_allowed=True,
        halt_transition={"reason_code": "LIVE_EXECUTION_FAILED"},
    )
    artifact = RuntimeCycleArtifact(
        cycle_id="halt:live_execution_failed",
        candle_ts=1,
        execution_result_hash=result.as_dict()["decision_hash"],
    )
    assert artifact.as_dict()["execution_result_hash"] == result.as_dict()["decision_hash"]
