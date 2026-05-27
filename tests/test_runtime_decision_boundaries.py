from __future__ import annotations

import ast
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from bithumb_bot.config import settings
from bithumb_bot import engine
from bithumb_bot import runtime_strategy_decision
from bithumb_bot.core.sma_policy import PositionSnapshot, StrategyDecisionV2
from bithumb_bot.db_core import ensure_schema
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.execution_service import (
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_execution_decision_summary,
    validate_execution_submit_plan_payload,
)
from bithumb_bot.run_loop_execution_planner import (
    ExecutionAuthorityEnvelope,
    ExecutionPlanner,
    ExecutionPlanningInput,
)
from bithumb_bot.run_loop_compatibility import (
    legacy_context_planning_allowed_for_compatibility,
)
from bithumb_bot.runtime_recovery_gate import RuntimeRecoveryGateService
from bithumb_bot.research.backtest_kernel import run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.runtime_sma_snapshot_builder import (
    RuntimeSmaDecisionResult,
    RuntimeSmaPolicyHashes,
)
from bithumb_bot.runtime_sma_snapshot_builder import build_sma_with_filter_decision_from_normalized_db
from bithumb_bot.strategy.base import PositionContext
from bithumb_bot.strategy.sma import create_sma_with_filter_strategy


class CountingConnection(sqlite3.Connection):
    commit_count: int

    def commit(self) -> None:
        self.commit_count = getattr(self, "commit_count", 0) + 1
        super().commit()


def _insert_candles(conn: sqlite3.Connection, *, pair: str, interval: str, base_ts: int) -> None:
    for idx in range(40):
        close = 10.0 + 0.2 * idx
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (base_ts + idx * 60_000, pair, interval, close, close, close, close, 1.0),
        )


def _typed_decision(
    *,
    final_signal: str = "HOLD",
    final_reason: str = "unit hold",
    strategy_name: str = "sma_with_filter",
) -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name=strategy_name,
        raw_signal=final_signal,
        raw_reason=final_reason,
        entry_signal=final_signal,
        entry_reason=final_reason,
        exit_signal=final_signal,
        exit_reason=final_reason,
        final_signal=final_signal,
        final_reason=final_reason,
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=None,
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal, "final_reason": final_reason},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def _runtime_result(*, strategy_name: str = "sma_with_filter") -> RuntimeSmaDecisionResult:
    return RuntimeSmaDecisionResult(
        decision=_typed_decision(strategy_name=strategy_name),
        base_context={
            "market_price": 10.0,
            "last_close": 10.0,
            "position_state": {"normalized_exposure": {"sellable_executable_lot_count": 0}},
        },
        position=PositionContext(in_position=False),
        exposure=object(),
        position_state=object(),
        candle_ts=1_700_003_000_000,
        market_price=10.0,
        replay_fingerprint={"schema_version": 1, "candle_ts": 1_700_003_000_000},
        boundary={"decision_boundary_phase": "post_normalization_decision"},
    )


@dataclass
class _GenericRuntimeDecisionResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]
    policy_hashes: dict[str, object] | None = None

    def as_legacy_dict(self) -> dict[str, object]:
        return dict(self.base_context)


def _generic_runtime_result(*, strategy_name: str = "unit_promotion") -> _GenericRuntimeDecisionResult:
    return _GenericRuntimeDecisionResult(
        decision=_typed_decision(strategy_name=strategy_name),
        base_context={
            "market_price": 10.0,
            "last_close": 10.0,
            "position_state": {"normalized_exposure": {"sellable_executable_lot_count": 0}},
        },
        candle_ts=1_700_003_000_000,
        market_price=10.0,
        replay_fingerprint={"schema_version": 1, "candle_ts": 1_700_003_000_000},
        boundary={"decision_boundary_phase": "unit_generic_decision"},
        policy_hashes={"unit_policy_hash": "sha256:unit"},
    )


class _Readiness:
    def as_dict(self) -> dict[str, object]:
        return {}


def _planner() -> ExecutionPlanner:
    return ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness(),
        summary_builder=build_execution_decision_summary,
        target_state_resolver=lambda _conn, **_kwargs: {
            "previous_target_exposure_krw": None,
            "target_policy_metadata": {},
            "target_state": None,
        },
    )


def test_pure_sma_policy_has_no_runtime_imports_or_side_effect_dependencies() -> None:
    source = Path("src/bithumb_bot/core/sma_policy.py").read_text()
    tree = ast.parse(source)
    forbidden_modules = {
        "sqlite3",
        "time",
        "datetime",
        "bithumb_bot.config",
        "bithumb_bot.broker",
        "bithumb_bot.notifier",
        "bithumb_bot.db_core",
        "bithumb_bot.runtime_state",
    }
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    assert not forbidden_modules.intersection(imported)
    assert ".commit(" not in source
    assert ".execute(" not in source


def test_normalized_db_decision_path_does_not_commit() -> None:
    conn = sqlite3.connect(":memory:", factory=CountingConnection)
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        _insert_candles(conn, pair=settings.PAIR, interval=settings.INTERVAL, base_ts=1_700_001_000_000)
        conn.commit()
        conn.commit_count = 0

        strategy = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
        )
        decision = build_sma_with_filter_decision_from_normalized_db(
            conn,
            strategy,
            through_ts_ms=1_700_001_000_000 + 39 * 60_000,
        )
    finally:
        conn.close()

    assert decision is not None
    assert conn.commit_count == 0


def test_execution_submit_plan_contract_detects_missing_or_inconsistent_fields() -> None:
    valid_plan = {
        "side": "BUY",
        "source": "strategy_position",
        "authority": "configured_strategy_order_size",
        "final_action": "ENTER_STRATEGY_POSITION",
        "qty": 0.001,
        "notional_krw": 100_000.0,
        "target_exposure_krw": 100_000.0,
        "current_effective_exposure_krw": 0.0,
        "delta_krw": 100_000.0,
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "block_reason": "none",
        "idempotency_key": None,
    }

    validate_execution_submit_plan_payload(valid_plan, field_name="buy_submit_plan")

    missing = dict(valid_plan)
    missing.pop("final_action")
    with pytest.raises(ValueError, match="buy_submit_plan_schema_missing_fields:final_action"):
        validate_execution_submit_plan_payload(missing, field_name="buy_submit_plan")

    inconsistent = dict(valid_plan)
    inconsistent["pre_submit_proof_status"] = "failed"
    with pytest.raises(ValueError, match="buy_submit_plan_schema_submit_expected_with_failed_proof"):
        validate_execution_submit_plan_payload(inconsistent, field_name="buy_submit_plan")


def test_runtime_result_decision_envelope_preserves_typed_observability() -> None:
    result = _runtime_result()
    envelope = DecisionEnvelope.from_runtime_result(result)
    context = envelope.as_persistence_context()

    assert envelope.strategy_decision.final_signal == "HOLD"
    assert envelope.strategy_decision.final_reason == "unit hold"
    assert isinstance(envelope.policy_hashes, RuntimeSmaPolicyHashes)
    assert context["policy_contract_hash"] == "sha256:contract"
    assert context["policy_input_hash"] == "sha256:input"
    assert context["policy_decision_hash"] == "sha256:decision"
    assert context["pure_policy_hash"] == "sha256:pure"
    assert context["replay_fingerprint"] == {
        "schema_version": 1,
        "candle_ts": 1_700_003_000_000,
    }
    assert context["boundary"] == {
        "decision_boundary_phase": "post_normalization_decision",
    }
    assert context["decision_authority_source"] == "DecisionEnvelope.strategy_decision"
    assert context["persistence_context_authoritative"] == 0


def test_execution_planner_plan_envelope_matches_legacy_context_summary_semantics() -> None:
    result = _runtime_result()
    envelope = DecisionEnvelope.from_runtime_result(result)
    planner = _planner()

    bundle = planner.plan_envelope(None, envelope, updated_ts=1_700_003_060_000)
    legacy = planner.plan_strategy_decision(
        None,
        decision_context=envelope.as_persistence_context(),
        signal=result.decision.final_signal,
        reason=result.decision.final_reason,
        updated_ts=1_700_003_060_000,
        allow_legacy_context_planning=True,
    )

    assert bundle.summary is not None
    assert legacy.execution_decision_summary is not None
    assert bundle.summary.as_dict() == legacy.execution_decision_summary.as_dict()
    assert bundle.persistence_context["execution_decision"] == legacy.context["execution_decision"]
    assert bundle.persistence_context["execution_plan_bundle_present"] is True
    assert bundle.persistence_context["persistence_context_authoritative"] == 0
    assert bundle.status is not None
    assert bundle.status.status == "BLOCKED"


def test_legacy_plan_strategy_decision_fails_closed_by_default() -> None:
    result = _runtime_result()
    envelope = DecisionEnvelope.from_runtime_result(result)

    planning = _planner().plan_strategy_decision(
        None,
        decision_context=envelope.as_persistence_context(),
        signal="BUY",
        reason="legacy context only",
        updated_ts=1_700_003_060_000,
    )

    assert planning.execution_decision_summary is None
    assert planning.planning_error == "legacy_context_planning_disabled"
    assert planning.context["final_action"] == "BLOCK_RECOVERY"
    assert planning.context["submit_expected"] is False
    assert planning.context["persistence_context_authoritative"] == 0


def test_legacy_plan_strategy_decision_fails_closed_for_live_real_order_even_when_opted_in() -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        result = _runtime_result()
        envelope = DecisionEnvelope.from_runtime_result(result)

        planning = _planner().plan_strategy_decision(
            None,
            decision_context=envelope.as_persistence_context(),
            signal="BUY",
            reason="legacy context only",
            updated_ts=1_700_003_060_000,
            allow_legacy_context_planning=True,
        )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)

    assert planning.execution_decision_summary is None
    assert planning.planning_error == "legacy_context_planning_live_real_order_disabled"
    assert planning.context["submit_expected"] is False


def test_sma_with_filter_live_runtime_requires_typed_handoff() -> None:
    original = {
        "MODE": settings.MODE,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

        assert engine._promotion_grade_typed_runtime_decision_required(
            selected_strategy_name="sma_with_filter"
        ) is True
        assert engine._promotion_grade_typed_runtime_decision_required(
            selected_strategy_name="sma_cross"
        ) is False
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_sma_with_filter_live_dict_handoff_from_monkey_patch_fails_closed() -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

        reason = engine._typed_runtime_handoff_failure_reason(
            {"signal": "BUY", "reason": "legacy monkey patch"},
            selected_strategy_name="sma_with_filter",
        )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)

    assert reason == "typed_runtime_decision_required"


def test_sma_with_filter_paper_dict_handoff_is_only_non_promotion_compatible() -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "paper")
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

        reason = engine._typed_runtime_handoff_failure_reason(
            {"signal": "BUY", "reason": "legacy paper diagnostic"},
            selected_strategy_name="sma_with_filter",
        )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)

    assert reason is None


def test_sma_with_filter_approved_profile_runtime_requires_typed_handoff() -> None:
    original = {
        "MODE": settings.MODE,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "paper")
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "/tmp/profile.json")

        assert engine._promotion_grade_typed_runtime_decision_required(
            selected_strategy_name="sma_with_filter"
        ) is True
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_engine_promotion_runtime_path_has_no_concrete_sma_branch() -> None:
    source = Path("src/bithumb_bot/engine.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)

    assert 'selected_strategy_name == "sma_with_filter"' not in source
    assert "SmaWithFilterStrategy" not in source
    assert "isinstance(strategy, SmaWithFilterStrategy)" not in source
    assert "decide_sma_with_filter_runtime_snapshot_from_db" not in source
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "SmaWithFilterStrategy" not in imported_names
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "decide_sma_with_filter_runtime_snapshot_from_db" not in called_names


def test_engine_import_boundary_stays_thin_for_runtime_entrypoint() -> None:
    source = Path("src/bithumb_bot/engine.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    imports: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imports.setdefault(node.module, set()).update(alias.name for alias in node.names)

    forbidden_imports = {
        "runtime_strategy_decision": {
            "build_read_only_strategy_decision_snapshot",
            "compute_strategy_decision_after_normalization",
            "normalize_position_state_before_strategy_decision",
            "normalize_position_state_for_runtime_decision",
        },
        "runtime_sma_snapshot": {"decide_sma_with_filter_runtime_snapshot_from_db"},
        "runtime_sma_snapshot_builder": {"RuntimeSmaDecisionResult", "RuntimeSmaPolicyHashes"},
        "strategy.sma_policy_strategy": {"SmaWithFilterStrategy"},
        "strategy.sma": {"SmaWithFilterStrategy"},
        "fee_gap_repair": {"build_fee_gap_accounting_repair_preview"},
        "manual_flat_repair": {"build_manual_flat_accounting_repair_preview"},
        "notifier": {"format_event", "notify"},
        "flatten": {"flatten_btc_position"},
    }
    violations = {
        module: sorted(imports.get(module, set()) & names)
        for module, names in forbidden_imports.items()
        if imports.get(module, set()) & names
    }

    assert violations == {}
    assert "from .runtime_decision_service import" in source
    assert "from .operator_repair_service import" in source
    assert "from .operator_notification_service import" in source
    assert "from .operator_flatten_service import" in source


def test_runtime_decision_adapter_registry_drives_promotion_path_without_engine_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, int | None]] = []

    class _UnitPromotionAdapter:
        strategy_name = "unit_promotion"

        def decide(self, conn, *, short_n, long_n, through_ts_ms=None):
            calls.append((short_n, long_n, through_ts_ms))
            return _generic_runtime_result(strategy_name=self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    monkeypatch.setitem(
        runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS,
        "unit_promotion",
        _UnitPromotionAdapter,
    )

    result = engine.compute_strategy_decision_snapshot(
        None,
        5,
        20,
        through_ts_ms=1_700_003_000_000,
        strategy_name="unit_promotion",
    )

    assert isinstance(result, _GenericRuntimeDecisionResult)
    assert result.decision.strategy_name == "unit_promotion"
    assert calls == [(5, 20, 1_700_003_000_000)]
    assert "unit_promotion" not in Path("src/bithumb_bot/engine.py").read_text()


def test_generic_runtime_result_flows_through_envelope_and_planner() -> None:
    result = _generic_runtime_result(strategy_name="unit_non_sma")

    assert runtime_strategy_decision.is_runtime_strategy_decision_result(result)

    envelope = DecisionEnvelope.from_runtime_result(result)
    assert envelope.strategy_decision.strategy_name == "unit_non_sma"
    assert envelope.observability_fields()["unit_policy_hash"] == "sha256:unit"

    bundle = _planner().plan_envelope(None, envelope, updated_ts=1_700_003_060_000)

    assert bundle.summary is not None
    assert bundle.persistence_context["decision_authority_source"] == "DecisionEnvelope.strategy_decision"
    assert bundle.persistence_context["unit_policy_hash"] == "sha256:unit"
    assert bundle.persistence_context["pure_policy_hash"] == "sha256:pure"
    assert bundle.persistence_context["policy_contract_hash"] == "sha256:contract"
    assert bundle.persistence_context["policy_input_hash"] == "sha256:input"
    assert bundle.persistence_context["policy_decision_hash"] == "sha256:decision"
    assert bundle.persistence_context["replay_fingerprint_hash"]


def test_generic_promotion_adapter_dict_handoff_fails_closed_when_typed_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RequiredTypedAdapter:
        strategy_name = "unit_required_typed"

        def decide(self, conn, *, short_n, long_n, through_ts_ms=None):
            raise AssertionError("not used")

        def typed_authority_required(self) -> bool:
            return True

    monkeypatch.setitem(
        runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS,
        "unit_required_typed",
        _RequiredTypedAdapter,
    )

    reason = engine._typed_runtime_handoff_failure_reason(
        {"signal": "BUY", "reason": "legacy dict"},
        selected_strategy_name="unit_required_typed",
    )

    assert reason == "typed_runtime_decision_required"


def test_run_loop_does_not_unconditionally_enable_legacy_context_planning() -> None:
    source = Path("src/bithumb_bot/engine.py").read_text(encoding="utf-8-sig")
    run_loop_source = source.split("def run_loop", 1)[1]

    assert "allow_legacy_context_planning=True" not in run_loop_source
    assert "allow_legacy_context_planning=" not in run_loop_source
    assert ".plan_strategy_decision(" not in run_loop_source
    assert "RunLoopCompatibilityPlanner" in run_loop_source


def test_run_loop_legacy_context_planning_gate_blocks_normal_live_adapter_path() -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", True)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

        allowed = engine._legacy_context_planning_allowed_for_run_loop(
            selected_strategy_name="sma_with_filter",
            signal_handoff_fn=engine.compute_signal_runtime_handoff,
        )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)

    assert allowed is False


def test_run_loop_compatibility_planning_is_not_live_real_order_authority() -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

        assert (
            legacy_context_planning_allowed_for_compatibility(
                signal_handoff_fn=lambda *_args, **_kwargs: {"signal": "BUY"},
                runtime_handoff_fn=engine.compute_signal_runtime_handoff,
            )
            is False
        )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_engine_recovery_policy_functions_delegate_to_services() -> None:
    source = Path("src/bithumb_bot/engine.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name in {"evaluate_startup_safety_gate", "_evaluate_stale_risk_state_mismatch_halt"}
    }

    startup_names = {node.func.id for node in ast.walk(functions["evaluate_startup_safety_gate"]) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    stale_names = {node.func.id for node in ast.walk(functions["_evaluate_stale_risk_state_mismatch_halt"]) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}

    assert "StartupSafetyGateService" in startup_names
    assert "StaleRiskStateMismatchHaltService" in stale_names
    for forbidden in {
        "collect_risky_order_state",
        "compute_accounting_replay",
        "build_external_position_accounting_repair_preview",
    }:
        assert forbidden not in startup_names
        assert forbidden not in stale_names


def test_recovery_gate_service_classifies_startup_blocker_without_engine_callbacks() -> None:
    state = type(
        "State",
        (),
        {
            "last_reconcile_status": "ok",
            "recovery_required_count": 0,
        },
    )()
    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: (
            "startup safety gate: position_authority_projection_repair_required=projection/portfolio divergence"
        ),
        stale_initial_reconcile_halt_clearer=lambda: False,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: False,
        state_snapshot=lambda: state,
    )

    blockers = service.startup_safety_resume_blockers(service.prepare_resume_gate().startup_gate_reason)

    assert len(blockers) == 1
    assert blockers[0].code == "STARTUP_SAFETY_GATE_BLOCKED"
    assert blockers[0].reason_code == "POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED"
    assert blockers[0].overridable is False


def test_mutating_persistence_context_does_not_change_typed_submit_authority() -> None:
    decision = _typed_decision(final_signal="BUY", final_reason="unit buy")
    envelope = DecisionEnvelope(
        strategy_decision=decision,
        candle_ts=1_700_003_000_000,
        market_price=10.0,
        base_context={
            "market_price": 10.0,
            "last_close": 10.0,
            "total_effective_exposure_notional_krw": 0.0,
        },
        policy_hashes=None,
        replay_fingerprint={"schema_version": 1},
        boundary={},
    )
    planner = _planner()

    bundle = planner.plan_envelope(None, envelope, updated_ts=1_700_003_060_000)
    assert bundle.summary is not None
    before = bundle.summary.as_dict()

    persistence = dict(bundle.persistence_context)
    persistence["final_signal"] = "SELL"
    persistence.pop("policy_decision_hash", None)

    assert bundle.summary.as_dict() == before
    assert bundle.summary.final_signal == "BUY"
    assert bundle.summary.typed_buy_submit_plan() is not None


def test_plan_envelope_uses_typed_decision_over_conflicting_base_context() -> None:
    decision = _typed_decision(final_signal="BUY", final_reason="typed unit buy")
    envelope = DecisionEnvelope(
        strategy_decision=decision,
        candle_ts=1_700_003_000_000,
        market_price=10.0,
        base_context={
            "signal": "SELL",
            "reason": "legacy context must not win",
            "final_signal": "SELL",
            "final_reason": "legacy context must not win",
            "raw_signal": "SELL",
            "last_close": 10.0,
            "total_effective_exposure_notional_krw": 0.0,
        },
        policy_hashes=None,
        replay_fingerprint={"schema_version": 1},
        boundary={},
    )
    bundle = _planner().plan_envelope(None, envelope, updated_ts=1_700_003_060_000)

    assert bundle.summary is not None
    assert bundle.summary.raw_signal == "BUY"
    assert bundle.summary.final_signal == "BUY"
    assert bundle.summary.block_reason != "legacy context must not win"
    assert bundle.submit_plan is not None
    assert bundle.submit_plan.side == "BUY"


def test_mutating_original_readiness_and_target_dicts_after_planning_does_not_change_output() -> None:
    readiness_payload: dict[str, object] = {
        "cash_available": 500_000.0,
        "total_effective_exposure_notional_krw": 0.0,
    }
    target_metadata: dict[str, object] = {"target_policy_action": "use_existing_target"}
    decision = _typed_decision(final_signal="BUY", final_reason="typed unit buy")
    envelope = DecisionEnvelope(
        strategy_decision=decision,
        candle_ts=1_700_003_000_000,
        market_price=10.0,
        base_context={"last_close": 10.0},
        policy_hashes=None,
        replay_fingerprint={"schema_version": 1},
        boundary={},
    )
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: type(
            "Readiness",
            (),
            {"as_dict": lambda _self: dict(readiness_payload)},
        )(),
        summary_builder=build_execution_decision_summary,
        target_state_resolver=lambda _conn, **_kwargs: {
            "previous_target_exposure_krw": None,
            "target_policy_metadata": dict(target_metadata),
            "target_state": None,
        },
    )

    bundle = planner.plan_envelope(None, envelope, updated_ts=1_700_003_060_000)
    assert bundle.summary is not None
    before = bundle.summary.as_dict()

    readiness_payload["cash_available"] = 1.0
    target_metadata["target_policy_action"] = "mutated"

    assert bundle.summary.as_dict() == before
    assert bundle.readiness_payload["cash_available"] == 500_000.0
    assert bundle.target_policy_metadata["target_policy_action"] == "use_existing_target"


def test_execution_authority_envelope_requires_typed_readiness_and_target() -> None:
    planning_input = ExecutionPlanningInput.from_envelope(DecisionEnvelope.from_runtime_result(_runtime_result()))

    with pytest.raises(TypeError, match="typed_execution_readiness_missing"):
        ExecutionAuthorityEnvelope(
            planning_input=planning_input,
            readiness={},  # type: ignore[arg-type]
            target=ExecutionTargetPlanningInput(),
        )

    with pytest.raises(TypeError, match="typed_execution_target_missing"):
        ExecutionAuthorityEnvelope(
            planning_input=planning_input,
            readiness=TypedExecutionPlanningInput(
                strategy_decision=planning_input.strategy_decision,
                candle_ts=planning_input.candle_ts,
                market_price=planning_input.market_price,
            ).readiness,
            target={},  # type: ignore[arg-type]
        )


def test_research_kernel_marks_missing_sma_policy_metadata_non_comparable() -> None:
    base_ts = 1_700_002_000_000
    dataset = DatasetSnapshot(
        snapshot_id="unit",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange(start="2024-01-01", end="2024-01-02"),
        candles=tuple(
            Candle(
                ts=base_ts + idx * 60_000,
                open=10.0 + idx,
                high=10.0 + idx,
                low=10.0 + idx,
                close=10.0 + idx,
                volume=1.0,
            )
            for idx in range(3)
        ),
    )
    event = ResearchDecisionEvent(
        candle_ts=base_ts + 60_000,
        decision_ts=base_ts + 61_000,
        strategy_name="sma_with_filter",
        strategy_version="unit",
        raw_signal="BUY",
        final_signal="BUY",
        reason="legacy final signal must not be authoritative",
        feature_snapshot={},
        strategy_diagnostics={},
        entry_signal="BUY",
        extra_payload={},
    )

    run = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 1,
            "SMA_LONG": 2,
            "SMA_FILTER_VOL_WINDOW": 1,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "BUY_FRACTION": 1.0,
            "MAX_ORDER_KRW": 100_000.0,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
    )

    assert run.decisions
    decision = run.decisions[0]
    assert decision["final_signal"] == "HOLD"
    assert decision["research_policy_unsupported"] is True
    assert decision["research_policy_comparable"] is False
    assert decision["research_policy_unsupported_reason"] == (
        "sma_with_filter_policy_decision_missing_not_comparable"
    )
