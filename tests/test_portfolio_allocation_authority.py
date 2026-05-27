from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

from bithumb_bot.config import settings
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionReadinessPlanningInput,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.portfolio_allocation import (
    PortfolioAllocationInput,
    PortfolioAllocator,
    PortfolioAllocatorConfig,
    SignalAggregator,
)
from bithumb_bot.portfolio_target import PortfolioTarget
from bithumb_bot.strategy_preference import (
    StrategyPreference,
    StrategyPreferenceSet,
    strategy_decision_to_preference,
)
from bithumb_bot.runtime_strategy_decision import (
    register_runtime_decision_adapter,
    reset_runtime_decision_adapters_for_tests,
)
from bithumb_bot.runtime_adapter_bootstrap import reset_runtime_decision_adapter_bootstrap_for_tests
from bithumb_bot.runtime_strategy_set import (
    RuntimeStrategyDecisionCollector,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    RuntimeStrategySetResolver,
    RuntimeStrategySpec,
)
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2


class _Readiness:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


@dataclass(frozen=True)
class _RuntimeResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    policy_hashes: object | None
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]

    def as_legacy_dict(self) -> dict[str, object]:
        return {
            **self.base_context,
            "strategy": self.decision.strategy_name,
            "signal": self.decision.final_signal,
            "reason": self.decision.final_reason,
            "ts": int(self.candle_ts),
            "last_close": float(self.market_price),
        }


class _Adapter:
    def __init__(self, result: _RuntimeResult) -> None:
        self.strategy_name = result.decision.strategy_name
        self._result = result

    def decide(self, conn, *, short_n: int, long_n: int, through_ts_ms: int | None = None):
        del conn, short_n, long_n, through_ts_ms
        return self._result

    def typed_authority_required(self) -> bool:
        return True


def _decision(*, final_signal: str = "BUY", strategy_name: str = "sma_with_filter") -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name=strategy_name,
        raw_signal=final_signal,
        raw_reason=f"raw {final_signal}",
        entry_signal=final_signal,
        entry_reason=f"entry {final_signal}",
        exit_signal=final_signal,
        exit_reason=f"exit {final_signal}",
        final_signal=final_signal,
        final_reason=f"final {final_signal}",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=EntryExecutionIntent(
            side="BUY",
            intent="enter",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=70_000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal},
        policy_hash=f"sha256:policy-{strategy_name}",
        policy_contract_hash="sha256:contract",
        policy_input_hash=f"sha256:input-{strategy_name}",
        policy_decision_hash=f"sha256:decision-{strategy_name}-{final_signal}",
    )


def _runtime_result(signal: str, name: str, *, candle_ts: int = 123) -> _RuntimeResult:
    decision = _decision(final_signal=signal, strategy_name=name)
    return _RuntimeResult(
        decision=decision,
        base_context={
            "strategy": name,
            "signal": signal,
            "reason": decision.final_reason,
            "market_price": 100_000_000.0,
        },
        candle_ts=candle_ts,
        market_price=100_000_000.0,
        policy_hashes={
            "policy_contract_hash": decision.policy_contract_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
        },
        replay_fingerprint={"candle_ts": candle_ts, "strategy_name": name},
        boundary={"phase": "unit"},
    )


def _preference(signal: str, name: str) -> StrategyPreference:
    return strategy_decision_to_preference(
        _decision(final_signal=signal, strategy_name=name),
        pair="KRW-BTC",
        desired_exposure_krw=70_000.0,
    )


def _allocate(
    preferences: tuple[StrategyPreference, ...],
    *,
    config: PortfolioAllocatorConfig | None = None,
    previous_target_exposure_krw: float | None = 0.0,
    reference_price: float = 100_000_000.0,
):
    actual_config = config or PortfolioAllocatorConfig(target_exposure_krw=70_000.0)
    preference_set = SignalAggregator().aggregate(preferences)
    allocation_input = PortfolioAllocationInput(
        preference_set=preference_set,
        allocator_config=actual_config,
        previous_target_exposure_krw=previous_target_exposure_krw,
        reference_price=reference_price,
    )
    return PortfolioAllocator(actual_config).allocate(allocation_input)


def _readiness(*, broker_qty: float = 0.0) -> dict[str, object]:
    return {
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True},
        "broker_portfolio_converged": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "accounting_projection_ok": True,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "cash_available": 1_000_000.0,
    }


def test_strategy_preference_and_portfolio_target_hashes_are_deterministic() -> None:
    first = _preference("BUY", "strategy_a")
    second = _preference("BUY", "strategy_a")
    assert first.as_dict() == second.as_dict()
    assert first.content_hash() == second.content_hash()
    decision = _allocate((first,))
    target = decision.target_for_pair("KRW-BTC")
    assert isinstance(target, PortfolioTarget)
    assert target.as_dict() == decision.target_for_pair("KRW-BTC").as_dict()
    assert target.content_hash() == target.as_dict()["final_portfolio_target_hash"]


def test_allocator_hashes_change_with_config_and_strategy_contribution() -> None:
    preference = _preference("BUY", "strategy_a")
    first = _allocate((preference,), config=PortfolioAllocatorConfig(target_exposure_krw=70_000.0))
    changed_config = _allocate(
        (preference,),
        config=PortfolioAllocatorConfig(target_exposure_krw=80_000.0),
    )
    changed_contribution = _allocate((_preference("BUY", "strategy_b"),))
    assert first.allocator_config_hash != changed_config.allocator_config_hash
    assert first.content_hash() != changed_config.content_hash()
    assert first.strategy_contribution_hash != changed_contribution.strategy_contribution_hash
    assert first.content_hash() != changed_contribution.content_hash()


def test_single_strategy_decision_converts_to_preference_and_allocator_target() -> None:
    preference = strategy_decision_to_preference(
        _decision(final_signal="BUY"),
        pair="KRW-BTC",
        desired_exposure_krw=70_000.0,
    )
    assert preference.signal_direction == "BUY"
    assert preference.execution_intent_hint is not None
    assert preference.as_dict()["execution_intent_authority"] == "non_authoritative_strategy_hint"
    decision = _allocate((preference,))
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is True
    assert target.target_exposure_krw == pytest.approx(70_000.0)
    assert target.target_qty == pytest.approx(0.0007)


def test_runtime_strategy_set_resolver_defaults_to_strategy_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ACTIVE_STRATEGIES", raising=False)
    monkeypatch.delenv("RUNTIME_STRATEGY_SET_JSON", raising=False)
    old_strategy = settings.STRATEGY_NAME
    try:
        object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
        strategy_set = RuntimeStrategySetResolver().resolve()
    finally:
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy)
    assert strategy_set.source == "STRATEGY_NAME"
    assert strategy_set.multi_strategy_enabled is False
    assert [item.strategy_name for item in strategy_set.active_strategies] == ["sma_with_filter"]


def test_runtime_strategy_set_resolver_reads_structured_strategy_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "RUNTIME_STRATEGY_SET_JSON",
        """
        [
          {"strategy_name":"strategy_b","priority":20,"weight":2,"desired_exposure_krw":90000,"risk_budget_krw":50000},
          {"strategy_name":"strategy_a","priority":10,"weight":1,"desired_exposure_krw":30000,"risk_budget_krw":30000}
        ]
        """,
    )
    strategy_set = RuntimeStrategySetResolver().resolve()
    assert strategy_set.multi_strategy_enabled is True
    assert [item.strategy_name for item in strategy_set.active_strategies] == [
        "strategy_a",
        "strategy_b",
    ]
    assert strategy_set.spec_for_strategy("strategy_b").risk_budget_krw == pytest.approx(50_000.0)  # type: ignore[union-attr]


def test_multi_strategy_collector_executes_all_on_same_candle() -> None:
    reset_runtime_decision_adapters_for_tests()
    try:
        first = _runtime_result("BUY", "strategy_a")
        second = _runtime_result("HOLD", "strategy_b")
        register_runtime_decision_adapter("strategy_a", lambda: _Adapter(first))
        register_runtime_decision_adapter("strategy_b", lambda: _Adapter(second))
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(
                RuntimeStrategySpec("strategy_a", priority=10),
                RuntimeStrategySpec("strategy_b", priority=10),
            ),
        )
        bundle = RuntimeStrategyDecisionCollector().collect(
            object(),
            strategy_set,
            short_n=7,
            long_n=30,
            through_ts_ms=123,
        )
    finally:
        reset_runtime_decision_adapters_for_tests()
        reset_runtime_decision_adapter_bootstrap_for_tests()
    assert bundle is not None
    assert bundle.candle_ts == 123
    assert [result.decision.strategy_name for result in bundle.results] == [
        "strategy_a",
        "strategy_b",
    ]


def test_multi_strategy_equal_priority_buy_sell_conflict_fails_closed() -> None:
    decision = _allocate(
        (
            _preference("BUY", "strategy_a"),
            _preference("SELL", "strategy_b"),
            _preference("HOLD", "strategy_c"),
        )
    )
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert decision.content_hash() == _allocate(
        (
            _preference("BUY", "strategy_a"),
            _preference("SELL", "strategy_b"),
            _preference("HOLD", "strategy_c"),
        )
    ).content_hash()
    assert target.authoritative is False
    assert target.fail_closed_reason == "conflicting_equal_priority_signals"
    assert target.conflict_resolution["conflict_count"] == 1
    assert decision.primary_block_reason == "conflicting_equal_priority_signals"


def test_equal_priority_buy_sell_hold_conflict_metadata_is_persistable() -> None:
    decision = _allocate(
        (
            _preference("HOLD", "strategy_c"),
            _preference("SELL", "strategy_b"),
            _preference("BUY", "strategy_a"),
        )
    )
    payload = decision.as_dict()
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is False
    assert payload["primary_block_reason"] == "conflicting_equal_priority_signals"
    assert payload["conflict_resolution"]["conflict_count"] == 1
    assert payload["targets"][0]["conflict_resolution"]["selected_signals"] == [
        "BUY",
        "HOLD",
        "SELL",
    ]


@pytest.mark.parametrize(
    ("active_signal", "expected_exposure", "expected_reason"),
    [
        ("BUY", 70_000.0, "buy_weighted_target_from_allocator"),
        ("SELL", 0.0, "sell_target_zero_exposure"),
    ],
)
def test_equal_priority_hold_yields_to_active_signal_policy(
    active_signal: str,
    expected_exposure: float,
    expected_reason: str,
) -> None:
    decision = _allocate(
        (
            _preference("HOLD", "strategy_hold"),
            _preference(active_signal, "strategy_active"),
        ),
        previous_target_exposure_krw=42_000.0,
    )
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is True
    assert target.target_exposure_krw == pytest.approx(expected_exposure)
    assert target.reason == expected_reason
    assert target.conflict_resolution["mixed_hold_policy"] == "active_signal_over_hold"


def test_hold_without_previous_target_exposure_fails_closed() -> None:
    decision = _allocate(
        (_preference("HOLD", "strategy_hold"),),
        previous_target_exposure_krw=None,
    )
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is False
    assert target.fail_closed_reason == "hold_missing_previous_target_exposure"


def test_higher_priority_conflicting_strategy_wins_independent_of_input_order() -> None:
    config = PortfolioAllocatorConfig(
        target_exposure_krw=70_000.0,
        strategy_priorities={"strategy_buy": 20, "strategy_sell": 10},
    )
    first = _allocate(
        (
            _preference("BUY", "strategy_buy"),
            _preference("SELL", "strategy_sell"),
        ),
        config=config,
    )
    second = _allocate(
        (
            _preference("SELL", "strategy_sell"),
            _preference("BUY", "strategy_buy"),
        ),
        config=config,
    )
    assert first.content_hash() == second.content_hash()
    target = first.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is True
    assert target.target_exposure_krw == pytest.approx(0.0)
    assert target.conflict_resolution["selected_strategies"] == ["strategy_sell"]


def test_weights_and_risk_budgets_affect_buy_target_deterministically() -> None:
    first = strategy_decision_to_preference(
        _decision(final_signal="BUY", strategy_name="strategy_a"),
        pair="KRW-BTC",
        desired_exposure_krw=100_000.0,
        desired_weight=1.0,
        risk_budget_krw=60_000.0,
    )
    second = strategy_decision_to_preference(
        _decision(final_signal="BUY", strategy_name="strategy_b"),
        pair="KRW-BTC",
        desired_exposure_krw=40_000.0,
        desired_weight=3.0,
        risk_budget_krw=20_000.0,
    )
    decision = _allocate((first, second))
    target = decision.target_for_pair("KRW-BTC")
    assert target is not None
    assert target.authoritative is True
    assert target.target_exposure_krw == pytest.approx(55_000.0)
    assert decision.contributions[0].risk_budget_krw is not None


def test_target_delta_typed_planning_uses_allocator_portfolio_target(monkeypatch: pytest.MonkeyPatch) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        allocation = _allocate((_preference("BUY", "strategy_a"),))
        target = allocation.target_for_pair("KRW-BTC")
        assert target is not None
        summary = build_typed_execution_decision_summary(
            typed_input=TypedExecutionPlanningInput(
                strategy_decision=_decision(final_signal="HOLD", strategy_name="strategy_a"),
                candle_ts=123,
                market_price=100_000_000.0,
                readiness=ExecutionReadinessPlanningInput.from_payload(_readiness(broker_qty=0.0)),
                target=ExecutionTargetPlanningInput(
                    previous_target_exposure_krw=0.0,
                    portfolio_target=target,
                    portfolio_target_hash=target.content_hash(),
                    allocation_decision_hash=allocation.content_hash(),
                    allocator_config_hash=allocation.allocator_config_hash,
                    strategy_contribution_hash=allocation.strategy_contribution_hash,
                ),
            )
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)
    plan = summary.typed_target_submit_plan()
    assert plan is not None
    assert plan.source == "target_delta"
    assert plan.target_exposure_krw == pytest.approx(70_000.0)
    assert plan.extra_payload["portfolio_target_authoritative"] is True
    assert plan.extra_payload["portfolio_target_hash"] == target.content_hash()
    assert plan.extra_payload["allocation_decision_hash"] == allocation.content_hash()


def test_run_loop_single_strategy_path_passes_through_allocator() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    seen: dict[str, object] = {}

    def _summary_builder(**kwargs) -> ExecutionDecisionSummary:
        typed_input = kwargs["typed_input"]
        target = typed_input.target.portfolio_target
        seen["portfolio_target_present"] = target is not None
        seen["portfolio_target_authoritative"] = False if target is None else target.authoritative
        seen["portfolio_target_hash"] = typed_input.target.portfolio_target_hash
        return build_typed_execution_decision_summary(**kwargs)

    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {},
        },
        summary_builder=_summary_builder,
    )
    envelope = DecisionEnvelope(
        strategy_decision=_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=100_000_000.0,
        base_context={},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )
    bundle = planner.plan_envelope(object(), envelope, updated_ts=456)
    assert seen["portfolio_target_present"] is True
    assert seen["portfolio_target_authoritative"] is True
    assert str(seen["portfolio_target_hash"]).startswith("sha256:")
    assert bundle.persistence_context["portfolio_target_present"] is True
    assert bundle.persistence_context["portfolio_target_authoritative"] is True
    assert str(bundle.persistence_context["allocation_decision_hash"]).startswith("sha256:")
    assert bundle.persistence_context["allocation_contributions"]


def test_run_loop_multi_strategy_path_sends_multiple_preferences_to_allocator() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    seen: dict[str, object] = {}

    def _summary_builder(**kwargs) -> ExecutionDecisionSummary:
        typed_input = kwargs["typed_input"]
        seen["portfolio_target_present"] = typed_input.target.portfolio_target is not None
        seen["allocation_decision_hash"] = typed_input.target.allocation_decision_hash
        return build_typed_execution_decision_summary(**kwargs)

    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {},
        },
        summary_builder=_summary_builder,
    )
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec("strategy_buy", priority=10, desired_exposure_krw=60_000.0),
            RuntimeStrategySpec("strategy_hold", priority=10, desired_exposure_krw=60_000.0),
        ),
    )
    bundle = RuntimeStrategyDecisionResultBundle(
        strategy_set=strategy_set,
        results=(
            _runtime_result("HOLD", "strategy_hold"),
            _runtime_result("BUY", "strategy_buy"),
        ),
    )
    result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    assert seen["portfolio_target_present"] is True
    assert str(seen["allocation_decision_hash"]).startswith("sha256:")
    assert result.persistence_context["runtime_multi_strategy_enabled"] is True
    assert result.persistence_context["strategy_preference_count"] == 2
    assert len(result.persistence_context["allocation_contributions"]) == 2
    assert result.persistence_context["allocation_selected_signal"] == "BUY"
    assert result.persistence_context["allocation_selected_strategies"] == [
        "strategy_buy",
        "strategy_hold",
    ]


def test_run_loop_multi_strategy_conflict_fails_closed_without_submit() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
        )
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(
                RuntimeStrategySpec("strategy_buy", priority=10),
                RuntimeStrategySpec("strategy_sell", priority=10),
            ),
        )
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=strategy_set,
            results=(
                _runtime_result("BUY", "strategy_buy"),
                _runtime_result("SELL", "strategy_sell"),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
    assert result.submit_plan is not None
    assert result.submit_plan.submit_expected is False
    assert result.submit_plan.block_reason == "conflicting_equal_priority_signals"
    assert result.persistence_context["allocation_primary_block_reason"] == "conflicting_equal_priority_signals"
    assert result.persistence_context["allocation_conflict_count"] == 1


def test_target_delta_typed_planning_fails_closed_without_portfolio_target() -> None:
    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        summary = build_typed_execution_decision_summary(
            typed_input=TypedExecutionPlanningInput(
                strategy_decision=_decision(final_signal="BUY"),
                candle_ts=123,
                market_price=100_000_000.0,
                readiness=ExecutionReadinessPlanningInput.from_payload(_readiness(broker_qty=0.0)),
                target=ExecutionTargetPlanningInput(previous_target_exposure_krw=0.0),
            )
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
    plan = summary.typed_target_submit_plan()
    assert plan is not None
    assert plan.submit_expected is False
    assert plan.block_reason == "portfolio_target_missing"
    assert plan.extra_payload["portfolio_target_present"] is False


def test_target_delta_typed_planning_fails_closed_on_malformed_target_hash() -> None:
    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        allocation = _allocate((_preference("BUY", "strategy_a"),))
        target = allocation.target_for_pair("KRW-BTC")
        assert target is not None
        summary = build_typed_execution_decision_summary(
            typed_input=TypedExecutionPlanningInput(
                strategy_decision=_decision(final_signal="BUY", strategy_name="strategy_a"),
                candle_ts=123,
                market_price=100_000_000.0,
                readiness=ExecutionReadinessPlanningInput.from_payload(_readiness(broker_qty=0.0)),
                target=ExecutionTargetPlanningInput(
                    previous_target_exposure_krw=0.0,
                    portfolio_target=target,
                    portfolio_target_hash="sha256:bad",
                    allocation_decision_hash=allocation.content_hash(),
                    allocator_config_hash=allocation.allocator_config_hash,
                    strategy_contribution_hash=allocation.strategy_contribution_hash,
                ),
            )
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
    plan = summary.typed_target_submit_plan()
    assert plan is not None
    assert plan.submit_expected is False
    assert plan.block_reason == "portfolio_target_hash_mismatch"


def test_target_delta_typed_planning_fails_closed_on_non_authoritative_target() -> None:
    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        allocation = _allocate(
            (_preference("HOLD", "strategy_hold"),),
            previous_target_exposure_krw=None,
        )
        target = allocation.target_for_pair("KRW-BTC")
        assert target is not None
        summary = build_typed_execution_decision_summary(
            typed_input=TypedExecutionPlanningInput(
                strategy_decision=_decision(final_signal="HOLD", strategy_name="strategy_hold"),
                candle_ts=123,
                market_price=100_000_000.0,
                readiness=ExecutionReadinessPlanningInput.from_payload(_readiness(broker_qty=0.0)),
                target=ExecutionTargetPlanningInput(
                    previous_target_exposure_krw=None,
                    portfolio_target=target,
                    portfolio_target_hash=target.content_hash(),
                    allocation_decision_hash=allocation.content_hash(),
                    allocator_config_hash=allocation.allocator_config_hash,
                    strategy_contribution_hash=allocation.strategy_contribution_hash,
                ),
            )
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
    plan = summary.typed_target_submit_plan()
    assert plan is not None
    assert plan.submit_expected is False
    assert plan.block_reason == "hold_missing_previous_target_exposure"


def test_strategy_modules_do_not_import_execution_submit_authority() -> None:
    forbidden = {
        "ExecutionSubmitPlan",
        "SignalExecutionRequest",
        "LiveSignalExecutionService",
        "PaperSignalExecutionService",
        "live_execute_signal",
        "paper_execute",
    }
    strategy_roots = (
        Path("src/bithumb_bot/strategy"),
        Path("src/bithumb_bot/strategy_plugins"),
        Path("src/bithumb_bot/runtime_adapters"),
    )
    violations: list[str] = []
    for strategy_root in strategy_roots:
        paths = strategy_root.rglob("*.py") if strategy_root.exists() else ()
        for path in paths:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module in {
                    "bithumb_bot.execution_service",
                    "..execution_service",
                    ".execution_service",
                }:
                    names = {alias.name for alias in node.names}
                    blocked = sorted(names.intersection(forbidden))
                    if blocked:
                        violations.append(f"{path}:{','.join(blocked)}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "bithumb_bot.execution_service":
                            violations.append(f"{path}:bithumb_bot.execution_service")
    assert violations == []


def test_research_typed_planning_can_use_allocator_portfolio_target() -> None:
    allocation = _allocate((_preference("BUY", "research_strategy"),))
    target = allocation.target_for_pair("KRW-BTC")
    assert target is not None
    summary = build_typed_execution_decision_summary(
        typed_input=TypedExecutionPlanningInput(
            strategy_decision=_decision(final_signal="BUY", strategy_name="research_strategy"),
            candle_ts=123,
            market_price=100_000_000.0,
            readiness=ExecutionReadinessPlanningInput.from_payload(_readiness(broker_qty=0.0)),
            target=ExecutionTargetPlanningInput(
                previous_target_exposure_krw=0.0,
                portfolio_target=target,
                portfolio_target_hash=target.content_hash(),
                allocation_decision_hash=allocation.content_hash(),
                allocator_config_hash=allocation.allocator_config_hash,
                strategy_contribution_hash=allocation.strategy_contribution_hash,
            ),
        )
    )
    assert summary.typed_buy_submit_plan() is not None or summary.typed_target_submit_plan() is not None
