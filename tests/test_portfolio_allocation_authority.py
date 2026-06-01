from __future__ import annotations

import ast
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from bithumb_bot.db_core import (
    ensure_db,
    ensure_schema,
    rebuild_allocation_decision_from_bundle,
    rebuild_execution_submit_plan_from_execution_plan,
    rebuild_portfolio_target_from_allocation,
    record_execution_plan,
    record_portfolio_allocation_decision,
    record_runtime_strategy_decision_bundle,
    record_runtime_strategy_set_manifest,
    record_strategy_decision,
    replay_allocation_decision_hash,
    replay_allocation_decision_from_bundle,
    replay_execution_submit_plan_from_execution_plan,
    replay_execution_submit_plan_hash,
    replay_portfolio_target_from_allocation,
    replay_portfolio_target_hash,
    replay_runtime_strategy_set_manifest,
    replay_manifest_request_hashes,
)
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
from bithumb_bot.runtime_strategy_set import (
    derive_strategy_instance_id,
    RuntimeDecisionRequestBuilder,
    RuntimeMarketScope,
    RuntimeStrategyDecisionCollector,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    RuntimeStrategySetResolver,
    RuntimeStrategySpec,
    normalized_runtime_strategy_set_manifest,
)
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2


class _Readiness:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


def _complete_sma_parameters(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "SMA_SHORT": 2,
        "SMA_LONG": 5,
        "SMA_FILTER_GAP_MIN_RATIO": 0.9,
        "SMA_FILTER_VOL_WINDOW": 3,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.8,
        "SMA_FILTER_OVEREXT_LOOKBACK": 4,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.7,
        "SMA_MARKET_REGIME_ENABLED": False,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_COST_EDGE_MIN_RATIO": 0.6,
        "ENTRY_EDGE_BUFFER_RATIO": 0.5,
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.4,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": 33,
        "LIVE_FEE_RATE_ESTIMATE": 0.0123,
        "STRATEGY_EXIT_RULES": "opposite_cross",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 11,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.22,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.11,
    }
    params.update(overrides)
    return params


def _complete_canary_parameters(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "CANARY_ORDER_START_INDEX": 0,
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": "unit_canary",
    }
    params.update(overrides)
    return params


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

    def decide(self, conn, request):
        del conn, request
        return self._result

    def decide_feature_snapshot(self, request, feature_snapshot):
        del request, feature_snapshot
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


def _runtime_result(
    signal: str,
    name: str,
    *,
    candle_ts: int = 123,
    strategy_instance_id: str | None = None,
    spec: RuntimeStrategySpec | None = None,
) -> _RuntimeResult:
    decision = _decision(final_signal=signal, strategy_name=name)
    request_spec = spec or RuntimeStrategySpec(name, strategy_instance_id=strategy_instance_id)
    request = RuntimeDecisionRequestBuilder().build_for_spec(request_spec, through_ts_ms=candle_ts)
    instance_id = request.strategy_instance_id
    request_hash = request.request_hash
    return _RuntimeResult(
        decision=decision,
        base_context={
            "strategy": name,
            "signal": signal,
            "reason": decision.final_reason,
            "market_price": 100_000_000.0,
            "runtime_decision_request_hash": request_hash,
            "strategy_instance_id": instance_id,
            "strategy_parameters_hash": request.strategy_parameters_hash,
            "approved_profile_hash": request.approved_profile_hash,
            "runtime_contract_hash": request.runtime_contract_hash,
            "plugin_contract_hash": request.plugin_contract_hash,
            "through_ts_ms": candle_ts,
        },
        candle_ts=candle_ts,
        market_price=100_000_000.0,
        policy_hashes={
            "policy_contract_hash": decision.policy_contract_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
        },
        replay_fingerprint={
            "candle_ts": candle_ts,
            "strategy_name": name,
            "runtime_decision_request_hash": request_hash,
            "strategy_instance_id": instance_id,
            "strategy_parameters_hash": request.strategy_parameters_hash,
            "approved_profile_hash": request.approved_profile_hash,
            "runtime_contract_hash": request.runtime_contract_hash,
            "plugin_contract_hash": request.plugin_contract_hash,
            "through_ts_ms": candle_ts,
        },
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


def _runtime_data_conn():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    for idx in range(40):
        ts = 123 - (39 - idx) * 60_000
        close = 100_000_000.0 + idx
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, "KRW-BTC", "1m", close, close, close, close, 1.0),
        )
    conn.commit()
    return conn


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
          {"strategy_name":"canary_non_sma","priority":20,"weight":2,"desired_exposure_krw":90000,"risk_budget_krw":50000,
           "parameters":{"CANARY_ORDER_SIDE":"BUY","CANARY_ORDER_REASON":"canary_json"}},
          {"strategy_name":"sma_with_filter","priority":10,"weight":1,"desired_exposure_krw":30000,"risk_budget_krw":30000,
           "parameters":{"SMA_SHORT":7,"SMA_LONG":30}}
        ]
        """,
    )
    strategy_set = RuntimeStrategySetResolver().resolve()
    assert strategy_set.multi_strategy_enabled is True
    assert [item.strategy_name for item in strategy_set.active_strategies] == [
        "sma_with_filter",
        "canary_non_sma",
    ]
    canary_spec = strategy_set.spec_for_strategy("canary_non_sma")
    sma_spec = strategy_set.spec_for_strategy("sma_with_filter")
    assert canary_spec is not None
    assert sma_spec is not None
    assert canary_spec.risk_budget_krw == pytest.approx(50_000.0)
    assert dict(canary_spec.parameters) == {
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": "canary_json",
    }
    assert dict(sma_spec.parameters) == {"SMA_SHORT": 7, "SMA_LONG": 30}


def test_multi_strategy_collector_executes_all_on_same_candle() -> None:
    first = _runtime_result("BUY", "sma_with_filter")
    second = _runtime_result("HOLD", "canary_non_sma")
    adapters = {
        "sma_with_filter": _Adapter(first),
        "canary_non_sma": _Adapter(second),
    }
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec("sma_with_filter", priority=10, parameters=_complete_sma_parameters(SMA_SHORT=7, SMA_LONG=30)),
            RuntimeStrategySpec("canary_non_sma", priority=10),
        ),
    )
    bundle = RuntimeStrategyDecisionCollector(
        adapter_resolver=lambda strategy_name: adapters.get(str(strategy_name).strip().lower()),
    ).collect(
        _runtime_data_conn(),
        strategy_set,
        through_ts_ms=123,
    )
    assert bundle is not None
    assert bundle.candle_ts == 123
    assert [result.decision.strategy_name for result in bundle.results] == [
        "canary_non_sma",
        "sma_with_filter",
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


def test_risk_budget_is_not_silent_exposure_cap_without_declared_semantics() -> None:
    first = strategy_decision_to_preference(
        _decision(final_signal="BUY", strategy_name="strategy_a"),
        pair="KRW-BTC",
        desired_exposure_krw=100_000.0,
        desired_weight=1.0,
        risk_budget_krw=60_000.0,
    )
    decision = _allocate((first,))
    target = decision.target_for_pair("KRW-BTC")

    assert target is not None
    assert target.target_exposure_krw == pytest.approx(60_000.0)
    payload = decision.as_dict()
    assert payload["risk_budget_semantics"] == "max_target_exposure_cap"
    assert payload["targets"][0]["risk_budget_semantics"] == "max_target_exposure_cap"
    assert payload["contributions"][0]["max_target_exposure_krw"] == pytest.approx(60_000.0)
    assert payload["contributions"][0]["risk_budget_semantics"] == "max_target_exposure_cap"
    assert payload["targets"][0]["pre_cap_weighted_target_exposure_krw"] == pytest.approx(100_000.0)
    assert payload["targets"][0]["exposure_cap_krw"] == pytest.approx(60_000.0)
    assert payload["targets"][0]["exposure_cap_applied"] is True


def test_exposure_cap_alias_preserves_legacy_risk_budget_krw() -> None:
    preference = strategy_decision_to_preference(
        _decision(final_signal="BUY", strategy_name="strategy_a"),
        pair="KRW-BTC",
        desired_exposure_krw=100_000.0,
        risk_budget_krw=25_000.0,
    )

    assert preference.max_target_exposure_krw == pytest.approx(25_000.0)
    decision = _allocate((preference,))
    contribution = decision.as_dict()["contributions"][0]
    assert contribution["max_target_exposure_krw"] == pytest.approx(25_000.0)
    assert contribution["risk_budget_krw"] == pytest.approx(25_000.0)


def test_allocator_records_risk_budget_semantics() -> None:
    decision = _allocate(
        (
            strategy_decision_to_preference(
                _decision(final_signal="BUY", strategy_name="strategy_a"),
                pair="KRW-BTC",
                desired_exposure_krw=80_000.0,
                max_target_exposure_krw=50_000.0,
            ),
        )
    )
    payload = decision.as_dict()

    assert payload["risk_budget_semantics"] == "max_target_exposure_cap"
    assert payload["targets"][0]["exposure_cap_source"] == "max_target_exposure_krw"
    assert payload["targets"][0]["risk_budget_semantics"] == "max_target_exposure_cap"


def test_exposure_cap_limits_buy_target_with_declared_semantics() -> None:
    preference = strategy_decision_to_preference(
        _decision(final_signal="BUY", strategy_name="strategy_a"),
        pair="KRW-BTC",
        desired_exposure_krw=120_000.0,
        max_target_exposure_krw=30_000.0,
    )
    decision = _allocate((preference,))
    target = decision.target_for_pair("KRW-BTC")

    assert target is not None
    assert target.target_exposure_krw == pytest.approx(30_000.0)
    assert target.as_dict()["exposure_cap_applied"] is True
    assert target.as_dict()["risk_budget_semantics"] == "max_target_exposure_cap"


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
    assert str(bundle.persistence_context["allocator_config_hash"]).startswith("sha256:")
    assert str(bundle.persistence_context["strategy_contribution_hash"]).startswith("sha256:")
    assert bundle.persistence_context["allocation_selected_signal"] == "BUY"
    assert bundle.persistence_context["allocation_selected_strategies"] == ["sma_with_filter"]
    assert bundle.persistence_context["allocation_conflict_count"] == 0
    assert bundle.persistence_context["allocation_primary_block_reason"] == "none"
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
    buy_spec = RuntimeStrategySpec("canary_non_sma", priority=10, desired_exposure_krw=60_000.0)
    hold_spec = RuntimeStrategySpec("safe_hold", priority=10, desired_exposure_krw=60_000.0)
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(buy_spec, hold_spec),
    )
    bundle = RuntimeStrategyDecisionResultBundle(
        strategy_set=strategy_set,
        results=(
            _runtime_result("HOLD", "safe_hold", spec=hold_spec),
            _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
        ),
    )
    result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    assert seen["portfolio_target_present"] is True
    assert str(seen["allocation_decision_hash"]).startswith("sha256:")
    assert result.persistence_context["runtime_multi_strategy_enabled"] is True
    assert result.persistence_context["strategy_preference_count"] == 2
    assert str(result.persistence_context["allocation_decision_hash"]).startswith("sha256:")
    assert str(result.persistence_context["allocator_config_hash"]).startswith("sha256:")
    assert str(result.persistence_context["strategy_contribution_hash"]).startswith("sha256:")
    assert len(result.persistence_context["allocation_contributions"]) == 2
    assert result.persistence_context["allocation_selected_signal"] == "BUY"
    assert result.persistence_context["allocation_selected_strategies"] == [
        "canary_non_sma",
        "safe_hold",
    ]
    assert result.persistence_context["allocation_conflict_count"] == 0
    assert result.persistence_context["allocation_primary_block_reason"] == "none"
    assert len(result.persistence_context["runtime_strategy_result_contexts"]) == 2
    assert all(
        item["strategy_instance_id"]
        for item in result.persistence_context["runtime_strategy_result_contexts"]
    )


def test_run_loop_multi_strategy_allocator_signal_overrides_representative_hold() -> None:
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
        hold_spec = RuntimeStrategySpec(
            "safe_hold",
            strategy_instance_id="aaa_strategy_hold",
            priority=10,
            desired_exposure_krw=60_000.0,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="zzz_strategy_buy",
            priority=10,
            desired_exposure_krw=60_000.0,
        )
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(hold_spec, buy_spec),
        )
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=strategy_set,
            results=(
                _runtime_result("HOLD", "safe_hold", spec=hold_spec),
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert bundle.results[0].decision.final_signal == "HOLD"
    assert result.persistence_context["authoritative_execution_signal"] == "BUY"
    assert result.persistence_context["signal"] == "BUY"
    assert result.submit_plan is not None
    assert result.submit_plan.side == "BUY"
    assert result.submit_plan.submit_expected is True
    assert result.target_policy_metadata.get("target_origin") is None


def test_run_loop_multi_strategy_target_policy_uses_allocator_signal_not_representative() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    seen_signals: list[str] = []
    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")

        def _target_state_resolver(*_args, **kwargs) -> dict[str, object]:
            signal = str(kwargs["raw_signal"])
            seen_signals.append(signal)
            return {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {
                    "target_policy_signal": signal,
                    "target_origin": "allocator_selected_signal",
                },
            }

        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=_target_state_resolver,
        )
        hold_spec = RuntimeStrategySpec(
            "safe_hold",
            strategy_instance_id="aaa_strategy_hold",
            priority=10,
            desired_exposure_krw=60_000.0,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="zzz_strategy_buy",
            priority=10,
            desired_exposure_krw=60_000.0,
        )
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(hold_spec, buy_spec)),
            results=(
                _runtime_result("HOLD", "safe_hold", spec=hold_spec),
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert bundle.results[0].decision.final_signal == "HOLD"
    assert seen_signals == ["BUY"]
    assert result.persistence_context["target_policy_signal"] == "BUY"
    assert result.persistence_context["target_origin"] == "allocator_selected_signal"
    assert result.submit_plan is not None
    assert result.submit_plan.side == "BUY"


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
        buy_spec = RuntimeStrategySpec("canary_non_sma", priority=10)
        sell_spec = RuntimeStrategySpec(
            "sma_with_filter",
            priority=10,
            parameters=_complete_sma_parameters(SMA_SHORT=7, SMA_LONG=30),
        )
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(buy_spec, sell_spec),
        )
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=strategy_set,
            results=(
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
                _runtime_result("SELL", "sma_with_filter", spec=sell_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
    assert result.submit_plan is not None
    assert result.submit_plan.submit_expected is False
    assert result.submit_plan.block_reason == "conflicting_equal_priority_signals"
    assert str(result.persistence_context["allocation_decision_hash"]).startswith("sha256:")
    assert str(result.persistence_context["allocator_config_hash"]).startswith("sha256:")
    assert str(result.persistence_context["strategy_contribution_hash"]).startswith("sha256:")
    assert result.persistence_context["allocation_primary_block_reason"] == "conflicting_equal_priority_signals"
    assert result.persistence_context["allocation_conflict_count"] == 1
    assert result.persistence_context["allocation_selected_signal"] == ""
    assert result.persistence_context["allocation_selected_strategies"] == [
        "canary_non_sma",
        "sma_with_filter",
    ]
    assert result.persistence_context["authoritative_execution_signal"] == "HOLD"
    assert result.persistence_context["signal"] == "HOLD"
    assert result.persistence_context["final_reason"] == "conflicting_equal_priority_signals"


def test_single_pair_planner_rejects_multi_target_allocation_before_submit() -> None:
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
        btc_spec = RuntimeStrategySpec("canary_non_sma", strategy_instance_id="btc_buy", pair="KRW-BTC", priority=10)
        eth_spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="eth_hold", pair="KRW-ETH", priority=10)
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit_bypass", strategies=(btc_spec, eth_spec)),
            results=(
                _runtime_result("BUY", "canary_non_sma", spec=btc_spec),
                _runtime_result("HOLD", "safe_hold", spec=eth_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert result.submit_plan is None
    assert result.planning_error == "single_pair_allocation_target_count_mismatch"
    assert result.persistence_context["execution_block_reason"] == "single_pair_allocation_target_count_mismatch"
    assert result.persistence_context["allocation_target_count"] == 2


def test_single_pair_planner_rejects_target_pair_mismatch_before_submit() -> None:
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
        eth_spec = RuntimeStrategySpec("canary_non_sma", strategy_instance_id="eth_buy", pair="KRW-ETH", priority=10)
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit_bypass", strategies=(eth_spec,)),
            results=(_runtime_result("BUY", "canary_non_sma", spec=eth_spec),),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert result.submit_plan is None
    assert result.planning_error == "single_pair_allocation_target_pair_mismatch"
    assert result.persistence_context["execution_block_reason"] == "single_pair_allocation_target_pair_mismatch"
    assert result.persistence_context["allocation_target_pairs"] == ["KRW-ETH"]


def test_live_performance_gate_uses_allocator_selected_contributions_not_global_strategy() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    calls: list[tuple[str, str]] = []

    def _gate(_conn, *, strategy_name: str | None = None, pair: str | None = None):
        calls.append((str(strategy_name), str(pair)))
        return {
            "enabled": True,
            "allowed": True,
            "blocked": False,
            "reason_code": "STRATEGY_PERFORMANCE_OK",
            "reason": "ok",
            "recommended_next_action": "none",
            "summary": {"sample_count": 100, "expectancy_per_trade": 1.0, "net_pnl": 100.0},
            "thresholds": {"min_sample": 30},
        }

    old_values = {
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "MODE": settings.MODE,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "STRATEGY_NAME": settings.STRATEGY_NAME,
    }
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "STRATEGY_NAME", "global_should_not_be_used")
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            performance_gate_evaluator=_gate,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="selected_buy",
            priority=10,
            parameters=_complete_canary_parameters(),
        )
        hold_spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="selected_hold", priority=10)
        loser_spec = RuntimeStrategySpec("sma_with_filter", strategy_instance_id="unselected_sell", priority=20, parameters=_complete_sma_parameters())
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(buy_spec, hold_spec, loser_spec)),
            results=(
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
                _runtime_result("HOLD", "safe_hold", spec=hold_spec),
                _runtime_result("SELL", "sma_with_filter", spec=loser_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        for key, value in old_values.items():
            object.__setattr__(settings, key, value)

    assert result.submit_plan is not None
    assert result.planning_error is None
    assert result.persistence_context["execution_block_reason"] != "selected_strategy_performance_gate_blocked"
    assert calls == [("canary_non_sma", "KRW-BTC")]
    assert result.persistence_context["performance_gate_scope"]["selected_strategy_instance_ids"] == ["selected_buy"]
    assert result.persistence_context["performance_gate_scope"]["selected_strategy_names"] == ["canary_non_sma"]
    assert result.persistence_context["performance_gate_scope"]["selected_signal"] == "BUY"


def test_selected_buy_performance_gate_failure_blocks_before_submit_plan() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    def _gate(_conn, *, strategy_name: str | None = None, pair: str | None = None):
        return {
            "enabled": True,
            "allowed": str(strategy_name) != "canary_non_sma",
            "blocked": str(strategy_name) == "canary_non_sma",
            "reason_code": "STRATEGY_PERFORMANCE_BLOCKED:STRATEGY_SAMPLE_INSUFFICIENT",
            "reason": "sample_count=0 below min_sample=30",
            "recommended_next_action": "review strategy-report",
            "summary": {"sample_count": 0, "expectancy_per_trade": 0.0, "net_pnl": 0.0},
            "thresholds": {"min_sample": 30},
        }

    old_values = {
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "MODE": settings.MODE,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
    }
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            performance_gate_evaluator=_gate,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="selected_buy",
            priority=10,
            parameters=_complete_canary_parameters(),
        )
        hold_spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="selected_hold", priority=10)
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(buy_spec, hold_spec)),
            results=(
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
                _runtime_result("HOLD", "safe_hold", spec=hold_spec),
            ),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        for key, value in old_values.items():
            object.__setattr__(settings, key, value)

    assert result.submit_plan is None
    assert result.planning_error == "selected_strategy_performance_gate_blocked"
    assert result.persistence_context["execution_block_reason"] == "selected_strategy_performance_gate_blocked"
    assert result.persistence_context["performance_gate_scope"]["blocking_strategy_instance_ids"] == ["selected_buy"]
    assert result.persistence_context["strategy_performance_gate_reason_code"] == "STRATEGY_PERFORMANCE_BLOCKED:SELECTED_ALLOCATOR_CONTRIBUTION"


def test_performance_gate_threshold_changes_execution_plan_hash_when_blocking() -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    def _plan_with_min_sample(min_sample: int):
        def _gate(_conn, *, strategy_name: str | None = None, pair: str | None = None):
            del strategy_name, pair
            return {
                "enabled": True,
                "allowed": False,
                "blocked": True,
                "reason_code": "STRATEGY_PERFORMANCE_BLOCKED:STRATEGY_SAMPLE_INSUFFICIENT",
                "reason": f"sample_count=0 below min_sample={min_sample}",
                "recommended_next_action": "review strategy-report",
                "summary": {"sample_count": 0, "expectancy_per_trade": 0.0, "net_pnl": 0.0},
                "thresholds": {"min_sample": min_sample},
            }

        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            performance_gate_evaluator=_gate,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="selected_buy",
            priority=10,
            parameters=_complete_canary_parameters(),
        )
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(buy_spec,)),
            results=(_runtime_result("BUY", "canary_non_sma", spec=buy_spec),),
        )
        return planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)

    old_values = {
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "MODE": settings.MODE,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
    }
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        first = _plan_with_min_sample(30)
        second = _plan_with_min_sample(60)
    finally:
        for key, value in old_values.items():
            object.__setattr__(settings, key, value)

    assert first.planning_error == "selected_strategy_performance_gate_blocked"
    assert second.planning_error == "selected_strategy_performance_gate_blocked"
    assert first.content_hash() != second.content_hash()


@pytest.mark.parametrize("first_signal", ["BUY", "SELL", "HOLD"])
def test_multi_strategy_conflict_projection_never_uses_representative_signal(first_signal: str) -> None:
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
        first_spec = RuntimeStrategySpec(
            "canary_non_sma" if first_signal != "SELL" else "sma_with_filter",
            strategy_instance_id="aaa_first",
            priority=10,
            parameters=(
                _complete_sma_parameters(SMA_SHORT=7, SMA_LONG=30)
                if first_signal == "SELL"
                else None
            ),
        )
        buy_spec = RuntimeStrategySpec("canary_non_sma", strategy_instance_id="bbb_buy", priority=10)
        sell_spec = RuntimeStrategySpec(
            "sma_with_filter",
            strategy_instance_id="ccc_sell",
            priority=10,
            parameters=_complete_sma_parameters(SMA_SHORT=7, SMA_LONG=30),
        )
        results = [_runtime_result(first_signal, first_spec.strategy_name, spec=first_spec)]
        if first_signal != "BUY":
            results.append(_runtime_result("BUY", "canary_non_sma", spec=buy_spec))
        if first_signal != "SELL":
            results.append(_runtime_result("SELL", "sma_with_filter", spec=sell_spec))
        specs = (first_spec,) + (() if first_signal == "BUY" else (buy_spec,)) + (() if first_signal == "SELL" else (sell_spec,))
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=specs),
            results=tuple(results),
        )
        result = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert bundle.results[0].decision.final_signal == first_signal
    assert result.persistence_context["allocation_primary_block_reason"] == "conflicting_equal_priority_signals"
    assert result.persistence_context["submit_expected"] is False
    assert result.persistence_context["portfolio_target_authoritative"] is False
    assert result.persistence_context["authoritative_execution_signal"] == "HOLD"
    assert result.persistence_context["signal"] == "HOLD"
    assert result.submit_plan is not None
    assert result.submit_plan.submit_expected is False
    assert result.submit_plan.block_reason == "conflicting_equal_priority_signals"


def test_multi_strategy_execution_plan_hash_stable_when_input_result_order_changes() -> None:
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
        hold_spec = RuntimeStrategySpec(
            "safe_hold",
            strategy_instance_id="hold",
            priority=10,
            desired_exposure_krw=70_000.0,
        )
        buy_spec = RuntimeStrategySpec(
            "canary_non_sma",
            strategy_instance_id="buy",
            priority=10,
            desired_exposure_krw=70_000.0,
        )
        hold_result = _runtime_result("HOLD", "safe_hold", spec=hold_spec)
        buy_result = _runtime_result("BUY", "canary_non_sma", spec=buy_spec)
        first = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(hold_spec, buy_spec)),
            results=(hold_result, buy_result),
        )
        second = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(hold_spec, buy_spec)),
            results=(buy_result, hold_result),
        )
        first_plan = planner.plan_runtime_strategy_results(object(), first, updated_ts=456)
        second_plan = planner.plan_runtime_strategy_results(object(), second, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert first_plan.content_hash() == second_plan.content_hash()
    assert first_plan.submit_plan is not None
    assert first_plan.submit_plan.side == "BUY"


def test_multi_strategy_artifacts_persist_and_replay_without_strategy_context_json(tmp_path) -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {"target_origin": "runtime_state"},
            },
        )
        buy_spec = RuntimeStrategySpec("canary_non_sma", priority=10, desired_exposure_krw=70_000.0)
        hold_spec = RuntimeStrategySpec("safe_hold", priority=10, desired_exposure_krw=70_000.0)
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(buy_spec, hold_spec)),
            results=(
                _runtime_result("BUY", "canary_non_sma", spec=buy_spec),
                _runtime_result("HOLD", "safe_hold", spec=hold_spec),
            ),
        )
        plan = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    conn = ensure_db(str(tmp_path / "multi_strategy_artifacts.sqlite"))
    try:
        bundle_refs = record_runtime_strategy_decision_bundle(
            conn,
            result_bundle=bundle,
            pair="KRW-BTC",
            interval="1m",
            created_ts=456,
        )
        allocation_refs = record_portfolio_allocation_decision(
            conn,
            bundle_id=int(bundle_refs["runtime_strategy_decision_bundle_id"]),
            allocation_decision=plan.persistence_context["portfolio_allocation_decision"],  # type: ignore[arg-type]
        )
        execution_refs = record_execution_plan(
            conn,
            allocation_id=int(allocation_refs["portfolio_allocation_decision_id"]),
            portfolio_target_hash=str(allocation_refs["portfolio_target_hash"]),
            execution_plan_bundle=plan,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=456,
            strategy_name="multi_strategy",
            signal=str(plan.persistence_context["authoritative_execution_signal"]),
            reason=str(plan.persistence_context["final_reason"]),
            candle_ts=123,
            market_price=100_000_000.0,
            confidence=None,
            context=plan.persistence_context,
            runtime_strategy_decision_bundle_id=int(bundle_refs["runtime_strategy_decision_bundle_id"]),
            portfolio_allocation_decision_id=int(allocation_refs["portfolio_allocation_decision_id"]),
            portfolio_target_id=int(allocation_refs["portfolio_target_id"]),
            execution_plan_id=int(execution_refs["execution_plan_id"]),
            strategy_decision_projection_type="multi_strategy_compatibility_projection",
            strategy_decisions_authority="compatibility_projection_not_execution_authority",
        )
        conn.commit()

        assert (
            conn.execute("SELECT COUNT(*) FROM runtime_strategy_decision_bundle").fetchone()[0]
            == 1
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM runtime_strategy_decision_result").fetchone()[0]
            == 2
        )
        assert conn.execute("SELECT COUNT(*) FROM strategy_contribution").fetchone()[0] == 2
        joined = conn.execute(
            """
            SELECT b.bundle_hash, r.strategy_instance_id, r.final_signal,
                   a.selected_signal, t.final_portfolio_target_hash,
                   e.execution_submit_plan_hash, e.submit_plan_side, e.submit_plan_qty,
                   e.submit_plan_notional_krw, e.submit_plan_idempotency_key
            FROM strategy_decisions sd
            JOIN runtime_strategy_decision_bundle b ON b.id = sd.runtime_strategy_decision_bundle_id
            JOIN runtime_strategy_decision_result r ON r.bundle_id = b.id
            JOIN portfolio_allocation_decision a ON a.id = sd.portfolio_allocation_decision_id
            JOIN strategy_contribution c ON c.allocation_id = a.id AND c.strategy_instance_id = r.strategy_instance_id
            JOIN portfolio_target t ON t.id = sd.portfolio_target_id
            JOIN execution_plan e ON e.id = sd.execution_plan_id
            WHERE sd.id = ?
            ORDER BY r.strategy_instance_id
            """
            ,
            (decision_id,),
        ).fetchall()
        assert len(joined) == 2
        assert {row["final_signal"] for row in joined} == {"BUY", "HOLD"}
        assert {row["selected_signal"] for row in joined} == {"BUY"}
        assert str(joined[0]["bundle_hash"]).startswith("sha256:")
        assert str(joined[0]["final_portfolio_target_hash"]).startswith("sha256:")
        assert str(joined[0]["execution_submit_plan_hash"]).startswith("sha256:")
        assert {row["submit_plan_side"] for row in joined} == {"BUY"}
        assert all(float(row["submit_plan_qty"]) > 0.0 for row in joined)
        assert all(float(row["submit_plan_notional_krw"]) > 0.0 for row in joined)
        assert all(str(row["submit_plan_idempotency_key"]) for row in joined)
        assert replay_allocation_decision_hash(
            conn, int(allocation_refs["portfolio_allocation_decision_id"])
        ) == allocation_refs["allocation_decision_hash"]
        assert replay_portfolio_target_hash(
            conn, int(allocation_refs["portfolio_target_id"])
        ) == allocation_refs["portfolio_target_hash"]
        assert replay_execution_submit_plan_hash(
            conn, int(execution_refs["execution_plan_id"])
        ) == execution_refs["execution_submit_plan_hash"]
        assert replay_allocation_decision_from_bundle(
            conn, int(bundle_refs["runtime_strategy_decision_bundle_id"])
        ) == allocation_refs["allocation_decision_hash"]
        assert replay_portfolio_target_from_allocation(
            conn, int(allocation_refs["portfolio_allocation_decision_id"])
        ) == allocation_refs["portfolio_target_hash"]
        assert replay_execution_submit_plan_from_execution_plan(
            conn, int(execution_refs["execution_plan_id"])
        ) == execution_refs["execution_submit_plan_hash"]
        assert rebuild_allocation_decision_from_bundle(
            conn, int(bundle_refs["runtime_strategy_decision_bundle_id"])
        )["allocation_decision_hash"] == allocation_refs["allocation_decision_hash"]
        assert rebuild_portfolio_target_from_allocation(
            conn, int(allocation_refs["portfolio_allocation_decision_id"])
        )["final_portfolio_target_hash"] == allocation_refs["portfolio_target_hash"]
        assert rebuild_execution_submit_plan_from_execution_plan(
            conn, int(execution_refs["execution_plan_id"])
        )["side"] == "BUY"

        conn.execute(
            "UPDATE strategy_contribution SET signal_direction='SELL' WHERE strategy_name=?",
            ("canary_non_sma",),
        )
        with pytest.raises(RuntimeError, match="strategy_contribution_rebuild_hash_mismatch"):
            rebuild_allocation_decision_from_bundle(
                conn, int(bundle_refs["runtime_strategy_decision_bundle_id"])
            )
    finally:
        conn.close()


def test_runtime_manifest_is_persisted_at_run_start(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "manifest.sqlite"))
    try:
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="hold"),),
        )
        refs = record_runtime_strategy_set_manifest(conn, strategy_set=strategy_set, created_ts=123)
        row = conn.execute("SELECT * FROM runtime_strategy_set_manifest WHERE id=?", (refs["runtime_strategy_set_manifest_id"],)).fetchone()

        assert row is not None
        assert row["manifest_hash"] == refs["runtime_strategy_set_manifest_hash"]
        assert replay_runtime_strategy_set_manifest(conn, int(refs["runtime_strategy_set_manifest_id"])) == refs["runtime_strategy_set_manifest_hash"]
    finally:
        conn.close()


def test_run_start_persists_runtime_strategy_set_manifest_before_first_decision(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "run_start_manifest.sqlite"))
    try:
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="hold"),),
        )
        manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set, settings_obj=settings)

        refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=strategy_set,
            manifest_payload=manifest,
            settings_obj=settings,
            created_ts=111,
        )

        assert refs["runtime_strategy_set_manifest_hash"] == manifest["runtime_strategy_set_manifest_hash"]
        assert conn.execute("SELECT COUNT(*) FROM runtime_strategy_decision_bundle").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM runtime_strategy_set_manifest").fetchone()[0] == 1
    finally:
        conn.close()


def test_startup_blocked_run_still_records_strategy_set_manifest(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "startup_blocked_manifest.sqlite"))
    try:
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="hold"),),
        )
        manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set, settings_obj=settings)

        refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=strategy_set,
            manifest_payload=manifest,
            settings_obj=settings,
            created_ts=222,
        )

        assert replay_runtime_strategy_set_manifest(
            conn,
            int(refs["runtime_strategy_set_manifest_id"]),
        ) == manifest["runtime_strategy_set_manifest_hash"]
        assert conn.execute("SELECT COUNT(*) FROM runtime_strategy_decision_bundle").fetchone()[0] == 0
    finally:
        conn.close()


def test_decision_bundle_reuses_run_start_manifest_hash(tmp_path) -> None:
    spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="hold")
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set, settings_obj=settings)
    bundle = RuntimeStrategyDecisionResultBundle(
        strategy_set=strategy_set,
        results=(_runtime_result("HOLD", "safe_hold", spec=spec),),
    )
    conn = ensure_db(str(tmp_path / "bundle_reuses_manifest.sqlite"))
    try:
        run_start_refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=strategy_set,
            manifest_payload=manifest,
            settings_obj=settings,
            created_ts=333,
        )

        bundle_refs = record_runtime_strategy_decision_bundle(
            conn,
            result_bundle=bundle,
            pair="KRW-BTC",
            interval="1m",
            created_ts=444,
            manifest_payload=manifest,
            settings_obj=settings,
            runtime_strategy_set_manifest_id=int(run_start_refs["runtime_strategy_set_manifest_id"]),
            runtime_strategy_set_manifest_hash=str(run_start_refs["runtime_strategy_set_manifest_hash"]),
        )

        assert bundle_refs["runtime_strategy_set_manifest_id"] == run_start_refs["runtime_strategy_set_manifest_id"]
        assert bundle_refs["runtime_strategy_set_manifest_hash"] == run_start_refs["runtime_strategy_set_manifest_hash"]
        assert conn.execute("SELECT COUNT(*) FROM runtime_strategy_set_manifest").fetchone()[0] == 1
    finally:
        conn.close()


def test_record_runtime_strategy_set_manifest_uses_explicit_settings_context(tmp_path) -> None:
    cfg = replace(settings, INTERVAL="3m")
    strategy_set = RuntimeStrategySet(
        source="unit",
        market_scope=RuntimeMarketScope(pair=str(cfg.PAIR), interval="3m"),
        strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="hold", interval="3m"),),
    )
    conn = ensure_db(str(tmp_path / "explicit_settings_manifest.sqlite"))
    try:
        refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=strategy_set,
            settings_obj=cfg,
            created_ts=555,
        )

        row = conn.execute(
            "SELECT manifest_json FROM runtime_strategy_set_manifest WHERE id=?",
            (refs["runtime_strategy_set_manifest_id"],),
        ).fetchone()
        assert '"runtime_interval":"3m"' in row["manifest_json"]
        assert refs["runtime_strategy_set_manifest_hash"] == replay_runtime_strategy_set_manifest(
            conn,
            int(refs["runtime_strategy_set_manifest_id"]),
        )
    finally:
        conn.close()


def test_runtime_manifest_hash_mismatch_between_run_start_and_bundle_fails_closed(tmp_path) -> None:
    spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="hold")
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set, settings_obj=settings)
    bundle = RuntimeStrategyDecisionResultBundle(
        strategy_set=strategy_set,
        results=(_runtime_result("HOLD", "safe_hold", spec=spec),),
    )
    conn = ensure_db(str(tmp_path / "manifest_mismatch.sqlite"))
    try:
        record_runtime_strategy_set_manifest(
            conn,
            strategy_set=strategy_set,
            manifest_payload=manifest,
            settings_obj=settings,
            created_ts=666,
        )

        with pytest.raises(RuntimeError, match="runtime_strategy_set_manifest_hash_mismatch"):
            record_runtime_strategy_decision_bundle(
                conn,
                result_bundle=bundle,
                pair="KRW-BTC",
                interval="1m",
                created_ts=777,
                manifest_payload=manifest,
                settings_obj=settings,
                runtime_strategy_set_manifest_hash="sha256:wrong",
            )
    finally:
        conn.close()


def test_allocation_and_execution_plan_link_to_same_manifest_hash(tmp_path) -> None:
    from bithumb_bot.run_loop_execution_planner import ExecutionPlanner

    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(_readiness(broker_qty=0.0)),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {"target_origin": "runtime_state"},
            },
        )
        spec = RuntimeStrategySpec("canary_non_sma", priority=10, desired_exposure_krw=70_000.0)
        bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(spec,)),
            results=(_runtime_result("BUY", "canary_non_sma", spec=spec),),
        )
        plan = planner.plan_runtime_strategy_results(object(), bundle, updated_ts=456)
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    conn = ensure_db(str(tmp_path / "manifest_chain.sqlite"))
    try:
        bundle_refs = record_runtime_strategy_decision_bundle(
            conn, result_bundle=bundle, pair="KRW-BTC", interval="1m", created_ts=456
        )
        allocation_refs = record_portfolio_allocation_decision(
            conn,
            bundle_id=int(bundle_refs["runtime_strategy_decision_bundle_id"]),
            allocation_decision=plan.persistence_context["portfolio_allocation_decision"],  # type: ignore[arg-type]
        )
        execution_refs = record_execution_plan(
            conn,
            allocation_id=int(allocation_refs["portfolio_allocation_decision_id"]),
            portfolio_target_hash=str(allocation_refs["portfolio_target_hash"]),
            execution_plan_bundle=plan,
        )

        assert allocation_refs["runtime_strategy_set_manifest_hash"] == bundle_refs["runtime_strategy_set_manifest_hash"]
        assert execution_refs["runtime_strategy_set_manifest_hash"] == bundle_refs["runtime_strategy_set_manifest_hash"]
        allocation_json = conn.execute(
            "SELECT allocation_decision_json FROM portfolio_allocation_decision WHERE id=?",
            (allocation_refs["portfolio_allocation_decision_id"],),
        ).fetchone()[0]
        execution_json = conn.execute(
            "SELECT execution_plan_bundle_json FROM execution_plan WHERE id=?",
            (execution_refs["execution_plan_id"],),
        ).fetchone()[0]
        assert bundle_refs["runtime_strategy_set_manifest_hash"] in allocation_json
        assert bundle_refs["runtime_strategy_set_manifest_hash"] in execution_json
    finally:
        conn.close()


def test_manifest_contains_execution_and_risk_config_hashes(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "manifest_hashes.sqlite"))
    try:
        strategy_set = RuntimeStrategySet(
            source="unit",
            strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="hold"),),
        )
        refs = record_runtime_strategy_set_manifest(conn, strategy_set=strategy_set, created_ts=123)
        row = conn.execute(
            "SELECT execution_config_hash, risk_config_hash FROM runtime_strategy_set_manifest WHERE id=?",
            (refs["runtime_strategy_set_manifest_id"],),
        ).fetchone()

        assert str(row["execution_config_hash"]).startswith("sha256:")
        assert str(row["risk_config_hash"]).startswith("sha256:")
    finally:
        conn.close()


def test_runtime_manifest_replays_decision_request_hashes_exactly(tmp_path) -> None:
    spec = RuntimeStrategySpec("safe_hold", strategy_instance_id="hold")
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    expected = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=None)
    conn = ensure_db(str(tmp_path / "manifest_request_hashes.sqlite"))
    try:
        refs = record_runtime_strategy_set_manifest(conn, strategy_set=strategy_set, created_ts=123)

        replayed = replay_manifest_request_hashes(conn, int(refs["runtime_strategy_set_manifest_id"]))

        assert replayed == {"hold": expected.request_hash}
    finally:
        conn.close()


def test_replay_fails_when_manifest_strategy_instance_is_missing(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "manifest_missing.sqlite"))
    try:
        conn.execute(
            """
            INSERT INTO runtime_strategy_set_manifest(
                manifest_hash, source, market_scope_json, active_strategy_count,
                single_pair_runtime_enforced, execution_config_hash, risk_config_hash,
                manifest_json, created_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sha256:bad", "unit", "{}", 1, 1, "sha256:e", "sha256:r", '{"active_instances":[]}', 123),
        )
        manifest_id = conn.execute("SELECT id FROM runtime_strategy_set_manifest").fetchone()[0]
        with pytest.raises(RuntimeError, match="runtime_strategy_set_manifest_instances_missing"):
            replay_manifest_request_hashes(conn, int(manifest_id))
    finally:
        conn.close()


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


def test_decision_cycle_result_as_dict_exposes_top_level_multi_strategy_artifacts() -> None:
    from bithumb_bot.runtime.decision_coordinator import DecisionCycleResult

    result = DecisionCycleResult(
        candle_ts=123,
        strategy_name="multi_strategy",
        signal="BUY",
        reason="allocated",
        decision_id=42,
        decision_context={},
        execution_decision_summary=None,
        execution_plan_bundle=None,
        strategy_decision_hash="sha256:decision",
        execution_plan_bundle_hash="sha256:plan-bundle",
        persistence_status="persisted",
        mark_processed_candidate=True,
        runtime_strategy_decision_bundle_id=1,
        runtime_strategy_decision_bundle_hash="sha256:bundle",
        portfolio_allocation_decision_id=2,
        portfolio_allocation_decision_hash="sha256:allocation",
        portfolio_target_id=3,
        portfolio_target_hash="sha256:target",
        strategy_contribution_hash="sha256:contribution",
        execution_plan_id=4,
        execution_submit_plan_hash="sha256:submit",
    )

    payload = result.as_dict()
    assert payload["runtime_strategy_decision_bundle_id"] == 1
    assert payload["runtime_strategy_decision_bundle_hash"] == "sha256:bundle"
    assert payload["portfolio_allocation_decision_id"] == 2
    assert payload["portfolio_allocation_decision_hash"] == "sha256:allocation"
    assert payload["portfolio_target_id"] == 3
    assert payload["portfolio_target_hash"] == "sha256:target"
    assert payload["strategy_contribution_hash"] == "sha256:contribution"
    assert payload["execution_plan_id"] == 4
    assert payload["execution_plan_bundle_hash"] == "sha256:plan-bundle"
    assert payload["execution_submit_plan_hash"] == "sha256:submit"


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
