from __future__ import annotations

from collections.abc import Callable

import pytest

from bithumb_bot.config import settings
from bithumb_bot.broker import live as live_broker
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionObservabilityPayload,
    ExecutionSubmitPlan,
    LiveSignalExecutionService,
    PaperSignalExecutionService,
    SignalExecutionRequest,
    TypedExecutionRequest,
    validate_execution_submit_plan_payload,
)
from bithumb_bot.submit_authority_policy import evaluate_submit_authority_policy
from bithumb_bot.research.backtest_kernel import ResearchExecutionContext, ResearchVirtualExecutionService
from bithumb_bot.research.execution_model import FixedBpsExecutionModel


@pytest.fixture(autouse=True)
def _restore_execution_settings():
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "RESIDUAL_LIVE_SELL_MODE": settings.RESIDUAL_LIVE_SELL_MODE,
    }
    yield
    for key, value in original.items():
        object.__setattr__(settings, key, value)


def _arm_live_real_orders(*, engine: str = "lot_native") -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", engine)


def _service(calls: list[dict[str, object]]) -> LiveSignalExecutionService:
    def _executor(_broker, signal, ts, market_price, **kwargs):
        calls.append(
            {
                "signal": signal,
                "ts": ts,
                "market_price": market_price,
                "kwargs": dict(kwargs),
            }
        )
        return {"status": "submitted", "signal": signal}

    return LiveSignalExecutionService(
        broker=object(),
        executor=_executor,
        harmless_dust_recorder=lambda **_kwargs: False,
    )


def _paper_service(calls: list[dict[str, object]]) -> PaperSignalExecutionService:
    def _executor(signal, ts, market_price, **kwargs):
        calls.append(
            {
                "signal": signal,
                "ts": ts,
                "market_price": market_price,
                "kwargs": dict(kwargs),
            }
        )
        return {"status": "submitted", "signal": signal}

    return PaperSignalExecutionService(executor=_executor)


def _valid_target_submit_plan() -> dict[str, object]:
    return {
        "side": "BUY",
        "source": "target_delta",
        "authority": "canonical_target_delta_sizing",
        "final_action": "REBALANCE_TO_TARGET",
        "qty": 0.001,
        "notional_krw": 100_000.0,
        "target_exposure_krw": 100_000.0,
        "current_effective_exposure_krw": 0.0,
        "delta_krw": 100_000.0,
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "idempotency_key": "target-plan-key",
        "portfolio_target_authoritative": True,
        "portfolio_target_hash": "sha256:portfolio-target",
        "allocation_decision_hash": "sha256:allocation-decision",
        "allocator_config_hash": "sha256:allocator-config",
        "strategy_contribution_hash": "sha256:strategy-contribution",
        "allocator_policy": "deterministic_priority_target_v1:1",
        "allocator_reason": "buy_target_from_allocator",
        "allocation_conflict_count": 0,
        "allocation_primary_block_reason": "none",
    }


def _valid_residual_submit_plan() -> dict[str, object]:
    return {
        "side": "SELL",
        "source": "residual_inventory",
        "authority": "residual_inventory_policy",
        "final_action": "CLOSE_RESIDUAL_CANDIDATE",
        "qty": 0.0005,
        "notional_krw": 55_000.0,
        "target_exposure_krw": None,
        "current_effective_exposure_krw": None,
        "delta_krw": None,
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "idempotency_key": "residual-plan-key",
    }


def _valid_buy_submit_plan() -> dict[str, object]:
    return {
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
        "idempotency_key": "buy-plan-key",
    }


def _typed_plan(payload: dict[str, object]) -> ExecutionSubmitPlan:
    required = {
        "side",
        "source",
        "authority",
        "final_action",
        "qty",
        "notional_krw",
        "target_exposure_krw",
        "current_effective_exposure_krw",
        "delta_krw",
        "submit_expected",
        "pre_submit_proof_status",
        "block_reason",
        "idempotency_key",
    }
    return ExecutionSubmitPlan(
        side=str(payload["side"]),
        source=str(payload["source"]),
        authority=str(payload["authority"]),
        final_action=str(payload["final_action"]),
        qty=payload["qty"],  # type: ignore[arg-type]
        notional_krw=payload["notional_krw"],  # type: ignore[arg-type]
        target_exposure_krw=payload["target_exposure_krw"],  # type: ignore[arg-type]
        current_effective_exposure_krw=payload["current_effective_exposure_krw"],  # type: ignore[arg-type]
        delta_krw=payload["delta_krw"],  # type: ignore[arg-type]
        submit_expected=bool(payload["submit_expected"]),
        pre_submit_proof_status=str(payload["pre_submit_proof_status"]),
        block_reason=str(payload["block_reason"]),
        idempotency_key=payload["idempotency_key"],  # type: ignore[arg-type]
        extra_payload={key: value for key, value in payload.items() if key not in required},
    )


class _PolicySettings:
    def __init__(
        self,
        *,
        mode: str = "paper",
        dry_run: bool = True,
        armed: bool = False,
        engine: str = "lot_native",
        residual_mode: str = "block",
    ) -> None:
        self.MODE = mode
        self.LIVE_DRY_RUN = dry_run
        self.LIVE_REAL_ORDER_ARMED = armed
        self.EXECUTION_ENGINE = engine
        self.RESIDUAL_LIVE_SELL_MODE = residual_mode


@pytest.mark.parametrize(
    ("settings_obj", "plan", "plan_kind", "allowed", "reason"),
    [
        (
            _PolicySettings(mode="paper", dry_run=True, armed=False, engine="lot_native"),
            _valid_buy_submit_plan(),
            "buy",
            True,
            "allowed_mode_compatibility",
        ),
        (
            _PolicySettings(mode="live", dry_run=True, armed=False, engine="target_delta"),
            _valid_target_submit_plan(),
            "target",
            False,
            "live_dry_run_non_submitting",
        ),
        (
            _PolicySettings(mode="live", dry_run=False, armed=True, engine="target_delta"),
            _valid_target_submit_plan(),
            "target",
            True,
            "allowed_target_delta",
        ),
        (
            _PolicySettings(mode="live", dry_run=False, armed=True, engine="target_delta"),
            _valid_buy_submit_plan(),
            "buy",
            False,
            "live_real_order_buy_plan_rejected_target_delta_required",
        ),
        (
            _PolicySettings(
                mode="live",
                dry_run=False,
                armed=True,
                engine="target_delta",
                residual_mode="enabled",
            ),
            _valid_residual_submit_plan(),
            "residual",
            True,
            "allowed_residual_inventory_policy",
        ),
        (
            _PolicySettings(
                mode="live",
                dry_run=False,
                armed=True,
                engine="target_delta",
                residual_mode="telemetry",
            ),
            _valid_residual_submit_plan(),
            "residual",
            False,
            "live_real_order_residual_policy_not_enabled",
        ),
    ],
)
def test_submit_authority_policy_matrix_is_mode_aware(
    settings_obj: _PolicySettings,
    plan: dict[str, object],
    plan_kind: str,
    allowed: bool,
    reason: str,
) -> None:
    decision = evaluate_submit_authority_policy(
        plan,
        settings_obj=settings_obj,
        plan_kind=plan_kind,
    )

    assert decision.allowed is allowed
    assert decision.reason == reason


def test_submit_authority_policy_rejects_live_target_delta_without_provenance() -> None:
    plan = {**_valid_target_submit_plan(), "portfolio_target_hash": ""}
    decision = evaluate_submit_authority_policy(
        plan,
        settings_obj=_PolicySettings(mode="live", dry_run=False, armed=True, engine="target_delta"),
        plan_kind="target",
    )

    assert decision.allowed is False
    assert decision.reason == "live_real_order_target_plan_missing_portfolio_target_hash"


def _typed_target_execution_summary() -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=_typed_plan(_valid_target_submit_plan()),
    )


def _typed_buy_execution_summary(
    *,
    plan: dict[str, object] | None = None,
    submit_expected: bool | None = None,
    proof_status: str | None = None,
    block_reason: str | None = None,
) -> ExecutionDecisionSummary:
    payload = _valid_buy_submit_plan() if plan is None else dict(plan)
    if submit_expected is not None:
        payload["submit_expected"] = submit_expected
    if proof_status is not None:
        payload["pre_submit_proof_status"] = proof_status
    if block_reason is not None:
        payload["block_reason"] = block_reason
    return ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action=str(payload["final_action"]),
        submit_expected=bool(payload["submit_expected"]),
        pre_submit_proof_status=str(payload["pre_submit_proof_status"]),
        block_reason=str(payload["block_reason"]),
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=_typed_plan(payload),
        target_shadow_decision=None,
        target_submit_plan=None,
    )


def _forged_summary_with_raw_plan(*, field_name: str, plan: dict[str, object]) -> ExecutionDecisionSummary:
    summary = object.__new__(ExecutionDecisionSummary)
    values = {
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "final_action": "REBALANCE_TO_TARGET",
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "strategy_sell_candidate": None,
        "residual_sell_candidate": None,
        "target_exposure_krw": 100_000.0,
        "current_effective_exposure_krw": 0.0,
        "tracked_residual_exposure_krw": None,
        "buy_delta_krw": 100_000.0,
        "residual_live_sell_mode": "enabled",
        "residual_buy_sizing_mode": "block",
        "residual_submit_plan": None,
        "buy_submit_plan": None,
        "target_shadow_decision": None,
        "target_submit_plan": None,
        "pre_trade_economics": None,
        "signal_flow": None,
    }
    values[field_name] = plan
    for key, value in values.items():
        object.__setattr__(summary, key, value)
    return summary


def _typed_residual_execution_summary() -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal="SELL",
        final_signal="SELL",
        final_action="CLOSE_RESIDUAL_CANDIDATE",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=None,
        current_effective_exposure_krw=None,
        tracked_residual_exposure_krw=55_000.0,
        buy_delta_krw=None,
        residual_live_sell_mode="enabled",
        residual_buy_sizing_mode="block",
        residual_submit_plan=_typed_plan(_valid_residual_submit_plan()),
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=None,
    )


def test_execution_submit_plan_final_payload_validates_after_extra_fields() -> None:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="target-plan-key",
    )

    payload = plan.as_final_payload(extra={"intent_type": "target_delta_rebalance"})
    assert payload["intent_type"] == "target_delta_rebalance"
    assert payload["schema_version"] == 1
    assert payload["authority_label"] == "ExecutionSubmitPlan.final_payload.v1"
    assert str(payload["content_hash"]).startswith("sha256:")

    invalid = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="failed",
        block_reason="none",
        idempotency_key="target-plan-key",
    )
    with pytest.raises(ValueError, match="execution_submit_plan_schema_submit_expected_with_failed_proof"):
        invalid.as_final_payload()


def test_production_execution_request_is_typed_only() -> None:
    assert PaperSignalExecutionService.execute.__annotations__["request"] == "TypedExecutionRequest"
    assert LiveSignalExecutionService.execute.__annotations__["request"] == "TypedExecutionRequest"

    typed = TypedExecutionRequest(
        signal="BUY",
        ts=123,
        market_price=100_000_000.0,
        execution_decision_summary=_typed_buy_execution_summary(submit_expected=False, block_reason="blocked"),
        observability_payload=ExecutionObservabilityPayload({"execution_decision": {"dict": "observability_only"}}),
    )
    assert typed.observability_payload is not None
    assert typed.observability_payload.as_dict()["execution_decision"] == {"dict": "observability_only"}


def test_paper_target_delta_fails_closed_on_scalar_signal_submit_plan_mismatch() -> None:
    calls: list[dict[str, object]] = []
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    service = _paper_service(calls)

    result = service.execute(
        TypedExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=100_000_000.0,
            strategy_name="multi_strategy",
            execution_decision_summary=_typed_target_execution_summary(),
        )
    )

    assert result is None
    assert calls == []


def test_live_target_delta_fails_closed_on_scalar_signal_submit_plan_mismatch() -> None:
    calls: list[dict[str, object]] = []
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    service = _service(calls)

    result = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=100_000_000.0,
            strategy_name="multi_strategy",
            execution_decision_summary=_typed_target_execution_summary(),
            observability_payload=ExecutionObservabilityPayload({}),
        )
    )

    assert result is None
    assert calls == []


def test_final_submit_payload_requires_typed_serialization_proof() -> None:
    raw_plan = _valid_buy_submit_plan()
    with pytest.raises(ValueError, match="execution_submit_plan_schema_missing_fields:authority_label,content_hash,schema_version"):
        validate_execution_submit_plan_payload(
            raw_plan,
            field_name="execution_submit_plan",
            require_final_payload=True,
        )

    final_payload = _typed_plan(raw_plan).as_final_payload()
    tampered = dict(final_payload)
    tampered["qty"] = 0.002
    with pytest.raises(ValueError, match="execution_submit_plan_schema_content_hash_mismatch"):
        validate_execution_submit_plan_payload(
            tampered,
            field_name="execution_submit_plan",
            require_final_payload=True,
        )


@pytest.mark.parametrize(
    ("plan_factory", "field_name", "mutate", "expected_reason"),
    [
        (
            _valid_target_submit_plan,
            "target_submit_plan",
            lambda plan: plan.pop("notional_krw"),
            "target_submit_plan_schema_missing_fields:notional_krw",
        ),
        (
            _valid_residual_submit_plan,
            "residual_submit_plan",
            lambda plan: plan.pop("notional_krw"),
            "residual_submit_plan_schema_missing_fields:notional_krw",
        ),
        (
            _valid_target_submit_plan,
            "target_submit_plan",
            lambda plan: plan.update({"side": "CANCEL"}),
            "target_submit_plan_schema_invalid_side:CANCEL",
        ),
        (
            _valid_target_submit_plan,
            "target_submit_plan",
            lambda plan: plan.update({"block_reason": ""}),
            "target_submit_plan_schema_missing_block_reason",
        ),
        (
            _valid_target_submit_plan,
            "target_submit_plan",
            lambda plan: plan.update({"pre_submit_proof_status": "failed"}),
            "target_submit_plan_schema_submit_expected_with_failed_proof",
        ),
    ],
)
def test_malformed_explicit_submit_plan_blocks_executor_without_fallback(
    caplog: pytest.LogCaptureFixture,
    plan_factory: Callable[[], dict[str, object]],
    field_name: str,
    mutate: Callable[[dict[str, object]], object],
    expected_reason: str,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    plan = plan_factory()
    mutate(plan)
    calls: list[dict[str, object]] = []

    with pytest.raises(TypeError, match="decision_context_not_execution_authority"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": {field_name: plan}},
        )

    assert calls == []


def test_explicit_plan_present_but_not_consumed_does_not_call_executor(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_target_execution_summary(),
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_requires_execution_engine_target_delta" in caplog.text


def test_valid_target_plan_reaches_executor_only_for_target_delta_engine() -> None:
    calls: list[dict[str, object]] = []
    _arm_live_real_orders(engine="lot_native")

    blocked = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_target_execution_summary(),
        )
    )
    assert blocked is None
    assert calls == []

    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=124,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_target_execution_summary(),
        )
    )

    assert submitted == {"status": "submitted", "signal": "BUY"}
    assert len(calls) == 1
    assert calls[0]["kwargs"]["execution_submit_plan"]["source"] == "target_delta"  # type: ignore[index]


def test_typed_execution_summary_can_supply_validated_target_submit_plan() -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    summary = _typed_target_execution_summary()

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert submitted == {"status": "submitted", "signal": "BUY"}
    assert len(calls) == 1
    assert calls[0]["kwargs"]["execution_submit_plan"] == summary.target_submit_plan.as_final_payload()  # type: ignore[index,union-attr]
    assert calls[0]["kwargs"]["execution_submit_plan"]["submit_plan_hash"] == summary.target_submit_plan.content_hash()  # type: ignore[index,union-attr]


def test_execution_intent_telemetry_does_not_change_live_target_delta_submit_size() -> None:
    _arm_live_real_orders(engine="target_delta")
    first_calls: list[dict[str, object]] = []
    second_calls: list[dict[str, object]] = []
    summary = _typed_target_execution_summary()

    first = _service(first_calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
            observability_payload=ExecutionObservabilityPayload(
                {
                    "execution_intent": {
                        "intent": "enter_strategy_position",
                        "budget_model": "cash_fraction_capped_by_max_order_krw",
                        "budget_fraction_of_cash": 0.99,
                        "max_budget_krw": 999_000_000.0,
                        "qty": 999.0,
                        "notional_krw": 999_000_000.0,
                    }
                }
            ),
        )
    )
    second = _service(second_calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=124,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
            observability_payload=ExecutionObservabilityPayload(
                {
                    "execution_intent": {
                        "intent": "enter_strategy_position",
                        "budget_model": "cash_fraction_capped_by_max_order_krw",
                        "budget_fraction_of_cash": 0.01,
                        "max_budget_krw": 1.0,
                        "qty": 0.00000001,
                        "notional_krw": 1.0,
                    }
                }
            ),
        )
    )

    assert first == {"status": "submitted", "signal": "BUY"}
    assert second == {"status": "submitted", "signal": "BUY"}
    first_plan = first_calls[0]["kwargs"]["execution_submit_plan"]  # type: ignore[index]
    second_plan = second_calls[0]["kwargs"]["execution_submit_plan"]  # type: ignore[index]
    assert first_plan["source"] == "target_delta"
    assert second_plan["source"] == "target_delta"
    assert first_plan["qty"] == second_plan["qty"] == pytest.approx(0.001)
    assert first_plan["notional_krw"] == second_plan["notional_krw"] == pytest.approx(100_000.0)
    assert first_plan["target_exposure_krw"] == second_plan["target_exposure_krw"] == pytest.approx(100_000.0)
    assert first_plan["delta_krw"] == second_plan["delta_krw"] == pytest.approx(100_000.0)


def test_live_dry_run_target_delta_validates_but_does_not_call_executor(
    caplog: pytest.LogCaptureFixture,
) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    calls: list[dict[str, object]] = []

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_target_execution_summary(),
        )
    )

    assert submitted is None
    assert calls == []
    assert "live_dry_run_non_submitting" in caplog.text


def test_live_real_order_lot_native_typed_buy_submit_plan_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []
    summary = _typed_buy_execution_summary()

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert submitted is None
    assert calls == []
    assert "live_real_order_requires_execution_engine_target_delta" in caplog.text


def test_live_real_order_target_delta_blocks_strategy_position_buy_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_buy_execution_summary(),
        )
    )

    assert submitted is None
    assert calls == []
    assert "live_real_order_buy_plan_rejected_target_delta_required" in caplog.text


@pytest.mark.parametrize(
    "authority",
    [
        "residual_inventory_delta",
        "strategy_execution_intent",
        "research_compatibility_execution_intent",
    ],
)
def test_live_real_order_blocks_non_target_buy_authorities(
    caplog: pytest.LogCaptureFixture,
    authority: str,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    plan = {**_valid_buy_submit_plan(), "authority": authority}
    summary = _typed_buy_execution_summary(plan=plan)

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert submitted is None
    assert calls == []
    assert "live_real_order_buy_plan_rejected_target_delta_required" in caplog.text


@pytest.mark.parametrize(
    ("mutate", "expected_reason"),
    [
        (
            lambda plan: plan.update({"source": "legacy_context"}),
            "execution_submit_plan_schema_invalid_source",
        ),
        (
            lambda plan: plan.update({"authority": "legacy_qty"}),
            "execution_submit_plan_schema_invalid_authority",
        ),
        (
            lambda plan: plan.update({"block_reason": "entry_filter_blocked", "submit_expected": False}),
            "buy_submit_plan_blocked",
        ),
        (
            lambda plan: plan.update({"pre_submit_proof_status": "failed", "submit_expected": False}),
            "buy_submit_plan_pre_submit_proof_not_compatible",
        ),
        (
            lambda plan: plan.update({"qty": 0.0}),
            "buy_submit_plan_non_positive_size",
        ),
        (
            lambda plan: plan.update({"notional_krw": 0.0}),
            "buy_submit_plan_non_positive_size",
        ),
    ],
)
def test_lot_native_typed_buy_submit_plan_invalid_cases_fail_closed(
    caplog: pytest.LogCaptureFixture,
    mutate: Callable[[dict[str, object]], object],
    expected_reason: str,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []
    plan = _valid_buy_submit_plan()
    mutate(plan)
    if expected_reason in {"execution_submit_plan_schema_invalid_source", "execution_submit_plan_schema_invalid_authority"}:
        construction_reason = expected_reason.replace("execution_submit_plan_", "buy_submit_plan_")
        with pytest.raises(ValueError, match=construction_reason):
            _typed_buy_execution_summary(plan=plan)
        summary = _forged_summary_with_raw_plan(field_name="buy_submit_plan", plan=_typed_plan(plan))  # type: ignore[arg-type]
    else:
        summary = _typed_buy_execution_summary(plan=plan)

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert expected_reason in caplog.text or "live_real_order_requires_execution_engine_target_delta" in caplog.text


def test_lot_native_typed_buy_submit_plan_mismatch_with_context_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []
    summary = _typed_buy_execution_summary()
    mismatched = summary.as_dict()
    buy_plan = dict(mismatched["buy_submit_plan"])  # type: ignore[arg-type]
    buy_plan["notional_krw"] = 200_000.0
    mismatched["buy_submit_plan"] = buy_plan

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": mismatched},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "execution_decision_summary_context_mismatch" in caplog.text


def test_target_delta_live_real_order_requires_passed_target_pre_submit_proof(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    plan = _typed_plan({**_valid_target_submit_plan(), "pre_submit_proof_status": "not_required"})
    summary = ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="not_required",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=plan,
    )

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "target_delta_pre_submit_proof_not_passed" in caplog.text


def test_typed_execution_summary_mismatch_with_context_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    summary = _typed_target_execution_summary()
    mismatched = summary.as_dict()
    target_plan = dict(mismatched["target_submit_plan"])  # type: ignore[arg-type]
    target_plan["qty"] = 0.002
    mismatched["target_submit_plan"] = target_plan

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": mismatched},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "execution_decision_summary_context_mismatch" in caplog.text


def test_observability_payload_is_non_authoritative_and_checked_for_tampering(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    summary = _typed_target_execution_summary()
    tampered = summary.as_dict()
    target_plan = dict(tampered["target_submit_plan"])  # type: ignore[arg-type]
    target_plan["source"] = "legacy_context"
    tampered["target_submit_plan"] = target_plan

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            observability_payload={"execution_decision": tampered},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "execution_decision_summary_context_mismatch" in caplog.text


@pytest.mark.parametrize(
    ("mode", "dry_run", "armed", "expected_calls"),
    [
        ("telemetry", False, True, 0),
        ("enabled", True, True, 0),
        ("enabled", False, False, 0),
        ("enabled", False, True, 1),
    ],
)
def test_valid_residual_plan_reaches_executor_only_when_residual_live_submit_is_enabled(
    mode: str,
    dry_run: bool,
    armed: bool,
    expected_calls: int,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", mode)
    object.__setattr__(settings, "LIVE_DRY_RUN", dry_run)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", armed)
    calls: list[dict[str, object]] = []

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=_typed_residual_execution_summary(),
        )
    )

    assert len(calls) == expected_calls
    if expected_calls:
        assert result == {"status": "submitted", "signal": "SELL"}
        assert calls[0]["kwargs"]["execution_submit_plan"]["source"] == "residual_inventory"  # type: ignore[index]
    else:
        assert result is None


@pytest.mark.parametrize(
    ("decision_context", "expected_reason"),
    [
        (None, "live_real_order_missing_typed_execution_summary"),
        ({}, "live_real_order_missing_typed_execution_summary"),
        ({"execution_decision": {}}, "decision_context_not_execution_authority"),
    ],
)
def test_missing_execution_plan_contract_fails_closed_in_live_real_order_mode(
    caplog: pytest.LogCaptureFixture,
    decision_context: dict[str, object] | None,
    expected_reason: str,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []

    with pytest.raises(TypeError, match=expected_reason):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context=decision_context,
        )

    assert calls == []


def test_live_real_order_blocks_dict_only_execution_decision_even_with_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []

    with pytest.raises(TypeError, match="decision_context_not_execution_authority"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": {"target_submit_plan": _valid_target_submit_plan()}},
        )

    assert calls == []


def test_live_real_order_executes_with_typed_authority_and_empty_observability_context() -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []

    submitted = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=_typed_target_execution_summary(),
            observability_context={},
        )
    )

    assert submitted == {"status": "submitted", "signal": "BUY"}
    assert len(calls) == 1
    assert calls[0]["kwargs"]["execution_submit_plan"]["source"] == "target_delta"  # type: ignore[index]


def test_live_real_order_request_construction_requires_typed_execution_summary() -> None:
    _arm_live_real_orders(engine="lot_native")

    with pytest.raises(TypeError, match="live_real_order_missing_typed_execution_summary"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
        )


def test_live_real_order_dict_context_cannot_be_submit_authority() -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=_typed_buy_execution_summary(),
            decision_context={"execution_decision": {"buy_submit_plan": _valid_buy_submit_plan()}},
        )
    )

    assert result is None
    assert calls == []


def test_explicit_observability_payload_is_non_authoritative_telemetry() -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    typed_request = TypedExecutionRequest(
        signal="BUY",
        ts=123,
        market_price=100_000_000.0,
        execution_decision_summary=_typed_target_execution_summary(),
        observability_payload=ExecutionObservabilityPayload(
            {"execution_decision": {"buy_submit_plan": _valid_buy_submit_plan()}, "trace": "telemetry"}
        ),
    )

    request = SignalExecutionRequest.from_typed(
        typed_request,
        observability_payload=typed_request.observability_payload,
    )
    submitted = _service(calls).execute(request)

    assert submitted == {"status": "submitted", "signal": "BUY"}
    assert calls[0]["kwargs"]["execution_submit_plan"] == typed_request.execution_decision_summary.target_submit_plan.as_final_payload()  # type: ignore[union-attr,index]
    assert request.observability_payload.as_dict()["trace"] == "telemetry"  # type: ignore[union-attr]


def test_paper_typed_path_rejects_missing_typed_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    object.__setattr__(settings, "MODE", "paper")
    calls: list[dict[str, object]] = []
    summary = ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="ENTER_STRATEGY_POSITION",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=None,
    )

    result = _paper_service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            observability_payload={"promotion_grade": True},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "paper_missing_typed_submit_plan" in caplog.text


def test_paper_consumes_typed_submit_plan_and_passes_authority_to_executor() -> None:
    object.__setattr__(settings, "MODE", "paper")
    calls: list[dict[str, object]] = []
    summary = _typed_buy_execution_summary()

    result = _paper_service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
            observability_payload={"cash_available": 1_000_000_000.0},
        )
    )

    assert result == {"status": "submitted", "signal": "BUY"}
    assert len(calls) == 1
    assert calls[0]["signal"] == "BUY"
    assert calls[0]["kwargs"]["execution_submit_plan"] is summary.buy_submit_plan  # type: ignore[index]


def test_paper_rejects_forged_dict_submit_plan() -> None:
    object.__setattr__(settings, "MODE", "paper")
    calls: list[dict[str, object]] = []
    summary = _forged_summary_with_raw_plan(
        field_name="buy_submit_plan",
        plan=_valid_buy_submit_plan(),
    )

    result = _paper_service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []


def test_live_real_order_blocks_typed_summary_without_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    summary = ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=None,
    )

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_submit_plan" in caplog.text


def test_live_real_order_blocks_summary_with_dict_only_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []
    summary = _forged_summary_with_raw_plan(
        field_name="target_submit_plan",
        plan=_valid_target_submit_plan(),
    )

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_submit_plan:target_submit_plan" in caplog.text


def test_live_real_order_blocks_summary_with_dict_only_residual_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    calls: list[dict[str, object]] = []
    summary = _forged_summary_with_raw_plan(
        field_name="residual_submit_plan",
        plan=_valid_residual_submit_plan(),
    )

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_submit_plan:residual_submit_plan" in caplog.text


def test_live_real_order_blocks_summary_with_dict_only_buy_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []
    summary = _forged_summary_with_raw_plan(
        field_name="buy_submit_plan",
        plan=_valid_buy_submit_plan(),
    )

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_submit_plan:buy_submit_plan" in caplog.text


def test_live_real_order_blocks_non_execution_decision_summary_object(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []

    with pytest.raises(TypeError, match="execution_decision_summary_must_be_typed"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={},
            execution_decision_summary=object(),  # type: ignore[arg-type]
        )

    assert calls == []


def test_execution_decision_summary_rejects_dict_submit_plan_at_core_model_boundary() -> None:
    with pytest.raises(TypeError, match="target_submit_plan_must_be_execution_submit_plan"):
        ExecutionDecisionSummary(
            raw_signal="BUY",
            final_signal="BUY",
            final_action="REBALANCE_TO_TARGET",
            submit_expected=True,
            pre_submit_proof_status="passed",
            block_reason="none",
            strategy_sell_candidate=None,
            residual_sell_candidate=None,
            target_exposure_krw=100_000.0,
            current_effective_exposure_krw=0.0,
            tracked_residual_exposure_krw=None,
            buy_delta_krw=100_000.0,
            residual_live_sell_mode="block",
            residual_buy_sizing_mode="block",
            residual_submit_plan=None,
            buy_submit_plan=None,
            target_shadow_decision=None,
            target_submit_plan=_valid_target_submit_plan(),  # type: ignore[arg-type]
        )


def test_live_broker_lot_native_buy_submit_plan_accepts_only_typed_authority_payload() -> None:
    valid_payload = _typed_plan(_valid_buy_submit_plan()).as_final_payload()
    valid = live_broker._lot_native_buy_submit_plan(valid_payload)
    assert valid is not None
    assert valid["side"] == "BUY"
    assert valid["qty"] == pytest.approx(0.001)
    assert valid["notional_krw"] == pytest.approx(100_000.0)

    assert live_broker._lot_native_buy_submit_plan(_valid_buy_submit_plan()) is None

    bad_source = {**_valid_buy_submit_plan(), "source": "legacy_context"}
    assert live_broker._lot_native_buy_submit_plan(bad_source) is None

    blocked = {
        **_typed_plan(_valid_buy_submit_plan()).as_final_payload(),
        "block_reason": "entry_blocked",
    }
    assert live_broker._lot_native_buy_submit_plan(blocked) is None


def test_live_broker_target_submit_plan_requires_final_payload_contract() -> None:
    assert live_broker._target_delta_submit_plan(_valid_target_submit_plan()) is None

    final_payload = _typed_plan(_valid_target_submit_plan()).as_final_payload()
    accepted = live_broker._target_delta_submit_plan(final_payload)
    assert accepted is not None
    assert accepted["source"] == "target_delta"

    tampered = dict(final_payload)
    tampered["content_hash"] = "sha256:forged"
    assert live_broker._target_delta_submit_plan(tampered) is None


def _summary_for_plan(plan: ExecutionSubmitPlan) -> ExecutionDecisionSummary:
    if plan.source == "target_delta":
        return _typed_target_execution_summary()
    if plan.source == "residual_inventory":
        return _typed_residual_execution_summary()
    return _typed_buy_execution_summary()


def _research_admission(plan: ExecutionSubmitPlan | object) -> str:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.0004, slippage_bps=1.0),
        fee_rate=0.0004,
    )
    try:
        fill = service.simulate_submit_plan(
            submit_plan=plan,  # type: ignore[arg-type]
            context=ResearchExecutionContext(
                signal_ts=1,
                decision_ts=2,
                timing_fields={},
                depth_fields={},
            ),
            reference_price=100_000_000.0,
        )
    except ValueError:
        return "blocked"
    return "accepted" if fill is not None else "blocked"


def _paper_admission(summary: ExecutionDecisionSummary) -> str:
    object.__setattr__(settings, "MODE", "paper")
    calls: list[dict[str, object]] = []
    result = _paper_service(calls).execute(
        SignalExecutionRequest(
            signal=summary.final_signal,
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
            observability_payload={"promotion_grade": True},
        )
    )
    return "accepted" if result is not None and calls else "blocked"


def _live_admission(summary: ExecutionDecisionSummary, *, engine: str, residual_mode: str = "block") -> str:
    _arm_live_real_orders(engine=engine)
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", residual_mode)
    calls: list[dict[str, object]] = []
    result = _service(calls).execute(
        SignalExecutionRequest(
            signal=summary.final_signal,
            ts=123,
            market_price=100_000_000.0,
            execution_decision_summary=summary,
        )
    )
    return "accepted" if result is not None and calls else "blocked"


@pytest.mark.parametrize(
    ("payload_factory", "engine", "residual_mode", "expected_live_admission"),
    [
        (_valid_buy_submit_plan, "target_delta", "block", "blocked"),
        (_valid_target_submit_plan, "target_delta", "block", "accepted"),
        (_valid_residual_submit_plan, "target_delta", "enabled", "accepted"),
    ],
)
def test_submit_plan_admission_equivalence_valid_typed_plans(
    payload_factory: Callable[[], dict[str, object]],
    engine: str,
    residual_mode: str,
    expected_live_admission: str,
) -> None:
    plan = _typed_plan(payload_factory())
    summary = _summary_for_plan(plan)

    assert _research_admission(plan) == "accepted"
    assert _paper_admission(summary) == "accepted"
    assert _live_admission(summary, engine=engine, residual_mode=residual_mode) == expected_live_admission


@pytest.mark.parametrize(
    "mutate",
    [
        lambda plan: plan.update({"source": "legacy_context"}),
        lambda plan: plan.update({"authority": "legacy_qty"}),
        lambda plan: plan.update({"pre_submit_proof_status": "failed"}),
        lambda plan: plan.update({"block_reason": "entry_filter_blocked", "submit_expected": False}),
        lambda plan: plan.update({"qty": 0.0}),
        lambda plan: plan.update({"notional_krw": 0.0}),
    ],
)
def test_submit_plan_admission_equivalence_invalid_buy_plans_fail_closed(
    caplog: pytest.LogCaptureFixture,
    mutate: Callable[[dict[str, object]], object],
) -> None:
    payload = _valid_buy_submit_plan()
    mutate(payload)
    plan = _typed_plan(payload)
    try:
        summary = _typed_buy_execution_summary(plan=payload)
    except (TypeError, ValueError):
        summary = _forged_summary_with_raw_plan(field_name="buy_submit_plan", plan=plan)  # type: ignore[arg-type]

    assert _research_admission(plan) == "blocked"
    assert _paper_admission(summary) == "blocked"
    assert _live_admission(summary, engine="target_delta") == "blocked"


def test_submit_plan_admission_equivalence_dict_only_plans_fail_closed() -> None:
    summary = _forged_summary_with_raw_plan(field_name="buy_submit_plan", plan=_valid_buy_submit_plan())

    assert _research_admission(_valid_buy_submit_plan()) == "blocked"
    assert _paper_admission(summary) == "blocked"
    assert _live_admission(summary, engine="target_delta") == "blocked"
