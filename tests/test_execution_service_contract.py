from __future__ import annotations

from collections.abc import Callable

import pytest

from bithumb_bot.config import settings
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionSubmitPlan,
    LiveSignalExecutionService,
    SignalExecutionRequest,
)


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


def _typed_plan(payload: dict[str, object]) -> ExecutionSubmitPlan:
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
    )


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

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": {field_name: plan}},
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_execution_summary" in caplog.text


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
    assert "explicit_submit_plan_not_consumed" in caplog.text


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
    assert calls[0]["kwargs"]["execution_submit_plan"] == summary.target_submit_plan.as_dict()  # type: ignore[index,union-attr]


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
    _arm_live_real_orders(engine="lot_native")
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
        (None, "live_real_order_missing_decision_context"),
        ({}, "live_real_order_missing_typed_execution_summary"),
        ({"execution_decision": {}}, "live_real_order_missing_typed_execution_summary"),
    ],
)
def test_missing_execution_plan_contract_fails_closed_in_live_real_order_mode(
    caplog: pytest.LogCaptureFixture,
    decision_context: dict[str, object] | None,
    expected_reason: str,
) -> None:
    _arm_live_real_orders(engine="lot_native")
    calls: list[dict[str, object]] = []

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context=decision_context,
        )
    )

    assert result is None
    assert calls == []
    assert expected_reason in caplog.text


def test_live_real_order_blocks_dict_only_execution_decision_even_with_submit_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _arm_live_real_orders(engine="target_delta")
    calls: list[dict[str, object]] = []

    result = _service(calls).execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=100_000_000.0,
            decision_context={"execution_decision": {"target_submit_plan": _valid_target_submit_plan()}},
        )
    )

    assert result is None
    assert calls == []
    assert "live_real_order_missing_typed_execution_summary" in caplog.text


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
        target_submit_plan=_valid_target_submit_plan(),
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


def test_execution_decision_summary_rejects_malformed_dict_submit_plan_schema() -> None:
    malformed_plan = dict(_valid_target_submit_plan())
    malformed_plan.pop("authority")

    with pytest.raises(ValueError, match="target_submit_plan_schema_missing_fields:authority"):
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
            target_submit_plan=malformed_plan,
        )
