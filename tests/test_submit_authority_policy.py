from __future__ import annotations

from dataclasses import fields

import pytest

from bithumb_bot.config import settings
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionObservabilityPayload,
    ExecutionSubmitPlan,
    LiveSignalExecutionService,
    TypedExecutionRequest,
)
from bithumb_bot.submit_authority_policy import evaluate_submit_authority_policy


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {field.name: getattr(settings, field.name) for field in fields(type(settings))}
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)


class _Broker:
    pass


def _settings(*, mode: str, dry_run: bool, armed: bool, engine: str = "target_delta"):
    return type(
        "Settings",
        (),
        {
            "MODE": mode,
            "LIVE_DRY_RUN": dry_run,
            "LIVE_REAL_ORDER_ARMED": armed,
            "EXECUTION_ENGINE": engine,
            "RESIDUAL_LIVE_SELL_MODE": "enabled",
        },
    )()


def _plan(
    *,
    side: str = "BUY",
    source: str = "target_delta",
    authority: str = "canonical_target_delta_sizing",
    submit_expected: bool = True,
    proof: str = "passed",
    extra: dict[str, object] | None = None,
) -> ExecutionSubmitPlan:
    return ExecutionSubmitPlan(
        side=side,
        source=source,
        authority=authority,
        final_action="REBALANCE_TO_TARGET" if source == "target_delta" else "ENTER_STRATEGY_POSITION",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=submit_expected,
        pre_submit_proof_status=proof,
        block_reason="none" if submit_expected else "blocked",
        idempotency_key="unit-key",
        extra_payload={
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:portfolio-target",
            "allocation_decision_hash": "sha256:allocation",
            "strategy_contribution_hash": "sha256:contribution",
            **dict(extra or {}),
        },
    )


def _summary(*, target: ExecutionSubmitPlan | None = None, buy: ExecutionSubmitPlan | None = None, residual: ExecutionSubmitPlan | None = None) -> ExecutionDecisionSummary:
    plan = target or residual or buy
    return ExecutionDecisionSummary(
        raw_signal="BUY" if plan is None else plan.side,
        final_signal="BUY" if plan is None else plan.side,
        final_action="STRATEGY_HOLD" if plan is None else plan.final_action,
        submit_expected=False if plan is None else plan.submit_expected,
        pre_submit_proof_status="not_required" if plan is None else plan.pre_submit_proof_status,
        block_reason="no_plan" if plan is None else plan.block_reason,
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=None if plan is None else plan.target_exposure_krw,
        current_effective_exposure_krw=None if plan is None else plan.current_effective_exposure_krw,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=None if plan is None else plan.delta_krw,
        residual_live_sell_mode="enabled",
        residual_buy_sizing_mode="telemetry",
        residual_submit_plan=residual,
        buy_submit_plan=buy,
        target_shadow_decision=None,
        target_submit_plan=target,
    )


def test_mode_aware_submit_authority_matrix() -> None:
    target = _plan()
    legacy_buy = _plan(source="strategy_position", authority="configured_strategy_order_size")
    residual = _plan(
        side="SELL",
        source="residual_inventory",
        authority="residual_inventory_policy",
        extra={"portfolio_target_authoritative": False},
    )

    assert evaluate_submit_authority_policy(
        legacy_buy,
        settings_obj=_settings(mode="paper", dry_run=True, armed=False, engine="lot_native"),
        plan_kind="buy",
    ).allowed
    assert evaluate_submit_authority_policy(
        legacy_buy,
        settings_obj=_settings(mode="live", dry_run=True, armed=False, engine="lot_native"),
        plan_kind="buy",
    ).reason == "live_dry_run_non_submitting"
    assert evaluate_submit_authority_policy(
        target,
        settings_obj=_settings(mode="live", dry_run=False, armed=True),
        plan_kind="target",
    ).allowed
    rejected = evaluate_submit_authority_policy(
        legacy_buy,
        settings_obj=_settings(mode="live", dry_run=False, armed=True),
        plan_kind="buy",
    )
    assert rejected.allowed is False
    assert rejected.reason == "live_real_order_buy_plan_rejected_target_delta_required"
    assert evaluate_submit_authority_policy(
        residual,
        settings_obj=_settings(mode="live", dry_run=False, armed=True),
        plan_kind="residual",
    ).allowed


@pytest.mark.parametrize(
    ("source", "authority"),
    [
        ("strategy_position", "configured_strategy_order_size"),
        ("strategy_position", "strategy_execution_intent"),
        ("strategy_position", "research_compatibility_execution_intent"),
        ("strategy_position", "residual_inventory_delta"),
    ],
)
def test_live_real_order_rejects_legacy_buy_before_executor(source: str, authority: str) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    called = False

    def _executor(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"status": "called"}

    buy = _plan(source=source, authority=authority)
    service = LiveSignalExecutionService(
        broker=_Broker(),
        executor=_executor,
        harmless_dust_recorder=lambda **_kwargs: False,
    )
    request = TypedExecutionRequest(
        signal="BUY",
        ts=1,
        market_price=100_000_000.0,
        execution_decision_summary=_summary(buy=buy),
        observability_payload=ExecutionObservabilityPayload({"execution_decision": _summary(buy=buy).as_dict()}),
    )

    assert service.execute(request) is None
    assert called is False


def test_live_real_order_missing_target_plan_fails_closed_before_executor() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    called = False

    def _executor(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"status": "called"}

    service = LiveSignalExecutionService(
        broker=_Broker(),
        executor=_executor,
        harmless_dust_recorder=lambda **_kwargs: False,
    )
    request = TypedExecutionRequest(
        signal="BUY",
        ts=1,
        market_price=100_000_000.0,
        execution_decision_summary=_summary(),
        observability_payload=ExecutionObservabilityPayload({"execution_decision": _summary().as_dict()}),
    )

    assert service.execute(request) is None
    assert called is False


def test_live_real_order_accepts_only_valid_residual_sell_exception_before_executor() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    calls: list[dict[str, object]] = []

    def _executor(*_args, **kwargs):
        calls.append(dict(kwargs))
        return {"status": "called"}

    residual = _plan(
        side="SELL",
        source="residual_inventory",
        authority="residual_inventory_policy",
        extra={"portfolio_target_authoritative": False},
    )
    service = LiveSignalExecutionService(
        broker=_Broker(),
        executor=_executor,
        harmless_dust_recorder=lambda **_kwargs: False,
    )
    request = TypedExecutionRequest(
        signal="SELL",
        ts=1,
        market_price=100_000_000.0,
        execution_decision_summary=_summary(residual=residual),
        observability_payload=ExecutionObservabilityPayload({"execution_decision": _summary(residual=residual).as_dict()}),
    )

    assert service.execute(request) == {"status": "called"}
    assert len(calls) == 1

    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
    calls.clear()
    assert service.execute(request) is None
    assert calls == []
