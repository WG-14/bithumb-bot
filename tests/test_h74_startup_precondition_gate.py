from __future__ import annotations

from bithumb_bot.h74_startup_gate import evaluate_h74_startup_gate
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.experiment_execution_contract import POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT
from bithumb_bot.run_loop_execution_planner import _inject_h74_startup_gate
from bithumb_bot.config import settings


def _readiness(**overrides) -> dict[str, object]:
    payload = {
        "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0},
        "projection_convergence": {"portfolio_qty": 0.0, "projected_total_qty": 0.0},
        "open_order_count": 0,
        "submit_unknown_count": 0,
        "recovery_required_count": 0,
        "residual_inventory_state": "flat",
    }
    payload.update(overrides)
    return payload


def test_h74_start_blocks_when_broker_qty_executable_residual_exists() -> None:
    result = evaluate_h74_startup_gate(
        readiness_payload=_readiness(
            broker_position_evidence={"broker_qty_known": True, "broker_qty": 0.0001},
            projection_convergence={"portfolio_qty": 0.0001, "projected_total_qty": 0.0001},
        )
    )

    assert result.status == "START_BLOCKED"
    assert result.reason_code == "broker_executable_residual_exists"


def test_h74_start_blocks_when_persisted_target_state_nonzero() -> None:
    result = evaluate_h74_startup_gate(readiness_payload=_readiness(), target_state={"target_exposure_krw": 100_000.0})

    assert result.status == "START_BLOCKED"
    assert result.reason_code == "target_state_nonzero"


def test_h74_start_blocks_when_submit_unknown_exists() -> None:
    result = evaluate_h74_startup_gate(readiness_payload=_readiness(submit_unknown_count=1))

    assert result.status == "START_BLOCKED"
    assert result.reason_code == "submit_unknown_count_nonzero"


def test_h74_start_allows_clean_flat_broker_and_local_state() -> None:
    result = evaluate_h74_startup_gate(readiness_payload=_readiness())

    assert result.status == "START_ALLOWED"
    assert result.allowed is True
    assert result.as_dict()["startup_gate_hash"].startswith("sha256:")


def test_h74_start_true_dust_requires_explicit_authority_policy() -> None:
    blocked = evaluate_h74_startup_gate(
        readiness_payload=_readiness(
            broker_position_evidence={"broker_qty_known": True, "broker_qty": 0.00001},
            projection_convergence={"portfolio_qty": 0.00001, "projected_total_qty": 0.00001},
            residual_inventory_state="terminal_true_dust",
        )
    )
    allowed = evaluate_h74_startup_gate(
        readiness_payload=_readiness(
            broker_position_evidence={"broker_qty_known": True, "broker_qty": 0.00001},
            projection_convergence={"portfolio_qty": 0.00001, "projected_total_qty": 0.00001},
            residual_inventory_state="terminal_true_dust",
        ),
        authority={"residual_inventory_mode": "allow_terminal_true_dust"},
    )

    assert blocked.status == "START_BLOCKED"
    assert allowed.status == "START_ALLOWED_WITH_TERMINAL_DUST"


def test_h74_runtime_planner_injects_startup_gate_block(monkeypatch) -> None:
    called = {"value": False}

    def fake_gate(*, readiness_payload, target_state=None, authority=None):
        called["value"] = True
        return evaluate_h74_startup_gate(
            readiness_payload=readiness_payload,
            target_state={"target_exposure_krw": 100_000.0},
            authority=authority,
        )

    monkeypatch.setattr(
        "bithumb_bot.run_loop_execution_planner.evaluate_h74_startup_gate",
        fake_gate,
    )
    original_engine = settings.EXECUTION_ENGINE
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    try:
        payload = _inject_h74_startup_gate(
            readiness_payload={
                **_readiness(),
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
            },
            target_state={"target_exposure_krw": 100_000.0},
            authority_fields={"residual_inventory_mode": "block_executable_residual"},
        )
        summary = build_execution_decision_summary(
            decision_context={
                **payload,
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
                "signal": "BUY",
                "final_signal": "BUY",
                "cash_available": 1_000_000.0,
            },
            raw_signal="BUY",
            final_signal="BUY",
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", original_engine)

    assert called["value"] is True
    assert payload["h74_startup_gate_status"] == "START_BLOCKED"
    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan.submit_expected is False


def test_h74_startup_gate_hash_is_recorded_in_submit_plan_or_certificate() -> None:
    original_engine = settings.EXECUTION_ENGINE
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    try:
        payload = _inject_h74_startup_gate(
            readiness_payload={
                **_readiness(submit_unknown_count=1),
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
            },
            target_state={"target_exposure_krw": 0.0},
            authority_fields={"residual_inventory_mode": "block_executable_residual"},
        )
        summary = build_execution_decision_summary(
            decision_context={
                **payload,
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
                "signal": "BUY",
                "final_signal": "BUY",
                "cash_available": 1_000_000.0,
            },
            raw_signal="BUY",
            final_signal="BUY",
        )

        assert str(payload["startup_gate_hash"]).startswith("sha256:")
        assert summary.target_submit_plan is not None
        plan = summary.target_submit_plan.as_dict()
        assert plan["startup_gate_hash"] == payload["startup_gate_hash"]
        assert plan["h74_startup_gate_reason_code"] == "submit_unknown_count_nonzero"
        assert plan["submit_expected"] is False
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", original_engine)
