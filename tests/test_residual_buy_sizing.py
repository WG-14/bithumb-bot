from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.execution_service import build_execution_decision_summary


def _summary(monkeypatch, *, mode: str, target: float = 100_000.0, current: float = 9_665.0):
    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", mode)
    object.__setattr__(settings, "MAX_ORDER_KRW", target)
    payload = {
        "market_price": 100_000_000.0,
        "cash_available": 1_000_000.0,
        "total_effective_exposure_notional_krw": current,
        "residual_inventory_notional_krw": current,
        "residual_inventory_policy_allows_run": True,
        "min_notional_krw": 5000.0,
    }
    return build_execution_decision_summary(
        decision_context=payload,
        raw_signal="BUY",
        final_signal="BUY",
    )


@pytest.fixture(autouse=True)
def restore_settings():
    original_mode = settings.RESIDUAL_BUY_SIZING_MODE
    original_max_order = settings.MAX_ORDER_KRW
    yield
    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", original_mode)
    object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)


def test_delta_buy_sizing_offsets_tracked_residual_exposure(monkeypatch):
    summary = _summary(monkeypatch, mode="delta")
    payload = summary.as_dict()
    plan = payload["buy_submit_plan"]

    assert payload["target_exposure_krw"] == pytest.approx(100_000.0)
    assert payload["current_effective_exposure_krw"] == pytest.approx(9_665.0)
    assert payload["tracked_residual_exposure_krw"] == pytest.approx(9_665.0)
    assert payload["buy_delta_krw"] == pytest.approx(90_335.0)
    assert payload["residual_buy_sizing_mode"] == "delta"
    assert plan["notional_krw"] == pytest.approx(90_335.0)
    assert plan["authority"] == "residual_inventory_delta"


def test_telemetry_buy_sizing_reports_delta_but_does_not_change_submit_notional(monkeypatch):
    summary = _summary(monkeypatch, mode="telemetry")
    payload = summary.as_dict()
    plan = payload["buy_submit_plan"]

    assert payload["buy_delta_krw"] == pytest.approx(90_335.0)
    assert payload["residual_buy_sizing_mode"] == "telemetry"
    assert plan["notional_krw"] == pytest.approx(100_000.0)
    assert plan["authority"] != "residual_inventory_delta"


def test_tracked_residual_covering_target_blocks_buy_submit(monkeypatch):
    summary = _summary(monkeypatch, mode="delta", current=100_000.0)
    payload = summary.as_dict()
    plan = payload["buy_submit_plan"]

    assert payload["buy_delta_krw"] == pytest.approx(0.0)
    assert plan["submit_expected"] is False
    assert plan["block_reason"] == "tracked_residual_exposure_covers_target"
