from __future__ import annotations

import json
import os
from dataclasses import replace
import sqlite3

import pytest

from bithumb_bot import h74_live_rehearsal
from bithumb_bot.config import settings
from bithumb_bot.h74_rehearsal_context import default_h74_live_rehearsal_context
from bithumb_bot.h74_live_rehearsal import (
    H74LiveRehearsalConfig,
    H74LiveRehearsalError,
    run_h74_live_rehearsal,
)


def _source_artifact(tmp_path, *, fee_rate: float = 0.0004) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {
                    "fee_rate": fee_rate,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
                "behavior_contract": {
                    "position_mode": "fixed_fill_qty_until_exit",
                    "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
                    "residual_inventory_mode": "terminal_dust_reported_not_reused_without_authority",
                    "initial_position_policy": "flat_start_required",
                    "partial_fill_policy": "accumulate_cycle_acquired_qty",
                    "fee_application_policy": "repository_observed_fee_fields",
                },
                "entry_submit_semantics": {
                    "schema_version": 1,
                    "entry_order_type": "price",
                    "entry_submit_field": "price",
                    "entry_quote_notional_krw": 100_000,
                    "entry_volume_forbidden": True,
                    "entry_qty_preview_authoritative": False,
                    "entry_fill_qty_authority": "broker_fills",
                },
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def test_h74_rehearsal_reaches_broker_submit_boundary_at_kst_10(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["strategy_name"] == "daily_participation_sma"
    assert payload["daily_participation_reason_code"] == "daily_participation_fallback_allowed"
    assert payload["pre_submit_risk_status"] == "ALLOW"
    assert payload["submit_authority_reason"] == "allowed_target_delta"
    assert payload["broker_submit_reached"] is True
    assert payload["actual_submit"] is False
    assert payload["LIVE_DRY_RUN"] is False


def test_h74_rehearsal_does_not_mutate_global_settings(tmp_path) -> None:
    before = (
        settings.MODE,
        settings.LIVE_DRY_RUN,
        settings.LIVE_REAL_ORDER_ARMED,
    )

    run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert (
        settings.MODE,
        settings.LIVE_DRY_RUN,
        settings.LIVE_REAL_ORDER_ARMED,
    ) == before


def test_h74_rehearsal_does_not_mutate_environment(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY", raising=False)

    run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert "H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY" not in os.environ


def test_h74_rehearsal_claim_scope_is_synthetic_gate_only(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["artifact_type"] == "SyntheticGateEvidence"
    assert payload["claims_scope"] == "synthetic_gate"
    assert payload["full_lifecycle_equivalence_supported"] is False


def test_h74_rehearsal_uses_injected_clock_and_db_factory(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[float] = []
    db_paths: list[str] = []
    base = default_h74_live_rehearsal_context()

    def _db_factory(path: str) -> sqlite3.Connection:
        db_paths.append(path)
        return sqlite3.connect(path)

    context = replace(
        base,
        clock=lambda: seen.append(12345.0) or 12345.0,
        db_factory=_db_factory,
    )

    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path)),
        context=context,
    )

    assert payload["broker_submit_reached"] is True
    assert seen
    assert db_paths


def test_h74_rehearsal_kst_10_allows_daily_participation_buy(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["would_submit"] is True
    assert payload["broker_submit_reached"] is True
    assert payload["actual_submit"] is False
    assert payload["daily_participation_entry_authorized"] is True
    assert payload["entry_authority_status"] == "ALLOW"


def test_h74_rehearsal_kst_18_blocks_out_of_window_buy(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["would_submit"] is False
    assert payload["broker_submit_reached"] is False
    assert payload["actual_submit"] is False
    assert payload["primary_block_gate"] == "entry_authority"
    assert payload["entry_authority_status"] == "BLOCK"
    assert payload["entry_authority_reason_code"] == "target_delta_entry_without_strategy_buy_authority"


def test_h74_negative_rehearsal_does_not_use_operator_smoke(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["operator_live_pipeline_smoke"] is False
    assert "operator_live_pipeline_smoke" not in payload["would_submit_plan"]


def test_h74_rehearsal_uses_runtime_cycle_pipeline(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["runtime_cycle_pipeline_called"] is True
    assert payload["execution_result_status"] == "submitted"
    assert payload["live_signal_execution_service_called"] is True


def test_h74_rehearsal_uses_production_runtime_strategy_set(tmp_path, monkeypatch) -> None:
    calls = {"count": 0}
    original = h74_live_rehearsal.runtime_strategy_set_manifest_hash

    def _wrapped(strategy_set):
        calls["count"] += 1
        return original(strategy_set)

    monkeypatch.setattr(h74_live_rehearsal, "runtime_strategy_set_manifest_hash", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] >= 1
    assert payload["production_runtime_strategy_set_called"] is True
    assert str(payload["runtime_strategy_set_manifest_hash"]).startswith("sha256:")
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_uses_production_allocator_portfolio_target(tmp_path, monkeypatch) -> None:
    from bithumb_bot import run_loop_execution_planner

    calls = {"count": 0}
    original = run_loop_execution_planner.PortfolioAllocator.allocate

    def _wrapped(self, allocation_input):
        calls["count"] += 1
        return original(self, allocation_input)

    monkeypatch.setattr(run_loop_execution_planner.PortfolioAllocator, "allocate", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] == 1
    assert payload["production_allocator_portfolio_target_called"] is True
    assert str(payload["portfolio_target_hash"]).startswith("sha256:")
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_invokes_live_signal_execution_service_before_mock_submit(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["live_signal_execution_service_called"] is True
    assert payload["target_delta_final_payload_created"] is True
    assert payload["pre_submit_proof_created"] is True
    assert payload["submit_authority_allowed"] is True
    assert payload["broker_submit_reached"] is True
    assert payload["would_submit_plan"]["pre_submit_risk_status"] == "ALLOW"
    assert payload["would_submit_plan"]["pre_submit_risk_decision_hash"].startswith("sha256:")
    assert payload["would_submit_plan"]["pre_submit_risk_evidence_hash"].startswith("sha256:")


def test_h74_rehearsal_uses_production_target_delta_planner(tmp_path, monkeypatch) -> None:
    from bithumb_bot import execution_service

    calls = {"count": 0}
    original = execution_service.build_target_delta_execution_sizing

    def _wrapped(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(execution_service, "build_target_delta_execution_sizing", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] == 1
    assert payload["would_submit_plan"]["source"] == "h74_source_observation"
    assert payload["would_submit_plan"]["authority"] == "h74_fixed_fill_quote_notional_buy"
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_invokes_pre_submit_and_submit_authority_policies(tmp_path, monkeypatch) -> None:
    from bithumb_bot import execution_service
    from bithumb_bot.pre_submit_risk_coordinator import PreSubmitRiskCoordinator

    calls = {"pre_submit": 0, "submit_authority": 0}
    original_pre_submit = PreSubmitRiskCoordinator.evaluate_and_persist
    original_submit_authority = execution_service.evaluate_submit_authority_policy

    def _wrapped_pre_submit(self, *args, **kwargs):
        calls["pre_submit"] += 1
        return original_pre_submit(self, *args, **kwargs)

    def _wrapped_submit_authority(*args, **kwargs):
        calls["submit_authority"] += 1
        return original_submit_authority(*args, **kwargs)

    monkeypatch.setattr(PreSubmitRiskCoordinator, "evaluate_and_persist", _wrapped_pre_submit)
    monkeypatch.setattr(execution_service, "evaluate_submit_authority_policy", _wrapped_submit_authority)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["pre_submit"] >= 1
    assert calls["submit_authority"] >= 1
    assert payload["pre_submit_risk_status"] == "ALLOW"
    assert payload["submit_authority_allowed"] is True
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_fails_if_target_plan_is_manual_fixture(tmp_path) -> None:
    assert not hasattr(h74_live_rehearsal, "_target_delta_submit_plan")

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["would_submit_plan"]["source"] == "h74_source_observation"
    assert payload["would_submit_plan"].get("authority_source") == "h74_fixed_fill_quote_notional_buy"
    assert payload["would_submit_plan"].get("portfolio_target_authoritative") is True


def test_h74_rehearsal_fails_if_daily_participation_plugin_not_called(tmp_path, monkeypatch) -> None:
    def _blocked(*_args, **_kwargs):
        raise AssertionError("daily_participation_sma plugin not called through rehearsal")

    monkeypatch.setattr(
        "bithumb_bot.strategy_plugins.daily_participation_sma.evaluate_daily_participation_policy",
        _blocked,
    )

    with pytest.raises(AssertionError, match="plugin not called"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))


def test_h74_rehearsal_does_not_use_operator_smoke_authority(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["operator_live_pipeline_smoke"] is False
    assert "operator_live_pipeline_smoke" not in payload["would_submit_plan"]
    with pytest.raises(H74LiveRehearsalError, match="rejects_operator_smoke_authority"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(smoke_authority_hash="sha256:smoke"))


def test_negative_rehearsal_reports_daily_window_result(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["decision_kst_hour"] == 18
    assert payload["daily_participation_entry_authorized"] is False
    assert payload["daily_participation_reason_code"] == "outside_daily_participation_window"
    assert payload["daily_participation_window_start_hour_kst"] == 9
    assert payload["daily_participation_window_end_hour_kst"] == 11


def test_h74_rehearsal_does_not_accept_smoke_proof_as_pre_submit_proof() -> None:
    with pytest.raises(H74LiveRehearsalError, match="rejects_operator_smoke_authority"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(smoke_authority_hash="sha256:smoke"))


def test_h74_rehearsal_does_not_use_live_dry_run_or_paper_mode_for_success_path(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["MODE"] == "live"
    assert payload["LIVE_DRY_RUN"] is False
    assert payload["LIVE_REAL_ORDER_ARMED"] is True
    assert payload["decision_path_MODE"] == "live"
    assert payload["decision_path_LIVE_DRY_RUN"] is False
    assert payload["decision_path_LIVE_REAL_ORDER_ARMED"] is True
    assert payload["planning_path_MODE"] == "live"
    assert payload["planning_path_LIVE_DRY_RUN"] is False
    assert payload["planning_path_LIVE_REAL_ORDER_ARMED"] is True
    assert payload["broker_submit_reached"] is True
    assert payload["actual_submit"] is False
    assert payload["operator_live_pipeline_smoke"] is False
    assert "operator_live_pipeline_smoke" not in payload["would_submit_plan"]


def test_h74_rehearsal_fails_when_pre_submit_broker_snapshot_missing(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            broker_snapshot_available=False,
            source_artifact_path=_source_artifact(tmp_path),
        )
    )

    assert payload["pre_submit_risk_status"] == "REQUIRE_RECONCILE"
    assert payload["pre_submit_risk_reason_code"] == "RISK_STATE_MISMATCH"
    assert payload["broker_submit_reached"] is False


def test_rehearsal_reports_fee_equivalence_gate(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            current_fee_rate=0.0025,
            fee_authority_source="chance_doc",
        )
    )

    assert payload["experiment_equivalence_status"] == "mismatch"
    assert payload["fee_authority_source"] == "chance_doc"
    gate = [entry for entry in payload["gate_trace"] if entry["gate"] == "fee_equivalence"][0]
    assert gate["status"] == "BLOCK"
    assert gate["reason_code"] == "mismatch"


def test_rehearsal_does_not_reach_submit_boundary_when_equivalence_blocks(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            current_fee_rate=0.0025,
        )
    )

    assert payload["experiment_equivalence_status"] == "mismatch"
    assert payload["broker_submit_reached"] is False
    assert payload["would_submit"] is False
    assert payload["primary_block_gate"] == "fee_equivalence"


def test_h74_would_submit_plan_contains_authority_hash_and_position_mode(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    plan = payload["would_submit_plan"]

    assert plan["position_mode"] == "fixed_fill_qty_until_exit"
    assert str(plan["authority_hash"]).startswith("sha256:")


def test_h74_plan_only_reports_closeout_qty_and_risk_authority(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            closeout_existing_qty=0.002,
            order_rules={"min_qty": 0.001, "qty_step": 0.0, "max_qty_decimals": 8, "min_notional_krw": 5000.0},
        )
    )
    preview = payload["h74_closeout_preview"]

    assert payload["h74_closeout_preview_present"] is True
    assert preview["remaining_cycle_qty"] == pytest.approx(0.002)
    assert preview["planned_sell_qty"] == pytest.approx(0.002)
    assert preview["qty_matches_remaining"] is True
    assert "risk_status" in preview
    assert "submit_authority_would_allow" in preview


def test_h74_plan_only_fails_when_closeout_qty_would_floor_remaining(tmp_path, monkeypatch) -> None:
    from bithumb_bot import h74_live_rehearsal as rehearsal_module

    original = rehearsal_module.run_h74_live_rehearsal
    payload = original(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            closeout_existing_qty=0.00209271,
            order_rules={"min_qty": 0.001, "qty_step": 0.001, "max_qty_decimals": 8, "min_notional_krw": 5000.0},
        )
    )
    preview = payload["h74_closeout_preview"]

    assert preview["planned_sell_qty"] == pytest.approx(0.002)
    assert preview["qty_matches_remaining"] is False
    assert preview["residual_policy"] != "none"


def test_h74_plan_only_fails_when_reduce_only_sell_would_be_rejected(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            closeout_existing_qty=0.002,
            order_rules={"min_qty": 0.001, "qty_step": 0.0, "max_qty_decimals": 8, "min_notional_krw": 5000.0},
            invalid_reduce_only_preview_case="allowed_actions_missing_sell",
        )
    )
    preview = payload["h74_closeout_preview"]

    assert preview["submit_authority_would_allow"] is False
    assert payload["would_submit"] is False
    assert payload["primary_block_gate"] in {"pre_submit_risk", "submit_authority"}
    assert preview["submit_authority_reason"]
