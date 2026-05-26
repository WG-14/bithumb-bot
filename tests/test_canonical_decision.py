from __future__ import annotations

from bithumb_bot.canonical_decision import (
    CANONICAL_DECISION_SCHEMA_FIELDS,
    canonical_flat_position_state_hash,
    export_research_decisions,
    export_runtime_replay_decisions,
    runtime_decision_to_canonical_event,
)
from bithumb_bot import runtime_sma_snapshot as runtime_sma_snapshot_module
from bithumb_bot.strategy.base import StrategyDecision


def test_runtime_strategy_decision_exports_canonical_operational_fields() -> None:
    decision = StrategyDecision(
        signal="HOLD",
        reason="filtered entry: cost_edge",
        context={
            "strategy": "sma_with_filter",
            "ts": 1_714_521_660_000,
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "prev_s": 100.0,
            "prev_l": 101.0,
            "curr_s": 102.0,
            "curr_l": 101.0,
            "gap_ratio": 0.01,
            "blocked_filters": ["cost_edge"],
            "filters": {
                "cost_edge": {
                    "value": 0.01,
                    "threshold": 0.02,
                }
            },
            "position_gate": {
                "entry_allowed": True,
                "exit_allowed": False,
                "dust_state": "flat",
                "effective_flat": True,
                "normalized_exposure_active": False,
                "order_rules": {
                    "min_qty": 0.0001,
                    "qty_step": 0.0001,
                    "max_qty_decimals": 4,
                    "min_notional_krw": 5000,
                    "source": "test",
                },
            },
            "exit": {
                "rule": None,
                "reason": "no exit rule triggered",
                "evaluations": [],
            },
            "fee_authority": {"fee_source": "order_rules"},
        },
    )

    event = runtime_decision_to_canonical_event(
        decision,
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:db",
        through_ts_ms=1_714_521_660_000,
        execution_timing_policy_hash="sha256:timing",
        strategy_version="sma_with_filter.test",
        strategy_decision_contract_version="research_sma_decision_contract.test",
    ).as_dict()

    assert event["decision_contract_version"] == 2
    assert event["strategy_decision_contract_version"] == "research_sma_decision_contract.test"
    assert event["raw_signal"] == "BUY"
    assert event["final_signal"] == "HOLD"
    assert event["blocked"] is True
    assert event["blocked_filters"] == ("cost_edge",)
    assert event["position_state_hash"].startswith("sha256:")
    assert event["position_authority"]["state_class"] == "flat_no_dust_no_position"
    assert event["exit_evaluations_hash"].startswith("sha256:")
    assert event["execution_timing_policy_hash"] == "sha256:timing"
    assert event["order_rules_hash"].startswith("sha256:")
    assert event["feature_snapshot_hash"].startswith("sha256:")
    assert event["strategy_behavior_hash"].startswith("sha256:")
    assert event["strategy_specific_payload"]["curr_s"] == 102.0
    for sma_field in (
        "prev_s",
        "prev_l",
        "curr_s",
        "curr_l",
        "gap_ratio",
        "range_ratio",
        "expected_edge_ratio",
        "required_edge_ratio",
    ):
        assert sma_field not in CANONICAL_DECISION_SCHEMA_FIELDS


def test_runtime_replay_routes_sma_with_filter_through_snapshot_orchestration(monkeypatch) -> None:
    calls: list[int] = []

    class _Strategy:
        name = "sma_with_filter"

        def decide(self, conn, *, through_ts_ms=None):
            raise AssertionError("runtime replay must use snapshot orchestration for sma_with_filter")

    def _snapshot_orchestration(conn, strategy, *, through_ts_ms=None):
        calls.append(int(through_ts_ms))
        return StrategyDecision(
            signal="HOLD",
            reason="unit",
            context={
                "strategy": strategy.name,
                "ts": int(through_ts_ms),
                "raw_signal": "HOLD",
                "final_signal": "HOLD",
                "position_gate": {
                    "entry_allowed": True,
                    "exit_allowed": False,
                    "dust_state": "flat",
                    "effective_flat": True,
                    "normalized_exposure_active": False,
                    "order_rules": {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "max_qty_decimals": 4,
                        "min_notional_krw": 5000,
                        "source": "test",
                    },
                },
                "exit": {"rule": None, "reason": "none", "evaluations": []},
                "fee_authority": {"fee_source": "test"},
            },
        )

    monkeypatch.setattr(
        runtime_sma_snapshot_module,
        "decide_sma_with_filter_snapshot_from_db",
        _snapshot_orchestration,
    )

    events = export_runtime_replay_decisions(
        conn=object(),
        strategy=_Strategy(),
        through_ts_list=[1_714_521_660_000],
        market="KRW-BTC",
        interval="1m",
    )

    assert len(events) == 1
    assert calls == [1_714_521_660_000]


def test_runtime_order_rules_hash_changes_with_rule_inputs() -> None:
    def event_for(min_qty: float) -> dict[str, object]:
        return runtime_decision_to_canonical_event(
            StrategyDecision(
                signal="BUY",
                reason="sma golden cross",
                context={
                    "strategy": "sma_with_filter",
                    "ts": 1_714_521_660_000,
                    "raw_signal": "BUY",
                    "final_signal": "BUY",
                    "position_gate": {
                        "entry_allowed": True,
                        "exit_allowed": False,
                        "dust_state": "flat",
                        "effective_flat": True,
                        "normalized_exposure_active": False,
                        "order_rules": {
                            "min_qty": min_qty,
                            "qty_step": 0.0001,
                            "max_qty_decimals": 4,
                            "min_notional_krw": 5000,
                            "source": "test",
                        },
                    },
                    "exit": {"evaluations": []},
                },
            ),
            market="KRW-BTC",
            interval="1m",
            profile_content_hash="sha256:profile",
            dataset_content_hash="sha256:data",
            db_data_fingerprint="sha256:data",
            execution_timing_policy_hash="sha256:timing",
        ).as_dict()

    assert event_for(0.0001)["order_rules_hash"] != event_for(0.0002)["order_rules_hash"]


def test_runtime_flat_no_dust_position_uses_research_comparable_hash() -> None:
    event = runtime_decision_to_canonical_event(
        StrategyDecision(
            signal="BUY",
            reason="sma golden cross",
            context={
                "strategy": "sma_with_filter",
                "ts": 1_714_521_660_000,
                "raw_signal": "BUY",
                "final_signal": "BUY",
                "position_gate": {
                    "entry_allowed": True,
                    "exit_allowed": False,
                    "dust_state": "no_dust",
                    "effective_flat": True,
                    "normalized_exposure_active": False,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                    "has_any_position_residue": False,
                    "order_rules": {"source": "test", "min_qty": 0.0001},
                },
                "position_state": {"runtime_detail": "retained_for_diagnostics"},
                "exit": {"evaluations": []},
            },
        ),
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    ).as_dict()

    assert event["dust_state"] == "flat"
    assert event["position_state_hash"] == canonical_flat_position_state_hash()
    assert event["position_authority"]["state_class"] == "flat_no_dust_no_position"


def test_research_decision_export_normalizes_to_canonical_schema() -> None:
    events = export_research_decisions(
        [
            {
                "strategy_name": "sma_with_filter",
                "market": "KRW-BTC",
                "interval": "1m",
                "signal_timestamp": "1714521660000",
                "candle_ts": 1_714_521_660_000,
                "raw_signal": "BUY",
                "final_signal": "BUY",
                "strategy_version": "sma_with_filter.test",
                "strategy_decision_contract_version": "research_sma_decision_contract.test",
            }
        ],
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )

    assert events[0]["decision_contract_version"] == 2
    assert events[0]["profile_content_hash"] == "sha256:profile"
    assert events[0]["dataset_content_hash"] == "sha256:data"
    assert events[0]["side"] == "BUY"
    assert events[0]["strategy_behavior_hash"].startswith("sha256:")


def test_non_sma_research_decision_exports_canonical_v2_without_sma_fields() -> None:
    events = export_research_decisions(
        [
            {
                "strategy_name": "buy_and_hold_baseline",
                "strategy_version": "buy_and_hold_baseline.research_contract.v1",
                "strategy_decision_contract_version": "research_buy_and_hold_baseline_decision_contract.v1",
                "market": "KRW-BTC",
                "interval": "1m",
                "signal_timestamp": "1714521660000",
                "candle_ts": 1_714_521_660_000,
                "raw_signal": "BUY",
                "final_signal": "BUY",
                "feature_snapshot": {"candle_index": 1, "close": 100.0},
                "strategy_diagnostics_namespace": "buy_and_hold_baseline",
                "strategy_diagnostics": {"reason": "canary"},
            }
        ],
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )

    event = events[0]
    assert event["decision_contract_version"] == 2
    assert event["strategy_name"] == "buy_and_hold_baseline"
    assert event["strategy_diagnostics_namespace"] == "buy_and_hold_baseline"
    assert event["feature_snapshot"] == {"candle_index": 1, "close": 100.0}
    assert "prev_s" not in event
    assert "curr_s" not in event
    assert "gap_ratio" not in event
    assert "range_ratio" not in event


def test_non_sma_runtime_like_decision_exports_canonical_v2_without_sma_fields() -> None:
    event = runtime_decision_to_canonical_event(
        StrategyDecision(
            signal="HOLD",
            reason="noop hold",
            context={
                "strategy": "noop_baseline",
                "strategy_version": "noop_baseline.research_contract.v1",
                "strategy_decision_contract_version": "research_noop_baseline_decision_contract.v1",
                "ts": 1_714_521_660_000,
                "raw_signal": "HOLD",
                "final_signal": "HOLD",
                "feature_snapshot": {"candle_index": 1},
                "position_gate": {
                    "entry_allowed": True,
                    "exit_allowed": False,
                    "dust_state": "flat",
                    "effective_flat": True,
                    "normalized_exposure_active": False,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                    "has_any_position_residue": False,
                    "order_rules": {"source": "test", "min_qty": 0.0001},
                },
                "exit": {"evaluations": []},
            },
        ),
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    ).as_dict()

    assert event["decision_contract_version"] == 2
    assert event["strategy_name"] == "noop_baseline"
    assert event["strategy_behavior_hash"].startswith("sha256:")
    assert "prev_s" not in event
    assert "curr_s" not in event
    assert "gap_ratio" not in event
