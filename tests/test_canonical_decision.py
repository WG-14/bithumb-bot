from __future__ import annotations

from bithumb_bot.canonical_decision import (
    export_research_decisions,
    runtime_decision_to_canonical_event,
)
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
    ).as_dict()

    assert event["decision_contract_version"] == 1
    assert event["strategy_contract_version"] == "sma_strategy_v1"
    assert event["raw_signal"] == "BUY"
    assert event["final_signal"] == "HOLD"
    assert event["blocked"] is True
    assert event["blocked_filters"] == ("cost_edge",)
    assert event["position_state_hash"].startswith("sha256:")
    assert event["exit_evaluations_hash"].startswith("sha256:")
    assert event["execution_timing_policy_hash"] == "sha256:timing"
    assert event["order_rules_hash"].startswith("sha256:")


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
            }
        ],
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )

    assert events[0]["decision_contract_version"] == 1
    assert events[0]["profile_content_hash"] == "sha256:profile"
    assert events[0]["dataset_content_hash"] == "sha256:data"
    assert events[0]["side"] == "BUY"
