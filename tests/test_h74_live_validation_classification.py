from __future__ import annotations

from bithumb_bot.live_trade_classification import classify_h74_live_trade, h74_performance_samples


def test_daily_participation_fill_is_validation_sample() -> None:
    classified = classify_h74_live_trade(
        {
            "side": "BUY",
            "filled": True,
            "decision_reason_code": "daily_participation_fallback_allowed",
            "authority_source": "daily_participation_entry",
            "decision_kst_hour": 10,
        }
    )

    assert classified["h74_backtest_validation_sample"] is True
    assert classified["incident_type"] == "none"


def test_out_of_window_target_delta_fill_is_incident_not_sample() -> None:
    classified = classify_h74_live_trade(
        {
            "side": "BUY",
            "filled": True,
            "decision_reason_code": "target_delta_rebalance",
            "authority_source": "target_delta",
            "entry_authority_status": "ALLOW",
            "decision_kst_hour": 18,
        }
    )

    assert classified["h74_backtest_validation_sample"] is False
    assert classified["incident_type"] == "out_of_window_target_delta_entry"


def test_performance_report_excludes_incident_samples() -> None:
    daily = {
        "side": "BUY",
        "filled": True,
        "decision_reason_code": "daily_participation_fallback_allowed",
        "authority_source": "daily_participation_entry",
        "decision_kst_hour": 10,
    }
    incident = {
        "side": "BUY",
        "filled": True,
        "decision_reason_code": "target_delta_rebalance",
        "authority_source": "target_delta",
        "entry_authority_status": "ALLOW",
        "decision_kst_hour": 18,
    }

    assert h74_performance_samples([daily, incident]) == [daily]
