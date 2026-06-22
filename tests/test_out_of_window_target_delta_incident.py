from __future__ import annotations

from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
from bithumb_bot.live_trade_classification import classify_h74_live_trade


INCIDENT = {
    "timestamp": "2026-06-22T18:26:09+09:00",
    "client_order_id": "live_1782120240000_buy_ae16bfbd",
    "exchange_order_id": "C0101000003118335790",
    "side": "BUY",
    "filled": True,
    "final_signal": "HOLD",
    "target_delta_side": "BUY",
    "target_delta_notional_krw": 100000,
    "current_exposure": 0,
    "desired_exposure_krw": 100000,
    "decision_reason_code": "target_delta_rebalance",
    "authority_source": "target_delta",
    "entry_authority_status": "ALLOW",
    "decision_kst_hour": 18,
}


def test_live_1782120240000_buy_incident_is_blocked_after_fix(tmp_path) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        '{"runtime_base_cost_assumption":{"fee_rate":0.0004,"fee_source":"research_realistic_bithumb_app_fee","slippage_bps":10,"slippage_source":"research_assumption"},"candle_timing":"closed_candle_kst"}',
        encoding="utf-8",
    )
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(kst_time="18:00", source_artifact_path=str(source)))

    assert payload["broker_submit_reached"] is False
    assert payload["actual_submit"] is False
    assert payload["primary_block_gate"] == "entry_authority"
    assert payload["primary_block_reason"] == "target_delta_entry_without_strategy_buy_authority"


def test_incident_is_classified_not_h74_backtest_sample() -> None:
    classified = classify_h74_live_trade(INCIDENT)

    assert classified["live_plumbing_success"] is True
    assert classified["h74_backtest_validation_sample"] is False
    assert classified["incident_type"] == "out_of_window_target_delta_entry"
