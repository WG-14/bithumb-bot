from __future__ import annotations

import json

from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal


def _source_artifact(tmp_path) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {
                    "fee_rate": 0.0004,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def test_kst18_artifact_records_outside_window(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["decision_kst_hour"] == 18
    assert payload["daily_participation_entry_authorized"] is False
    assert payload["daily_participation_reason_code"] == "outside_daily_participation_window"


def test_kst10_artifact_records_fallback_allowed(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["decision_kst_hour"] == 10
    assert payload["daily_participation_entry_authorized"] is True
    assert payload["daily_participation_reason_code"] == "daily_participation_fallback_allowed"
