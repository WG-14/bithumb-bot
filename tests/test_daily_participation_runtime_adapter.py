from __future__ import annotations

from bithumb_bot.runtime_adapters.daily_participation_sma import DailyParticipationSmaRuntimeDecisionAdapter
from bithumb_bot.runtime_data_provider import RuntimeFeatureSnapshot
from bithumb_bot.runtime_strategy_decision import get_runtime_decision_adapter


class _Request:
    strategy_name = "daily_participation_sma"
    pair = "KRW-BTC"
    interval = "1m"
    through_ts_ms = 1_704_046_800_000
    parameters = {}
    runtime_strategy_spec = None

    def observability_fields(self):
        return {}


def test_daily_adapter_exposes_feature_snapshot_entry_only() -> None:
    adapter = DailyParticipationSmaRuntimeDecisionAdapter()

    assert hasattr(adapter, "decide_feature_snapshot")
    assert adapter.typed_authority_required() is True


def test_daily_adapter_rejects_missing_daily_count_snapshot() -> None:
    adapter = DailyParticipationSmaRuntimeDecisionAdapter()
    snapshot = RuntimeFeatureSnapshot(
        {
            "feature_payload": {"sma_with_filter": {"payload_hash": "sha256:" + "1" * 64}},
            "feature_snapshot_hash": "sha256:" + "2" * 64,
        }
    )

    assert adapter.decide_feature_snapshot(_Request(), snapshot) is None


def test_daily_adapter_rejects_missing_base_sma_projection() -> None:
    adapter = DailyParticipationSmaRuntimeDecisionAdapter()
    snapshot = RuntimeFeatureSnapshot(
        {
            "feature_payload": {
                "daily_participation_count_snapshot": {
                    "count_basis": "filled",
                    "timezone": "Asia/Seoul",
                    "kst_day": "2024-01-01",
                    "count_for_kst_day": 0,
                    "timestamp_field": "fill_ts",
                    "source": "unit",
                    "rows": [],
                },
            },
            "feature_snapshot_hash": "sha256:" + "2" * 64,
        }
    )

    assert adapter.decide_feature_snapshot(_Request(), snapshot) is None


def test_daily_adapter_does_not_expose_db_bound_decide() -> None:
    adapter = DailyParticipationSmaRuntimeDecisionAdapter()

    assert "decide" not in type(adapter).__dict__


def test_get_runtime_decision_adapter_daily_participation_sma_returns_adapter() -> None:
    adapter = get_runtime_decision_adapter("daily_participation_sma")

    assert isinstance(adapter, DailyParticipationSmaRuntimeDecisionAdapter)
