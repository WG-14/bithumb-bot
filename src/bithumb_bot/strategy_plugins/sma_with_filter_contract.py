from __future__ import annotations

from typing import Any

from bithumb_bot.research.strategy_registry import (
    DataCapabilityRequirement,
    ResearchStrategyDataRequirements,
)
from bithumb_bot.research.strategy_spec import SMA_WITH_FILTER_SPEC
from bithumb_bot.strategy_evidence_contract import DecisionEvidenceContract


SMA_DECISION_EVIDENCE_CONTRACT = DecisionEvidenceContract(
    requires_decision_input_bundle=True,
    required_promotion_provenance_fields=(
        "decision_input_bundle_hash",
        "decision_input_contract_hash",
        "decision_input_bundle_payload_hash",
        "market_feature_hash",
        "final_exit_decision_input_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
    ),
    required_live_real_order_fields=(
        "decision_input_bundle_hash",
        "decision_input_contract_hash",
        "decision_input_bundle_payload_hash",
        "market_feature_hash",
        "final_exit_decision_input_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
    ),
    snapshot_projector_contract="sma_with_filter_snapshot_projector_v1",
    decision_input_contract_kind="generic",
)


def sma_runtime_data_requirements(runtime_strategy_spec: object | None) -> ResearchStrategyDataRequirements:
    params = (
        dict(getattr(runtime_strategy_spec, "parameters", {}) or {})
        if runtime_strategy_spec is not None
        else {}
    )

    def _int_param(name: str, default: int) -> int:
        try:
            return int(params.get(name, default))
        except (TypeError, ValueError):
            return default

    long_n = _int_param("SMA_LONG", 30)
    vol_window = _int_param("SMA_FILTER_VOL_WINDOW", 10)
    overext_lookback = _int_param("SMA_FILTER_OVEREXT_LOOKBACK", 3)
    lookback_rows = max(long_n + 2, vol_window, overext_lookback + 1)
    return ResearchStrategyDataRequirements(
        required_data=SMA_WITH_FILTER_SPEC.required_data,
        optional_data=SMA_WITH_FILTER_SPEC.optional_data,
        capabilities=(
            DataCapabilityRequirement(
                name="candles",
                required=True,
                min_coverage_pct=100.0,
                lookback_rows=lookback_rows,
                closed_candle_required=True,
                source="sqlite_candles",
                evidence_level="closed_candle_lookback",
            ),
            DataCapabilityRequirement(name="top_of_book", required=False),
        ),
    )


__all__ = ["SMA_DECISION_EVIDENCE_CONTRACT", "sma_runtime_data_requirements"]
