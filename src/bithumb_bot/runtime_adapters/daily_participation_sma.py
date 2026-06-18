from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bithumb_bot.research.strategy_spec import SMA_WITH_FILTER_SPEC
from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeConfig
from bithumb_bot.runtime_sma_snapshot_builder import build_sma_with_filter_runtime_decision_from_feature_snapshot
from bithumb_bot.runtime_strategy_decision import RuntimeStrategyDecisionResult
from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationCountSnapshot,
    require_runtime_comparable_daily_count_snapshot,
)


@dataclass(frozen=True)
class DailyParticipationSmaRuntimeDecisionAdapter:
    strategy_name: str = "daily_participation_sma"

    def decide_feature_snapshot(
        self,
        request: Any,
        feature_snapshot: Any,
    ) -> RuntimeStrategyDecisionResult | None:
        payload = feature_snapshot.as_dict() if hasattr(feature_snapshot, "as_dict") else {}
        feature_payload = payload.get("feature_payload") if isinstance(payload, dict) else {}
        if not isinstance(feature_payload, dict):
            return None
        if not isinstance(feature_payload.get("sma_with_filter"), dict):
            return None
        count_payload = feature_payload.get("daily_participation_count_snapshot")
        if not isinstance(count_payload, dict):
            return None
        count_snapshot = _count_snapshot_from_payload(count_payload)
        require_runtime_comparable_daily_count_snapshot(count_snapshot)

        request_parameters = getattr(request, "parameters", {}) or {}
        request_parameters = dict(request_parameters) if isinstance(request_parameters, Mapping) else {}
        base_params = {
            key: value
            for key, value in request_parameters.items()
            if key in SMA_WITH_FILTER_SPEC.accepted_parameter_names
        }
        strategy = SmaWithFilterRuntimeConfig.from_parameter_payload(
            pair=str(getattr(request, "pair", "") or ""),
            interval=str(getattr(request, "interval", "") or ""),
            parameters=base_params,
        ).build_strategy(candidate_regime_policy=_candidate_regime_policy(request))
        boundary_telemetry = request.observability_fields() if hasattr(request, "observability_fields") else {}
        base_result = build_sma_with_filter_runtime_decision_from_feature_snapshot(
            strategy,
            feature_snapshot,
            boundary_telemetry=boundary_telemetry,
        )
        if base_result is None:
            return None
        from bithumb_bot.strategy_plugins.daily_participation_sma import (
            daily_participation_config_from_parameters,
            materialize_strategy_parameters,
            _daily_runtime_result_from_base,
        )

        participation_config = daily_participation_config_from_parameters(
            materialize_strategy_parameters(
                "daily_participation_sma",
                request_parameters,
                fee_rate=0.0,
                slippage_bps=0.0,
            )
        )
        return _daily_runtime_result_from_base(
            base_result=base_result,
            participation_config=participation_config,
            count_snapshot=count_snapshot,
            decision_ts=int(getattr(request, "through_ts_ms", None) or getattr(base_result, "candle_ts", 0) or 0),
        )

    def typed_authority_required(self) -> bool:
        return True


def _candidate_regime_policy(request: Any) -> dict[str, object] | None:
    runtime_instance = getattr(request, "runtime_strategy_spec", None)
    runtime_adapter_config = (
        dict(getattr(runtime_instance, "runtime_adapter_config", {}) or {})
        if runtime_instance is not None
        else {}
    )
    return (
        dict(runtime_adapter_config.get("candidate_regime_policy"))
        if isinstance(runtime_adapter_config.get("candidate_regime_policy"), dict)
        else None
    )


def _count_snapshot_from_payload(payload: dict[str, object]) -> DailyParticipationCountSnapshot:
    return DailyParticipationCountSnapshot(
        count_basis=str(payload.get("count_basis") or "filled"),  # type: ignore[arg-type]
        timezone=str(payload.get("timezone") or "Asia/Seoul"),
        kst_day=str(payload.get("kst_day") or ""),
        count_for_kst_day=int(payload.get("count_for_kst_day") or 0),
        timestamp_field=str(payload.get("timestamp_field") or "fill_ts"),
        source=str(payload.get("source") or ""),
        rows=tuple(dict(row) for row in payload.get("rows") or () if isinstance(row, dict)),
        fail_closed_reason=str(payload.get("fail_closed_reason") or ""),
        pair=str(payload.get("pair") or ""),
        strategy_instance_id=str(payload.get("strategy_instance_id") or ""),
        event_set_hash=str(payload.get("event_set_hash") or ""),
        source_contract_hash=str(payload.get("source_contract_hash") or ""),
        query_contract_hash=str(payload.get("query_contract_hash") or ""),
        source_contract_version=str(payload.get("source_contract_version") or ""),
    )
