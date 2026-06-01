from __future__ import annotations

import sqlite3
import time
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from .broker.order_rules import get_effective_order_rules
from .canonical_decision import order_rules_snapshot_payload
from .config import settings
from .core.sma_policy import PositionSnapshot, StrategyDecisionV2
from .dust import build_dust_display_context, build_position_state_model
from .fee_authority import build_fee_authority_snapshot
from .lifecycle import (
    OPEN_POSITION_STATE,
    summarize_position_lots,
    summarize_reserved_exit_qty,
)
from .runtime_readonly_guard import readonly_decision_context
from .strategy_plugins.sma_with_filter_assembly import (
    MaterializationMode,
    SmaWithFilterPolicyAssembly,
)
from .strategy_plugins.sma_with_filter_projector import SmaWithFilterSnapshotProjector
from .strategy_plugins.sma_with_filter_projector import SmaWithFilterRuntimeProjectionResult
from .strategy_decision_service import StrategyDecisionService, StrategyEvaluationRequest
from .research.strategy_spec import materialized_strategy_parameters_hash
from .runtime_position_state_normalizer import (
    load_last_reconcile_metadata,
)
from .runtime_sma_context import (
    build_entry_decision_context,
    build_position_gate_context,
    build_position_state_context,
    fee_authority_context,
    legacy_strategy_decision_from_sma_final_decision,
    live_armed_entry_fee_authority_blocks,
    resolve_strategy_fee_authority,
    safe_ratio,
)
from .strategy.base import PositionContext, StrategyDecision
from .strategy.sma_policy_strategy import SmaWithFilterStrategy


@dataclass(frozen=True)
class RuntimeSmaPolicyHashes:
    pure_policy_hash: str
    policy_contract_hash: str
    policy_input_hash: str
    policy_decision_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            "pure_policy_hash": self.pure_policy_hash,
            "policy_contract_hash": self.policy_contract_hash,
            "policy_input_hash": self.policy_input_hash,
            "policy_decision_hash": self.policy_decision_hash,
        }


@dataclass(frozen=True)
class RuntimeSmaReplayFingerprint:
    payload: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


@dataclass(frozen=True)
class RuntimeSmaDecisionContext:
    """Typed authority-critical SMA decision context.

    ``as_dict`` is for persistence and replay observability. Execution
    authority stays with typed policy and execution objects.
    """

    policy_contract_hash: str
    policy_input_hash: str
    policy_decision_hash: str
    pure_policy_hash: str
    pure_policy_trace: dict[str, object]
    final_signal: str
    final_reason: str
    blocked_filters: tuple[str, ...]
    entry_blocked: bool
    entry_block_reason: str
    execution_intent: dict[str, object] | None
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]

    @classmethod
    def from_decision(
        cls,
        *,
        decision: StrategyDecisionV2,
        replay_fingerprint: dict[str, object],
        boundary: dict[str, object] | None = None,
    ) -> "RuntimeSmaDecisionContext":
        trace = decision.as_trace()
        raw_intent = trace.get("execution_intent")
        return cls(
            policy_contract_hash=decision.policy_contract_hash,
            policy_input_hash=decision.policy_input_hash,
            policy_decision_hash=decision.policy_decision_hash,
            pure_policy_hash=decision.policy_hash,
            pure_policy_trace=deepcopy(trace),
            final_signal=decision.final_signal,
            final_reason=decision.final_reason,
            blocked_filters=tuple(str(item) for item in decision.blocked_filters),
            entry_blocked=bool(decision.entry_blocked),
            entry_block_reason=str(decision.entry_block_reason or ""),
            execution_intent=dict(raw_intent) if isinstance(raw_intent, dict) else None,
            replay_fingerprint=dict(replay_fingerprint),
            boundary=dict(boundary or {}),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "policy_contract_hash": self.policy_contract_hash,
            "policy_input_hash": self.policy_input_hash,
            "policy_decision_hash": self.policy_decision_hash,
            "pure_policy_hash": self.pure_policy_hash,
            "pure_policy_trace": deepcopy(self.pure_policy_trace),
            "final_signal": self.final_signal,
            "final_reason": self.final_reason,
            "blocked_filters": list(self.blocked_filters),
            "entry_blocked": bool(self.entry_blocked),
            "entry_block_reason": self.entry_block_reason,
            "execution_intent": (
                None if self.execution_intent is None else deepcopy(self.execution_intent)
            ),
            "replay_fingerprint": deepcopy(self.replay_fingerprint),
            "boundary": deepcopy(self.boundary),
        }


@dataclass(frozen=True)
class RuntimeSmaDecisionResult:
    """Typed SMA runtime decision before legacy persistence serialization."""

    decision: StrategyDecisionV2
    base_context: dict[str, Any]
    position: PositionContext
    exposure: object
    position_state: object
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object] | None = None

    def __post_init__(self) -> None:
        # ``base_context`` is legacy serialization material. Keep a private copy
        # boundary so callers cannot mutate the originally supplied mapping after
        # construction and accidentally alter this result's compatibility payload.
        object.__setattr__(self, "base_context", dict(self.base_context))
        object.__setattr__(self, "replay_fingerprint", dict(self.replay_fingerprint))
        object.__setattr__(self, "boundary", dict(self.boundary or {}))

    @property
    def policy_hashes(self) -> RuntimeSmaPolicyHashes:
        return RuntimeSmaPolicyHashes(
            pure_policy_hash=self.decision.policy_hash,
            policy_contract_hash=self.decision.policy_contract_hash,
            policy_input_hash=self.decision.policy_input_hash,
            policy_decision_hash=self.decision.policy_decision_hash,
        )

    @property
    def replay_fingerprint_snapshot(self) -> RuntimeSmaReplayFingerprint:
        return RuntimeSmaReplayFingerprint(self.replay_fingerprint)

    @property
    def runtime_decision_context(self) -> RuntimeSmaDecisionContext:
        return RuntimeSmaDecisionContext.from_decision(
            decision=self.decision,
            replay_fingerprint=self.replay_fingerprint,
            boundary=self.boundary,
        )

    @property
    def policy_observability(self) -> dict[str, object]:
        context = self.runtime_decision_context.as_dict()
        return {
            **self.policy_hashes.as_dict(),
            "pure_policy_trace": context["pure_policy_trace"],
            "final_signal": context["final_signal"],
            "final_reason": context["final_reason"],
            "blocked_filters": context["blocked_filters"],
            "entry_blocked": context["entry_blocked"],
            "entry_block_reason": context["entry_block_reason"],
            "exit_rule": self.decision.exit_rule,
            "exit_evaluations": [dict(item) for item in self.decision.exit_evaluations],
            "execution_intent": context["execution_intent"],
            "replay_fingerprint": context["replay_fingerprint"],
            "boundary": context["boundary"],
        }

    def _authoritative_policy_context(self) -> dict[str, object]:
        return {
            "pure_policy_hash": self.decision.policy_hash,
            "policy_contract_hash": self.decision.policy_contract_hash,
            "policy_input_hash": self.decision.policy_input_hash,
            "policy_decision_hash": self.decision.policy_decision_hash,
            "pure_policy_trace": self.decision.as_trace(),
            "boundary": deepcopy(self.boundary),
        }

    def legacy_strategy_decision(self) -> StrategyDecision:
        return legacy_strategy_decision_from_sma_final_decision(
            decision=self.decision,
            base_context={**dict(self.base_context), **self._authoritative_policy_context()},
            position=self.position,
            exposure=self.exposure,
            position_state=self.position_state,
        )

    def as_legacy_dict(self) -> dict[str, Any]:
        payload = self.legacy_strategy_decision().as_dict()
        payload.update(self._authoritative_policy_context())
        return payload


def _load_signal_rows(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int | None,
) -> list[sqlite3.Row | tuple[Any, ...]]:
    from .runtime_data_provider_sma import load_sma_signal_rows

    return load_sma_signal_rows(
        conn,
        pair=pair,
        interval=interval,
        through_ts_ms=through_ts_ms,
    )


def _closed_candle_cutoff_ts_ms(*, interval_sec: int, now_ms: int | None = None) -> int | None:
    """Return the latest candle start timestamp that is safely closed now."""
    interval_ms = max(1, int(interval_sec)) * 1000
    close_guard_ms = max(2_000, min(30_000, interval_ms // 20))
    current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff_ts_ms = current_ms - interval_ms - close_guard_ms
    return cutoff_ts_ms if cutoff_ts_ms >= 0 else None


def _load_position_context(
    conn: sqlite3.Connection,
    *,
    pair: str,
    candle_ts: int,
    market_price: float,
    signal_context: dict[str, Any],
    slippage_bps: float,
    entry_edge_buffer_ratio: float,
) -> tuple[PositionContext, object, object, dict[str, object]]:
    from .runtime_data_provider_sma import load_sma_position_context

    return load_sma_position_context(
        conn,
        pair=pair,
        candle_ts=candle_ts,
        market_price=market_price,
        signal_context=signal_context,
        slippage_bps=slippage_bps,
        entry_edge_buffer_ratio=entry_edge_buffer_ratio,
    )


def _policy_position_snapshot(
    *,
    position: PositionContext,
    exposure: object,
) -> PositionSnapshot:
    return PositionSnapshot(
        in_position=bool(position.in_position),
        entry_allowed=bool(exposure.entry_allowed),
        exit_allowed=bool(exposure.exit_allowed),
        entry_block_reason=str(exposure.entry_block_reason or ""),
        exit_block_reason=str(exposure.exit_block_reason or ""),
        terminal_state=str(exposure.terminal_state),
        entry_ts=position.entry_ts,
        entry_price=position.entry_price,
        qty_open=float(position.qty_open),
        holding_time_sec=float(position.holding_time_sec),
        unrealized_pnl=float(position.unrealized_pnl),
        unrealized_pnl_ratio=float(position.unrealized_pnl_ratio),
        raw_qty_open=float(exposure.raw_qty_open),
        raw_total_asset_qty=float(exposure.raw_total_asset_qty),
        open_lot_count=int(exposure.open_lot_count),
        dust_tracking_lot_count=int(exposure.dust_tracking_lot_count),
        reserved_exit_lot_count=int(exposure.reserved_exit_lot_count),
        sellable_executable_lot_count=int(exposure.sellable_executable_lot_count),
        dust_classification=str(exposure.dust_classification),
        dust_state=str(exposure.dust_state),
        effective_flat=bool(exposure.effective_flat),
        has_executable_exposure=bool(exposure.has_executable_exposure),
        has_any_position_residue=bool(exposure.has_any_position_residue),
        has_non_executable_residue=bool(exposure.has_non_executable_residue),
        has_dust_only_remainder=bool(exposure.has_dust_only_remainder),
    )


def _namespace_from_mapping(payload: dict[str, object]) -> SimpleNamespace:
    converted = {
        key: (
            _namespace_from_mapping(value)
            if isinstance(value, dict)
            else value
        )
        for key, value in dict(payload).items()
    }
    return SimpleNamespace(**converted)


def _position_state_proxy(payload: dict[str, object]) -> SimpleNamespace:
    normalized = dict(payload.get("normalized_exposure") or {})
    operator = dict(payload.get("operator_diagnostics") or {})
    normalized.setdefault("dust_operator_view", _namespace_from_mapping(operator))
    exposure = _namespace_from_mapping(normalized)
    if not hasattr(exposure, "dust_operator_view"):
        object.__setattr__(exposure, "dust_operator_view", _namespace_from_mapping(operator))

    class _PositionStateProxy(SimpleNamespace):
        def as_dict(self) -> dict[str, object]:
            return dict(payload)

    return _PositionStateProxy(normalized_exposure=exposure)


def build_sma_with_filter_runtime_decision_from_feature_snapshot(
    strategy: object,
    feature_snapshot: object,
    *,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Build the promotion SMA decision from RuntimeFeatureSnapshot material only."""
    payload = feature_snapshot.as_dict() if hasattr(feature_snapshot, "as_dict") else {}
    feature_payload = payload.get("feature_payload") if isinstance(payload, dict) else {}
    if not isinstance(feature_payload, dict):
        return None
    runtime_payload = feature_payload.get("sma_with_filter")
    if not isinstance(runtime_payload, dict):
        return None
    rows = [dict(item) for item in runtime_payload.get("candles") or () if isinstance(item, dict)]
    if not rows:
        return None
    if int(strategy.short_n) >= int(strategy.long_n):
        raise ValueError("short must be less than long; example: short=7 long=30")
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_from_strategy(strategy, MaterializationMode.RUNTIME_REPLAY)
    closes = [float(row["close"]) for row in rows]
    highs = [float(row.get("high", row["close"]) if row.get("high") is not None else row["close"]) for row in rows]
    lows = [float(row.get("low", row["close"]) if row.get("low") is not None else row["close"]) for row in rows]
    volumes = [float(row.get("volume", 0.0) or 0.0) for row in rows]
    ts_list = [int(row["ts"]) for row in rows]
    signal_through_ts_ms = payload.get("through_ts_ms")
    projector = SmaWithFilterSnapshotProjector(assembly)
    features = projector.project_features_from_arrays(
        pair=strategy.pair,
        interval=strategy.interval,
        ts_list=ts_list,
        closes=closes,
        highs=highs,
        lows=lows,
        volumes=volumes,
        materialized=materialized,
        through_ts_ms=None if signal_through_ts_ms is None else int(signal_through_ts_ms),
        allow_initial_cross=True,
    )
    if features is None:
        return None
    position_payload = runtime_payload.get("position_context")
    position_snapshot_payload = runtime_payload.get("position_snapshot")
    position_state_payload = runtime_payload.get("position_state")
    if not isinstance(position_payload, dict) or not isinstance(position_snapshot_payload, dict):
        return None
    if not isinstance(position_state_payload, dict):
        return None
    position = PositionContext(**position_payload)
    position_snapshot = PositionSnapshot(**position_snapshot_payload)
    position_state = _position_state_proxy(position_state_payload)
    exposure = position_state.normalized_exposure
    order_rules_snapshot = (
        dict(runtime_payload.get("order_rules"))
        if isinstance(runtime_payload.get("order_rules"), dict)
        else {}
    )
    fee_authority_payload = (
        dict(runtime_payload.get("fee_authority"))
        if isinstance(runtime_payload.get("fee_authority"), dict)
        else {}
    )
    fee_rate_for_decision = float(runtime_payload.get("fee_rate_for_decision") or 0.0)
    signal_context = {
        "strategy": strategy.name,
        "prev_s": features.prev_s,
        "prev_l": features.prev_l,
        "curr_s": features.curr_s,
        "curr_l": features.curr_l,
    }
    market_snapshot = assembly.build_market_snapshot(
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=features.candle_ts,
        closes=features.closes,
        prev_s=features.prev_s,
        prev_l=features.prev_l,
        curr_s=features.curr_s,
        curr_l=features.curr_l,
        through_ts_ms=None if signal_through_ts_ms is None else int(signal_through_ts_ms),
        gap_ratio=features.gap_ratio,
        volatility_ratio=features.volatility_ratio,
        overextended_ratio=features.overextended_ratio,
        market_regime_snapshot=features.market_regime_snapshot,
        previous_cross_state=features.previous_cross_state,
        allow_initial_cross=features.allow_initial_cross,
    )
    policy_config = assembly.build_policy_config(
        materialized,
        strategy,
        candidate_regime_policy=strategy.candidate_regime_policy,
    )
    execution_snapshot = assembly.build_execution_snapshot_from_payloads(
        fee_rate_for_decision=fee_rate_for_decision,
        fee_authority_degraded_blocks_entry=bool(runtime_payload.get("fee_authority_degraded_blocks_entry")),
        fee_authority=fee_authority_payload,
        order_rules=order_rules_snapshot,
    )
    exit_policy_config = assembly.build_exit_policy_config(
        materialized,
        fee_rate_for_decision=fee_rate_for_decision,
    )
    decision_input_bundle = projector.project_from_runtime_projection(
        projection=SmaWithFilterRuntimeProjectionResult(
            strategy=strategy,
            materialized=materialized,
            market=market_snapshot,
            position=position_snapshot,
            config=policy_config,
            execution_constraints=execution_snapshot,
            exit_policy_config=exit_policy_config,
            provenance={
                "candle_ts": int(ts_list[-1]),
                "through_ts_ms": None if signal_through_ts_ms is None else int(signal_through_ts_ms),
                "canonical_feature_projection": features.diagnostics_payload(),
                "previous_cross_state": features.previous_cross_state,
                "provider_contract_hash": payload.get("provider_contract_hash"),
                "runtime_data_availability_report_hash": payload.get("runtime_data_availability_report_hash"),
                "feature_snapshot_hash": payload.get("feature_snapshot_hash"),
                "source_schema_hash": payload.get("source_schema_hash"),
            },
        ),
    )
    initial_replay_fingerprint = projector.build_replay_fingerprint(
        strategy_name=strategy.name,
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=int(ts_list[-1]),
        through_ts_ms=None if signal_through_ts_ms is None else int(signal_through_ts_ms),
        materialized=materialized,
        bundle=decision_input_bundle,
        regime_version=str((market_snapshot.market_regime_snapshot or {}).get("version") or ""),
    )
    request_metadata = {
        **dict(boundary_telemetry or {}),
        "feature_snapshot_hash": payload.get("feature_snapshot_hash"),
        "runtime_data_market_snapshot_hash": payload.get("market_snapshot_hash"),
        "runtime_data_contract_hash": payload.get("runtime_data_contract_hash"),
        "provider_contract_hash": payload.get("provider_contract_hash"),
        "runtime_data_availability_report_hash": payload.get("runtime_data_availability_report_hash"),
        "source_schema_hash": payload.get("source_schema_hash"),
    }
    strategy_parameters_hash = str(
        request_metadata.get("strategy_parameters_hash")
        or materialized_strategy_parameters_hash(dict(materialized.values))
    )
    approved_profile_hash = (
        str(request_metadata.get("approved_profile_hash") or "")
        or (
            strategy.candidate_regime_policy.get("strategy_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else ""
        )
        or None
    )
    policy_strategy = (
        strategy
        if isinstance(strategy, SmaWithFilterStrategy)
        else assembly.build_strategy(
            materialized,
            pair=strategy.pair,
            interval=strategy.interval,
            candidate_regime_policy=strategy.candidate_regime_policy,
        )
    )
    final_policy_result = StrategyDecisionService().evaluate(
        StrategyEvaluationRequest(
            strategy_name=strategy.name,
            strategy_instance_id=(
                str(request_metadata.get("strategy_instance_id") or "")
                or f"{strategy.name}:runtime_replay"
            ),
            mode="runtime_replay",
            strategy_policy=policy_strategy,
            market_snapshot=market_snapshot,
            position_snapshot=position_snapshot,
            strategy_config=policy_config,
            execution_constraints=execution_snapshot,
            exit_policy_config=exit_policy_config,
            rule_sources=_default_sma_exit_rule_sources(exit_policy_config),
            approved_profile_hash=approved_profile_hash,
            runtime_contract_hash=str(request_metadata.get("runtime_contract_hash") or "") or None,
            plugin_contract_hash=str(request_metadata.get("plugin_contract_hash") or "") or None,
            request_hash=str(request_metadata.get("runtime_decision_request_hash") or "") or None,
            provenance={
                **request_metadata,
                "decision_boundary": "StrategyDecisionService.evaluate",
                "snapshot_builder": "RuntimeDataProvider.RuntimeFeatureSnapshot",
                "runtime_feature_snapshot": payload,
                "strategy_parameters_hash": strategy_parameters_hash,
                "replay_fingerprint": initial_replay_fingerprint,
                "approved_profile_hash_unavailable_reason": "runtime_snapshot_no_approved_profile_hash"
                if not approved_profile_hash
                else "",
                "plugin_contract_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("plugin_contract_hash")
                else "",
                "runtime_contract_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("runtime_contract_hash")
                else "",
                "runtime_decision_request_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("runtime_decision_request_hash")
                else "",
                "code_provenance": {
                    "policy_module": policy_strategy.__class__.__module__,
                    "policy_class": policy_strategy.__class__.__name__,
                },
            },
            decision_input_bundle=decision_input_bundle,
        )
    )
    final_policy_decision = final_policy_result.decision
    entry_decision = final_policy_decision.entry_decision
    blocked_filters = list(final_policy_decision.blocked_filters)
    base_signal = final_policy_decision.raw_signal
    base_reason = final_policy_decision.raw_reason
    entry_signal = final_policy_decision.entry_signal
    entry_reason = final_policy_decision.entry_reason
    edge_filter_details = entry_decision.edge_filter_details
    replay_fingerprint = dict(final_policy_result.replay_fingerprint)
    base_context = {
        **request_metadata,
        "ts": ts_list[-1],
        "last_close": float(closes[-1]),
        "market_price": float(closes[-1]),
        "strategy": strategy.name,
        "pair": strategy.pair,
        "interval": strategy.interval,
        "base_signal": base_signal,
        "base_reason": base_reason,
        "entry_signal": entry_signal,
        "entry_reason": entry_reason,
        "pure_policy_hash": final_policy_decision.policy_hash,
        "pure_policy_trace": final_policy_decision.as_trace(),
        "policy_contract_hash": final_policy_decision.policy_contract_hash,
        "policy_input_hash": final_policy_decision.policy_input_hash,
        "policy_decision_hash": final_policy_decision.policy_decision_hash,
        "decision_input_bundle_hash": decision_input_bundle.decision_input_bundle_hash,
        "decision_input_contract_hash": decision_input_bundle.decision_input_contract_hash,
        "decision_input_bundle_payload_hash": decision_input_bundle.decision_input_bundle_payload_hash,
        "snapshot_projector_version": decision_input_bundle.snapshot_projector_version,
        "snapshot_projector_hash": decision_input_bundle.snapshot_projector_hash,
        "materialized_parameters_hash": decision_input_bundle.materialized_parameters_hash,
        "parameter_sources": dict(materialized.sources),
        "runtime_comparable": bool(materialized.runtime_comparable),
        "materialization_mode": materialized.mode.value,
        "policy_materialization_mode": materialized.mode.value,
        "legacy_defaults_used": list(materialized.legacy_defaults_used),
        "market_snapshot_hash": replay_fingerprint.get("market_snapshot_hash", decision_input_bundle.market_snapshot_hash),
        "market_feature_hash": replay_fingerprint.get("market_feature_hash", decision_input_bundle.market_feature_hash),
        "canonical_feature_projection_hash": replay_fingerprint.get(
            "market_feature_hash",
            decision_input_bundle.market_feature_hash,
        ),
        "feature_snapshot_hash": replay_fingerprint.get("market_feature_hash", decision_input_bundle.market_feature_hash),
        "final_exit_decision_input_hash": final_policy_decision.as_trace().get("final_exit_decision_input_hash"),
        "position_snapshot_hash": decision_input_bundle.position_snapshot_hash,
        "execution_constraints_hash": decision_input_bundle.execution_constraints_hash,
        "policy_config_hash": decision_input_bundle.policy_config_hash,
        "exit_policy_config_hash": decision_input_bundle.exit_policy_config_hash,
        "strategy_evaluation_provenance": dict(final_policy_result.provenance),
        "replay_fingerprint_hash": final_policy_result.replay_fingerprint_hash,
        "prev_s": features.prev_s,
        "prev_l": features.prev_l,
        "curr_s": features.curr_s,
        "curr_l": features.curr_l,
        "features": {
            "prev_s": features.prev_s,
            "prev_l": features.prev_l,
            "curr_s": features.curr_s,
            "curr_l": features.curr_l,
            "sma_gap_ratio": entry_decision.gap_ratio,
            "volatility_range_ratio": entry_decision.volatility_ratio,
            "overextended_abs_return_ratio": entry_decision.overextended_ratio,
            "base_signal": base_signal,
            "base_reason": base_reason,
        },
        "order_rules": order_rules_snapshot,
        "position_gate": build_position_gate_context(exposure, order_rules=order_rules_snapshot),
        "position_state": build_position_state_context(position_state),
        "fee_authority": fee_authority_payload,
        "filters": {
            "cost_edge": {
                "enabled": bool(edge_filter_details["enabled"]),
                "configured_enabled": bool(edge_filter_details["configured_enabled"]),
                "signal_eligible": bool(edge_filter_details["signal_eligible"]),
                "passed": not bool(edge_filter_details["blocked"]),
                "value": float(edge_filter_details["expected_edge_ratio"]),
                "threshold": float(edge_filter_details["required_edge_ratio"]),
                "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                "fee_authority_source": fee_authority_payload.get("fee_source"),
                "fee_authority_degraded": bool(fee_authority_payload.get("degraded", False)),
            },
        },
        "blocked_filters": blocked_filters,
        "entry": build_entry_decision_context(
            pair=strategy.pair,
            base_signal=base_signal,
            base_reason=base_reason,
            entry_signal=entry_signal,
            entry_reason=entry_reason,
            buy_fraction=float(strategy.buy_fraction),
            max_order_krw=float(strategy.max_order_krw),
        ),
        "replay_fingerprint": replay_fingerprint,
    }
    boundary = {
        "normalization_boundary": "runtime_data_provider.position_snapshot_materialization",
        "normalization_updated_count": None,
        "post_normalization_read_only_guard": None,
        "post_decision_total_changes_delta": None,
        "decision_boundary_phase": "feature_snapshot_decision",
        **request_metadata,
    }
    base_context.update(boundary)
    return RuntimeSmaDecisionResult(
        decision=final_policy_decision,
        base_context=base_context,
        position=position,
        exposure=exposure,
        position_state=position_state,
        candle_ts=int(ts_list[-1]),
        market_price=float(closes[-1]),
        replay_fingerprint=replay_fingerprint,
        boundary=boundary,
    )


def _build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Read normalized DB state and return the typed final SMA runtime decision.

    This is the post-normalization read-only phase. It may read candles,
    position projections, fee/rule snapshots, and assemble replay/legacy
    observability payloads, but it must not repair, reclassify, or persist DB
    state. Runtime callers that need state repair must run the explicit
    pre-decision normalizer before entering this helper.
    """
    from .utils_time import parse_interval_sec

    if int(strategy.short_n) >= int(strategy.long_n):
        raise ValueError("short must be less than long; example: short=7 long=30")
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_from_strategy(strategy, MaterializationMode.RUNTIME_REPLAY)

    min_rows = max(
        int(strategy.long_n) + 2,
        int(strategy.volatility_window),
        int(strategy.overextended_lookback) + 1,
    )
    interval_sec = parse_interval_sec(str(strategy.interval))
    signal_through_ts_ms = through_ts_ms
    if signal_through_ts_ms is None:
        signal_through_ts_ms = _closed_candle_cutoff_ts_ms(interval_sec=interval_sec)
        if signal_through_ts_ms is None:
            return None

    rows = _load_signal_rows(
        conn,
        pair=strategy.pair,
        interval=strategy.interval,
        through_ts_ms=signal_through_ts_ms,
    )
    if len(rows) < min_rows:
        return None

    closes = [float(r[1]) for r in rows]
    highs = [float(r[2]) if len(r) > 2 and r[2] is not None else float(r[1]) for r in rows]
    lows = [float(r[3]) if len(r) > 3 and r[3] is not None else float(r[1]) for r in rows]
    volumes = [float(r[4]) if len(r) > 4 and r[4] is not None else 0.0 for r in rows]
    ts_list = [int(r[0]) for r in rows]

    projector = SmaWithFilterSnapshotProjector(assembly)
    features = projector.project_features_from_arrays(
        pair=strategy.pair,
        interval=strategy.interval,
        ts_list=ts_list,
        closes=closes,
        highs=highs,
        lows=lows,
        volumes=volumes,
        materialized=materialized,
        through_ts_ms=signal_through_ts_ms,
        allow_initial_cross=True,
    )
    if features is None:
        return None
    prev_s = features.prev_s
    prev_l = features.prev_l
    curr_s = features.curr_s
    curr_l = features.curr_l

    fee_authority = resolve_strategy_fee_authority(
        pair=strategy.pair,
        config_fallback_fee_rate=float(strategy.live_fee_rate_estimate),
    )
    fee_rate_for_decision = float(fee_authority.taker_roundtrip_fee_rate / 2)
    signal_context = {
        "strategy": strategy.name,
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
    }
    position, exposure, position_state, order_rules_snapshot = _load_position_context(
        conn,
        pair=strategy.pair,
        candle_ts=ts_list[-1],
        market_price=float(closes[-1]),
        signal_context=signal_context,
        slippage_bps=float(strategy.slippage_bps),
        entry_edge_buffer_ratio=float(strategy.entry_edge_buffer_ratio),
    )
    market_snapshot = assembly.build_market_snapshot(
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=features.candle_ts,
        closes=features.closes,
        prev_s=features.prev_s,
        prev_l=features.prev_l,
        curr_s=features.curr_s,
        curr_l=features.curr_l,
        through_ts_ms=signal_through_ts_ms,
        gap_ratio=features.gap_ratio,
        volatility_ratio=features.volatility_ratio,
        overextended_ratio=features.overextended_ratio,
        market_regime_snapshot=features.market_regime_snapshot,
        previous_cross_state=features.previous_cross_state,
        allow_initial_cross=features.allow_initial_cross,
    )
    position_snapshot = _policy_position_snapshot(position=position, exposure=exposure)
    policy_config = assembly.build_policy_config(
        materialized,
        strategy,
        candidate_regime_policy=strategy.candidate_regime_policy,
    )
    execution_snapshot = assembly.build_execution_snapshot_from_payloads(
        fee_rate_for_decision=fee_rate_for_decision,
        fee_authority_degraded_blocks_entry=live_armed_entry_fee_authority_blocks(fee_authority),
        fee_authority=fee_authority_context(fee_authority),
        order_rules=order_rules_snapshot,
    )
    exit_policy_config = assembly.build_exit_policy_config(
        materialized,
        fee_rate_for_decision=fee_rate_for_decision,
    )
    decision_input_bundle = projector.project_from_runtime_projection(
        projection=SmaWithFilterRuntimeProjectionResult(
            strategy=strategy,
            materialized=materialized,
            market=market_snapshot,
            position=position_snapshot,
            config=policy_config,
            execution_constraints=execution_snapshot,
            exit_policy_config=exit_policy_config,
            provenance={
                "candle_ts": int(ts_list[-1]),
                "through_ts_ms": int(signal_through_ts_ms),
                "canonical_feature_projection": features.diagnostics_payload(),
                "previous_cross_state": features.previous_cross_state,
            },
        ),
    )
    initial_replay_fingerprint = projector.build_replay_fingerprint(
        strategy_name=strategy.name,
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=int(ts_list[-1]),
        through_ts_ms=None if signal_through_ts_ms is None else int(signal_through_ts_ms),
        materialized=materialized,
        bundle=decision_input_bundle,
        regime_version=str((market_snapshot.market_regime_snapshot or {}).get("version") or ""),
    )
    policy_strategy = (
        strategy
        if isinstance(strategy, SmaWithFilterStrategy)
        else assembly.build_strategy(
            materialized,
            pair=strategy.pair,
            interval=strategy.interval,
            candidate_regime_policy=strategy.candidate_regime_policy,
        )
    )
    request_metadata = dict(boundary_telemetry or {})
    strategy_parameters_hash = str(
        request_metadata.get("strategy_parameters_hash")
        or materialized_strategy_parameters_hash(dict(materialized.values))
    )
    approved_profile_hash = (
        str(request_metadata.get("approved_profile_hash") or "")
        or (
            strategy.candidate_regime_policy.get("strategy_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else ""
        )
        or None
    )
    final_policy_result = StrategyDecisionService().evaluate(
        StrategyEvaluationRequest(
            strategy_name=strategy.name,
            strategy_instance_id=(
                str(request_metadata.get("strategy_instance_id") or "")
                or f"{strategy.name}:runtime_replay"
            ),
            mode="runtime_replay",
            strategy_policy=policy_strategy,
            market_snapshot=market_snapshot,
            position_snapshot=position_snapshot,
            strategy_config=policy_config,
            execution_constraints=execution_snapshot,
            exit_policy_config=exit_policy_config,
            rule_sources=_default_sma_exit_rule_sources(exit_policy_config),
            approved_profile_hash=approved_profile_hash,
            runtime_contract_hash=str(request_metadata.get("runtime_contract_hash") or "") or None,
            plugin_contract_hash=str(request_metadata.get("plugin_contract_hash") or "") or None,
            request_hash=str(request_metadata.get("runtime_decision_request_hash") or "") or None,
            provenance={
                **request_metadata,
                "decision_boundary": "StrategyDecisionService.evaluate",
                "snapshot_builder": "SmaWithFilterSnapshotProjector",
                "strategy_parameters_hash": strategy_parameters_hash,
                "replay_fingerprint": initial_replay_fingerprint,
                "approved_profile_hash_unavailable_reason": "runtime_snapshot_no_approved_profile_hash"
                if not approved_profile_hash
                else "",
                "plugin_contract_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("plugin_contract_hash")
                else "",
                "runtime_contract_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("runtime_contract_hash")
                else "",
                "runtime_decision_request_hash_unavailable_reason": "runtime_snapshot_direct_call"
                if not request_metadata.get("runtime_decision_request_hash")
                else "",
                "code_provenance": {
                    "policy_module": policy_strategy.__class__.__module__,
                    "policy_class": policy_strategy.__class__.__name__,
                },
            },
            decision_input_bundle=decision_input_bundle,
        )
    )
    final_policy_decision = final_policy_result.decision
    replay_fingerprint = dict(final_policy_result.replay_fingerprint)
    policy_decision = final_policy_decision
    entry_decision = policy_decision.entry_decision
    base_signal = policy_decision.raw_signal
    base_reason = policy_decision.raw_reason
    entry_signal = policy_decision.entry_signal
    entry_reason = policy_decision.entry_reason
    gap_ratio = entry_decision.gap_ratio
    volatility_ratio = entry_decision.volatility_ratio
    overextended_ratio = entry_decision.overextended_ratio
    edge_filter_details = entry_decision.edge_filter_details
    edge_filter_triggered = entry_decision.edge_filter_triggered
    blocked_filters = list(policy_decision.blocked_filters)
    market_regime_triggered = entry_decision.market_regime_triggered
    candidate_regime_triggered = entry_decision.candidate_regime_triggered
    candidate_regime_decision = entry_decision.candidate_regime_decision
    market_regime = entry_decision.market_regime
    vol_window = max(1, int(strategy.volatility_window))
    overext_lookback = max(1, int(strategy.overextended_lookback))
    raw_filter_would_block = bool(entry_decision.raw_filter_would_block)
    entry_blocked_by_filter = bool(entry_decision.entry_blocked)
    should_filter_entry = base_signal == "BUY"

    base_context = {
        "ts": ts_list[-1],
        "last_close": float(closes[-1]),
        "strategy": strategy.name,
        "pair": strategy.pair,
        "interval": strategy.interval,
        "approved_profile_hash": (
            strategy.candidate_regime_policy.get("strategy_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_path": settings.APPROVED_STRATEGY_PROFILE_PATH or None,
        "approved_profile_mode": (
            strategy.candidate_regime_policy.get("approved_profile_mode")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_verification_ok": (
            strategy.candidate_regime_policy.get("approved_profile_verification_ok")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_block_reason": (
            strategy.candidate_regime_policy.get("approved_profile_block_reason")
            or strategy.candidate_regime_policy.get("_policy_load_error")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_loaded": (
            strategy.candidate_regime_policy.get("approved_profile_loaded")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_schema_hash_valid": (
            strategy.candidate_regime_policy.get("approved_profile_schema_hash_valid")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_source_verified": (
            strategy.candidate_regime_policy.get("approved_profile_source_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_evidence_verified": (
            strategy.candidate_regime_policy.get("approved_profile_evidence_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_runtime_verified": (
            strategy.candidate_regime_policy.get("approved_profile_runtime_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_contract_scope": (
            strategy.candidate_regime_policy.get("approved_profile_contract_scope")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_candidate_profile_path_used": (
            strategy.candidate_regime_policy.get("legacy_candidate_profile_path_used")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_profile_contract_scope": (
            strategy.candidate_regime_policy.get("legacy_profile_contract_scope")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "source_promotion_artifact_path": (
            strategy.candidate_regime_policy.get("source_promotion_artifact_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "promotion_content_hash": (
            strategy.candidate_regime_policy.get("source_promotion_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_profile_hash": (
            strategy.candidate_regime_policy.get("candidate_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "manifest_hash": (
            strategy.candidate_regime_policy.get("manifest_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "dataset_content_hash": (
            strategy.candidate_regime_policy.get("dataset_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "lineage_hash": (
            strategy.candidate_regime_policy.get("lineage_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_compatibility_used": (
            strategy.candidate_regime_policy.get("legacy_compatibility_used")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "paper_validation_evidence_path": (
            strategy.candidate_regime_policy.get("paper_validation_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "paper_validation_evidence_content_hash": (
            strategy.candidate_regime_policy.get("paper_validation_evidence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "live_readiness_evidence_path": (
            strategy.candidate_regime_policy.get("live_readiness_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "live_readiness_evidence_content_hash": (
            strategy.candidate_regime_policy.get("live_readiness_evidence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "decision_equivalence_report_path": (
            strategy.candidate_regime_policy.get("decision_equivalence_report_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "decision_equivalence_content_hash": (
            strategy.candidate_regime_policy.get("decision_equivalence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_applied_in_research": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_applied_in_research")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_required_for_live": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_required_for_live")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_required": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_required")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_hash": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_path": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_status": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_status")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_limitation_reasons": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_limitation_reasons")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_next_action": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_next_action")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "base_signal": base_signal,
        "base_reason": base_reason,
        "entry_signal": entry_signal,
        "entry_reason": entry_reason,
        "pure_policy_hash": final_policy_decision.policy_hash,
        "pure_policy_trace": final_policy_decision.as_trace(),
        "policy_contract_hash": final_policy_decision.policy_contract_hash,
        "policy_input_hash": final_policy_decision.policy_input_hash,
        "policy_decision_hash": final_policy_decision.policy_decision_hash,
        "decision_input_bundle_hash": decision_input_bundle.decision_input_bundle_hash,
        "decision_input_contract_hash": decision_input_bundle.decision_input_contract_hash,
        "decision_input_bundle_payload_hash": decision_input_bundle.decision_input_bundle_payload_hash,
        "snapshot_projector_version": decision_input_bundle.snapshot_projector_version,
        "snapshot_projector_hash": decision_input_bundle.snapshot_projector_hash,
        "materialized_parameters_hash": decision_input_bundle.materialized_parameters_hash,
        "parameter_sources": dict(materialized.sources),
        "runtime_comparable": bool(materialized.runtime_comparable),
        "materialization_mode": materialized.mode.value,
        "policy_materialization_mode": materialized.mode.value,
        "legacy_defaults_used": list(materialized.legacy_defaults_used),
        "market_snapshot_hash": replay_fingerprint.get("market_snapshot_hash", decision_input_bundle.market_snapshot_hash),
        "market_feature_hash": replay_fingerprint.get("market_feature_hash", decision_input_bundle.market_feature_hash),
        "canonical_feature_projection_hash": replay_fingerprint.get(
            "market_feature_hash",
            decision_input_bundle.market_feature_hash,
        ),
        "feature_snapshot_hash": replay_fingerprint.get("market_feature_hash", decision_input_bundle.market_feature_hash),
        "final_exit_decision_input_hash": final_policy_decision.as_trace().get("final_exit_decision_input_hash"),
        "position_snapshot_hash": decision_input_bundle.position_snapshot_hash,
        "execution_constraints_hash": decision_input_bundle.execution_constraints_hash,
        "policy_config_hash": decision_input_bundle.policy_config_hash,
        "exit_policy_config_hash": decision_input_bundle.exit_policy_config_hash,
        "strategy_evaluation_provenance": dict(final_policy_result.provenance),
        "replay_fingerprint_hash": final_policy_result.replay_fingerprint_hash,
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
        "features": {
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "sma_gap_ratio": gap_ratio,
            "volatility_range_ratio": volatility_ratio,
            "overextended_abs_return_ratio": overextended_ratio,
            "base_signal": base_signal,
            "base_reason": base_reason,
        },
        "market_regime": market_regime,
        "current_market_regime_snapshot": market_regime,
        "current_regime": candidate_regime_decision.get("current_regime"),
        "current_regime_classifier_version": candidate_regime_decision.get("current_regime_classifier_version"),
        "candidate_regime_classifier_version": candidate_regime_decision.get("candidate_regime_classifier_version"),
        "candidate_allowed_regimes": list(candidate_regime_decision.get("candidate_allowed_regimes") or ()),
        "candidate_blocked_regimes": list(candidate_regime_decision.get("candidate_blocked_regimes") or ()),
        "regime_decision": candidate_regime_decision.get("regime_decision"),
        "regime_block_reason": candidate_regime_decision.get("regime_block_reason"),
        "regime_policy_source": candidate_regime_decision.get("regime_policy_source"),
        "regime_policy_present": bool(candidate_regime_decision.get("regime_policy_present")),
        "regime_policy_valid": bool(candidate_regime_decision.get("regime_policy_valid")),
        **policy_config.candidate_regime_policy_status,
        "order_rules": order_rules_snapshot,
        "position_gate": build_position_gate_context(
            position_state.normalized_exposure,
            order_rules=order_rules_snapshot,
        ),
        "position_state": build_position_state_context(position_state),
        "fee_authority": fee_authority_context(fee_authority),
        "filters": {
            "gap": {
                "enabled": entry_decision.gap_filter_enabled,
                "passed": not entry_decision.gap_triggered,
                "threshold": float(strategy.min_gap_ratio),
                "value": gap_ratio,
            },
            "volatility": {
                "enabled": entry_decision.volatility_filter_enabled,
                "passed": not entry_decision.volatility_triggered,
                "window": vol_window,
                "threshold": float(strategy.min_volatility_ratio),
                "value": volatility_ratio,
            },
            "overextended": {
                "enabled": entry_decision.overextended_filter_enabled,
                "passed": not entry_decision.overextended_triggered,
                "lookback": overext_lookback,
                "threshold": float(strategy.overextended_max_return_ratio),
                "value": overextended_ratio,
            },
            "cost_edge": {
                "enabled": bool(edge_filter_details["enabled"]),
                "configured_enabled": bool(edge_filter_details["configured_enabled"]),
                "signal_eligible": bool(edge_filter_details["signal_eligible"]),
                "passed": not bool(edge_filter_details["blocked"]),
                "value": float(edge_filter_details["expected_edge_ratio"]),
                "threshold": float(edge_filter_details["required_edge_ratio"]),
                "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                "fee_authority_source": fee_authority.fee_source,
                "fee_authority_degraded": bool(fee_authority.degraded),
            },
        },
        "filter_blocked": bool(should_filter_entry and blocked_filters),
        "raw_filter_would_block": bool(raw_filter_would_block),
        "entry_blocked": bool(entry_blocked_by_filter),
        "entry_filter_blocked": bool(raw_filter_would_block),
        "market_regime_blocked": bool(market_regime_triggered),
        "candidate_regime_blocked": bool(candidate_regime_triggered),
        "decision_type": (
            "BLOCKED_ENTRY"
            if base_signal == "BUY" and (blocked_filters or market_regime_triggered or candidate_regime_triggered)
            else base_signal
        ),
        "decision_type_authority": "diagnostic_non_authoritative",
        "signal_observability_authority": "StrategyDecisionV2_non_authoritative_projection",
        "strategy_diagnostics": {
            "decision_type": (
                "BLOCKED_ENTRY"
                if base_signal == "BUY" and (blocked_filters or market_regime_triggered or candidate_regime_triggered)
                else base_signal
            ),
            "raw_signal": base_signal,
            "final_signal": final_policy_decision.final_signal,
            "blocked_filter_count": len(blocked_filters),
            "authority": "diagnostic_non_authoritative",
        },
        "blocked_filters": blocked_filters,
        "gap_ratio": gap_ratio,
        "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
        "position_lot_interpretation_costs": {
            "exit_slippage_bps": float(strategy.slippage_bps),
            "exit_buffer_ratio": float(strategy.entry_edge_buffer_ratio),
        },
        "blocked_by_cost_filter": bool(should_filter_entry and edge_filter_triggered),
        "blocked_by_fee_authority": bool("fee_authority_degraded" in blocked_filters),
        "entry": {
            **build_entry_decision_context(
                pair=strategy.pair,
                base_signal=base_signal,
                base_reason=base_reason,
                entry_signal=entry_signal,
                entry_reason=entry_reason,
                buy_fraction=float(strategy.buy_fraction),
                max_order_krw=float(strategy.max_order_krw),
            ),
            "cost_edge_blocked": bool(should_filter_entry and edge_filter_triggered),
            "blocked_filters": blocked_filters,
            "filter_blocked": bool(should_filter_entry and blocked_filters),
            "raw_filter_would_block": bool(raw_filter_would_block),
            "entry_blocked": bool(entry_blocked_by_filter),
            "raw_filter_blocked": bool(raw_filter_would_block),
        },
    }
    replay_fingerprint = dict(final_policy_result.replay_fingerprint)
    base_context["replay_fingerprint"] = replay_fingerprint
    boundary = {
        "normalization_boundary": "engine.normalize_position_state_before_strategy_decision",
        "normalization_updated_count": None,
        "post_normalization_read_only_guard": None,
        "post_decision_total_changes_delta": None,
        "decision_boundary_phase": "post_normalization_decision",
        **dict(boundary_telemetry or {}),
    }
    base_context.update(boundary)

    return RuntimeSmaDecisionResult(
        decision=final_policy_decision,
        base_context=base_context,
        position=position,
        exposure=exposure,
        position_state=position_state,
        candle_ts=int(ts_list[-1]),
        market_price=float(closes[-1]),
        replay_fingerprint=replay_fingerprint,
        boundary=boundary,
    )


def build_sma_with_filter_runtime_decision_from_normalized_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Guarded post-normalization read-only SMA runtime decision phase."""
    phase = "post_normalization_decision"
    with readonly_decision_context(conn, phase=phase) as guard:
        result = _build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl(
            conn,
            strategy,
            through_ts_ms=through_ts_ms,
            boundary_telemetry={
                "normalization_boundary": "engine.normalize_position_state_before_strategy_decision",
                "normalization_updated_count": None,
                "decision_boundary_phase": phase,
                **dict(boundary_telemetry or {}),
            },
        )
    if result is None:
        return None
    guard_report = guard.report.as_dict()
    boundary = {
        **dict(result.boundary),
        "post_normalization_read_only_guard": guard_report,
        "post_decision_total_changes_delta": guard_report["total_changes_delta"],
    }
    result.base_context.update(boundary)
    object.__setattr__(result, "boundary", boundary)
    return result


def build_sma_with_filter_decision_from_normalized_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    """Compatibility serializer for legacy callers expecting StrategyDecision."""
    result = build_sma_with_filter_runtime_decision_from_normalized_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )
    return None if result is None else result.legacy_strategy_decision()


def decide_sma_with_filter_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    """Read-only compatibility serializer for legacy callers expecting StrategyDecision."""
    result = decide_sma_with_filter_runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )
    return None if result is None else result.legacy_strategy_decision()


def decide_sma_with_filter_runtime_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Read normalized DB state and return a typed sma_with_filter decision.

    This helper is intentionally read-only. Position/dust repair must happen at
    a named orchestration boundary before this function is called.
    """
    signal_through_ts_ms = _resolve_signal_through_ts_ms(
        interval=strategy.interval,
        through_ts_ms=through_ts_ms,
    )
    if signal_through_ts_ms is None:
        return None
    result = build_sma_with_filter_runtime_decision_from_normalized_db(
        conn,
        strategy,
        through_ts_ms=signal_through_ts_ms,
    )
    if result is not None and boundary_telemetry:
        boundary = {**dict(result.boundary), **dict(boundary_telemetry)}
        result.base_context.update(boundary)
        object.__setattr__(result, "boundary", boundary)
    return result


def _resolve_signal_through_ts_ms(*, interval: str, through_ts_ms: int | None) -> int | None:
    from .utils_time import parse_interval_sec

    interval_sec = parse_interval_sec(interval)
    signal_through_ts_ms = through_ts_ms
    if signal_through_ts_ms is None:
        signal_through_ts_ms = _closed_candle_cutoff_ts_ms(interval_sec=interval_sec)
        if signal_through_ts_ms is None:
            return None
    return int(signal_through_ts_ms)


def _default_sma_exit_rule_sources(exit_policy_config: object) -> dict[str, str]:
    return {
        name: "common_risk" if name in {"stop_loss", "max_holding_time"} else "plugin"
        for name in (
            str(item).strip().lower()
            for item in getattr(exit_policy_config, "rule_names", ())
            if str(item).strip()
        )
    }


def _latest_signal_close(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int,
) -> float | None:
    from .runtime_data_provider_sma import latest_sma_signal_close

    return latest_sma_signal_close(
        conn,
        pair=pair,
        interval=interval,
        through_ts_ms=through_ts_ms,
    )
