from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping, Protocol, runtime_checkable

from .config import settings, validate_live_strategy_selection
from .runtime_data_provider import RuntimeFeatureSnapshot
from .strategy_policy_contract import StrategyDecisionV2


@dataclass(frozen=True)
class RuntimeDecisionRequest:
    strategy_instance_id: str
    strategy_name: str
    pair: str
    interval: str
    through_ts_ms: int | None
    parameters: Mapping[str, object]
    parameters_raw: Mapping[str, object]
    parameters_materialized: Mapping[str, object]
    strategy_parameters_hash: str
    approved_profile_path: str | None
    approved_profile_hash: str | None
    runtime_strategy_spec: object
    runtime_contract_hash: str | None
    parameter_source: str
    plugin_contract_hash: str | None
    strategy_version: str | None
    request_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_instance_id", str(self.strategy_instance_id or "").strip())
        object.__setattr__(self, "strategy_name", str(self.strategy_name or "").strip().lower())
        object.__setattr__(self, "pair", str(self.pair or "").strip())
        object.__setattr__(self, "interval", str(self.interval or "").strip())
        object.__setattr__(
            self,
            "parameters",
            MappingProxyType({str(key): value for key, value in dict(self.parameters or {}).items()}),
        )
        object.__setattr__(
            self,
            "parameters_raw",
            MappingProxyType({str(key): value for key, value in dict(self.parameters_raw or {}).items()}),
        )
        object.__setattr__(
            self,
            "parameters_materialized",
            MappingProxyType(
                {str(key): value for key, value in dict(self.parameters_materialized or {}).items()}
            ),
        )

    def observability_fields(self) -> dict[str, object]:
        spec = self.runtime_strategy_spec
        authority_audit = (
            spec.parameter_authority_audit
            if hasattr(spec, "parameter_authority_audit")
            else {}
        )
        legacy_used = (
            bool(spec.legacy_compatibility_used)
            if hasattr(spec, "legacy_compatibility_used")
            else False
        )
        return {
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "strategy_parameters": dict(self.parameters),
            "strategy_parameters_raw": dict(self.parameters_raw),
            "strategy_parameters_materialized": dict(self.parameters_materialized),
            "strategy_parameters_hash": self.strategy_parameters_hash,
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "runtime_contract_hash": self.runtime_contract_hash,
            "through_ts_ms": self.through_ts_ms,
            "candle_ts": self.through_ts_ms,
            "plugin_contract_hash": self.plugin_contract_hash,
            "runtime_decision_request_hash": self.request_hash,
            "request_hash": self.request_hash,
            "parameter_source": self.parameter_source,
            "parameter_authority_audit": dict(authority_audit or {}),
            "legacy_parameter_compatibility_used": legacy_used,
        }

    def as_dict(self) -> dict[str, object]:
        spec = self.runtime_strategy_spec
        spec_payload = spec.as_dict() if hasattr(spec, "as_dict") else str(spec)
        return {
            "schema_version": 1,
            **self.observability_fields(),
            "strategy": self.strategy_name,
            "pair": self.pair,
            "interval": self.interval,
            "runtime_strategy_spec": spec_payload,
        }


@runtime_checkable
class RuntimeStrategyDecisionResult(Protocol):
    decision: object
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    policy_hashes: object | None
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]

    def as_legacy_dict(self) -> dict[str, object]: ...


class LegacyDiagnosticRuntimeDecisionAdapter(Protocol):
    strategy_name: str

    def decide(
        self,
        conn,
        request: RuntimeDecisionRequest,
    ) -> RuntimeStrategyDecisionResult | None: ...


class PromotionRuntimeDecisionAdapter(Protocol):
    strategy_name: str

    def decide_feature_snapshot(
        self,
        request: RuntimeDecisionRequest,
        feature_snapshot: RuntimeFeatureSnapshot,
    ) -> RuntimeStrategyDecisionResult | None: ...

    def typed_authority_required(self) -> bool: ...


class RuntimeDecisionAdapter(PromotionRuntimeDecisionAdapter, Protocol):
    """Deprecated alias for connection-free promotion adapters."""


RuntimeDecisionAdapterFactory = Callable[[], PromotionRuntimeDecisionAdapter]

_DERIVED_RUNTIME_DECISION_ADAPTER_CACHE: dict[tuple[str, str, str], PromotionRuntimeDecisionAdapter] = {}


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("runtime strategy name must not be empty")
    return key


def list_runtime_decision_adapters() -> tuple[str, ...]:
    from .research.strategy_registry import list_research_strategy_plugins

    return tuple(
        sorted(
            plugin.name
            for plugin in list_research_strategy_plugins()
            if plugin.runtime_capabilities.promotion_runtime_decisions_supported
            and plugin.runtime_decision_adapter_factory is not None
        )
    )


def get_runtime_decision_adapter(name: str) -> PromotionRuntimeDecisionAdapter | None:
    from .research.strategy_registry import ResearchStrategyRegistryError, resolve_research_strategy_plugin

    key = _normalize_name(name)
    try:
        plugin = resolve_research_strategy_plugin(key)
    except ResearchStrategyRegistryError:
        return None
    if not plugin.runtime_capabilities.promotion_runtime_decisions_supported:
        return None
    factory = plugin.runtime_decision_adapter_factory
    if factory is None:
        return None
    contract_hash = plugin.contract_hash()
    cache_key = (plugin.name, contract_hash, "plugin")
    cached = _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.get(cache_key)
    if cached is not None:
        return cached
    adapter = factory()
    adapter_name = _normalize_name(getattr(adapter, "strategy_name", ""))
    if adapter_name != plugin.name:
        raise RuntimeError(f"runtime_decision_adapter_name_mismatch:{plugin.name}:{adapter_name}")
    if not promotion_adapter_supports_feature_snapshot(adapter):
        raise RuntimeError(f"runtime_decision_feature_snapshot_required:{plugin.name}")
    if _has_db_bound_decide_method(adapter):
        raise RuntimeError(f"promotion_runtime_adapter_db_bound_decide_forbidden:{plugin.name}")
    _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.clear()
    _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE[cache_key] = adapter
    return adapter


def is_runtime_strategy_decision_result(value: object) -> bool:
    if not isinstance(value, RuntimeStrategyDecisionResult):
        return False
    return isinstance(getattr(value, "decision", None), StrategyDecisionV2)


def production_runtime_strategy_missing_error(selected_strategy_name: str) -> RuntimeError:
    return RuntimeError(f"runtime_decision_adapter_not_registered:{selected_strategy_name}")


def promotion_grade_typed_runtime_decision_required(
    *,
    selected_strategy_name: str,
) -> bool:
    adapter = get_runtime_decision_adapter(selected_strategy_name)
    if adapter is None:
        return _production_missing_adapter_requires_typed_handoff()
    return adapter.typed_authority_required()


def typed_runtime_handoff_failure_reason(
    signal_handoff: object,
    *,
    selected_strategy_name: str,
) -> str | None:
    if not promotion_grade_typed_runtime_decision_required(
        selected_strategy_name=selected_strategy_name,
    ):
        return None
    if is_runtime_strategy_decision_result(signal_handoff):
        return None
    if get_runtime_decision_adapter(selected_strategy_name) is None:
        return "runtime_decision_adapter_not_registered"
    return "typed_runtime_decision_required"


def legacy_db_strategy_fallback_allowed(*, selected_strategy_name: str) -> bool:
    return False


def _production_missing_adapter_requires_typed_handoff() -> bool:
    mode = str(settings.MODE or "").strip().lower()
    if mode == "live":
        return True
    if str(getattr(settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip():
        return True
    return True


@dataclass(frozen=True)
class DecisionRunner:
    """Production runtime strategy decision runner.

    Missing adapters fail closed. Legacy DB-bound strategy execution is exposed
    only by the explicit compatibility API in ``run_loop_compatibility``.
    """

    strategy_name: str | None = None

    def decide_snapshot(
        self,
        conn,
        *,
        through_ts_ms: int | None = None,
        strategy_name: str | None = None,
        parameter_overrides: Mapping[str, object] | None = None,
        parameter_source: str = "runtime_override",
        runtime_strategy_spec: object | None = None,
    ) -> RuntimeStrategyDecisionResult | None:
        from .runtime_strategy_set import (
            RuntimeDecisionRequestBuilder,
            RuntimeMarketScope,
            RuntimeStrategySet,
            RuntimeStrategySpec,
        )

        if runtime_strategy_spec is not None:
            if not isinstance(runtime_strategy_spec, RuntimeStrategySpec):
                raise TypeError("runtime_strategy_spec_invalid")
            spec = runtime_strategy_spec
            selected_strategy_name = spec.strategy_name
        else:
            selected_strategy_name = str(
                strategy_name or self.strategy_name or settings.STRATEGY_NAME
            ).strip().lower()
            parameters = dict(parameter_overrides or {})
            if parameters:
                _reject_unapproved_runtime_overrides(selected_strategy_name)
            spec = RuntimeStrategySpec(
                strategy_name=selected_strategy_name,
                parameters=parameters or None,
                parameter_source=parameter_source if parameters else None,
            )
        validate_live_strategy_selection(
            replace(settings, STRATEGY_NAME=selected_strategy_name)
        )
        adapter = get_runtime_decision_adapter(selected_strategy_name)
        if adapter is None:
            raise production_runtime_strategy_missing_error(selected_strategy_name)

        request = RuntimeDecisionRequestBuilder().build_for_spec(
            spec,
            through_ts_ms=through_ts_ms,
        )
        database_snapshot_decider = getattr(adapter, "decide_database_snapshot", None)
        if callable(database_snapshot_decider):
            result = database_snapshot_decider(conn, request)
            if result is not None:
                _attach_runtime_request_metadata(result, request)
                return result
            if str(settings.MODE or "").strip().lower() != "live":
                return None
        materialized_spec = replace(
            spec,
            parameters=dict(request.parameters),
            parameter_source=request.parameter_source,
            approved_profile_path=request.approved_profile_path,
            approved_profile_hash=request.approved_profile_hash,
            runtime_contract_hash=request.runtime_contract_hash,
            strategy_version=request.strategy_version,
        )
        from .runtime_data_provider import (
            RuntimeDataRequirementResolver,
            SQLiteRuntimeDataProvider,
        )

        provider = SQLiteRuntimeDataProvider(conn)
        strategy_set = RuntimeStrategySet(
            strategies=(materialized_spec,),
            source="DecisionRunner",
            market_scope=RuntimeMarketScope(pair=materialized_spec.pair, interval=materialized_spec.interval),
        )
        requirements = RuntimeDataRequirementResolver().resolve_for_strategy_set(strategy_set)
        preflight = provider.preflight(
            strategy_set,
            through_ts_ms=through_ts_ms,
        )
        if not preflight.ok:
            raise RuntimeError(";".join(preflight.reasons))
        feature_snapshot = provider.snapshot(request, requirements)
        if feature_snapshot is None:
            return None
        feature_decider = getattr(adapter, "decide_feature_snapshot", None)
        if not callable(feature_decider):
            raise RuntimeError(f"runtime_decision_feature_snapshot_required:{selected_strategy_name}")
        result = feature_decider(request, feature_snapshot)
        if result is not None:
            _attach_runtime_feature_snapshot_metadata(result, feature_snapshot)
            _attach_runtime_request_metadata(result, request)
        return result


def compute_strategy_decision_snapshot(
    conn,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
    parameter_overrides: Mapping[str, object] | None = None,
    parameter_source: str = "runtime_override",
    runtime_strategy_spec: object | None = None,
) -> RuntimeStrategyDecisionResult | None:
    return DecisionRunner(strategy_name=strategy_name).decide_snapshot(
        conn,
        through_ts_ms=through_ts_ms,
        parameter_overrides=parameter_overrides,
        parameter_source=parameter_source,
        runtime_strategy_spec=runtime_strategy_spec,
    )


def compute_strategy_decision_for_diagnostics(
    conn,
    *diagnostic_parameters: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
    parameter_overrides: Mapping[str, object] | None = None,
    parameter_source: str = "runtime_override",
    runtime_strategy_spec: object | None = None,
) -> RuntimeStrategyDecisionResult | None:
    if diagnostic_parameters:
        raise TypeError("positional_diagnostic_parameters_unsupported")
    return compute_strategy_decision_snapshot(
        conn,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
        parameter_overrides=parameter_overrides,
        parameter_source=parameter_source,
        runtime_strategy_spec=runtime_strategy_spec,
    )


def compute_legacy_signal_for_diagnostics(
    conn,
    *diagnostic_parameters: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
):
    """Return a legacy dict for explicit CLI/diagnostic callers only.

    Production runtime authority must use RuntimeDecisionGateway and never this
    compatibility serialization.
    """
    if diagnostic_parameters:
        selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME or "").strip().lower()
        validate_live_strategy_selection(replace(settings, STRATEGY_NAME=selected_strategy_name))
        adapter = get_runtime_decision_adapter(selected_strategy_name)
        if adapter is None:
            raise production_runtime_strategy_missing_error(selected_strategy_name)
        legacy_adapter = getattr(adapter, "legacy_diagnostic_signal", None)
        if legacy_adapter is None or not callable(legacy_adapter):
            raise TypeError(f"positional_diagnostic_parameters_unsupported:{selected_strategy_name}")
        return legacy_adapter(conn, *diagnostic_parameters, through_ts_ms=through_ts_ms)
    result = compute_strategy_decision_for_diagnostics(
        conn,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
    )
    if result is None:
        return None
    payload = result.as_legacy_dict()
    payload.setdefault("strategy", result.decision.strategy_name)
    return payload


def _reject_unapproved_runtime_overrides(selected_strategy_name: str) -> None:
    mode = str(settings.MODE or "").strip().lower()
    approved_profile_path = str(getattr(settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
    approved_profile_alias = str(getattr(settings, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
    live_like = mode == "live" or bool(approved_profile_path or approved_profile_alias)
    if live_like:
        raise RuntimeError(f"runtime_parameter_overrides_unapproved:{selected_strategy_name}")


def _attach_runtime_request_metadata(
    result: RuntimeStrategyDecisionResult,
    request: RuntimeDecisionRequest,
) -> None:
    fields = request.observability_fields()
    if isinstance(result.base_context, dict):
        result.base_context.update(fields)
    if isinstance(result.replay_fingerprint, dict):
        result.replay_fingerprint.update(
            {
                "runtime_decision_request_hash": request.request_hash,
                "strategy_instance_id": request.strategy_instance_id,
                "strategy_parameters_hash": request.strategy_parameters_hash,
                "approved_profile_hash": request.approved_profile_hash,
                "runtime_contract_hash": request.runtime_contract_hash,
                "plugin_contract_hash": request.plugin_contract_hash,
                "through_ts_ms": request.through_ts_ms,
            }
        )
    if isinstance(result.boundary, dict):
        result.boundary.update(
            {
                "runtime_decision_request_hash": request.request_hash,
                "strategy_instance_id": request.strategy_instance_id,
                "strategy_parameters_hash": request.strategy_parameters_hash,
            }
        )


def promotion_adapter_supports_feature_snapshot(adapter: object) -> bool:
    return callable(getattr(adapter, "decide_feature_snapshot", None))


def _has_db_bound_decide_method(adapter: object) -> bool:
    return callable(getattr(adapter, "decide", None))


def _attach_runtime_feature_snapshot_metadata(
    result: RuntimeStrategyDecisionResult,
    feature_snapshot: RuntimeFeatureSnapshot,
) -> None:
    payload = feature_snapshot.as_dict()
    fields = {
        "runtime_data_contract_hash": payload.get("runtime_data_contract_hash"),
        "provider_contract_hash": payload.get("provider_contract_hash"),
        "runtime_data_availability_report_hash": payload.get("runtime_data_availability_report_hash"),
        "source_schema_hash": payload.get("source_schema_hash"),
        "feature_snapshot_hash": payload.get("feature_snapshot_hash"),
        "runtime_data_market_snapshot_hash": payload.get("market_snapshot_hash"),
    }
    if isinstance(result.base_context, dict):
        result.base_context.update(fields)
        result.base_context.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
    if isinstance(result.replay_fingerprint, dict):
        result.replay_fingerprint.update(fields)
        result.replay_fingerprint.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
    if isinstance(result.boundary, dict):
        result.boundary.update(fields)
        result.boundary.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
