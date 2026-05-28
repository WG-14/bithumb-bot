from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping, Protocol, runtime_checkable

from .config import settings, validate_live_strategy_selection
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


class RuntimeDecisionAdapter(Protocol):
    strategy_name: str

    def decide(
        self,
        conn,
        request: RuntimeDecisionRequest,
    ) -> RuntimeStrategyDecisionResult | None: ...

    def typed_authority_required(self) -> bool: ...


RuntimeDecisionAdapterFactory = Callable[[], RuntimeDecisionAdapter]

_DERIVED_RUNTIME_DECISION_ADAPTER_CACHE: dict[tuple[str, str, str], RuntimeDecisionAdapter] = {}
_TEST_ONLY_RUNTIME_DECISION_ADAPTERS: dict[str, RuntimeDecisionAdapterFactory] = {}
_RUNTIME_DECISION_ADAPTERS = _TEST_ONLY_RUNTIME_DECISION_ADAPTERS


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("runtime strategy name must not be empty")
    return key


def register_runtime_decision_adapter(
    name: str,
    factory: RuntimeDecisionAdapterFactory,
) -> None:
    """Compatibility test hook.

    Production runtime adapter resolution is derived from ResearchStrategyPlugin
    manifests. Registering here intentionally does not make an unregistered
    strategy runtime-resolvable.
    """
    _TEST_ONLY_RUNTIME_DECISION_ADAPTERS[_normalize_name(name)] = factory


def reset_runtime_decision_adapters_for_tests() -> None:
    _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.clear()
    _TEST_ONLY_RUNTIME_DECISION_ADAPTERS.clear()


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


def get_runtime_decision_adapter(name: str) -> RuntimeDecisionAdapter | None:
    from .research.strategy_registry import ResearchStrategyRegistryError, resolve_research_strategy_plugin

    key = _normalize_name(name)
    try:
        plugin = resolve_research_strategy_plugin(key)
    except ResearchStrategyRegistryError:
        return None
    if not plugin.runtime_capabilities.promotion_runtime_decisions_supported:
        return None
    test_factory = _TEST_ONLY_RUNTIME_DECISION_ADAPTERS.get(plugin.name)
    factory = test_factory or plugin.runtime_decision_adapter_factory
    if factory is None:
        return None
    if test_factory is not None:
        adapter = test_factory()
        adapter_name = _normalize_name(getattr(adapter, "strategy_name", ""))
        if adapter_name != plugin.name:
            raise RuntimeError(f"runtime_decision_adapter_name_mismatch:{plugin.name}:{adapter_name}")
        return adapter
    contract_hash = plugin.contract_hash()
    cache_key = (plugin.name, contract_hash, "plugin")
    cached = _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.get(cache_key)
    if cached is not None:
        return cached
    adapter = factory()
    adapter_name = _normalize_name(getattr(adapter, "strategy_name", ""))
    if adapter_name != plugin.name:
        raise RuntimeError(f"runtime_decision_adapter_name_mismatch:{plugin.name}:{adapter_name}")
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
    compute_signal_fn: object | None = None,
    original_compute_signal_fn: object | None = None,
) -> bool:
    adapter = get_runtime_decision_adapter(selected_strategy_name)
    if adapter is None:
        return _production_missing_adapter_requires_typed_handoff()
    if (
        compute_signal_fn is not None
        and original_compute_signal_fn is not None
        and compute_signal_fn is not original_compute_signal_fn
    ):
        return False
    return adapter.typed_authority_required()


def typed_runtime_handoff_failure_reason(
    signal_handoff: object,
    *,
    selected_strategy_name: str,
    compute_signal_fn: object | None = None,
    original_compute_signal_fn: object | None = None,
) -> str | None:
    if not promotion_grade_typed_runtime_decision_required(
        selected_strategy_name=selected_strategy_name,
        compute_signal_fn=compute_signal_fn,
        original_compute_signal_fn=original_compute_signal_fn,
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
    ) -> RuntimeStrategyDecisionResult | None:
        selected_strategy_name = str(
            strategy_name or self.strategy_name or settings.STRATEGY_NAME
        ).strip().lower()
        validate_live_strategy_selection(
            replace(settings, STRATEGY_NAME=selected_strategy_name)
        )
        adapter = get_runtime_decision_adapter(selected_strategy_name)
        if adapter is None:
            raise production_runtime_strategy_missing_error(selected_strategy_name)
        from .runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec

        request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(strategy_name=selected_strategy_name),
            through_ts_ms=through_ts_ms,
        )
        result = adapter.decide(conn, request)
        if result is not None:
            _attach_runtime_request_metadata(result, request)
        return result


def compute_strategy_decision_snapshot(
    conn,
    *diagnostic_sma_windows: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
) -> RuntimeStrategyDecisionResult | None:
    if diagnostic_sma_windows:
        selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME or "").strip().lower()
        if selected_strategy_name != "sma_with_filter":
            validate_live_strategy_selection(replace(settings, STRATEGY_NAME=selected_strategy_name))
            adapter = get_runtime_decision_adapter(selected_strategy_name)
            if adapter is None:
                raise production_runtime_strategy_missing_error(selected_strategy_name)
        else:
            from .research.strategy_registry import runtime_strategy_parameters_from_settings
            from .runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec

            short_n = int(diagnostic_sma_windows[0])
            long_n = int(diagnostic_sma_windows[1]) if len(diagnostic_sma_windows) > 1 else int(settings.SMA_LONG)
            parameters = runtime_strategy_parameters_from_settings("sma_with_filter", settings)
            parameters["SMA_SHORT"] = short_n
            parameters["SMA_LONG"] = long_n
            request = RuntimeDecisionRequestBuilder().build_for_spec(
                RuntimeStrategySpec(
                    strategy_name="sma_with_filter",
                    parameters=parameters,
                    parameter_source="diagnostic_sma_windows",
                ),
                through_ts_ms=through_ts_ms,
            )
            adapter = get_runtime_decision_adapter("sma_with_filter")
            if adapter is None:
                raise production_runtime_strategy_missing_error("sma_with_filter")
            result = adapter.decide(conn, request)
            if result is not None:
                _attach_runtime_request_metadata(result, request)
            return result
    return DecisionRunner(strategy_name=strategy_name).decide_snapshot(
        conn,
        through_ts_ms=through_ts_ms,
    )


def compute_signal_runtime_handoff(
    conn,
    *diagnostic_sma_windows: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
) -> RuntimeStrategyDecisionResult | None:
    return compute_strategy_decision_snapshot(
        conn,
        *diagnostic_sma_windows,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
    )


def compute_signal(
    conn,
    *diagnostic_sma_windows: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
):
    if diagnostic_sma_windows:
        selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME or "").strip().lower()
        if selected_strategy_name != "sma_with_filter":
            validate_live_strategy_selection(replace(settings, STRATEGY_NAME=selected_strategy_name))
            adapter = get_runtime_decision_adapter(selected_strategy_name)
            if adapter is None:
                raise production_runtime_strategy_missing_error(selected_strategy_name)
        from .runtime_adapters.sma_with_filter import compute_sma_with_filter_signal

        return compute_sma_with_filter_signal(
            conn,
            *diagnostic_sma_windows,
            through_ts_ms=through_ts_ms,
        )
    result = compute_signal_runtime_handoff(
        conn,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
    )
    if result is None:
        return None
    payload = result.as_legacy_dict()
    payload.setdefault("strategy", result.decision.strategy_name)
    return payload


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


ORIGINAL_COMPUTE_SIGNAL = compute_signal
