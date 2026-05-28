from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping

from .approved_profile import (
    ApprovedProfileError,
    PROFILE_HASH_FIELD,
    approved_profile_path_from_env,
    diff_profile_to_runtime,
    expected_profile_modes_for_runtime,
    load_approved_profile,
    runtime_contract_from_settings,
)
from .config import settings, validate_live_strategy_selection
from .decision_equivalence import sha256_prefixed
from .research.strategy_registry import (
    ResearchStrategyRegistryError,
    resolve_research_strategy_plugin,
    runtime_strategy_parameters_from_settings,
)
from .research.strategy_spec import (
    exit_policy_from_parameters,
    materialized_strategy_parameters_hash,
)
from .runtime_strategy_decision import (
    RuntimeDecisionRequest,
    RuntimeStrategyDecisionResult,
    _attach_runtime_request_metadata,
    get_runtime_decision_adapter,
    is_runtime_strategy_decision_result,
    production_runtime_strategy_missing_error,
)


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


@dataclass(frozen=True)
class RuntimeStrategySpec:
    strategy_name: str
    enabled: bool = True
    pair: str | None = None
    interval: str | None = None
    priority: int = 100
    weight: float = 1.0
    desired_exposure_krw: float | None = None
    risk_budget_krw: float | None = None
    parameters: Mapping[str, object] | None = None
    approved_profile_path: str | None = None
    approved_profile_hash: str | None = None
    parameter_source: str | None = None
    runtime_contract_hash: str | None = None
    strategy_version: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        name = str(self.strategy_name or "").strip().lower()
        if not name:
            raise ValueError("runtime_strategy_name_missing")
        pair = str(self.pair or settings.PAIR).strip()
        if not pair:
            raise ValueError("runtime_strategy_pair_missing")
        interval = str(self.interval or settings.INTERVAL).strip()
        if not interval:
            raise ValueError("runtime_strategy_interval_missing")
        weight = float(self.weight)
        if weight <= 0.0:
            raise ValueError("runtime_strategy_weight_must_be_positive")
        risk_budget = _optional_float(self.risk_budget_krw)
        if risk_budget is not None and risk_budget < 0.0:
            raise ValueError("runtime_strategy_risk_budget_must_be_non_negative")
        desired_exposure = _optional_float(self.desired_exposure_krw)
        if desired_exposure is not None and desired_exposure < 0.0:
            raise ValueError("runtime_strategy_desired_exposure_must_be_non_negative")
        object.__setattr__(self, "strategy_name", name)
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", weight)
        object.__setattr__(self, "desired_exposure_krw", desired_exposure)
        object.__setattr__(self, "risk_budget_krw", risk_budget)
        object.__setattr__(
            self,
            "parameters",
            MappingProxyType({str(key): value for key, value in dict(self.parameters or {}).items()}),
        )
        if self.approved_profile_path is not None:
            object.__setattr__(self, "approved_profile_path", str(self.approved_profile_path).strip() or None)
        if self.approved_profile_hash is not None:
            object.__setattr__(self, "approved_profile_hash", str(self.approved_profile_hash).strip() or None)
        if self.parameter_source is not None:
            object.__setattr__(self, "parameter_source", str(self.parameter_source).strip() or None)
        if self.runtime_contract_hash is not None:
            object.__setattr__(self, "runtime_contract_hash", str(self.runtime_contract_hash).strip() or None)
        if self.strategy_version is not None:
            object.__setattr__(self, "strategy_version", str(self.strategy_version).strip() or None)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "strategy_name": self.strategy_name,
            "enabled": bool(self.enabled),
            "pair": self.pair,
            "interval": self.interval,
            "priority": int(self.priority),
            "weight": float(self.weight),
            "desired_exposure_krw": self.desired_exposure_krw,
            "risk_budget_krw": self.risk_budget_krw,
            "parameters": dict(self.parameters or {}),
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "parameter_source": self.parameter_source,
            "runtime_contract_hash": self.runtime_contract_hash,
            "strategy_version": self.strategy_version,
        }


@dataclass(frozen=True)
class RuntimeStrategySet:
    strategies: tuple[RuntimeStrategySpec, ...]
    source: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        active = tuple(item for item in self.strategies if item.enabled)
        if not active:
            raise ValueError("runtime_strategy_set_empty")
        seen: set[tuple[str, str]] = set()
        for item in active:
            key = (item.strategy_name, str(item.pair))
            if key in seen:
                raise ValueError(f"runtime_strategy_duplicate:{item.strategy_name}:{item.pair}")
            seen.add(key)
        object.__setattr__(
            self,
            "strategies",
            tuple(sorted(self.strategies, key=lambda item: (str(item.pair), item.priority, item.strategy_name))),
        )

    @property
    def active_strategies(self) -> tuple[RuntimeStrategySpec, ...]:
        return tuple(item for item in self.strategies if item.enabled)

    @property
    def multi_strategy_enabled(self) -> bool:
        return len(self.active_strategies) > 1

    def spec_for_strategy(self, strategy_name: str) -> RuntimeStrategySpec | None:
        normalized = str(strategy_name or "").strip().lower()
        for spec in self.active_strategies:
            if spec.strategy_name == normalized:
                return spec
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "source": self.source,
            "multi_strategy_enabled": self.multi_strategy_enabled,
            "strategies": [item.as_dict() for item in self.strategies],
            "active_strategies": [item.as_dict() for item in self.active_strategies],
        }


class RuntimeStrategySetResolver:
    """Resolve the runtime strategy set without changing legacy defaults."""

    def __init__(
        self,
        *,
        env_getter: Callable[[str], str | None] | None = None,
        settings_obj: object = settings,
    ) -> None:
        self._env_getter = env_getter or os.getenv
        self._settings = settings_obj

    def resolve(self) -> RuntimeStrategySet:
        raw_json = str(self._env_getter("RUNTIME_STRATEGY_SET_JSON") or "").strip()
        if raw_json:
            return RuntimeStrategySet(
                strategies=tuple(self._spec_from_mapping(item) for item in self._load_json_specs(raw_json)),
                source="RUNTIME_STRATEGY_SET_JSON",
            )
        raw_active = str(
            self._env_getter("ACTIVE_STRATEGIES")
            or getattr(self._settings, "ACTIVE_STRATEGIES", "")
            or ""
        ).strip()
        if raw_active:
            return RuntimeStrategySet(
                strategies=tuple(
                    self._default_spec(name.strip())
                    for name in raw_active.split(",")
                    if name.strip()
                ),
                source="ACTIVE_STRATEGIES",
            )
        return RuntimeStrategySet(
            strategies=(self._default_spec(str(getattr(self._settings, "STRATEGY_NAME", ""))),),
            source="STRATEGY_NAME",
        )

    def _load_json_specs(self, raw_json: str) -> list[Mapping[str, object]]:
        payload = json.loads(raw_json)
        if isinstance(payload, Mapping):
            payload = payload.get("strategies", ())
        if not isinstance(payload, list):
            raise ValueError("runtime_strategy_set_json_must_be_list")
        specs: list[Mapping[str, object]] = []
        for item in payload:
            if not isinstance(item, Mapping):
                raise ValueError("runtime_strategy_set_json_item_must_be_object")
            specs.append(item)
        return specs

    def _default_spec(self, strategy_name: str) -> RuntimeStrategySpec:
        target = getattr(self._settings, "TARGET_EXPOSURE_KRW", None)
        if target is None:
            target = getattr(self._settings, "MAX_ORDER_KRW", None)
        return RuntimeStrategySpec(
            strategy_name=strategy_name,
            pair=str(getattr(self._settings, "PAIR", "")),
            interval=str(getattr(self._settings, "INTERVAL", "")),
            desired_exposure_krw=_optional_float(target),
        )

    def _spec_from_mapping(self, payload: Mapping[str, object]) -> RuntimeStrategySpec:
        if "strategy_name" not in payload and "name" in payload:
            payload = {**dict(payload), "strategy_name": payload["name"]}
        default = self._default_spec(str(payload.get("strategy_name", "")))
        return RuntimeStrategySpec(
            strategy_name=str(payload.get("strategy_name", default.strategy_name)),
            enabled=bool(payload.get("enabled", default.enabled)),
            pair=str(payload.get("pair", default.pair)),
            interval=str(payload.get("interval", default.interval)),
            priority=int(payload.get("priority", default.priority)),
            weight=float(payload.get("weight", default.weight)),
            desired_exposure_krw=payload.get("desired_exposure_krw", default.desired_exposure_krw),
            risk_budget_krw=payload.get("risk_budget_krw", default.risk_budget_krw),
            parameters=payload.get("parameters") if isinstance(payload.get("parameters"), Mapping) else None,
            approved_profile_path=(
                str(payload.get("approved_profile_path") or payload.get("profile_path") or "").strip()
                or default.approved_profile_path
            ),
            approved_profile_hash=(
                str(payload.get("approved_profile_hash") or payload.get("profile_hash") or "").strip()
                or default.approved_profile_hash
            ),
            parameter_source=(
                str(payload.get("parameter_source") or "").strip()
                or default.parameter_source
            ),
            runtime_contract_hash=(
                str(payload.get("runtime_contract_hash") or "").strip()
                or default.runtime_contract_hash
            ),
            strategy_version=(
                str(payload.get("strategy_version") or "").strip()
                or default.strategy_version
            ),
        )


@dataclass(frozen=True)
class RuntimeDecisionRequestBuilder:
    settings_obj: object = settings

    def build_for_spec(
        self,
        spec: RuntimeStrategySpec,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeDecisionRequest:
        try:
            plugin = resolve_research_strategy_plugin(spec.strategy_name)
        except (ResearchStrategyRegistryError, ApprovedProfileError):
            if spec.strategy_name != "safe_hold":
                raise
            plugin = None
        cfg = replace(self.settings_obj, STRATEGY_NAME=spec.strategy_name)
        approved_profile_path = spec.approved_profile_path or approved_profile_path_from_env() or None
        profile = None
        if approved_profile_path:
            profile = load_approved_profile(approved_profile_path)
            profile_hash = str(profile.get(PROFILE_HASH_FIELD) or "")
            if spec.approved_profile_hash and spec.approved_profile_hash != profile_hash:
                raise RuntimeError(f"approved_profile_hash_mismatch_for_runtime_strategy:{spec.strategy_name}")
            approved_profile_hash = profile_hash
            parameters = dict(profile["strategy_parameters"])
            parameter_source = "strict_profile"
        else:
            self._require_profile_for_live_compatible_runtime(spec)
            approved_profile_hash = spec.approved_profile_hash
            parameters, parameter_source = self._parameters_for_spec(spec)
        strategy_parameters_hash = materialized_strategy_parameters_hash(parameters)
        try:
            settings_runtime_contract = runtime_contract_from_settings(cfg)
        except (ResearchStrategyRegistryError, ApprovedProfileError):
            if spec.strategy_name != "safe_hold":
                raise
            settings_runtime_contract = {
                "schema_version": 1,
                "mode": str(getattr(cfg, "MODE", "")),
                "strategy_name": spec.strategy_name,
                "market": str(spec.pair or getattr(cfg, "PAIR", "")),
                "interval": str(spec.interval or getattr(cfg, "INTERVAL", "")),
                "strategy_parameters": {},
            }
        settings_runtime_contract = dict(settings_runtime_contract)
        settings_runtime_contract["strategy_name"] = spec.strategy_name
        settings_runtime_contract["market"] = str(spec.pair or getattr(cfg, "PAIR", ""))
        settings_runtime_contract["interval"] = str(spec.interval or getattr(cfg, "INTERVAL", ""))
        if approved_profile_path and not str(settings_runtime_contract.get("profile_selector") or "").strip():
            settings_runtime_contract["profile_selector"] = approved_profile_path
        if profile is not None:
            expected_modes, mode_reason = expected_profile_modes_for_runtime(settings_runtime_contract)
            if expected_modes is not None and len(expected_modes) == 0:
                raise RuntimeError(
                    f"approved_profile_runtime_mode_invalid:{spec.strategy_name}:{mode_reason or 'unknown'}"
                )
            if expected_modes is not None and str(profile.get("profile_mode") or "") not in expected_modes:
                raise RuntimeError(
                    "approved_profile_runtime_parameter_mismatch:"
                    f"{spec.strategy_name}:profile_mode"
                )
            mismatches = diff_profile_to_runtime(
                profile,
                settings_runtime_contract,
                profile_path=approved_profile_path,
            )
            if mismatches:
                fields = ",".join(str(item.get("field") or "unknown") for item in mismatches)
                raise RuntimeError(f"approved_profile_runtime_parameter_mismatch:{spec.strategy_name}:{fields}")
        runtime_contract = dict(settings_runtime_contract)
        runtime_contract = dict(runtime_contract)
        runtime_contract["strategy_name"] = spec.strategy_name
        runtime_contract["market"] = str(spec.pair or getattr(cfg, "PAIR", ""))
        runtime_contract["interval"] = str(spec.interval or getattr(cfg, "INTERVAL", ""))
        runtime_contract["strategy_parameters"] = dict(parameters)
        if spec.strategy_name == "safe_hold":
            runtime_contract["exit_policy"] = {"schema_version": 1, "rules": (), "strategy_name": "safe_hold"}
        else:
            runtime_contract["exit_policy"] = exit_policy_from_parameters(spec.strategy_name, dict(parameters))
        runtime_contract["exit_policy_hash"] = sha256_prefixed(runtime_contract["exit_policy"])
        runtime_contract_hash = spec.runtime_contract_hash or sha256_prefixed(runtime_contract)
        plugin_contract_hash = plugin.contract_hash() if hasattr(plugin, "contract_hash") else None
        strategy_version = spec.strategy_version or getattr(plugin, "version", None)
        request_payload = {
            "schema_version": 1,
            "strategy_name": spec.strategy_name,
            "pair": spec.pair,
            "interval": str(spec.interval or getattr(cfg, "INTERVAL", "")),
            "through_ts_ms": through_ts_ms,
            "parameters": dict(parameters),
            "strategy_parameters_hash": strategy_parameters_hash,
            "approved_profile_path": approved_profile_path,
            "approved_profile_hash": approved_profile_hash,
            "runtime_contract_hash": runtime_contract_hash,
            "plugin_contract_hash": plugin_contract_hash,
            "strategy_version": strategy_version,
            "runtime_strategy_spec": spec.as_dict(),
            "parameter_source": parameter_source,
        }
        request_hash = sha256_prefixed(request_payload)
        return RuntimeDecisionRequest(
            strategy_name=spec.strategy_name,
            pair=str(spec.pair),
            interval=str(spec.interval or getattr(cfg, "INTERVAL", "")),
            through_ts_ms=through_ts_ms,
            parameters=parameters,
            strategy_parameters_hash=strategy_parameters_hash,
            approved_profile_path=approved_profile_path,
            approved_profile_hash=approved_profile_hash,
            runtime_strategy_spec=spec,
            runtime_contract_hash=runtime_contract_hash,
            parameter_source=parameter_source,
            plugin_contract_hash=plugin_contract_hash,
            strategy_version=strategy_version,
            request_hash=request_hash,
        )

    def _parameters_for_spec(self, spec: RuntimeStrategySpec) -> tuple[dict[str, object], str]:
        if spec.parameters:
            return dict(spec.parameters), spec.parameter_source or "runtime_strategy_spec"
        if spec.strategy_name == "safe_hold":
            return {}, spec.parameter_source or "runtime_builtin_no_parameters"
        return (
            runtime_strategy_parameters_from_settings(spec.strategy_name, self.settings_obj),
            spec.parameter_source or "settings_compat",
        )

    def _require_profile_for_live_compatible_runtime(self, spec: RuntimeStrategySpec) -> None:
        mode = str(getattr(self.settings_obj, "MODE", "") or "").strip().lower()
        if mode != "live":
            return
        live_dry_run = bool(getattr(self.settings_obj, "LIVE_DRY_RUN", False))
        live_real_order_armed = bool(getattr(self.settings_obj, "LIVE_REAL_ORDER_ARMED", False))
        if live_dry_run or live_real_order_armed:
            raise RuntimeError(
                f"approved_profile_required_for_live_compatible_runtime_strategy:{spec.strategy_name}"
            )


@dataclass(frozen=True)
class RuntimeStrategyDecisionResultBundle:
    strategy_set: RuntimeStrategySet
    results: tuple[RuntimeStrategyDecisionResult, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        active_names = {item.strategy_name for item in self.strategy_set.active_strategies}
        result_names = {str(result.decision.strategy_name).strip().lower() for result in self.results}
        if active_names != result_names:
            missing = sorted(active_names - result_names)
            extra = sorted(result_names - active_names)
            raise ValueError(f"runtime_strategy_result_set_mismatch:missing={missing}:extra={extra}")
        candle_ts_values = {int(result.candle_ts) for result in self.results}
        if len(candle_ts_values) != 1:
            raise ValueError("runtime_strategy_results_must_share_candle")
        object.__setattr__(
            self,
            "results",
            tuple(sorted(self.results, key=lambda item: str(item.decision.strategy_name).strip().lower())),
        )

    @property
    def candle_ts(self) -> int:
        return int(self.results[0].candle_ts)

    @property
    def market_price(self) -> float:
        return float(self.results[0].market_price)


class RuntimeStrategyDecisionCollector:
    request_builder: RuntimeDecisionRequestBuilder = RuntimeDecisionRequestBuilder()

    def collect(
        self,
        conn,
        strategy_set: RuntimeStrategySet,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeStrategyDecisionResultBundle | None:
        results: list[RuntimeStrategyDecisionResult] = []
        for spec in strategy_set.active_strategies:
            adapter = get_runtime_decision_adapter(spec.strategy_name)
            validate_live_strategy_selection(replace(settings, STRATEGY_NAME=spec.strategy_name))
            if adapter is None:
                raise production_runtime_strategy_missing_error(spec.strategy_name)
            request = self.request_builder.build_for_spec(spec, through_ts_ms=through_ts_ms)
            result = adapter.decide(conn, request)
            if result is None:
                return None
            if not is_runtime_strategy_decision_result(result):
                raise TypeError(f"typed_runtime_decision_required:{spec.strategy_name}")
            _attach_runtime_request_metadata(result, request)
            if int(result.candle_ts) != int(through_ts_ms or result.candle_ts):
                raise ValueError(f"runtime_strategy_candle_mismatch:{spec.strategy_name}")
            results.append(result)
        return RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=tuple(results))


def active_runtime_strategy_set() -> RuntimeStrategySet:
    return RuntimeStrategySetResolver().resolve()


def collect_runtime_strategy_decisions(
    conn,
    *,
    through_ts_ms: int | None,
    strategy_set: RuntimeStrategySet | None = None,
) -> RuntimeStrategyDecisionResultBundle | None:
    resolved = strategy_set or active_runtime_strategy_set()
    return RuntimeStrategyDecisionCollector().collect(
        conn,
        resolved,
        through_ts_ms=through_ts_ms,
    )
