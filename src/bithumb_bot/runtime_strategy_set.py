from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from typing import Callable, Mapping

from .config import settings, validate_live_strategy_selection
from .runtime_strategy_decision import (
    RuntimeStrategyDecisionResult,
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
    priority: int = 100
    weight: float = 1.0
    desired_exposure_krw: float | None = None
    risk_budget_krw: float | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        name = str(self.strategy_name or "").strip().lower()
        if not name:
            raise ValueError("runtime_strategy_name_missing")
        pair = str(self.pair or settings.PAIR).strip()
        if not pair:
            raise ValueError("runtime_strategy_pair_missing")
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
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", weight)
        object.__setattr__(self, "desired_exposure_krw", desired_exposure)
        object.__setattr__(self, "risk_budget_krw", risk_budget)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "strategy_name": self.strategy_name,
            "enabled": bool(self.enabled),
            "pair": self.pair,
            "priority": int(self.priority),
            "weight": float(self.weight),
            "desired_exposure_krw": self.desired_exposure_krw,
            "risk_budget_krw": self.risk_budget_krw,
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
            priority=int(payload.get("priority", default.priority)),
            weight=float(payload.get("weight", default.weight)),
            desired_exposure_krw=payload.get("desired_exposure_krw", default.desired_exposure_krw),
            risk_budget_krw=payload.get("risk_budget_krw", default.risk_budget_krw),
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
    def collect(
        self,
        conn,
        strategy_set: RuntimeStrategySet,
        *,
        short_n: int,
        long_n: int,
        through_ts_ms: int | None,
    ) -> RuntimeStrategyDecisionResultBundle | None:
        results: list[RuntimeStrategyDecisionResult] = []
        for spec in strategy_set.active_strategies:
            adapter = get_runtime_decision_adapter(spec.strategy_name)
            validate_live_strategy_selection(replace(settings, STRATEGY_NAME=spec.strategy_name))
            if adapter is None:
                raise production_runtime_strategy_missing_error(spec.strategy_name)
            result = adapter.decide(
                conn,
                short_n=short_n,
                long_n=long_n,
                through_ts_ms=through_ts_ms,
            )
            if result is None:
                return None
            if not is_runtime_strategy_decision_result(result):
                raise TypeError(f"typed_runtime_decision_required:{spec.strategy_name}")
            if int(result.candle_ts) != int(through_ts_ms or result.candle_ts):
                raise ValueError(f"runtime_strategy_candle_mismatch:{spec.strategy_name}")
            results.append(result)
        return RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=tuple(results))


def active_runtime_strategy_set() -> RuntimeStrategySet:
    return RuntimeStrategySetResolver().resolve()


def collect_runtime_strategy_decisions(
    conn,
    *,
    short_n: int,
    long_n: int,
    through_ts_ms: int | None,
    strategy_set: RuntimeStrategySet | None = None,
) -> RuntimeStrategyDecisionResultBundle | None:
    resolved = strategy_set or active_runtime_strategy_set()
    return RuntimeStrategyDecisionCollector().collect(
        conn,
        resolved,
        short_n=short_n,
        long_n=long_n,
        through_ts_ms=through_ts_ms,
    )
