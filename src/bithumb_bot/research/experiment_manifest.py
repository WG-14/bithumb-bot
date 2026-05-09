from __future__ import annotations

import json
import itertools
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.market_regime import RegimeAcceptanceGate

from .deployment_policy import DEPLOYMENT_TIERS, normalize_deployment_tier
from .hashing import sha256_prefixed


class ManifestValidationError(ValueError):
    pass


@dataclass(frozen=True)
class DateRange:
    start: str
    end: str

    def start_ts_ms(self) -> int:
        return _date_start_ts_ms(self.start)

    def end_ts_ms(self) -> int:
        return _date_end_ts_ms(self.end)

    def as_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True)
class DatasetSplit:
    train: DateRange
    validation: DateRange
    final_holdout: DateRange | None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "train": self.train.as_dict(),
            "validation": self.validation.as_dict(),
        }
        if self.final_holdout is not None:
            payload["final_holdout"] = self.final_holdout.as_dict()
        return payload


@dataclass(frozen=True)
class TopOfBookDatasetSpec:
    source: str = "sqlite_orderbook_top_snapshots"
    required: bool = False
    join_tolerance_ms: int = 3000
    missing_policy: str = "warn"
    quote_source: str | None = None
    min_coverage_pct: float = 100.0

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source": self.source,
            "required": self.required,
            "join_tolerance_ms": self.join_tolerance_ms,
            "missing_policy": self.missing_policy,
            "min_coverage_pct": self.min_coverage_pct,
        }
        if self.quote_source is not None:
            payload["quote_source"] = self.quote_source
        return payload


@dataclass(frozen=True)
class DatasetSpec:
    source: str
    snapshot_id: str
    split: DatasetSplit
    top_of_book: TopOfBookDatasetSpec | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            **self.split.as_dict(),
        }
        if self.top_of_book is not None:
            payload["top_of_book"] = self.top_of_book.as_dict()
        return payload


@dataclass(frozen=True)
class CostModel:
    fee_rate: float
    slippage_bps: tuple[float, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "fee_rate": self.fee_rate,
            "slippage_bps": list(self.slippage_bps),
        }


@dataclass(frozen=True)
class ExecutionScenario:
    type: str
    fee_rate: float
    slippage_bps: float
    latency_ms: int = 0
    partial_fill_rate: float = 0.0
    order_failure_rate: float = 0.0
    market_order_extra_cost_bps: float = 0.0
    seed: int | None = None
    source: str = "execution_model"
    scenario_policy: str = "single_scenario"
    scenario_role: str = "base"
    scenario_role_source: str = "derived"

    def as_dict(self) -> dict[str, object]:
        return {
            "type": self.type,
            "fee_rate": self.fee_rate,
            "slippage_bps": self.slippage_bps,
            "latency_ms": self.latency_ms,
            "partial_fill_rate": self.partial_fill_rate,
            "order_failure_rate": self.order_failure_rate,
            "market_order_extra_cost_bps": self.market_order_extra_cost_bps,
            "seed": self.seed,
            "source": self.source,
            "scenario_policy": self.scenario_policy,
            "scenario_role": self.scenario_role,
            "scenario_role_source": self.scenario_role_source,
        }


@dataclass(frozen=True)
class ExecutionModelConfig:
    scenarios: tuple[ExecutionScenario, ...]
    source: str
    scenario_policy: str
    calibration_required: bool = False
    calibration_strictness: str = "fail"

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "scenario_policy": self.scenario_policy,
            "calibration_required": self.calibration_required,
            "calibration_strictness": self.calibration_strictness,
            "scenarios": [scenario.as_dict() for scenario in self.scenarios],
        }


@dataclass(frozen=True)
class ExecutionTimingPolicy:
    signal_basis: str = "closed_candle"
    decision_time: str = "candle_close"
    decision_guard_ms: int = 0
    fill_reference_policy: str = "candle_close_legacy"
    quote_selection: str = "first_after_or_equal"
    max_quote_wait_ms: int = 3000
    missing_quote_policy: str = "warn"
    allow_same_candle_close_fill: bool = True
    min_execution_reality_level_for_promotion: str | None = None
    source: str = "legacy_default"

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "signal_basis": self.signal_basis,
            "decision_time": self.decision_time,
            "decision_guard_ms": self.decision_guard_ms,
            "fill_reference_policy": self.fill_reference_policy,
            "quote_selection": self.quote_selection,
            "max_quote_wait_ms": self.max_quote_wait_ms,
            "missing_quote_policy": self.missing_quote_policy,
            "allow_same_candle_close_fill": self.allow_same_candle_close_fill,
            "source": self.source,
        }
        if self.min_execution_reality_level_for_promotion is not None:
            payload["min_execution_reality_level_for_promotion"] = self.min_execution_reality_level_for_promotion
        return payload


@dataclass(frozen=True)
class AcceptanceGate:
    min_trade_count: int
    max_mdd_pct: float
    min_profit_factor: float
    oos_return_must_be_positive: bool
    parameter_stability_required: bool
    walk_forward_required: bool = False
    final_holdout_required_for_promotion: bool = True
    min_cagr_pct: float | None = None
    min_expectancy_per_trade_krw: float | None = None
    min_expectancy_per_trade_pct: float | None = None
    max_exposure_time_pct: float | None = None
    max_avg_holding_time_minutes: float | None = None
    max_fee_drag_ratio: float | None = None
    max_slippage_drag_ratio: float | None = None
    reject_open_position_at_end: bool = False
    metrics_contract_required: bool = False
    regime_acceptance_gate: RegimeAcceptanceGate = field(default_factory=RegimeAcceptanceGate)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "min_trade_count": self.min_trade_count,
            "max_mdd_pct": self.max_mdd_pct,
            "min_profit_factor": self.min_profit_factor,
            "oos_return_must_be_positive": self.oos_return_must_be_positive,
            "parameter_stability_required": self.parameter_stability_required,
            "walk_forward_required": self.walk_forward_required,
            "final_holdout_required_for_promotion": self.final_holdout_required_for_promotion,
            "regime_acceptance_gate": self.regime_acceptance_gate.as_dict(),
        }
        optional_fields = {
            "min_cagr_pct": self.min_cagr_pct,
            "min_expectancy_per_trade_krw": self.min_expectancy_per_trade_krw,
            "min_expectancy_per_trade_pct": self.min_expectancy_per_trade_pct,
            "max_exposure_time_pct": self.max_exposure_time_pct,
            "max_avg_holding_time_minutes": self.max_avg_holding_time_minutes,
            "max_fee_drag_ratio": self.max_fee_drag_ratio,
            "max_slippage_drag_ratio": self.max_slippage_drag_ratio,
        }
        payload.update(optional_fields)
        payload["reject_open_position_at_end"] = self.reject_open_position_at_end
        payload["metrics_contract_required"] = self.metrics_contract_required
        return payload


@dataclass(frozen=True)
class WalkForwardConfig:
    train_window_days: int
    test_window_days: int
    step_days: int
    min_windows: int

    def as_dict(self) -> dict[str, int]:
        return {
            "train_window_days": self.train_window_days,
            "test_window_days": self.test_window_days,
            "step_days": self.step_days,
            "min_windows": self.min_windows,
        }


@dataclass(frozen=True)
class ExperimentManifest:
    experiment_id: str
    hypothesis: str
    strategy_name: str
    market: str
    interval: str
    dataset: DatasetSpec
    parameter_space: dict[str, tuple[object, ...]]
    cost_model: CostModel
    execution_model: ExecutionModelConfig
    execution_timing: ExecutionTimingPolicy
    deployment_tier: str
    acceptance_gate: AcceptanceGate
    walk_forward: WalkForwardConfig | None
    raw: dict[str, Any]

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "hypothesis": self.hypothesis,
            "strategy_name": self.strategy_name,
            "market": self.market,
            "interval": self.interval,
            "dataset": self.dataset.as_dict(),
            "parameter_space": {key: sorted(list(value), key=repr) for key, value in sorted(self.parameter_space.items())},
            "cost_model": self.cost_model.as_dict(),
            "execution_model": self.execution_model.as_dict(),
            "execution_timing": self.execution_timing.as_dict(),
            "deployment_tier": self.deployment_tier,
            "acceptance_gate": self.acceptance_gate.as_dict(),
            "walk_forward": self.walk_forward.as_dict() if self.walk_forward is not None else None,
        }

    def manifest_hash(self) -> str:
        return sha256_prefixed(self.canonical_payload())


def load_manifest(path: str | Path) -> ExperimentManifest:
    manifest_path = Path(path).expanduser()
    with manifest_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return parse_manifest(payload)


def parse_manifest(payload: dict[str, Any]) -> ExperimentManifest:
    if not isinstance(payload, dict):
        raise ManifestValidationError("manifest must be a JSON object")

    experiment_id = _required_str(payload, "experiment_id")
    hypothesis = _required_str(payload, "hypothesis")
    strategy_name = _required_str(payload, "strategy_name")
    market = _required_str(payload, "market")
    interval = _required_str(payload, "interval")
    dataset_payload = _required_dict(payload, "dataset")
    dataset = _parse_dataset(dataset_payload)
    parameter_space = _parse_parameter_space(payload.get("parameter_space"))
    if payload.get("cost_model") is None and payload.get("execution_model") is None:
        raise ManifestValidationError("manifest requires cost_model or execution_model")
    cost_model = _parse_cost_model(payload.get("cost_model"))
    execution_model = _parse_execution_model(payload.get("execution_model"), cost_model)
    execution_timing = _parse_execution_timing(payload.get("execution_timing"))
    deployment_tier = _parse_deployment_tier(payload.get("deployment_tier") or payload.get("promotion_target"))
    acceptance_gate = _parse_acceptance_gate(_required_dict(payload, "acceptance_gate"))
    walk_forward = _parse_walk_forward(payload.get("walk_forward"))
    if acceptance_gate.walk_forward_required and walk_forward is None:
        raise ManifestValidationError("walk_forward is required when acceptance_gate.walk_forward_required=true")

    _validate_split_order(dataset.split)

    return ExperimentManifest(
        experiment_id=experiment_id,
        hypothesis=hypothesis,
        strategy_name=strategy_name,
        market=market,
        interval=interval,
        dataset=dataset,
        parameter_space=parameter_space,
        cost_model=cost_model,
        execution_model=execution_model,
        execution_timing=execution_timing,
        deployment_tier=deployment_tier,
        acceptance_gate=acceptance_gate,
        walk_forward=walk_forward,
        raw=dict(payload),
    )


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"manifest field {key!r} is required")
    return value.strip()


def _required_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ManifestValidationError(f"manifest field {key!r} must be an object")
    return value


def _parse_deployment_tier(value: Any) -> str:
    tier = normalize_deployment_tier(value)
    if value is not None and tier == "research_only" and str(value).strip().lower() not in DEPLOYMENT_TIERS:
        raise ManifestValidationError(
            "deployment_tier must be one of research_only, paper_candidate, live_dry_run_candidate, small_live_candidate"
        )
    return tier


def _parse_date_range(payload: dict[str, Any], key: str) -> DateRange:
    section = payload.get(key)
    if not isinstance(section, dict):
        raise ManifestValidationError(f"dataset.{key} must be an object")
    date_range = DateRange(start=_required_str(section, "start"), end=_required_str(section, "end"))
    if date_range.start_ts_ms() > date_range.end_ts_ms():
        raise ManifestValidationError(f"dataset.{key}.start must be earlier than or equal to end")
    return date_range


def _parse_dataset(payload: dict[str, Any]) -> DatasetSpec:
    allowed_fields = {"source", "snapshot_id", "train", "validation", "final_holdout", "top_of_book"}
    unknown = sorted(set(payload) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"dataset unsupported fields: {','.join(unknown)}")
    source = _required_str(payload, "source")
    if source != "sqlite_candles":
        raise ManifestValidationError("dataset.source currently supports only 'sqlite_candles'")
    split = DatasetSplit(
        train=_parse_date_range(payload, "train"),
        validation=_parse_date_range(payload, "validation"),
        final_holdout=(
            _parse_date_range(payload, "final_holdout")
            if isinstance(payload.get("final_holdout"), dict)
            else None
        ),
    )
    return DatasetSpec(
        source=source,
        snapshot_id=_required_str(payload, "snapshot_id"),
        split=split,
        top_of_book=_parse_top_of_book_dataset(payload.get("top_of_book")),
    )


def _parse_top_of_book_dataset(value: Any) -> TopOfBookDatasetSpec | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("dataset.top_of_book must be an object")
    allowed_fields = {
        "source",
        "required",
        "join_tolerance_ms",
        "missing_policy",
        "quote_source",
        "min_coverage_pct",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"dataset.top_of_book unsupported fields: {','.join(unknown)}")
    source = str(value.get("source") or "sqlite_orderbook_top_snapshots").strip()
    if source != "sqlite_orderbook_top_snapshots":
        raise ManifestValidationError("dataset.top_of_book.source must be sqlite_orderbook_top_snapshots")
    join_tolerance_ms = _positive_int(value.get("join_tolerance_ms", 3000), "dataset.top_of_book.join_tolerance_ms")
    missing_policy = str(value.get("missing_policy") or "warn").strip().lower()
    if missing_policy not in {"warn", "fail"}:
        raise ManifestValidationError("dataset.top_of_book.missing_policy must be warn or fail")
    required = bool(value.get("required", False))
    if required and missing_policy != "fail":
        missing_policy = "fail"
    quote_source = value.get("quote_source")
    parsed_quote_source = None
    if quote_source is not None:
        parsed_quote_source = str(quote_source).strip()
        if not parsed_quote_source:
            raise ManifestValidationError("dataset.top_of_book.quote_source must be non-empty when supplied")
    min_coverage_pct = _finite_non_negative_float(
        value.get("min_coverage_pct", 100.0),
        "dataset.top_of_book.min_coverage_pct",
    )
    if min_coverage_pct > 100.0:
        raise ManifestValidationError("dataset.top_of_book.min_coverage_pct must be <= 100")
    return TopOfBookDatasetSpec(
        source=source,
        required=required,
        join_tolerance_ms=join_tolerance_ms,
        missing_policy=missing_policy,
        quote_source=parsed_quote_source,
        min_coverage_pct=min_coverage_pct,
    )


def _parse_parameter_space(value: Any) -> dict[str, tuple[object, ...]]:
    if not isinstance(value, dict) or not value:
        raise ManifestValidationError("parameter_space must be a non-empty object")
    out: dict[str, tuple[object, ...]] = {}
    for key, raw_values in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ManifestValidationError("parameter_space keys must be non-empty strings")
        if not isinstance(raw_values, list) or len(raw_values) == 0:
            raise ManifestValidationError(f"parameter_space.{key} must be a non-empty array")
        out[key.strip()] = tuple(raw_values)
    return out


def _parse_cost_model(payload: Any) -> CostModel:
    if payload is None:
        return CostModel(fee_rate=0.0, slippage_bps=(0.0,))
    if not isinstance(payload, dict):
        raise ManifestValidationError("manifest field 'cost_model' must be an object")
    fee_rate = _finite_non_negative_float(payload.get("fee_rate"), "cost_model.fee_rate")
    slippage = payload.get("slippage_bps")
    if not isinstance(slippage, list) or not slippage:
        raise ManifestValidationError("cost_model.slippage_bps must be a non-empty array")
    return CostModel(
        fee_rate=fee_rate,
        slippage_bps=tuple(_finite_non_negative_float(value, "cost_model.slippage_bps") for value in slippage),
    )


def _parse_execution_model(value: Any, cost_model: CostModel) -> ExecutionModelConfig:
    if value is None:
        scenarios = tuple(
            ExecutionScenario(
                type="fixed_bps",
                fee_rate=cost_model.fee_rate,
                slippage_bps=float(slippage),
                source="legacy_cost_model",
                scenario_policy="legacy_cost_model_single_pass",
            )
            for slippage in cost_model.slippage_bps
        )
        return ExecutionModelConfig(
            scenarios=scenarios,
            source="legacy_cost_model",
            scenario_policy="legacy_cost_model_single_pass",
        )
    if not isinstance(value, dict):
        raise ManifestValidationError("execution_model must be an object")
    allowed_fields = {
        "type",
        "fee_rate",
        "slippage_bps",
        "latency_ms",
        "partial_fill_rate",
        "order_failure_rate",
        "market_order_extra_cost_bps",
        "scenario_policy",
        "scenario_role",
        "seed",
        "calibration_required",
        "calibration_strictness",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"execution_model unsupported fields: {','.join(unknown)}")
    model_type = _required_str(value, "type")
    if model_type not in {"fixed_bps", "stress"}:
        raise ManifestValidationError("execution_model.type must be fixed_bps or stress")
    explicit_scenario_policy = value.get("scenario_policy")
    scenario_policy = str(explicit_scenario_policy or "").strip()
    if scenario_policy and scenario_policy not in {
        "single_scenario",
        "must_pass_base_and_survive_stress",
    }:
        raise ManifestValidationError(
            "execution_model.scenario_policy must be single_scenario or must_pass_base_and_survive_stress"
        )
    scenario_role = _optional_scenario_role(value.get("scenario_role"))
    scenario_role_source = "manifest" if scenario_role is not None else "derived"
    strictness = str(value.get("calibration_strictness") or "fail").strip().lower()
    if strictness not in {"fail", "warn"}:
        raise ManifestValidationError("execution_model.calibration_strictness must be fail or warn")
    fees = _number_array(value, "fee_rate", default=(cost_model.fee_rate,))
    slippages = _number_array(value, "slippage_bps", default=cost_model.slippage_bps)
    latencies = _int_array(value, "latency_ms", default=(0,))
    partial_rates = _number_array(value, "partial_fill_rate", default=(0.0,))
    failure_rates = _number_array(value, "order_failure_rate", default=(0.0,))
    market_extra = _number_array(value, "market_order_extra_cost_bps", default=(0.0,))
    seed = value.get("seed")
    parsed_seed = None if seed is None else int(seed)
    scenarios = []
    for index, (fee, slippage, latency, partial, failure, extra) in enumerate(itertools.product(
        fees, slippages, latencies, partial_rates, failure_rates, market_extra
    )):
        active_role = scenario_role or _derived_scenario_role(index)
        scenarios.append(
            ExecutionScenario(
                type=model_type,
                fee_rate=float(fee),
                slippage_bps=float(slippage),
                latency_ms=int(latency),
                partial_fill_rate=float(partial),
                order_failure_rate=float(failure),
                market_order_extra_cost_bps=float(extra),
                seed=parsed_seed,
                source="execution_model",
                scenario_policy=scenario_policy or "pending_default",
                scenario_role=active_role,
                scenario_role_source=scenario_role_source,
            )
        )
    if not scenarios:
        raise ManifestValidationError("execution_model produced no scenarios")
    if not scenario_policy:
        scenario_policy = "single_scenario" if len(scenarios) == 1 else "must_pass_base_and_survive_stress"
        scenarios = [
            ExecutionScenario(
                type=scenario.type,
                fee_rate=scenario.fee_rate,
                slippage_bps=scenario.slippage_bps,
                latency_ms=scenario.latency_ms,
                partial_fill_rate=scenario.partial_fill_rate,
                order_failure_rate=scenario.order_failure_rate,
                market_order_extra_cost_bps=scenario.market_order_extra_cost_bps,
                seed=scenario.seed,
                source=scenario.source,
                scenario_policy=scenario_policy,
                scenario_role=scenario.scenario_role,
                scenario_role_source=scenario.scenario_role_source,
            )
            for scenario in scenarios
        ]
    _validate_scenario_policy_role_consistency(
        explicit_scenario_policy=explicit_scenario_policy,
        scenario_policy=scenario_policy,
        scenarios=tuple(scenarios),
    )
    return ExecutionModelConfig(
        scenarios=tuple(scenarios),
        source="execution_model",
        scenario_policy=scenario_policy,
        calibration_required=bool(value.get("calibration_required", False)),
        calibration_strictness=strictness,
    )


def _parse_execution_timing(value: Any) -> ExecutionTimingPolicy:
    if value is None:
        return ExecutionTimingPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("execution_timing must be an object")
    allowed_fields = {
        "signal_basis",
        "decision_time",
        "decision_guard_ms",
        "fill_reference_policy",
        "quote_selection",
        "max_quote_wait_ms",
        "missing_quote_policy",
        "allow_same_candle_close_fill",
        "min_execution_reality_level_for_promotion",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"execution_timing unsupported fields: {','.join(unknown)}")
    signal_basis = str(value.get("signal_basis") or "closed_candle").strip().lower()
    if signal_basis != "closed_candle":
        raise ManifestValidationError("execution_timing.signal_basis must be closed_candle")
    decision_time = str(value.get("decision_time") or "candle_close").strip().lower()
    if decision_time not in {"candle_close", "candle_close_plus_guard"}:
        raise ManifestValidationError("execution_timing.decision_time must be candle_close or candle_close_plus_guard")
    guard_ms = _positive_or_zero_int(value.get("decision_guard_ms", 0), "execution_timing.decision_guard_ms")
    if decision_time == "candle_close" and guard_ms:
        decision_time = "candle_close_plus_guard"
    fill_policy = str(value.get("fill_reference_policy") or "next_candle_open").strip().lower()
    if fill_policy not in {
        "candle_close_legacy",
        "next_candle_open",
        "first_orderbook_after_decision",
        "latency_adjusted_orderbook",
    }:
        raise ManifestValidationError("execution_timing.fill_reference_policy is unsupported")
    quote_selection = str(value.get("quote_selection") or "first_after_or_equal").strip().lower()
    if quote_selection != "first_after_or_equal":
        raise ManifestValidationError("execution_timing.quote_selection must be first_after_or_equal")
    max_wait = _positive_or_zero_int(value.get("max_quote_wait_ms", 3000), "execution_timing.max_quote_wait_ms")
    missing_quote_policy = str(value.get("missing_quote_policy") or "warn").strip().lower()
    if missing_quote_policy not in {"fail", "skip", "warn"}:
        raise ManifestValidationError("execution_timing.missing_quote_policy must be fail, skip, or warn")
    explicit_allow = value.get("allow_same_candle_close_fill")
    allow_same = bool(explicit_allow) if explicit_allow is not None else fill_policy == "candle_close_legacy"
    min_level_raw = value.get("min_execution_reality_level_for_promotion")
    min_level = None if min_level_raw is None else str(min_level_raw).strip()
    if min_level is not None and min_level not in {
        "candle_close_optimistic",
        "candle_next_open",
        "top_of_book_after_decision",
        "latency_adjusted_top_of_book",
    }:
        raise ManifestValidationError("execution_timing.min_execution_reality_level_for_promotion is unsupported")
    return ExecutionTimingPolicy(
        signal_basis=signal_basis,
        decision_time=decision_time,
        decision_guard_ms=guard_ms,
        fill_reference_policy=fill_policy,
        quote_selection=quote_selection,
        max_quote_wait_ms=max_wait,
        missing_quote_policy=missing_quote_policy,
        allow_same_candle_close_fill=allow_same,
        min_execution_reality_level_for_promotion=min_level,
        source="manifest",
    )


def _optional_scenario_role(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        raise ManifestValidationError("execution_model.scenario_role must be a scalar base or stress value")
    role = str(value).strip()
    if role not in {"base", "stress"}:
        raise ManifestValidationError("execution_model.scenario_role must be base or stress")
    return role


def _derived_scenario_role(index: int) -> str:
    return "base" if index == 0 else "stress"


def _validate_scenario_policy_role_consistency(
    *,
    explicit_scenario_policy: Any,
    scenario_policy: str,
    scenarios: tuple[ExecutionScenario, ...],
) -> None:
    if str(explicit_scenario_policy or "").strip() != "must_pass_base_and_survive_stress":
        return
    if scenario_policy != "must_pass_base_and_survive_stress" or len(scenarios) <= 1:
        return
    if not all(scenario.scenario_role_source == "manifest" for scenario in scenarios):
        return
    roles = {scenario.scenario_role for scenario in scenarios}
    if roles in ({"base"}, {"stress"}):
        raise ManifestValidationError(
            "execution_model.scenario_role conflicts with must_pass_base_and_survive_stress"
        )


def _number_array(payload: dict[str, Any], key: str, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if key not in payload:
        return tuple(float(item) for item in default)
    value = payload.get(key)
    raw_values = value if isinstance(value, list) else [value]
    if not raw_values:
        raise ManifestValidationError(f"execution_model.{key} must not be empty")
    return tuple(_finite_non_negative_float(item, f"execution_model.{key}") for item in raw_values)


def _int_array(payload: dict[str, Any], key: str, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if key not in payload:
        return tuple(int(item) for item in default)
    value = payload.get(key)
    raw_values = value if isinstance(value, list) else [value]
    if not raw_values:
        raise ManifestValidationError(f"execution_model.{key} must not be empty")
    return tuple(_positive_or_zero_int(item, f"execution_model.{key}") for item in raw_values)


def _parse_acceptance_gate(payload: dict[str, Any]) -> AcceptanceGate:
    allowed_fields = {
        "min_trade_count",
        "max_mdd_pct",
        "min_profit_factor",
        "oos_return_must_be_positive",
        "parameter_stability_required",
        "walk_forward_required",
        "final_holdout_required_for_promotion",
        "regime_acceptance_gate",
        "min_cagr_pct",
        "min_expectancy_per_trade_krw",
        "min_expectancy_per_trade_pct",
        "max_exposure_time_pct",
        "max_avg_holding_time_minutes",
        "max_fee_drag_ratio",
        "max_slippage_drag_ratio",
        "reject_open_position_at_end",
        "metrics_contract_required",
    }
    unknown = sorted(set(payload) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"acceptance_gate unsupported fields: {','.join(unknown)}")
    min_trade_count = _positive_int(payload.get("min_trade_count"), "acceptance_gate.min_trade_count")
    max_mdd_pct = _finite_non_negative_float(payload.get("max_mdd_pct"), "acceptance_gate.max_mdd_pct")
    min_profit_factor = _finite_non_negative_float(
        payload.get("min_profit_factor"), "acceptance_gate.min_profit_factor"
    )
    if min_profit_factor <= 0.0:
        raise ManifestValidationError("acceptance_gate.min_profit_factor must be > 0")
    return AcceptanceGate(
        min_trade_count=min_trade_count,
        max_mdd_pct=max_mdd_pct,
        min_profit_factor=min_profit_factor,
        oos_return_must_be_positive=bool(payload.get("oos_return_must_be_positive", True)),
        parameter_stability_required=bool(payload.get("parameter_stability_required", False)),
        walk_forward_required=bool(payload.get("walk_forward_required", False)),
        final_holdout_required_for_promotion=bool(payload.get("final_holdout_required_for_promotion", True)),
        min_cagr_pct=_optional_finite_float(payload.get("min_cagr_pct"), "acceptance_gate.min_cagr_pct"),
        min_expectancy_per_trade_krw=_optional_finite_float(
            payload.get("min_expectancy_per_trade_krw"),
            "acceptance_gate.min_expectancy_per_trade_krw",
        ),
        min_expectancy_per_trade_pct=_optional_finite_float(
            payload.get("min_expectancy_per_trade_pct"),
            "acceptance_gate.min_expectancy_per_trade_pct",
        ),
        max_exposure_time_pct=_optional_pct(payload.get("max_exposure_time_pct"), "acceptance_gate.max_exposure_time_pct"),
        max_avg_holding_time_minutes=_optional_finite_non_negative_float(
            payload.get("max_avg_holding_time_minutes"),
            "acceptance_gate.max_avg_holding_time_minutes",
        ),
        max_fee_drag_ratio=_optional_finite_non_negative_float(
            payload.get("max_fee_drag_ratio"),
            "acceptance_gate.max_fee_drag_ratio",
        ),
        max_slippage_drag_ratio=_optional_finite_non_negative_float(
            payload.get("max_slippage_drag_ratio"),
            "acceptance_gate.max_slippage_drag_ratio",
        ),
        reject_open_position_at_end=bool(payload.get("reject_open_position_at_end", False)),
        metrics_contract_required=bool(payload.get("metrics_contract_required", False)),
        regime_acceptance_gate=_parse_regime_acceptance_gate(payload.get("regime_acceptance_gate")),
    )


def _parse_regime_acceptance_gate(value: Any) -> RegimeAcceptanceGate:
    if value is None:
        return RegimeAcceptanceGate(required=False)
    if not isinstance(value, dict):
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate must be an object")
    min_trade_count = int(value.get("min_trade_count_per_required_regime", 0) or 0)
    blocked_count = int(value.get("blocked_regime_max_trade_count", 0) or 0)
    if min_trade_count < 0:
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate.min_trade_count_per_required_regime must be >= 0")
    if blocked_count < 0:
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate.blocked_regime_max_trade_count must be >= 0")
    return RegimeAcceptanceGate(
        required=bool(value.get("required", False)),
        min_trade_count_per_required_regime=min_trade_count,
        required_regimes=tuple(_str_list(value.get("required_regimes"), "required_regimes")),
        blocked_regimes=tuple(_str_list(value.get("blocked_regimes"), "blocked_regimes")),
        blocked_regime_max_trade_count=blocked_count,
        blocked_regime_max_net_pnl_loss_krw=_finite_non_negative_float(
            value.get("blocked_regime_max_net_pnl_loss_krw", 0.0),
            "acceptance_gate.regime_acceptance_gate.blocked_regime_max_net_pnl_loss_krw",
        ),
        min_profit_factor_by_regime=_float_map(value.get("min_profit_factor_by_regime"), "min_profit_factor_by_regime"),
        min_expectancy_by_regime=_float_map(value.get("min_expectancy_by_regime"), "min_expectancy_by_regime"),
        max_loss_share_by_single_regime=(
            None
            if value.get("max_loss_share_by_single_regime") is None
            else _finite_non_negative_float(
                value.get("max_loss_share_by_single_regime"),
                "acceptance_gate.regime_acceptance_gate.max_loss_share_by_single_regime",
            )
        ),
        max_pnl_dependency_by_single_regime=(
            None
            if value.get("max_pnl_dependency_by_single_regime") is None
            else _finite_non_negative_float(
                value.get("max_pnl_dependency_by_single_regime"),
                "acceptance_gate.regime_acceptance_gate.max_pnl_dependency_by_single_regime",
            )
        ),
    )


def _str_list(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ManifestValidationError(f"acceptance_gate.regime_acceptance_gate.{field} must be an array")
    return [str(item).strip() for item in value if str(item).strip()]


def _float_map(value: Any, field: str) -> dict[str, float]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ManifestValidationError(f"acceptance_gate.regime_acceptance_gate.{field} must be an object")
    out: dict[str, float] = {}
    for key, raw in value.items():
        out[str(key)] = _finite_non_negative_float(raw, f"acceptance_gate.regime_acceptance_gate.{field}.{key}")
    return out


def _parse_walk_forward(value: Any) -> WalkForwardConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("walk_forward must be an object")
    return WalkForwardConfig(
        train_window_days=_positive_int(value.get("train_window_days"), "walk_forward.train_window_days"),
        test_window_days=_positive_int(value.get("test_window_days"), "walk_forward.test_window_days"),
        step_days=_positive_int(value.get("step_days"), "walk_forward.step_days"),
        min_windows=_positive_int(value.get("min_windows"), "walk_forward.min_windows"),
    )


def _validate_split_order(split: DatasetSplit) -> None:
    if split.train.end_ts_ms() >= split.validation.start_ts_ms():
        raise ManifestValidationError("dataset.train must end before dataset.validation starts")
    if split.final_holdout is not None and split.validation.end_ts_ms() >= split.final_holdout.start_ts_ms():
        raise ManifestValidationError("dataset.validation must end before dataset.final_holdout starts")


def _date_start_ts_ms(value: str) -> int:
    return int(_parse_date(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _date_end_ts_ms(value: str) -> int:
    return _date_start_ts_ms(value) + 86_400_000 - 1


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ManifestValidationError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def _finite_non_negative_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be a number") from exc
    if parsed < 0.0 or parsed != parsed or parsed in {float("inf"), float("-inf")}:
        raise ManifestValidationError(f"{field} must be a finite value >= 0")
    return parsed


def _optional_finite_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be a number") from exc
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        raise ManifestValidationError(f"{field} must be finite")
    return parsed


def _optional_finite_non_negative_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    parsed = _optional_finite_float(value, field)
    assert parsed is not None
    if parsed < 0.0:
        raise ManifestValidationError(f"{field} must be >= 0")
    return parsed


def _optional_pct(value: Any, field: str) -> float | None:
    parsed = _optional_finite_non_negative_float(value, field)
    if parsed is not None and parsed > 100.0:
        raise ManifestValidationError(f"{field} must be <= 100")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be an integer") from exc
    if parsed <= 0:
        raise ManifestValidationError(f"{field} must be > 0")
    return parsed


def _positive_or_zero_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be an integer") from exc
    if parsed < 0:
        raise ManifestValidationError(f"{field} must be >= 0")
    return parsed
