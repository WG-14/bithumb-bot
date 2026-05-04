from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.market_regime import RegimeAcceptanceGate

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
class DatasetSpec:
    source: str
    snapshot_id: str
    split: DatasetSplit

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            **self.split.as_dict(),
        }


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
class AcceptanceGate:
    min_trade_count: int
    max_mdd_pct: float
    min_profit_factor: float
    oos_return_must_be_positive: bool
    parameter_stability_required: bool
    walk_forward_required: bool = False
    regime_acceptance_gate: RegimeAcceptanceGate = field(default_factory=RegimeAcceptanceGate)

    def as_dict(self) -> dict[str, object]:
        return {
            "min_trade_count": self.min_trade_count,
            "max_mdd_pct": self.max_mdd_pct,
            "min_profit_factor": self.min_profit_factor,
            "oos_return_must_be_positive": self.oos_return_must_be_positive,
            "parameter_stability_required": self.parameter_stability_required,
            "walk_forward_required": self.walk_forward_required,
            "regime_acceptance_gate": self.regime_acceptance_gate.as_dict(),
        }


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
            "parameter_space": {key: list(value) for key, value in sorted(self.parameter_space.items())},
            "cost_model": self.cost_model.as_dict(),
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
    cost_model = _parse_cost_model(_required_dict(payload, "cost_model"))
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


def _parse_date_range(payload: dict[str, Any], key: str) -> DateRange:
    section = payload.get(key)
    if not isinstance(section, dict):
        raise ManifestValidationError(f"dataset.{key} must be an object")
    date_range = DateRange(start=_required_str(section, "start"), end=_required_str(section, "end"))
    if date_range.start_ts_ms() > date_range.end_ts_ms():
        raise ManifestValidationError(f"dataset.{key}.start must be earlier than or equal to end")
    return date_range


def _parse_dataset(payload: dict[str, Any]) -> DatasetSpec:
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


def _parse_cost_model(payload: dict[str, Any]) -> CostModel:
    fee_rate = _finite_non_negative_float(payload.get("fee_rate"), "cost_model.fee_rate")
    slippage = payload.get("slippage_bps")
    if not isinstance(slippage, list) or not slippage:
        raise ManifestValidationError("cost_model.slippage_bps must be a non-empty array")
    return CostModel(
        fee_rate=fee_rate,
        slippage_bps=tuple(_finite_non_negative_float(value, "cost_model.slippage_bps") for value in slippage),
    )


def _parse_acceptance_gate(payload: dict[str, Any]) -> AcceptanceGate:
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


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be an integer") from exc
    if parsed <= 0:
        raise ManifestValidationError(f"{field} must be > 0")
    return parsed
