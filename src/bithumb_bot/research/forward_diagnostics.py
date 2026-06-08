from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bithumb_bot.research.dataset_snapshot import DatasetSnapshot, load_dataset_split
from bithumb_bot.research.experiment_manifest import ExperimentManifest
from bithumb_bot.research.feature_bucket_metrics import (
    FeatureBucketMetric,
    FeatureObservation,
    compute_feature_bucket_metrics,
)
from bithumb_bot.research.feature_diagnostic_features import (
    AsOfCandleView,
    FeatureValue,
    feature_providers_for_names,
)
from bithumb_bot.research.forward_targets import (
    ForwardTarget,
    compute_forward_targets,
)


@dataclass(frozen=True)
class ForwardDiagnosticsResult:
    experiment_id: str
    split_name: str
    feature_names: tuple[str, ...]
    horizon_steps: tuple[int, ...]
    bucket_method: str
    entry_price_mode: str
    sample_count: int
    target_count: int
    feature_bucket_metrics: tuple[FeatureBucketMetric, ...]
    feature_horizon_metrics: tuple[FeatureBucketMetric, ...]
    warnings: tuple[dict[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "experiment_id": self.experiment_id,
            "split_name": self.split_name,
            "feature_names": list(self.feature_names),
            "horizon_steps": list(self.horizon_steps),
            "bucket_method": self.bucket_method,
            "entry_price_mode": self.entry_price_mode,
            "sample_count": self.sample_count,
            "target_count": self.target_count,
            "feature_bucket_metrics": [metric.as_dict() for metric in self.feature_bucket_metrics],
            "feature_horizon_metrics": [metric.as_dict() for metric in self.feature_horizon_metrics],
            "warnings": list(self.warnings),
        }


def run_forward_diagnostics(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    split_name: str,
    feature_names: tuple[str, ...],
    horizon_steps: tuple[int, ...],
    bucket_method: str,
    entry_price_mode: str = "next_open",
    min_bucket_count: int = 30,
) -> dict[str, Any]:
    snapshot = load_dataset_split(
        db_path=db_path,
        manifest=manifest,
        split_name=split_name,
    )
    result = run_forward_diagnostics_on_snapshot(
        snapshot=snapshot,
        experiment_id=manifest.experiment_id,
        feature_names=feature_names,
        horizon_steps=horizon_steps,
        bucket_method=bucket_method,
        entry_price_mode=entry_price_mode,
        min_bucket_count=min_bucket_count,
    )
    return result.as_dict()


def run_forward_diagnostics_on_snapshot(
    *,
    snapshot: DatasetSnapshot,
    feature_names: tuple[str, ...],
    horizon_steps: tuple[int, ...],
    bucket_method: str,
    entry_price_mode: str = "next_open",
    min_bucket_count: int = 30,
    experiment_id: str | None = None,
) -> ForwardDiagnosticsResult:
    features = _normalize_feature_names(feature_names)
    horizons = _normalize_horizons(horizon_steps)
    providers = feature_providers_for_names(features)
    observations: list[FeatureObservation] = []
    target_count = 0
    for index in range(len(snapshot.candles)):
        targets = compute_forward_targets(
            candles=snapshot.candles,
            index=index,
            horizon_steps=horizons,
            entry_price_mode=entry_price_mode,
        )
        if not targets:
            continue
        target_count += len(targets)
        view = AsOfCandleView(candles=snapshot.candles, index=index)
        values = tuple(
            value
            for value in (provider.compute(view=view) for provider in providers)
            if value is not None
        )
        observations.extend(_observations_for_values(values=values, targets=targets))

    bucket_metrics = compute_feature_bucket_metrics(
        observations=observations,
        bucket_method=bucket_method,
        min_bucket_count=min_bucket_count,
    )
    horizon_metrics = compute_feature_bucket_metrics(
        observations=observations,
        bucket_method="quantile:1",
        min_bucket_count=min_bucket_count,
    )
    warnings = tuple(
        {
            "feature_name": metric.feature_name,
            "bucket_id": metric.bucket_id,
            "horizon_label": metric.horizon_label,
            "warnings": list(metric.warnings),
        }
        for metric in bucket_metrics
        if metric.warnings
    )
    return ForwardDiagnosticsResult(
        experiment_id=str(experiment_id or snapshot.snapshot_id),
        split_name=snapshot.split_name,
        feature_names=features,
        horizon_steps=horizons,
        bucket_method=bucket_method,
        entry_price_mode=str(entry_price_mode),
        sample_count=len(observations),
        target_count=target_count,
        feature_bucket_metrics=bucket_metrics,
        feature_horizon_metrics=horizon_metrics,
        warnings=warnings,
    )


def _observations_for_values(
    *,
    values: tuple[FeatureValue, ...],
    targets: tuple[ForwardTarget, ...],
) -> tuple[FeatureObservation, ...]:
    return tuple(FeatureObservation(feature=value, target=target) for value in values for target in targets)


def _normalize_feature_names(feature_names: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(part.strip() for part in feature_names if part.strip())
    if not normalized:
        raise ValueError("features must not be empty")
    return normalized


def _normalize_horizons(horizon_steps: tuple[int, ...]) -> tuple[int, ...]:
    normalized = tuple(int(step) for step in horizon_steps)
    if not normalized:
        raise ValueError("horizons must not be empty")
    if any(step <= 0 for step in normalized):
        raise ValueError("horizons must be positive")
    return normalized
