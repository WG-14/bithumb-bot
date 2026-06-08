from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Iterable

from bithumb_bot.research.feature_diagnostic_features import FeatureValue
from bithumb_bot.research.forward_targets import ForwardTarget


@dataclass(frozen=True)
class FeatureBucketMetric:
    feature_name: str
    bucket_id: str
    bucket_label: str
    horizon_label: str
    count: int
    mean_forward_return: float | None
    median_forward_return: float | None
    win_rate: float | None
    p10_forward_return: float | None
    p90_forward_return: float | None
    mean_mfe: float | None
    median_mfe: float | None
    mean_mae: float | None
    median_mae: float | None
    mfe_mae_ratio: float | None
    warnings: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "feature_name": self.feature_name,
            "bucket_id": self.bucket_id,
            "bucket_label": self.bucket_label,
            "horizon_label": self.horizon_label,
            "count": self.count,
            "mean_forward_return": self.mean_forward_return,
            "median_forward_return": self.median_forward_return,
            "win_rate": self.win_rate,
            "p10_forward_return": self.p10_forward_return,
            "p90_forward_return": self.p90_forward_return,
            "mean_mfe": self.mean_mfe,
            "median_mfe": self.median_mfe,
            "mean_mae": self.mean_mae,
            "median_mae": self.median_mae,
            "mfe_mae_ratio": self.mfe_mae_ratio,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class FeatureObservation:
    feature: FeatureValue
    target: ForwardTarget


def compute_feature_bucket_metrics(
    *,
    observations: Iterable[FeatureObservation],
    bucket_method: str,
    min_bucket_count: int = 30,
) -> tuple[FeatureBucketMetric, ...]:
    bucket_count = _parse_bucket_method(bucket_method)
    grouped: dict[tuple[str, str], list[FeatureObservation]] = {}
    for observation in observations:
        key = (observation.feature.name, observation.target.horizon_label)
        grouped.setdefault(key, []).append(observation)

    metrics: list[FeatureBucketMetric] = []
    for key in sorted(grouped):
        feature_name, horizon_label = key
        rows = sorted(grouped[key], key=_observation_sort_key)
        buckets = _bucket_observations(rows, bucket_count=bucket_count)
        for bucket_index in range(bucket_count):
            bucket_rows = buckets.get(bucket_index, [])
            metrics.append(
                _metric_for_bucket(
                    feature_name=feature_name,
                    horizon_label=horizon_label,
                    bucket_index=bucket_index,
                    bucket_count=bucket_count,
                    rows=bucket_rows,
                    min_bucket_count=min_bucket_count,
                )
            )
    return tuple(metrics)


def _parse_bucket_method(bucket_method: str) -> int:
    method = str(bucket_method or "").strip().lower()
    if not method.startswith("quantile:"):
        raise ValueError("only quantile:N bucket method is supported")
    try:
        count = int(method.split(":", 1)[1])
    except ValueError as exc:
        raise ValueError("quantile bucket count must be an integer") from exc
    if count <= 0:
        raise ValueError("quantile bucket count must be positive")
    return count


def _observation_sort_key(observation: FeatureObservation) -> tuple[str, float | str, int, str]:
    value = observation.feature.value
    comparable: float | str
    comparable = float(value) if isinstance(value, (int, float, bool)) else str(value)
    return (observation.feature.value_type, comparable, observation.target.entry_ts, observation.target.horizon_label)


def _bucket_observations(
    rows: list[FeatureObservation],
    *,
    bucket_count: int,
) -> dict[int, list[FeatureObservation]]:
    buckets: dict[int, list[FeatureObservation]] = {index: [] for index in range(bucket_count)}
    total = len(rows)
    if total == 0:
        return buckets
    for rank, row in enumerate(rows):
        bucket_index = min(bucket_count - 1, (rank * bucket_count) // total)
        buckets[bucket_index].append(row)
    return buckets


def _metric_for_bucket(
    *,
    feature_name: str,
    horizon_label: str,
    bucket_index: int,
    bucket_count: int,
    rows: list[FeatureObservation],
    min_bucket_count: int,
) -> FeatureBucketMetric:
    bucket_id = f"q{bucket_index:02d}"
    bucket_label = f"quantile {bucket_index + 1}/{bucket_count}"
    count = len(rows)
    if count == 0:
        return FeatureBucketMetric(
            feature_name=feature_name,
            bucket_id=bucket_id,
            bucket_label=bucket_label,
            horizon_label=horizon_label,
            count=0,
            mean_forward_return=None,
            median_forward_return=None,
            win_rate=None,
            p10_forward_return=None,
            p90_forward_return=None,
            mean_mfe=None,
            median_mfe=None,
            mean_mae=None,
            median_mae=None,
            mfe_mae_ratio=None,
            warnings=("low_sample_count",),
        )

    returns = tuple(float(row.target.gross_forward_return) for row in rows)
    mfes = tuple(float(row.target.mfe) for row in rows)
    maes = tuple(float(row.target.mae) for row in rows)
    mean_return = sum(returns) / count
    median_return = float(median(returns))
    mean_mfe = sum(mfes) / count
    mean_mae = sum(maes) / count
    abs_mean_mae = abs(mean_mae)
    warnings: list[str] = []
    if count < int(min_bucket_count):
        warnings.append("low_sample_count")
    if median_return < 0.0 < mean_return:
        warnings.append("negative_median_positive_mean")
    if abs_mean_mae > max(mean_mfe, 0.0):
        warnings.append("high_mae_relative_to_mfe")
    return FeatureBucketMetric(
        feature_name=feature_name,
        bucket_id=bucket_id,
        bucket_label=bucket_label,
        horizon_label=horizon_label,
        count=count,
        mean_forward_return=mean_return,
        median_forward_return=median_return,
        win_rate=sum(1 for value in returns if value > 0.0) / count,
        p10_forward_return=_percentile(returns, 0.10),
        p90_forward_return=_percentile(returns, 0.90),
        mean_mfe=mean_mfe,
        median_mfe=float(median(mfes)),
        mean_mae=mean_mae,
        median_mae=float(median(maes)),
        mfe_mae_ratio=(mean_mfe / abs_mean_mae) if abs_mean_mae > 0.0 else None,
        warnings=tuple(warnings),
    )


def _percentile(values: tuple[float, ...], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * float(fraction)))))
    return float(ordered[position])
