from __future__ import annotations

from bithumb_bot.research.feature_bucket_metrics import FeatureObservation, compute_feature_bucket_metrics
from bithumb_bot.research.feature_diagnostic_features import FeatureValue
from bithumb_bot.research.forward_targets import ForwardTarget


def _target(index: int, value: float, *, mfe: float = 0.05, mae: float = -0.02) -> ForwardTarget:
    return ForwardTarget(
        horizon_label="1c",
        horizon_steps=1,
        entry_ts=index,
        exit_ts=index + 1,
        entry_price=100.0,
        exit_price=100.0 * (1.0 + value),
        gross_forward_return=value,
        mfe=mfe,
        mae=mae,
        entry_price_mode="next_open",
    )


def _obs(values: list[float]) -> list[FeatureObservation]:
    return [
        FeatureObservation(
            feature=FeatureValue(name="sma_gap", value=float(index), value_type="float"),
            target=_target(index, value),
        )
        for index, value in enumerate(values)
    ]


def test_quantile_bucket_metrics_are_deterministic() -> None:
    first = compute_feature_bucket_metrics(observations=_obs([0.01] * 20), bucket_method="quantile:10")
    second = compute_feature_bucket_metrics(observations=_obs([0.01] * 20), bucket_method="quantile:10")

    assert first == second
    assert [metric.bucket_id for metric in first] == [f"q{index:02d}" for index in range(10)]


def test_bucket_metrics_include_mean_and_median() -> None:
    metric = compute_feature_bucket_metrics(observations=_obs([0.01, 0.03]), bucket_method="quantile:1")[0]

    assert metric.mean_forward_return == 0.02
    assert metric.median_forward_return == 0.02


def test_bucket_metrics_include_win_rate() -> None:
    metric = compute_feature_bucket_metrics(observations=_obs([-0.01, 0.03]), bucket_method="quantile:1")[0]

    assert metric.win_rate == 0.5


def test_empty_bucket_metrics_use_none_not_zero() -> None:
    metrics = compute_feature_bucket_metrics(observations=_obs([0.01, 0.02]), bucket_method="quantile:10")
    empty = next(metric for metric in metrics if metric.count == 0)

    assert empty.mean_forward_return is None
    assert empty.median_forward_return is None


def test_low_sample_count_warning_is_machine_readable() -> None:
    metric = compute_feature_bucket_metrics(
        observations=_obs([0.01]),
        bucket_method="quantile:1",
        min_bucket_count=2,
    )[0]

    assert "low_sample_count" in metric.warnings
    assert isinstance(metric.warnings, tuple)


def test_negative_median_positive_mean_warning() -> None:
    observations = _obs([-0.02, -0.01, 0.20])
    metric = compute_feature_bucket_metrics(observations=observations, bucket_method="quantile:1", min_bucket_count=1)[0]

    assert "negative_median_positive_mean" in metric.warnings


def test_high_mae_relative_to_mfe_warning() -> None:
    observations = [
        FeatureObservation(
            feature=FeatureValue(name="sma_gap", value=1.0, value_type="float"),
            target=_target(1, 0.01, mfe=0.01, mae=-0.05),
        )
    ]

    metric = compute_feature_bucket_metrics(observations=observations, bucket_method="quantile:1", min_bucket_count=1)[0]

    assert "high_mae_relative_to_mfe" in metric.warnings
