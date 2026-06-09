from __future__ import annotations

import pytest

from bithumb_bot.research.feature_bucket_metrics import FeatureObservation, compute_feature_bucket_metrics
from bithumb_bot.research.feature_diagnostic_features import FeatureValue
from tests.test_feature_bucket_metrics import _feature_spec, _target


def test_bucketizer_type_category_uses_category_buckets_even_when_bucket_method_is_quantile() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="regime", value="trend_up", value_type="str"),
            target=_target(1, 0.01),
        ),
    )

    metrics = compute_feature_bucket_metrics(
        observations=observations,
        bucket_method="quantile:10",
        feature_specs=(_feature_spec(name="regime", value_type="str", bucketizer_type="category"),),
        min_bucket_count=1,
    )

    assert {metric.bucket_id for metric in metrics} == {"category:trend_up"}
    assert not any(metric.bucket_id.startswith("q") for metric in metrics)


def test_bucketizer_type_quantile_rejects_string_value() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="sma_gap", value="trend_up", value_type="str"),
            target=_target(1, 0.01),
        ),
    )

    with pytest.raises(ValueError, match="does not match bucket policy"):
        compute_feature_bucket_metrics(
            observations=observations,
            bucket_method="quantile:10",
            feature_specs=(_feature_spec(name="sma_gap", value_type="float", bucketizer_type="quantile"),),
            min_bucket_count=1,
        )


def test_missing_bucket_policy_fails_closed() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="sma_gap", value=0.1, value_type="float"),
            target=_target(1, 0.01),
        ),
    )

    with pytest.raises(ValueError, match="missing feature bucket policy"):
        compute_feature_bucket_metrics(
            observations=observations,
            bucket_method="quantile:1",
            feature_specs=(),
            min_bucket_count=1,
        )


def test_registry_bucketizer_type_change_changes_metric_bucketizer_behavior() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="sma_gap", value=0.1, value_type="float"),
            target=_target(1, 0.01),
        ),
    )

    with pytest.raises(ValueError, match="category bucketizer requires categorical value_type"):
        compute_feature_bucket_metrics(
            observations=observations,
            bucket_method="quantile:10",
            feature_specs=(_feature_spec(name="sma_gap", value_type="float", bucketizer_type="category"),),
            min_bucket_count=1,
        )
