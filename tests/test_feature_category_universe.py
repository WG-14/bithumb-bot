from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import product
from pathlib import Path

from bithumb_bot.research.feature_bucket_metrics import FeatureObservation, compute_feature_bucket_metrics
from bithumb_bot.research.feature_diagnostic_features import FeatureValue
from bithumb_bot.research.feature_provider_registry import (
    FeatureProviderSpec,
    REGIME_CATEGORY_UNIVERSE,
    feature_provider_spec_for_name,
)
from bithumb_bot.research.forward_diagnostics import run_forward_diagnostics_on_snapshot
from bithumb_bot.research.forward_diagnostics_report import write_forward_diagnostics_report
import bithumb_bot.research.forward_diagnostics as forward_diagnostics
from tests.test_feature_bucket_metrics import _feature_spec, _target
from tests.test_forward_diagnostics_feature_contract import _snapshot
from tests.test_forward_diagnostics_report import _manager, _manifest, _result


@dataclass(frozen=True)
class _UnknownCategoryProvider:
    name: str = "regime"

    def compute(self, *, view) -> FeatureValue:
        return FeatureValue(name=self.name, value="unknown_regime", value_type="str")


def test_category_universe_creates_zero_count_bucket_for_missing_category() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="regime", value="trend_up", value_type="str"),
            target=_target(1, 0.01),
        ),
    )

    metrics = compute_feature_bucket_metrics(
        observations=observations,
        bucket_method="quantile:10",
        feature_specs=(
            _feature_spec(
                name="regime",
                value_type="str",
                bucketizer_type="category",
                category_universe=("trend_up", "range"),
            ),
        ),
        min_bucket_count=1,
    )
    by_bucket = {metric.bucket_id: metric for metric in metrics}

    assert by_bucket["category:range"].count == 0
    assert "category_universe_missing" in by_bucket["category:range"].warnings
    assert "category_coverage_drift" in by_bucket["category:range"].warnings


def test_unknown_category_value_emits_warning() -> None:
    observations = (
        FeatureObservation(
            feature=FeatureValue(name="regime", value="unknown_regime", value_type="str"),
            target=_target(1, 0.01),
        ),
    )

    metrics = compute_feature_bucket_metrics(
        observations=observations,
        bucket_method="quantile:10",
        feature_specs=(
            _feature_spec(
                name="regime",
                value_type="str",
                bucketizer_type="category",
                category_universe=("trend_up", "range"),
            ),
        ),
        min_bucket_count=1,
    )
    unknown = {metric.bucket_id: metric for metric in metrics}["category:unknown_regime"]

    assert "unknown_category_value" in unknown.warnings
    assert "category_coverage_drift" in unknown.warnings


def test_unknown_category_value_emits_report_warning(monkeypatch) -> None:
    spec = FeatureProviderSpec(
        name="regime",
        provider=_UnknownCategoryProvider(),
        value_type="str",
        required_history=1,
        definition_hash="sha256:" + "1" * 64,
        bucketizer_type="category",
        causal_inputs=("test",),
        category_universe=("trend_up", "range"),
    )
    monkeypatch.setattr(forward_diagnostics, "feature_provider_specs_for_names", lambda names: (spec,))

    result = run_forward_diagnostics_on_snapshot(
        snapshot=_snapshot(),
        feature_names=("regime",),
        horizon_steps=(1,),
        bucket_method="quantile:10",
        min_bucket_count=1,
    )

    assert "unknown_category_value" in {warning["reason"] for warning in result.warnings}


def test_regime_provider_spec_declares_category_universe() -> None:
    spec = feature_provider_spec_for_name("regime")
    expected = tuple(
        "_".join(parts)
        for parts in product(
            ("unknown", "sideways", "uptrend", "downtrend"),
            ("low_vol", "normal_vol", "high_vol"),
            ("unknown", "volume_decreasing", "volume_normal", "volume_increasing"),
        )
    )

    assert spec.category_universe == expected
    assert REGIME_CATEGORY_UNIVERSE == expected


def test_category_universe_is_included_in_report_feature_provider_specs(tmp_path: Path) -> None:
    spec = feature_provider_spec_for_name("regime")

    report = write_forward_diagnostics_report(
        manager=_manager(tmp_path),
        manifest=_manifest(),
        result=_result(feature_provider_specs=(spec,)),
    )

    assert report["feature_provider_specs"][0]["name"] == "regime"
    assert report["feature_provider_specs"][0]["category_universe"] == list(spec.category_universe)


def test_report_content_hash_changes_when_category_universe_changes(tmp_path: Path) -> None:
    spec = feature_provider_spec_for_name("regime")
    changed_spec = replace(spec, category_universe=spec.category_universe + ("new_category",))

    first = write_forward_diagnostics_report(
        manager=_manager(tmp_path / "a"),
        manifest=_manifest(),
        result=_result(feature_provider_specs=(spec,)),
    )
    second = write_forward_diagnostics_report(
        manager=_manager(tmp_path / "b"),
        manifest=_manifest(),
        result=_result(feature_provider_specs=(changed_spec,)),
    )

    assert first["content_hash"] != second["content_hash"]
