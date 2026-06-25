from __future__ import annotations

from bithumb_bot.reason_codes import (
    RISK_METRIC_DENOMINATOR_MISSING,
    RISK_METRIC_SCOPE_MISMATCH,
    RISK_METRIC_UNIT_MISMATCH,
)
from bithumb_bot.risk_contract import RiskLimit, RiskMetric, compare_risk_metric_to_limit


def _metric(**overrides: object) -> RiskMetric:
    payload = {
        "value": 2.0,
        "unit": "percent_point",
        "scope": "risk_scope",
        "denominator_kind": "allocated_capital",
        "denominator_value": 100_000.0,
        "sample_count": 1,
        "state": "valid",
        "source_table": "trade_lifecycles",
        "formula_version": "unit",
    }
    payload.update(overrides)
    return RiskMetric(**payload)  # type: ignore[arg-type]


def test_drawdown_metric_rejects_unit_mismatch() -> None:
    result = compare_risk_metric_to_limit(
        _metric(value=100.0, unit="percent_point"),
        RiskLimit(value=0.03, unit="ratio", scope="risk_scope"),
    )

    assert result.reason_code == RISK_METRIC_UNIT_MISMATCH


def test_drawdown_metric_requires_denominator() -> None:
    result = compare_risk_metric_to_limit(
        _metric(denominator_value=0.0),
        RiskLimit(value=3.0, unit="percent_point", scope="risk_scope"),
    )

    assert result.reason_code == RISK_METRIC_DENOMINATOR_MISSING


def test_drawdown_metric_compares_only_same_scope() -> None:
    result = compare_risk_metric_to_limit(
        _metric(scope="strategy_instance"),
        RiskLimit(value=3.0, unit="percent_point", scope="risk_scope"),
    )

    assert result.reason_code == RISK_METRIC_SCOPE_MISMATCH
