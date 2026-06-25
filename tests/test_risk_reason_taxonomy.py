from __future__ import annotations

from bithumb_bot.reason_codes import (
    DRAWDOWN_UNDEFINED_NO_CAPITAL_BASE,
    MAX_DRAWDOWN_PCT,
    RISK_METRIC_UNIT_MISMATCH,
)
from bithumb_bot.risk_contract import RiskLimit, RiskMetric, compare_risk_metric_to_limit


def test_undefined_drawdown_uses_no_capital_reason() -> None:
    metric = RiskMetric(
        value=None,
        unit="percent_point",
        scope="risk_scope",
        denominator_kind="allocated_capital",
        denominator_value=None,
        sample_count=0,
        state="undefined",
        source_table="trade_lifecycles",
        formula_version="unit",
        reason_code=DRAWDOWN_UNDEFINED_NO_CAPITAL_BASE,
    )

    result = compare_risk_metric_to_limit(metric, RiskLimit(value=3.0, unit="percent_point", scope="risk_scope"))

    assert result.reason_code == DRAWDOWN_UNDEFINED_NO_CAPITAL_BASE


def test_unit_mismatch_uses_metric_unit_mismatch() -> None:
    metric = RiskMetric(
        value=100.0,
        unit="percent_point",
        scope="risk_scope",
        denominator_kind="allocated_capital",
        denominator_value=100_000.0,
        sample_count=1,
        state="valid",
        source_table="trade_lifecycles",
        formula_version="unit",
    )

    result = compare_risk_metric_to_limit(metric, RiskLimit(value=0.03, unit="ratio", scope="risk_scope"))

    assert result.reason_code == RISK_METRIC_UNIT_MISMATCH


def test_valid_exceeded_drawdown_uses_max_drawdown_pct() -> None:
    metric = RiskMetric(
        value=4.0,
        unit="percent_point",
        scope="risk_scope",
        denominator_kind="allocated_capital",
        denominator_value=100_000.0,
        sample_count=1,
        state="valid",
        source_table="trade_lifecycles",
        formula_version="unit",
    )

    result = compare_risk_metric_to_limit(metric, RiskLimit(value=3.0, unit="percent_point", scope="risk_scope"))

    assert result.exceeded is True
    assert result.reason_code == MAX_DRAWDOWN_PCT
