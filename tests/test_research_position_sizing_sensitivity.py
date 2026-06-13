from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.experiment_manifest import (
    DateRange,
    PortfolioPolicy,
    PositionSizingPolicy,
    legacy_research_portfolio_policy,
)
from bithumb_bot.research.validation_protocol import (
    PORTFOLIO_POLICY_EXECUTION_MISMATCH_REASON,
    _invoke_strategy_runner,
    _portfolio_policy_execution_gate_reasons,
    _position_sizing_sensitivity_summary,
)


def _candidate() -> dict[str, object]:
    return {
        "validation_metrics_v2": {
            "total_return_pct": 10.0,
            "max_drawdown_pct": 5.0,
            "profit_factor": 2.0,
        },
        "validation_closed_trades": [
            {"entry_ts": 1, "exit_ts": 2, "return_pct": 10.0, "net_pnl": 99_000.0},
            {"entry_ts": 3, "exit_ts": 4, "return_pct": -5.0, "net_pnl": -49_500.0},
            {"entry_ts": 5, "exit_ts": 6, "return_pct": 20.0, "net_pnl": 198_000.0},
        ],
    }


def _manifest_portfolio_policy(*, starting_cash: float = 100_000.0) -> PortfolioPolicy:
    return PortfolioPolicy(
        schema_version=1,
        starting_cash_krw=starting_cash,
        quote_currency="KRW",
        initial_position_qty=0.0,
        cash_interest_policy="zero",
        position_sizing=PositionSizingPolicy(
            type="fractional_cash",
            buy_fraction=0.99,
            sell_policy="sell_all_available_position",
            cash_buffer_policy="retain_1_percent_before_fees",
            min_order_krw=None,
            max_order_krw=None,
            rounding_policy="engine_float_no_exchange_lot_rounding",
        ),
        source="manifest",
    )


def test_invoke_strategy_runner_keeps_manifest_starting_cash_when_risk_policy_unsupported() -> None:
    received: dict[str, object] = {}

    def fake_runner(
        *,
        dataset,
        parameter_values,
        fee_rate,
        slippage_bps,
        parameter_stability_score=None,
        execution_model=None,
        execution_timing_policy=None,
        portfolio_policy=None,
        context=None,
    ):
        received["starting_cash_krw"] = portfolio_policy.starting_cash_krw
        received["risk_policy_present"] = "risk_policy" in locals()
        return SimpleNamespace(resource_usage={})

    _invoke_strategy_runner(
        runner=fake_runner,
        dataset=DatasetSnapshot(
            snapshot_id="empty",
            source="unit",
            market="KRW-BTC",
            interval="1m",
            split_name="validation",
            date_range=DateRange(start="2023-01-01", end="2023-01-01"),
            candles=(),
        ),
        parameter_values={},
        fee_rate=0.0004,
        slippage_bps=0.0,
        parameter_stability_score=None,
        execution_model=None,
        execution_timing_policy=None,
        portfolio_policy=_manifest_portfolio_policy(starting_cash=100_000.0),
        risk_policy=object(),
        context=None,
    )

    assert received["starting_cash_krw"] == 100_000.0


def test_portfolio_policy_mismatch_fails_with_fixed_reason() -> None:
    assert _portfolio_policy_execution_gate_reasons(
        {
            "work_unit_portfolio_policy_hash": "sha256:manifest",
            "executed_portfolio_policy_hash": "sha256:legacy",
        }
    ) == [PORTFOLIO_POLICY_EXECUTION_MISMATCH_REASON]


def test_position_sizing_sensitivity_keeps_separate_portfolio_policy_hashes() -> None:
    summary = _position_sizing_sensitivity_summary(
        base_policy=legacy_research_portfolio_policy(),
        candidate=_candidate(),
    )

    by_fraction = summary["by_buy_fraction"]
    assert len(by_fraction) >= 2
    assert by_fraction["0.99"]["portfolio_policy_hash"].startswith("sha256:")
    assert by_fraction["0.10"]["portfolio_policy_hash"].startswith("sha256:")
    assert by_fraction["0.99"]["portfolio_policy_hash"] != by_fraction["0.10"]["portfolio_policy_hash"]


def test_position_sizing_sensitivity_does_not_override_primary_metrics() -> None:
    candidate = _candidate()
    original = dict(candidate["validation_metrics_v2"])

    summary = _position_sizing_sensitivity_summary(
        base_policy=legacy_research_portfolio_policy(),
        candidate=candidate,
    )

    assert candidate["validation_metrics_v2"] == original
    assert summary["primary_metrics_overridden"] is False
    assert summary["promotion_authority"] == "diagnostic_only_excluded_from_promotion"


def test_position_sizing_sensitivity_uses_independent_portfolio_simulation() -> None:
    summary = _position_sizing_sensitivity_summary(
        base_policy=legacy_research_portfolio_policy(),
        candidate=_candidate(),
    )

    assert summary["status"] == "available"
    assert summary["direct_linear_scaling_used"] is False
    assert "missing_reason" not in summary
    assert summary["by_buy_fraction"]["0.50"]["simulation_method"] == "independent_closed_trade_portfolio_replay"
    assert summary["by_buy_fraction"]["0.50"]["validation_trade_count"] == 3


def test_position_sizing_sensitivity_does_not_linearly_scale_primary_metrics() -> None:
    summary = _position_sizing_sensitivity_summary(
        base_policy=legacy_research_portfolio_policy(),
        candidate=_candidate(),
    )

    assert summary["by_buy_fraction"]["0.50"]["validation_return_pct"] != 5.0
    assert summary["by_buy_fraction"]["0.10"]["validation_return_pct"] != 10.0 * (0.10 / 0.99)
    assert summary["by_buy_fraction"]["0.50"]["validation_max_drawdown_pct"] is not None


def test_position_sizing_sensitivity_persists_non_null_metrics_for_each_fraction() -> None:
    summary = _position_sizing_sensitivity_summary(
        base_policy=legacy_research_portfolio_policy(),
        candidate=_candidate(),
    )

    for fraction in ("0.99", "0.50", "0.25", "0.10"):
        result = summary["by_buy_fraction"][fraction]
        assert result["validation_return_pct"] is not None
        assert result["validation_max_drawdown_pct"] is not None
        assert result["validation_profit_factor"] is not None
        assert result["portfolio_policy_hash"].startswith("sha256:")
