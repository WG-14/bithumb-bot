from __future__ import annotations

import inspect
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import ApprovedProfileError, runtime_contract_from_env_values
from bithumb_bot.paths import PathManager
from bithumb_bot.research import backtest_kernel, validation_protocol
from bithumb_bot.research.backtest_engine import BacktestRunContext
from bithumb_bot.research.backtest_kernel import run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.execution_model import ExecutionFill, ExecutionRequest, FixedBpsExecutionModel
from bithumb_bot.research.experiment_manifest import DateRange, parse_manifest
from bithumb_bot.research.strategy_registry import resolve_research_strategy, resolve_research_strategy_plugin
from bithumb_bot.research.strategy_spec import StrategySpecError, validate_parameter_space_against_strategy_spec
from bithumb_bot.research.validation_protocol import run_research_backtest


def _dataset() -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id="noop_canary_unit",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=tuple(
            Candle(index * 60_000, 100.0 + index, 100.0 + index, 100.0 + index, 100.0 + index, 1.0)
            for index in range(5)
        ),
    )


class _TrackingExecutionModel:
    name = "tracking_fixed_bps"
    version = "tracking_fixed_bps_v1"

    def __init__(self) -> None:
        self.delegate = FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=5.0)
        self.simulate_count = 0

    def params_payload(self) -> dict[str, object]:
        return {"type": self.name, "version": self.version, "delegate": self.delegate.params_payload()}

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        self.simulate_count += 1
        fill = self.delegate.simulate(request)
        return ExecutionFill(**{**fill.as_dict(), "model_name": self.name, "model_version": self.version})


def test_research_decision_event_is_strategy_neutral() -> None:
    event = ResearchDecisionEvent(
        candle_ts=1,
        decision_ts=2,
        strategy_name="noop_baseline",
        strategy_version="noop_baseline.research_contract.v1",
        raw_signal="HOLD",
        final_signal="HOLD",
        reason="noop_baseline_hold",
        feature_snapshot={"close": 100.0},
        strategy_diagnostics={"hold_decision_count": 1},
    )

    assert event.strategy_name == "noop_baseline"
    assert event.feature_snapshot == {"close": 100.0}
    assert event.order_intent is None


def test_noop_baseline_runs_through_backtest_result_contract() -> None:
    runner = resolve_research_strategy("noop_baseline")

    result = runner(
        _dataset(),
        {"NOOP_DECISION_START_INDEX": 1},
        0.001,
        0.0,
        None,
        None,
        None,
        None,
        None,
    )

    assert result.candle_count == 5
    assert result.trades == ()
    assert result.metrics_v2 is not None
    assert result.execution_event_summary is not None
    assert result.resource_usage is not None
    assert result.resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["strategy_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["composite_behavior_hash"] == result.resource_usage["behavior_hash"]
    assert result.strategy_diagnostics is not None
    assert result.strategy_diagnostics["strategy_diagnostics_namespace"] == "noop_baseline"
    assert set(result.strategy_diagnostics["strategy_specific_diagnostics"]) == {"noop_baseline"}
    assert result.decisions
    first = result.decisions[0]
    assert first["strategy_name"] == "noop_baseline"
    assert first["strategy_plugin_contract"]["name"] == "noop_baseline"
    assert first["strategy_plugin_contract_hash"].startswith("sha256:")
    assert first["strategy_decision_contract_version"] == "research_noop_baseline_decision_contract.v1"
    assert first["execution_intent"] == "none"
    assert first["strategy_diagnostics_namespace"] == "noop_baseline"


def test_buy_and_hold_baseline_is_independent_executable_canary() -> None:
    plugin = resolve_research_strategy_plugin("buy_and_hold_baseline")

    assert plugin.name == "buy_and_hold_baseline"
    assert plugin.spec.strategy_name == "buy_and_hold_baseline"
    assert plugin.spec.spec_hash() != resolve_research_strategy_plugin("sma_with_filter").spec.spec_hash()
    assert plugin.runtime_replay_builder is None
    assert plugin.diagnostics_namespace == "buy_and_hold_baseline"


def test_buy_and_hold_baseline_uses_common_execution_kernel() -> None:
    runner = resolve_research_strategy("buy_and_hold_baseline")
    model = _TrackingExecutionModel()

    result = runner(
        _dataset(),
        {"BUY_HOLD_BUY_INDEX": 1},
        0.001,
        5.0,
        None,
        model,
        None,
        None,
        BacktestRunContext(report_detail="full"),
    )

    assert model.simulate_count == 1
    assert result.trades
    assert result.execution_event_summary is not None
    assert result.execution_event_summary["execution_attempt_count"] == 1
    assert result.execution_event_summary["filled_execution_count"] == 1
    assert result.execution_event_summary["portfolio_applied_trade_count"] == 1
    assert result.metrics_v2 is not None
    assert result.metrics_v2.cost_execution.filled_execution_count == 1
    assert result.metrics_v2.return_risk.open_position_at_end is True
    assert result.metrics_v2.trade_quality.execution_count == 1
    assert result.trades[0]["side"] == "BUY"
    assert result.trades[0]["asset_qty"] > 0.0
    assert result.resource_usage is not None
    assert result.resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["strategy_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["composite_behavior_hash_v2"].startswith("sha256:")
    assert result.strategy_diagnostics is not None
    assert result.strategy_diagnostics["strategy_diagnostics_namespace"] == "buy_and_hold_baseline"
    assert set(result.strategy_diagnostics["strategy_specific_diagnostics"]) == {"buy_and_hold_baseline"}
    buy_decisions = [item for item in result.decisions if item["final_signal"] == "BUY"]
    assert len(buy_decisions) == 1
    assert buy_decisions[0]["execution_intent"] == "buy"
    assert buy_decisions[0]["strategy_plugin_contract"]["name"] == "buy_and_hold_baseline"
    assert buy_decisions[0]["strategy_decision_contract_version"] == (
        "research_buy_and_hold_baseline_decision_contract.v1"
    )


def test_buy_and_hold_baseline_enters_common_kernel_through_public_boundary(monkeypatch) -> None:
    runner = resolve_research_strategy("buy_and_hold_baseline")
    calls: list[str] = []
    original = backtest_kernel.run_decision_event_backtest

    def counting_kernel(**kwargs):
        calls.append(str(kwargs["strategy_name"]))
        return original(**kwargs)

    monkeypatch.setattr(backtest_kernel, "run_decision_event_backtest", counting_kernel)

    result = runner(
        _dataset(),
        {"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "canary"},
        0.001,
        5.0,
        None,
        None,
        None,
        None,
        BacktestRunContext(report_detail="full"),
    )

    assert calls == ["buy_and_hold_baseline"]
    assert result.execution_event_summary["execution_attempt_count"] == 1
    assert result.strategy_diagnostics["strategy_diagnostics_namespace"] == "buy_and_hold_baseline"


def test_decision_event_kernel_does_not_require_sma_features() -> None:
    dataset = _dataset()
    event = ResearchDecisionEvent(
        candle_ts=dataset.candles[1].ts,
        decision_ts=dataset.candles[1].ts + 60_000,
        strategy_name="buy_and_hold_baseline",
        strategy_version="buy_and_hold_baseline.research_contract.v1",
        raw_signal="BUY",
        final_signal="BUY",
        reason="kernel_contract_buy",
        feature_snapshot={"candle_index": 1, "close": dataset.candles[1].close},
        strategy_diagnostics={"schema_version": 1, "emitted_buy_intent": True},
        entry_signal="BUY",
        order_intent={"side": "BUY"},
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="buy_and_hold_baseline",
        parameter_values={"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "kernel_contract_buy"},
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
        context=BacktestRunContext(report_detail="full"),
    )

    assert result.trades
    assert result.trades[0]["side"] == "BUY"
    assert result.decisions[0]["feature_snapshot"] == {"candle_index": 1, "close": dataset.candles[1].close}
    assert result.decisions[0]["curr_s"] == 0.0
    assert result.decisions[0]["gap_ratio"] == 0.0


def test_noop_baseline_parameter_validation_rejects_unknowns() -> None:
    with pytest.raises(StrategySpecError, match="unknown strategy parameter"):
        validate_parameter_space_against_strategy_spec(
            strategy_name="noop_baseline",
            parameter_space={"SMA_SHORT": (2,)},
            deployment_tier="research",
        )


def test_buy_and_hold_manifest_uses_its_own_parameter_contract() -> None:
    validate_parameter_space_against_strategy_spec(
        strategy_name="buy_and_hold_baseline",
        parameter_space={"BUY_HOLD_BUY_INDEX": (1,), "BUY_HOLD_DECISION_REASON": ("architecture_canary",)},
        deployment_tier="research",
    )


def test_buy_and_hold_manifest_rejects_sma_parameters() -> None:
    with pytest.raises(StrategySpecError, match="unknown strategy parameter"):
        validate_parameter_space_against_strategy_spec(
            strategy_name="buy_and_hold_baseline",
            parameter_space={"SMA_SHORT": (2,), "SMA_LONG": (4,)},
            deployment_tier="research",
        )


def test_production_bound_buy_and_hold_manifest_requires_behavior_parameters() -> None:
    with pytest.raises(StrategySpecError, match="BUY_HOLD_BUY_INDEX"):
        validate_parameter_space_against_strategy_spec(
            strategy_name="buy_and_hold_baseline",
            parameter_space={"BUY_HOLD_DECISION_REASON": ("architecture_canary",)},
            deployment_tier="paper_candidate",
        )


def test_noop_baseline_runtime_env_contract_fails_closed() -> None:
    with pytest.raises(ApprovedProfileError, match="runtime_replay_unsupported_for_strategy:noop_baseline"):
        runtime_contract_from_env_values({"STRATEGY_NAME": "noop_baseline"})


def test_buy_and_hold_runtime_env_contract_fails_closed() -> None:
    with pytest.raises(ApprovedProfileError, match="runtime_replay_unsupported_for_strategy:buy_and_hold_baseline"):
        runtime_contract_from_env_values({"STRATEGY_NAME": "buy_and_hold_baseline", "SMA_SHORT": "2", "SMA_LONG": "4"})


def test_validation_protocol_has_no_buy_and_hold_specific_branch() -> None:
    source = inspect.getsource(validation_protocol)

    assert "buy_and_hold_baseline" not in source


def test_buy_and_hold_full_research_backtest_report_contains_common_kernel_fields(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_buy_and_hold_manifest())

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-24T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    assert candidate["strategy_name"] == "buy_and_hold_baseline"
    assert candidate["strategy_plugin_contract"]["name"] == "buy_and_hold_baseline"
    assert candidate["strategy_plugin_contract_hash"].startswith("sha256:")
    assert candidate["strategy_spec_hash"].startswith("sha256:")
    assert candidate["validation_metrics_v2"]["metrics_schema_version"] == 2
    assert candidate["validation_metrics_v2"]["cost_execution"]["filled_execution_count"] == 1
    assert candidate["validation_execution_event_summary"]["execution_attempt_count"] == 1
    assert candidate["validation_execution_event_summary"]["filled_execution_count"] == 1
    resource_usage = candidate["validation_resource_usage"]
    assert resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert resource_usage["strategy_behavior_hash"].startswith("sha256:")
    assert resource_usage["composite_behavior_hash_v2"].startswith("sha256:")
    assert candidate["common_decision_behavior_hash"] == resource_usage["common_decision_behavior_hash"]
    assert candidate["strategy_behavior_hash"] == resource_usage["strategy_behavior_hash"]
    assert candidate["composite_behavior_hash_v2"] == resource_usage["composite_behavior_hash_v2"]
    assert candidate["strategy_diagnostics"]["strategy_diagnostics_namespace"] == "buy_and_hold_baseline"
    assert set(candidate["strategy_diagnostics"]["strategy_specific_diagnostics"]) == {"buy_and_hold_baseline"}
    assert candidate["validation_audit_trace_index"] is None
    assert candidate["resource_guard"] is None


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE candles(
                ts INTEGER PRIMARY KEY,
                pair TEXT,
                interval TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            for index in range(8):
                close = 100.0 + float(index)
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close + 1.0, close - 1.0, close),
                )
        conn.commit()
    finally:
        conn.close()


def _buy_and_hold_manifest() -> dict[str, object]:
    return {
        "experiment_id": "buy_hold_canary_common_kernel",
        "hypothesis": "Executable canary proves strategy-neutral execution/accounting path.",
        "strategy_name": "buy_and_hold_baseline",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "buy_hold_canary_candles",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "BUY_HOLD_BUY_INDEX": [1],
            "BUY_HOLD_DECISION_REASON": ["architecture_canary_buy"],
        },
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [5.0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
        },
    }
