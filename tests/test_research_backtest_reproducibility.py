from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.canonical_decision import export_research_decisions, export_runtime_replay_decisions
from bithumb_bot.decision_equivalence import compare_decision_equivalence
from bithumb_bot.research import backtest_engine
from bithumb_bot.research.backtest_engine import (
    BacktestHeartbeatPolicy,
    BacktestResourceLimitExceeded,
    BacktestResourceLimits,
    BacktestRunContext,
    run_sma_backtest,
)
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot, TopOfBookQuote
from bithumb_bot.research.execution_calibration import build_calibration_artifact
from bithumb_bot.research.execution_model import ExecutionFill, ExecutionRequest, FixedBpsExecutionModel, StressExecutionModel
from bithumb_bot.research.experiment_manifest import (
    DateRange,
    ExecutionTimingPolicy,
    ManifestValidationError,
    legacy_research_portfolio_policy,
    parse_manifest,
)
from bithumb_bot.research.audit_trail import AuditTraceScope, AuditTrailPolicy, verify_audit_trail, write_trace_manifest
from bithumb_bot.research.return_panel import build_candidate_return_panel
from bithumb_bot.research import cli as research_cli
from bithumb_bot.research.cli import _print_report_summary
from bithumb_bot.research.experiment_registry import (
    experiment_registry_path,
    load_experiment_registry_rows,
    reserve_research_attempt_checked,
)
from bithumb_bot.research.parameter_space import candidate_id
from bithumb_bot.research.promotion_gate import PromotionGateError, _verify_report_content_hash, promote_candidate
from bithumb_bot.research.validation_protocol import _promotion_blocking_reasons, run_research_backtest, run_research_walk_forward
from bithumb_bot.research import validation_protocol
from bithumb_bot.strategy.sma import create_sma_with_filter_strategy


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
        pattern = [100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96]
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            for index in range(24 * 60):
                close = pattern[index % len(pattern)]
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close * 1.01, close * 0.99, close),
                )
        conn.commit()
    finally:
        conn.close()


def _manifest() -> dict[str, object]:
    return {
        "experiment_id": "deterministic_sma",
        "hypothesis": "SMA candidate remains deterministic across repeated research runs.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "unit_candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
        },
    }


def _portfolio_policy(*, starting_cash: float = 1_000_000.0, buy_fraction: float = 0.99) -> dict[str, object]:
    cash_buffer_policy = (
        "retain_1_percent_before_fees"
        if buy_fraction == 0.99
        else "derived_from_buy_fraction_before_fees"
    )
    return {
        "schema_version": 1,
        "starting_cash_krw": starting_cash,
        "quote_currency": "KRW",
        "initial_position_qty": 0.0,
        "cash_interest_policy": "zero",
        "position_sizing": {
            "type": "fractional_cash",
            "buy_fraction": buy_fraction,
            "sell_policy": "sell_all_available_position",
            "cash_buffer_policy": cash_buffer_policy,
            "min_order_krw": None,
            "max_order_krw": None,
            "rounding_policy": "engine_float_no_exchange_lot_rounding",
        },
        "source": "manifest",
    }


def _max_holding_dataset() -> DatasetSnapshot:
    prices = [10, 9, 8, 9, 10, 11, 12, 12, 12, 12, 12, 12]
    candles = tuple(
        Candle(index * 60_000, price, price, price, price, 1.0)
        for index, price in enumerate(prices)
    )
    return DatasetSnapshot(
        snapshot_id="max_holding_fixture",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=candles,
    )


def test_backtest_max_holding_changes_decision_hash() -> None:
    dataset = _max_holding_dataset()
    base = {
        "SMA_SHORT": 2,
        "SMA_LONG": 3,
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_MARKET_REGIME_ENABLED": False,
        "STRATEGY_EXIT_RULES": "opposite_cross,max_holding_time",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0,
    }

    disabled = run_sma_backtest(
        dataset=dataset,
        parameter_values={**base, "STRATEGY_EXIT_MAX_HOLDING_MIN": 0},
        fee_rate=0.0,
        slippage_bps=0.0,
        portfolio_policy=legacy_research_portfolio_policy(),
    )
    enabled = run_sma_backtest(
        dataset=dataset,
        parameter_values={**base, "STRATEGY_EXIT_MAX_HOLDING_MIN": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        portfolio_policy=legacy_research_portfolio_policy(),
    )

    assert disabled.resource_usage["behavior_hash"] != enabled.resource_usage["behavior_hash"]
    assert any(trade.exit_rule == "max_holding_time" for trade in enabled.closed_trades)


def test_closed_trade_diagnostics_include_mae_mfe_and_exit_rule() -> None:
    result = run_sma_backtest(
        dataset=_max_holding_dataset(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
            "STRATEGY_EXIT_RULES": "opposite_cross,max_holding_time",
            "STRATEGY_EXIT_MAX_HOLDING_MIN": 2,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        portfolio_policy=legacy_research_portfolio_policy(),
    )

    closed = result.closed_trades[0].as_dict()
    for key in (
        "entry_ts",
        "exit_ts",
        "holding_minutes",
        "entry_price",
        "exit_price",
        "entry_regime",
        "exit_regime",
        "exit_rule",
        "exit_reason",
        "mae",
        "mfe",
        "mae_pct",
        "mfe_pct",
        "bars_to_mae",
        "bars_to_mfe",
        "unrealized_pnl_path_summary",
        "entry_decision_hash",
        "exit_decision_hash",
    ):
        assert key in closed
    assert closed["exit_rule"] == "max_holding_time"
    assert closed["exit_reason"] == "exit by max holding time"


def _production_bound_statistical_manifest() -> dict[str, object]:
    payload = _manifest()
    payload["deployment_tier"] = "paper_candidate"
    payload["portfolio_policy"] = _portfolio_policy()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": 0.0,
        "latency_ms": 0,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "market_order_extra_cost_bps": 0.0,
        "scenario_policy": "single_scenario",
        "scenario_role": "base",
        "label": "test_operator_declared_zero_fee_zero_slippage",
        "fee_source": "operator_declared_bithumb_app_fee",
        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
        "slippage_source": "test_execution_calibration",
        "promotable_as_base": True,
        "calibration_required": False,
    }
    payload["execution_timing"] = {
        "signal_basis": "closed_candle",
        "decision_time": "candle_close",
        "decision_guard_ms": 0,
        "fill_reference_policy": "next_candle_open",
        "quote_selection": "first_after_or_equal",
        "max_quote_wait_ms": 3000,
        "missing_quote_policy": "warn",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "candle_next_open",
    }
    payload["acceptance_gate"]["max_single_trade_dependency_score"] = 1.0
    payload["statistical_validation"] = {
        "required_for_promotion": True,
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "selection_universe": "all_parameter_candidates_all_required_scenarios",
        "multiple_testing_scope": "experiment",
        "bootstrap": {
            "method": "metric_centered_max_bootstrap",
            "n_bootstrap": 20,
            "block_length_policy": "not_applicable_summary_metric",
            "seed_policy": "derived_from_selection_universe_hash",
        },
        "gates": {
            "max_reality_check_p_value": 1.0,
            "max_spa_p_value": None,
            "min_deflated_sharpe_probability": None,
            "max_holdout_reuse_count": 0,
            "max_attempt_index_without_new_hypothesis": 1,
        },
    }
    payload["stress_suite"] = _stress_suite_contract()
    payload["final_selection"] = {
        "schema_version": 1,
        "required_for_promotion": True,
        "candidate_universe": "acceptance_gate_passed_required_scenarios",
        "must_pass": {
            "dataset_quality_gate_status": "PASS",
            "statistical_gate_result": "PASS",
            "production_calibration_policy_result": "PASS",
            "final_holdout_present": True,
        },
        "selection_exposure_policy": {
            "final_holdout_usage": "confirmatory_metric_in_rank",
            "counts_as_holdout_reuse": True,
        },
        "method": "lexicographic",
        "null_metric_policy": "fail_if_required_else_worst_rank",
        "ranking": [
            {
                "metric": "final_holdout.metrics_v2.trade_quality.expectancy_per_trade_krw",
                "order": "desc",
                "required": True,
            },
            {"metric": "parameter_candidate_id", "order": "asc", "required": True},
        ],
        "unsupported_metric_policy": {
            "sharpe_ratio": "fail_if_required",
            "sortino_ratio": "fail_if_required",
        },
    }
    return payload


def _registry_payload_for_production_manifest(**overrides: object) -> dict[str, object]:
    payload = {
        "run_id": "deterministic_sma",
        "experiment_family_id": "deterministic_sma",
        "hypothesis_id": "SMA candidate remains deterministic across repeated research runs.",
        "hypothesis_status": "pre_registered",
        "hypothesis_identity_source": "manifest.hypothesis",
        "experiment_family_identity_source": "experiment_id",
        "experiment_id": "deterministic_sma",
        "manifest_hash": "sha256:manifest",
        "manifest_metadata_hash": "sha256:metadata",
        "dataset_snapshot_id": "unit_candles_v1",
        "dataset_content_hash": None,
        "dataset_quality_hash": None,
        "train_split_hash": "sha256:train",
        "validation_split_hash": "sha256:validation",
        "final_holdout_split_hash": None,
        "final_holdout_fingerprint": "sha256:holdout-identity",
        "final_holdout_identity_hash": "sha256:holdout-identity",
        "final_holdout_content_hash": None,
        "final_holdout_reuse_key_hash": "sha256:holdout-identity",
        "final_holdout_content_pending_until_completion": True,
        "parameter_space_hash": "sha256:space",
        "parameter_grid_size": 1,
        "candidate_count": None,
        "declared_attempt_index": None,
        "declared_holdout_reuse_count": None,
        "statistical_evidence_hash": None,
        "return_panel_hash": None,
        "promotion_artifact_hash": None,
        "promoted_candidate_id": None,
        "repository_version": "test",
        "command_args_hash": "sha256:args",
    }
    payload.update(overrides)
    return payload


def _stress_suite_contract(*, min_retention: float | None = None, min_survival: float = 0.0) -> dict[str, object]:
    payload = {
        "required_for_promotion": True,
        "trade_removal": {
            "top_n_by_net_pnl": [1],
        },
        "trade_order_monte_carlo": {
            "iterations": 20,
            "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
            "min_survival_probability": min_survival,
            "ruin_max_drawdown_pct": 90.0,
            "min_closed_trades": 1,
        },
    }
    if min_retention is not None:
        payload["trade_removal"]["min_return_retention_pct"] = min_retention
    return payload


class _FailSellExecutionModel:
    name = "fail_sell_test"
    version = "test_v1"

    def __init__(self) -> None:
        self._fixed = FixedBpsExecutionModel(fee_rate=0.0, slippage_bps=0.0)

    def params_payload(self) -> dict[str, object]:
        return {"type": self.name, "version": self.version}

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        fill = self._fixed.simulate(request)
        if str(request.side).upper() != "SELL":
            return fill
        return replace(
            fill,
            filled_qty=0.0,
            remaining_qty=float(request.requested_qty or 0.0),
            avg_fill_price=None,
            fee=0.0,
            fill_status="failed",
            model_name=self.name,
            model_version=self.version,
        )


class _PartialSellExecutionModel:
    name = "partial_sell_test"
    version = "test_v1"

    def __init__(self) -> None:
        self._fixed = FixedBpsExecutionModel(fee_rate=0.0, slippage_bps=0.0)

    def params_payload(self) -> dict[str, object]:
        return {"type": self.name, "version": self.version}

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        fill = self._fixed.simulate(request)
        if str(request.side).upper() != "SELL":
            return fill
        filled_qty = float(fill.filled_qty) * 0.5
        return replace(
            fill,
            filled_qty=filled_qty,
            remaining_qty=max(0.0, float(fill.requested_qty) - filled_qty),
            fee=0.0,
            fill_status="partial",
            model_name=self.name,
            model_version=self.version,
        )


def _snapshot_from_closes(closes: list[float], *, quotes: tuple[TopOfBookQuote, ...] = ()) -> DatasetSnapshot:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=float(close),
            high=max(float(close), 130.0),
            low=min(float(close), 100.0) * 0.9,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    manifest = parse_manifest(_manifest())
    return DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
        top_of_book_event_quotes=quotes,
    )


def test_same_manifest_and_dataset_produce_same_content_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_manifest())

    first = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert first["content_hash"] == second["content_hash"]
    assert first["candidates"][0]["candidate_profile_hash"] == second["candidates"][0]["candidate_profile_hash"]
    assert first["candidates"][0]["regime_classifier_version"] == "market_regime_v2"
    assert first["metrics_schema_version"] == 2
    assert first["candidates"][0]["validation_metrics_v2"]["metrics_schema_version"] == 2
    assert first["candidates"][0]["final_holdout_metrics_v2"]["metrics_schema_version"] == 2
    assert first["best_validation_metrics_v2"]["metrics_schema_version"] == 2
    json.dumps(first, allow_nan=False)
    json.dumps(first["candidates"][0], allow_nan=False)
    assert first["candidates"][0]["market_regime_bucket_performance"]
    assert first["candidates"][0]["market_regime_coverage"]
    assert "regime_gate_result" in first["candidates"][0]
    assert Path(first["artifact_paths"]["report_path"]).exists()
    persisted = json.loads(Path(first["artifact_paths"]["report_path"]).read_text(encoding="utf-8"))
    assert persisted["content_hash"] == first["content_hash"]
    assert persisted["artifact_refs"] == first["artifact_refs"]
    assert persisted["artifact_paths"] == first["artifact_paths"]
    assert persisted["artifact_refs"] == {
        "derived_candidates": "derived/research/deterministic_sma/backtest_candidates.json",
        "report": "reports/research/deterministic_sma/backtest_report.json",
        "candidate_events": "derived/research/deterministic_sma/candidate_events.jsonl",
        "candidate_results_dir": "derived/research/deterministic_sma/candidate_results",
        "candidate_failures_dir": "derived/research/deterministic_sma/candidate_failures",
        "audit_trace_manifest": "derived/research/deterministic_sma/trace_manifest.json",
    }
    assert _verify_report_content_hash(persisted, label="backtest_report") == persisted["content_hash"]


def test_research_report_candidate_and_lineage_bind_portfolio_policy(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["portfolio_policy"] = _portfolio_policy(starting_cash=2_000_000.0, buy_fraction=0.5)
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate = report["candidates"][0]

    assert report["portfolio_policy"] == manifest.portfolio_policy.as_dict()
    assert report["portfolio_policy_hash"] == manifest.portfolio_policy_hash()
    assert report["simulation_policy_hash"] == manifest.simulation_policy_hash()
    assert candidate["portfolio_policy"] == report["portfolio_policy"]
    assert candidate["portfolio_policy_hash"] == report["portfolio_policy_hash"]
    assert candidate["simulation_policy_hash"] == report["simulation_policy_hash"]
    assert report["lineage"]["portfolio_policy_hash"] == report["portfolio_policy_hash"]
    assert report["lineage"]["simulation_policy_hash"] == report["simulation_policy_hash"]
    assert candidate["candidate_profile_hash"].startswith("sha256:")


def test_sma_backtest_uses_manifest_portfolio_policy_for_cash_and_buy_fraction() -> None:
    dataset = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    manifest = parse_manifest({**_manifest(), "portfolio_policy": _portfolio_policy(starting_cash=2_000_000.0, buy_fraction=0.5)})

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
        portfolio_policy=manifest.portfolio_policy,
        context=BacktestRunContext(report_detail="full"),
    )
    buy = next(trade for trade in result.trades if trade["side"] == "BUY")

    assert result.equity_curve[0].cash == pytest.approx(2_000_000.0)
    assert buy["execution"]["requested_notional"] == pytest.approx(1_000_000.0)


def test_decision_hash_changes_when_portfolio_policy_changes() -> None:
    dataset = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    common = {
        "dataset": dataset,
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0},
        "fee_rate": 0.0,
        "slippage_bps": 0.0,
        "context": BacktestRunContext(report_detail="full"),
    }
    baseline_manifest = parse_manifest({**_manifest(), "portfolio_policy": _portfolio_policy(buy_fraction=0.99)})
    changed_manifest = parse_manifest({**_manifest(), "portfolio_policy": _portfolio_policy(buy_fraction=0.5)})

    baseline = run_sma_backtest(**common, portfolio_policy=baseline_manifest.portfolio_policy)
    changed = run_sma_backtest(**common, portfolio_policy=changed_manifest.portfolio_policy)

    assert baseline.retained_detail_summary["decision_hash"] != changed.retained_detail_summary["decision_hash"]
    assert baseline.decisions[0]["portfolio_policy_hash"] == baseline_manifest.portfolio_policy_hash()
    assert baseline.decisions[0]["decision_contract_hash"] == baseline.decisions[0]["replay_fingerprint_hash"]


def test_research_engine_has_no_hidden_portfolio_policy_constants() -> None:
    source = Path(backtest_engine.__file__).read_text(encoding="utf-8")

    assert "START_CASH_KRW =" not in source
    assert "BUY_FRACTION =" not in source
    assert "cash_fraction_0.99" not in source


def test_production_declared_mismatch_rejects_before_final_holdout_split_load(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    reserve_research_attempt_checked(manager=manager, base_payload=_registry_payload_for_production_manifest())
    manifest_payload = _production_bound_statistical_manifest()
    manifest_payload["attempt_index"] = 1
    manifest = parse_manifest(manifest_payload)
    loaded_splits: list[str] = []
    original_load = validation_protocol.load_dataset_split

    def tracking_load_dataset_split(*args, **kwargs):
        loaded_splits.append(str(kwargs.get("split_name")))
        return original_load(*args, **kwargs)

    monkeypatch.setattr(validation_protocol, "load_dataset_split", tracking_load_dataset_split)

    with pytest.raises(Exception, match="experiment_registry_preflight_failed"):
        run_research_backtest(manifest=manifest, db_path=db_path, manager=manager)

    assert "final_holdout" not in loaded_splits
    rows = load_experiment_registry_rows(experiment_registry_path(manager=manager))
    assert rows[-1]["event_type"] == "research_attempt_rejected"
    assert rows[-1]["counted_attempt"] is False


def test_production_budget_exceeded_rejects_before_final_holdout_split_load(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    reserve_research_attempt_checked(manager=manager, base_payload=_registry_payload_for_production_manifest())
    manifest_payload = _production_bound_statistical_manifest()
    manifest = parse_manifest(manifest_payload)
    loaded_splits: list[str] = []
    original_load = validation_protocol.load_dataset_split

    def tracking_load_dataset_split(*args, **kwargs):
        loaded_splits.append(str(kwargs.get("split_name")))
        return original_load(*args, **kwargs)

    monkeypatch.setattr(validation_protocol, "load_dataset_split", tracking_load_dataset_split)

    with pytest.raises(Exception, match="experiment_registry_preflight_failed"):
        run_research_backtest(manifest=manifest, db_path=db_path, manager=manager)

    assert "final_holdout" not in loaded_splits
    rows = load_experiment_registry_rows(experiment_registry_path(manager=manager))
    assert rows[-1]["event_type"] == "research_attempt_rejected"
    assert "attempt_budget_exceeded" in rows[-1]["rejection_reasons"]


def test_production_accepted_reservation_then_loads_final_holdout_split(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_production_bound_statistical_manifest())
    loaded_splits: list[str] = []
    original_load = validation_protocol.load_dataset_split

    def tracking_load_dataset_split(*args, **kwargs):
        loaded_splits.append(str(kwargs.get("split_name")))
        return original_load(*args, **kwargs)

    monkeypatch.setattr(validation_protocol, "load_dataset_split", tracking_load_dataset_split)

    run_research_backtest(manifest=manifest, db_path=db_path, manager=manager)

    assert loaded_splits.index("final_holdout") > loaded_splits.index("validation")
    rows = load_experiment_registry_rows(experiment_registry_path(manager=manager))
    assert rows[0]["event_type"] == "research_attempt_reserved"
    assert rows[0]["final_holdout_content_pending_until_completion"] is True


def test_pre_content_reservation_completion_binds_final_holdout_content_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_production_bound_statistical_manifest())

    report = run_research_backtest(manifest=manifest, db_path=db_path, manager=manager)

    rows = load_experiment_registry_rows(experiment_registry_path(manager=manager))
    reservation = rows[0]
    completion = next(row for row in rows if row["event_type"] == "research_attempt_completed")
    assert reservation["final_holdout_content_hash"] is None
    assert completion["final_holdout_content_hash"] == report["final_holdout_content_hash"]
    assert completion["final_holdout_split_hash"] == report["final_holdout_split_hash"]


def test_required_stress_suite_is_attached_to_report_and_candidate(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["stress_suite"] = _stress_suite_contract()
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate = report["candidates"][0]

    assert report["stress_suite_required"] is True
    assert report["stress_suite_contract_hash"].startswith("sha256:")
    assert candidate["stress_suite_gate_result"] == "PASS"
    assert candidate["validation_stress_suite"]["stress_suite_hash"].startswith("sha256:")
    assert report["best_validation_stress_suite"]["stress_suite_hash"] == candidate["validation_stress_suite"]["stress_suite_hash"]
    json.dumps(report, allow_nan=False)


def test_required_stress_suite_failure_blocks_candidate_acceptance(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["stress_suite"] = _stress_suite_contract(min_retention=100.0, min_survival=1.0)
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 0.01
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate = report["candidates"][0]

    assert candidate["acceptance_gate_result"] == "FAIL"
    assert candidate["stress_suite_gate_result"] == "FAIL"
    assert "stress_suite_gate_not_passed" in candidate["gate_fail_reasons"]
    assert report["best_candidate_id"] is None
    assert report["gate_result"] == "FAIL"
    assert report["stress_suite_gate_result"] == "FAIL"
    assert "stress_monte_carlo_survival_probability_failed" in report["stress_suite_fail_reasons"]
    assert report["best_validation_stress_suite"]["stress_suite_hash"] == candidate["validation_stress_suite"]["stress_suite_hash"]


def test_report_content_hash_is_independent_of_data_root(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    manifest = parse_manifest(_manifest())

    reports = []
    for root_name in ("runtime_a", "runtime_b"):
        runtime_root = tmp_path / root_name
        for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
            monkeypatch.setenv(key, str(runtime_root / f"{key.lower()}_root"))
        monkeypatch.setenv("MODE", "paper")
        reports.append(
            run_research_backtest(
                manifest=manifest,
                db_path=db_path,
                manager=PathManager.from_env(Path.cwd()),
                generated_at="2026-05-03T00:00:00+00:00",
            )
        )

    first, second = reports
    assert first["content_hash"] == second["content_hash"]
    assert first["artifact_refs"] == second["artifact_refs"]
    assert first["artifact_paths"]["report_path"] != second["artifact_paths"]["report_path"]


def test_sma_backtest_attaches_entry_and_exit_regime_snapshots() -> None:
    candles = tuple(
        Candle(
            ts=1_700_000_000_000 + index * 60_000,
            open=float(close),
            high=float(close) * 1.02,
            low=float(close) * 0.98,
            close=float(close),
            volume=float(100 + index * 10),
        )
        for index, close in enumerate([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0, "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    closed = [trade for trade in result.trades if trade["side"] == "SELL"]
    assert closed
    assert closed[0]["entry_regime"]
    assert closed[0]["exit_regime"]
    assert isinstance(closed[0]["entry_regime_snapshot"], dict)
    assert isinstance(closed[0]["exit_regime_snapshot"], dict)
    assert result.regime_performance
    assert result.regime_coverage
    assert result.metrics_v2 is not None
    assert result.metrics_v2.metrics_schema_version == 2
    assert result.metrics_v2.trade_quality.closed_trade_count == result.metrics.trade_count
    assert result.metrics_v2.trade_quality.execution_count == len(result.trades)
    assert result.metrics_v2.time_exposure.exposure_time_pct is not None
    assert result.decisions
    assert {"raw_signal", "final_signal", "position_state_hash"} <= set(result.decisions[0])


def test_sma_backtest_uses_bounded_regime_fast_path(monkeypatch) -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    calls: list[int] = []
    original = backtest_engine.classify_market_regime_from_arrays

    def counting_classifier(**kwargs):
        calls.append(int(kwargs["index"]))
        assert len(kwargs["closes"]) == len(snapshot.candles)
        return original(**kwargs)

    monkeypatch.setattr(backtest_engine, "classify_market_regime_from_arrays", counting_classifier)

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.decisions
    assert calls == list(range(4, len(snapshot.candles)))


def test_sma_backtest_caches_dataset_content_hash(monkeypatch) -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    calls = 0

    def counted_content_hash(self: DatasetSnapshot) -> str:
        nonlocal calls
        assert self is snapshot
        calls += 1
        return "sha256:cached_dataset_hash"

    monkeypatch.setattr(DatasetSnapshot, "content_hash", counted_content_hash)

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert calls == 1
    assert result.decisions
    fingerprints = [decision["replay_fingerprint_hash"] for decision in result.decisions]
    assert all(str(item).startswith("sha256:") for item in fingerprints)
    assert fingerprints == [
        decision["replay_fingerprint_hash"]
        for decision in run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
            fee_rate=0.0,
            slippage_bps=0.0,
        ).decisions
    ]


def test_tiny_three_day_sma_backtest_completes_structurally() -> None:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=float(100 + (index % 17) - 8),
            high=float(101 + (index % 17) - 8),
            low=float(99 + (index % 17) - 8),
            close=float(100 + (index % 17) - 8),
            volume=1.0 + float(index % 5),
        )
        for index in range(3 * 24 * 60)
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="tiny_three_day",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=manifest.dataset.split.train,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 7, "SMA_LONG": 30},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.candle_count == 4320
    assert len(result.decisions) == 4320 - 30
    assert result.metrics_v2 is not None


def test_research_run_policy_participates_in_manifest_hash() -> None:
    bounded = parse_manifest(_manifest())
    full_payload = dict(_manifest())
    full_payload["research_run"] = {
        "report_detail": "full",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": None,
            "max_trades": None,
            "max_equity_points_retained": None,
            "max_rss_mb": None,
        },
    }
    full = parse_manifest(full_payload)

    assert bounded.research_run.report_detail == "summary"
    assert bounded.research_run.resource_limits.max_decisions_retained == 0
    assert bounded.manifest_hash() != full.manifest_hash()


def test_summary_mode_does_not_retain_full_per_candle_decisions_and_is_deterministic() -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    context = BacktestRunContext(
        report_detail="summary",
        resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
    )

    first = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=context,
    )
    second = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert first.decisions == ()
    assert first.equity_curve == ()
    assert first.retained_detail_summary["decision_count"] == len(snapshot.candles) - 4
    assert first.retained_detail_summary["retained_regime_snapshot_count"] == 0
    assert first.regime_coverage
    assert first.regime_performance
    assert first.retained_detail_summary["decision_hash"] == second.retained_detail_summary["decision_hash"]
    assert first.metrics.as_dict() == second.metrics.as_dict()


def test_summary_metrics_v2_match_full_when_equity_retention_is_zero() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 50, 40, 30, 20, 30, 45])
    kwargs = {
        "dataset": snapshot,
        "parameter_values": {"SMA_SHORT": 1, "SMA_LONG": 2},
        "fee_rate": 0.0,
        "slippage_bps": 0.0,
    }
    full = run_sma_backtest(
        **kwargs,
        context=BacktestRunContext(report_detail="full"),
    )
    summary = run_sma_backtest(
        **kwargs,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert full.equity_curve
    assert summary.equity_curve == ()
    assert summary.retained_detail_summary["retained_regime_snapshot_count"] == 0
    assert full.metrics_v2 is not None
    assert summary.metrics_v2 is not None
    assert summary.metrics_v2.return_risk.cagr_pct == pytest.approx(full.metrics_v2.return_risk.cagr_pct)
    assert summary.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(full.metrics_v2.return_risk.max_drawdown_pct)
    assert summary.metrics_v2.time_exposure.exposure_time_pct == pytest.approx(full.metrics_v2.time_exposure.exposure_time_pct)
    assert summary.metrics_v2.time_exposure.active_bar_count == full.metrics_v2.time_exposure.active_bar_count
    assert summary.metrics_v2.time_exposure.period_start_ts == full.metrics_v2.time_exposure.period_start_ts
    assert summary.metrics_v2.time_exposure.period_end_ts == full.metrics_v2.time_exposure.period_end_ts
    assert summary.metrics_v2.time_exposure.elapsed_ms == full.metrics_v2.time_exposure.elapsed_ms
    assert summary.metrics_v2.time_exposure.calendar_days == pytest.approx(full.metrics_v2.time_exposure.calendar_days)
    assert summary.regime_coverage == full.regime_coverage
    assert summary.regime_performance == full.regime_performance


def test_summary_and_full_metrics_v2_gates_match_for_cagr_and_exposure(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    base_payload = _manifest()
    base_payload["acceptance_gate"]["metrics_contract_required"] = True
    base_payload["acceptance_gate"]["min_cagr_pct"] = 0.0
    base_payload["acceptance_gate"]["max_exposure_time_pct"] = 100.0

    full_payload = dict(base_payload)
    full_payload["research_run"] = {
        "report_detail": "full",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": None,
            "max_trades": None,
            "max_equity_points_retained": None,
            "max_rss_mb": None,
        },
    }
    summary_payload = dict(base_payload)
    summary_payload["research_run"] = {
        "report_detail": "summary",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": 0,
            "max_trades": None,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
    }

    full = run_research_backtest(
        manifest=parse_manifest(full_payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    summary = run_research_backtest(
        manifest=parse_manifest(summary_payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert summary["candidates"][0]["validation_metrics_v2"]["return_risk"]["cagr_pct"] == pytest.approx(
        full["candidates"][0]["validation_metrics_v2"]["return_risk"]["cagr_pct"]
    )
    assert summary["candidates"][0]["validation_metrics_v2"]["time_exposure"]["exposure_time_pct"] == pytest.approx(
        full["candidates"][0]["validation_metrics_v2"]["time_exposure"]["exposure_time_pct"]
    )
    assert summary["candidates"][0]["acceptance_gate_result"] == full["candidates"][0]["acceptance_gate_result"]
    assert summary["candidates"][0]["gate_fail_reasons"] == full["candidates"][0]["gate_fail_reasons"]


def test_heartbeat_and_max_trades_guard_trip() -> None:
    events: list[dict[str, object]] = []
    snapshot = _snapshot_from_closes(([100, 90, 110, 90, 110, 90, 110, 90] * 5))

    with pytest.raises(BacktestResourceLimitExceeded) as raised:
        run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
            fee_rate=0.0,
            slippage_bps=0.0,
            context=BacktestRunContext(
                experiment_id="guard_exp",
                candidate_id="candidate_guard",
                scenario_id="scenario_1",
                split_name="validation",
                report_detail="summary",
                resource_limits=BacktestResourceLimits(max_trades=2, max_decisions_retained=0, max_equity_points_retained=0),
                heartbeat=BacktestHeartbeatPolicy(interval_s=None, bar_interval=1),
                progress_callback=events.append,
            ),
        )

    assert any(event.get("stage") == "heartbeat" for event in events)
    assert raised.value.reason == "candidate_resource_limit_exceeded"
    assert "max_trades_exceeded" in raised.value.evidence["reasons"]
    assert raised.value.evidence["retained_decision_count"] == 0


def test_research_sweep_continues_after_guard_failure_and_writes_candidate_artifacts(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "bounded_sweep"
    payload["parameter_space"] = {
        "SMA_SHORT": [2],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0, 1.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    payload["research_run"] = {
        "report_detail": "summary",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": 60,
            "max_decisions_retained": 0,
            "max_trades": 1,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
        "heartbeat": {"interval_s": None, "bar_interval": 5},
    }

    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert len(report["candidates"]) == 2
    assert any("candidate_resource_limit_exceeded" in (candidate.get("gate_fail_reasons") or []) for candidate in report["candidates"])
    assert Path(report["artifact_paths"]["report_path"]).exists()
    assert Path(report["artifact_paths"]["derived_path"]).exists()
    assert Path(report["artifact_paths"]["candidate_events_path"]).exists()
    assert Path(report["artifact_paths"]["candidate_results_dir"]).is_dir()
    assert Path(report["artifact_paths"]["candidate_failures_dir"]).is_dir()
    persisted = json.loads(Path(report["artifact_paths"]["report_path"]).read_text(encoding="utf-8"))
    assert persisted["artifact_refs"]["candidate_events"] == "derived/research/bounded_sweep/candidate_events.jsonl"
    assert persisted["artifact_refs"]["candidate_results_dir"] == "derived/research/bounded_sweep/candidate_results"
    assert persisted["artifact_refs"]["candidate_failures_dir"] == "derived/research/bounded_sweep/candidate_failures"
    assert persisted["artifact_paths"] == report["artifact_paths"]
    root = manager.data_dir() / "derived" / "research" / "bounded_sweep"
    assert (root / "candidate_events.jsonl").exists()
    assert list((root / "candidate_results").glob("candidate_*.json"))
    failures = list((root / "candidate_failures").glob("candidate_*.json"))
    assert failures
    failed = [candidate for candidate in persisted["candidates"] if candidate.get("failure_artifact_path")]
    assert failed
    assert failed[0]["failure_artifact_ref"].startswith("derived/research/bounded_sweep/candidate_failures/")
    assert Path(failed[0]["failure_artifact_path"]).exists()
    assert failed[0]["resource_guard"]["status"] == "TRIPPED"


def test_full_decisions_external_jsonl_maps_to_complete_external_audit_policy() -> None:
    payload = _manifest()
    payload["research_run"] = {
        "artifact_policy": {
            "full_decisions_external_jsonl": True,
        },
    }

    manifest = parse_manifest(payload)

    assert manifest.research_run.artifact_policy.full_decisions_external_jsonl is True
    assert manifest.research_run.audit_trail.mode == "complete_external"
    assert manifest.research_run.audit_trail.decisions_required is True
    assert manifest.research_run.audit_trail.equity_required is True
    assert manifest.research_run.audit_trail.executions_required is True


def test_production_example_declares_complete_external_audit_policy() -> None:
    path = Path("examples/research/sma_filter_manifest.production.example.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    manifest = parse_manifest(payload)

    assert manifest.research_run.report_detail == "summary"
    assert manifest.research_run.artifact_policy.full_decisions_external_jsonl is True
    assert manifest.research_run.audit_trail.mode == "complete_external"
    assert manifest.research_run.audit_trail.decisions_required is True
    assert manifest.research_run.audit_trail.equity_required is True
    assert manifest.research_run.audit_trail.executions_required is True
    assert manifest.research_run.audit_trail.hash_chain_required is True
    assert manifest.research_run.audit_trail.required_for_promotion is True


def test_summary_zero_retention_writes_complete_external_audit_traces(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_summary_zero"
    payload["research_run"] = {
        "report_detail": "summary",
        "artifact_policy": {"full_decisions_external_jsonl": True},
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": 0,
            "max_trades": None,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
    }

    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    candidate = report["candidates"][0]
    scenario = candidate["scenario_results"][0]
    validation_index = scenario["validation_audit_trace_index"]
    assert candidate["validation_equity_curve"] == []
    assert scenario["validation_equity_curve"] == []
    assert scenario["retained_detail_summary"]["retained_decision_count"] == 0
    assert scenario["retained_detail_summary"]["retained_equity_point_count"] == 0
    assert validation_index["decision_row_count"] == scenario["retained_detail_summary"]["decision_count"]
    assert validation_index["equity_row_count"] > 0
    assert validation_index["completion_status"] == "completed"
    assert report["audit_trail_status"] == "PASS"
    assert report["audit_trail_trace_manifest_ref"] == "derived/research/audit_summary_zero/trace_manifest.json"
    assert report["audit_trail_trace_manifest_hash"].startswith("sha256:")
    assert Path(report["audit_trail_trace_manifest_path"]).exists()
    assert report["artifact_refs"]["audit_trace_manifest"] == report["audit_trail_trace_manifest_ref"]
    verification = verify_audit_trail(manager=manager, experiment_id="audit_summary_zero")
    assert verification["ok"] is True
    assert verification["reasons"] == []
    decisions_path = manager.data_dir() / validation_index["decisions"]["path"]
    equity_path = manager.data_dir() / validation_index["equity"]["path"]
    assert sum(1 for _ in decisions_path.open("r", encoding="utf-8")) == validation_index["decision_row_count"]
    assert sum(1 for _ in equity_path.open("r", encoding="utf-8")) == validation_index["equity_row_count"]


def test_research_report_keeps_subprocess_candidate_isolation_pending_visible(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())

    report = run_research_backtest(
        manifest=parse_manifest(_manifest()),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["data_limitations"]["subprocess_candidate_isolation"] == "subprocess_candidate_isolation_pending"


def test_audit_trace_verification_detects_tamper_and_missing_stream(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_tamper"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    index = report["candidates"][0]["scenario_results"][0]["validation_audit_trace_index"]
    decisions_path = manager.data_dir() / index["decisions"]["path"]
    lines = decisions_path.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    row["payload"]["raw_signal"] = "TAMPERED"
    lines[0] = json.dumps(row, sort_keys=True, separators=(",", ":"))
    decisions_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    tampered = verify_audit_trail(manager=manager, experiment_id="audit_tamper")
    assert tampered["ok"] is False
    assert "audit_trail_hash_chain_mismatch" in tampered["reasons"]

    equity_path = manager.data_dir() / index["equity"]["path"]
    equity_path.unlink()
    missing = verify_audit_trail(manager=manager, experiment_id="audit_tamper")
    assert missing["ok"] is False
    assert "audit_trail_equity_stream_missing" in missing["reasons"]


def test_audit_trace_verification_accepts_aborted_terminal_status(tmp_path, monkeypatch) -> None:
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    scope = AuditTraceScope(
        manager=manager,
        experiment_id="audit_aborted_terminal",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        candidate_id="candidate_001",
        scenario_id="scenario_001",
        scenario_index=0,
        split="validation",
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
    )
    scope.write_decision({"decision_ts": 1, "raw_signal": "HOLD"})
    index = scope.complete(status="aborted")
    write_trace_manifest(
        manager=manager,
        experiment_id="audit_aborted_terminal",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        trace_indexes=[index],
        policy=AuditTrailPolicy(mode="complete_external", decisions_required=True, equity_required=True, executions_required=True),
    )

    result = verify_audit_trail(manager=manager, experiment_id="audit_aborted_terminal", expected_manifest_hash="sha256:manifest")

    assert result["ok"] is True
    assert result["reasons"] == []


def test_resource_limit_failure_trace_is_report_and_manifest_bound(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_resource_failure"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}

    def runner(**kwargs):
        context = kwargs.get("context")
        if context.split_name == "validation":
            raise BacktestResourceLimitExceeded(
                "candidate_resource_limit_exceeded",
                {"status": "TRIPPED", "reasons": ["max_runtime_exceeded"]},
            )
        return run_sma_backtest(**kwargs)

    monkeypatch.setattr(validation_protocol, "resolve_research_strategy", lambda _name: runner)
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    scenario = report["candidates"][0]["scenario_results"][0]
    failed_index = scenario["validation_audit_trace_index"]
    assert failed_index["completion_status"] == "failed"
    assert scenario["resource_guard"]["audit_trace_index"] == failed_index
    manifest_payload = json.loads(Path(report["audit_trail_trace_manifest_path"]).read_text(encoding="utf-8"))
    assert failed_index["trace_index_ref"] in {
        item["trace_index_ref"] for item in manifest_payload["trace_indexes"]
    }
    assert verify_audit_trail(manager=manager, experiment_id="audit_resource_failure")["ok"] is True


def test_generic_candidate_exception_trace_is_report_and_manifest_bound(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_generic_failure"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}

    def runner(**kwargs):
        context = kwargs.get("context")
        if context.split_name == "validation":
            raise RuntimeError("synthetic validation failure")
        return run_sma_backtest(**kwargs)

    monkeypatch.setattr(validation_protocol, "resolve_research_strategy", lambda _name: runner)
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    scenario = report["candidates"][0]["scenario_results"][0]
    failed_index = scenario["validation_audit_trace_index"]
    assert failed_index["completion_status"] == "failed"
    assert scenario["resource_guard"]["audit_trace_index"] == failed_index
    assert scenario["resource_guard"]["split"] == "validation"
    manifest_payload = json.loads(Path(report["audit_trail_trace_manifest_path"]).read_text(encoding="utf-8"))
    assert failed_index["trace_index_ref"] in {
        item["trace_index_ref"] for item in manifest_payload["trace_indexes"]
    }
    assert verify_audit_trail(manager=manager, experiment_id="audit_generic_failure")["ok"] is True


def test_complete_external_audit_rerun_replaces_streams_without_append_contamination(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_rerun_clean"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}

    first = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    first_index = first["candidates"][0]["scenario_results"][0]["validation_audit_trace_index"]
    second = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second_index = second["candidates"][0]["scenario_results"][0]["validation_audit_trace_index"]

    assert verify_audit_trail(manager=manager, experiment_id="audit_rerun_clean")["ok"] is True
    decisions_path = manager.data_dir() / second_index["decisions"]["path"]
    equity_path = manager.data_dir() / second_index["equity"]["path"]
    assert sum(1 for _ in decisions_path.open("r", encoding="utf-8")) == second_index["decision_row_count"]
    assert sum(1 for _ in equity_path.open("r", encoding="utf-8")) == second_index["equity_row_count"]
    assert second_index["decision_row_count"] == first_index["decision_row_count"]


def test_return_panel_uses_external_equity_trace_when_embedded_curve_is_zero_retained(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_return_panel"
    payload["research_run"] = {
        "report_detail": "summary",
        "artifact_policy": {"full_decisions_external_jsonl": True},
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": 0,
            "max_trades": None,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
    }
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    panel = build_candidate_return_panel(
        experiment_id=report["experiment_id"],
        manifest_hash=report["manifest_hash"],
        dataset_content_hash=report["dataset_content_hash"],
        dataset_quality_hash=report["dataset_quality_hash"],
        split="validation",
        benchmark="cash",
        candidates=report["candidates"],
        manager=manager,
    )

    assert report["candidates"][0]["validation_equity_curve"] == []
    assert panel["return_unit"] == "portfolio_bar_return"
    assert panel["promotion_grade_available"] is True
    assert panel["observation_count"] > 0


def test_production_bound_statistical_validation_requires_audit_trace_when_missing(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _production_bound_statistical_manifest()
    payload["experiment_id"] = "audit_required_prod"
    payload["statistical_validation"]["bootstrap"]["method"] = "white_reality_check_block_bootstrap"
    payload["statistical_validation"]["bootstrap"]["block_length_policy"] = "fixed"
    payload["research_run"] = {
        "report_detail": "summary",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": 0,
            "max_trades": None,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
    }

    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["audit_trail_status"] == "DISABLED"
    assert "audit_trail_required_for_promotion" in report["statistical_gate_fail_reasons"]
    assert report["statistical_gate_result"] == "FAIL"


def test_promotion_revalidates_audit_trace_and_refuses_tampered_stream(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _production_bound_statistical_manifest()
    payload["experiment_id"] = "audit_promotion_tamper"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate_id_value = report["candidates"][0]["parameter_candidate_id"]
    index = report["candidates"][0]["scenario_results"][0]["validation_audit_trace_index"]
    decisions_path = manager.data_dir() / index["decisions"]["path"]
    lines = decisions_path.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    row["payload"]["raw_signal"] = "TAMPERED"
    lines[0] = json.dumps(row, sort_keys=True, separators=(",", ":"))
    decisions_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(PromotionGateError, match="audit_trail_hash_chain_mismatch"):
        promote_candidate(
            experiment_id="audit_promotion_tamper",
            candidate_id=candidate_id_value,
            manager=manager,
        )


def test_registry_validate_revalidates_audit_trace_and_refuses_missing_stream(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    payload = _production_bound_statistical_manifest()
    payload["experiment_id"] = "audit_registry_missing"
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}
    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    index = report["candidates"][0]["scenario_results"][0]["validation_audit_trace_index"]
    (manager.data_dir() / index["equity"]["path"]).unlink()

    exit_code = research_cli.cmd_research_registry_validate(experiment_id="audit_registry_missing")
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "audit_trail_equity_stream_missing" in output


def test_walk_forward_complete_external_audit_traces_all_windows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "audit_walk_forward"
    payload["acceptance_gate"]["walk_forward_required"] = True
    payload["walk_forward"] = {
        "train_window_days": 1,
        "test_window_days": 1,
        "step_days": 1,
        "min_windows": 1,
    }
    payload["research_run"] = {"artifact_policy": {"full_decisions_external_jsonl": True}}

    report = run_research_walk_forward(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    windows = report["candidates"][0]["scenario_results"][0]["walk_forward_metrics"]["windows"]
    assert windows
    for window in windows:
        assert window["train_audit_trace_index"]["split"].endswith("_train")
        assert window["test_audit_trace_index"]["split"].endswith("_test")
    manifest_payload = json.loads(Path(report["audit_trail_trace_manifest_path"]).read_text(encoding="utf-8"))
    manifest_splits = {item["split"] for item in manifest_payload["trace_indexes"]}
    for window in windows:
        assert window["train_audit_trace_index"]["split"] in manifest_splits
        assert window["test_audit_trace_index"]["split"] in manifest_splits
    assert verify_audit_trail(manager=manager, experiment_id="audit_walk_forward")["ok"] is True


def test_retention_caps_do_not_fail_candidate_but_max_trades_guard_does() -> None:
    snapshot = _snapshot_from_closes(([100, 90, 110, 90, 110, 90, 110, 90] * 2))
    capped = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert capped.retained_detail_summary["retained_decision_count"] == 0
    assert capped.retained_detail_summary["retained_equity_point_count"] == 0
    assert capped.retained_detail_summary["decision_count"] > 0

    with pytest.raises(BacktestResourceLimitExceeded) as raised:
        run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
            fee_rate=0.0,
            slippage_bps=0.0,
            context=BacktestRunContext(
                report_detail="summary",
                resource_limits=BacktestResourceLimits(
                    max_trades=1,
                    max_decisions_retained=0,
                    max_equity_points_retained=0,
                ),
            ),
        )
    assert raised.value.evidence["reasons"] == ["max_trades_exceeded"]


def test_failed_sell_records_failure_candle_equity_and_mdd() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 50, 40, 30, 20])

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=_FailSellExecutionModel(),
    )

    sell = [trade for trade in result.trades if trade["side"] == "SELL"][0]
    failure_mark = next(point for point in result.equity_curve if point.ts == sell["decision_ts"])
    assert sell["execution"]["fill_status"] == "failed"
    assert failure_mark.asset_qty > 0.0
    assert failure_mark.equity == pytest.approx(505000.0)
    assert result.metrics.max_drawdown_pct > 60.0
    assert result.metrics_v2 is not None
    assert result.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(result.metrics.max_drawdown_pct)


def test_missing_quote_skipped_sell_records_failure_candle_equity_and_mdd() -> None:
    base_ts = 1_700_000_000_000
    buy_decision_ts = base_ts + 5 * 60_000
    snapshot = _snapshot_from_closes(
        [100, 90, 100, 80, 100, 130, 50, 40, 30, 20],
        quotes=(
            TopOfBookQuote(
                ts=buy_decision_ts,
                pair="KRW-BTC",
                bid_price=99.9,
                ask_price=100.1,
                spread_bps=20.0,
                source="test",
            ),
        ),
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            missing_quote_policy="warn",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    sell = [trade for trade in result.trades if trade["side"] == "SELL"][0]
    failure_mark = next(point for point in result.equity_curve if point.ts == sell["decision_ts"])
    assert sell["execution"]["fill_status"] == "skipped_with_warning"
    assert failure_mark.asset_qty > 0.0
    assert failure_mark.equity == pytest.approx(504505.49450549454)
    assert result.metrics.max_drawdown_pct > 60.0
    assert result.metrics_v2 is not None
    assert result.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(result.metrics.max_drawdown_pct)


def test_partial_sell_keeps_residual_position_open_in_metrics_v2() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 120, 110, 90, 80])

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=_PartialSellExecutionModel(),
    )

    assert result.metrics_v2 is not None
    assert result.metrics.trade_count == 1
    assert result.metrics_v2.return_risk.open_position_at_end is True
    assert result.metrics_v2.return_risk.unrealized_pnl_end == pytest.approx(-99000.0)
    assert result.metrics_v2.trade_quality.closed_trade_count == 1
    assert result.closed_trades[0].net_pnl == pytest.approx(99000.0)
    assert len(result.position_intervals) == 1
    assert result.position_intervals[0].close_ts is None
    assert result.metrics_v2.time_exposure.exposure_time_pct is not None
    assert result.metrics_v2.time_exposure.exposure_time_pct > 0.0
    assert result.metrics_v2.cost_execution.filled_execution_count == 2
    assert result.metrics_v2.cost_execution.partial_fill_count == 1
    assert result.metrics_v2.cost_execution.failed_execution_count == 0
    assert result.metrics_v2.cost_execution.skipped_execution_count == 0


def test_research_runtime_decision_generation_gap_is_visible_not_silent() -> None:
    closes = [100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96]
    candles = tuple(
        Candle(
            ts=1_700_000_000_000 + index * 60_000,
            open=float(close),
            high=float(close) * 1.02,
            low=float(close) * 0.98,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )
    research = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0, "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
    )
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE candles(ts INTEGER, pair TEXT, interval TEXT, close REAL)")
    for candle in candles:
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (candle.ts, "KRW-BTC", "1m", candle.close),
        )
    conn.commit()
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=4,
        pair="KRW-BTC",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=1,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["opposite_cross", "max_holding_time"],
    )
    selected_research_decision = dict(research.decisions[1])
    runtime_decisions = export_runtime_replay_decisions(
        conn=conn,
        strategy=strategy,
        through_ts_list=[selected_research_decision["candle_ts"]],
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )
    research_decisions = export_research_decisions(
        [selected_research_decision],
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )

    result = compare_decision_equivalence(
        research_decisions=research_decisions,
        runtime_decisions=runtime_decisions,
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert runtime_decisions
    assert result.ok is False
    assert result.report["promotion_grade_comparison"] is False or result.report["mismatch_count"] > 0


def test_fixed_bps_execution_model_preserves_legacy_backtest_metrics() -> None:
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=1_700_000_000_000 + index * 60_000,
                open=float(close),
                high=float(close) * 1.01,
                low=float(close) * 0.99,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 99, 98, 99, 101, 103, 102, 100, 98, 97, 99, 102])
        ),
    )

    legacy = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.001,
        slippage_bps=5.0,
    )
    modeled = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.001,
        slippage_bps=5.0,
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=5.0),
    )

    assert modeled.metrics.as_dict() == legacy.metrics.as_dict()
    assert modeled.trades[0]["execution"]["model_name"] == "fixed_bps"
    assert modeled.trades[0]["execution"]["model_params_hash"].startswith("sha256:")


def test_seeded_stress_execution_model_is_deterministic_and_auditable() -> None:
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=1_700_000_000_000 + index * 60_000,
                open=float(close),
                high=float(close) * 1.01,
                low=float(close) * 0.99,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 99, 98, 99, 101, 103, 102, 100, 98, 97, 99, 102])
        ),
    )
    def _run():
        return run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
            fee_rate=0.001,
            slippage_bps=20.0,
            execution_model=StressExecutionModel(
            fee_rate=0.001,
            slippage_bps=20.0,
            latency_ms=500,
            partial_fill_rate=1.0,
            order_failure_rate=0.0,
            market_order_extra_cost_bps=5.0,
            seed=42,
            ),
        )

    first = _run()
    second = _run()

    assert first.trades == second.trades
    execution = first.trades[0]["execution"]
    assert execution["fill_status"] == "partial"
    assert execution["latency_ms"] == 500
    assert execution["slippage_bps"] == 25.0
    assert execution["fee"] >= 0.0
    assert execution["filled_qty"] > 0.0
    assert execution["remaining_qty"] > 0.0


def test_sma_signal_close_executes_next_candle_open_not_same_close() -> None:
    manifest = parse_manifest(_manifest())
    base_ts = 1_700_000_000_000
    closes = [100, 90, 100, 80, 100, 130]
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=130.0 if index == 5 else float(close),
            high=max(float(close), 130.0 if index == 5 else float(close)) + 1.0,
            low=min(float(close), 130.0 if index == 5 else float(close)) - 1.0,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    assert result.trades
    execution = result.trades[0]["execution"]
    assert result.trades[0]["side"] == "BUY"
    assert result.trades[0]["price"] == 130.0
    assert execution["signal_reference_price"] == 100.0
    assert execution["fill_reference_price"] == 130.0
    assert execution["fill_reference_source"] == "next_candle_open"


def test_decision_ts_is_after_signal_candle_close() -> None:
    manifest = parse_manifest(_manifest())
    base_ts = 1_700_000_000_000
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=base_ts + index * 60_000,
                open=float(close),
                high=float(close) + 1.0,
                low=float(close) - 1.0,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 90, 100, 80, 100, 130])
        ),
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    assert execution["signal_candle_start_ts"] == base_ts + 4 * 60_000
    assert execution["signal_candle_close_ts"] == base_ts + 5 * 60_000
    assert execution["decision_ts"] >= execution["signal_candle_close_ts"]
    assert execution["decision_ts"] != execution["signal_candle_start_ts"]


def test_reproducibility_hash_changes_when_execution_timing_policy_changes(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    legacy_manifest = parse_manifest(_manifest())
    next_open_payload = _manifest()
    next_open_payload["execution_timing"] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
    }
    next_open_manifest = parse_manifest(next_open_payload)

    legacy = run_research_backtest(
        manifest=legacy_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    next_open = run_research_backtest(
        manifest=next_open_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert legacy["manifest_hash"] != next_open["manifest_hash"]
    assert legacy["candidates"][0]["candidate_profile_hash"] != next_open["candidates"][0]["candidate_profile_hash"]


def test_metrics_gate_threshold_change_changes_manifest_and_candidate_evidence_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    base_payload = _manifest()
    base_payload["acceptance_gate"]["metrics_contract_required"] = True
    base_payload["acceptance_gate"]["min_cagr_pct"] = 1.0
    changed_payload = _manifest()
    changed_payload["acceptance_gate"]["metrics_contract_required"] = True
    changed_payload["acceptance_gate"]["min_cagr_pct"] = 2.0
    base_manifest = parse_manifest(base_payload)
    changed_manifest = parse_manifest(changed_payload)

    base_report = run_research_backtest(
        manifest=base_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    changed_report = run_research_backtest(
        manifest=changed_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert base_report["manifest_hash"] != changed_report["manifest_hash"]
    assert base_report["candidates"][0]["metrics_gate_policy"]["min_cagr_pct"] == 1.0
    assert changed_report["candidates"][0]["metrics_gate_policy"]["min_cagr_pct"] == 2.0
    assert base_report["candidates"][0]["metrics_gate_policy_hash"] != changed_report["candidates"][0]["metrics_gate_policy_hash"]
    assert base_report["candidates"][0]["candidate_profile_hash"] != changed_report["candidates"][0]["candidate_profile_hash"]


def test_research_backtest_fails_candidate_when_calibration_breaches_assumptions(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5],
        "latency_ms": [100],
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 50,
            "median_slippage_vs_signal_bps": 8.0,
            "p90_slippage_vs_signal_bps": 12.0,
            "p95_slippage_vs_signal_bps": 20.0,
            "p95_submit_to_fill_ms": 200,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_p90_slippage_exceeds_assumption" in report["candidates"][0]["gate_fail_reasons"]


def test_research_backtest_fails_candidate_when_required_calibration_missing(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": 5,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_missing" in report["candidates"][0]["gate_fail_reasons"]


def test_production_bound_screening_report_exposes_promotion_grade_unavailable(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _production_bound_statistical_manifest()
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 50,
            "median_slippage_vs_signal_bps": 0.0,
            "p90_slippage_vs_signal_bps": 0.0,
            "p95_slippage_vs_signal_bps": 0.0,
            "p95_submit_to_fill_ms": 0,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    assert report["evidence_grade"] == "SCREENING_SUMMARY_BOOTSTRAP"
    assert report["official_promotion_grade_wrc_generation_available"] is False
    assert "promotion_grade_statistical_generation_unavailable" in report["warnings"]
    assert "promotion_grade_statistical_generation_unavailable" in report["promotion_grade_limitations"]
    assert report["promotion_eligibility_gate_result"] == "FAIL"


def test_production_bound_screening_only_promotion_blocking_reasons_include_grade_insufficient() -> None:
    evidence = {
        "content_hash": "sha256:evidence",
        "candidate_metric_values_hash": "sha256:metric",
        "selection_universe_hash": "sha256:selection",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 1,
        "candidate_metric_values_summary": {
            "candidate_count": 1,
            "metric_value_count": 1,
            "missing_metric_count": 0,
        },
        "metric_value_count": 1,
        "missing_metric_count": 0,
        "search_budget": 1,
        "parameter_grid_size": 1,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "primary_metric_source": "validation_metrics",
        "evidence_grade": "SCREENING_SUMMARY_BOOTSTRAP",
        "summary_metric_max_bootstrap_p_value": 0.01,
        "white_reality_check_p_value": None,
        "white_reality_check_method": None,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
        "effective_trial_count": 1,
        "official_promotion_grade_wrc_generation_available": False,
    }
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 1,
        "search_budget": 1,
        "parameter_grid_size": 1,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_validation_required": True,
        "statistical_validation_contract": _production_bound_statistical_manifest()["statistical_validation"],
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": "sha256:metric",
        "metric_value_count": 1,
        "missing_metric_count": 0,
        "statistical_evidence_hash": "sha256:evidence",
        "candidates": [{"parameter_candidate_id": "candidate_001", "validation_metrics": {"net_excess_return": 1.0}}],
    }
    best = {
        **report["candidates"][0],
        "deployment_tier": "paper_candidate",
        "statistical_validation_required": True,
        "statistical_validation_contract": report["statistical_validation_contract"],
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": "sha256:metric",
        "metric_value_count": 1,
        "missing_metric_count": 0,
        "statistical_evidence_hash": "sha256:evidence",
    }

    reasons = _promotion_blocking_reasons(
        best=best,
        statistical_required=True,
        statistical_evidence=evidence,
        report=report,
    )

    assert "statistical_evidence_grade_insufficient" in reasons


def test_research_report_cli_summary_prints_promotion_grade_unavailable(capsys) -> None:
    report = {
        "experiment_id": "exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "candidate_count": 1,
        "best_candidate_id": "candidate_001",
        "gate_result": "FAIL",
        "promotion_eligibility_gate_result": "FAIL",
        "promotion_blocking_reasons": ["statistical_evidence_grade_insufficient"],
        "statistical_validation_required": True,
        "evidence_grade": "SCREENING_SUMMARY_BOOTSTRAP",
        "statistical_method": "summary_metric_centered_max_bootstrap",
        "official_promotion_grade_wrc_generation_available": False,
        "promotion_grade_limitations": ["promotion_grade_statistical_generation_unavailable"],
        "warnings": ["promotion_grade_statistical_generation_unavailable"],
        "candidates": [
            {
                "parameter_candidate_id": "candidate_001",
                "acceptance_gate_result": "PASS",
                "gate_fail_reasons": [],
            }
        ],
    }

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "  official_promotion_grade_wrc_generation_available=0" in output
    assert "  promotion_grade_limitations=promotion_grade_statistical_generation_unavailable" in output
    assert "  warnings=promotion_grade_statistical_generation_unavailable" in output


def test_research_backtest_fails_candidate_when_calibration_market_mismatches(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": 50,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 50,
            "median_slippage_vs_signal_bps": 1.0,
            "p90_slippage_vs_signal_bps": 2.0,
            "p95_slippage_vs_signal_bps": 3.0,
            "p95_submit_to_fill_ms": 0,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-ETH",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_market_mismatch" in report["candidates"][0]["gate_fail_reasons"]


def test_research_backtest_candidate_gate_receives_execution_fill_quality_failures(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": 0.0,
        "slippage_bps": 50,
        "latency_ms": 500,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 20,
            "median_slippage_vs_signal_bps": 1.0,
            "p90_slippage_vs_signal_bps": 2.0,
            "p95_slippage_vs_signal_bps": 3.0,
            "p95_submit_to_fill_ms": 100,
            "partial_fill_rate": 0.01,
            "unfilled_rate": 0.02,
            "model_breach_rate": 0.0,
            "quality_gate_status": "FAIL",
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    reasons = report["candidates"][0]["gate_fail_reasons"]
    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_partial_fill_rate_exceeds_assumption" in reasons
    assert "execution_calibration_unfilled_rate_exceeds_assumption" in reasons
    assert "execution_calibration_sample_count_below_required" in reasons
    assert "execution_calibration_quality_gate_not_passed" in reasons


def test_research_backtest_aggregates_scenarios_and_promotion_refuses_failed_stress(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "scenario_aggregation_integration"
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [0.0],
        "order_failure_rate": [0.0, 1.0],
        "seed": 42,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["candidate_count"] == 1
    candidate = report["candidates"][0]
    assert candidate["scenario_policy"] == "must_pass_base_and_survive_stress"
    assert len(candidate["scenario_results"]) == 2
    assert [result["scenario_role"] for result in candidate["scenario_results"]] == ["base", "stress"]
    assert [result["scenario_role_source"] for result in candidate["scenario_results"]] == ["derived", "derived"]
    assert candidate["required_scenario_count"] == 2
    assert len(candidate["required_scenario_ids"]) == 2
    assert candidate["acceptance_gate_result"] == "FAIL"
    assert candidate["scenario_fail_count"] > 0
    assert report["gate_result"] == "FAIL"
    assert "scenario_policy_no_passing_stress_scenario" in candidate["gate_fail_reasons"]
    assert any(
        str(reason).startswith("scenario_policy_required_scenario_failed:")
        for reason in candidate["gate_fail_reasons"]
    )
    assert candidate["candidate_profile_hash"].startswith("sha256:")
    assert Path(report["artifact_paths"]["report_path"]).exists()

    with pytest.raises(PromotionGateError, match="scenario_policy"):
        promote_candidate(
            experiment_id="scenario_aggregation_integration",
            candidate_id=candidate["parameter_candidate_id"],
            manager=manager,
        )


def test_research_backtest_promotes_candidate_when_base_and_stress_pass(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "scenario_aggregation_positive_integration"
    payload["acceptance_gate"]["max_mdd_pct"] = 99.9
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [0.0, 0.0],
        "order_failure_rate": [0.0],
        "seed": 42,
    }
    payload["execution_timing"] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["candidate_count"] == 1
    assert report["gate_result"] == "PASS"
    candidate = report["candidates"][0]
    assert candidate["acceptance_gate_result"] == "PASS"
    assert candidate["scenario_policy"] == "must_pass_base_and_survive_stress"
    assert len(candidate["scenario_results"]) == 2
    assert candidate["scenario_pass_count"] == 2
    assert candidate["scenario_fail_count"] == 0
    assert candidate["required_scenario_count"] == 2
    assert [result["scenario_role"] for result in candidate["scenario_results"]] == ["base", "stress"]
    assert [result["scenario_role_source"] for result in candidate["scenario_results"]] == ["derived", "derived"]
    assert candidate["final_holdout_present"] is True
    assert candidate["final_holdout_metrics"]["trade_count"] is not None
    assert candidate["candidate_profile_hash"].startswith("sha256:")

    with pytest.raises(PromotionGateError, match="probe_grade_pass_not_promotable"):
        promote_candidate(
            experiment_id="scenario_aggregation_positive_integration",
            candidate_id=candidate["parameter_candidate_id"],
            manager=manager,
        )


def test_stress_report_is_candidate_order_independent(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["parameter_space"] = {
        "SMA_SHORT": [2, 3],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5, 10],
        "partial_fill_rate": [0.5],
        "order_failure_rate": [0.1],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "seed": 42,
    }
    reordered = dict(payload)
    reordered["parameter_space"] = {
        "SMA_SHORT": [3, 2],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    target_params = {
        "SMA_SHORT": 2,
        "SMA_LONG": 4,
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
    }
    target_id = candidate_id(target_params, 0)

    first = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=parse_manifest(reordered),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    first_candidate = {item["parameter_candidate_id"]: item for item in first["candidates"]}[target_id]
    second_candidate = {item["parameter_candidate_id"]: item for item in second["candidates"]}[target_id]
    for first_scenario, second_scenario in zip(
        first_candidate["scenario_results"],
        second_candidate["scenario_results"],
        strict=True,
    ):
        assert first_scenario["scenario_id"] == second_scenario["scenario_id"]
        assert first_scenario["validation_metrics"] == second_scenario["validation_metrics"]
        assert first_scenario["validation_execution_metadata"] == second_scenario["validation_execution_metadata"]
    execution = first_candidate["scenario_results"][0]["validation_execution_metadata"][0]
    assert execution["base_seed"] == 42
    assert execution["derived_seed_hash"].startswith("sha256:")
    assert execution["seed_derivation_inputs"]["parameter_candidate_id"] == target_id


def test_different_stress_seed_changes_auditable_seed_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5, 10],
        "partial_fill_rate": [0.5],
        "order_failure_rate": [0.1],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "seed": 42,
    }
    changed_seed = dict(payload)
    changed_seed["execution_model"] = dict(payload["execution_model"])
    changed_seed["execution_model"]["seed"] = 43

    first = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=parse_manifest(changed_seed),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    first_execution = first["candidates"][0]["scenario_results"][0]["validation_execution_metadata"][0]
    second_execution = second["candidates"][0]["scenario_results"][0]["validation_execution_metadata"][0]
    assert first_execution["base_seed"] == 42
    assert second_execution["base_seed"] == 43
    assert first_execution["derived_seed_hash"] != second_execution["derived_seed_hash"]
