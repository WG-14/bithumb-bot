from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec, parser_error


def _report(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_report

    cmd_report(max(1, int(args.days)))


def _ops(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import cmd_ops_report

    cmd_ops_report(limit=max(1, int(args.limit)))


def _risk(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import cmd_risk_report

    cmd_risk_report(limit=max(1, int(args.limit)), as_json=bool(args.json))


def _fee_diagnostics(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import cmd_fee_diagnostics

    cmd_fee_diagnostics(
        fill_limit=max(1, int(args.fill_limit)),
        roundtrip_limit=max(1, int(args.roundtrip_limit)),
        estimated_fee_rate=args.estimated_fee_rate,
        as_json=bool(args.json),
    )


def _cash_drift(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import cmd_cash_drift_report

    cmd_cash_drift_report(recent_limit=max(1, int(args.recent_limit)), as_json=bool(args.json))


def _decision_telemetry(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import cmd_decision_telemetry

    cmd_decision_telemetry(limit=max(1, int(args.limit)))


def _execution_quality(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_execution_quality_report

    cmd_execution_quality_report(
        limit=max(1, int(args.limit)),
        since=args.since,
        market=args.market,
        mode=args.mode,
        compare_manifest=args.compare_manifest,
        output_format=str(args.format),
        group_by=args.by,
        write_calibration=bool(args.write_calibration),
    )


def _strategy_report(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import parse_kst_date_range_to_ts_ms

    try:
        from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except ValueError:
        parser_error(args, "invalid date format for --from-date/--to-date; expected YYYY-MM-DD")
    if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
        parser_error(args, "--from-date must be earlier than or equal to --to-date")
    group_by = tuple(part.strip() for part in str(args.group_by or "").split(",") if part.strip())
    from bithumb_bot.reporting import cmd_strategy_report

    cmd_strategy_report(
        strategy_name=args.strategy_name,
        exit_rule_name=args.exit_rule_name,
        pair=args.pair,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
        group_by=group_by,
        observation_window_bars=max(1, int(args.observation_window_bars)),
        min_observation_sample=max(1, int(args.min_observation_sample)),
        as_json=bool(args.json),
    )


def _experiment_report(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.reporting import parse_kst_date_range_to_ts_ms

    try:
        from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except ValueError:
        parser_error(args, "invalid date format for --from-date/--to-date; expected YYYY-MM-DD")
    if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
        parser_error(args, "--from-date must be earlier than or equal to --to-date")
    from bithumb_bot.reporting import cmd_experiment_report

    cmd_experiment_report(
        strategy_name=args.strategy_name,
        pair=args.pair,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
        sample_threshold=max(1, int(args.sample_threshold)),
        top_n=max(1, int(args.top_n)),
        concentration_warn_threshold=max(0.0, float(args.concentration_threshold)),
        regime_skew_warn_threshold=max(0.0, float(args.regime_skew_threshold)),
        regime_pnl_skew_warn_threshold=max(0.0, float(args.regime_pnl_skew_threshold)),
        as_json=bool(args.json),
    )


def _decision_attribution(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.config import settings
    from bithumb_bot.decision_attribution import (
        build_decision_attribution_summary_from_db,
        decision_attribution_summary_json,
        format_decision_attribution_summary,
    )
    from bithumb_bot.reporting import parse_kst_date_range_to_ts_ms

    try:
        from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except ValueError:
        parser_error(args, "invalid date format for --from/--to; expected YYYY-MM-DD")
    if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
        parser_error(args, "--from must be earlier than or equal to --to")
    conn = None
    try:
        conn = sqlite3.connect(f"file:{Path(settings.DB_PATH).absolute()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        summary = build_decision_attribution_summary_from_db(
            conn,
            limit=max(1, int(args.limit)),
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            pair=args.pair,
            interval=args.interval,
        )
    except sqlite3.OperationalError:
        empty_conn = sqlite3.connect(":memory:")
        try:
            summary = build_decision_attribution_summary_from_db(
                empty_conn,
                limit=max(1, int(args.limit)),
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
                pair=args.pair,
                interval=args.interval,
            )
        finally:
            empty_conn.close()
    finally:
        if conn is not None:
            conn.close()
    if bool(args.json):
        print(decision_attribution_summary_json(summary))
    else:
        print(format_decision_attribution_summary(summary))


def command_specs() -> list[CommandSpec]:
    return [
        make_spec("report", domain="reports", handler=_report, build=lambda p: p.add_argument("--days", type=int, default=30)),
        make_spec("ops-report", domain="reports", handler=_ops, help="operator observability report", build=lambda p: p.add_argument("--limit", type=int, default=20)),
        make_spec("risk-report", domain="reports", handler=_risk, help="show daily-loss baseline and recent risk evaluations", build=_build_limit_json(20), json_output_supported=True),
        make_spec("fee-diagnostics", domain="reports", handler=_fee_diagnostics, help="validate real fee application against recent fills/roundtrips", build=_build_fee_diagnostics, json_output_supported=True),
        make_spec("strategy-report", domain="reports", handler=_strategy_report, help="strategy performance comparison report", description="Aggregate trade_lifecycles by strategy/exit-rule/date range for experiments.", build=_build_strategy_report, produces_artifact=True, json_output_supported=True),
        make_spec("experiment-report", domain="reports", handler=_experiment_report, help="expectancy validation report for small live experiments", description="Report realized PnL/sample distribution/time-regime bias for experiment interpretation.", build=_build_experiment_report, produces_artifact=True, json_output_supported=True),
        make_spec("cash-drift-report", domain="reports", handler=_cash_drift, help="audit broker cash versus local ledger and recent external cash adjustments", description="Read-only cash drift diagnostic for broker/local comparison and adjustment review.", build=_build_cash_drift, json_output_supported=True),
        make_spec("decision-telemetry", domain="reports", handler=_decision_telemetry, help="summary of HOLD/blocked decision telemetry", build=lambda p: p.add_argument("--limit", type=int, default=200)),
        make_spec("decision-attribution", domain="reports", handler=_decision_attribution, help="funnel-oriented attribution summary from stored strategy decision context", description="Read-only decision attribution report for strategy_decisions.context_json.", build=_build_decision_attribution, json_output_supported=True),
        make_spec("execution-quality-report", domain="reports", handler=_execution_quality, help="report signal-submit-fill execution quality against research cost assumptions", description="Materialize and summarize order-level execution quality from strategy decisions, submit evidence, and fills without changing live trading behavior.", build=_build_execution_quality, produces_artifact=True, json_output_supported=True),
    ]


def _build_limit_json(default: int):
    def _build(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--limit", type=int, default=default)
        parser.add_argument("--json", action="store_true")

    return _build


def _build_fee_diagnostics(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fill-limit", type=int, default=100)
    parser.add_argument("--roundtrip-limit", type=int, default=50)
    parser.add_argument("--estimated-fee-rate", type=float, default=None, help="expected fee rate (default: FEE_RATE setting)")
    parser.add_argument("--json", action="store_true")


def _build_strategy_report(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--strategy-name")
    parser.add_argument("--exit-rule-name")
    parser.add_argument("--pair")
    parser.add_argument("--from-date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--group-by", default="strategy_name,exit_rule_name", help="comma-separated axes: strategy_name,exit_rule_name,pair")
    parser.add_argument("--observation-window-bars", type=int, default=5)
    parser.add_argument("--min-observation-sample", type=int, default=10)
    parser.add_argument("--json", action="store_true")


def _build_experiment_report(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--strategy-name")
    parser.add_argument("--pair")
    parser.add_argument("--from-date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--sample-threshold", type=int, default=30)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--concentration-threshold", type=float, default=0.6)
    parser.add_argument("--regime-skew-threshold", type=float, default=0.7)
    parser.add_argument("--regime-pnl-skew-threshold", type=float, default=0.7)
    parser.add_argument("--json", action="store_true")


def _build_cash_drift(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--recent-limit", type=int, default=5)
    parser.add_argument("--json", action="store_true")


def _build_decision_attribution(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--from", dest="from_date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--pair")
    parser.add_argument("--interval")
    parser.add_argument("--json", action="store_true")


def _build_execution_quality(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--since")
    parser.add_argument("--market")
    parser.add_argument("--mode")
    parser.add_argument("--by", choices=("order_type",))
    parser.add_argument("--compare-manifest")
    parser.add_argument("--write-calibration", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
