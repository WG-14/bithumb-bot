from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _settings_default(name: str):
    from bithumb_bot.config import settings

    if hasattr(settings, name):
        return getattr(settings, name)
    if name in {"SMA_SHORT", "SMA_LONG"}:
        from bithumb_bot.strategy_config import _sma_int

        return _sma_int(name)
    raise AttributeError(name)


def _simple(function_name: str):
    def _handler(_args: argparse.Namespace, _context) -> int | None:
        from bithumb_bot import operator_commands

        return getattr(operator_commands, function_name)()

    return _handler


def _target_delta_dry_run(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_target_delta_dry_run

    cmd_target_delta_dry_run(args.short, args.long)


def _resume(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_resume

    cmd_resume(force=bool(args.force))


def _flatten(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_flatten_position

    cmd_flatten_position(dry_run=bool(args.dry_run), json_output=bool(args.json))


def _smoke_buy(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_smoke_buy

    cmd_smoke_buy(
        krw=float(args.krw),
        market=str(args.market),
        confirm=str(args.confirm),
        authority_path=args.authority_path,
        reference_price=args.reference_price,
    )


def _live_pipeline_smoke(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_live_pipeline_smoke

    cmd_live_pipeline_smoke(
        plan=bool(args.plan),
        apply=bool(args.apply),
        yes=bool(args.yes),
        cycles=int(args.cycles),
        max_orders=int(args.max_orders),
        max_notional_krw=float(args.max_notional_krw),
        authority_path=args.authority_path,
        confirm=args.confirm,
        json_output=bool(args.json),
    )


def _live_pipeline_smoke_authority(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_live_pipeline_smoke_authority

    cmd_live_pipeline_smoke_authority(
        out=str(args.out),
        cycles=int(args.cycles),
        max_orders=int(args.max_orders),
        max_notional_krw=float(args.max_notional_krw),
        expires_min=int(args.expires_min),
    )


def _h74_live_rehearsal(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal

    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            kst_time=str(args.kst_time),
            no_submit=bool(args.no_submit),
            source_artifact_path=args.source_artifact,
        )
    )
    if bool(args.json):
        print(json.dumps(payload, sort_keys=True))
        return
    print(payload["rehearsal_hash"])


def _h74_readiness_certificate(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
    from bithumb_bot.h74_readiness_certificate import (
        build_h74_readiness_certificate,
        validate_h74_readiness_certificate,
    )

    rehearsal = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            kst_time=str(args.kst_time),
            no_submit=bool(args.no_submit),
            source_artifact_path=args.source_artifact,
        )
    )
    payload = build_h74_readiness_certificate(
        rehearsal,
        env_file=os.getenv("BITHUMB_ENV_FILE"),
    )
    validation = validate_h74_readiness_certificate(
        payload,
        env_file=os.getenv("BITHUMB_ENV_FILE"),
        broker_balance_snapshot_hash=str(payload.get("broker_balance_snapshot_hash") or ""),
        current_commit_sha=str(payload.get("commit_sha") or ""),
        current_db_schema_hash=str(payload.get("db_schema_hash") or ""),
        current_order_rule_fee_authority_hash=str(payload.get("order_rule_fee_authority_hash") or ""),
        current_gate_trace_hash=str(payload.get("gate_trace_hash") or ""),
        current_would_submit_plan_hash=str(payload.get("would_submit_plan_hash") or ""),
        strict=True,
    )
    if not bool(validation.get("valid")):
        raise SystemExit("h74_readiness_certificate_invalid:" + ",".join(validation.get("reasons") or []))
    if bool(args.json):
        print(json.dumps(payload, sort_keys=True))
        return
    print(payload["certificate_hash"])


def _h74_long_run_preflight(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.h74_readiness_certificate import validate_h74_long_run_preflight

    with Path(str(args.certificate)).expanduser().open("r", encoding="utf-8") as handle:
        certificate = json.load(handle)
    if not isinstance(certificate, dict):
        raise SystemExit("h74_long_run_preflight_certificate_not_object")
    payload = validate_h74_long_run_preflight(certificate)
    if bool(args.json):
        print(json.dumps(payload, sort_keys=True))
    elif bool(payload.get("valid")):
        print("h74_long_run_preflight pass")
    else:
        print("h74_long_run_preflight blocked:" + ",".join(payload.get("reasons") or []))
    return 0 if bool(payload.get("valid")) else 2


def _exchange_submit_diagnose(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.config import settings
    from bithumb_bot.exchange_submit_diagnostics import diagnose_exchange_submit_reachability

    client_order_id = str(args.client_order_id)
    broker_orders: list[dict[str, object]] = []
    lookup_available = True
    lookup_error: str | None = None
    try:
        from bithumb_bot.broker.bithumb import BithumbBroker

        broker = BithumbBroker()
        broker_orders = list(
            broker.get_recent_orders(
                limit=int(args.limit),
                client_order_ids=(client_order_id,),
                exchange_order_ids=(),
            )
        )
    except Exception as exc:
        lookup_available = False
        lookup_error = f"{type(exc).__name__}: {exc}"
    db_uri = f"file:{settings.DB_PATH}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    try:
        payload = diagnose_exchange_submit_reachability(
            conn,
            client_order_id=client_order_id,
            broker_recent_orders=broker_orders,
            broker_lookup_available=lookup_available,
            broker_lookup_error=lookup_error,
        )
    finally:
        conn.close()
    if bool(args.json):
        print(json.dumps(payload, sort_keys=True))
        return
    print(payload["reason_code"])


def _panic(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_panic_stop

    cmd_panic_stop(flatten=bool(args.flatten))


def _build_window_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--short", type=int, default=_settings_default("SMA_SHORT"))
    parser.add_argument("--long", type=int, default=_settings_default("SMA_LONG"))


def command_specs() -> list[CommandSpec]:
    return [
        make_spec(
            "pause",
            domain="live_ops",
            handler=_simple("cmd_pause"),
            help="persistently pause new trading",
            description="Persistently disable trading until explicit resume.",
            read_only=False,
            mutating=True,
            requires_live=True,
            writes_db=True,
        ),
        make_spec(
            "resume",
            domain="live_ops",
            handler=_resume,
            help="resume trading if safety checks pass",
            description="Resume trading with safety gates; use --force only as a last resort.",
            build=lambda p: p.add_argument("--force", action="store_true"),
            read_only=False,
            mutating=True,
            requires_live=True,
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "reconcile",
            domain="live_ops",
            handler=_simple("cmd_reconcile"),
            help="reconcile local/exchange order state",
            description="Run order-state reconciliation with the broker.",
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_recovery",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "broker-diagnose",
            domain="live_ops",
            handler=_simple("cmd_broker_diagnose"),
            help="read-only live broker/API diagnostics",
            description="Run read-only live broker diagnostics (no order create/cancel).",
            requires_live=True,
            guard_policy="read_only_broker_diagnostic",
            uses_broker=True,
        ),
        make_spec(
            "target-delta-dry-run",
            domain="live_ops",
            handler=_target_delta_dry_run,
            help="read-only target_delta submit-plan diagnostic",
            description=(
                "Run one live target_delta decision cycle and print the target submit plan "
                "without real-order arming or order submission."
            ),
            build=_build_window_parser,
            requires_live=True,
            uses_broker=True,
        ),
        make_spec(
            "smoke-buy",
            domain="live_ops",
            handler=_smoke_buy,
            help="operator-only live BUY smoke order",
            description="Submit an explicitly confirmed operator smoke BUY through the live submit path.",
            build=lambda p: (
                p.add_argument("--krw", type=float, default=50000.0),
                p.add_argument("--market", default="KRW-BTC"),
            p.add_argument("--confirm", required=True),
                p.add_argument("--authority-path", required=True),
                p.add_argument("--reference-price", type=float),
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_execution_smoke",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "live-pipeline-smoke",
            domain="live_ops",
            handler=_live_pipeline_smoke,
            help="operator-authorized live run-loop pipeline smoke",
            description=(
                "Plan or execute a bounded 5x BUY/SELL live pipeline smoke through "
                "the automatic decision/execution path."
            ),
            build=lambda p: (
                p.add_argument("--plan", action="store_true"),
                p.add_argument("--apply", action="store_true"),
                p.add_argument("--yes", action="store_true"),
                p.add_argument("--cycles", type=int, default=5),
                p.add_argument("--max-orders", type=int, default=10),
                p.add_argument("--max-notional-krw", type=float, default=10000.0),
                p.add_argument("--authority-path"),
                p.add_argument("--confirm"),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_live_pipeline_smoke",
            requires_confirmation=True,
            writes_db=True,
            uses_broker=True,
            json_output_supported=True,
        ),
        make_spec(
            "live-pipeline-smoke-authority",
            domain="live_ops",
            handler=_live_pipeline_smoke_authority,
            help="create bounded live pipeline smoke authority artifact",
            description="Create a one-shot authority artifact for live-pipeline-smoke.",
            build=lambda p: (
                p.add_argument("--out", required=True),
                p.add_argument("--cycles", type=int, default=5),
                p.add_argument("--max-orders", type=int, default=10),
                p.add_argument("--max-notional-krw", type=float, default=10000.0),
                p.add_argument("--expires-min", type=int, default=10),
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_live_pipeline_smoke_authority",
            produces_artifact=True,
        ),
        make_spec(
            "h74-live-rehearsal",
            domain="live_ops",
            handler=_h74_live_rehearsal,
            help="rehearse normal h74 live-real path to the broker submit boundary",
            description="Run the normal h74 rehearsal with broker submit suppressed.",
            build=lambda p: (
                p.add_argument("--kst-time", default="10:00"),
                p.add_argument("--no-submit", action="store_true", default=True),
                p.add_argument("--source-artifact"),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=True,
            requires_live=True,
            uses_broker=False,
            json_output_supported=True,
        ),
        make_spec(
            "h74-readiness-certificate",
            domain="live_ops",
            handler=_h74_readiness_certificate,
            help="issue an h74 readiness certificate from normal h74 rehearsal",
            description="Issue a hash-bound h74 readiness certificate from h74-live-rehearsal.",
            build=lambda p: (
                p.add_argument("--kst-time", default="10:00"),
                p.add_argument("--no-submit", action="store_true", default=True),
                p.add_argument("--source-artifact"),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=True,
            requires_live=True,
            uses_broker=False,
            produces_artifact=True,
            json_output_supported=True,
        ),
        make_spec(
            "h74-long-run-preflight",
            domain="live_ops",
            handler=_h74_long_run_preflight,
            help="validate h74 one-week live preflight certificate",
            description=(
                "Read an h74 readiness certificate and block long-running operation unless "
                "KST10 positive and KST18 negative entry-gate coverage pass."
            ),
            build=lambda p: (
                p.add_argument("--certificate", required=True),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=True,
            requires_live=True,
            uses_broker=False,
            json_output_supported=True,
        ),
        make_spec(
            "exchange-submit-diagnose",
            domain="live_ops",
            handler=_exchange_submit_diagnose,
            help="read-only exchange submit/reject diagnosis for a local order",
            description=(
                "Compare local orders/order_events with broker recent orders without placing or "
                "canceling orders."
            ),
            build=lambda p: (
                p.add_argument("--client-order-id", required=True),
                p.add_argument("--limit", type=int, default=100),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=True,
            requires_live=True,
            guard_policy="read_only_broker_diagnostic",
            uses_broker=True,
            json_output_supported=True,
        ),
        make_spec(
            "panic-stop",
            domain="live_ops",
            handler=_panic,
            help="halt trading, cancel open orders, and optionally flatten the position",
            build=lambda p: p.add_argument(
                "--flatten",
                action="store_true",
                help="attempt an explicit flatten after open-order cancellation",
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_risk_reduction",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "flatten-position",
            domain="live_ops",
            handler=_flatten,
            help="emergency flatten open position",
            description="Flatten current position for emergency exposure reduction.",
            build=lambda p: (
                p.add_argument("--dry-run", action="store_true"),
                p.add_argument("--json", action="store_true"),
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_risk_reduction",
            writes_db=True,
            uses_broker=True,
            json_output_supported=True,
        ),
        make_spec(
            "cancel-open-orders",
            domain="live_ops",
            handler=_simple("cmd_cancel_open_orders"),
            help="cancel all remote open orders in live mode",
            description="Cancel all remote open orders (live mode only).",
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_risk_reduction",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "target-closeout",
            domain="live_ops",
            handler=_simple("cmd_target_closeout"),
            help="explicitly persist target_delta closeout target",
            description=(
                "Persist target=0 with origin=operator_closeout. The normal target_delta "
                "submit path may close the broker position only when live submit gates allow it."
            ),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="operator_risk_reduction",
            writes_db=True,
            uses_broker=True,
        ),
    ]
