from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _settings_default(name: str):
    from bithumb_bot.config import settings

    return getattr(settings, name)


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

    cmd_flatten_position(dry_run=bool(args.dry_run))


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
            guard_policy="live_preflight",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "flatten-position",
            domain="live_ops",
            handler=_flatten,
            help="emergency flatten open position",
            description="Flatten current position for emergency exposure reduction.",
            build=lambda p: p.add_argument("--dry-run", action="store_true"),
            read_only=False,
            mutating=True,
            requires_live=True,
            guard_policy="live_preflight",
            writes_db=True,
            uses_broker=True,
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
            guard_policy="live_preflight",
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
            guard_policy="live_preflight",
            writes_db=True,
            uses_broker=True,
        ),
    ]
