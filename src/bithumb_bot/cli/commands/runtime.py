from __future__ import annotations

import argparse
import json

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


def _with_limit(function_name: str, attr: str = "limit", *, minimum: int | None = None):
    def _handler(args: argparse.Namespace, _context) -> int | None:
        from bithumb_bot import operator_commands

        value = int(getattr(args, attr))
        if minimum is not None:
            value = max(minimum, value)
        return getattr(operator_commands, function_name)(value)

    return _handler


def _signal(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_signal

    cmd_signal(args.short, args.long)


def _explain(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_explain

    cmd_explain(args.short, args.long)


def _audit(_args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_audit

    cmd_audit()


def _config_dump(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_config_dump

    cmd_config_dump(masked=bool(args.masked))


def _notification_diagnose(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.notification_diagnostics import cmd_notification_diagnose

    return int(
        cmd_notification_diagnose(
            as_json=bool(args.json),
            probe=bool(args.probe),
            policy=str(args.notification_policy) if args.notification_policy else None,
        )
    )


def _validate_db(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.operator_commands import cmd_validate_db

    return int(cmd_validate_db(as_json=bool(args.json)))


def _run(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_run

    del args
    cmd_run()


def _live_dry_run(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_live_dry_run

    del args
    cmd_live_dry_run()


def _runtime_strategy_set_lint(_args: argparse.Namespace, context) -> int:
    from bithumb_bot.config import validate_runtime_strategy_set_selection
    from bithumb_bot.runtime_strategy_set import normalized_runtime_strategy_set_manifest

    validate_runtime_strategy_set_selection(context.settings)
    manifest = normalized_runtime_strategy_set_manifest(settings_obj=context.settings)
    context.printer(
        "runtime_strategy_set_lint_ok "
        f"runtime_scope={manifest['runtime_scope']!r} "
        f"manifest_hash={manifest['runtime_strategy_set_manifest_hash']} "
        f"active_strategy_count={manifest['active_strategy_count']} "
        f"source={manifest['source']}"
    )
    return 0


def _runtime_strategy_set_dump(args: argparse.Namespace, context) -> int:
    from bithumb_bot.config import validate_runtime_strategy_set_selection
    from bithumb_bot.runtime_strategy_set import normalized_runtime_strategy_set_manifest

    validate_runtime_strategy_set_selection(context.settings)
    manifest = normalized_runtime_strategy_set_manifest(settings_obj=context.settings)
    context.printer(json.dumps(manifest, indent=2 if args.pretty else None, sort_keys=True))
    return 0


def _build_window_parser(parser: argparse.ArgumentParser) -> None:
    from bithumb_bot.strategy_config import _sma_int

    parser.add_argument("--short", type=int, default=_sma_int("SMA_SHORT"))
    parser.add_argument("--long", type=int, default=_sma_int("SMA_LONG"))


def _limit(default: int):
    def _build(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--limit", type=int, default=default)

    return _build


def command_specs() -> list[CommandSpec]:
    return [
        make_spec("signal", domain="runtime", handler=_signal, build=_build_window_parser),
        make_spec("explain", domain="runtime", handler=_explain, build=_build_window_parser),
        make_spec("status", domain="runtime", handler=_simple("cmd_status")),
        make_spec(
            "health",
            domain="runtime",
            handler=_simple("cmd_health"),
            help="show health summary (staleness/errors/trading state/recovery)",
            description="Show health summary for limited unattended operation checks.",
        ),
        make_spec("audit", domain="runtime", handler=_audit),
        make_spec("check", domain="runtime", handler=_audit),
        make_spec("audit-ledger", domain="runtime", handler=_simple("cmd_audit_ledger")),
        make_spec(
            "validate-db",
            domain="runtime",
            handler=_validate_db,
            help="validate operational DB schema without applying repair",
            build=lambda p: p.add_argument("--json", action="store_true"),
            json_output_supported=True,
        ),
        make_spec(
            "config-dump",
            domain="runtime",
            handler=_config_dump,
            help="show bootstrap-loaded effective config for operator validation",
            description="Print selected effective settings; use --masked for normal operator use.",
            build=lambda p: p.add_argument("--masked", action="store_true"),
        ),
        make_spec(
            "notification-diagnose",
            domain="runtime",
            handler=_notification_diagnose,
            help="inspect notification configuration or explicitly send a probe",
            description="Print masked notification configuration; --probe explicitly attempts delivery.",
            build=_build_notification_diagnose,
            json_output_supported=True,
        ),
        make_spec("orders", domain="runtime", handler=_with_limit("cmd_orders"), build=_limit(50)),
        make_spec("fills", domain="runtime", handler=_with_limit("cmd_fills"), build=_limit(50)),
        make_spec("trades", domain="runtime", handler=_with_limit("cmd_trades"), build=_limit(20)),
        make_spec(
            "run",
            domain="runtime",
            handler=_run,
            read_only=False,
            mutating=True,
            guard_policy="live_run_loop",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "live-dry-run",
            domain="runtime",
            handler=_live_dry_run,
            help="run one live no-submit decision cycle",
            description="Validate live decision flow, target_delta plan, and performance gate without broker submission.",
            read_only=False,
            mutating=True,
            guard_policy="live_dry_run_loop",
            writes_db=True,
            uses_broker=True,
        ),
        make_spec(
            "runtime-strategy-set-lint",
            domain="runtime",
            handler=_runtime_strategy_set_lint,
            help="validate the active runtime strategy set without placing orders",
            description="Validate and materialize the active runtime strategy set using startup validation.",
        ),
        make_spec(
            "runtime-strategy-set-dump",
            domain="runtime",
            handler=_runtime_strategy_set_dump,
            help="print the normalized active runtime strategy-set manifest",
            description="Validate and print the materialized active runtime strategy set without placing orders.",
            build=lambda p: p.add_argument("--pretty", action="store_true"),
            json_output_supported=True,
        ),
    ]


def _build_notification_diagnose(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--probe", action="store_true")
    parser.add_argument(
        "--notification-policy",
        choices=("best_effort", "require_delivery", "disabled"),
        help="policy label to include in diagnostic output",
    )
