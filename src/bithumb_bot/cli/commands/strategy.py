from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec, parser_error


def _strategy_plugin_inventory(args: argparse.Namespace, context) -> int:
    del args
    from bithumb_bot.strategy_plugin_inventory import strategy_plugin_inventory_json

    context.printer(strategy_plugin_inventory_json())
    return 0


def _strategy_sweep(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.config import settings
    from bithumb_bot.reporting import parse_kst_date_range_to_ts_ms

    try:
        from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
            from_date=args.from_date,
            to_date=args.to_date,
        )
        through_ts_ms = parse_kst_date_range_to_ts_ms(
            from_date=None,
            to_date=args.through_date,
        )[1]
    except ValueError:
        parser_error(args, "invalid date format for --from/--to/--through; expected YYYY-MM-DD")
    if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
        parser_error(args, "--from must be earlier than or equal to --to")
    if (
        settings.MODE == "live"
        and from_ts_ms is None
        and to_ts_ms is None
        and through_ts_ms is None
        and args.max_candles is None
        and not bool(args.allow_full_history)
    ):
        parser_error(
            args,
            "strategy-sweep in live mode requires --from/--to/--through/--max-candles or --allow-full-history",
        )
    from bithumb_bot.operator_commands import cmd_strategy_sweep

    cmd_strategy_sweep(
        short_values=args.short,
        long_values=args.long,
        entry_edge_buffer_values=args.edge_buffer,
        strategy_min_expected_edge_values=args.min_expected_edge,
        slippage_bps_values=args.slippage_bps,
        pair=args.pair,
        interval=args.interval,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
        through_ts_ms=through_ts_ms,
        max_candles=args.max_candles,
        allow_full_history=bool(args.allow_full_history),
        max_operations=args.max_operations,
        allow_large_sweep=bool(args.allow_large_sweep),
        as_json=bool(args.json),
    )


def command_specs() -> list[CommandSpec]:
    return [
        make_spec(
            "strategy-plugin-inventory",
            domain="strategy",
            handler=_strategy_plugin_inventory,
            help="print read-only strategy plugin discovery inventory as deterministic JSON",
            description=(
                "Read-only strategy plugin inventory. Does not open the trading DB, "
                "contact brokers, submit orders, or write runtime artifacts."
            ),
            build=_build_strategy_plugin_inventory,
            json_output_supported=True,
        ),
        make_spec(
            "strategy-sweep",
            domain="strategy",
            handler=_strategy_sweep,
            help="run attribution-only SMA replay sweeps over local candles",
            description=(
                "Read-only attribution replay sweep over candles; emits signal/filter metrics "
                "without PnL, drawdown, fill simulation, or order actions."
            ),
            build=_build_strategy_sweep,
            json_output_supported=True,
        )
    ]


def _build_strategy_plugin_inventory(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit deterministic JSON (default)")


def _build_strategy_sweep(parser: argparse.ArgumentParser) -> None:
    from bithumb_bot.strategy_sweep import DEFAULT_STRATEGY_SWEEP_MAX_OPERATIONS

    parser.add_argument("--short", required=True, type=_parse_csv_ints("--short"))
    parser.add_argument("--long", required=True, type=_parse_csv_ints("--long"))
    parser.add_argument("--edge-buffer", required=True, type=_parse_csv_floats("--edge-buffer"))
    parser.add_argument("--min-expected-edge", required=True, type=_parse_csv_floats("--min-expected-edge"))
    parser.add_argument("--slippage-bps", required=True, type=_parse_csv_floats("--slippage-bps"))
    parser.add_argument("--pair")
    parser.add_argument("--interval")
    parser.add_argument("--from", dest="from_date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--through", dest="through_date", help="KST date (YYYY-MM-DD)")
    parser.add_argument("--max-candles", type=_positive_int_arg)
    parser.add_argument("--allow-full-history", action="store_true")
    parser.add_argument("--max-operations", type=_positive_int_arg, default=DEFAULT_STRATEGY_SWEEP_MAX_OPERATIONS)
    parser.add_argument("--allow-large-sweep", action="store_true")
    parser.add_argument("--json", action="store_true")


def _parse_csv_values(value: str, *, option_name: str, parser):
    parts = [part.strip() for part in str(value).split(",") if part.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(f"{option_name} requires a non-empty comma-separated list")
    parsed = []
    for part in parts:
        try:
            parsed.append(parser(part))
        except (TypeError, ValueError) as exc:
            raise argparse.ArgumentTypeError(f"{option_name} contains invalid value: {part}") from exc
    return tuple(parsed)


def _parse_csv_ints(option_name: str):
    return lambda value: _parse_csv_values(value, option_name=option_name, parser=int)


def _parse_csv_floats(option_name: str):
    return lambda value: _parse_csv_values(value, option_name=option_name, parser=float)


def _positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--max-candles must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--max-candles must be a positive integer")
    return parsed
