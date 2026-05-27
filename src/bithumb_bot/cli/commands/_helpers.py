from __future__ import annotations

import argparse
from collections.abc import Callable
from bithumb_bot.cli.context import AppContext
from bithumb_bot.cli.registry import CommandSpec


ParserBuilder = Callable[[argparse.ArgumentParser], None]
CommandHandler = Callable[[argparse.Namespace, AppContext], int | None]


def add_parser(
    subparsers: argparse._SubParsersAction,
    name: str,
    *,
    help: str | None = None,
    description: str | None = None,
    build: ParserBuilder | None = None,
) -> None:
    parser = subparsers.add_parser(name, help=help, description=description)
    if build is not None:
        build(parser)


def make_spec(
    name: str,
    *,
    domain: str,
    handler: CommandHandler,
    build: ParserBuilder | None = None,
    help: str | None = None,
    description: str | None = None,
    read_only: bool = True,
    mutating: bool = False,
    requires_live: bool = False,
    guard_policy: str | None = None,
    requires_confirmation: bool = False,
    writes_db: bool = False,
    uses_broker: bool = False,
    produces_artifact: bool = False,
    json_output_supported: bool = False,
) -> CommandSpec:
    return CommandSpec(
        name=name,
        domain=domain,
        handler=handler,
        register_parser=lambda subparsers: add_parser(
            subparsers,
            name,
            help=help,
            description=description,
            build=build,
        ),
        read_only=read_only,
        mutating=mutating,
        requires_live=requires_live,
        guard_policy=guard_policy,
        requires_confirmation=requires_confirmation,
        writes_db=writes_db,
        uses_broker=uses_broker,
        produces_artifact=produces_artifact,
        json_output_supported=json_output_supported,
    )


def parser_error(args: argparse.Namespace, message: str) -> None:
    parser = argparse.ArgumentParser(prog=f"bithumb-bot {getattr(args, 'cmd', '')}".rstrip())
    parser.error(message)
