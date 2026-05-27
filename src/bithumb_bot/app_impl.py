"""Deprecated compatibility facade for historical ``bithumb_bot.app_impl`` imports.

Command implementations live in domain command modules. Historical imports are
resolved lazily so this module cannot become the CLI implementation sink again.
"""

from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    from . import operator_commands

    return getattr(operator_commands, name)


def main(argv: list[str] | None = None) -> int:
    from bithumb_bot.cli.main import main as cli_main

    return cli_main(argv)


legacy_main = main
