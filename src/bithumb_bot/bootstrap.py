from __future__ import annotations

import importlib
import importlib.util
import os
import sys

# legacy flags that may appear in old commands
FLAGS_WITH_VALUE = {
    "--mode",
    "--entry",
    "--interval",
    "--every",
    "--cooldown",
    "--cooldown-bars",
    "--cooldown_bars",
    "--min-gap",
    "--min_gap",
}


def _pop_flag_value(argv: list[str], flag: str) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            v = argv[i + 1]
            del argv[i : i + 2]
            return v
        del argv[i]
    return None


def _strip_legacy_flags(argv: list[str]) -> list[str]:
    i = 1
    out = [argv[0]]
    while i < len(argv):
        a = argv[i]
        if a in FLAGS_WITH_VALUE:
            # drop flag and its value (if present)
            i += 2
            continue
        out.append(a)
        i += 1
    return out


def resolve_explicit_env_file(mode: str | None) -> str | None:
    explicit_env_file = os.getenv("BITHUMB_ENV_FILE")
    if explicit_env_file:
        return explicit_env_file

    normalized_mode = (mode or os.getenv("MODE") or "").strip().lower()
    if normalized_mode == "live":
        return os.getenv("BITHUMB_ENV_FILE_LIVE")
    if normalized_mode in {"paper", "test"}:
        return os.getenv("BITHUMB_ENV_FILE_PAPER")
    return None


def _load_dotenv(dotenv_path: str) -> None:
    if importlib.util.find_spec("dotenv") is None:
        return
    dotenv_module = importlib.import_module("dotenv")
    dotenv_module.load_dotenv(dotenv_path=dotenv_path)


def load_explicit_env_file(mode: str | None) -> None:
    env_file = resolve_explicit_env_file(mode)
    if env_file:
        _load_dotenv(env_file)


def bootstrap_argv(argv: list[str]) -> list[str]:
    mode = _pop_flag_value(argv, "--mode")
    if mode:
        os.environ["MODE"] = mode

    load_explicit_env_file(mode)

    interval = _pop_flag_value(argv, "--interval")
    if interval:
        os.environ["INTERVAL"] = interval

    entry = _pop_flag_value(argv, "--entry")
    if entry:
        os.environ["ENTRY_MODE"] = entry

    every = _pop_flag_value(argv, "--every")
    if every:
        os.environ["EVERY"] = every

    return _strip_legacy_flags(argv)


def run_cli() -> None:
    sys.argv = bootstrap_argv(sys.argv[:])

    from .observability import configure_runtime_logging
    from .cli import main as cli_main

    configure_runtime_logging()
    cli_main()
