from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from dataclasses import dataclass

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


@dataclass(frozen=True)
class ExplicitEnvLoadSummary:
    mode: str | None
    env_file: str | None
    source_key: str | None
    loaded: bool
    exists: bool
    override: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "env_file": self.env_file,
            "source_key": self.source_key,
            "loaded": self.loaded,
            "exists": self.exists,
            "override": self.override,
        }


_LAST_ENV_LOAD_SUMMARY = ExplicitEnvLoadSummary(
    mode=None,
    env_file=None,
    source_key=None,
    loaded=False,
    exists=False,
    override=False,
)


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
    return describe_explicit_env_file(mode).env_file


def describe_explicit_env_file(mode: str | None) -> ExplicitEnvLoadSummary:
    explicit_env_file = os.getenv("BITHUMB_ENV_FILE")
    if explicit_env_file:
        return ExplicitEnvLoadSummary(
            mode=(mode or os.getenv("MODE") or None),
            env_file=explicit_env_file,
            source_key="BITHUMB_ENV_FILE",
            loaded=False,
            exists=os.path.isfile(explicit_env_file),
            override=False,
        )

    normalized_mode = (mode or os.getenv("MODE") or "").strip().lower()
    if normalized_mode == "live":
        env_file = os.getenv("BITHUMB_ENV_FILE_LIVE")
        return ExplicitEnvLoadSummary(
            mode=normalized_mode,
            env_file=env_file,
            source_key="BITHUMB_ENV_FILE_LIVE" if env_file else None,
            loaded=False,
            exists=bool(env_file and os.path.isfile(env_file)),
            override=False,
        )
    if normalized_mode in {"paper", "test"}:
        env_file = os.getenv("BITHUMB_ENV_FILE_PAPER")
        return ExplicitEnvLoadSummary(
            mode=normalized_mode,
            env_file=env_file,
            source_key="BITHUMB_ENV_FILE_PAPER" if env_file else None,
            loaded=False,
            exists=bool(env_file and os.path.isfile(env_file)),
            override=False,
        )
    return ExplicitEnvLoadSummary(
        mode=normalized_mode or None,
        env_file=None,
        source_key=None,
        loaded=False,
        exists=False,
        override=False,
    )


def _load_dotenv(dotenv_path: str) -> None:
    if importlib.util.find_spec("dotenv") is None:
        return
    dotenv_module = importlib.import_module("dotenv")
    dotenv_module.load_dotenv(dotenv_path=dotenv_path)


def load_explicit_env_file(mode: str | None) -> None:
    global _LAST_ENV_LOAD_SUMMARY
    summary = describe_explicit_env_file(mode)
    env_file = summary.env_file
    if env_file:
        _load_dotenv(env_file)
        _LAST_ENV_LOAD_SUMMARY = ExplicitEnvLoadSummary(
            mode=summary.mode,
            env_file=summary.env_file,
            source_key=summary.source_key,
            loaded=True,
            exists=summary.exists,
            override=False,
        )
        return
    _LAST_ENV_LOAD_SUMMARY = summary


def get_last_explicit_env_load_summary() -> ExplicitEnvLoadSummary:
    return _LAST_ENV_LOAD_SUMMARY


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
