import os
import sys
try:
    from dotenv import load_dotenv
except ImportError:  # optional dependency for local convenience
    load_dotenv = None

# legacy flags that may appear in old commands
FLAGS_WITH_VALUE = {
    "--mode", "--entry", "--interval", "--every",
    "--cooldown", "--cooldown-bars", "--cooldown_bars",
    "--min-gap", "--min_gap",
}

def _pop_flag_value(argv: list[str], flag: str):
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            v = argv[i + 1]
            del argv[i:i+2]
            return v
        del argv[i]
    return None

def _strip_legacy_flags(argv: list[str]):
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


def _resolve_explicit_env_file(mode: str | None) -> str | None:
    explicit_env_file = os.getenv("BITHUMB_ENV_FILE")
    if explicit_env_file:
        return explicit_env_file

    normalized_mode = (mode or os.getenv("MODE") or "").strip().lower()
    if normalized_mode == "live":
        return os.getenv("BITHUMB_ENV_FILE_LIVE")
    if normalized_mode in {"paper", "test"}:
        return os.getenv("BITHUMB_ENV_FILE_PAPER")
    return None


def _load_explicit_env_file(mode: str | None) -> None:
    if not load_dotenv:
        return
    env_file = _resolve_explicit_env_file(mode)
    if env_file:
        load_dotenv(dotenv_path=env_file)

def main():
    argv = sys.argv[:]

    # map a few legacy flags to env so main.py can read them if needed
    mode = _pop_flag_value(argv, "--mode")
    if mode:
        os.environ["MODE"] = mode

    _load_explicit_env_file(mode)

    interval = _pop_flag_value(argv, "--interval")
    if interval:
        os.environ["INTERVAL"] = interval

    entry = _pop_flag_value(argv, "--entry")
    if entry:
        os.environ["ENTRY_MODE"] = entry

    every = _pop_flag_value(argv, "--every")
    if every:
        os.environ["EVERY"] = every

    sys.argv = _strip_legacy_flags(argv)

    from bithumb_bot.cli import main as _main
    _main()

if __name__ == "__main__":
    main()
