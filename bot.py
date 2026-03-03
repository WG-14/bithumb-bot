import os
import sys
from pathlib import Path

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

def main():
    # Local convenience: load .env automatically when present.
    # In server/production, prefer injecting environment variables externally.
    if load_dotenv and Path(".env").exists():
        load_dotenv()

    argv = sys.argv[:]

    # map a few legacy flags to env so main.py can read them if needed
    mode = _pop_flag_value(argv, "--mode")
    if mode:
        os.environ["MODE"] = mode

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
