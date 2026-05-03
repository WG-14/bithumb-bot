from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Deprecated compatibility wrapper. Use "
            "`uv run bithumb-bot research-backtest --manifest <path>`."
        )
    )
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)

    from bithumb_bot.app import main as app_main

    print(
        "[BACKTEST2-DEPRECATED] backtest2.py no longer loads repo-root .env or runs "
        "standalone backtests; delegating to canonical research-backtest."
    )
    return app_main(["research-backtest", "--manifest", str(args.manifest)])


if __name__ == "__main__":
    raise SystemExit(main())
