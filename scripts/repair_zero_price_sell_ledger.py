#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bithumb_bot.repair_zero_price_sell_ledger import run_repair


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-time live DB repair for historical zero-price SELL fill contamination. "
            "Default is dry-run. Use --apply for commit."
        )
    )
    parser.add_argument("--db-path", default=None, help="SQLite DB path (default: DB_PATH from environment/config)")
    parser.add_argument("--apply", action="store_true", help="Apply changes (without this flag, dry-run only)")
    parser.add_argument("--backup-path", default=None, help="Path to a verified DB backup file")
    parser.add_argument(
        "--allow-no-backup",
        action="store_true",
        help="Allow --apply even when backup check is not satisfied (not recommended)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    return run_repair(
        db_path=args.db_path,
        apply=bool(args.apply),
        backup_path=args.backup_path,
        allow_no_backup=bool(args.allow_no_backup),
    )


if __name__ == "__main__":
    raise SystemExit(main())
