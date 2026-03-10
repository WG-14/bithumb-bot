#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sqlite3
import tempfile
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore a SQLite backup into a temp location and verify key reads."
    )
    parser.add_argument("backup", help="Path to backup SQLite file")
    parser.add_argument(
        "--restore-dir",
        default=None,
        help="Optional directory to place restored DB (default: temporary directory)",
    )
    return parser.parse_args()


def _verify_restored_db(restored_path: Path) -> dict[str, int | float]:
    conn = sqlite3.connect(restored_path)
    conn.row_factory = sqlite3.Row
    try:
        orders_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM orders").fetchone()["cnt"])

        health_row = conn.execute(
            """
            SELECT trading_enabled, error_count, updated_ts
            FROM bot_health
            WHERE id=1
            """
        ).fetchone()
        if health_row is None:
            raise RuntimeError("bot_health row (id=1) missing")

        health_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM bot_health").fetchone()["cnt"])
        return {
            "orders_count": orders_count,
            "bot_health_count": health_count,
            "trading_enabled": int(health_row["trading_enabled"]),
            "error_count": int(health_row["error_count"]),
        }
    finally:
        conn.close()


def run_verification(backup_path: Path, restore_dir: Path | None = None) -> tuple[Path, dict[str, int | float]]:
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"backup file not found: {backup_path}")

    if restore_dir is None:
        restore_dir_path = Path(tempfile.mkdtemp(prefix="sqlite-restore-verify-"))
    else:
        restore_dir_path = restore_dir
        restore_dir_path.mkdir(parents=True, exist_ok=True)

    restored_path = restore_dir_path / backup_path.name
    shutil.copy2(backup_path, restored_path)
    result = _verify_restored_db(restored_path)
    return restored_path, result


def main() -> int:
    args = _parse_args()
    backup_path = Path(args.backup)
    restore_dir = Path(args.restore_dir) if args.restore_dir else None

    restored_path, result = run_verification(backup_path=backup_path, restore_dir=restore_dir)

    print(f"[RESTORE-VERIFY] restored_to={restored_path}")
    print(
        "[RESTORE-VERIFY] "
        f"orders_count={result['orders_count']} "
        f"bot_health_count={result['bot_health_count']} "
        f"trading_enabled={result['trading_enabled']} "
        f"error_count={result['error_count']}"
    )
    print("[RESTORE-VERIFY] ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
