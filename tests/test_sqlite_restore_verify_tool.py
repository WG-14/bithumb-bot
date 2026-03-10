from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from bithumb_bot.db_core import ensure_db


def test_restore_verify_tool_smoke(tmp_path: Path) -> None:
    db_path = tmp_path / "live.sqlite"
    conn = ensure_db(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, submit_attempt_id, exchange_order_id, status, side,
                price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "cid-1",
                "attempt-1",
                "ex-1",
                "OPEN",
                "BUY",
                1000.0,
                0.01,
                0.0,
                1,
                1,
                None,
            ),
        )
        conn.execute(
            "UPDATE bot_health SET trading_enabled=1, error_count=2, updated_ts=123 WHERE id=1"
        )
        conn.commit()
    finally:
        conn.close()

    backup_path = tmp_path / "backup.sqlite"
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    restore_dir = tmp_path / "restore"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/verify_sqlite_restore.py",
            str(backup_path),
            "--restore-dir",
            str(restore_dir),
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "[RESTORE-VERIFY] ok" in completed.stdout
    assert "orders_count=1" in completed.stdout
    assert "bot_health_count=1" in completed.stdout
    assert "trading_enabled=1" in completed.stdout
    assert "error_count=2" in completed.stdout
