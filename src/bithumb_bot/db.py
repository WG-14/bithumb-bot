# src/bithumb_bot/db.py
from __future__ import annotations

import sqlite3

from .config import prepare_db_path_for_connection, settings
from .sqlite_resilience import configure_connection


def connect(db_path: str | None = None) -> sqlite3.Connection:
    """
    Single place to open sqlite connection with sane defaults.
    """
    path = prepare_db_path_for_connection(db_path or settings.DB_PATH, mode=settings.MODE)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    # Recommended pragmas for trading logs (still safe for sqlite)
    try:
        configure_connection(conn)
    except Exception:
        pass

    return conn
