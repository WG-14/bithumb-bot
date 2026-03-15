# src/bithumb_bot/db.py
from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import resolve_db_path, settings
from .sqlite_resilience import configure_connection


def connect(db_path: str | None = None) -> sqlite3.Connection:
    """
    Single place to open sqlite connection with sane defaults.
    """
    path = resolve_db_path(db_path or settings.DB_PATH)
    # ensure parent directory exists (if path is a file path)
    try:
        p = Path(path)
        if p.parent and str(p.parent) not in (".", ""):
            p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # ignore for special paths like ":memory:"
        pass

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    # Recommended pragmas for trading logs (still safe for sqlite)
    try:
        configure_connection(conn)
    except Exception:
        pass

    return conn
