import argparse
import sqlite3
from pathlib import Path

from bithumb_bot.broker.paper import paper_execute
from bithumb_bot.config import PROJECT_ROOT, settings


def _resolve_db_path(db_path_arg: str | None) -> Path:
    candidate = (db_path_arg or settings.DB_PATH or "").strip()
    if not candidate:
        raise SystemExit("DB path is required. Use --db-path or DB_PATH env.")

    db_path = Path(candidate).expanduser()
    if not db_path.is_absolute():
        raise SystemExit(
            "DB path must be absolute. Inject an absolute DB_PATH managed by PathManager/runtime env."
        )

    resolved = db_path.resolve()
    if PROJECT_ROOT.resolve() in resolved.parents:
        raise SystemExit(
            "Refusing repo-local DB path for smoke/manual run. "
            "Use runtime root outside repository or a test framework temp directory."
        )
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run one BUY/SELL paper smoke cycle against an existing external SQLite DB. "
            "Artifact class: data/<mode>/trades (runtime ledger), never repo-local."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Absolute SQLite path (default: DB_PATH env/settings)",
    )
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path)
    if not db_path.exists():
        raise SystemExit(
            f"DB file not found: {db_path}. Create/sync runtime DB first (outside repository)."
        )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT ts, close FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
            (settings.PAIR, settings.INTERVAL),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        raise SystemExit("No candles. 먼저 캔들을 쌓아야 함: uv run bithumb-bot sync")

    ts = int(row["ts"])
    price = float(row["close"])

    print("BUY  ->", paper_execute("BUY", ts, price))
    print("SELL ->", paper_execute("SELL", ts, price))

    conn = sqlite3.connect(db_path)
    try:
        orders = conn.execute("select count(*) from orders").fetchone()[0]
        fills = conn.execute("select count(*) from fills").fetchone()[0]
    finally:
        conn.close()
    print("orders", orders, "fills", fills)


if __name__ == "__main__":
    main()
