import sqlite3
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.broker.paper import paper_execute

# 최근 캔들 한 개를 가져와서 강제로 BUY/SELL 한번씩 실행
conn = ensure_db()
row = conn.execute(
    "SELECT ts, close FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
    (settings.PAIR, settings.INTERVAL),
).fetchone()
conn.close()

if row is None:
    raise SystemExit("No candles. 먼저 캔들을 쌓아야 함: uv run python bot.py sync")

ts = int(row["ts"]) if isinstance(row, sqlite3.Row) else int(row[0])
price = float(row["close"]) if isinstance(row, sqlite3.Row) else float(row[1])

print("BUY  ->", paper_execute("BUY", ts, price))
print("SELL ->", paper_execute("SELL", ts, price))

conn = sqlite3.connect(settings.DB_PATH)
orders = conn.execute("select count(*) from orders").fetchone()[0]
fills  = conn.execute("select count(*) from fills").fetchone()[0]
print("orders", orders, "fills", fills)
conn.close()
