from __future__ import annotations
from datetime import datetime, timezone, timedelta

def kst_str(ts_ms: int) -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(kst).strftime("%Y-%m-%d %H:%M:%S %Z")

def parse_interval_sec(interval: str) -> int:
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    raise ValueError(f"지원하지 않는 INTERVAL: {interval} (예: 1m, 5m, 1h)")