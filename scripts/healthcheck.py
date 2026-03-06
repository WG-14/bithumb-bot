#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

from bithumb_bot.engine import get_health_status
from bithumb_bot.notifier import notify


def main() -> int:
    stale_threshold_sec = float(os.getenv("HEALTH_MAX_CANDLE_AGE_SEC", "180"))
    error_threshold = int(os.getenv("HEALTH_MAX_ERROR_COUNT", "3"))

    health = get_health_status()
    problems: list[str] = []

    age = health.get("last_candle_age_sec")
    if age is not None and float(age) > stale_threshold_sec:
        problems.append(f"stale candles: age={float(age):.1f}s > {stale_threshold_sec:.1f}s")

    error_count = int(health.get("error_count", 0))
    if error_count > error_threshold:
        problems.append(f"error_count={error_count} > {error_threshold}")

    trading_enabled = bool(health.get("trading_enabled", True))
    if not trading_enabled:
        retry_at = health.get("retry_at_epoch_sec")
        problems.append(f"trading disabled (retry_at={retry_at})")

    if problems:
        notify("healthcheck failed: " + "; ".join(problems))
        print("[HEALTHCHECK] FAIL", "; ".join(problems))
        return 1

    print("[HEALTHCHECK] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
