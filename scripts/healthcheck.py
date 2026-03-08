#!/usr/bin/env python3
from __future__ import annotations

import os
import time

from bithumb_bot.notifier import notify
from bithumb_bot.runtime_state import refresh_open_order_health, snapshot


def main() -> int:
    stale_threshold_sec = float(os.getenv("HEALTH_MAX_CANDLE_AGE_SEC", "180"))
    error_threshold = int(os.getenv("HEALTH_MAX_ERROR_COUNT", "3"))

    reconcile_stale_threshold_sec = float(os.getenv("HEALTH_MAX_RECONCILE_AGE_SEC", "900"))
    unresolved_age_threshold_sec = float(os.getenv("HEALTH_MAX_UNRESOLVED_ORDER_AGE_SEC", "900"))

    refresh_open_order_health()
    state = snapshot()
    health = {
        "last_candle_age_sec": state.last_candle_age_sec,
        "error_count": state.error_count,
        "trading_enabled": state.trading_enabled,
        "retry_at_epoch_sec": state.retry_at_epoch_sec,
        "last_disable_reason": state.last_disable_reason,
        "unresolved_open_order_count": state.unresolved_open_order_count,
        "oldest_unresolved_order_age_sec": state.oldest_unresolved_order_age_sec,
        "recovery_required_count": state.recovery_required_count,
        "last_reconcile_epoch_sec": state.last_reconcile_epoch_sec,
        "last_reconcile_status": state.last_reconcile_status,
        "last_reconcile_error": state.last_reconcile_error,
    }

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
        disable_reason = health.get("last_disable_reason")
        problems.append(f"trading disabled (retry_at={retry_at}, reason={disable_reason})")

    recovery_required_count = int(health.get("recovery_required_count", 0))
    if recovery_required_count > 0:
        problems.append(f"recovery_required_count={recovery_required_count}")

    unresolved_count = int(health.get("unresolved_open_order_count", 0))
    oldest_unresolved_age_sec = health.get("oldest_unresolved_order_age_sec")
    if unresolved_count > 0 and oldest_unresolved_age_sec is not None:
        if float(oldest_unresolved_age_sec) > unresolved_age_threshold_sec:
            problems.append(
                "stale unresolved orders: "
                f"count={unresolved_count} age={float(oldest_unresolved_age_sec):.1f}s "
                f"> {unresolved_age_threshold_sec:.1f}s"
            )

    reconcile_status = health.get("last_reconcile_status")
    reconcile_error = health.get("last_reconcile_error")
    reconcile_ts = health.get("last_reconcile_epoch_sec")
    if reconcile_status == "error":
        problems.append(f"last reconcile failed: {reconcile_error}")
    if reconcile_ts is not None:
        reconcile_age_sec = max(0.0, time.time() - float(reconcile_ts))
        if reconcile_age_sec > reconcile_stale_threshold_sec:
            problems.append(
                f"reconcile stale: age={reconcile_age_sec:.1f}s > {reconcile_stale_threshold_sec:.1f}s"
            )

    if problems:
        notify("healthcheck failed: " + "; ".join(problems))
        print("[HEALTHCHECK] FAIL", "; ".join(problems))
        return 1

    print("[HEALTHCHECK] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
