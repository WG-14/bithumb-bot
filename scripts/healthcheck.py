#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from pathlib import Path


def _resolve_explicit_env_file() -> str | None:
    explicit_env_file = os.getenv("BITHUMB_ENV_FILE")
    if explicit_env_file:
        return explicit_env_file

    normalized_mode = (os.getenv("MODE") or "").strip().lower()
    if normalized_mode == "live":
        return os.getenv("BITHUMB_ENV_FILE_LIVE")
    if normalized_mode in {"paper", "test"}:
        return os.getenv("BITHUMB_ENV_FILE_PAPER")
    return None


def _load_env_file(env_file: Path) -> None:
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"\"", "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _validate_healthcheck_env() -> str | None:
    env_file_value = _resolve_explicit_env_file()
    if env_file_value is None or not env_file_value.strip():
        return (
            "healthcheck config error: explicit env file is required; "
            "set BITHUMB_ENV_FILE (or BITHUMB_ENV_FILE_LIVE/BITHUMB_ENV_FILE_PAPER)"
        )

    env_file = Path(env_file_value).expanduser()
    if not env_file.exists() or not env_file.is_file():
        return f"healthcheck config error: env file not found: {env_file}"

    _load_env_file(env_file)

    db_path_env = os.getenv("DB_PATH")
    if db_path_env is None or not db_path_env.strip():
        return (
            f"healthcheck config error: DB_PATH is missing or empty in env file {env_file}; "
            "refusing to fall back to default DB"
        )

    return None


def main() -> int:
    env_error = _validate_healthcheck_env()
    if env_error:
        print(f"[HEALTHCHECK] FAIL {env_error}")
        return 1

    from bithumb_bot.config import settings
    from bithumb_bot.notifier import notify
    from bithumb_bot.run_lock import read_run_lock_status
    from bithumb_bot.runtime_state import refresh_open_order_health, snapshot

    stale_threshold_sec = float(os.getenv("HEALTH_MAX_CANDLE_AGE_SEC", "180"))
    error_threshold = int(os.getenv("HEALTH_MAX_ERROR_COUNT", "3"))

    reconcile_stale_threshold_sec = float(os.getenv("HEALTH_MAX_RECONCILE_AGE_SEC", "900"))
    unresolved_age_threshold_sec = float(os.getenv("HEALTH_MAX_UNRESOLVED_ORDER_AGE_SEC", "900"))

    refresh_open_order_health()
    lock_status = read_run_lock_status(Path(settings.RUN_LOCK_PATH))
    print(f"[HEALTHCHECK] RUN_LOCK {lock_status.to_human_text()}")

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
