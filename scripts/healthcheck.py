#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from pathlib import Path


def _validate_healthcheck_env():
    from bithumb_bot.bootstrap import describe_explicit_env_file, load_explicit_env_file

    mode = (os.getenv("MODE") or "").strip().lower() or None
    summary = describe_explicit_env_file(mode)
    env_file_value = summary.env_file
    if env_file_value is None or not env_file_value.strip():
        return None, (
            "healthcheck config error: explicit env file is required; "
            "set BITHUMB_ENV_FILE (or BITHUMB_ENV_FILE_LIVE/BITHUMB_ENV_FILE_PAPER)"
        )

    env_file = Path(env_file_value).expanduser()
    if not env_file.exists() or not env_file.is_file():
        return None, f"healthcheck config error: env file not found: {env_file}"

    load_explicit_env_file(mode)

    db_path_env = os.getenv("DB_PATH")
    if db_path_env is None or not db_path_env.strip():
        return summary, (
            f"healthcheck config error: DB_PATH is missing or empty in env file {env_file}; "
            "refusing to fall back to default DB"
        )

    return summary, None


def main() -> int:
    env_summary, env_error = _validate_healthcheck_env()
    if env_error:
        print(f"[HEALTHCHECK] FAIL {env_error}")
        return 1

    from bithumb_bot.bootstrap import get_last_explicit_env_load_summary
    from bithumb_bot.config import PROJECT_ROOT, settings
    from bithumb_bot.notifier import notify
    from bithumb_bot.broker.bithumb import BithumbBroker
    from bithumb_bot.paths import PathManager
    from bithumb_bot.run_lock import read_run_lock_status
    from bithumb_bot.runtime_state import refresh_open_order_health, snapshot

    stale_threshold_sec = float(os.getenv("HEALTH_MAX_CANDLE_AGE_SEC", "180"))
    error_threshold = int(os.getenv("HEALTH_MAX_ERROR_COUNT", "3"))

    reconcile_stale_threshold_sec = float(os.getenv("HEALTH_MAX_RECONCILE_AGE_SEC", "900"))
    unresolved_age_threshold_sec = float(os.getenv("HEALTH_MAX_UNRESOLVED_ORDER_AGE_SEC", "900"))
    balance_source_stale_threshold_sec = float(os.getenv("HEALTH_MAX_BALANCE_SOURCE_AGE_SEC", "120"))
    pm = PathManager.from_env(PROJECT_ROOT)

    refresh_open_order_health()
    lock_status = read_run_lock_status(Path(settings.RUN_LOCK_PATH))
    print(f"[HEALTHCHECK] RUN_LOCK {lock_status.to_human_text()}")
    print(
        "[HEALTHCHECK] PATHS "
        f"mode={pm.config.mode} "
        f"db={settings.DB_PATH} "
        f"run_lock={settings.RUN_LOCK_PATH} "
        f"runtime_state={pm.runtime_state_path()} "
        f"backup_db_dir={pm.config.backup_root / pm.config.mode / 'db'}"
    )
    active_env_summary = get_last_explicit_env_load_summary().as_dict()
    if env_summary is not None and not active_env_summary.get("env_file"):
        active_env_summary = env_summary.as_dict()
    print(
        "[HEALTHCHECK] ENV_SOURCE "
        f"mode={active_env_summary.get('mode') or '-'} "
        f"source_key={active_env_summary.get('source_key') or '-'} "
        f"env_file={active_env_summary.get('env_file') or '-'} "
        f"exists={1 if active_env_summary.get('exists') else 0} "
        f"loaded={1 if active_env_summary.get('loaded') else 0} "
        f"override={1 if active_env_summary.get('override') else 0}"
    )

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
    balance_diag: dict[str, object] = {
        "source": "unavailable",
        "reason": "not_checked",
        "failure_category": "none",
        "last_success_ts_ms": None,
        "last_asset_ts_ms": None,
        "stale": None,
    }
    try:
        broker = BithumbBroker()
        auth_diag = broker.get_auth_runtime_diagnostics(
            caller="healthcheck",
            env_summary=active_env_summary,
        )
        print(
            "[HEALTHCHECK] AUTH_INIT "
            f"caller={auth_diag.get('caller') or '-'} "
            f"mode={auth_diag.get('mode') or '-'} "
            f"balance_source={auth_diag.get('balance_source_selected') or '-'} "
            f"api_key_present={1 if auth_diag.get('api_key_present') else 0} "
            f"api_key_length={auth_diag.get('api_key_length')} "
            f"api_secret_present={1 if auth_diag.get('api_secret_present') else 0} "
            f"api_secret_length={auth_diag.get('api_secret_length')} "
            f"live_dry_run={1 if auth_diag.get('live_dry_run') else 0} "
            f"live_real_order_armed={1 if auth_diag.get('live_real_order_armed') else 0} "
            f"ws_myasset_enabled={1 if auth_diag.get('ws_myasset_enabled') else 0}"
        )
        accounts_auth = auth_diag.get("accounts_auth") if isinstance(auth_diag.get("accounts_auth"), dict) else {}
        chance_auth = auth_diag.get("chance_auth") if isinstance(auth_diag.get("chance_auth"), dict) else {}
        print(
            "[HEALTHCHECK] AUTH_PREVIEW "
            f"endpoint={accounts_auth.get('endpoint') or '-'} "
            f"method={accounts_auth.get('method') or '-'} "
            f"auth_branch={accounts_auth.get('auth_branch') or '-'} "
            f"query_hash_included={1 if accounts_auth.get('query_hash_included') else 0} "
            f"fallback_branch_used={1 if accounts_auth.get('fallback_branch_used') else 0}"
        )
        print(
            "[HEALTHCHECK] AUTH_PREVIEW "
            f"endpoint={chance_auth.get('endpoint') or '-'} "
            f"method={chance_auth.get('method') or '-'} "
            f"auth_branch={chance_auth.get('auth_branch') or '-'} "
            f"query_hash_included={1 if chance_auth.get('query_hash_included') else 0} "
            f"query_hash_preview={chance_auth.get('query_hash_preview') or '-'} "
            f"payload_keys={','.join(str(item) for item in list(chance_auth.get('payload_keys') or [])) or '-'} "
            f"fallback_branch_used={1 if chance_auth.get('fallback_branch_used') else 0}"
        )
        try:
            broker.get_balance_snapshot()
        except Exception:
            pass
        raw_diag = broker.get_accounts_validation_diagnostics()
        if isinstance(raw_diag, dict):
            balance_diag.update(raw_diag)
    except Exception as exc:
        balance_diag["reason"] = f"diagnostic_probe_failed: {type(exc).__name__}"
        balance_diag["failure_category"] = "transport_failure"
    print(
        "[HEALTHCHECK] BALANCE_SOURCE "
        f"source={balance_diag.get('source') or '-'} "
        f"reason={balance_diag.get('reason') or '-'} "
        f"category={balance_diag.get('failure_category') or '-'} "
        f"last_success_ts_ms={balance_diag.get('last_success_ts_ms')} "
        f"last_asset_ts_ms={balance_diag.get('last_asset_ts_ms')} "
        f"stale={balance_diag.get('stale')}"
    )

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

    reconcile_freshness_required = unresolved_count > 0 or recovery_required_count > 0
    if reconcile_freshness_required:
        if reconcile_ts is None:
            problems.append("reconcile stale: no reconcile timestamp while unresolved/recovery state exists")
        else:
            reconcile_age_sec = max(0.0, time.time() - float(reconcile_ts))
            if reconcile_age_sec > reconcile_stale_threshold_sec:
                problems.append(
                    f"reconcile stale: age={reconcile_age_sec:.1f}s > {reconcile_stale_threshold_sec:.1f}s"
                )
    else:
        print(
            "[HEALTHCHECK] SKIP reconcile freshness check: "
            "no unresolved open orders / no recovery required"
        )

    source_failure_category = str(balance_diag.get("failure_category") or "none")
    if source_failure_category == "schema_mismatch":
        problems.append("balance source schema mismatch")
    elif source_failure_category == "auth_failure":
        problems.append("balance source auth failure")
    elif source_failure_category == "transport_failure":
        problems.append("balance source transport failure")
    elif source_failure_category == "stale_source":
        problems.append("balance source stale")

    last_success_ts_ms = balance_diag.get("last_success_ts_ms")
    if last_success_ts_ms is not None:
        try:
            balance_age_sec = max(0.0, (time.time() * 1000 - float(last_success_ts_ms)) / 1000)
            if balance_age_sec > balance_source_stale_threshold_sec:
                problems.append(
                    "balance source stale: "
                    f"age={balance_age_sec:.1f}s > {balance_source_stale_threshold_sec:.1f}s"
                )
        except (TypeError, ValueError):
            problems.append("balance source stale: invalid last_success_ts_ms")
    if problems:
        notify("healthcheck failed: " + "; ".join(problems))
        print("[HEALTHCHECK] FAIL", "; ".join(problems))
        return 1

    print("[HEALTHCHECK] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
