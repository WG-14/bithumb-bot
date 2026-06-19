from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import (
    PROJECT_ROOT,
    LIVE_DB_PATH_REQUIRED_MSG,
    LiveModeValidationError,
    PathManager,
    PathPolicyError,
    Settings,
    resolve_db_path_for_mode,
    validate_market_preflight,
    validate_runtime_root_separation,
)
from .db_core import assert_current_schema
from .runtime_readiness import compute_runtime_readiness_snapshot


_BROKER_LOCAL_QTY_EPSILON = 1e-12


def _issue_if_live_root_invalid(key: str) -> str | None:
    raw = os.getenv(key)
    if raw is None or not raw.strip():
        return f"{key} must be explicitly set when MODE=live"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        return f"{key} must be an absolute path when MODE=live"
    resolved = path.resolve()
    if PathManager._is_within(resolved, PROJECT_ROOT.resolve()):
        return f"{key} must be outside repository when MODE=live ({resolved})"
    if PathManager._contains_segment(resolved, "paper"):
        return f"{key} must not contain a paper-scoped path segment when MODE=live"
    return None


def _live_path_policy_issues(cfg: Settings) -> list[str]:
    issues: list[str] = []
    try:
        live_path_manager = PathManager.from_env(PROJECT_ROOT)
        validate_runtime_root_separation(live_path_manager.config)
    except PathPolicyError as exc:
        issues.append(str(exc))
    for root_key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT"):
        issue = _issue_if_live_root_invalid(root_key)
        if issue:
            issues.append(issue)
    db_path_env = os.getenv("DB_PATH")
    if db_path_env is None or not db_path_env.strip():
        issues.append(LIVE_DB_PATH_REQUIRED_MSG)
    else:
        try:
            resolve_db_path_for_mode(str(cfg.DB_PATH), mode="live")
        except ValueError as exc:
            issues.append(str(exc))
    return issues


def validate_live_operator_basic_guard(cfg: Settings) -> None:
    if cfg.MODE != "live":
        return
    issues = _live_path_policy_issues(cfg)
    if issues:
        raise LiveModeValidationError(
            "live operator command guard validation failed: " + "; ".join(issues)
        )


def validate_operator_smoke_cli_guard(cfg: Settings) -> None:
    if cfg.MODE != "live":
        return
    issues = _live_path_policy_issues(cfg)
    if bool(cfg.LIVE_DRY_RUN):
        issues.append("LIVE_DRY_RUN=false is required for operator smoke real submit")
    if not bool(cfg.LIVE_REAL_ORDER_ARMED):
        issues.append("LIVE_REAL_ORDER_ARMED=true is required for operator smoke real submit")
    if bool(cfg.KILL_SWITCH):
        issues.append("operator smoke blocked by KILL_SWITCH=true")
    if not str(cfg.BITHUMB_API_KEY or "").strip():
        issues.append("BITHUMB_API_KEY is required when MODE=live")
    if not str(cfg.BITHUMB_API_SECRET or "").strip():
        issues.append("BITHUMB_API_SECRET is required when MODE=live")
    if issues:
        raise LiveModeValidationError(
            "operator smoke preflight validation failed: " + "; ".join(issues)
        )


def _validate_operator_smoke_broker_local_consistency(conn: Any) -> None:
    try:
        snapshot = compute_runtime_readiness_snapshot(conn)
    except Exception as exc:
        raise LiveModeValidationError(
            f"operator_smoke_broker_local_match_unavailable:{type(exc).__name__}:{exc}"
        ) from exc

    issues: list[str] = []
    evidence = dict(snapshot.broker_position_evidence or {})
    projection = dict(snapshot.projection_convergence or {})
    broker_qty_known = bool(evidence.get("broker_qty_known"))
    balance_stale = bool(evidence.get("balance_source_stale"))
    projection_converged = bool(projection.get("converged"))
    broker_qty = float(evidence.get("broker_qty") or 0.0)
    portfolio_qty = float(projection.get("portfolio_qty") or 0.0)
    projected_qty = float(projection.get("projected_total_qty") or 0.0)
    broker_local_match = bool(
        broker_qty_known
        and not balance_stale
        and projection_converged
        and abs(broker_qty - portfolio_qty) <= _BROKER_LOCAL_QTY_EPSILON
        and abs(projected_qty - portfolio_qty) <= _BROKER_LOCAL_QTY_EPSILON
    )

    if not broker_qty_known:
        missing = ",".join(str(item) for item in (evidence.get("missing_evidence_fields") or []))
        issues.append(f"broker_local_match_unavailable:{missing or 'broker_position_evidence_missing'}")
    if balance_stale:
        issues.append("broker_local_match_unavailable:balance_source_stale")
    if not projection_converged:
        issues.append(f"broker_local_projection_mismatch:{projection.get('reason') or 'projection_non_converged'}")
    if not broker_local_match:
        issues.append(
            "broker_local_mismatch="
            f"broker_qty={broker_qty:.12f},portfolio_qty={portfolio_qty:.12f},projected_qty={projected_qty:.12f}"
        )
    if int(snapshot.recovery_required_count or 0) > 0:
        issues.append(f"recovery_required_count={int(snapshot.recovery_required_count)}")
    if int(snapshot.fee_pending_count or 0) > 0:
        issues.append(f"fee_pending_count={int(snapshot.fee_pending_count)}")
    if bool(snapshot.active_fee_accounting_blocker):
        issues.append("active_fee_accounting_blocker=true")

    if issues:
        raise LiveModeValidationError(
            "operator_smoke_broker_local_match_failed: " + "; ".join(dict.fromkeys(issues))
        )


def validate_operator_smoke_preflight(
    *,
    cfg: Settings,
    conn: Any,
    market: str,
    market_preflight: Any | None = None,
) -> None:
    validate_operator_smoke_cli_guard(cfg)
    try:
        assert_current_schema(conn)
    except Exception as exc:
        raise LiveModeValidationError(f"operator_smoke_db_schema_failed:{type(exc).__name__}:{exc}") from exc
    if str(market or "").strip().upper() != str(cfg.PAIR or "").strip().upper():
        raise LiveModeValidationError("operator_smoke_market_mismatch_with_settings_pair")
    preflight = market_preflight or validate_market_preflight
    try:
        preflight(cfg)
    except Exception as exc:
        raise LiveModeValidationError(f"operator_smoke_market_account_order_preflight_failed:{exc}") from exc
    if cfg.MODE == "live" and not bool(cfg.LIVE_DRY_RUN):
        _validate_operator_smoke_broker_local_consistency(conn)
