from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from .schema import MARKET_REGIME_VERSION


@dataclass(frozen=True)
class RegimeAcceptanceGate:
    required: bool = False
    min_trade_count_per_required_regime: int = 0
    required_regimes: tuple[str, ...] = ()
    blocked_regimes: tuple[str, ...] = ()
    blocked_regime_max_trade_count: int = 0
    blocked_regime_max_net_pnl_loss_krw: float = 0.0
    min_profit_factor_by_regime: dict[str, float] = field(default_factory=dict)
    min_expectancy_by_regime: dict[str, float] = field(default_factory=dict)
    max_loss_share_by_single_regime: float | None = None
    max_pnl_dependency_by_single_regime: float | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "required": self.required,
            "min_trade_count_per_required_regime": self.min_trade_count_per_required_regime,
            "required_regimes": list(self.required_regimes),
            "blocked_regimes": list(self.blocked_regimes),
            "blocked_regime_max_trade_count": self.blocked_regime_max_trade_count,
            "blocked_regime_max_net_pnl_loss_krw": self.blocked_regime_max_net_pnl_loss_krw,
            "min_profit_factor_by_regime": dict(self.min_profit_factor_by_regime),
            "min_expectancy_by_regime": dict(self.min_expectancy_by_regime),
            "max_loss_share_by_single_regime": self.max_loss_share_by_single_regime,
            "max_pnl_dependency_by_single_regime": self.max_pnl_dependency_by_single_regime,
        }


@dataclass(frozen=True)
class RegimeGateResult:
    passed: bool
    reasons: tuple[str, ...]
    allowed_live_regimes: tuple[str, ...]
    blocked_live_regimes: tuple[str, ...]
    evidence: dict[str, dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "result": "PASS" if self.passed else "FAIL",
            "passed": self.passed,
            "reasons": list(self.reasons),
            "allowed_live_regimes": list(self.allowed_live_regimes),
            "blocked_live_regimes": list(self.blocked_live_regimes),
            "evidence": self.evidence,
        }


def _safe_source_label(path: str | Path | None) -> str:
    if path is None:
        return "none"
    text = str(path).strip()
    if not text:
        return "none"
    return Path(text).name or "configured_path"


def load_candidate_regime_policy_from_path(path: str | Path | None) -> dict[str, object] | None:
    if path is None or not str(path).strip():
        return None
    source = f"file:{_safe_source_label(path)}"
    try:
        with Path(path).expanduser().open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError:
        return {
            "_policy_source": source,
            "_policy_load_error": "regime_policy_invalid_json",
        }
    except OSError:
        return {
            "_policy_source": source,
            "_policy_load_error": "regime_policy_unreadable",
        }
    if not isinstance(payload, dict):
        return {
            "_policy_source": source,
            "_policy_load_error": "regime_policy_invalid",
        }
    result = dict(payload)
    result.setdefault("_policy_source", source)
    return result


def _list_field(payload: dict[str, object], *names: str) -> tuple[bool, list[str]]:
    for name in names:
        value = payload.get(name)
        if value is None:
            continue
        if not isinstance(value, list):
            return False, []
        return True, [str(item) for item in value]
    return False, []


def normalize_live_regime_policy(candidate_policy: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(candidate_policy, dict):
        return {
            "regime_policy_present": False,
            "regime_policy_valid": False,
            "regime_block_reason": "regime_policy_missing",
            "regime_policy_source": "none",
            "candidate_regime_classifier_version": None,
            "candidate_allowed_regimes": [],
            "candidate_blocked_regimes": [],
            "regime_evidence": {},
        }

    source = str(candidate_policy.get("_policy_source") or candidate_policy.get("policy_source") or "injected")
    load_error = candidate_policy.get("_policy_load_error")
    if load_error:
        return {
            "regime_policy_present": True,
            "regime_policy_valid": False,
            "regime_block_reason": str(load_error),
            "regime_policy_source": source,
            "candidate_regime_classifier_version": None,
            "candidate_allowed_regimes": [],
            "candidate_blocked_regimes": [],
            "regime_evidence": {},
        }

    policy_payload = candidate_policy.get("live_regime_policy")
    if isinstance(policy_payload, dict):
        payload = {**candidate_policy, **policy_payload}
    else:
        payload = dict(candidate_policy)

    version = payload.get("regime_classifier_version")
    if not isinstance(version, str) or not version.strip():
        return {
            "regime_policy_present": True,
            "regime_policy_valid": False,
            "regime_block_reason": "regime_policy_missing_classifier_version",
            "regime_policy_source": source,
            "candidate_regime_classifier_version": None,
            "candidate_allowed_regimes": [],
            "candidate_blocked_regimes": [],
            "regime_evidence": {},
        }

    allowed_present, allowed = _list_field(payload, "allowed_live_regimes", "allowed_regimes")
    if not allowed_present:
        return {
            "regime_policy_present": True,
            "regime_policy_valid": False,
            "regime_block_reason": "regime_policy_missing_allowed_regimes",
            "regime_policy_source": source,
            "candidate_regime_classifier_version": version,
            "candidate_allowed_regimes": [],
            "candidate_blocked_regimes": [],
            "regime_evidence": {},
        }

    blocked_present, blocked = _list_field(payload, "blocked_live_regimes", "blocked_regimes")
    if not blocked_present:
        return {
            "regime_policy_present": True,
            "regime_policy_valid": False,
            "regime_block_reason": "regime_policy_missing_blocked_regimes",
            "regime_policy_source": source,
            "candidate_regime_classifier_version": version,
            "candidate_allowed_regimes": allowed,
            "candidate_blocked_regimes": [],
            "regime_evidence": {},
        }

    evidence = payload.get("regime_evidence")
    return {
        "regime_policy_present": True,
        "regime_policy_valid": True,
        "regime_block_reason": "none",
        "regime_policy_source": source,
        "candidate_regime_classifier_version": version,
        "candidate_allowed_regimes": allowed,
        "candidate_blocked_regimes": blocked,
        "regime_evidence": dict(evidence) if isinstance(evidence, dict) else {},
    }


def _row_regime(row: Any) -> str:
    return str(row.get("regime") if isinstance(row, dict) else getattr(row, "regime", "unknown"))


def _row_dimension(row: Any) -> str:
    return str(row.get("dimension") if isinstance(row, dict) else getattr(row, "dimension", "unknown"))


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _matches(row: Any, regime: str) -> bool:
    name = _row_regime(row)
    dimension = _row_dimension(row)
    return name == regime or (dimension in {"price_regime", "volatility_bucket", "volume_bucket"} and name == regime)


def evaluate_regime_acceptance_gate(
    *,
    gate: RegimeAcceptanceGate,
    performance_rows: tuple[Any, ...],
) -> RegimeGateResult:
    if not gate.required:
        evidence = {
            _row_regime(row): {
                "trade_count": int(_row_value(row, "trade_count", 0) or 0),
                "profit_factor": _row_value(row, "profit_factor"),
                "expectancy": _row_value(row, "expectancy"),
                "net_pnl": float(_row_value(row, "net_pnl", 0.0) or 0.0),
            }
            for row in performance_rows
            if _row_dimension(row) == "composite_regime"
        }
        return RegimeGateResult(True, (), tuple(sorted(evidence)), (), evidence)

    reasons: list[str] = []
    evidence: dict[str, dict[str, object]] = {}
    for row in performance_rows:
        if _row_dimension(row) != "composite_regime":
            continue
        evidence[_row_regime(row)] = {
            "trade_count": int(_row_value(row, "trade_count", 0) or 0),
            "profit_factor": _row_value(row, "profit_factor"),
            "expectancy": _row_value(row, "expectancy"),
            "net_pnl": float(_row_value(row, "net_pnl", 0.0) or 0.0),
            "candle_count": int(_row_value(row, "candle_count", 0) or 0),
            "candle_share": float(_row_value(row, "candle_share", 0.0) or 0.0),
        }

    for required in gate.required_regimes:
        rows = [row for row in performance_rows if _matches(row, required)]
        trade_count = sum(int(_row_value(row, "trade_count", 0) or 0) for row in rows)
        if trade_count < gate.min_trade_count_per_required_regime:
            reasons.append(
                f"regime_coverage_failed: {required} trade_count={trade_count} < min={gate.min_trade_count_per_required_regime}"
            )

    for blocked in gate.blocked_regimes:
        rows = [row for row in performance_rows if _matches(row, blocked)]
        trade_count = sum(int(_row_value(row, "trade_count", 0) or 0) for row in rows)
        net_pnl = sum(float(_row_value(row, "net_pnl", 0.0) or 0.0) for row in rows)
        if trade_count > gate.blocked_regime_max_trade_count:
            reasons.append(f"blocked_regime_leakage: {blocked} produced {trade_count} BUY decisions")
        if net_pnl < -abs(float(gate.blocked_regime_max_net_pnl_loss_krw)):
            reasons.append(f"blocked_regime_loss: {blocked} net_pnl={net_pnl:.6f}")

    for regime, min_pf in gate.min_profit_factor_by_regime.items():
        rows = [row for row in performance_rows if _matches(row, regime)]
        pf_values = [float(_row_value(row, "profit_factor")) for row in rows if _row_value(row, "profit_factor") is not None]
        if not pf_values or min(pf_values) < float(min_pf):
            actual = min(pf_values) if pf_values else None
            reasons.append(f"regime_gate_failed: {regime} profit_factor={actual} < min={float(min_pf)}")

    for regime, min_expectancy in gate.min_expectancy_by_regime.items():
        rows = [row for row in performance_rows if _matches(row, regime)]
        values = [float(_row_value(row, "expectancy")) for row in rows if _row_value(row, "expectancy") is not None]
        if not values or min(values) < float(min_expectancy):
            reasons.append(f"regime_gate_failed: {regime} expectancy={min(values) if values else None} < min={float(min_expectancy)}")

    composite_rows = [row for row in performance_rows if _row_dimension(row) == "composite_regime"]
    losses = [abs(float(_row_value(row, "net_pnl", 0.0) or 0.0)) for row in composite_rows if float(_row_value(row, "net_pnl", 0.0) or 0.0) < 0.0]
    if losses and gate.max_loss_share_by_single_regime is not None:
        share = max(losses) / sum(losses)
        if share > float(gate.max_loss_share_by_single_regime):
            reasons.append(f"regime_loss_concentration_failed: max_loss_share={share:.6f}")
    profits = [float(_row_value(row, "net_pnl", 0.0) or 0.0) for row in composite_rows if float(_row_value(row, "net_pnl", 0.0) or 0.0) > 0.0]
    if profits and gate.max_pnl_dependency_by_single_regime is not None:
        share = max(profits) / sum(profits)
        if share > float(gate.max_pnl_dependency_by_single_regime):
            reasons.append(f"regime_profit_dependency_failed: max_pnl_dependency={share:.6f}")

    blocked = tuple(sorted(set(gate.blocked_regimes)))
    allowed = tuple(
        sorted(
            regime
            for regime, row in evidence.items()
            if regime not in blocked and int(row.get("trade_count") or 0) > 0
        )
    )
    return RegimeGateResult(not reasons, tuple(reasons), allowed, blocked, evidence)


def evaluate_live_regime_policy(
    *,
    current_snapshot: dict[str, object],
    candidate_policy: dict[str, object] | None,
) -> dict[str, object]:
    normalized = normalize_live_regime_policy(candidate_policy)
    current_version = str(
        current_snapshot.get("version")
        or current_snapshot.get("regime_classifier_version")
        or MARKET_REGIME_VERSION
    )
    current = str(current_snapshot.get("composite_regime") or "unknown")

    if not bool(normalized["regime_policy_present"]):
        return {
            **normalized,
            "allowed": False,
            "regime_decision": "OFF",
            "regime_block_reason": "regime_policy_missing",
            "current_regime": current,
            "current_regime_classifier_version": current_version,
        }
    if not bool(normalized["regime_policy_valid"]):
        return {
            "allowed": False,
            "regime_decision": "OFF",
            "current_regime": current,
            "current_regime_classifier_version": current_version,
            **normalized,
        }

    candidate_version = str(normalized.get("candidate_regime_classifier_version") or "")
    allowed_regimes = [str(item) for item in normalized.get("candidate_allowed_regimes") or ()]
    blocked_regimes = [str(item) for item in normalized.get("candidate_blocked_regimes") or ()]

    if candidate_version != current_version:
        return {
            **normalized,
            "allowed": False,
            "regime_decision": "OFF",
            "regime_block_reason": "regime_policy_version_mismatch",
            "current_regime": current,
            "current_regime_classifier_version": current_version,
        }

    if current in blocked_regimes:
        reason = "current_regime_in_candidate_blocked_regimes"
        allowed = False
    elif current not in allowed_regimes:
        reason = "current_regime_not_in_candidate_allowed_regimes"
        allowed = False
    else:
        reason = "none"
        allowed = True
    return {
        **normalized,
        "allowed": allowed,
        "regime_decision": "ON" if allowed else "OFF",
        "regime_block_reason": reason,
        "current_regime": current,
        "current_regime_classifier_version": current_version,
    }
