from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from .config import settings


STRATEGY_PERFORMANCE_BLOCKED = "STRATEGY_PERFORMANCE_BLOCKED"
STRATEGY_EXPECTANCY_NEGATIVE = "STRATEGY_EXPECTANCY_NEGATIVE"
STRATEGY_SAMPLE_INSUFFICIENT = "STRATEGY_SAMPLE_INSUFFICIENT"
STRATEGY_NET_PNL_NEGATIVE = "STRATEGY_NET_PNL_NEGATIVE"
STRATEGY_PROFIT_FACTOR_LOW = "STRATEGY_PROFIT_FACTOR_LOW"
STRATEGY_FEE_DRAG_EXCESSIVE = "STRATEGY_FEE_DRAG_EXCESSIVE"
FEE_DRAG_RATIO_BASIS_TRADED_NOTIONAL = "traded_notional"
FEE_TO_GROSS_PNL_RATIO_BASIS = "gross_pnl_abs"


@dataclass(frozen=True)
class StrategyPerformanceSummary:
    sample_count: int
    gross_pnl: float
    fee_total: float
    net_pnl: float
    expectancy_per_trade: float
    win_rate: float
    profit_factor: float | None
    profit_factor_unbounded: bool
    fee_drag_ratio: float | None
    fee_drag_ratio_basis: str
    fee_to_gross_pnl_ratio: float | None
    fee_to_gross_pnl_ratio_basis: str
    traded_notional_total: float | None
    worst_trade: float | None
    best_trade: float | None
    filter_scope: dict[str, object] = field(default_factory=dict)
    by_strategy_name: dict[str, dict[str, float | int]] = field(default_factory=dict)
    by_exit_rule_name: dict[str, dict[str, float | int]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "sample_count": self.sample_count,
            "gross_pnl": self.gross_pnl,
            "fee_total": self.fee_total,
            "net_pnl": self.net_pnl,
            "expectancy_per_trade": self.expectancy_per_trade,
            "win_rate": self.win_rate,
            "win_rate_pct": self.win_rate * 100.0,
            "profit_factor": self.profit_factor,
            "profit_factor_unbounded": self.profit_factor_unbounded,
            "fee_drag_ratio": self.fee_drag_ratio,
            "fee_drag_ratio_basis": self.fee_drag_ratio_basis,
            "fee_to_gross_pnl_ratio": self.fee_to_gross_pnl_ratio,
            "fee_to_gross_pnl_ratio_basis": self.fee_to_gross_pnl_ratio_basis,
            "traded_notional_total": self.traded_notional_total,
            "worst_trade": self.worst_trade,
            "best_trade": self.best_trade,
            "filter_scope": dict(self.filter_scope),
            "by_strategy_name": self.by_strategy_name,
            "by_exit_rule_name": self.by_exit_rule_name,
        }


@dataclass(frozen=True)
class StrategyPerformanceGateResult:
    enabled: bool
    allowed: bool
    reason_code: str
    reason: str
    recommended_next_action: str
    summary: StrategyPerformanceSummary
    thresholds: dict[str, float | int | None | str]

    def as_dict(self) -> dict[str, object]:
        return {
            "enabled": bool(self.enabled),
            "allowed": bool(self.allowed),
            "blocked": bool(self.enabled and not self.allowed),
            "reason_code": self.reason_code,
            "reason": self.reason,
            "recommended_next_action": self.recommended_next_action,
            "summary": self.summary.as_dict(),
            "thresholds": dict(self.thresholds),
            "filter_scope": dict(self.summary.filter_scope),
        }


def _empty_summary() -> StrategyPerformanceSummary:
    return StrategyPerformanceSummary(
        sample_count=0,
        gross_pnl=0.0,
        fee_total=0.0,
        net_pnl=0.0,
        expectancy_per_trade=0.0,
        win_rate=0.0,
        profit_factor=None,
        profit_factor_unbounded=False,
        fee_drag_ratio=None,
        fee_drag_ratio_basis=FEE_DRAG_RATIO_BASIS_TRADED_NOTIONAL,
        fee_to_gross_pnl_ratio=None,
        fee_to_gross_pnl_ratio_basis=FEE_TO_GROSS_PNL_RATIO_BASIS,
        traded_notional_total=None,
        worst_trade=None,
        best_trade=None,
    )


def _bucket(rows: list[sqlite3.Row], key: str) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for row in rows:
        name = str(row[key] or "<unknown>")
        item = out.setdefault(
            name,
            {
                "sample_count": 0,
                "gross_pnl": 0.0,
                "fee_total": 0.0,
                "net_pnl": 0.0,
                "expectancy_per_trade": 0.0,
            },
        )
        item["sample_count"] = int(item["sample_count"]) + 1
        item["gross_pnl"] = float(item["gross_pnl"]) + float(row["gross_pnl"] or 0.0)
        item["fee_total"] = float(item["fee_total"]) + float(row["fee_total"] or 0.0)
        item["net_pnl"] = float(item["net_pnl"]) + float(row["net_pnl"] or 0.0)
    for item in out.values():
        count = max(1, int(item["sample_count"]))
        item["expectancy_per_trade"] = float(item["net_pnl"]) / count
    return out


def fetch_strategy_performance_summary(
    conn: sqlite3.Connection,
    *,
    strategy_instance_id: str | None = None,
    strategy_name: str | None = None,
    pair: str | None = None,
    runtime_strategy_set_manifest_hash: str | None = None,
    recent_limit: int = 200,
) -> StrategyPerformanceSummary:
    try:
        cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(trade_lifecycles)").fetchall()}
    except sqlite3.Error:
        return _empty_summary()
    if not {"gross_pnl", "fee_total", "net_pnl", "exit_ts"}.issubset(cols):
        return _empty_summary()
    has_notional_cols = {"matched_qty", "entry_price", "exit_price"}.issubset(cols)
    owner_name_expr = (
        "COALESCE(owner_strategy_name, strategy_name, '<unknown>')"
        if "owner_strategy_name" in cols
        else "COALESCE(strategy_name, '<unknown>')"
    )
    owner_instance_expr = (
        "COALESCE(owner_strategy_instance_id, strategy_instance_id, '')"
        if "owner_strategy_instance_id" in cols and "strategy_instance_id" in cols
        else "COALESCE(strategy_instance_id, '')"
    )
    has_instance_col = "owner_strategy_instance_id" in cols or "strategy_instance_id" in cols
    has_manifest_col = "runtime_strategy_set_manifest_hash" in cols

    filters: list[str] = []
    params: list[object] = []
    instance_filter_applied = False
    manifest_filter_applied = False
    if strategy_instance_id and has_instance_col:
        filters.append(f"{owner_instance_expr} = ?")
        params.append(str(strategy_instance_id))
        instance_filter_applied = True
    elif strategy_name:
        filters.append(f"{owner_name_expr} = ?")
        params.append(str(strategy_name))
    if pair:
        filters.append("COALESCE(pair, '<unknown>') = ?")
        params.append(str(pair))
    if runtime_strategy_set_manifest_hash and has_manifest_col and not instance_filter_applied:
        filters.append("COALESCE(runtime_strategy_set_manifest_hash, '') = ?")
        params.append(str(runtime_strategy_set_manifest_hash))
        manifest_filter_applied = True
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    scope = {
        "strategy_instance_id": strategy_instance_id,
        "strategy_name": strategy_name,
        "pair": pair,
        "runtime_strategy_set_manifest_hash": runtime_strategy_set_manifest_hash,
        "strategy_instance_id_filter_available": has_instance_col,
        "strategy_instance_id_filter_applied": instance_filter_applied,
        "runtime_strategy_set_manifest_hash_filter_available": has_manifest_col,
        "runtime_strategy_set_manifest_hash_filter_applied": manifest_filter_applied,
        "fallback_filter_applied": bool(strategy_instance_id and not has_instance_col),
        "filter_precedence": (
            "strategy_instance_id"
            if instance_filter_applied
            else "strategy_name_pair_compatibility"
        ),
    }
    rows = conn.execute(
        f"""
        SELECT
            {owner_instance_expr + " AS strategy_instance_id," if has_instance_col else ""}
            {"COALESCE(runtime_strategy_set_manifest_hash, '') AS runtime_strategy_set_manifest_hash," if has_manifest_col else ""}
            {owner_name_expr} AS strategy_name,
            COALESCE(exit_rule_name, '<unknown>') AS exit_rule_name,
            gross_pnl,
            fee_total,
            net_pnl
            {", matched_qty, entry_price, exit_price" if has_notional_cols else ""}
        FROM trade_lifecycles
        {where}
        ORDER BY exit_ts DESC, id DESC
        LIMIT ?
        """,
        (*params, max(1, int(recent_limit))),
    ).fetchall()
    if not rows:
        empty = _empty_summary()
        return StrategyPerformanceSummary(**{**empty.__dict__, "filter_scope": scope})

    net_values = [float(row["net_pnl"] or 0.0) for row in rows]
    gross_pnl = float(sum(float(row["gross_pnl"] or 0.0) for row in rows))
    fee_total = float(sum(float(row["fee_total"] or 0.0) for row in rows))
    net_pnl = float(sum(net_values))
    traded_notional_total = (
        float(
            sum(
                abs(float(row["matched_qty"] or 0.0) * float(row["entry_price"] or 0.0))
                + abs(float(row["matched_qty"] or 0.0) * float(row["exit_price"] or 0.0))
                for row in rows
            )
        )
        if has_notional_cols
        else None
    )
    wins = [pnl for pnl in net_values if pnl > 0.0]
    losses = [pnl for pnl in net_values if pnl < 0.0]
    profit_factor_unbounded = bool(wins and not losses)
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else None
    fee_drag_ratio = (
        (fee_total / traded_notional_total)
        if traded_notional_total is not None and traded_notional_total > 1e-12
        else None
    )
    fee_to_gross_pnl_ratio = (fee_total / abs(gross_pnl)) if abs(gross_pnl) > 1e-12 else None
    return StrategyPerformanceSummary(
        sample_count=len(rows),
        gross_pnl=gross_pnl,
        fee_total=fee_total,
        net_pnl=net_pnl,
        expectancy_per_trade=net_pnl / len(rows),
        win_rate=len(wins) / len(rows),
        profit_factor=profit_factor,
        profit_factor_unbounded=profit_factor_unbounded,
        fee_drag_ratio=fee_drag_ratio,
        fee_drag_ratio_basis=FEE_DRAG_RATIO_BASIS_TRADED_NOTIONAL,
        fee_to_gross_pnl_ratio=fee_to_gross_pnl_ratio,
        fee_to_gross_pnl_ratio_basis=FEE_TO_GROSS_PNL_RATIO_BASIS,
        traded_notional_total=traded_notional_total,
        worst_trade=min(net_values),
        best_trade=max(net_values),
        filter_scope=scope,
        by_strategy_name=_bucket(rows, "strategy_name"),
        by_exit_rule_name=_bucket(rows, "exit_rule_name"),
    )


def evaluate_strategy_performance_gate(
    conn: sqlite3.Connection,
    *,
    strategy_instance_id: str | None = None,
    strategy_name: str | None = None,
    pair: str | None = None,
    runtime_strategy_set_manifest_hash: str | None = None,
    settings_obj: object = settings,
) -> StrategyPerformanceGateResult:
    enabled = bool(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_ENABLED", True))
    min_sample = max(1, int(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 30)))
    recent_limit = max(min_sample, int(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_RECENT_LIMIT", 200)))
    summary = fetch_strategy_performance_summary(
        conn,
        strategy_instance_id=strategy_instance_id,
        strategy_name=strategy_name,
        pair=pair,
        runtime_strategy_set_manifest_hash=runtime_strategy_set_manifest_hash,
        recent_limit=recent_limit,
    )
    thresholds = {
        "scope": str(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_SCOPE", "closed_lifecycles_recent")),
        "min_sample": min_sample,
        "recent_limit": recent_limit,
        "min_expectancy_krw": float(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW", 0.0)),
        "min_net_pnl_krw": float(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW", 0.0)),
        "min_profit_factor": float(getattr(settings_obj, "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR", 1.0)),
        "max_fee_drag_ratio": getattr(settings_obj, "LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO", None),
        "max_fee_drag_ratio_basis": FEE_TO_GROSS_PNL_RATIO_BASIS,
        "filter_scope": dict(summary.filter_scope),
    }
    if not enabled:
        return StrategyPerformanceGateResult(
            enabled=False,
            allowed=True,
            reason_code="STRATEGY_PERFORMANCE_GATE_DISABLED",
            reason="live performance gate disabled by configuration",
            recommended_next_action="none",
            summary=summary,
            thresholds=thresholds,
        )

    reason_code = "STRATEGY_PERFORMANCE_OK"
    reason = "strategy performance gate passed"
    allowed = True
    if summary.sample_count < min_sample:
        allowed = False
        reason_code = STRATEGY_SAMPLE_INSUFFICIENT
        reason = f"sample_count={summary.sample_count} below min_sample={min_sample}"
    elif summary.expectancy_per_trade < float(thresholds["min_expectancy_krw"]):
        allowed = False
        reason_code = STRATEGY_EXPECTANCY_NEGATIVE
        reason = (
            f"expectancy_per_trade={summary.expectancy_per_trade:.6f} "
            f"below min={float(thresholds['min_expectancy_krw']):.6f}"
        )
    elif summary.net_pnl < float(thresholds["min_net_pnl_krw"]):
        allowed = False
        reason_code = STRATEGY_NET_PNL_NEGATIVE
        reason = f"net_pnl={summary.net_pnl:.6f} below min={float(thresholds['min_net_pnl_krw']):.6f}"
    elif (
        summary.profit_factor_unbounded is not True
        and (summary.profit_factor is None or summary.profit_factor < float(thresholds["min_profit_factor"]))
    ):
        allowed = False
        reason_code = STRATEGY_PROFIT_FACTOR_LOW
        reason = f"profit_factor={summary.profit_factor} below min={float(thresholds['min_profit_factor']):.6f}"
    max_fee_drag = thresholds["max_fee_drag_ratio"]
    if allowed and max_fee_drag not in (None, "", "0"):
        max_fee_drag_float = float(max_fee_drag)
        if summary.fee_to_gross_pnl_ratio is not None and summary.fee_to_gross_pnl_ratio > max_fee_drag_float:
            allowed = False
            reason_code = STRATEGY_FEE_DRAG_EXCESSIVE
            reason = (
                f"fee_to_gross_pnl_ratio={summary.fee_to_gross_pnl_ratio:.6f} "
                f"basis={summary.fee_to_gross_pnl_ratio_basis} above max={max_fee_drag_float:.6f}"
            )

    return StrategyPerformanceGateResult(
        enabled=True,
        allowed=allowed,
        reason_code=reason_code if allowed else STRATEGY_PERFORMANCE_BLOCKED + ":" + reason_code,
        reason=reason,
        recommended_next_action=(
            "review strategy-report and experiment-report; keep recovery/flatten commands available"
            if not allowed
            else "none"
        ),
        summary=summary,
        thresholds=thresholds,
    )
