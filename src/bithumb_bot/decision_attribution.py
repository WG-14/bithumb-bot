from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from statistics import median
from typing import Any, Iterable, Sequence

from .decision_contract import BLOCK_LAYER_PRIORITY


SIGNALS = ("HOLD", "BUY", "SELL", "UNKNOWN")
FILTER_KEYS = (
    "blocked_by_cost_filter",
    "blocked_by_fee_authority",
    "blocked_by_position_gate",
    "blocked_by_order_rule",
    "blocked_by_performance_gate",
)


@dataclass(frozen=True)
class DecisionAttribution:
    """Normalized, replay-compatible view of one stored strategy decision.

    This model is deliberately read-only and independent of runtime settings so
    future replay/sweep output can be aggregated through the same attribution
    path as live dry-run or historical strategy_decisions rows.
    """

    raw_signal: str
    final_signal: str
    decision_type: str
    base_reason: str
    entry_reason: str
    entry_block_reason: str | None
    primary_block_layer: str
    primary_block_reason: str
    all_block_reasons: tuple[str, ...]
    blocked_by_cost_filter: bool
    blocked_by_fee_authority: bool
    blocked_by_position_gate: bool
    blocked_by_order_rule: bool
    blocked_by_performance_gate: bool
    gap_ratio: float | None
    required_edge_ratio: float | None
    signal_strength_label: str
    submit_expected: bool | None
    execution_block_reason: str | None
    target_block_reason: str | None
    experiment_fingerprint: str | None = None
    context_status: str = "ok"
    primary_all_block_conflict: bool = False


@dataclass(frozen=True)
class DecisionAttributionSummary:
    sample_count: int
    malformed_context_count: int
    context_missing_count: int
    raw_signal_counts: dict[str, int]
    final_signal_counts: dict[str, int]
    decision_type_counts: dict[str, int]
    candidate_funnel: dict[str, int]
    block_layer_counts: dict[str, int]
    block_reason_counts: dict[str, int]
    entry_reason_counts: dict[str, int]
    entry_block_reason_counts: dict[str, int]
    filter_ratios: dict[str, float]
    edge_stats: dict[str, float | None]
    signal_strength_counts: dict[str, int]
    submit_mismatch: dict[str, int]
    schema_quality: dict[str, int]
    interpretation: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "sample_count": self.sample_count,
            "malformed_context_count": self.malformed_context_count,
            "context_missing_count": self.context_missing_count,
            "raw_signal_counts": self.raw_signal_counts,
            "final_signal_counts": self.final_signal_counts,
            "decision_type_counts": self.decision_type_counts,
            "candidate_funnel": self.candidate_funnel,
            "block_layer_counts": self.block_layer_counts,
            "block_reason_counts": self.block_reason_counts,
            "entry_reason_counts": self.entry_reason_counts,
            "entry_block_reason_counts": self.entry_block_reason_counts,
            "filter_ratios": self.filter_ratios,
            "edge_stats": self.edge_stats,
            "signal_strength_counts": self.signal_strength_counts,
            "submit_mismatch": self.submit_mismatch,
            "schema_quality": self.schema_quality,
            "interpretation": self.interpretation,
        }


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: Any, *, default: str = "unknown") -> str:
    text = str(value if value is not None else default).strip()
    return text or default


def _signal(value: Any, *, default: str = "UNKNOWN") -> str:
    text = _text(value, default=default).upper()
    return text if text in SIGNALS else text


def _optional_text(value: Any) -> str | None:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() in {"none", "null", "-"}:
        return None
    return text


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_text(*values: Any, default: str = "unknown") -> str:
    for value in values:
        text = _optional_text(value)
        if text is not None:
            return text
    return default


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def split_block_reason(value: str) -> tuple[str, str] | None:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() in {"none", "null", "-"} or "." not in text:
        return None
    layer, reason = (part.strip() for part in text.split(".", 1))
    if not layer or not reason:
        return None
    return layer, reason


def _normalize_block_reason_item(value: Any) -> str | None:
    if isinstance(value, str):
        parsed = split_block_reason(value)
        if parsed is None:
            return None
        layer, reason = parsed
        return f"{layer}.{reason}"
    if isinstance(value, (tuple, list)) and len(value) == 2:
        layer = _optional_text(value[0])
        reason = _optional_text(value[1])
        if layer is None or reason is None:
            return None
        return f"{layer}.{reason}"
    return None


def normalize_all_block_reasons(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    raw_items = value if isinstance(value, (list, tuple)) else [value]
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw_items:
        reason = _normalize_block_reason_item(item)
        if reason is None or reason in seen:
            continue
        seen.add(reason)
        normalized.append(reason)
    return tuple(normalized)


def select_primary_block_from_all_reasons(
    all_block_reasons: Sequence[str],
) -> tuple[str, str] | None:
    parsed = [item for item in (split_block_reason(reason) for reason in all_block_reasons) if item]
    if not parsed:
        return None
    for layer in BLOCK_LAYER_PRIORITY:
        for candidate_layer, candidate_reason in parsed:
            if candidate_layer == layer:
                return candidate_layer, candidate_reason
    return parsed[0]


def _reason_pairs(all_block_reasons: Sequence[str]) -> set[tuple[str, str]]:
    return {parsed for parsed in (split_block_reason(reason) for reason in all_block_reasons) if parsed}


def _has_layer(reason_pairs: set[tuple[str, str]], layer: str) -> bool:
    return any(parsed_layer == layer for parsed_layer, _reason in reason_pairs)


def _has_reason(reason_pairs: set[tuple[str, str]], layer: str, reason: str) -> bool:
    return (layer, reason) in reason_pairs


def _has_layer_reason_containing(
    reason_pairs: set[tuple[str, str]],
    layer: str,
    *needles: str,
) -> bool:
    needle_values = tuple(str(needle).strip().lower() for needle in needles if str(needle).strip())
    if not needle_values:
        return False
    for parsed_layer, parsed_reason in reason_pairs:
        if parsed_layer != layer:
            continue
        reason_text = parsed_reason.lower()
        if any(needle in reason_text for needle in needle_values):
            return True
    return False


def _legacy_reason_contains(*values: str | None, needles: str) -> bool:
    needle_values = tuple(part.strip().lower() for part in needles.split("|") if part.strip())
    return any(
        any(needle in value.lower() for needle in needle_values)
        for value in values
        if value
    )


def _load_context_from_row(row: Any) -> tuple[dict[str, Any], str]:
    try:
        raw = row["context_json"]
    except (KeyError, IndexError, TypeError):
        return {}, "missing"
    if raw in (None, ""):
        return {}, "missing"
    if isinstance(raw, dict):
        return dict(raw), "ok"
    try:
        loaded = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}, "malformed"
    if not isinstance(loaded, dict):
        return {}, "malformed"
    return loaded, "ok"


def normalize_decision_attribution_from_context(context: dict[str, Any]) -> DecisionAttribution:
    entry = _dict(context.get("entry"))
    signal_flow = _dict(context.get("signal_flow"))
    filters = _dict(context.get("filters"))
    cost_edge = _dict(filters.get("cost_edge"))
    position_gate = _dict(context.get("position_gate"))
    decision_summary = _dict(context.get("decision_summary"))
    execution_decision = _dict(context.get("execution_decision"))
    target_plan = _dict(context.get("target_submit_plan"))
    target_shadow = _dict(context.get("target_position_shadow"))
    signal_strength = _dict(context.get("signal_strength"))
    features = _dict(context.get("features"))
    pre_trade_economics = _dict(context.get("pre_trade_economics"))
    all_block_reasons = normalize_all_block_reasons(context.get("all_block_reasons"))
    nested_all_block_reasons = normalize_all_block_reasons(signal_flow.get("all_block_reasons"))
    if nested_all_block_reasons:
        all_block_reasons = normalize_all_block_reasons((*all_block_reasons, *nested_all_block_reasons))

    raw_signal = _signal(
        context.get("raw_signal")
        or context.get("base_signal")
        or entry.get("base_signal")
        or signal_flow.get("base_signal")
    )
    final_signal = _signal(
        context.get("final_signal")
        or signal_flow.get("final_signal")
        or context.get("signal")
        or signal_flow.get("final_action"),
        default=raw_signal,
    )
    submit_expected = _bool_or_none(
        context.get("submit_expected")
        if "submit_expected" in context
        else decision_summary.get("submit_expected")
        if "submit_expected" in decision_summary
        else execution_decision.get("submit_expected")
        if "submit_expected" in execution_decision
        else target_plan.get("submit_expected")
    )
    execution_block_reason = _optional_text(
        context.get("execution_block_reason")
        or decision_summary.get("execution_block_reason")
        or execution_decision.get("block_reason")
    )
    target_block_reason = _optional_text(
        context.get("target_block_reason")
        or target_plan.get("block_reason")
        or target_shadow.get("target_block_reason")
    )
    explicit_primary_block_layer = _first_text(
        context.get("primary_block_layer"),
        signal_flow.get("primary_block_layer"),
        default="none",
    )
    explicit_primary_block_reason = _first_text(
        context.get("primary_block_reason"),
        signal_flow.get("primary_block_reason"),
        context.get("entry_block_reason"),
        default="none",
    )
    derived_primary_block = select_primary_block_from_all_reasons(all_block_reasons)
    has_explicit_primary = (
        explicit_primary_block_layer != "none"
        and explicit_primary_block_reason != "none"
    )
    if has_explicit_primary:
        primary_block_layer = explicit_primary_block_layer
        primary_block_reason = explicit_primary_block_reason
    elif derived_primary_block is not None:
        primary_block_layer, primary_block_reason = derived_primary_block
    else:
        primary_block_layer = "none"
        primary_block_reason = "none"
    primary_all_block_conflict = (
        has_explicit_primary
        and derived_primary_block is not None
        and derived_primary_block != (explicit_primary_block_layer, explicit_primary_block_reason)
    )
    if not all_block_reasons and primary_block_layer != "none" and primary_block_reason != "none":
        all_block_reasons = (f"{primary_block_layer}.{primary_block_reason}",)
    reason_pairs = _reason_pairs(all_block_reasons)

    entry_allowed = _bool_or_none(position_gate.get("entry_allowed"))
    canonical_cost_filter = (
        _has_reason(reason_pairs, "strategy_filters", "cost_edge")
        or _has_reason(reason_pairs, "pre_trade_economics", "net_edge_below_minimum")
        or _has_layer_reason_containing(
            reason_pairs,
            "strategy_filters",
            "cost_edge",
            "edge_below",
        )
        or _has_layer_reason_containing(
            reason_pairs,
            "pre_trade_economics",
            "cost_edge",
            "edge_below",
            "net_edge_below_minimum",
        )
    )
    canonical_fee_authority = _has_layer(reason_pairs, "fee_authority")
    canonical_position_gate = _has_layer(reason_pairs, "position_gate")
    canonical_order_rule = _has_layer(reason_pairs, "execution_order_rule")
    canonical_performance_gate = _has_layer(reason_pairs, "performance_gate")

    explicit_cost_filter = _bool_or_none(context.get("blocked_by_cost_filter"))
    explicit_fee_authority = _bool_or_none(context.get("blocked_by_fee_authority"))
    explicit_position_gate = _bool_or_none(context.get("blocked_by_position_gate"))
    explicit_order_rule = _bool_or_none(context.get("blocked_by_order_rule"))
    explicit_performance_gate = _bool_or_none(context.get("blocked_by_performance_gate"))

    nested_cost_filter = cost_edge.get("blocked") is True
    nested_position_gate = raw_signal == "BUY" and entry_allowed is False
    legacy_order_rule = _legacy_reason_contains(
        execution_block_reason,
        target_block_reason,
        needles="order_rule|min_notional|min_qty",
    )
    legacy_performance_gate = _legacy_reason_contains(
        execution_block_reason,
        primary_block_reason,
        needles="performance_gate",
    )
    blocked_by_cost_filter = (
        canonical_cost_filter
        or (explicit_cost_filter is True)
        or nested_cost_filter
        or (
            not all_block_reasons
            and _legacy_reason_contains(
                execution_block_reason,
                target_block_reason,
                primary_block_reason,
                needles="cost_edge|edge_below|net_edge_below_minimum",
            )
        )
    )
    blocked_by_fee_authority = (
        canonical_fee_authority
        or (explicit_fee_authority is True)
        or (
            not all_block_reasons
            and _legacy_reason_contains(
                execution_block_reason,
                primary_block_layer,
                primary_block_reason,
                needles="fee_authority",
            )
        )
    )
    blocked_by_position_gate = (
        canonical_position_gate
        or (explicit_position_gate is True)
        or nested_position_gate
    )
    blocked_by_order_rule = (
        canonical_order_rule
        or (explicit_order_rule is True)
        or (not all_block_reasons and legacy_order_rule)
    )
    blocked_by_performance_gate = (
        canonical_performance_gate
        or (explicit_performance_gate is True)
        or (not all_block_reasons and legacy_performance_gate)
    )

    experiment_fingerprint = context.get("experiment_fingerprint")
    if isinstance(experiment_fingerprint, dict):
        experiment_fingerprint_text = json.dumps(experiment_fingerprint, sort_keys=True, separators=(",", ":"))
    else:
        experiment_fingerprint_text = _optional_text(experiment_fingerprint)

    return DecisionAttribution(
        raw_signal=raw_signal,
        final_signal=final_signal,
        decision_type=_first_text(context.get("decision_type"), default="unknown"),
        base_reason=_first_text(context.get("base_reason"), entry.get("base_reason"), default="unknown"),
        entry_reason=_first_text(context.get("entry_reason"), entry.get("entry_reason"), default="unknown"),
        entry_block_reason=_optional_text(context.get("entry_block_reason")),
        primary_block_layer=primary_block_layer,
        primary_block_reason=primary_block_reason,
        all_block_reasons=all_block_reasons,
        blocked_by_cost_filter=blocked_by_cost_filter,
        blocked_by_fee_authority=blocked_by_fee_authority,
        blocked_by_position_gate=blocked_by_position_gate,
        blocked_by_order_rule=blocked_by_order_rule,
        blocked_by_performance_gate=blocked_by_performance_gate,
        gap_ratio=_first_float(context.get("gap_ratio"), signal_strength.get("gap_ratio"), cost_edge.get("value"), features.get("sma_gap_ratio")),
        required_edge_ratio=_first_float(
            context.get("required_edge_ratio"),
            signal_strength.get("required_edge_ratio"),
            cost_edge.get("threshold"),
            pre_trade_economics.get("required_edge_ratio"),
        ),
        signal_strength_label=_first_text(
            context.get("signal_strength_label"),
            signal_strength.get("label"),
            default="unknown",
        ),
        submit_expected=submit_expected,
        execution_block_reason=execution_block_reason,
        target_block_reason=target_block_reason,
        experiment_fingerprint=experiment_fingerprint_text,
        primary_all_block_conflict=primary_all_block_conflict,
    )


def normalize_decision_attribution_from_row(row: sqlite3.Row | Any) -> DecisionAttribution:
    context, status = _load_context_from_row(row)
    if context:
        attribution = normalize_decision_attribution_from_context(context)
    else:
        try:
            fallback_signal = row["signal"]
        except (KeyError, IndexError, TypeError):
            fallback_signal = None
        attribution = DecisionAttribution(
            raw_signal=_signal(fallback_signal),
            final_signal=_signal(fallback_signal),
            decision_type="unknown",
            base_reason="unknown",
            entry_reason="unknown",
            entry_block_reason=None,
            primary_block_layer="none",
            primary_block_reason="none",
            all_block_reasons=(),
            blocked_by_cost_filter=False,
            blocked_by_fee_authority=False,
            blocked_by_position_gate=False,
            blocked_by_order_rule=False,
            blocked_by_performance_gate=False,
            gap_ratio=None,
            required_edge_ratio=None,
            signal_strength_label="unknown",
            submit_expected=None,
            execution_block_reason=None,
            target_block_reason=None,
            context_status=status,
        )
    if status == "ok":
        return attribution
    return DecisionAttribution(**{**attribution.__dict__, "context_status": status})


def _sorted_counts(counter: Counter[str]) -> dict[str, int]:
    return {key: int(counter[key]) for key in sorted(counter)}


def _ratio(count: int, total: int) -> float:
    return 0.0 if total <= 0 else count / float(total)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * pct)))
    return ordered[index]


def _edge_stats(rows: list[DecisionAttribution]) -> dict[str, float | None]:
    gaps = [float(row.gap_ratio) for row in rows if row.gap_ratio is not None]
    required = [float(row.required_edge_ratio) for row in rows if row.required_edge_ratio is not None]
    comparable = [
        row for row in rows if row.gap_ratio is not None and row.required_edge_ratio is not None
    ]
    below_count = sum(1 for row in comparable if float(row.gap_ratio) < float(row.required_edge_ratio))
    return {
        "gap_ratio_avg": None if not gaps else sum(gaps) / float(len(gaps)),
        "gap_ratio_median": None if not gaps else float(median(gaps)),
        "gap_ratio_p90": _percentile(gaps, 0.9),
        "required_edge_ratio_avg": None if not required else sum(required) / float(len(required)),
        "required_edge_ratio_median": None if not required else float(median(required)),
        "gap_lt_required_ratio": None if not comparable else below_count / float(len(comparable)),
    }


@dataclass
class DecisionAttributionAccumulator:
    sample_count: int = 0
    malformed_context_count: int = 0
    context_missing_count: int = 0
    raw_signal_counts: Counter[str] = field(default_factory=Counter)
    final_signal_counts: Counter[str] = field(default_factory=Counter)
    decision_type_counts: Counter[str] = field(default_factory=Counter)
    block_layer_counts: Counter[str] = field(default_factory=Counter)
    block_reason_counts: Counter[str] = field(default_factory=Counter)
    entry_reason_counts: Counter[str] = field(default_factory=Counter)
    entry_block_reason_counts: Counter[str] = field(default_factory=Counter)
    signal_strength_counts: Counter[str] = field(default_factory=Counter)
    filter_counts: Counter[str] = field(default_factory=Counter)
    submit_expected_buy_count: int = 0
    submit_expected_sell_count: int = 0
    submit_mismatch_buy_count: int = 0
    submit_mismatch_sell_count: int = 0
    all_block_reasons_present_count: int = 0
    primary_block_present_count: int = 0
    primary_all_block_conflict_count: int = 0
    gap_ratios: list[float] = field(default_factory=list)
    required_edge_ratios: list[float] = field(default_factory=list)
    comparable_edge_count: int = 0
    gap_lt_required_count: int = 0

    def add(self, row: DecisionAttribution) -> None:
        self.sample_count += 1
        self.raw_signal_counts[row.raw_signal] += 1
        self.final_signal_counts[row.final_signal] += 1
        self.decision_type_counts[row.decision_type] += 1
        if row.primary_block_layer != "none":
            self.block_layer_counts[row.primary_block_layer] += 1
        self.block_reason_counts.update(row.all_block_reasons)
        self.entry_reason_counts[row.entry_reason] += 1
        self.entry_block_reason_counts[row.entry_block_reason or "none"] += 1
        self.signal_strength_counts[row.signal_strength_label] += 1
        if row.context_status == "malformed":
            self.malformed_context_count += 1
        if row.context_status == "missing":
            self.context_missing_count += 1
        if row.final_signal == "BUY" and row.submit_expected is True:
            self.submit_expected_buy_count += 1
        if row.final_signal == "SELL" and row.submit_expected is True:
            self.submit_expected_sell_count += 1
        if row.final_signal == "BUY" and row.submit_expected is False:
            self.submit_mismatch_buy_count += 1
        if row.final_signal == "SELL" and row.submit_expected is False:
            self.submit_mismatch_sell_count += 1
        for key in FILTER_KEYS:
            if getattr(row, key):
                self.filter_counts[key] += 1
        if row.all_block_reasons:
            self.all_block_reasons_present_count += 1
        if row.primary_block_layer != "none" and row.primary_block_reason != "none":
            self.primary_block_present_count += 1
        if row.primary_all_block_conflict:
            self.primary_all_block_conflict_count += 1
        if row.gap_ratio is not None:
            self.gap_ratios.append(float(row.gap_ratio))
        if row.required_edge_ratio is not None:
            self.required_edge_ratios.append(float(row.required_edge_ratio))
        if row.gap_ratio is not None and row.required_edge_ratio is not None:
            self.comparable_edge_count += 1
            if float(row.gap_ratio) < float(row.required_edge_ratio):
                self.gap_lt_required_count += 1

    def _edge_stats(self) -> dict[str, float | None]:
        return {
            "gap_ratio_avg": None if not self.gap_ratios else sum(self.gap_ratios) / float(len(self.gap_ratios)),
            "gap_ratio_median": None if not self.gap_ratios else float(median(self.gap_ratios)),
            "gap_ratio_p90": _percentile(self.gap_ratios, 0.9),
            "required_edge_ratio_avg": (
                None
                if not self.required_edge_ratios
                else sum(self.required_edge_ratios) / float(len(self.required_edge_ratios))
            ),
            "required_edge_ratio_median": (
                None if not self.required_edge_ratios else float(median(self.required_edge_ratios))
            ),
            "gap_lt_required_ratio": (
                None
                if self.comparable_edge_count <= 0
                else self.gap_lt_required_count / float(self.comparable_edge_count)
            ),
        }

    def summary(self) -> DecisionAttributionSummary:
        candidate_funnel = {
            "raw_BUY": self.raw_signal_counts.get("BUY", 0),
            "final_BUY": self.final_signal_counts.get("BUY", 0),
            "submit_expected_BUY": self.submit_expected_buy_count,
            "raw_SELL": self.raw_signal_counts.get("SELL", 0),
            "final_SELL": self.final_signal_counts.get("SELL", 0),
            "submit_expected_SELL": self.submit_expected_sell_count,
        }
        filter_ratios = {
            f"{key}_ratio": _ratio(self.filter_counts.get(key, 0), self.sample_count)
            for key in FILTER_KEYS
        }
        submit_mismatch = {
            "final_BUY_submit_expected_false": self.submit_mismatch_buy_count,
            "final_SELL_submit_expected_false": self.submit_mismatch_sell_count,
        }
        schema_quality = {
            "all_block_reasons_present_count": self.all_block_reasons_present_count,
            "primary_block_present_count": self.primary_block_present_count,
            "primary_all_block_conflict_count": self.primary_all_block_conflict_count,
        }
        edge_stats = self._edge_stats()
        partial = {
            "sample_count": self.sample_count,
            "malformed_context_count": self.malformed_context_count,
            "context_missing_count": self.context_missing_count,
            "candidate_funnel": candidate_funnel,
            "filter_ratios": filter_ratios,
            "edge_stats": edge_stats,
            "submit_mismatch": submit_mismatch,
        }
        return DecisionAttributionSummary(
            sample_count=self.sample_count,
            malformed_context_count=self.malformed_context_count,
            context_missing_count=self.context_missing_count,
            raw_signal_counts=_sorted_counts(self.raw_signal_counts),
            final_signal_counts=_sorted_counts(self.final_signal_counts),
            decision_type_counts=_sorted_counts(self.decision_type_counts),
            candidate_funnel=candidate_funnel,
            block_layer_counts=_sorted_counts(self.block_layer_counts),
            block_reason_counts=_sorted_counts(self.block_reason_counts),
            entry_reason_counts=_sorted_counts(self.entry_reason_counts),
            entry_block_reason_counts=_sorted_counts(self.entry_block_reason_counts),
            filter_ratios=filter_ratios,
            edge_stats=edge_stats,
            signal_strength_counts=_sorted_counts(self.signal_strength_counts),
            submit_mismatch=submit_mismatch,
            schema_quality=schema_quality,
            interpretation=_interpret(partial),
        )


def _interpret(summary: dict[str, Any]) -> dict[str, str]:
    sample_count = int(summary["sample_count"])
    if sample_count <= 0:
        return {"primary_issue": "no_matching_decisions", "secondary_issue": "none"}

    malformed_ratio = _ratio(
        int(summary["malformed_context_count"]) + int(summary["context_missing_count"]),
        sample_count,
    )
    raw_buy = int(summary["candidate_funnel"].get("raw_BUY", 0))
    raw_sell = int(summary["candidate_funnel"].get("raw_SELL", 0))
    final_buy = int(summary["candidate_funnel"].get("final_BUY", 0))
    final_sell = int(summary["candidate_funnel"].get("final_SELL", 0))
    cost_ratio = float(summary["filter_ratios"].get("blocked_by_cost_filter_ratio", 0.0))
    gap_below = summary["edge_stats"].get("gap_lt_required_ratio")
    mismatch_buy = int(summary["submit_mismatch"].get("final_BUY_submit_expected_false", 0))
    mismatch_sell = int(summary["submit_mismatch"].get("final_SELL_submit_expected_false", 0))

    issues: list[str] = []
    if malformed_ratio >= 0.2:
        issues.append("observability_schema_incomplete")
    if raw_buy <= max(1, int(sample_count * 0.05)) and raw_sell <= max(1, int(sample_count * 0.05)):
        issues.append("raw_signal_scarcity")
    elif raw_buy <= max(1, int(sample_count * 0.05)):
        issues.append("raw_buy_scarcity")
    if raw_buy > 0 and final_buy <= max(0, int(raw_buy * 0.1)) and cost_ratio >= 0.25:
        issues.append("entry_edge_insufficient_or_cost_filter_strict")
    if raw_buy > 0 and gap_below is not None and float(gap_below) >= 0.5:
        issues.append("gap_below_required_edge")
    if final_buy > 0 and mismatch_buy > 0:
        issues.append("execution_intent_or_order_rule_block")
    if final_sell > 0 and mismatch_sell > 0:
        issues.append("sell_execution_intent_or_order_rule_block")
    if not issues:
        issues.append("no_dominant_issue_detected")

    secondary = "none"
    for issue in issues[1:]:
        if issue != issues[0]:
            secondary = issue
            break
    return {"primary_issue": issues[0], "secondary_issue": secondary}


def summarize_decision_attributions(
    rows: Iterable[DecisionAttribution],
) -> DecisionAttributionSummary:
    accumulator = DecisionAttributionAccumulator()
    for row in rows:
        accumulator.add(row)
    return accumulator.summary()


def fetch_decision_attribution_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
) -> list[DecisionAttribution]:
    query = """
        SELECT signal, reason, decision_ts, candle_ts, context_json
        FROM strategy_decisions
        WHERE 1=1
    """
    params: list[object] = []
    if from_ts_ms is not None:
        query += " AND decision_ts >= ?"
        params.append(int(from_ts_ms))
    if to_ts_ms is not None:
        query += " AND decision_ts <= ?"
        params.append(int(to_ts_ms))
    if pair:
        query += " AND (context_json LIKE ? OR context_json LIKE ?)"
        params.append(f'%"pair": "{pair}"%')
        params.append(f'%"pair":"{pair}"%')
    if interval:
        query += " AND (context_json LIKE ? OR context_json LIKE ?)"
        params.append(f'%"interval": "{interval}"%')
        params.append(f'%"interval":"{interval}"%')
    query += " ORDER BY decision_ts DESC, rowid DESC LIMIT ?"
    params.append(max(1, int(limit)))
    try:
        rows = conn.execute(query, tuple(params)).fetchall()
    except sqlite3.OperationalError:
        return []
    return [normalize_decision_attribution_from_row(row) for row in rows]


def build_decision_attribution_summary_from_db(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
) -> DecisionAttributionSummary:
    return summarize_decision_attributions(
        fetch_decision_attribution_rows(
            conn,
            limit=limit,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            pair=pair,
            interval=interval,
        )
    )


def _fmt_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{float(value):.4f}"


def format_decision_attribution_summary(summary: DecisionAttributionSummary) -> str:
    payload = summary.as_dict()
    lines = [
        "[DECISION ATTRIBUTION]",
        "",
        f"sample_count={summary.sample_count}",
        f"malformed_context_count={summary.malformed_context_count}",
        f"context_missing_count={summary.context_missing_count}",
        "",
    ]

    def add_counts(title: str, counts: dict[str, int]) -> None:
        lines.append(f"{title}:")
        if counts:
            for key, count in counts.items():
                lines.append(f"  {key} {count}")
        else:
            lines.append("  none 0")
        lines.append("")

    add_counts("raw_signal", summary.raw_signal_counts)
    add_counts("final_signal", summary.final_signal_counts)
    add_counts("decision_type", summary.decision_type_counts)
    lines.append("candidate_funnel:")
    for key, value in summary.candidate_funnel.items():
        lines.append(f"  {key}={value}")
    lines.append("")
    add_counts("block_layers", summary.block_layer_counts)
    add_counts("block_reasons", summary.block_reason_counts)
    add_counts("entry_reasons", summary.entry_reason_counts)
    add_counts("entry_block_reasons", summary.entry_block_reason_counts)
    lines.append("filters:")
    for key, value in summary.filter_ratios.items():
        lines.append(f"  {key}={_fmt_ratio(value)}")
    lines.append("")
    lines.append("edge:")
    for key, value in summary.edge_stats.items():
        lines.append(f"  {key}={_fmt_ratio(value)}")
    lines.append("")
    add_counts("signal_strength", summary.signal_strength_counts)
    lines.append("submit_mismatch:")
    for key, value in summary.submit_mismatch.items():
        lines.append(f"  {key}={value}")
    lines.append("")
    lines.append("schema_quality:")
    for key, value in summary.schema_quality.items():
        lines.append(f"  {key}={value}")
    lines.append("")
    lines.append("interpretation:")
    interpretation = payload["interpretation"]
    lines.append(f"  primary_issue={interpretation['primary_issue']}")
    lines.append(f"  secondary_issue={interpretation['secondary_issue']}")
    return "\n".join(lines)


def decision_attribution_summary_json(summary: DecisionAttributionSummary) -> str:
    return json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, sort_keys=True)
