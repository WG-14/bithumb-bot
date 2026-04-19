from __future__ import annotations

import math

KNOWN_RULE_SOURCES = frozenset(
    {
        "chance_doc",
        "local_fallback",
        "merged",
        "unsupported_by_doc",
        "missing",
    }
)


def side_min_total_krw(*, rules, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        if float(rules.bid_min_total_krw) > 0:
            return float(rules.bid_min_total_krw)
    elif normalized_side == "SELL":
        if float(rules.ask_min_total_krw) > 0:
            return float(rules.ask_min_total_krw)
    return float(rules.min_notional_krw)


def side_price_unit(*, rules, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        return float(rules.bid_price_unit)
    if normalized_side == "SELL":
        return float(rules.ask_price_unit)
    return 0.0


def normalize_limit_price_for_side(*, price: float, side: str, rules) -> float:
    value = float(price)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"limit price must be > 0 (got {price})")

    unit = side_price_unit(rules=rules, side=side)
    if not math.isfinite(unit) or unit <= 0:
        return value

    steps = value / unit
    epsilon = 1e-12
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        aligned_steps = math.ceil(steps - epsilon)
    else:
        aligned_steps = math.floor(steps + epsilon)
    return float(aligned_steps * unit)


def required_rule_issues(rules) -> list[str]:
    issues: list[str] = []
    if not math.isfinite(float(rules.min_qty)) or float(rules.min_qty) <= 0:
        issues.append(f"min_qty must be > 0 (got {rules.min_qty})")
    if not math.isfinite(float(rules.qty_step)) or float(rules.qty_step) <= 0:
        issues.append(f"qty_step must be > 0 (got {rules.qty_step})")
    if not math.isfinite(float(rules.min_notional_krw)) or float(rules.min_notional_krw) <= 0:
        issues.append(f"min_notional_krw must be > 0 (got {rules.min_notional_krw})")
    if int(rules.max_qty_decimals) <= 0:
        issues.append(f"max_qty_decimals must be > 0 (got {rules.max_qty_decimals})")
    return issues


def rule_source_for(field: str, source: dict[str, str] | None) -> str:
    if not source:
        return "missing"
    normalized = str(source.get(field, "")).strip() or "missing"
    if normalized == "manual_config":
        normalized = "local_fallback"
    return normalized if normalized in KNOWN_RULE_SOURCES else "missing"


def required_rule_source_issues(
    source: dict[str, str] | None, *, require_price_unit_sources: bool = True
) -> list[str]:
    issues: list[str] = []
    doc_required_fields: tuple[str, ...] = (
        "bid_min_total_krw",
        "ask_min_total_krw",
    )
    if require_price_unit_sources:
        doc_required_fields += ("bid_price_unit", "ask_price_unit")
    for field in doc_required_fields:
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            issues.append(
                f"{field} source must be chance_doc for MODE=live "
                f"(got {field_source})"
            )
    return issues


def optional_rule_source_warnings(source: dict[str, str] | None) -> list[str]:
    warnings: list[str] = []
    for field in ("bid_price_unit", "ask_price_unit"):
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            warnings.append(
                f"{field} source is {field_source}; limit price tick normalization may be pass-through"
            )
    return warnings
