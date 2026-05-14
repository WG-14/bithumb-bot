from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .order_semantics import (
    CANONICAL_LIMIT_QTY_PRICE,
    CANONICAL_MARKET_BUY_QUOTE_NOTIONAL,
    CANONICAL_MARKET_SELL_BASE_QTY,
    classify_order_semantics,
)
from .execution_reality_contract import contract_hash_matches
from .research.experiment_manifest import load_manifest


QUALITY_WITHIN_MODEL = "within_model"
QUALITY_DEGRADED = "degraded"
QUALITY_MODEL_BREACH = "model_breach"
QUALITY_INSUFFICIENT_EVIDENCE = "insufficient_evidence"
GATE_PASS = "PASS"
GATE_WARN = "WARN"
GATE_FAIL = "FAIL"
GATE_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
ORDER_TYPE_COST_DELTA_INSUFFICIENT = "insufficient_order_type_samples"
ORDER_TYPE_COST_DELTA_MARKET_FAST_COSTLY = "market_fills_faster_but_costs_more"
ORDER_TYPE_COST_DELTA_MARKET_FAST_CHEAPER = "market_fills_faster_and_costs_less"
ORDER_TYPE_COST_DELTA_LIMIT_FAST_COSTLY = "limit_fills_faster_but_costs_more"
ORDER_TYPE_COST_DELTA_LIMIT_FAST_CHEAPER = "limit_fills_faster_and_costs_less"
ORDER_TYPE_COST_DELTA_MARKET_COSTS_MORE = "market_costs_more"
ORDER_TYPE_COST_DELTA_LIMIT_COSTS_MORE = "limit_costs_more"
ORDER_TYPE_COST_DELTA_NO_MATERIAL_DIFFERENCE = "no_material_order_type_difference"
ORDER_TYPE_COST_DELTA_ONE_TYPE_ONLY = "one_order_type_only"


@dataclass(frozen=True)
class ExecutionQualityThresholds:
    min_sample: int = 30
    max_p90_slippage_bps: float = 20.0
    max_p95_full_fill_latency_ms: float = 3000.0
    max_partial_fill_rate: float = 0.05
    max_model_breach_rate: float = 0.10


@dataclass(frozen=True)
class ExecutionQualityRecord:
    client_order_id: str
    submit_attempt_id: str | None
    decision_id: int | None
    strategy_name: str | None
    mode: str | None
    market: str | None
    side: str | None
    order_type: str | None
    exchange: str | None
    submit_contract_kind: str | None
    canonical_execution_kind: str
    semantic_evidence_quality: str
    market_equivalent: bool
    legacy_unknown_order_type: bool
    unsupported_unknown_order_type: bool
    exchange_order_id: str | None
    execution_reality_contract: dict[str, Any] | None
    execution_contract_hash: str | None
    execution_contract_hash_valid: bool | None
    execution_contract_mismatch_reason: str | None
    signal_ts_ms: int | None
    signal_reference_price: float | None
    signal_best_bid: float | None
    signal_best_ask: float | None
    signal_spread_bps: float | None
    submit_plan_ts_ms: int | None
    submit_sent_ts_ms: int | None
    submit_response_ts_ms: int | None
    submit_reference_price: float | None
    submit_best_bid: float | None
    submit_best_ask: float | None
    submit_spread_bps: float | None
    first_fill_ts_ms: int | None
    last_fill_ts_ms: int | None
    avg_fill_price: float | None
    filled_qty: float
    requested_qty: float | None
    remaining_qty: float | None
    remaining_notional_krw: float | None
    internal_target_remaining_qty: float | None
    internal_target_residue_material: bool
    internal_target_residue_reason: str
    exchange_submit_notional_krw: float | None
    exchange_spent_quote_krw: float | None
    exchange_remaining_quote_krw: float | None
    exchange_fill_completion_ratio: float | None
    qty_step: float | None
    effective_min_trade_qty: float | None
    min_notional_krw: float | None
    fee: float | None
    realized_fee_rate: float | None
    submit_latency_ms: int | None
    response_latency_ms: int | None
    first_fill_latency_ms: int | None
    full_fill_latency_ms: int | None
    slippage_vs_signal_bps: float | None
    slippage_vs_submit_ref_bps: float | None
    slippage_vs_best_quote_bps: float | None
    fill_ratio: float | None
    partial_fill_flag: bool
    unfilled_flag: bool
    material_partial_fill_flag: bool
    material_unfilled_flag: bool
    remaining_qty_materiality_reason: str
    quality_status: str
    quality_reason: str
    backtest_assumed_slippage_bps: float | None
    model_breach_flag: bool | None


def finite_positive(value: object) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or out <= 0:
        return None
    return out


def finite_non_negative(value: object) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or out < 0:
        return None
    return out


def side_aware_slippage_bps(*, side: str | None, reference_price: object, fill_price: object) -> float | None:
    ref = finite_positive(reference_price)
    fill = finite_positive(fill_price)
    if ref is None or fill is None:
        return None
    normalized_side = str(side or "").upper()
    if normalized_side == "BUY":
        return ((fill - ref) / ref) * 10_000.0
    if normalized_side == "SELL":
        return ((ref - fill) / ref) * 10_000.0
    return None


def spread_bps(*, best_bid: object, best_ask: object) -> float | None:
    bid = finite_positive(best_bid)
    ask = finite_positive(best_ask)
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def best_quote_for_side(*, side: str | None, best_bid: object, best_ask: object) -> float | None:
    normalized_side = str(side or "").upper()
    if normalized_side == "BUY":
        return finite_positive(best_ask)
    if normalized_side == "SELL":
        return finite_positive(best_bid)
    return None


def latency_ms(*, start_ms: object, end_ms: object) -> int | None:
    try:
        start = int(start_ms)  # type: ignore[arg-type]
        end = int(end_ms)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if start <= 0 or end <= 0 or end < start:
        return None
    return end - start


def weighted_average_price(fills: list[sqlite3.Row]) -> float | None:
    total_qty = 0.0
    notional = 0.0
    for fill in fills:
        qty = finite_non_negative(fill["qty"])
        price = finite_positive(fill["price"])
        if qty is None or price is None or qty <= 0:
            continue
        total_qty += qty
        notional += qty * price
    if total_qty <= 0:
        return None
    return notional / total_qty


def percentile(values: list[float], pct: float) -> float | None:
    clean = sorted(float(v) for v in values if math.isfinite(float(v)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * (float(pct) / 100.0)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return clean[lower]
    weight = rank - lower
    return clean[lower] + ((clean[upper] - clean[lower]) * weight)


def _normalized_order_type(value: object) -> str:
    semantics = classify_order_semantics(raw_order_type=value, side=None)
    if semantics.market_equivalent:
        return "market"
    if semantics.limit_equivalent:
        return "limit"
    return "unknown"


def _execution_kind_matches(row: ExecutionQualityRecord, order_type: str) -> bool:
    if order_type == "market":
        return bool(row.market_equivalent)
    if order_type == "limit":
        return row.canonical_execution_kind == CANONICAL_LIMIT_QTY_PRICE
    return False


def _order_type_metrics(records: list[ExecutionQualityRecord], order_type: str) -> dict[str, object]:
    typed = [row for row in records if _execution_kind_matches(row, order_type)]
    count = len(typed)
    slippage = [float(row.slippage_vs_signal_bps) for row in typed if row.slippage_vs_signal_bps is not None]
    latency = [float(row.full_fill_latency_ms) for row in typed if row.full_fill_latency_ms is not None]
    prefix = f"{order_type}_"
    return {
        f"{prefix}order_count": count,
        f"{prefix}median_slippage_bps": percentile(slippage, 50),
        f"{prefix}p90_slippage_bps": percentile(slippage, 90),
        f"{prefix}p95_slippage_bps": percentile(slippage, 95),
        f"{prefix}median_submit_to_fill_ms": percentile(latency, 50),
        f"{prefix}p90_submit_to_fill_ms": percentile(latency, 90),
        f"{prefix}p95_submit_to_fill_ms": percentile(latency, 95),
        f"{prefix}partial_fill_rate": (
            sum(1 for row in typed if row.material_partial_fill_flag) / count if count else 0.0
        ),
        f"{prefix}unfilled_rate": (sum(1 for row in typed if row.material_unfilled_flag) / count if count else 0.0),
        f"{prefix}raw_partial_fill_rate": (sum(1 for row in typed if row.partial_fill_flag) / count if count else 0.0),
        f"{prefix}raw_unfilled_rate": (sum(1 for row in typed if row.unfilled_flag) / count if count else 0.0),
    }


@dataclass(frozen=True)
class ExecutionRemainder:
    requested_qty: float | None
    filled_qty: float
    remaining_qty: float | None
    remaining_notional_krw: float | None
    qty_step: float | None
    effective_min_trade_qty: float | None
    min_notional_krw: float | None
    is_material_remaining: bool
    materiality_reason: str


def assess_execution_remainder(
    *,
    requested_qty: object,
    filled_qty: object,
    remaining_qty: object,
    reference_price: object,
    qty_step: object,
    effective_min_trade_qty: object,
    min_notional_krw: object,
) -> ExecutionRemainder:
    requested = finite_non_negative(requested_qty)
    filled = finite_non_negative(filled_qty) or 0.0
    remaining = finite_non_negative(remaining_qty)
    step = finite_positive(qty_step)
    min_qty = finite_positive(effective_min_trade_qty)
    min_notional = finite_positive(min_notional_krw)
    ref_price = finite_positive(reference_price)
    remaining_notional = None
    if remaining is not None and ref_price is not None:
        remaining_notional = remaining * ref_price

    if requested is None or requested <= 0:
        return ExecutionRemainder(
            requested_qty=requested,
            filled_qty=filled,
            remaining_qty=remaining,
            remaining_notional_krw=remaining_notional,
            qty_step=step,
            effective_min_trade_qty=min_qty,
            min_notional_krw=min_notional,
            is_material_remaining=False,
            materiality_reason="requested_qty_missing",
        )
    if remaining is None or remaining <= 0:
        return ExecutionRemainder(
            requested_qty=requested,
            filled_qty=filled,
            remaining_qty=remaining,
            remaining_notional_krw=remaining_notional,
            qty_step=step,
            effective_min_trade_qty=min_qty,
            min_notional_krw=min_notional,
            is_material_remaining=False,
            materiality_reason="no_remaining_qty",
        )
    if step is not None and remaining < step:
        reason = "remaining_qty_below_qty_step"
        material = False
    elif min_qty is not None and remaining < min_qty:
        reason = "remaining_qty_below_effective_min_trade_qty"
        material = False
    elif min_notional is not None and remaining_notional is not None and remaining_notional < min_notional:
        reason = "remaining_notional_below_min_notional_krw"
        material = False
    elif min_notional is not None and remaining_notional is None:
        reason = "material_executable_remaining_qty_notional_unknown"
        material = True
    else:
        reason = "material_executable_remaining_qty"
        material = True
    return ExecutionRemainder(
        requested_qty=requested,
        filled_qty=filled,
        remaining_qty=remaining,
        remaining_notional_krw=remaining_notional,
        qty_step=step,
        effective_min_trade_qty=min_qty,
        min_notional_krw=min_notional,
        is_material_remaining=material,
        materiality_reason=reason,
    )


def _classify_order_type_cost_delta(summary: dict[str, object]) -> str:
    market_count = int(summary.get("market_order_count") or 0)
    limit_count = int(summary.get("limit_order_count") or 0)
    if (market_count > 0) != (limit_count > 0):
        return ORDER_TYPE_COST_DELTA_ONE_TYPE_ONLY
    if market_count == 0 and limit_count == 0:
        return ORDER_TYPE_COST_DELTA_INSUFFICIENT

    market_slippage = summary.get("market_p90_slippage_bps")
    limit_slippage = summary.get("limit_p90_slippage_bps")
    market_latency = summary.get("market_p95_submit_to_fill_ms")
    limit_latency = summary.get("limit_p95_submit_to_fill_ms")
    if not all(isinstance(value, (int, float)) for value in (market_slippage, limit_slippage, market_latency, limit_latency)):
        return ORDER_TYPE_COST_DELTA_INSUFFICIENT

    slippage_delta = float(market_slippage) - float(limit_slippage)
    latency_delta = float(market_latency) - float(limit_latency)
    slippage_epsilon_bps = 0.1
    latency_epsilon_ms = 1.0
    market_costs_more = slippage_delta > slippage_epsilon_bps
    limit_costs_more = slippage_delta < -slippage_epsilon_bps
    market_faster = latency_delta < -latency_epsilon_ms
    limit_faster = latency_delta > latency_epsilon_ms

    if market_faster and market_costs_more:
        return ORDER_TYPE_COST_DELTA_MARKET_FAST_COSTLY
    if market_faster and limit_costs_more:
        return ORDER_TYPE_COST_DELTA_MARKET_FAST_CHEAPER
    if limit_faster and limit_costs_more:
        return ORDER_TYPE_COST_DELTA_LIMIT_FAST_COSTLY
    if limit_faster and market_costs_more:
        return ORDER_TYPE_COST_DELTA_LIMIT_FAST_CHEAPER
    if market_costs_more:
        return ORDER_TYPE_COST_DELTA_MARKET_COSTS_MORE
    if limit_costs_more:
        return ORDER_TYPE_COST_DELTA_LIMIT_COSTS_MORE
    return ORDER_TYPE_COST_DELTA_NO_MATERIAL_DIFFERENCE


def load_manifest_max_slippage_bps(path: str | Path | None) -> float | None:
    if path is None:
        return None
    manifest = load_manifest(path)
    return max(
        float(scenario.slippage_bps) + float(scenario.market_order_extra_cost_bps)
        for scenario in manifest.execution_model.scenarios
    )


def _decode_submit_evidence(raw: object) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        payload = json.loads(str(raw))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _nested_number(payload: dict[str, Any], *paths: tuple[str, ...]) -> float | None:
    for path in paths:
        current: Any = payload
        for part in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
        value = finite_positive(current)
        if value is not None:
            return value
    return None


def _event_by_type(events: list[sqlite3.Row], event_type: str) -> sqlite3.Row | None:
    candidates = [row for row in events if str(row["event_type"]) == event_type]
    return candidates[-1] if candidates else None


def _signal_context_prices(row: sqlite3.Row | None) -> tuple[float | None, float | None, float | None, float | None]:
    if row is None:
        return None, None, None, None
    context = _decode_submit_evidence(row["context_json"])
    reference = finite_positive(row["market_price"])
    bid = _nested_number(
        context,
        ("best_bid",),
        ("signal_best_bid",),
        ("orderbook", "best_bid"),
        ("top_of_book", "best_bid"),
        ("top_of_book", "bid"),
    )
    ask = _nested_number(
        context,
        ("best_ask",),
        ("signal_best_ask",),
        ("orderbook", "best_ask"),
        ("top_of_book", "best_ask"),
        ("top_of_book", "ask"),
    )
    return reference, bid, ask, spread_bps(best_bid=bid, best_ask=ask)


def _submit_evidence_prices(event: sqlite3.Row | None) -> tuple[int | None, int | None, float | None, float | None, float | None, float | None]:
    if event is None:
        return None, None, None, None, None, None
    evidence = _decode_submit_evidence(event["submit_evidence"])
    request_ts = None
    response_ts = None
    try:
        request_ts = int(evidence["request_ts"]) if evidence.get("request_ts") is not None else None
    except (TypeError, ValueError):
        request_ts = None
    try:
        response_ts = int(evidence["response_ts"]) if evidence.get("response_ts") is not None else None
    except (TypeError, ValueError):
        response_ts = None
    bid = _nested_number(
        evidence,
        ("submit_best_bid",),
        ("best_bid",),
        ("top_of_book", "best_bid"),
        ("top_of_book", "bid"),
    )
    ask = _nested_number(
        evidence,
        ("submit_best_ask",),
        ("best_ask",),
        ("top_of_book", "best_ask"),
        ("top_of_book", "ask"),
    )
    return request_ts, response_ts, finite_positive(event["price"]), bid, ask, spread_bps(best_bid=bid, best_ask=ask)


def _submit_evidence_contract_fields(event: sqlite3.Row | None) -> dict[str, object]:
    if event is None:
        return {}
    evidence = _decode_submit_evidence(event["submit_evidence"])
    return {
        "exchange": str(evidence.get("exchange") or evidence.get("broker") or "bithumb").strip().lower() or None,
        "submit_contract_kind": str(evidence.get("submit_contract_kind") or "").strip() or None,
        "exchange_order_type": str(evidence.get("exchange_order_type") or "").strip() or None,
        "exchange_submit_notional_krw": _nested_number(
            evidence,
            ("exchange_submit_notional_krw",),
            ("submit_contract_context", "exchange_submit_notional_krw"),
        ),
        "exchange_submit_qty": _nested_number(
            evidence,
            ("exchange_submit_qty",),
            ("submit_contract_context", "exchange_submit_qty"),
        ),
        "internal_executable_qty": _nested_number(
            evidence,
            ("internal_executable_qty",),
            ("submit_contract_context", "internal_executable_qty"),
        ),
    }


def _submit_execution_contract_fields(event: sqlite3.Row | None) -> dict[str, object]:
    if event is None:
        return {
            "execution_reality_contract": None,
            "execution_contract_hash": None,
            "execution_contract_hash_valid": None,
            "execution_contract_mismatch_reason": "submit_evidence_missing",
        }
    evidence = _decode_submit_evidence(event["submit_evidence"])
    contract = evidence.get("execution_reality_contract")
    contract_hash = evidence.get("execution_contract_hash")
    if not isinstance(contract, dict):
        return {
            "execution_reality_contract": None,
            "execution_contract_hash": str(contract_hash).strip() if contract_hash else None,
            "execution_contract_hash_valid": None,
            "execution_contract_mismatch_reason": "execution_reality_contract_missing",
        }
    observed_hash = str(contract_hash or contract.get("execution_contract_hash") or "").strip() or None
    if not observed_hash:
        return {
            "execution_reality_contract": dict(contract),
            "execution_contract_hash": None,
            "execution_contract_hash_valid": False,
            "execution_contract_mismatch_reason": "execution_contract_hash_missing",
        }
    valid = contract_hash_matches(contract, observed_hash)
    return {
        "execution_reality_contract": dict(contract),
        "execution_contract_hash": observed_hash,
        "execution_contract_hash_valid": valid,
        "execution_contract_mismatch_reason": None if valid else "execution_contract_hash_mismatch",
    }


def build_execution_quality_record(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    backtest_assumed_slippage_bps: float | None = None,
) -> ExecutionQualityRecord | None:
    order = conn.execute(
        """
        SELECT client_order_id, submit_attempt_id, exchange_order_id, status, side, order_type,
               price, qty_req, qty_filled, strategy_name, entry_decision_id, exit_decision_id, created_ts,
               effective_min_trade_qty, qty_step, min_notional_krw
        FROM orders
        WHERE client_order_id=?
        """,
        (client_order_id,),
    ).fetchone()
    if order is None:
        return None
    events = conn.execute(
        """
        SELECT *
        FROM order_events
        WHERE client_order_id=?
        ORDER BY event_ts ASC, id ASC
        """,
        (client_order_id,),
    ).fetchall()
    fills = conn.execute(
        """
        SELECT *
        FROM fills
        WHERE client_order_id=?
        ORDER BY fill_ts ASC, id ASC
        """,
        (client_order_id,),
    ).fetchall()
    decision_id = order["entry_decision_id"] if order["entry_decision_id"] is not None else order["exit_decision_id"]
    decision = None
    if decision_id is not None:
        decision = conn.execute(
            """
            SELECT id, decision_ts, strategy_name, market_price, context_json
            FROM strategy_decisions
            WHERE id=?
            """,
            (int(decision_id),),
        ).fetchone()

    preflight = _event_by_type(events, "submit_attempt_preflight")
    confirmation = _event_by_type(events, "submit_attempt_recorded")
    intent = _event_by_type(events, "intent_created")
    signal_ref, signal_bid, signal_ask, signal_spread = _signal_context_prices(decision)
    submit_sent_ts, submit_response_ts, submit_ref, submit_bid, submit_ask, submit_spread = _submit_evidence_prices(confirmation)
    contract_fields = _submit_evidence_contract_fields(confirmation)
    execution_contract_fields = _submit_execution_contract_fields(confirmation)
    if submit_ref is None and preflight is not None:
        submit_ref = finite_positive(preflight["price"])
    if submit_sent_ts is None and confirmation is not None:
        submit_sent_ts = int(confirmation["event_ts"])
    if submit_response_ts is None and confirmation is not None:
        evidence = _decode_submit_evidence(confirmation["submit_evidence"])
        if evidence.get("response_ts") is None and str(confirmation["submission_reason_code"] or "") == "confirmed_success":
            submit_response_ts = int(confirmation["event_ts"])

    avg_fill = weighted_average_price(fills)
    filled_qty = sum(float(fill["qty"]) for fill in fills if finite_non_negative(fill["qty"]) is not None)
    spent_quote = sum(
        float(fill["qty"]) * float(fill["price"])
        for fill in fills
        if finite_non_negative(fill["qty"]) is not None and finite_positive(fill["price"]) is not None
    )
    spent_quote = spent_quote if spent_quote > 0 else None
    requested_qty = finite_non_negative(order["qty_req"])
    remaining_qty = None if requested_qty is None else max(0.0, requested_qty - filled_qty)
    fee = sum(float(fill["fee"]) for fill in fills if finite_non_negative(fill["fee"]) is not None) if fills else None
    first_fill_ts = int(fills[0]["fill_ts"]) if fills else None
    last_fill_ts = int(fills[-1]["fill_ts"]) if fills else None
    fill_ratio = None if not requested_qty or requested_qty <= 0 else min(filled_qty / requested_qty, 1.0)
    unfilled = bool(requested_qty and requested_qty > 0 and filled_qty <= 0)
    partial = bool(fill_ratio is not None and 0 < fill_ratio < 0.999999)
    remainder = assess_execution_remainder(
        requested_qty=requested_qty,
        filled_qty=filled_qty,
        remaining_qty=remaining_qty,
        reference_price=(avg_fill if avg_fill is not None else submit_ref if submit_ref is not None else signal_ref),
        qty_step=order["qty_step"],
        effective_min_trade_qty=order["effective_min_trade_qty"],
        min_notional_krw=order["min_notional_krw"],
    )
    exchange_submit_notional = finite_positive(contract_fields.get("exchange_submit_notional_krw"))
    exchange_spent_quote = spent_quote
    exchange_remaining_quote = (
        max(0.0, exchange_submit_notional - float(exchange_spent_quote))
        if exchange_submit_notional is not None and exchange_spent_quote is not None
        else None
    )
    exchange_completion_ratio = (
        min(float(exchange_spent_quote) / exchange_submit_notional, 1.0)
        if exchange_submit_notional is not None and exchange_spent_quote is not None and exchange_submit_notional > 0
        else None
    )
    notional = (avg_fill * filled_qty) if avg_fill is not None and filled_qty > 0 else None
    realized_fee_rate = None if fee is None or notional is None or notional <= 0 else fee / notional
    side = str(order["side"] or "").upper() or None
    best_quote = best_quote_for_side(side=side, best_bid=submit_bid, best_ask=submit_ask)
    slippage_signal = side_aware_slippage_bps(side=side, reference_price=signal_ref, fill_price=avg_fill)
    slippage_submit = side_aware_slippage_bps(side=side, reference_price=submit_ref, fill_price=avg_fill)
    slippage_quote = side_aware_slippage_bps(side=side, reference_price=best_quote, fill_price=avg_fill)
    model_breach = None
    if backtest_assumed_slippage_bps is not None and slippage_signal is not None:
        model_breach = slippage_signal > float(backtest_assumed_slippage_bps)

    exchange = str(contract_fields.get("exchange") or "bithumb").strip().lower() or None
    submit_contract_kind = (
        str(contract_fields.get("submit_contract_kind") or "").strip()
        or ("market_sell_base_qty" if side == "SELL" and str(order["order_type"] or "").lower() == "market" else None)
    )
    semantics = classify_order_semantics(
        raw_order_type=order["order_type"],
        side=side,
        exchange=exchange,
        submit_contract_kind=submit_contract_kind,
    )
    quote_budget_buy = (
        semantics.canonical_execution_kind == CANONICAL_MARKET_BUY_QUOTE_NOTIONAL
        and submit_contract_kind == "market_buy_notional"
        and exchange_submit_notional is not None
    )
    quote_budget_remaining_material = False
    if quote_budget_buy and exchange_remaining_quote is not None:
        if remainder.min_notional_krw is None:
            quote_budget_remaining_material = exchange_remaining_quote > 0
        else:
            quote_budget_remaining_material = exchange_remaining_quote >= float(remainder.min_notional_krw)
    material_unfilled = bool(unfilled and (quote_budget_remaining_material if quote_budget_buy else remainder.is_material_remaining))
    material_partial = bool(partial and (quote_budget_remaining_material if quote_budget_buy else remainder.is_material_remaining))
    materiality_reason = remainder.materiality_reason
    if quote_budget_buy and exchange_remaining_quote is not None:
        if quote_budget_remaining_material:
            materiality_reason = "exchange_quote_budget_remaining_material"
        else:
            materiality_reason = "exchange_quote_budget_remaining_below_min_notional_krw"

    missing: list[str] = []
    if decision_id is None:
        missing.append("decision_id_missing")
    if signal_ref is None:
        missing.append("signal_reference_price_missing")
    if submit_sent_ts is None:
        missing.append("submit_sent_ts_missing")
    if submit_response_ts is None:
        missing.append("submit_response_ts_missing")
    if avg_fill is None:
        missing.append("fill_price_missing")
    if requested_qty is None or requested_qty <= 0:
        missing.append("requested_qty_missing")
    if material_unfilled:
        missing.append("unfilled")

    if missing:
        quality_status = QUALITY_INSUFFICIENT_EVIDENCE
        quality_reason = ",".join(missing)
    elif model_breach:
        quality_status = QUALITY_MODEL_BREACH
        quality_reason = "slippage_vs_signal_exceeds_backtest_model"
    elif material_partial:
        quality_status = QUALITY_DEGRADED
        quality_reason = "partial_fill"
    else:
        quality_status = QUALITY_WITHIN_MODEL
        quality_reason = "complete_evidence_within_thresholds"

    return ExecutionQualityRecord(
        client_order_id=str(order["client_order_id"]),
        submit_attempt_id=str(order["submit_attempt_id"]) if order["submit_attempt_id"] else None,
        decision_id=int(decision_id) if decision_id is not None else None,
        strategy_name=str(order["strategy_name"] or (decision["strategy_name"] if decision else "")) or None,
        mode=str(intent["mode"]) if intent is not None and intent["mode"] else None,
        market=str((confirmation or preflight or intent)["symbol"]) if (confirmation or preflight or intent) is not None and (confirmation or preflight or intent)["symbol"] else None,
        side=side,
        order_type=str(order["order_type"]) if order["order_type"] else None,
        exchange=exchange,
        submit_contract_kind=submit_contract_kind,
        canonical_execution_kind=semantics.canonical_execution_kind,
        semantic_evidence_quality=semantics.semantic_evidence_quality,
        market_equivalent=semantics.market_equivalent,
        legacy_unknown_order_type=semantics.legacy_unknown,
        unsupported_unknown_order_type=semantics.unsupported_unknown,
        exchange_order_id=str(order["exchange_order_id"]) if order["exchange_order_id"] else None,
        execution_reality_contract=(
            execution_contract_fields["execution_reality_contract"]
            if isinstance(execution_contract_fields.get("execution_reality_contract"), dict)
            else None
        ),
        execution_contract_hash=(
            str(execution_contract_fields["execution_contract_hash"])
            if execution_contract_fields.get("execution_contract_hash")
            else None
        ),
        execution_contract_hash_valid=(
            bool(execution_contract_fields["execution_contract_hash_valid"])
            if execution_contract_fields.get("execution_contract_hash_valid") is not None
            else None
        ),
        execution_contract_mismatch_reason=(
            str(execution_contract_fields["execution_contract_mismatch_reason"])
            if execution_contract_fields.get("execution_contract_mismatch_reason")
            else None
        ),
        signal_ts_ms=(int(decision["decision_ts"]) if decision is not None and decision["decision_ts"] is not None else None),
        signal_reference_price=signal_ref,
        signal_best_bid=signal_bid,
        signal_best_ask=signal_ask,
        signal_spread_bps=signal_spread,
        submit_plan_ts_ms=(int(preflight["event_ts"]) if preflight is not None else None),
        submit_sent_ts_ms=submit_sent_ts,
        submit_response_ts_ms=submit_response_ts,
        submit_reference_price=submit_ref,
        submit_best_bid=submit_bid,
        submit_best_ask=submit_ask,
        submit_spread_bps=submit_spread,
        first_fill_ts_ms=first_fill_ts,
        last_fill_ts_ms=last_fill_ts,
        avg_fill_price=avg_fill,
        filled_qty=filled_qty,
        requested_qty=requested_qty,
        remaining_qty=remaining_qty,
        remaining_notional_krw=remainder.remaining_notional_krw,
        internal_target_remaining_qty=remaining_qty,
        internal_target_residue_material=remainder.is_material_remaining,
        internal_target_residue_reason=remainder.materiality_reason,
        exchange_submit_notional_krw=exchange_submit_notional,
        exchange_spent_quote_krw=exchange_spent_quote,
        exchange_remaining_quote_krw=exchange_remaining_quote,
        exchange_fill_completion_ratio=exchange_completion_ratio,
        qty_step=remainder.qty_step,
        effective_min_trade_qty=remainder.effective_min_trade_qty,
        min_notional_krw=remainder.min_notional_krw,
        fee=fee,
        realized_fee_rate=realized_fee_rate,
        submit_latency_ms=latency_ms(start_ms=(preflight["event_ts"] if preflight is not None else None), end_ms=submit_sent_ts),
        response_latency_ms=latency_ms(start_ms=submit_sent_ts, end_ms=submit_response_ts),
        first_fill_latency_ms=latency_ms(start_ms=submit_sent_ts, end_ms=first_fill_ts),
        full_fill_latency_ms=latency_ms(start_ms=submit_sent_ts, end_ms=last_fill_ts),
        slippage_vs_signal_bps=slippage_signal,
        slippage_vs_submit_ref_bps=slippage_submit,
        slippage_vs_best_quote_bps=slippage_quote,
        fill_ratio=fill_ratio,
        partial_fill_flag=partial,
        unfilled_flag=unfilled,
        material_partial_fill_flag=material_partial,
        material_unfilled_flag=material_unfilled,
        remaining_qty_materiality_reason=materiality_reason,
        quality_status=quality_status,
        quality_reason=quality_reason,
        backtest_assumed_slippage_bps=backtest_assumed_slippage_bps,
        model_breach_flag=model_breach,
    )


_EXECUTION_QUALITY_COLUMNS = tuple(ExecutionQualityRecord.__dataclass_fields__.keys())


def upsert_execution_quality_record(conn: sqlite3.Connection, record: ExecutionQualityRecord) -> None:
    now_ms = int(time.time() * 1000)
    values = [_execution_quality_db_value(record, name) for name in _EXECUTION_QUALITY_COLUMNS]
    update_assignments = ", ".join(f"{name}=?" for name in _EXECUTION_QUALITY_COLUMNS if name != "client_order_id")
    placeholders = ", ".join("?" for _ in _EXECUTION_QUALITY_COLUMNS)
    columns = ", ".join(_EXECUTION_QUALITY_COLUMNS)
    update_values = [
        _execution_quality_db_value(record, name)
        for name in _EXECUTION_QUALITY_COLUMNS
        if name != "client_order_id"
    ]
    cursor = conn.execute(
        f"""
        UPDATE execution_quality_events
        SET {update_assignments}, updated_ts=?
        WHERE client_order_id=?
        """,
        (*update_values, now_ms, record.client_order_id),
    )
    if cursor.rowcount:
        return
    conn.execute(
        f"""
        INSERT INTO execution_quality_events({columns}, created_ts, updated_ts)
        VALUES ({placeholders}, ?, ?)
        """,
        (*values, now_ms, now_ms),
    )


def _execution_quality_db_value(record: ExecutionQualityRecord, name: str) -> object:
    value = getattr(record, name)
    if name == "execution_reality_contract" and isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return value


def refresh_execution_quality_records(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
    market: str | None = None,
    mode: str | None = None,
    backtest_assumed_slippage_bps: float | None = None,
) -> list[ExecutionQualityRecord]:
    clauses = ["1=1"]
    params: list[object] = []
    if market:
        clauses.append(
            """
            EXISTS (
                SELECT 1 FROM order_events oe
                WHERE oe.client_order_id=o.client_order_id
                  AND oe.symbol=?
            )
            """
        )
        params.append(str(market))
    if mode:
        clauses.append(
            """
            EXISTS (
                SELECT 1 FROM order_events oe
                WHERE oe.client_order_id=o.client_order_id
                  AND oe.mode=?
            )
            """
        )
        params.append(str(mode))
    limit_sql = "" if limit is None else "LIMIT ?"
    if limit is not None:
        params.append(max(1, int(limit)))
    rows = conn.execute(
        f"""
        SELECT o.client_order_id
        FROM orders o
        WHERE {' AND '.join(clauses)}
        ORDER BY o.created_ts DESC, o.client_order_id DESC
        {limit_sql}
        """,
        params,
    ).fetchall()
    records: list[ExecutionQualityRecord] = []
    for row in rows:
        record = build_execution_quality_record(
            conn,
            client_order_id=str(row["client_order_id"]),
            backtest_assumed_slippage_bps=backtest_assumed_slippage_bps,
        )
        if record is None:
            continue
        upsert_execution_quality_record(conn, record)
        records.append(record)
    return records


def summarize_execution_quality(
    records: list[ExecutionQualityRecord],
    *,
    thresholds: ExecutionQualityThresholds,
    backtest_slippage_bps_max: float | None = None,
) -> dict[str, object]:
    sample_count = len(records)
    market_buy_quote_order_count = sum(
        1 for row in records if row.canonical_execution_kind == CANONICAL_MARKET_BUY_QUOTE_NOTIONAL
    )
    market_sell_base_order_count = sum(
        1 for row in records if row.canonical_execution_kind == CANONICAL_MARKET_SELL_BASE_QTY
    )
    market_equivalent_order_count = sum(1 for row in records if row.market_equivalent)
    verified_market_equivalent_order_count = sum(
        1 for row in records if row.market_equivalent and row.semantic_evidence_quality == "current_verified"
    )
    unverified_market_equivalent_order_count = sum(
        1 for row in records if row.market_equivalent and row.semantic_evidence_quality != "current_verified"
    )
    limit_order_count = sum(1 for row in records if row.canonical_execution_kind == CANONICAL_LIMIT_QTY_PRICE)
    legacy_unknown_order_type_count = sum(1 for row in records if row.legacy_unknown_order_type)
    unsupported_unknown_order_type_count = sum(1 for row in records if row.unsupported_unknown_order_type)
    unknown_order_type_count = legacy_unknown_order_type_count + unsupported_unknown_order_type_count
    partial_count = sum(1 for row in records if row.material_partial_fill_flag)
    unfilled_count = sum(1 for row in records if row.material_unfilled_flag)
    raw_partial_count = sum(1 for row in records if row.partial_fill_flag)
    raw_unfilled_count = sum(1 for row in records if row.unfilled_flag)
    insufficient_count = sum(1 for row in records if row.quality_status == QUALITY_INSUFFICIENT_EVIDENCE)
    model_breach_count = sum(1 for row in records if row.model_breach_flag is True)
    sufficient_model_count = sum(1 for row in records if row.model_breach_flag is not None)
    contract_hashes = sorted(
        {
            str(row.execution_contract_hash)
            for row in records
            if row.execution_contract_hash is not None and str(row.execution_contract_hash).strip()
        }
    )
    missing_contract_count = sum(1 for row in records if not row.execution_contract_hash)
    invalid_contract_count = sum(1 for row in records if row.execution_contract_hash_valid is False)
    mixed_contract_hashes = len(contract_hashes) > 1
    submit_to_fill = [float(row.full_fill_latency_ms) for row in records if row.full_fill_latency_ms is not None]
    slip_signal = [float(row.slippage_vs_signal_bps) for row in records if row.slippage_vs_signal_bps is not None]
    slip_submit = [float(row.slippage_vs_submit_ref_bps) for row in records if row.slippage_vs_submit_ref_bps is not None]
    partial_rate = partial_count / sample_count if sample_count else 0.0
    unfilled_rate = unfilled_count / sample_count if sample_count else 0.0
    raw_partial_rate = raw_partial_count / sample_count if sample_count else 0.0
    raw_unfilled_rate = raw_unfilled_count / sample_count if sample_count else 0.0
    model_breach_rate = model_breach_count / sufficient_model_count if sufficient_model_count else None
    p90_slippage = percentile(slip_signal, 90)
    p95_latency = percentile(submit_to_fill, 95)

    status = GATE_PASS
    primary_issue = "none"
    next_action = "continue_live_observation"
    if sample_count == 0 or sample_count < thresholds.min_sample:
        status = GATE_INSUFFICIENT_EVIDENCE
        primary_issue = "insufficient_sample"
        next_action = "collect_more_live_execution_quality_samples"
    elif insufficient_count > 0:
        status = GATE_WARN
        primary_issue = "missing_execution_quality_evidence"
        next_action = "repair_or_instrument_missing_signal_submit_fill_links"
    elif mixed_contract_hashes:
        status = GATE_FAIL
        primary_issue = "mixed_execution_contract_hashes"
        next_action = "split_execution_quality_calibration_by_execution_contract_hash"
    elif invalid_contract_count > 0:
        status = GATE_FAIL
        primary_issue = "invalid_execution_contract_hash"
        next_action = "repair_or_regenerate_contract_bearing_submit_evidence"
    elif p90_slippage is not None and p90_slippage > thresholds.max_p90_slippage_bps:
        status = GATE_FAIL
        primary_issue = "p90_slippage_exceeds_execution_quality_threshold"
        next_action = "reduce_live_to_dry_run_or_update_research_cost_model"
    elif p95_latency is not None and p95_latency > thresholds.max_p95_full_fill_latency_ms:
        status = GATE_WARN
        primary_issue = "p95_submit_to_fill_latency_exceeds_threshold"
        next_action = "inspect_exchange_latency_and_order_policy"
    elif partial_rate > thresholds.max_partial_fill_rate:
        status = GATE_WARN
        primary_issue = "partial_fill_rate_exceeds_threshold"
        next_action = "inspect_order_type_and_liquidity"
    elif model_breach_rate is not None and model_breach_rate > thresholds.max_model_breach_rate:
        status = GATE_FAIL
        primary_issue = "model_breach_rate_exceeds_threshold"
        next_action = "reduce_live_to_dry_run_or_update_research_cost_model"
    elif backtest_slippage_bps_max is not None and p90_slippage is not None and p90_slippage > backtest_slippage_bps_max:
        status = GATE_FAIL
        primary_issue = "p90_slippage_exceeds_backtest_model"
        next_action = "reduce_live_to_dry_run_or_update_research_cost_model"

    summary = {
        "sample_count": sample_count,
        "market_order_count": market_equivalent_order_count,
        "market_equivalent_order_count": market_equivalent_order_count,
        "verified_market_equivalent_order_count": verified_market_equivalent_order_count,
        "unverified_market_equivalent_order_count": unverified_market_equivalent_order_count,
        "market_buy_quote_order_count": market_buy_quote_order_count,
        "market_sell_base_order_count": market_sell_base_order_count,
        "limit_order_count": limit_order_count,
        "unknown_order_type_count": unknown_order_type_count,
        "legacy_unknown_order_type_count": legacy_unknown_order_type_count,
        "unsupported_unknown_order_type_count": unsupported_unknown_order_type_count,
        "median_submit_to_fill_ms": percentile(submit_to_fill, 50),
        "p90_submit_to_fill_ms": percentile(submit_to_fill, 90),
        "p95_submit_to_fill_ms": p95_latency,
        "median_slippage_vs_signal_bps": percentile(slip_signal, 50),
        "p90_slippage_vs_signal_bps": p90_slippage,
        "p95_slippage_vs_signal_bps": percentile(slip_signal, 95),
        "median_slippage_vs_submit_ref_bps": percentile(slip_submit, 50),
        "p90_slippage_vs_submit_ref_bps": percentile(slip_submit, 90),
        "partial_fill_rate": partial_rate,
        "unfilled_rate": unfilled_rate,
        "raw_partial_fill_rate": raw_partial_rate,
        "raw_unfilled_rate": raw_unfilled_rate,
        "material_partial_fill_count": partial_count,
        "material_unfilled_count": unfilled_count,
        "raw_partial_fill_count": raw_partial_count,
        "raw_unfilled_count": raw_unfilled_count,
        "insufficient_evidence_count": insufficient_count,
        "backtest_slippage_bps_max": backtest_slippage_bps_max,
        "model_breach_count": model_breach_count if backtest_slippage_bps_max is not None else None,
        "model_breach_rate": model_breach_rate if backtest_slippage_bps_max is not None else None,
        "execution_contract_hash": contract_hashes[0] if len(contract_hashes) == 1 else None,
        "execution_contract_hashes": contract_hashes,
        "execution_contract_hash_present": len(contract_hashes) == 1 and missing_contract_count == 0,
        "mixed_execution_contract_hashes": mixed_contract_hashes,
        "execution_contract_mismatch_count": invalid_contract_count,
        "execution_contract_missing_count": missing_contract_count,
        "quality_gate_status": status,
        "primary_issue": primary_issue,
        "next_action": next_action,
    }
    summary.update(_order_type_metrics(records, "market"))
    summary.update(_order_type_metrics(records, "limit"))
    summary["order_type_cost_delta"] = _classify_order_type_cost_delta(summary)
    return summary


def format_execution_quality_text(summary: dict[str, object]) -> str:
    lines: list[str] = []
    for key in (
        "sample_count",
        "market_order_count",
        "market_equivalent_order_count",
        "verified_market_equivalent_order_count",
        "unverified_market_equivalent_order_count",
        "market_buy_quote_order_count",
        "market_sell_base_order_count",
        "limit_order_count",
        "unknown_order_type_count",
        "legacy_unknown_order_type_count",
        "unsupported_unknown_order_type_count",
        "median_submit_to_fill_ms",
        "p90_submit_to_fill_ms",
        "p95_submit_to_fill_ms",
        "median_slippage_vs_signal_bps",
        "p90_slippage_vs_signal_bps",
        "p95_slippage_vs_signal_bps",
        "median_slippage_vs_submit_ref_bps",
        "p90_slippage_vs_submit_ref_bps",
        "partial_fill_rate",
        "unfilled_rate",
        "raw_partial_fill_rate",
        "raw_unfilled_rate",
        "material_partial_fill_count",
        "material_unfilled_count",
        "raw_partial_fill_count",
        "raw_unfilled_count",
        "insufficient_evidence_count",
        "backtest_slippage_bps_max",
        "model_breach_count",
        "model_breach_rate",
        "execution_contract_hash",
        "execution_contract_hashes",
        "execution_contract_hash_present",
        "mixed_execution_contract_hashes",
        "execution_contract_mismatch_count",
        "execution_contract_missing_count",
        "market_median_slippage_bps",
        "market_p90_slippage_bps",
        "market_p95_slippage_bps",
        "market_median_submit_to_fill_ms",
        "market_p90_submit_to_fill_ms",
        "market_p95_submit_to_fill_ms",
        "market_partial_fill_rate",
        "market_unfilled_rate",
        "market_raw_partial_fill_rate",
        "market_raw_unfilled_rate",
        "limit_median_slippage_bps",
        "limit_p90_slippage_bps",
        "limit_p95_slippage_bps",
        "limit_median_submit_to_fill_ms",
        "limit_p90_submit_to_fill_ms",
        "limit_p95_submit_to_fill_ms",
        "limit_partial_fill_rate",
        "limit_unfilled_rate",
        "limit_raw_partial_fill_rate",
        "limit_raw_unfilled_rate",
        "order_type_cost_delta",
        "quality_gate_status",
        "primary_issue",
        "next_action",
    ):
        value = summary.get(key)
        if value is None:
            value_text = "NA"
        elif isinstance(value, float):
            value_text = f"{value:.6g}"
        else:
            value_text = str(value)
        lines.append(f"{key}={value_text}")
    return "\n".join(lines)
