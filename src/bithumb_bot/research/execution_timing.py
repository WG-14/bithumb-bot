from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any

from bithumb_bot.public_api_minute_candles import interval_to_minute_unit

from .dataset_snapshot import Candle, DatasetSnapshot, TopOfBookQuote
from .experiment_manifest import ExecutionTimingPolicy, ManifestValidationError


REALITY_ORDER = {
    "candle_close_optimistic": 0,
    "candle_next_open": 1,
    "top_of_book_after_decision": 2,
    "latency_adjusted_top_of_book": 3,
}


@dataclass(frozen=True)
class SignalEvent:
    signal_candle_start_ts: int
    signal_candle_close_ts: int
    decision_ts: int
    side: str
    signal_reference_price: float
    signal_reference_source: str
    feature_snapshot: dict[str, object]
    regime_snapshot: dict[str, object]


@dataclass(frozen=True)
class ExecutionReferenceEvent:
    submit_ts_assumption: int
    fill_reference_ts: int | None
    fill_reference_price: float | None
    fill_reference_source: str | None
    quote_ts: int | None
    quote_age_ms: int | None
    quote_source: str | None
    best_bid: float | None
    best_ask: float | None
    spread_bps: float | None
    execution_reality_level: str
    intra_candle_policy: str
    top_of_book_is_full_depth: bool = False
    failure_reason: str | None = None

    def request_fields(self) -> dict[str, object]:
        return {
            "submit_ts_assumption": self.submit_ts_assumption,
            "fill_reference_ts": self.fill_reference_ts,
            "fill_reference_price": self.fill_reference_price,
            "fill_reference_source": self.fill_reference_source,
            "quote_ts": self.quote_ts,
            "quote_age_ms": self.quote_age_ms,
            "quote_source": self.quote_source,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_bps": self.spread_bps,
            "execution_reality_level": self.execution_reality_level,
            "intra_candle_policy": self.intra_candle_policy,
            "top_of_book_is_full_depth": self.top_of_book_is_full_depth,
            "execution_reference_failure_reason": self.failure_reason,
        }


def interval_ms(interval: str) -> int:
    try:
        return interval_to_minute_unit(interval) * 60_000
    except ValueError as exc:
        raise ManifestValidationError(f"unsupported research interval for execution timing: {interval}") from exc


def candle_close_ts(candle: Candle, *, interval: str) -> int:
    return int(candle.ts) + interval_ms(interval)


def build_signal_event(
    *,
    candle: Candle,
    interval: str,
    side: str,
    policy: ExecutionTimingPolicy,
    feature_snapshot: dict[str, object],
    regime_snapshot: dict[str, object],
) -> SignalEvent:
    close_ts = candle_close_ts(candle, interval=interval)
    decision_ts = close_ts
    if policy.decision_time == "candle_close_plus_guard" or policy.decision_guard_ms:
        decision_ts = close_ts + int(policy.decision_guard_ms)
    return SignalEvent(
        signal_candle_start_ts=int(candle.ts),
        signal_candle_close_ts=close_ts,
        decision_ts=decision_ts,
        side=str(side).upper(),
        signal_reference_price=float(candle.close),
        signal_reference_source="candle_close",
        feature_snapshot=feature_snapshot,
        regime_snapshot=regime_snapshot,
    )


def resolve_execution_reference(
    *,
    dataset: DatasetSnapshot,
    signal: SignalEvent,
    signal_index: int,
    policy: ExecutionTimingPolicy,
    model_latency_ms: int = 0,
) -> ExecutionReferenceEvent:
    submit_ts = int(signal.decision_ts)
    if policy.fill_reference_policy == "latency_adjusted_orderbook":
        submit_ts += int(model_latency_ms)
    if policy.fill_reference_policy == "candle_close_legacy":
        quote = dataset.top_of_book_for_ts(signal.signal_candle_start_ts)
        return ExecutionReferenceEvent(
            submit_ts_assumption=submit_ts,
            fill_reference_ts=signal.signal_candle_start_ts,
            fill_reference_price=float(signal.signal_reference_price),
            fill_reference_source="candle_close",
            quote_ts=int(quote.ts) if quote is not None else None,
            quote_age_ms=quote.age_ms if quote is not None else None,
            quote_source=quote.source if quote is not None else None,
            best_bid=float(quote.bid_price) if quote is not None else None,
            best_ask=float(quote.ask_price) if quote is not None else None,
            spread_bps=float(quote.spread_bps) if quote is not None else None,
            execution_reality_level="candle_close_optimistic",
            intra_candle_policy="close_price_only_no_intracandle_path",
        )
    if policy.fill_reference_policy == "next_candle_open":
        next_index = signal_index + 1
        if next_index >= len(dataset.candles):
            return _failed_reference(
                signal=signal,
                submit_ts=submit_ts,
                policy=policy,
                reality_level="candle_next_open",
                reason="next_candle_missing",
            )
        next_candle = dataset.candles[next_index]
        return ExecutionReferenceEvent(
            submit_ts_assumption=submit_ts,
            fill_reference_ts=int(next_candle.ts),
            fill_reference_price=float(next_candle.open),
            fill_reference_source="next_candle_open",
            quote_ts=None,
            quote_age_ms=None,
            quote_source=None,
            best_bid=None,
            best_ask=None,
            spread_bps=None,
            execution_reality_level="candle_next_open",
            intra_candle_policy="next_candle_open_no_intracandle_path",
        )
    if policy.fill_reference_policy in {"first_orderbook_after_decision", "latency_adjusted_orderbook"}:
        target_ts = signal.decision_ts if policy.fill_reference_policy == "first_orderbook_after_decision" else submit_ts
        quote = first_quote_after_or_equal(dataset=dataset, target_ts=target_ts, max_wait_ms=policy.max_quote_wait_ms)
        reality = (
            "top_of_book_after_decision"
            if policy.fill_reference_policy == "first_orderbook_after_decision"
            else "latency_adjusted_top_of_book"
        )
        if quote is None:
            return _failed_reference(
                signal=signal,
                submit_ts=submit_ts,
                policy=policy,
                reality_level=reality,
                reason="quote_after_decision_missing",
            )
        price = quote.ask_price if signal.side == "BUY" else quote.bid_price
        return ExecutionReferenceEvent(
            submit_ts_assumption=submit_ts,
            fill_reference_ts=int(quote.ts),
            fill_reference_price=float(price),
            fill_reference_source=policy.fill_reference_policy,
            quote_ts=int(quote.ts),
            quote_age_ms=int(quote.ts) - int(target_ts),
            quote_source=str(quote.source),
            best_bid=float(quote.bid_price),
            best_ask=float(quote.ask_price),
            spread_bps=float(quote.spread_bps),
            execution_reality_level=reality,
            intra_candle_policy="top_of_book_snapshot_no_depth_no_queue",
        )
    raise ValueError(f"unsupported fill_reference_policy: {policy.fill_reference_policy}")


def first_quote_after_or_equal(
    *,
    dataset: DatasetSnapshot,
    target_ts: int,
    max_wait_ms: int,
) -> TopOfBookQuote | None:
    quotes = sorted(
        (quote for quote in dataset.execution_top_of_book_quotes() if quote is not None),
        key=lambda quote: (int(quote.ts), str(quote.source)),
    )
    max_ts = int(target_ts) + int(max_wait_ms)
    for quote in quotes:
        if int(target_ts) <= int(quote.ts) <= max_ts:
            return quote
    return None


def execution_reality_gate(
    *,
    policy: ExecutionTimingPolicy,
    observed_levels: list[str],
    fill_reference_sources: list[str],
    quote_coverage_pct: float | None = None,
) -> dict[str, object]:
    reasons: list[str] = []
    min_level = policy.min_execution_reality_level_for_promotion
    status = "PASS"
    if min_level is not None:
        required = REALITY_ORDER[min_level]
        observed = min((REALITY_ORDER.get(level, -1) for level in observed_levels), default=-1)
        if observed < required:
            reasons.append("execution_reality_level_below_required")
    if "candle_close" in fill_reference_sources and not policy.allow_same_candle_close_fill:
        reasons.append("execution_reference_price_candle_close_not_promotable")
    if policy.fill_reference_policy in {"first_orderbook_after_decision", "latency_adjusted_orderbook"}:
        if quote_coverage_pct is not None and quote_coverage_pct < 100.0 and policy.missing_quote_policy == "fail":
            reasons.append("quote_after_decision_signal_coverage_below_threshold")
    if reasons:
        status = "FAIL"
    return {
        "status": status,
        "reasons": reasons,
        "min_execution_reality_level_for_promotion": min_level,
        "observed_execution_reality_levels": sorted(set(observed_levels)),
        "fill_reference_sources": sorted(set(fill_reference_sources)),
    }


def signal_quote_coverage_summary(
    *,
    execution_metadata: list[dict[str, Any]],
    policy: ExecutionTimingPolicy,
) -> dict[str, object]:
    signal_count = len(execution_metadata)
    quote_ages = [
        int(item["quote_age_ms"])
        for item in execution_metadata
        if item.get("quote_age_ms") is not None
    ]
    fillable = [
        item for item in execution_metadata
        if item.get("fill_reference_price") is not None and not item.get("execution_reference_failure_reason")
    ]
    missing = [
        item for item in execution_metadata
        if item.get("execution_reference_failure_reason") in {"quote_after_decision_missing", "missing_quote_failed"}
    ]
    coverage = (len(quote_ages) / signal_count * 100.0) if signal_count else None
    return {
        "signal_event_count": signal_count,
        "fillable_signal_event_count": len(fillable),
        "missing_quote_on_signal_count": len(missing),
        "quote_after_decision_coverage_pct": round(coverage, 8) if coverage is not None else None,
        "median_quote_age_ms_on_signal": median(quote_ages) if quote_ages else None,
        "p95_quote_age_ms_on_signal": _percentile(quote_ages, 95) if quote_ages else None,
        "execution_reference_policy": policy.fill_reference_policy,
        "execution_reality_level": _summary_reality_level(execution_metadata),
    }


def _failed_reference(
    *,
    signal: SignalEvent,
    submit_ts: int,
    policy: ExecutionTimingPolicy,
    reality_level: str,
    reason: str,
) -> ExecutionReferenceEvent:
    return ExecutionReferenceEvent(
        submit_ts_assumption=submit_ts,
        fill_reference_ts=None,
        fill_reference_price=None,
        fill_reference_source=policy.fill_reference_policy,
        quote_ts=None,
        quote_age_ms=None,
        quote_source=None,
        best_bid=None,
        best_ask=None,
        spread_bps=None,
        execution_reality_level=reality_level,
        intra_candle_policy="reference_unavailable",
        failure_reason=reason,
    )


def _summary_reality_level(execution_metadata: list[dict[str, Any]]) -> str | None:
    levels = [str(item.get("execution_reality_level")) for item in execution_metadata if item.get("execution_reality_level")]
    if not levels:
        return None
    return min(levels, key=lambda level: REALITY_ORDER.get(level, -1))


def _percentile(values: list[int], pct: float) -> float | None:
    clean = sorted(float(value) for value in values)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * (float(pct) / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(clean) - 1)
    weight = rank - lower
    return clean[lower] + ((clean[upper] - clean[lower]) * weight)
