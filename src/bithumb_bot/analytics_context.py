from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

KST = timezone(timedelta(hours=9))


AnalysisBuckets = dict[str, str]
AnalysisRawFeatures = dict[str, float | None]


def _as_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _load_context_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(loaded, dict):
            return loaded
    return {}


def classify_time_bucket(*, ts_ms: int | None, tz: timezone = KST) -> str:
    if ts_ms is None:
        return "unknown"
    try:
        hour = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz).hour
    except (OSError, OverflowError, ValueError):
        return "unknown"
    if 0 <= hour <= 5:
        return "overnight"
    if 6 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 17:
        return "afternoon"
    return "evening"


def classify_signal_strength_bucket(*, label: str | None, score: float | None = None) -> str:
    normalized = _as_text(label, default="").strip().lower()
    if normalized in {"strong", "medium", "weak", "neutral"}:
        return normalized
    if score is None:
        return "unknown"
    if score >= 0.8:
        return "strong"
    if score >= 0.5:
        return "medium"
    if score > 0:
        return "weak"
    return "neutral"


def classify_gap_bucket(*, gap_ratio: float | None) -> str:
    if gap_ratio is None:
        return "unknown"
    value = abs(float(gap_ratio))
    if value < 0.0005:
        return "tiny"
    if value < 0.0015:
        return "small"
    if value < 0.003:
        return "medium"
    return "large"


def classify_volatility_bucket(*, volatility_ratio: float | None) -> str:
    if volatility_ratio is None:
        return "unknown"
    value = max(0.0, float(volatility_ratio))
    if value < 0.001:
        return "very_low"
    if value < 0.003:
        return "low"
    if value < 0.007:
        return "normal"
    return "high"


def classify_overextension_bucket(*, extension_ratio: float | None) -> str:
    if extension_ratio is None:
        return "unknown"
    value = abs(float(extension_ratio))
    if value < 0.01:
        return "normal"
    if value < 0.02:
        return "stretched"
    return "overextended"


def _extract_raw_features(context: dict[str, Any]) -> AnalysisRawFeatures:
    observations = context.get("market_observations") if isinstance(context.get("market_observations"), dict) else {}
    features = context.get("features") if isinstance(context.get("features"), dict) else {}
    signal_strength = context.get("signal_strength") if isinstance(context.get("signal_strength"), dict) else {}

    gap = _as_float_or_none(observations.get("gap"))
    if gap is None:
        gap = _as_float_or_none(context.get("gap_ratio", features.get("sma_gap_ratio")))

    volatility = _as_float_or_none(observations.get("volatility"))
    if volatility is None:
        volatility = _as_float_or_none(context.get("volatility_ratio", features.get("volatility_range_ratio")))

    extension = _as_float_or_none(observations.get("extension"))
    if extension is None:
        extension = _as_float_or_none(
            context.get("overextended_ratio", features.get("overextended_abs_return_ratio"))
        )

    signal_strength_score = _as_float_or_none(context.get("signal_strength_score", signal_strength.get("score")))

    return {
        "gap_ratio": gap,
        "volatility_ratio": volatility,
        "extension_ratio": extension,
        "signal_strength_score": signal_strength_score,
    }


def normalize_analysis_context(
    *,
    context: dict[str, Any] | None,
    decision_ts: int | None,
    candle_ts: int | None,
    signal_strength_label: str | None = None,
) -> dict[str, Any]:
    payload = dict(context or {})
    raw = _extract_raw_features(payload)
    resolved_signal_label = _as_text(
        signal_strength_label,
        default=_as_text(payload.get("signal_strength_label"), default="unknown"),
    )

    buckets: AnalysisBuckets = {
        "time_of_day": classify_time_bucket(ts_ms=candle_ts if candle_ts is not None else decision_ts),
        "signal_strength": classify_signal_strength_bucket(
            label=resolved_signal_label,
            score=raw.get("signal_strength_score"),
        ),
        "gap": classify_gap_bucket(gap_ratio=raw.get("gap_ratio")),
        "volatility": classify_volatility_bucket(volatility_ratio=raw.get("volatility_ratio")),
        "overextension": classify_overextension_bucket(extension_ratio=raw.get("extension_ratio")),
    }

    return {
        "raw": raw,
        "buckets": buckets,
        "flags": {
            "is_overextended": buckets["overextension"] == "overextended",
        },
    }


def normalize_analysis_context_from_decision_row(row: Any) -> dict[str, Any]:
    context = _load_context_json(row["context_json"] if "context_json" in row.keys() else None)
    return normalize_analysis_context(
        context=context,
        decision_ts=int(row["decision_ts"]) if row["decision_ts"] is not None else None,
        candle_ts=int(row["candle_ts"]) if row["candle_ts"] is not None else None,
        signal_strength_label=_as_text(context.get("signal_strength_label"), default="unknown"),
    )


def normalize_analysis_context_from_lifecycle_row(
    row: Any,
    *,
    entry_context_json: str | dict[str, Any] | None,
    exit_context_json: str | dict[str, Any] | None,
) -> dict[str, Any]:
    entry_context = _load_context_json(entry_context_json)
    exit_context = _load_context_json(exit_context_json)

    selected_context = entry_context if entry_context else exit_context
    decision_ts: int | None = None
    candle_ts: int | None = None

    if entry_context:
        decision_ts = _as_float_or_none(entry_context.get("decision_ts"))
        candle_ts = _as_float_or_none(entry_context.get("candle_ts"))
    if decision_ts is None and exit_context:
        decision_ts = _as_float_or_none(exit_context.get("decision_ts"))
    if candle_ts is None and exit_context:
        candle_ts = _as_float_or_none(exit_context.get("candle_ts"))

    if decision_ts is None:
        decision_ts = _as_float_or_none(row["entry_ts"] if "entry_ts" in row.keys() else None)
    if decision_ts is None:
        decision_ts = _as_float_or_none(row["exit_ts"] if "exit_ts" in row.keys() else None)

    return normalize_analysis_context(
        context=selected_context,
        decision_ts=None if decision_ts is None else int(decision_ts),
        candle_ts=None if candle_ts is None else int(candle_ts),
    )
