from __future__ import annotations

import math
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import settings


FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON = "fee_authority_degraded_live_entry_blocked"


def _decimal_or_zero(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    if not parsed.is_finite() or parsed < 0:
        return Decimal("0")
    return parsed


@dataclass(frozen=True)
class FeeAuthoritySnapshot:
    bid_fee: Decimal
    ask_fee: Decimal
    maker_bid_fee: Decimal
    maker_ask_fee: Decimal
    source_mode: str
    fee_source: str
    fallback_used: bool
    snapshot_derived: bool
    stale: bool
    retrieved_at_sec: float
    expires_at_sec: float
    degraded: bool
    degraded_reason: str
    diagnostic_summary: str

    @property
    def taker_bid_fee_rate(self) -> Decimal:
        return self.bid_fee

    @property
    def taker_ask_fee_rate(self) -> Decimal:
        return self.ask_fee

    @property
    def taker_roundtrip_fee_rate(self) -> Decimal:
        return self.bid_fee + self.ask_fee

    def live_entry_allowed(self) -> bool:
        return not self.degraded

    def live_entry_block_reason(self) -> str:
        return FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON if self.degraded else "none"

    def as_dict(self) -> dict[str, object]:
        return {
            "bid_fee": str(self.bid_fee),
            "ask_fee": str(self.ask_fee),
            "maker_bid_fee": str(self.maker_bid_fee),
            "maker_ask_fee": str(self.maker_ask_fee),
            "source_mode": self.source_mode,
            "fee_source": self.fee_source,
            "fallback_used": bool(self.fallback_used),
            "snapshot_derived": bool(self.snapshot_derived),
            "stale": bool(self.stale),
            "retrieved_at_sec": float(self.retrieved_at_sec),
            "expires_at_sec": float(self.expires_at_sec),
            "degraded": bool(self.degraded),
            "degraded_reason": self.degraded_reason,
            "diagnostic_summary": self.diagnostic_summary,
            "live_entry_allowed": bool(self.live_entry_allowed()),
            "live_entry_block_reason": self.live_entry_block_reason(),
        }


def _rules_fee_values(rules: object) -> dict[str, Decimal]:
    return {
        "bid_fee": _decimal_or_zero(getattr(rules, "bid_fee", 0)),
        "ask_fee": _decimal_or_zero(getattr(rules, "ask_fee", 0)),
        "maker_bid_fee": _decimal_or_zero(getattr(rules, "maker_bid_fee", 0)),
        "maker_ask_fee": _decimal_or_zero(getattr(rules, "maker_ask_fee", 0)),
    }


def _fee_sources(source: dict[str, Any]) -> dict[str, str]:
    return {
        field: str(source.get(field) or "missing")
        for field in ("bid_fee", "ask_fee", "maker_bid_fee", "maker_ask_fee")
    }


def build_fee_authority_snapshot(
    resolution: object,
    *,
    now_sec: float | None = None,
    config_fallback_fee_rate: float | None = None,
) -> FeeAuthoritySnapshot:
    now = float(time.time() if now_sec is None else now_sec)
    rules = getattr(resolution, "rules", resolution)
    source = getattr(resolution, "source", {}) or {}
    if not isinstance(source, dict):
        source = {}
    fee_sources = _fee_sources(source)
    fees = _rules_fee_values(rules)

    source_mode = str(getattr(resolution, "source_mode", "") or "unknown")
    fallback_used = bool(getattr(resolution, "fallback_used", False))
    retrieved_at_sec = float(getattr(resolution, "retrieved_at_sec", 0.0) or 0.0)
    expires_at_sec = float(getattr(resolution, "expires_at_sec", 0.0) or 0.0)
    snapshot_derived = bool(getattr(resolution, "snapshot_persisted", False) and expires_at_sec <= 0.0)
    stale = bool(getattr(resolution, "stale", False))
    is_stale = getattr(resolution, "is_stale", None)
    if callable(is_stale):
        try:
            stale = bool(is_stale(now_sec=now))
        except TypeError:
            stale = bool(is_stale())

    all_chance_doc = all(value == "chance_doc" for value in fee_sources.values())
    any_missing_or_unsupported = any(value in {"missing", "unsupported_by_doc"} for value in fee_sources.values())
    any_fee_negative_or_nonfinite = False
    for field, value in fees.items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            numeric = float("nan")
        if not math.isfinite(numeric) or numeric < 0:
            any_fee_negative_or_nonfinite = True
            fees[field] = Decimal("0")

    fee_source = "chance_doc" if all_chance_doc else "config_estimate_degraded"
    if fee_source == "config_estimate_degraded":
        fallback_rate = _decimal_or_zero(
            settings.LIVE_FEE_RATE_ESTIMATE if config_fallback_fee_rate is None else config_fallback_fee_rate
        )
        fees = {
            "bid_fee": fallback_rate,
            "ask_fee": fallback_rate,
            "maker_bid_fee": fallback_rate,
            "maker_ask_fee": fallback_rate,
        }

    degraded_reasons: list[str] = []
    if not all_chance_doc:
        degraded_reasons.append("fee_source_not_chance_doc")
    if any_missing_or_unsupported:
        degraded_reasons.append("fee_field_missing_or_unsupported")
    if fallback_used:
        degraded_reasons.append("rule_fallback_used")
    if snapshot_derived:
        degraded_reasons.append("persisted_snapshot_fee_authority")
    if stale:
        degraded_reasons.append("stale_fee_authority")
    if any_fee_negative_or_nonfinite:
        degraded_reasons.append("invalid_fee_value")

    degraded_reason = ",".join(dict.fromkeys(degraded_reasons)) or "none"
    degraded = degraded_reason != "none"
    diagnostic_summary = (
        f"fee_source={fee_source}; source_mode={source_mode}; "
        f"fallback_used={int(fallback_used)}; snapshot_derived={int(snapshot_derived)}; "
        f"stale={int(stale)}; degraded={int(degraded)}; degraded_reason={degraded_reason}"
    )
    return FeeAuthoritySnapshot(
        bid_fee=fees["bid_fee"],
        ask_fee=fees["ask_fee"],
        maker_bid_fee=fees["maker_bid_fee"],
        maker_ask_fee=fees["maker_ask_fee"],
        source_mode=source_mode,
        fee_source=fee_source,
        fallback_used=fallback_used,
        snapshot_derived=snapshot_derived,
        stale=stale,
        retrieved_at_sec=retrieved_at_sec,
        expires_at_sec=expires_at_sec,
        degraded=degraded,
        degraded_reason=degraded_reason,
        diagnostic_summary=diagnostic_summary,
    )


def resolve_fee_authority_snapshot(pair: str, *, now_sec: float | None = None) -> FeeAuthoritySnapshot:
    from .broker.order_rules import get_effective_order_rules

    return build_fee_authority_snapshot(get_effective_order_rules(pair), now_sec=now_sec)
