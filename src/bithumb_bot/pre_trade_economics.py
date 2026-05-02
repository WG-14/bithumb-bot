from __future__ import annotations

from dataclasses import dataclass


PRE_TRADE_ECONOMICS_VERSION = "pre_trade_economics_v1"


@dataclass(frozen=True)
class PreTradeEconomicsSnapshot:
    version: str
    side: str
    order_krw: float
    expected_edge_ratio: float
    required_edge_ratio: float
    roundtrip_fee_ratio: float
    slippage_ratio: float
    buffer_ratio: float
    margin_after_cost_ratio: float
    expected_edge_krw: float
    expected_cost_krw: float
    net_edge_krw: float
    meaningful_edge: bool
    reason: str
    blocking_enabled: bool
    inputs: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "side": self.side,
            "order_krw": float(self.order_krw),
            "expected_edge_ratio": float(self.expected_edge_ratio),
            "required_edge_ratio": float(self.required_edge_ratio),
            "roundtrip_fee_ratio": float(self.roundtrip_fee_ratio),
            "slippage_ratio": float(self.slippage_ratio),
            "buffer_ratio": float(self.buffer_ratio),
            "margin_after_cost_ratio": float(self.margin_after_cost_ratio),
            "expected_edge_krw": float(self.expected_edge_krw),
            "expected_cost_krw": float(self.expected_cost_krw),
            "net_edge_krw": float(self.net_edge_krw),
            "meaningful_edge": bool(self.meaningful_edge),
            "reason": self.reason,
            "blocking_enabled": bool(self.blocking_enabled),
            "inputs": dict(self.inputs),
        }


def build_pre_trade_economics_snapshot(
    *,
    side: str,
    order_krw: float | None,
    expected_edge_ratio: float,
    required_edge_ratio: float,
    roundtrip_fee_ratio: float,
    slippage_ratio: float,
    buffer_ratio: float,
    min_net_edge_krw: float,
    min_margin_after_cost_ratio: float,
    blocking_enabled: bool = False,
    source: str = "",
) -> PreTradeEconomicsSnapshot:
    normalized_order_krw = max(0.0, float(order_krw or 0.0))
    normalized_expected_edge_ratio = max(0.0, float(expected_edge_ratio))
    normalized_required_edge_ratio = max(0.0, float(required_edge_ratio))
    normalized_roundtrip_fee_ratio = max(0.0, float(roundtrip_fee_ratio))
    normalized_slippage_ratio = max(0.0, float(slippage_ratio))
    normalized_buffer_ratio = max(0.0, float(buffer_ratio))
    normalized_min_net_edge = max(0.0, float(min_net_edge_krw))
    normalized_min_margin = max(0.0, float(min_margin_after_cost_ratio))

    expected_edge_krw = normalized_order_krw * normalized_expected_edge_ratio
    expected_cost_krw = normalized_order_krw * (
        normalized_roundtrip_fee_ratio + normalized_slippage_ratio + normalized_buffer_ratio
    )
    net_edge_krw = expected_edge_krw - expected_cost_krw
    margin_after_cost_ratio = normalized_expected_edge_ratio - normalized_required_edge_ratio
    meaningful_edge = bool(
        net_edge_krw >= normalized_min_net_edge
        and margin_after_cost_ratio >= normalized_min_margin
    )
    if normalized_order_krw <= 0.0:
        reason = "order_size_missing"
    elif meaningful_edge:
        reason = "meaningful_edge"
    elif net_edge_krw < normalized_min_net_edge:
        reason = "net_edge_below_minimum"
    else:
        reason = "margin_after_cost_below_minimum"

    return PreTradeEconomicsSnapshot(
        version=PRE_TRADE_ECONOMICS_VERSION,
        side=str(side).strip().upper() or "UNKNOWN",
        order_krw=normalized_order_krw,
        expected_edge_ratio=normalized_expected_edge_ratio,
        required_edge_ratio=normalized_required_edge_ratio,
        roundtrip_fee_ratio=normalized_roundtrip_fee_ratio,
        slippage_ratio=normalized_slippage_ratio,
        buffer_ratio=normalized_buffer_ratio,
        margin_after_cost_ratio=float(margin_after_cost_ratio),
        expected_edge_krw=float(expected_edge_krw),
        expected_cost_krw=float(expected_cost_krw),
        net_edge_krw=float(net_edge_krw),
        meaningful_edge=meaningful_edge,
        reason=reason,
        blocking_enabled=bool(blocking_enabled),
        inputs={
            "min_net_edge_krw": float(normalized_min_net_edge),
            "min_margin_after_cost_ratio": float(normalized_min_margin),
            "source": str(source),
        },
    )
