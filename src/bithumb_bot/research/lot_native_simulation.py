from __future__ import annotations

from dataclasses import dataclass, replace

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.dust import build_position_state_model
from bithumb_bot.lot_model import DUST_POSITION_EPS, lot_count_to_qty, quantize_to_lot_count
from bithumb_bot.position_authority import (
    LEGACY_RESEARCH_POSITION_MODEL,
    LOT_NATIVE_RESEARCH_POSITION_MODEL,
    POSITIVE_EQUIVALENCE_STATE_CLASSES,
    PositionAuthoritySnapshot,
    lot_native_comparison_position_state,
    research_lot_native_position_authority_snapshot,
)


@dataclass(frozen=True)
class ResearchLotRules:
    internal_lot_size: float = 0.0001
    min_qty: float = 0.0001
    qty_step: float = 0.0001
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 8
    market_price: float = 100_000_000.0
    exit_fee_ratio: float = 0.0
    exit_slippage_bps: float = 0.0
    exit_buffer_ratio: float = 0.0


@dataclass(frozen=True)
class LotNativeResearchPositionModel:
    lot_rules: ResearchLotRules = ResearchLotRules()
    open_lot_count: int = 0
    reserved_exit_lot_count: int = 0
    dust_tracking_lot_count: int = 0
    raw_total_asset_qty: float = 0.0
    recovery_blocked: bool = False
    recovery_block_reason: str = "none"
    unsupported_reason: str = ""

    @classmethod
    def flat(cls, lot_rules: ResearchLotRules | None = None) -> "LotNativeResearchPositionModel":
        return cls(lot_rules=lot_rules or ResearchLotRules())

    def apply_buy_fill(self, *, qty: float) -> "LotNativeResearchPositionModel":
        lot_count, residue = self._lot_count_and_residue(qty)
        if lot_count <= 0:
            return replace(
                self,
                raw_total_asset_qty=max(0.0, float(self.raw_total_asset_qty) + max(0.0, float(qty))),
                unsupported_reason="research_model_lacks_lot_native_authority",
            )
        unsupported = self.unsupported_reason
        if residue > DUST_POSITION_EPS:
            unsupported = "research_model_lacks_dust_state"
        return replace(
            self,
            open_lot_count=int(self.open_lot_count) + lot_count,
            raw_total_asset_qty=max(0.0, float(self.raw_total_asset_qty) + max(0.0, float(qty))),
            unsupported_reason=unsupported,
        )

    def submit_sell(self, *, lot_count: int | None = None) -> "LotNativeResearchPositionModel":
        sellable = max(0, int(self.open_lot_count) - int(self.reserved_exit_lot_count))
        reserve = sellable if lot_count is None else min(sellable, max(0, int(lot_count)))
        return replace(self, reserved_exit_lot_count=int(self.reserved_exit_lot_count) + reserve)

    def apply_sell_fill(self, *, qty: float) -> "LotNativeResearchPositionModel":
        fill_lots, residue = self._lot_count_and_residue(qty)
        if fill_lots <= 0:
            return replace(self, unsupported_reason="research_model_lacks_lot_native_authority")
        open_lots = max(0, int(self.open_lot_count) - fill_lots)
        reserved_lots = max(0, int(self.reserved_exit_lot_count) - fill_lots)
        unsupported = self.unsupported_reason
        if residue > DUST_POSITION_EPS:
            unsupported = "research_model_lacks_dust_state"
        return replace(
            self,
            open_lot_count=open_lots,
            reserved_exit_lot_count=reserved_lots,
            raw_total_asset_qty=max(0.0, float(self.raw_total_asset_qty) - max(0.0, float(qty))),
            unsupported_reason=unsupported,
        )

    def mark_recovery_blocked(self, reason: str) -> "LotNativeResearchPositionModel":
        return replace(
            self,
            recovery_blocked=True,
            recovery_block_reason=str(reason or "recovery_required_present"),
            unsupported_reason="research_model_lacks_lot_native_authority",
        )

    def authority_snapshot(
        self,
        *,
        order_rules_hash: str,
        fee_authority_hash: str,
    ) -> PositionAuthoritySnapshot:
        exposure = self._position_state().normalized_exposure
        state_class = "flat_no_dust_no_position" if exposure.terminal_state == "flat" else str(exposure.terminal_state)
        unsupported_reason = str(self.unsupported_reason or "")
        if state_class not in POSITIVE_EQUIVALENCE_STATE_CLASSES:
            unsupported_reason = unsupported_reason or "research_model_lacks_lot_native_authority"
        if bool(exposure.recovery_blocked):
            state_class = "recovery_blocked"
            unsupported_reason = unsupported_reason or "research_model_lacks_lot_native_authority"
        comparison_state = lot_native_comparison_position_state(
            {
                **exposure.as_dict(),
                "state_class": state_class,
                "recovery_blocked": bool(exposure.recovery_blocked),
                "recovery_block_reason": str(exposure.recovery_block_reason or "none"),
            }
        )
        position_state_hash = canonical_payload_hash(comparison_state)
        lot_native_fields = {
            "raw_total_asset_qty": float(exposure.raw_total_asset_qty),
            "open_lot_count": int(exposure.open_lot_count),
            "dust_tracking_lot_count": int(exposure.dust_tracking_lot_count),
            "reserved_exit_lot_count": int(exposure.reserved_exit_lot_count),
            "sellable_executable_lot_count": int(exposure.sellable_executable_lot_count),
            "open_exposure_qty": float(exposure.open_exposure_qty),
            "dust_tracking_qty": float(exposure.dust_tracking_qty),
            "reserved_exit_qty": float(exposure.reserved_exit_qty),
            "sellable_executable_qty": float(exposure.sellable_executable_qty),
            "terminal_state": str(exposure.terminal_state),
            "entry_allowed": bool(exposure.entry_allowed),
            "exit_allowed": bool(exposure.exit_allowed),
            "recovery_blocked": bool(exposure.recovery_blocked),
            "recovery_block_reason": str(exposure.recovery_block_reason or "none"),
            "order_rules_hash": str(order_rules_hash),
            "fee_authority_hash": str(fee_authority_hash),
            "position_state_hash": position_state_hash,
        }
        if not unsupported_reason:
            return research_lot_native_position_authority_snapshot(
                lot_native_fields=lot_native_fields,
                order_rules_hash=str(order_rules_hash),
                fee_authority_hash=str(fee_authority_hash),
                position_state_hash=position_state_hash,
            )
        return PositionAuthoritySnapshot(
            raw_total_asset_qty=float(exposure.raw_total_asset_qty),
            open_lot_count=int(exposure.open_lot_count),
            dust_tracking_lot_count=int(exposure.dust_tracking_lot_count),
            reserved_exit_lot_count=int(exposure.reserved_exit_lot_count),
            sellable_executable_lot_count=int(exposure.sellable_executable_lot_count),
            open_exposure_qty=float(exposure.open_exposure_qty),
            dust_tracking_qty=float(exposure.dust_tracking_qty),
            reserved_exit_qty=float(exposure.reserved_exit_qty),
            sellable_executable_qty=float(exposure.sellable_executable_qty),
            terminal_state=str(exposure.terminal_state),
            entry_allowed=bool(exposure.entry_allowed),
            exit_allowed=bool(exposure.exit_allowed),
            recovery_blocked=bool(exposure.recovery_blocked),
            recovery_block_reason=str(exposure.recovery_block_reason or "none"),
            order_rules_hash=str(order_rules_hash),
            fee_authority_hash=str(fee_authority_hash),
            position_state_hash=position_state_hash,
            state_class=state_class if not unsupported_reason else (
                state_class if state_class != "flat_no_dust_no_position" else "research_model_lacks_lot_native_authority"
            ),
            unsupported_reason=unsupported_reason,
            research_position_model=(
                LOT_NATIVE_RESEARCH_POSITION_MODEL
                if not unsupported_reason
                else f"{LOT_NATIVE_RESEARCH_POSITION_MODEL}_partial"
            ),
        )

    def legacy_cash_qty_authority_snapshot(
        self,
        *,
        order_rules_hash: str,
        fee_authority_hash: str,
    ) -> PositionAuthoritySnapshot:
        snapshot = self.authority_snapshot(
            order_rules_hash=order_rules_hash,
            fee_authority_hash=fee_authority_hash,
        )
        return replace(
            snapshot,
            state_class="research_model_lacks_lot_native_authority",
            unsupported_reason="research_model_lacks_lot_native_authority",
            research_position_model=LEGACY_RESEARCH_POSITION_MODEL,
        )

    def _position_state(self):
        lot_size = float(self.lot_rules.internal_lot_size)
        open_qty = lot_count_to_qty(lot_count=int(self.open_lot_count), lot_size=lot_size)
        dust_qty = lot_count_to_qty(lot_count=int(self.dust_tracking_lot_count), lot_size=lot_size)
        metadata = {
            "dust_state": "no_dust",
            "dust_residual_present": 0,
            "unresolved_open_order_count": 0,
            "recovery_required_count": 1 if self.recovery_blocked else 0,
        }
        return build_position_state_model(
            raw_qty_open=open_qty,
            metadata_raw=metadata,
            raw_total_asset_qty=max(0.0, float(open_qty) + float(dust_qty)),
            open_exposure_qty=open_qty,
            dust_tracking_qty=dust_qty,
            reserved_exit_qty=lot_count_to_qty(
                lot_count=int(self.reserved_exit_lot_count),
                lot_size=lot_size,
            ),
            open_lot_count=int(self.open_lot_count),
            dust_tracking_lot_count=int(self.dust_tracking_lot_count),
            internal_lot_size=lot_size,
            market_price=float(self.lot_rules.market_price),
            min_qty=float(self.lot_rules.min_qty),
            qty_step=float(self.lot_rules.qty_step),
            min_notional_krw=float(self.lot_rules.min_notional_krw),
            max_qty_decimals=int(self.lot_rules.max_qty_decimals),
            exit_fee_ratio=float(self.lot_rules.exit_fee_ratio),
            exit_slippage_bps=float(self.lot_rules.exit_slippage_bps),
            exit_buffer_ratio=float(self.lot_rules.exit_buffer_ratio),
        )

    def _lot_count_and_residue(self, qty: float) -> tuple[int, float]:
        normalized_qty = max(0.0, float(qty))
        lot_count = quantize_to_lot_count(
            qty=normalized_qty,
            lot_size=float(self.lot_rules.internal_lot_size),
        )
        lot_qty = lot_count_to_qty(lot_count=lot_count, lot_size=float(self.lot_rules.internal_lot_size))
        return lot_count, max(0.0, normalized_qty - lot_qty)


def lot_native_model_from_quantities(
    *,
    qty: float,
    sellable_qty: float,
    lot_rules: ResearchLotRules | None = None,
) -> LotNativeResearchPositionModel:
    rules = lot_rules or ResearchLotRules()
    model = LotNativeResearchPositionModel.flat(rules)
    if float(qty) <= DUST_POSITION_EPS and float(sellable_qty) <= DUST_POSITION_EPS:
        return model
    lot_count, residue = model._lot_count_and_residue(qty)
    if lot_count <= 0 or residue > DUST_POSITION_EPS:
        return replace(
            model,
            raw_total_asset_qty=max(0.0, float(qty)),
            unsupported_reason="research_model_lacks_lot_native_authority",
        )
    sellable_lots, sellable_residue = model._lot_count_and_residue(sellable_qty)
    if sellable_residue > DUST_POSITION_EPS or sellable_lots > lot_count:
        return replace(
            model,
            raw_total_asset_qty=max(0.0, float(qty)),
            unsupported_reason="research_model_lacks_lot_native_authority",
        )
    return replace(
        model,
        open_lot_count=lot_count,
        reserved_exit_lot_count=max(0, lot_count - sellable_lots),
        raw_total_asset_qty=max(0.0, float(qty)),
    )
