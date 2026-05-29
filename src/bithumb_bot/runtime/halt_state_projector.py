from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ..config import settings
from ..dust import build_dust_display_context, build_position_state_model
from ..lifecycle import summarize_position_lots
from ..oms import OPEN_ORDER_STATUSES


@dataclass(frozen=True)
class HaltStateProjector:
    db_factory: Callable[[], Any] | None = None

    def build_halt_projection(
        self,
        *,
        open_orders: Mapping[str, object] | None = None,
        portfolio: Mapping[str, object] | None = None,
        lot_snapshot: Mapping[str, object] | None = None,
        dust_context: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "open_orders": dict(open_orders or {}),
            "portfolio": dict(portfolio or {}),
            "lot_snapshot": dict(lot_snapshot or {}),
            "dust_context": dict(dust_context or {}),
        }

    def project_from_db(self, *, metadata_raw: str | None) -> dict[str, object]:
        if self.db_factory is None:
            return self.build_halt_projection()
        conn = self.db_factory()
        try:
            open_row = conn.execute(
                "SELECT COUNT(*) AS open_count FROM orders WHERE status IN ({})".format(
                    ",".join("?" for _ in OPEN_ORDER_STATUSES)
                ),
                OPEN_ORDER_STATUSES,
            ).fetchone()
            portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
            lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
            lot_definition = getattr(lot_snapshot, "lot_definition", None)
        finally:
            conn.close()
        open_count = int(open_row["open_count"] if open_row else 0)
        asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
        dust_context = build_dust_display_context(metadata_raw)
        position_state = build_position_state_model(
            raw_qty_open=asset_qty,
            metadata_raw=metadata_raw,
            raw_total_asset_qty=max(
                asset_qty,
                float(lot_snapshot.raw_total_asset_qty),
                float(dust_context.raw_holdings.broker_qty),
            ),
            open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
            dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
            open_lot_count=int(lot_snapshot.open_lot_count),
            dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
            internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
            min_qty=(None if lot_definition is None else lot_definition.min_qty),
            qty_step=(None if lot_definition is None else lot_definition.qty_step),
            min_notional_krw=(None if lot_definition is None else lot_definition.min_notional_krw),
            max_qty_decimals=(None if lot_definition is None else lot_definition.max_qty_decimals),
        )
        return {
            "open_count": open_count,
            "position_present": bool(position_state.normalized_exposure.has_any_position_residue),
            "dust_only_remainder": bool(position_state.normalized_exposure.has_dust_only_remainder),
            "projection_evidence": self.build_halt_projection(
                open_orders={"open_count": open_count},
                portfolio={"asset_qty": asset_qty},
                lot_snapshot={
                    "open_lot_count": int(lot_snapshot.open_lot_count),
                    "dust_tracking_lot_count": int(lot_snapshot.dust_tracking_lot_count),
                    "open_exposure_qty": float(lot_snapshot.raw_open_exposure_qty),
                    "dust_tracking_qty": float(lot_snapshot.dust_tracking_qty),
                },
                dust_context={"classification": str(dust_context.classification.label)},
            ),
        }


__all__ = ["HaltStateProjector"]
