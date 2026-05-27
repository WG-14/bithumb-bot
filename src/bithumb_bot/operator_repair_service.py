from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .fee_gap_repair import build_fee_gap_accounting_repair_preview
from .manual_flat_repair import build_manual_flat_accounting_repair_preview


@dataclass(frozen=True)
class OperatorRepairService:
    """Stable operator repair-preview boundary used by runtime entrypoints."""

    fee_gap_preview_builder: Callable[[object], dict[str, object]] = (
        build_fee_gap_accounting_repair_preview
    )
    manual_flat_preview_builder: Callable[[object], dict[str, object]] = (
        build_manual_flat_accounting_repair_preview
    )

    def fee_gap_accounting_preview(self, conn: object) -> dict[str, object]:
        return self.fee_gap_preview_builder(conn)

    def manual_flat_accounting_preview(self, conn: object) -> dict[str, object]:
        return self.manual_flat_preview_builder(conn)


__all__ = [
    "OperatorRepairService",
    "build_fee_gap_accounting_repair_preview",
    "build_manual_flat_accounting_repair_preview",
]
