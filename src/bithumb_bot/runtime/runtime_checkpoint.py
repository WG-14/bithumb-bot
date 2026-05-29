from __future__ import annotations

from .lifecycle_artifacts import RuntimeCycleArtifact
from .. import runtime_state


def apply_processed_candle_checkpoint(*, candle_ts_ms: int, now_epoch_sec: float | None = None) -> None:
    runtime_state.mark_processed_candle(candle_ts_ms=candle_ts_ms, now_epoch_sec=now_epoch_sec)


__all__ = ["RuntimeCycleArtifact", "apply_processed_candle_checkpoint"]
