from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .. import runtime_state


@dataclass(frozen=True)
class RuntimeStateStore:
    snapshot_reader: Callable[[], object]

    def snapshot(self) -> object:
        return self.snapshot_reader()


def pause_trading_until(epoch_sec: float, reason: str | None = None) -> None:
    runtime_state.disable_trading_until(epoch_sec, reason=reason)


__all__ = ["RuntimeStateStore", "pause_trading_until"]
