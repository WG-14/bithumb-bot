from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .flatten import flatten_btc_position


@dataclass(frozen=True)
class OperatorFlattenService:
    """Stable emergency flatten boundary used by runtime operator flows."""

    flattener: Callable[..., object] = flatten_btc_position

    def flatten_position(self, **kwargs: object) -> object:
        return self.flattener(**kwargs)


__all__ = ["OperatorFlattenService", "flatten_btc_position"]
