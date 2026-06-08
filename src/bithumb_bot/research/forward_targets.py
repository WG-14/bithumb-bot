from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.research.dataset_snapshot import Candle


ENTRY_PRICE_NEXT_OPEN = "next_open"
ENTRY_PRICE_SIGNAL_CLOSE = "signal_close"
SUPPORTED_ENTRY_PRICE_MODES = frozenset({ENTRY_PRICE_NEXT_OPEN, ENTRY_PRICE_SIGNAL_CLOSE})


@dataclass(frozen=True)
class ForwardTarget:
    horizon_label: str
    horizon_steps: int
    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    gross_forward_return: float
    mfe: float
    mae: float
    entry_price_mode: str


def compute_forward_target(
    *,
    candles: tuple[Candle, ...],
    index: int,
    horizon_steps: int,
    entry_price_mode: str = ENTRY_PRICE_NEXT_OPEN,
    horizon_label: str | None = None,
) -> ForwardTarget | None:
    mode = str(entry_price_mode or "").strip()
    if mode not in SUPPORTED_ENTRY_PRICE_MODES:
        allowed = ", ".join(sorted(SUPPORTED_ENTRY_PRICE_MODES))
        raise ValueError(f"unknown entry_price_mode={entry_price_mode!r}; allowed values: {allowed}")
    steps = int(horizon_steps)
    if steps <= 0:
        raise ValueError("horizon_steps must be positive")
    if index < 0 or index >= len(candles):
        raise IndexError("index out of range")

    entry_index = index + 1 if mode == ENTRY_PRICE_NEXT_OPEN else index
    exit_index = index + steps
    if entry_index >= len(candles) or exit_index >= len(candles):
        return None

    entry_candle = candles[entry_index]
    signal_candle = candles[index]
    exit_candle = candles[exit_index]
    entry_price = (
        float(entry_candle.open)
        if mode == ENTRY_PRICE_NEXT_OPEN
        else float(signal_candle.close)
    )
    if entry_price <= 0.0:
        return None
    path = candles[entry_index : exit_index + 1]
    high_path = [float(candle.high) for candle in path]
    low_path = [float(candle.low) for candle in path]
    exit_price = float(exit_candle.close)
    return ForwardTarget(
        horizon_label=horizon_label or f"{steps}c",
        horizon_steps=steps,
        entry_ts=int(entry_candle.ts),
        exit_ts=int(exit_candle.ts),
        entry_price=entry_price,
        exit_price=exit_price,
        gross_forward_return=(exit_price / entry_price) - 1.0,
        mfe=(max(high_path) / entry_price) - 1.0,
        mae=(min(low_path) / entry_price) - 1.0,
        entry_price_mode=mode,
    )


def compute_forward_targets(
    *,
    candles: tuple[Candle, ...],
    index: int,
    horizon_steps: tuple[int, ...],
    entry_price_mode: str = ENTRY_PRICE_NEXT_OPEN,
) -> tuple[ForwardTarget, ...]:
    targets: list[ForwardTarget] = []
    for steps in horizon_steps:
        target = compute_forward_target(
            candles=candles,
            index=index,
            horizon_steps=int(steps),
            entry_price_mode=entry_price_mode,
        )
        if target is not None:
            targets.append(target)
    return tuple(targets)
