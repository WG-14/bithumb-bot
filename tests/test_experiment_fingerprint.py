from __future__ import annotations

from bithumb_bot.config import settings
from bithumb_bot.experiment_fingerprint import experiment_fingerprint


def test_experiment_fingerprint_is_stable_for_same_settings() -> None:
    assert experiment_fingerprint(strategy_name="sma_with_filter") == experiment_fingerprint(
        strategy_name="sma_with_filter"
    )


def test_experiment_fingerprint_changes_when_strategy_param_changes() -> None:
    old_short = settings.SMA_SHORT
    try:
        baseline = experiment_fingerprint(strategy_name="sma_with_filter")
        object.__setattr__(settings, "SMA_SHORT", old_short + 1)
        changed = experiment_fingerprint(strategy_name="sma_with_filter")
    finally:
        object.__setattr__(settings, "SMA_SHORT", old_short)

    assert changed != baseline

