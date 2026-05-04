from __future__ import annotations

import pytest

from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest


def _manifest() -> dict[str, object]:
    return {
        "experiment_id": "sma_filter_v1_2026_05",
        "hypothesis": "SMA filter has positive expectancy after costs.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2, 3],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
        },
    }


def test_manifest_parses_required_contract() -> None:
    manifest = parse_manifest(_manifest())

    assert manifest.experiment_id == "sma_filter_v1_2026_05"
    assert manifest.hypothesis
    assert manifest.manifest_hash().startswith("sha256:")


def test_manifest_parses_valid_walk_forward_config() -> None:
    payload = _manifest()
    payload["walk_forward"] = {
        "train_window_days": 2,
        "test_window_days": 1,
        "step_days": 1,
        "min_windows": 1,
    }

    manifest = parse_manifest(payload)

    assert manifest.walk_forward is not None
    assert manifest.walk_forward.train_window_days == 2


def test_manifest_parses_regime_acceptance_gate() -> None:
    payload = _manifest()
    payload["acceptance_gate"]["regime_acceptance_gate"] = {
        "required": True,
        "min_trade_count_per_required_regime": 10,
        "required_regimes": ["uptrend"],
        "blocked_regimes": ["sideways_low_vol_volume_decreasing"],
        "blocked_regime_max_trade_count": 0,
        "blocked_regime_max_net_pnl_loss_krw": 0,
        "min_profit_factor_by_regime": {"uptrend": 1.2},
        "max_loss_share_by_single_regime": 0.4,
        "max_pnl_dependency_by_single_regime": 0.5,
    }

    manifest = parse_manifest(payload)

    gate = manifest.acceptance_gate.regime_acceptance_gate
    assert gate.required is True
    assert gate.required_regimes == ("uptrend",)
    assert gate.blocked_regimes == ("sideways_low_vol_volume_decreasing",)


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda payload: payload.pop("hypothesis"), "hypothesis"),
        (lambda payload: payload["dataset"].pop("validation"), "dataset.validation"),
        (lambda payload: payload.__setitem__("parameter_space", {}), "parameter_space"),
        (
            lambda payload: payload["dataset"]["train"].__setitem__("start", "2023-01-03"),
            "dataset.train.start",
        ),
        (
            lambda payload: payload["acceptance_gate"].__setitem__("min_trade_count", 0),
            "acceptance_gate.min_trade_count",
        ),
        (
            lambda payload: payload.__setitem__(
                "walk_forward",
                {"train_window_days": 0, "test_window_days": 1, "step_days": 1, "min_windows": 1},
            ),
            "walk_forward.train_window_days",
        ),
        (
            lambda payload: (
                payload["acceptance_gate"].__setitem__("walk_forward_required", True),
                payload.pop("walk_forward", None),
            ),
            "walk_forward is required",
        ),
    ],
)
def test_manifest_validation_rejects_invalid_contract(mutate, expected: str) -> None:
    payload = _manifest()
    mutate(payload)

    with pytest.raises(ManifestValidationError, match=expected):
        parse_manifest(payload)
