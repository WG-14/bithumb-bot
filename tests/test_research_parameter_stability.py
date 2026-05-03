from __future__ import annotations

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.parameter_space import candidate_id, iter_parameter_candidates
from bithumb_bot.research.validation_protocol import _gate_result, _parameter_stability_scores


def _manifest(parameter_space: dict[str, list[object]], *, required: bool = True):
    return parse_manifest(
        {
            "experiment_id": "stability_unit",
            "hypothesis": "Neighboring parameters should support a promoted profile.",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "unit",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            },
            "parameter_space": parameter_space,
            "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 20,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": required,
            },
        }
    )


def _evaluated(candidates: list[dict[str, object]], returns: list[float]) -> list[dict[str, object]]:
    rows = []
    for index, params in enumerate(candidates):
        good = returns[index] > 0.0
        rows.append(
            {
                "candidate_id": candidate_id(params, index),
                "validation_metrics": {
                    "return_pct": returns[index],
                    "trade_count": 2 if good else 0,
                    "max_drawdown_pct": 1.0,
                    "profit_factor": 2.0 if good else None,
                },
            }
        )
    return rows


def test_stable_plateau_has_high_parameter_stability_score() -> None:
    manifest = _manifest({"SMA_SHORT": [2, 3, 4], "SMA_LONG": [8]})
    candidates = iter_parameter_candidates(manifest.parameter_space)

    scores = _parameter_stability_scores(
        manifest=manifest,
        candidates=candidates,
        evaluated_candidates=_evaluated(candidates, [1.0, 2.0, 1.5]),
    )

    assert scores[1]["score"] == 1.0
    assert scores[1]["acceptable_neighbor_count"] == 2


def test_isolated_spike_has_low_parameter_stability_score() -> None:
    manifest = _manifest({"SMA_SHORT": [2, 3, 4], "SMA_LONG": [8]})
    candidates = iter_parameter_candidates(manifest.parameter_space)

    scores = _parameter_stability_scores(
        manifest=manifest,
        candidates=candidates,
        evaluated_candidates=_evaluated(candidates, [-1.0, 4.0, -0.5]),
    )

    assert scores[1]["score"] == 0.0
    assert scores[1]["neighbor_count"] == 2


def test_too_few_neighbors_reports_none_when_stability_required() -> None:
    manifest = _manifest({"SMA_SHORT": [2], "SMA_LONG": [8]})
    candidates = iter_parameter_candidates(manifest.parameter_space)

    scores = _parameter_stability_scores(
        manifest=manifest,
        candidates=candidates,
        evaluated_candidates=_evaluated(candidates, [2.0]),
    )

    assert scores[0]["score"] is None
    assert scores[0]["neighbor_count"] == 0


def test_required_parameter_stability_fails_low_or_missing_scores() -> None:
    manifest = _manifest({"SMA_SHORT": [2, 3, 4], "SMA_LONG": [8]})
    metrics = {"return_pct": 2.0, "trade_count": 2, "max_drawdown_pct": 1.0, "profit_factor": 2.0}

    low_result, low_reasons = _gate_result(
        manifest=manifest,
        validation_metrics=metrics,
        final_holdout_metrics=None,
        walk_forward_metrics=None,
        stability_score=0.0,
        include_walk_forward=False,
    )
    none_result, none_reasons = _gate_result(
        manifest=manifest,
        validation_metrics=metrics,
        final_holdout_metrics=None,
        walk_forward_metrics=None,
        stability_score=None,
        include_walk_forward=False,
    )

    assert low_result == "FAIL"
    assert none_result == "FAIL"
    assert "parameter_stability_failed" in low_reasons
    assert "parameter_stability_failed" in none_reasons
