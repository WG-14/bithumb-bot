from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.promotion_gate import PromotionGateError, build_candidate_profile, promote_candidate
from bithumb_bot.storage_io import write_json_atomic


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _candidate(**overrides):
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        "validation_metrics": {
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        "acceptance_gate_result": "PASS",
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_volume_increasing"],
        "blocked_live_regimes": ["sideways_low_vol_volume_decreasing"],
        "regime_evidence": {
            "uptrend_normal_vol_volume_increasing": {
                "trade_count": 12,
                "profit_factor": 1.4,
                "expectancy": 100.0,
            }
        },
        "regime_gate_result": {
            "result": "PASS",
            "passed": True,
            "reasons": [],
        },
        "walk_forward_required": False,
    }
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _write_report(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    write_json_atomic(
        path,
        {
            "experiment_id": "promo_exp",
            "manifest_hash": "sha256:manifest",
            "candidates": [candidate],
        },
    )


def _walk_forward_candidate(backtest_candidate: dict[str, object], **overrides) -> dict[str, object]:
    payload = dict(backtest_candidate)
    payload.update(
        {
            "walk_forward_metrics": {
                "window_count": 3,
                "pass_window_count": 3,
                "fail_window_count": 0,
                "mean_test_return_pct": 1.0,
                "median_test_return_pct": 1.0,
                "worst_test_return_pct": 0.5,
                "return_consistency_pass": True,
            },
            "walk_forward_gate_result": "PASS",
        }
    )
    payload.pop("candidate_profile_hash", None)
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _write_walk_forward_report(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    write_json_atomic(
        path,
        {
            "experiment_id": "promo_exp",
            "manifest_hash": "sha256:manifest",
            "candidates": [candidate],
        },
    )


def test_promotion_refuses_candidate_without_validation_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(validation_metrics=None)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="validation_oos_evidence_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


@pytest.mark.parametrize(
    "candidate",
    [
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["min_trade_count_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["max_drawdown_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["profit_factor_failed"]),
    ],
)
def test_promotion_refuses_failed_gate_candidates(tmp_path, monkeypatch, candidate) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_does_not_mutate_env_file(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    env_file = tmp_path / "live.env"
    env_file.write_text("SMA_SHORT=99\n", encoding="utf-8")
    before = env_file.read_text(encoding="utf-8")
    _write_report(manager, _candidate())

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact_path.exists()
    assert env_file.read_text(encoding="utf-8") == before
    assert result.artifact["operator_next_step"].startswith("Review this artifact")


def test_promotion_refuses_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(candidate_profile_hash="sha256:tampered")
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_promotion_refuses_backtest_candidate_hash_mismatch_even_when_walk_forward_exists(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    backtest_candidate["candidate_profile_hash"] = "sha256:tampered"
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    with pytest.raises(PromotionGateError, match="backtest_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_promotion_refuses_backtest_gate_failure_even_when_walk_forward_passes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(
        acceptance_gate_result="FAIL",
        gate_fail_reasons=["min_trade_count_failed"],
        walk_forward_required=True,
    )
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))

    with pytest.raises(PromotionGateError, match="backtest_acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(
        manager,
        _walk_forward_candidate(backtest_candidate, candidate_profile_hash="sha256:tampered"),
    )

    with pytest.raises(PromotionGateError, match="walk_forward_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_uses_verified_candidate_profile_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    expected_hash = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["candidate_profile_hash"] == expected_hash
    assert result.artifact["verified_candidate_profile_hash"] == expected_hash
    assert result.artifact["strategy_profile_hash"] == expected_hash


def test_promotion_artifact_records_backtest_and_walk_forward_evidence_hashes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is True
    assert result.artifact["walk_forward_evidence_source"] == "walk_forward_report.json"
    assert result.artifact["walk_forward_candidate_profile_hash"] == walk_forward_candidate["candidate_profile_hash"]
    assert result.artifact["walk_forward_candidate_profile_verified"] is True


def test_promotion_artifact_records_no_walk_forward_evidence_when_not_required(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=False)
    _write_report(manager, backtest_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is False
    assert result.artifact["walk_forward_evidence_source"] is None
    assert result.artifact["walk_forward_candidate_profile_hash"] is None
    assert result.artifact["walk_forward_candidate_profile_verified"] is False
    assert result.artifact["regime_classifier_version"] == "market_regime_v2"
    assert result.artifact["allowed_regimes"] == ["uptrend_normal_vol_volume_increasing"]
    assert result.artifact["blocked_regimes"] == ["sideways_low_vol_volume_decreasing"]
    assert result.artifact["live_regime_policy"]["missing_policy_behavior"] == "fail_closed"


def test_promotion_refuses_old_candidate_without_regime_policy(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    for key in (
        "regime_classifier_version",
        "allowed_live_regimes",
        "blocked_live_regimes",
        "regime_evidence",
        "regime_gate_result",
    ):
        candidate.pop(key, None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="regime_policy_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
