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
        "walk_forward_required": False,
    }
    payload["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(payload))
    payload.update(overrides)
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


def test_promotion_artifact_uses_verified_candidate_profile_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    expected_hash = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["candidate_profile_hash"] == expected_hash
    assert result.artifact["verified_candidate_profile_hash"] == expected_hash
    assert result.artifact["strategy_profile_hash"] == expected_hash
