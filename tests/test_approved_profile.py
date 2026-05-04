from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import (
    ApprovedProfileError,
    build_approved_profile,
    content_hash_payload,
    diff_profile_to_runtime,
    load_approved_profile,
    parse_env_file,
    promote_profile_mode,
    runtime_contract_from_env_values,
    sha256_prefixed,
    validate_approved_profile,
)
from bithumb_bot.profile_cli import cmd_profile_diff, cmd_profile_generate, cmd_profile_verify
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.storage_io import write_json_atomic


def _candidate() -> dict[str, object]:
    payload = {
        "experiment_id": "exp1",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap1",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": {
            "SMA_SHORT": 2,
            "SMA_LONG": 4,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0012,
            "SMA_FILTER_VOL_WINDOW": 10,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.003,
            "SMA_FILTER_OVEREXT_LOOKBACK": 3,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.02,
            "SMA_COST_EDGE_ENABLED": True,
            "SMA_COST_EDGE_MIN_RATIO": 0.001,
            "ENTRY_EDGE_BUFFER_RATIO": 0.0005,
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.001,
            "STRATEGY_EXIT_RULES": "opposite_cross,max_holding_time",
            "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0,
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0,
        },
        "cost_model": {"fee_rate": 0.0025, "slippage_bps": 50.0},
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_unknown"],
        "blocked_live_regimes": ["downtrend_normal_vol_unknown"],
    }
    payload["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(payload))
    return payload


def _promotion(**overrides) -> dict[str, object]:
    candidate = _candidate()
    promotion = {
        "strategy_name": candidate["strategy_name"],
        "strategy_profile_id": "exp1_candidate_001",
        "strategy_profile_source_experiment": "exp1",
        "strategy_profile_hash": candidate["candidate_profile_hash"],
        "candidate_id": candidate["parameter_candidate_id"],
        "manifest_hash": candidate["manifest_hash"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "dataset_content_hash": candidate["dataset_content_hash"],
        "market": "KRW-BTC",
        "interval": "1m",
        "repository_version": "test",
        "candidate_profile": build_candidate_profile(candidate),
        "candidate_profile_hash": candidate["candidate_profile_hash"],
        "verified_candidate_profile_hash": candidate["candidate_profile_hash"],
        "gate_result": "PASS",
        "live_regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": ["uptrend_normal_vol_unknown"],
            "blocked_regimes": ["downtrend_normal_vol_unknown"],
            "missing_policy_behavior": "fail_closed",
        },
        "generated_at": "2026-05-04T00:00:00+00:00",
    }
    promotion.update(overrides)
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    return promotion


def _profile() -> dict[str, object]:
    return build_approved_profile(
        promotion=_promotion(),
        mode="paper",
        source_promotion_path="/runtime/promotion.json",
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-04T00:00:00+00:00",
    )


def _write_env(path: Path, *, sma_short: int = 2, profile_path: str = "") -> None:
    path.write_text(
        "\n".join(
            [
                "MODE=paper",
                f"APPROVED_STRATEGY_PROFILE_PATH={profile_path}",
                "STRATEGY_NAME=sma_with_filter",
                "MARKET=KRW-BTC",
                "INTERVAL=1m",
                f"SMA_SHORT={sma_short}",
                "SMA_LONG=4",
                "SMA_FILTER_GAP_MIN_RATIO=0.0012",
                "SMA_FILTER_VOL_WINDOW=10",
                "SMA_FILTER_VOL_MIN_RANGE_RATIO=0.003",
                "SMA_FILTER_OVEREXT_LOOKBACK=3",
                "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO=0.02",
                "SMA_COST_EDGE_ENABLED=true",
                "SMA_COST_EDGE_MIN_RATIO=0.001",
                "ENTRY_EDGE_BUFFER_RATIO=0.0005",
                "STRATEGY_MIN_EXPECTED_EDGE_RATIO=0.001",
                "STRATEGY_EXIT_RULES=opposite_cross,max_holding_time",
                "STRATEGY_EXIT_MAX_HOLDING_MIN=0",
                "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO=0",
                "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO=0",
                "LIVE_FEE_RATE_ESTIMATE=0.0025",
                "STRATEGY_ENTRY_SLIPPAGE_BPS=50",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_profile_generation_rejects_tampered_candidate_profile_hash(tmp_path: Path) -> None:
    promotion = _promotion(candidate_profile_hash="sha256:tampered")
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    path = tmp_path / "promotion.json"
    write_json_atomic(path, promotion)

    out = tmp_path / "profiles" / "paper.json"
    rc = cmd_profile_generate(
        promotion_path=str(path),
        mode="paper",
        out_path=str(out),
        market=None,
        interval=None,
    )

    assert rc == 1
    assert not out.exists()


def test_generated_at_change_does_not_change_profile_content_hash() -> None:
    first = _profile()
    second = dict(first)
    second["generated_at"] = "2026-05-05T00:00:00+00:00"

    assert validate_approved_profile(second)["profile_content_hash"] == first["profile_content_hash"]


def test_corrupted_profile_content_hash_is_rejected() -> None:
    profile = _profile()
    profile["strategy_parameters"] = {**profile["strategy_parameters"], "SMA_SHORT": 99}

    with pytest.raises(ApprovedProfileError, match="profile_content_hash_mismatch"):
        validate_approved_profile(profile)


def test_profile_diff_detects_env_drift(tmp_path: Path) -> None:
    profile_path = tmp_path / "paper_profile.json"
    write_json_atomic(profile_path, _profile())
    env_path = tmp_path / "paper.env"
    _write_env(env_path, sma_short=99)

    profile = load_approved_profile(profile_path)
    runtime = runtime_contract_from_env_values(parse_env_file(env_path))
    mismatches = diff_profile_to_runtime(profile, runtime)

    assert {"field": "strategy_parameters.SMA_SHORT", "expected": 2, "actual": "99"} in mismatches
    assert cmd_profile_diff(profile_path=str(profile_path), target_env=str(env_path), as_json=True) == 1


def test_profile_verify_fails_on_strategy_parameter_mismatch(tmp_path: Path) -> None:
    profile_path = tmp_path / "paper_profile.json"
    write_json_atomic(profile_path, _profile())
    env_path = tmp_path / "paper.env"
    _write_env(env_path, sma_short=99, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1


def test_profile_verify_passes_when_env_matches(tmp_path: Path) -> None:
    profile_path = tmp_path / "paper_profile.json"
    write_json_atomic(profile_path, _profile())
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 0


def test_paper_profile_cannot_skip_directly_to_small_live() -> None:
    with pytest.raises(ApprovedProfileError, match="profile_transition_requires_live_dry_run_parent"):
        promote_profile_mode(parent_profile=_profile(), target_mode="small_live", live_readiness_evidence="ok")


def test_live_profile_creation_requires_paper_validation_evidence() -> None:
    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_required"):
        promote_profile_mode(parent_profile=_profile(), target_mode="live_dry_run")


def test_profile_transition_preserves_parent_hash() -> None:
    parent = _profile()
    child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence="/runtime/reports/paper_validation.json",
    )

    assert child["profile_mode"] == "live_dry_run"
    assert child["parent_profile_hash"] == parent["profile_content_hash"]
