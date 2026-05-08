from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import (
    ApprovedProfileError,
    build_approved_profile,
    compute_approved_profile_hash,
    compute_file_content_hash,
    content_hash_payload,
    default_profile_output_path,
    diff_profile_to_runtime,
    load_approved_profile,
    load_profile_or_promotion_regime_policy,
    parse_env_file,
    promote_profile_mode,
    runtime_contract_from_env_values,
    runtime_contract_from_settings,
    sha256_prefixed,
    validate_approved_profile,
    write_approved_profile_atomic,
)
from bithumb_bot.evidence_chain import compute_evidence_content_hash
from bithumb_bot.decision_equivalence import compare_decision_equivalence, compute_decision_equivalence_hash
from bithumb_bot.profile_cli import cmd_profile_diff, cmd_profile_generate, cmd_profile_promote, cmd_profile_verify
from bithumb_bot.paths import PathConfig, PathManager, PathPolicyError
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


def _profile(source_promotion_path: str) -> dict[str, object]:
    return build_approved_profile(
        promotion=_promotion(),
        mode="paper",
        source_promotion_path=source_promotion_path,
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


def _write_live_dry_run_env(path: Path, *, profile_path: str = "") -> None:
    _write_env(path, profile_path=profile_path)
    text = path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    text += "LIVE_DRY_RUN=true\nLIVE_REAL_ORDER_ARMED=false\n"
    path.write_text(text, encoding="utf-8")


def _write_profile_with_source(tmp_path: Path) -> Path:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile_path = tmp_path / "paper_profile.json"
    write_json_atomic(profile_path, _profile(source_promotion_path=str(promotion_path)))
    return profile_path


def _evidence_payload(profile: dict[str, object], *, evidence_type: str = "paper_validation") -> dict[str, object]:
    mode = "paper" if evidence_type == "paper_validation" else "live"
    payload: dict[str, object] = {
        "evidence_schema_version": 1,
        "evidence_type": evidence_type,
        "mode": mode,
        "market": profile["market"],
        "interval": profile["interval"],
        "strategy_name": profile["strategy_name"],
        "approved_profile_path": "/external/profile.json",
        "approved_profile_content_hash": profile["profile_content_hash"],
        "source_promotion_path": profile["source_promotion_artifact_path"],
        "source_promotion_content_hash": profile["source_promotion_content_hash"],
        "observation_start": "2026-05-01T00:00:00+00:00",
        "observation_end": "2026-05-03T00:00:00+00:00",
        "observation_duration_seconds": 172800,
        "decision_count": 20,
        "blocked_decision_count": 2,
        "closed_lifecycle_count": 5,
        "gross_pnl": 1000.0,
        "fee_total": 100.0,
        "net_pnl": 900.0,
        "expectancy_per_trade": 180.0,
        "profit_factor": 2.0,
        "fee_drag_ratio": 0.1,
        "execution_quality_status": "pass",
        "execution_quality_breach_count": 0,
        "unresolved_open_orders_count": 0,
        "recovery_blocker_count": 0,
        "runtime_profile_drift_status": "none",
        "db_data_fingerprint": "sha256:db",
        "thresholds": {
            "min_observation_seconds": 86400,
            "min_decision_count": 10,
            "min_closed_lifecycle_count": 3,
            "max_blocked_decision_ratio": 0.5,
            "max_execution_quality_breach_count": 0,
        },
        "generated_at": "2026-05-03T00:00:00+00:00",
    }
    payload["content_hash"] = compute_evidence_content_hash(payload)
    return payload


def _write_evidence(
    tmp_path: Path,
    name: str = "evidence.json",
    *,
    profile: dict[str, object] | None = None,
    evidence_type: str = "paper_validation",
    content: str | None = None,
) -> Path:
    path = tmp_path / name
    if content is None:
        if profile is None:
            content = '{"ok":true}\n'
        else:
            payload = _evidence_payload(profile, evidence_type=evidence_type)
            payload["evidence_path"] = str(path.resolve())
            _attach_decision_equivalence_report(tmp_path, payload, profile)
            payload["content_hash"] = compute_evidence_content_hash(payload)
            content = json.dumps(payload, sort_keys=True) + "\n"
    path.write_text(content, encoding="utf-8")
    return path


def _write_evidence_payload(tmp_path: Path, name: str, payload: dict[str, object]) -> Path:
    path = tmp_path / name
    payload["evidence_path"] = str(path.resolve())
    profile_hash = str(payload.get("approved_profile_content_hash") or "sha256:profile")
    profile_stub = {
        "profile_content_hash": profile_hash,
        "dataset_content_hash": payload.get("decision_equivalence_dataset_content_hash")
        or "sha256:dataset",
    }
    _attach_decision_equivalence_report(tmp_path, payload, profile_stub)
    payload["content_hash"] = compute_evidence_content_hash(payload)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _attach_decision_equivalence_report(
    tmp_path: Path,
    payload: dict[str, object],
    profile: dict[str, object],
) -> Path:
    if payload.get("decision_equivalence_report_path") and payload.get("decision_equivalence_content_hash"):
        return Path(str(payload["decision_equivalence_report_path"]))
    report_path = tmp_path / f"decision_equivalence_{len(list(tmp_path.glob('decision_equivalence_*.json')))}.json"
    report = {
        "schema_version": 2,
        "comparison_contract_version": "canonical_decision_v1",
        "canonical_schema": True,
        "legacy_schema": False,
        "promotion_grade_comparison": True,
        "ok": True,
        "reason_codes": [],
        "profile_content_hash": profile["profile_content_hash"],
        "approved_profile_hash": profile["profile_content_hash"],
        "market": payload.get("market"),
        "interval": payload.get("interval"),
        "data_fingerprint": profile.get("dataset_content_hash"),
        "dataset_content_hash": profile.get("dataset_content_hash"),
        "research_decision_count": 20,
        "runtime_decision_count": 20,
        "matched_decision_count": 20,
        "mismatched_decision_count": 0,
        "mismatch_count": 0,
        "mismatch_reasons": [],
        "blocked_decision_equivalence": False,
        "missing_research_decisions": [],
        "missing_runtime_decisions": [],
        "mismatches": [],
        "canonical_incomplete_decision_count": 0,
        "canonical_missing_field_count": 0,
        "canonical_missing_fields_by_decision": {},
        "recommended_next_action": "none",
        "generated_at": "2026-05-03T00:00:00+00:00",
    }
    report["content_hash"] = compute_decision_equivalence_hash(report)
    report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    payload["decision_equivalence_report_path"] = str(report_path.resolve())
    payload["decision_equivalence_content_hash"] = report["content_hash"]
    payload["decision_equivalence_approved_profile_hash"] = profile["profile_content_hash"]
    payload["decision_equivalence_dataset_content_hash"] = profile.get("dataset_content_hash")
    payload["matched_decision_count"] = report["matched_decision_count"]
    payload["mismatch_count"] = 0
    return report_path


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


def test_profile_generation_fails_closed_when_required_lineage_missing(tmp_path: Path) -> None:
    promotion = _promotion(lineage_required=True, lineage_hash="sha256:missing")
    promotion.pop("content_hash", None)
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    path = tmp_path / "promotion.json"
    write_json_atomic(path, promotion)

    with pytest.raises(ApprovedProfileError, match="lineage_missing"):
        build_approved_profile(
            promotion=promotion,
            mode="paper",
            source_promotion_path=str(path),
            market="KRW-BTC",
            interval="1m",
        )


def test_profile_generate_refuses_live_modes_without_explicit_transition(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    out = tmp_path / "profiles" / "live.json"

    rc = cmd_profile_generate(
        promotion_path=str(promotion_path),
        mode="small_live",
        out_path=str(out),
        market=None,
        interval=None,
    )

    assert rc == 1
    assert not out.exists()


def test_profile_promote_refuses_legacy_dataset_quality_bypass_for_live_readiness(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    promotion = _promotion(
        legacy_compatibility_used=True,
        dataset_quality_legacy_bypass_used=True,
    )
    write_json_atomic(promotion_path, promotion)
    paper = build_approved_profile(
        promotion=promotion,
        mode="paper",
        source_promotion_path=str(promotion_path),
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-04T00:00:00+00:00",
    )

    with pytest.raises(ApprovedProfileError, match="legacy_dataset_quality_bypass_not_live_ready"):
        promote_profile_mode(
            parent_profile=paper,
            target_mode="live_dry_run",
            paper_validation_evidence=str(tmp_path / "paper_validation.json"),
        )


def test_generated_at_change_does_not_change_profile_content_hash(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    first = _profile(str(promotion_path))
    second = dict(first)
    second["generated_at"] = "2026-05-05T00:00:00+00:00"

    assert validate_approved_profile(second)["profile_content_hash"] == first["profile_content_hash"]


def test_profile_content_hash_field_is_excluded_from_hash_payload(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile = _profile(str(promotion_path))
    first_hash = profile["profile_content_hash"]
    profile["profile_content_hash"] = "sha256:temporary"

    assert compute_approved_profile_hash(profile) == first_hash


def test_strategy_relevant_field_change_changes_profile_content_hash(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile = _profile(str(promotion_path))
    changed = dict(profile)
    changed["market"] = "KRW-ETH"

    assert compute_approved_profile_hash(changed) != profile["profile_content_hash"]


def test_corrupted_profile_content_hash_is_rejected(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile = _profile(str(promotion_path))
    profile["strategy_parameters"] = {**profile["strategy_parameters"], "SMA_SHORT": 99}

    with pytest.raises(ApprovedProfileError, match="profile_content_hash_mismatch"):
        validate_approved_profile(profile)


def test_profile_diff_detects_env_drift(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, sma_short=99)

    profile = load_approved_profile(profile_path)
    runtime = runtime_contract_from_env_values(parse_env_file(env_path))
    mismatches = diff_profile_to_runtime(profile, runtime)

    assert {"field": "strategy_parameters.SMA_SHORT", "expected": 2, "actual": "99"} in mismatches
    assert cmd_profile_diff(profile_path=str(profile_path), target_env=str(env_path), as_json=True) == 1


def test_profile_diff_json_clarifies_artifact_chain_is_not_verified(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))

    assert cmd_profile_diff(profile_path=str(profile_path), target_env=str(env_path), as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["source_promotion_verified"] is False
    assert payload["evidence_verified"] is False
    assert payload["use_profile_verify_for_artifact_chain"] is True


def test_profile_verify_fails_on_strategy_parameter_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, sma_short=99, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "approved_profile_runtime_mismatch"
    assert payload["approved_profile_loaded"] is True
    assert payload["approved_profile_schema_hash_valid"] is True
    assert payload["approved_profile_source_verified"] is True
    assert payload["approved_profile_evidence_verified"] is True
    assert payload["approved_profile_runtime_verified"] is False
    assert payload["approved_profile_mismatch_count"] == 1


def test_profile_verify_passes_when_env_matches(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["approved_profile_loaded"] is True
    assert payload["approved_profile_schema_hash_valid"] is True
    assert payload["approved_profile_source_verified"] is True
    assert payload["approved_profile_evidence_verified"] is True
    assert payload["approved_profile_runtime_verified"] is True
    assert payload["approved_profile_contract_scope"] == "full_approved_profile"
    assert payload["legacy_candidate_profile_path_used"] is False


def test_profile_verify_json_preserves_ambiguous_live_flags_reason(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "live.env"
    _write_env(env_path, profile_path=str(profile_path))
    text = env_path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    env_path.write_text(text + "LIVE_DRY_RUN=true\nLIVE_REAL_ORDER_ARMED=true\n", encoding="utf-8")

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "live_mode_arming_flags_ambiguous"
    assert payload["approved_profile_block_reason"] == "live_mode_arming_flags_ambiguous"


def test_profile_verify_json_preserves_live_not_dry_run_or_armed_reason(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "live.env"
    _write_env(env_path, profile_path=str(profile_path))
    text = env_path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    env_path.write_text(text + "LIVE_DRY_RUN=false\nLIVE_REAL_ORDER_ARMED=false\n", encoding="utf-8")

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "live_mode_not_dry_run_or_armed"
    assert payload["approved_profile_block_reason"] == "live_mode_not_dry_run_or_armed"


def test_profile_verify_fails_when_env_selector_points_to_other_profile(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    other_path = tmp_path / "other_profile.json"
    write_json_atomic(other_path, load_approved_profile(profile_path))
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(other_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1


def test_profile_verify_fails_when_paper_env_selector_missing(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path="")

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1


def test_profile_verify_fails_when_source_promotion_hash_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))
    promotion = _promotion(repository_version="other-version")
    write_json_atomic(tmp_path / "promotion.json", promotion)

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "source_promotion_content_hash_mismatch"
    assert payload["approved_profile_loaded"] is True
    assert payload["approved_profile_schema_hash_valid"] is True
    assert payload["approved_profile_source_verified"] is False
    assert payload["approved_profile_evidence_verified"] is False
    assert payload["approved_profile_runtime_verified"] is False


def test_profile_diff_detects_profile_mode_env_incompatibility(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "live.env"
    _write_env(env_path, profile_path=str(profile_path))
    env_text = env_path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    env_path.write_text(env_text + "LIVE_DRY_RUN=false\nLIVE_REAL_ORDER_ARMED=true\n", encoding="utf-8")

    assert cmd_profile_diff(profile_path=str(profile_path), target_env=str(env_path), as_json=True) == 1


def test_paper_profile_cannot_skip_directly_to_small_live(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    with pytest.raises(ApprovedProfileError, match="profile_transition_requires_live_dry_run_parent"):
        promote_profile_mode(
            parent_profile=parent,
            target_mode="small_live",
            live_readiness_evidence=str(
                _write_evidence(tmp_path, "live_ready.json", profile=parent, evidence_type="live_readiness")
            ),
        )


def test_live_profile_creation_requires_paper_validation_evidence(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_required"):
        promote_profile_mode(parent_profile=_profile(str(promotion_path)), target_mode="live_dry_run")


def test_profile_transition_preserves_parent_hash_and_stores_evidence_hash(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    evidence_path = _write_evidence(tmp_path, "paper_validation.json", profile=parent)
    child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(evidence_path),
    )

    assert child["profile_mode"] == "live_dry_run"
    assert child["parent_profile_hash"] == parent["profile_content_hash"]
    assert child["paper_validation_evidence_path"] == str(evidence_path.resolve())
    assert child["paper_validation_evidence_content_hash"] == json.loads(evidence_path.read_text())["content_hash"]
    assert child["paper_validation_approved_profile_hash"] == parent["profile_content_hash"]


@pytest.mark.parametrize(
    ("field", "weak_value"),
    [
        ("min_observation_seconds", 1),
        ("min_decision_count", 0),
        ("min_closed_lifecycle_count", 0),
        ("max_blocked_decision_ratio", 1.0),
        ("max_execution_quality_breach_count", 999),
    ],
)
def test_paper_validation_evidence_fails_when_artifact_threshold_is_weaker_than_policy(
    tmp_path: Path,
    field: str,
    weak_value: object,
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    payload = _evidence_payload(parent)
    thresholds = dict(payload["thresholds"])
    thresholds[field] = weak_value
    payload["thresholds"] = thresholds
    evidence_path = _write_evidence_payload(tmp_path, f"weak_{field}.json", payload)

    with pytest.raises(
        ApprovedProfileError,
        match=f"paper_validation_evidence_policy_threshold_too_weak:{field}",
    ):
        promote_profile_mode(
            parent_profile=parent,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


@pytest.mark.parametrize(
    ("field", "weak_value"),
    [
        ("min_observation_seconds", 1),
        ("min_decision_count", 0),
        ("min_closed_lifecycle_count", 0),
        ("max_blocked_decision_ratio", 1.0),
        ("max_execution_quality_breach_count", 999),
    ],
)
def test_live_readiness_evidence_fails_when_artifact_threshold_is_weaker_than_policy(
    tmp_path: Path,
    field: str,
    weak_value: object,
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    payload = _evidence_payload(live_dry_run, evidence_type="live_readiness")
    thresholds = dict(payload["thresholds"])
    thresholds[field] = weak_value
    payload["thresholds"] = thresholds
    evidence_path = _write_evidence_payload(tmp_path, f"weak_live_{field}.json", payload)

    with pytest.raises(
        ApprovedProfileError,
        match=f"live_readiness_evidence_policy_threshold_too_weak:{field}",
    ):
        promote_profile_mode(
            parent_profile=live_dry_run,
            target_mode="small_live",
            live_readiness_evidence=str(evidence_path),
        )


def test_paper_validation_evidence_fails_when_profile_hash_mismatches(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    payload = _evidence_payload(parent)
    payload["approved_profile_content_hash"] = "sha256:other"
    evidence_path = _write_evidence_payload(tmp_path, "paper_validation.json", payload)

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_profile_hash_mismatch"):
        promote_profile_mode(
            parent_profile=parent,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_paper_validation_evidence_fails_when_observation_window_too_short(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    payload = _evidence_payload(parent)
    payload["observation_end"] = "2026-05-01T01:00:00+00:00"
    payload["observation_duration_seconds"] = 3600
    evidence_path = _write_evidence_payload(tmp_path, "paper_validation.json", payload)

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_observation_window_insufficient"):
        promote_profile_mode(
            parent_profile=parent,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_paper_validation_evidence_fails_when_decision_or_lifecycle_count_insufficient(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    for field, reason in (
        ("decision_count", "paper_validation_evidence_decision_count_insufficient"),
        ("closed_lifecycle_count", "paper_validation_evidence_closed_lifecycle_count_insufficient"),
    ):
        payload = _evidence_payload(parent)
        payload[field] = 0
        if field == "decision_count":
            payload["blocked_decision_count"] = 0
        evidence_path = _write_evidence_payload(tmp_path, f"{field}.json", payload)
        with pytest.raises(ApprovedProfileError, match=reason):
            promote_profile_mode(
                parent_profile=parent,
                target_mode="live_dry_run",
                paper_validation_evidence=str(evidence_path),
            )


def test_paper_validation_evidence_fails_on_execution_quality_unresolved_or_recovery(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    cases = (
        ("execution_quality_breach_count", 1, "paper_validation_evidence_execution_quality_breached"),
        ("unresolved_open_orders_count", 1, "paper_validation_evidence_unresolved_orders_present"),
        ("recovery_blocker_count", 1, "paper_validation_evidence_recovery_blocker_present"),
    )
    for field, value, reason in cases:
        payload = _evidence_payload(parent)
        payload[field] = value
        evidence_path = _write_evidence_payload(tmp_path, f"{field}.json", payload)
        with pytest.raises(ApprovedProfileError, match=reason):
            promote_profile_mode(
                parent_profile=parent,
                target_mode="live_dry_run",
                paper_validation_evidence=str(evidence_path),
            )


def test_evidence_fails_when_db_data_fingerprint_is_missing_or_malformed(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    for name, value in (("missing", None), ("empty", ""), ("malformed", "md5:abc"), ("non_string", 123)):
        payload = _evidence_payload(parent)
        if value is None:
            payload.pop("db_data_fingerprint")
        else:
            payload["db_data_fingerprint"] = value
        evidence_path = _write_evidence_payload(tmp_path, f"db_fingerprint_{name}.json", payload)
        with pytest.raises(
            ApprovedProfileError,
            match="paper_validation_evidence_schema_invalid:db_data_fingerprint",
        ):
            promote_profile_mode(
                parent_profile=parent,
                target_mode="live_dry_run",
                paper_validation_evidence=str(evidence_path),
            )


def test_paper_validation_not_applicable_execution_quality_is_policy_explicit(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    payload = _evidence_payload(parent)
    payload["execution_quality_status"] = "not_applicable"
    evidence_path = _write_evidence_payload(tmp_path, "paper_not_applicable.json", payload)

    child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(evidence_path),
    )

    assert child["paper_validation_evidence_content_hash"] == json.loads(evidence_path.read_text())["content_hash"]


def test_live_readiness_not_applicable_execution_quality_fails_by_default(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    payload = _evidence_payload(live_dry_run, evidence_type="live_readiness")
    payload["execution_quality_status"] = "not_applicable"
    evidence_path = _write_evidence_payload(tmp_path, "live_not_applicable.json", payload)

    with pytest.raises(
        ApprovedProfileError,
        match="live_readiness_evidence_execution_quality_not_applicable",
    ):
        promote_profile_mode(
            parent_profile=live_dry_run,
            target_mode="small_live",
            live_readiness_evidence=str(evidence_path),
        )


def test_live_readiness_evidence_fails_closed_with_equivalent_semantic_checks(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    payload = _evidence_payload(live_dry_run, evidence_type="live_readiness")
    payload["closed_lifecycle_count"] = 0
    evidence_path = _write_evidence_payload(tmp_path, "live_ready.json", payload)

    with pytest.raises(ApprovedProfileError, match="live_readiness_evidence_closed_lifecycle_count_insufficient"):
        promote_profile_mode(
            parent_profile=live_dry_run,
            target_mode="small_live",
            live_readiness_evidence=str(evidence_path),
        )


def test_profile_promote_refuses_malformed_semantic_evidence(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    evidence_path = _write_evidence(tmp_path, "malformed.json", content='{"ok":true}\n')

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(evidence_path),
        live_readiness_evidence=None,
    ) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["error"].startswith("paper_validation_evidence_schema_invalid")
    assert payload["recommended_next_action"] == "regenerate_typed_evidence_artifact"


def test_profile_promote_failure_json_recommends_policy_threshold_recovery(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    thresholds = dict(payload["thresholds"])
    thresholds["min_decision_count"] = 0
    payload["thresholds"] = thresholds
    evidence_path = _write_evidence_payload(tmp_path, "weak_policy.json", payload)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(evidence_path),
        live_readiness_evidence=None,
    ) == 1
    output = json.loads(capsys.readouterr().out)

    assert output["ok"] is False
    assert output["command"] == "profile-promote"
    assert output["artifact_path"] == str(evidence_path)
    assert output["error"] == "paper_validation_evidence_policy_threshold_too_weak:min_decision_count"
    assert (
        output["recommended_next_action"]
        == "regenerate_typed_evidence_with_repo_trusted_thresholds_or_update_policy"
    )


def test_profile_promote_failure_json_recommends_db_fingerprint_recovery(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload.pop("db_data_fingerprint")
    evidence_path = _write_evidence_payload(tmp_path, "missing_db_fingerprint.json", payload)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(evidence_path),
        live_readiness_evidence=None,
    ) == 1
    output = json.loads(capsys.readouterr().out)

    assert output["error"] == "paper_validation_evidence_schema_invalid:db_data_fingerprint"
    assert output["recommended_next_action"] == "regenerate_typed_evidence_with_db_fingerprint"


def test_profile_promote_failure_json_recommends_execution_quality_recovery(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, live_dry_run)
    payload = _evidence_payload(live_dry_run, evidence_type="live_readiness")
    payload["execution_quality_status"] = "not_applicable"
    evidence_path = _write_evidence_payload(tmp_path, "live_no_execution_quality.json", payload)

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(tmp_path / "small_live.json"),
        paper_validation_evidence=None,
        live_readiness_evidence=str(evidence_path),
    ) == 1
    output = json.loads(capsys.readouterr().out)

    assert output["error"] == "live_readiness_evidence_execution_quality_not_applicable"
    assert (
        output["recommended_next_action"]
        == "generate_or_attach_execution_quality_evidence_before_promotion"
    )


def test_evidence_content_hash_excludes_generated_at(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    first = _evidence_payload(parent)
    second = dict(first)
    second["generated_at"] = "2026-05-04T00:00:00+00:00"

    assert compute_evidence_content_hash(first) == compute_evidence_content_hash(second)


def _decision(**overrides: object) -> dict[str, object]:
    payload = {
        "decision_contract_version": 1,
        "strategy_contract_version": "sma_strategy_v1",
        "signal_timestamp": "2026-05-01T00:01:00+00:00",
        "candle_ts": 1_714_521_660_000,
        "through_ts_ms": 1_714_521_660_000,
        "candle_basis": "closed",
        "decision_ts": 1_714_521_720_000,
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "side": "BUY",
        "strategy_name": "sma_with_filter",
        "profile_content_hash": "sha256:profile",
        "candidate_profile_hash": "sha256:candidate",
        "dataset_content_hash": "sha256:data",
        "db_data_fingerprint": "sha256:data",
        "market": "KRW-BTC",
        "interval": "1m",
        "blocked_filters": [],
        "prev_s": 100.0,
        "prev_l": 101.0,
        "curr_s": 102.0,
        "curr_l": 101.0,
        "feature_hash": "sha256:feature",
        "gap_ratio": 0.01,
        "range_ratio": 0.02,
        "expected_edge_ratio": 0.01,
        "required_edge_ratio": 0.001,
        "fee_authority_hash": "sha256:fee_authority",
        "fee_model_hash": "sha256:fee",
        "slippage_model_hash": "sha256:slippage",
        "order_rules_hash": "sha256:order_rules",
        "market_regime": "trend",
        "regime_decision": "allowed",
        "regime_block_reason": "",
        "position_state_hash": "sha256:position",
        "entry_allowed": True,
        "exit_allowed": False,
        "dust_state": "flat",
        "effective_flat": True,
        "normalized_exposure_active": False,
        "exit_rule": "",
        "exit_reason": "",
        "exit_evaluations_hash": "sha256:exit_evaluations",
        "execution_timing_policy_hash": "sha256:timing",
        "replay_fingerprint_hash": "sha256:replay",
        "blocked": False,
        "block_reason": "",
    }
    payload.update(overrides)
    return payload


def test_decision_equivalence_passes_on_matching_synthetic_decisions() -> None:
    result = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[_decision()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert result.ok is True
    assert result.report["matched_decision_count"] == 1
    assert result.report["content_hash"] == compute_decision_equivalence_hash(result.report)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("signal_timestamp", "2026-05-01T00:02:00+00:00", "missing_runtime_decision"),
        ("side", "SELL", "decision_final_signal_mismatch"),
        ("strategy_name", "other", "decision_strategy_name_mismatch"),
        ("market", "KRW-ETH", "decision_market_mismatch"),
        ("interval", "5m", "decision_interval_mismatch"),
        ("profile_content_hash", "sha256:other", "decision_profile_hash_mismatch"),
        ("fee_model_hash", "sha256:other", "decision_fee_authority_mismatch"),
        ("slippage_model_hash", "sha256:other", "decision_slippage_model_mismatch"),
        ("blocked", True, "decision_filter_block_reason_mismatch"),
    ],
)
def test_decision_equivalence_fails_with_clear_reason_codes(field: str, value: object, reason: str) -> None:
    runtime = _decision(**{field: value})
    result = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[runtime],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.ok is False
    assert reason in result.report["reason_codes"]


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("exit_rule", "max_holding_time", "decision_exit_rule_mismatch"),
        ("exit_reason", "exit by max holding time", "decision_exit_rule_mismatch"),
        ("position_state_hash", "sha256:other_position", "decision_position_dust_mismatch"),
        ("dust_state", "dust_only", "decision_position_dust_mismatch"),
        ("entry_allowed", False, "decision_position_dust_mismatch"),
        ("exit_allowed", True, "decision_position_dust_mismatch"),
        ("fee_authority_hash", "sha256:other_fee_authority", "decision_fee_authority_mismatch"),
        ("order_rules_hash", "sha256:other_order_rules", "decision_order_rules_mismatch"),
        ("regime_decision", "blocked", "decision_regime_mismatch"),
        ("candle_basis", "unsafe_open_candle", "decision_timestamp_candle_basis_mismatch"),
        ("profile_content_hash", "sha256:other_profile", "decision_profile_hash_mismatch"),
        ("dataset_content_hash", "sha256:other_data", "decision_data_fingerprint_mismatch"),
        ("execution_timing_policy_hash", "sha256:other_timing", "decision_execution_timing_policy_mismatch"),
    ],
)
def test_canonical_decision_equivalence_fails_on_safety_semantic_mutations(
    field: str,
    value: object,
    reason: str,
) -> None:
    result = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[_decision(**{field: value})],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.ok is False
    assert reason in result.report["reason_codes"]
    assert result.report["comparison_contract_version"] == "canonical_decision_v1"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("market", "KRW-ETH", "decision_market_mismatch"),
        ("interval", "5m", "decision_interval_mismatch"),
    ],
)
def test_decision_equivalence_timestamp_only_diagnostics_do_not_pass(
    field: str,
    value: object,
    reason: str,
) -> None:
    result = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[_decision(**{field: value})],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.ok is False
    assert result.report["matched_decision_count"] == 0
    assert "missing_runtime_decision" in result.report["reason_codes"]
    assert reason in result.report["reason_codes"]
    assert result.report["mismatches"][0]["diagnostic_only"] is True


def test_decision_equivalence_hash_excludes_generated_at() -> None:
    first = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[_decision()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        generated_at="2026-05-03T00:00:00+00:00",
    ).report
    second = dict(first)
    second["generated_at"] = "2026-05-04T00:00:00+00:00"

    assert compute_decision_equivalence_hash(first) == compute_decision_equivalence_hash(second)


def test_profile_promote_fails_when_paper_evidence_missing(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(tmp_path / "missing.json"),
        live_readiness_evidence=None,
    ) == 1


def test_profile_promote_fails_when_decision_equivalence_missing(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload["evidence_path"] = str((tmp_path / "missing_decision_equivalence.json").resolve())
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "missing_decision_equivalence.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_decision_equivalence_missing"):
        promote_profile_mode(
            parent_profile=profile,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_profile_promote_fails_when_live_readiness_decision_equivalence_missing(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    payload = _evidence_payload(live_dry_run, evidence_type="live_readiness")
    payload["evidence_path"] = str((tmp_path / "live_missing_decision_equivalence.json").resolve())
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "live_missing_decision_equivalence.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match="live_readiness_evidence_decision_equivalence_missing"):
        promote_profile_mode(
            parent_profile=live_dry_run,
            target_mode="small_live",
            live_readiness_evidence=str(evidence_path),
        )


def test_profile_promote_fails_when_decision_equivalence_report_path_missing(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload["evidence_path"] = str((tmp_path / "decision_report_path_missing.json").resolve())
    payload["decision_equivalence_report_path"] = str((tmp_path / "missing_report.json").resolve())
    payload["decision_equivalence_content_hash"] = "sha256:missing"
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "decision_report_path_missing.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_decision_equivalence_path_not_found"):
        promote_profile_mode(
            parent_profile=profile,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_profile_promote_fails_when_decision_equivalence_hash_mismatches(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload["evidence_path"] = str((tmp_path / "bad_decision_hash.json").resolve())
    _attach_decision_equivalence_report(tmp_path, payload, profile)
    payload["decision_equivalence_content_hash"] = "sha256:bad"
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "bad_decision_hash.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_decision_equivalence_hash_mismatch"):
        promote_profile_mode(
            parent_profile=profile,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_profile_promote_fails_when_decision_equivalence_mismatch_count_nonzero(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload["evidence_path"] = str((tmp_path / "decision_mismatch_count.json").resolve())
    report_path = _attach_decision_equivalence_report(tmp_path, payload, profile)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["ok"] = False
    report["mismatched_decision_count"] = 1
    report["mismatch_count"] = 1
    report["reason_codes"] = ["decision_side_mismatch"]
    report["content_hash"] = compute_decision_equivalence_hash(report)
    report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    payload["decision_equivalence_content_hash"] = report["content_hash"]
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "decision_mismatch_count.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_decision_equivalence_mismatch_count_nonzero"):
        promote_profile_mode(
            parent_profile=profile,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


@pytest.mark.parametrize(
    ("mutations", "reason"),
    [
        ({"ok": False}, "paper_validation_evidence_decision_equivalence_not_ok"),
        (
            {"missing_research_decisions": ["2026-05-03T00:00:00+00:00|KRW-BTC|1m"]},
            "paper_validation_evidence_decision_equivalence_missing_research_decisions",
        ),
        (
            {"missing_runtime_decisions": ["2026-05-03T00:00:00+00:00|KRW-BTC|1m"]},
            "paper_validation_evidence_decision_equivalence_missing_runtime_decisions",
        ),
        (
            {"profile_content_hash": "sha256:other", "approved_profile_hash": "sha256:other"},
            "paper_validation_evidence_decision_equivalence_profile_hash_mismatch",
        ),
        ({"market": "KRW-ETH"}, "paper_validation_evidence_decision_equivalence_market_mismatch"),
        ({"interval": "5m"}, "paper_validation_evidence_decision_equivalence_interval_mismatch"),
        (
            {"db_data_fingerprint": "sha256:other_db"},
            "paper_validation_evidence_decision_equivalence_data_fingerprint_mismatch",
        ),
        (
            {
                "comparison_contract_version": "legacy_shallow_v1",
                "canonical_schema": False,
                "legacy_schema": True,
            },
            "paper_validation_evidence_decision_equivalence_legacy_schema",
        ),
        (
            {"promotion_grade_comparison": False},
            "paper_validation_evidence_decision_equivalence_not_promotion_grade",
        ),
        (
            {"canonical_incomplete_decision_count": 1},
            "paper_validation_evidence_decision_equivalence_incomplete_canonical",
        ),
    ],
)
def test_profile_promote_fails_when_decision_equivalence_semantics_invalid(
    tmp_path: Path,
    mutations: dict[str, object],
    reason: str,
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    profile = load_approved_profile(profile_path)
    payload = _evidence_payload(profile)
    payload["evidence_path"] = str((tmp_path / "decision_semantics_invalid.json").resolve())
    report_path = _attach_decision_equivalence_report(tmp_path, payload, profile)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.update(mutations)
    report["content_hash"] = compute_decision_equivalence_hash(report)
    report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    payload["decision_equivalence_content_hash"] = report["content_hash"]
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path = tmp_path / "decision_semantics_invalid.json"
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ApprovedProfileError, match=reason):
        promote_profile_mode(
            parent_profile=profile,
            target_mode="live_dry_run",
            paper_validation_evidence=str(evidence_path),
        )


def test_profile_promote_rejects_repo_local_evidence_path(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(Path.cwd() / "README.md"),
        live_readiness_evidence=None,
    ) == 1


def test_profile_promote_fails_when_live_readiness_evidence_missing(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, live_dry_run)

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(tmp_path / "small_live.json"),
        paper_validation_evidence=None,
        live_readiness_evidence=str(tmp_path / "missing_live_ready.json"),
    ) == 1


def test_profile_promote_fails_when_parent_source_promotion_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    promotion = _promotion(repository_version="other-version")
    write_json_atomic(tmp_path / "promotion.json", promotion)
    out_path = tmp_path / "live_dry_run.json"

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(out_path),
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json")),
        live_readiness_evidence=None,
    ) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["error"] == "source_promotion_content_hash_mismatch"
    assert not out_path.exists()


def test_profile_promote_small_live_fails_when_parent_source_promotion_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, live_dry_run)
    write_json_atomic(promotion_path, _promotion(repository_version="other-version"))
    out_path = tmp_path / "small_live.json"

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(out_path),
        paper_validation_evidence=None,
        live_readiness_evidence=str(
            _write_evidence(tmp_path, "live_ready.json", profile=live_dry_run, evidence_type="live_readiness")
        ),
    ) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["error"] == "source_promotion_content_hash_mismatch"
    assert not out_path.exists()


def test_profile_promote_fails_when_parent_paper_validation_evidence_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    paper_evidence = _write_evidence(tmp_path, "paper_validation.json", profile=paper)
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(paper_evidence),
    )
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, live_dry_run)
    paper_evidence.write_text('{"ok":false}\n', encoding="utf-8")
    out_path = tmp_path / "small_live.json"

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(out_path),
        paper_validation_evidence=None,
        live_readiness_evidence=str(
            _write_evidence(tmp_path, "live_ready.json", profile=live_dry_run, evidence_type="live_readiness")
        ),
    ) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["error"] == "paper_validation_evidence_content_hash_mismatch"
    assert not out_path.exists()


def test_profile_promote_fails_when_any_attached_parent_evidence_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json", profile=paper)),
    )
    attached_live_ready = _write_evidence(
        tmp_path,
        "attached_live_ready.json",
        profile=live_dry_run,
        evidence_type="live_readiness",
    )
    live_dry_run["live_readiness_evidence_path"] = str(attached_live_ready.resolve())
    live_dry_run["live_readiness_evidence_content_hash"] = json.loads(attached_live_ready.read_text())["content_hash"]
    live_dry_run["live_readiness_approved_profile_hash"] = live_dry_run["profile_content_hash"]
    live_dry_run.pop("profile_content_hash")
    live_dry_run["profile_content_hash"] = compute_approved_profile_hash(live_dry_run)
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, validate_approved_profile(live_dry_run))
    attached_live_ready.write_text('{"ok":false}\n', encoding="utf-8")
    out_path = tmp_path / "small_live.json"

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(out_path),
        paper_validation_evidence=None,
        live_readiness_evidence=str(
            _write_evidence(tmp_path, "new_live_ready.json", profile=live_dry_run, evidence_type="live_readiness")
        ),
    ) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["error"] == "live_readiness_evidence_content_hash_mismatch"
    assert not out_path.exists()


def test_regime_policy_helper_verify_source_fails_on_source_drift(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    promotion = _promotion(repository_version="other-version")
    write_json_atomic(tmp_path / "promotion.json", promotion)

    policy = load_profile_or_promotion_regime_policy(profile_path, verify_source=True)

    assert policy is not None
    assert policy["_policy_load_error"] == "source_promotion_content_hash_mismatch"
    assert policy["approved_profile_verification_ok"] is False
    assert policy["approved_profile_block_reason"] == "source_promotion_content_hash_mismatch"
    assert policy["approved_profile_loaded"] is True
    assert policy["approved_profile_schema_hash_valid"] is True
    assert policy["approved_profile_source_verified"] is False
    assert policy["approved_profile_evidence_verified"] is False
    assert policy["approved_profile_runtime_verified"] is False


def test_regime_policy_helper_without_verify_source_marks_legacy_scope(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)

    policy = load_profile_or_promotion_regime_policy(
        profile_path,
        verify_source=False,
        approved_profile_contract_scope="legacy_regime_policy_only",
    )

    assert policy is not None
    assert policy["approved_profile_verification_ok"] is False
    assert policy["approved_profile_block_reason"] == "legacy_regime_policy_only_source_not_verified"
    assert policy["approved_profile_contract_scope"] == "legacy_regime_policy_only"
    assert policy["approved_profile_loaded"] is True
    assert policy["approved_profile_schema_hash_valid"] is True
    assert policy["approved_profile_source_verified"] is False
    assert policy["approved_profile_evidence_verified"] is False
    assert policy["approved_profile_runtime_verified"] is False


def test_runtime_contract_settings_supports_approved_profile_alias_with_canonical_precedence(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical.json"
    alias = tmp_path / "alias.json"

    class Cfg:
        MODE = "paper"
        LIVE_DRY_RUN = True
        LIVE_REAL_ORDER_ARMED = False
        APPROVED_STRATEGY_PROFILE_PATH = str(canonical)
        STRATEGY_APPROVED_PROFILE_PATH = str(alias)
        STRATEGY_NAME = "sma_with_filter"
        PAIR = "KRW-BTC"
        INTERVAL = "1m"
        SMA_SHORT = 2
        SMA_LONG = 4
        SMA_FILTER_GAP_MIN_RATIO = 0.0012
        SMA_FILTER_VOL_WINDOW = 10
        SMA_FILTER_VOL_MIN_RANGE_RATIO = 0.003
        SMA_FILTER_OVEREXT_LOOKBACK = 3
        SMA_FILTER_OVEREXT_MAX_RETURN_RATIO = 0.02
        SMA_COST_EDGE_ENABLED = True
        SMA_COST_EDGE_MIN_RATIO = 0.001
        ENTRY_EDGE_BUFFER_RATIO = 0.0005
        STRATEGY_MIN_EXPECTED_EDGE_RATIO = 0.001
        STRATEGY_EXIT_RULES = "opposite_cross,max_holding_time"
        STRATEGY_EXIT_MAX_HOLDING_MIN = 0
        STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO = 0
        STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO = 0
        LIVE_FEE_RATE_ESTIMATE = 0.0025
        STRATEGY_ENTRY_SLIPPAGE_BPS = 50

    assert runtime_contract_from_settings(Cfg)["profile_selector"] == str(canonical)
    Cfg.APPROVED_STRATEGY_PROFILE_PATH = ""
    assert runtime_contract_from_settings(Cfg)["profile_selector"] == str(alias)


def test_changing_evidence_content_changes_child_profile_hash(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    first_evidence = _write_evidence(tmp_path, "paper_validation_a.json", profile=parent)
    second_payload = _evidence_payload(parent)
    second_payload["net_pnl"] = 100.0
    second_payload["expectancy_per_trade"] = 20.0
    second_payload["evidence_path"] = str((tmp_path / "paper_validation_b.json").resolve())
    _attach_decision_equivalence_report(tmp_path, second_payload, parent)
    second_payload["content_hash"] = compute_evidence_content_hash(second_payload)
    second_evidence = _write_evidence(
        tmp_path,
        "paper_validation_b.json",
        content=json.dumps(second_payload, sort_keys=True) + "\n",
    )

    first_child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(first_evidence),
        generated_at="2026-05-04T00:00:00+00:00",
    )
    second_child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(second_evidence),
        generated_at="2026-05-04T00:00:00+00:00",
    )

    assert first_child["profile_content_hash"] != second_child["profile_content_hash"]


def test_profile_verify_fails_when_evidence_content_drifts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    evidence_path = _write_evidence(tmp_path, "paper_validation.json", profile=paper)
    child = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(evidence_path),
    )
    child_path = tmp_path / "live_dry_run.json"
    write_json_atomic(child_path, child)
    env_path = tmp_path / "live.env"
    _write_live_dry_run_env(env_path, profile_path=str(child_path))
    evidence_path.write_text('{"ok":false}\n', encoding="utf-8")

    assert cmd_profile_verify(profile_path=str(child_path), env_path=str(env_path)) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "paper_validation_evidence_content_hash_mismatch"
    assert payload["approved_profile_loaded"] is True
    assert payload["approved_profile_schema_hash_valid"] is True
    assert payload["approved_profile_source_verified"] is True
    assert payload["approved_profile_evidence_verified"] is False
    assert payload["approved_profile_runtime_verified"] is False


def test_profile_generation_rejects_repo_local_source_promotion_path() -> None:
    with pytest.raises(ApprovedProfileError, match="source_promotion_artifact_path_repo_local_not_allowed"):
        build_approved_profile(
            promotion=_promotion(),
            mode="paper",
            source_promotion_path=str(Path.cwd() / "README.md"),
            market="KRW-BTC",
            interval="1m",
            generated_at="2026-05-04T00:00:00+00:00",
        )


def test_profile_generation_rejects_missing_source_promotion_path(tmp_path: Path) -> None:
    with pytest.raises(ApprovedProfileError, match="source_promotion_artifact_path_not_found"):
        build_approved_profile(
            promotion=_promotion(),
            mode="paper",
            source_promotion_path=str(tmp_path / "missing_promotion.json"),
            market="KRW-BTC",
            interval="1m",
            generated_at="2026-05-04T00:00:00+00:00",
        )


def test_profile_generation_accepts_external_absolute_source_promotion_path(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())

    profile = build_approved_profile(
        promotion=_promotion(),
        mode="paper",
        source_promotion_path=str(promotion_path),
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-04T00:00:00+00:00",
    )

    assert profile["source_promotion_artifact_path"] == str(promotion_path.resolve())


def test_profile_generation_accepts_managed_data_reports_source_promotion_path(tmp_path: Path) -> None:
    manager = PathManager(
        project_root=Path.cwd(),
        config=PathConfig(
            mode="paper",
            env_root=tmp_path / "env_root",
            run_root=tmp_path / "run_root",
            data_root=tmp_path / "data_root",
            log_root=tmp_path / "log_root",
            backup_root=tmp_path / "backup_root",
        ),
    )
    promotion_path = manager.data_dir() / "reports" / "profiles" / "promotion.json"
    promotion_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(promotion_path, _promotion())

    profile = build_approved_profile(
        promotion=_promotion(),
        mode="paper",
        source_promotion_path=str(promotion_path),
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-04T00:00:00+00:00",
        manager=manager,
    )

    assert profile["source_promotion_artifact_path"] == str(promotion_path.resolve())


def test_approved_profile_output_rejects_repo_local_path(tmp_path: Path) -> None:
    manager = PathManager(
        project_root=Path.cwd(),
        config=PathConfig(
            mode="paper",
            env_root=tmp_path / "env_root",
            run_root=tmp_path / "run_root",
            data_root=tmp_path / "data_root",
            log_root=tmp_path / "log_root",
            backup_root=tmp_path / "backup_root",
        ),
    )
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile = _profile(str(promotion_path))
    out_path = Path.cwd() / "tmp" / "repo_local_profile.json"

    with pytest.raises(PathPolicyError, match="profile output path must be outside repository"):
        write_approved_profile_atomic(out_path, profile, manager=manager)

    assert not out_path.exists()


def test_default_approved_profile_output_uses_managed_data_reports(tmp_path: Path) -> None:
    manager = PathManager(
        project_root=Path.cwd(),
        config=PathConfig(
            mode="paper",
            env_root=tmp_path / "env_root",
            run_root=tmp_path / "run_root",
            data_root=tmp_path / "data_root",
            log_root=tmp_path / "log_root",
            backup_root=tmp_path / "backup_root",
        ),
    )
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    profile = _profile(str(promotion_path))

    out_path = default_profile_output_path(manager=manager, profile=profile)
    written = write_approved_profile_atomic(out_path, profile, manager=manager)

    assert written.parent == manager.data_dir() / "reports" / "profiles"
    assert load_approved_profile(written)["profile_content_hash"] == profile["profile_content_hash"]


def test_profile_promote_accepts_managed_data_reports_evidence_path(tmp_path: Path) -> None:
    manager = PathManager(
        project_root=Path.cwd(),
        config=PathConfig(
            mode="paper",
            env_root=tmp_path / "env_root",
            run_root=tmp_path / "run_root",
            data_root=tmp_path / "data_root",
            log_root=tmp_path / "log_root",
            backup_root=tmp_path / "backup_root",
        ),
    )
    promotion_path = manager.data_dir() / "reports" / "research" / "promotion.json"
    evidence_path = manager.data_dir() / "reports" / "profiles" / "paper_validation.json"
    promotion_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(promotion_path, _promotion())
    parent = _profile(str(promotion_path))
    payload = _evidence_payload(parent)
    payload["evidence_path"] = str(evidence_path.resolve())
    _attach_decision_equivalence_report(tmp_path, payload, parent)
    payload["content_hash"] = compute_evidence_content_hash(payload)
    evidence_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(evidence_path),
        manager=manager,
    )

    assert child["paper_validation_evidence_path"] == str(evidence_path.resolve())
    assert child["paper_validation_evidence_content_hash"] == json.loads(evidence_path.read_text())["content_hash"]


def test_live_runtime_arming_ambiguity_returns_reason_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "live.env"
    _write_env(env_path, profile_path=str(profile_path))
    text = env_path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    env_path.write_text(text + "LIVE_DRY_RUN=true\nLIVE_REAL_ORDER_ARMED=true\n", encoding="utf-8")

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    assert "live_mode_arming_flags_ambiguous" in capsys.readouterr().out


def test_live_runtime_unarmed_non_dry_run_returns_reason_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "live.env"
    _write_env(env_path, profile_path=str(profile_path))
    text = env_path.read_text(encoding="utf-8").replace("MODE=paper", "MODE=live")
    env_path.write_text(text + "LIVE_DRY_RUN=false\nLIVE_REAL_ORDER_ARMED=false\n", encoding="utf-8")

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1
    assert "live_mode_not_dry_run_or_armed" in capsys.readouterr().out
