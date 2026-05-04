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
    diff_profile_to_runtime,
    load_approved_profile,
    load_profile_or_promotion_regime_policy,
    parse_env_file,
    promote_profile_mode,
    runtime_contract_from_env_values,
    runtime_contract_from_settings,
    sha256_prefixed,
    validate_approved_profile,
)
from bithumb_bot.profile_cli import cmd_profile_diff, cmd_profile_generate, cmd_profile_promote, cmd_profile_verify
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


def _write_evidence(tmp_path: Path, name: str = "evidence.json", *, content: str = '{"ok":true}\n') -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


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


def test_profile_verify_fails_on_strategy_parameter_mismatch(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, sma_short=99, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1


def test_profile_verify_passes_when_env_matches(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 0


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


def test_profile_verify_fails_when_source_promotion_hash_drifts(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    env_path = tmp_path / "paper.env"
    _write_env(env_path, profile_path=str(profile_path))
    promotion = _promotion()
    promotion["dataset_content_hash"] = "sha256:other-dataset"
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    write_json_atomic(tmp_path / "promotion.json", promotion)

    assert cmd_profile_verify(profile_path=str(profile_path), env_path=str(env_path)) == 1


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
    with pytest.raises(ApprovedProfileError, match="profile_transition_requires_live_dry_run_parent"):
        promote_profile_mode(
            parent_profile=_profile(str(promotion_path)),
            target_mode="small_live",
            live_readiness_evidence=str(_write_evidence(tmp_path, "live_ready.json")),
        )


def test_live_profile_creation_requires_paper_validation_evidence(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    with pytest.raises(ApprovedProfileError, match="paper_validation_evidence_required"):
        promote_profile_mode(parent_profile=_profile(str(promotion_path)), target_mode="live_dry_run")


def test_profile_transition_preserves_parent_hash_and_stores_evidence_hash(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    evidence_path = _write_evidence(tmp_path, "paper_validation.json")
    parent = _profile(str(promotion_path))
    child = promote_profile_mode(
        parent_profile=parent,
        target_mode="live_dry_run",
        paper_validation_evidence=str(evidence_path),
    )

    assert child["profile_mode"] == "live_dry_run"
    assert child["parent_profile_hash"] == parent["profile_content_hash"]
    assert child["paper_validation_evidence_path"] == str(evidence_path.resolve())
    assert child["paper_validation_evidence_content_hash"] == compute_file_content_hash(evidence_path)


def test_profile_promote_fails_when_paper_evidence_missing(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(tmp_path / "missing.json"),
        live_readiness_evidence=None,
    ) == 1


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
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json")),
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


def test_profile_promote_fails_when_parent_source_promotion_drifts(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    promotion = _promotion(repository_version="other-version")
    write_json_atomic(tmp_path / "promotion.json", promotion)

    assert cmd_profile_promote(
        profile_path=str(profile_path),
        mode="live_dry_run",
        out_path=str(tmp_path / "live_dry_run.json"),
        paper_validation_evidence=str(_write_evidence(tmp_path, "paper_validation.json")),
        live_readiness_evidence=None,
    ) == 1


def test_profile_promote_fails_when_parent_paper_validation_evidence_drifts(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    paper_evidence = _write_evidence(tmp_path, "paper_validation.json")
    live_dry_run = promote_profile_mode(
        parent_profile=paper,
        target_mode="live_dry_run",
        paper_validation_evidence=str(paper_evidence),
    )
    live_dry_run_path = tmp_path / "live_dry_run.json"
    write_json_atomic(live_dry_run_path, live_dry_run)
    paper_evidence.write_text('{"ok":false}\n', encoding="utf-8")

    assert cmd_profile_promote(
        profile_path=str(live_dry_run_path),
        mode="small_live",
        out_path=str(tmp_path / "small_live.json"),
        paper_validation_evidence=None,
        live_readiness_evidence=str(_write_evidence(tmp_path, "live_ready.json")),
    ) == 1


def test_regime_policy_helper_verify_source_fails_on_source_drift(tmp_path: Path) -> None:
    profile_path = _write_profile_with_source(tmp_path)
    promotion = _promotion(repository_version="other-version")
    write_json_atomic(tmp_path / "promotion.json", promotion)

    policy = load_profile_or_promotion_regime_policy(profile_path, verify_source=True)

    assert policy is not None
    assert policy["_policy_load_error"] == "source_promotion_content_hash_mismatch"
    assert policy["approved_profile_verification_ok"] is False
    assert policy["approved_profile_block_reason"] == "source_promotion_content_hash_mismatch"


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
    first_evidence = _write_evidence(tmp_path, "paper_validation_a.json", content='{"ok":true}\n')
    second_evidence = _write_evidence(tmp_path, "paper_validation_b.json", content='{"ok":false}\n')

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


def test_profile_verify_fails_when_evidence_content_drifts(tmp_path: Path) -> None:
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, _promotion())
    paper = _profile(str(promotion_path))
    evidence_path = _write_evidence(tmp_path, "paper_validation.json")
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
