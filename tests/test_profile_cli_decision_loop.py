from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from bithumb_bot import app, profile_cli
from bithumb_bot.approved_profile import build_approved_profile
from bithumb_bot.broker.order_rules import DerivedOrderConstraints, RuleResolution
from bithumb_bot.db_core import ensure_db
from bithumb_bot.decision_equivalence import (
    compare_decision_export_artifacts,
    load_decision_export_artifact,
)
from bithumb_bot.profile_cli import (
    _candidate_regime_policy_from_approved_profile,
    _validate_research_export_profile_binding,
    cmd_decision_equivalence,
)
from bithumb_bot.research.dataset_snapshot import load_dataset_split
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.parameter_space import candidate_id
from bithumb_bot.research.promotion_gate import build_candidate_profile
from tests.test_decision_equivalence_canonical import _decision


def test_runtime_replay_policy_uses_approved_profile_regime_and_audit_fields() -> None:
    profile = {
        "profile_content_hash": "sha256:profile",
        "profile_mode": "paper",
        "regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": ["uptrend_normal_vol_unknown"],
            "blocked_regimes": [],
        },
        "candidate_profile_hash": "sha256:candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "source_promotion_content_hash": "sha256:promotion",
        "lineage_hash": "sha256:lineage",
        "legacy_compatibility_used": False,
    }

    policy = _candidate_regime_policy_from_approved_profile(profile)

    assert policy["live_regime_policy"] == profile["regime_policy"]
    assert policy["strategy_profile_hash"] == "sha256:profile"
    assert policy["approved_profile_hash"] == "sha256:profile"
    assert policy["approved_profile_verification_ok"] is True
    assert policy["approved_profile_block_reason"] == "ok"
    assert policy["candidate_profile_hash"] == "sha256:candidate"
    assert policy["manifest_hash"] == "sha256:manifest"
    assert policy["dataset_content_hash"] == "sha256:dataset"


def test_cli_dispatch_reaches_research_export_decisions(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(**kwargs):
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(app, "cmd_research_export_decisions", fake_cmd)

    assert app.main(
        [
            "research-export-decisions",
            "--manifest",
            "manifest.json",
            "--candidate-id",
            "candidate_001",
            "--split",
            "validation",
            "--profile",
            "profile.json",
            "--out",
            "research.json",
        ]
    ) == 0
    assert calls == {
        "manifest_path": "manifest.json",
        "candidate_id_value": "candidate_001",
        "split": "validation",
        "out_path": "research.json",
        "profile_path": "profile.json",
    }


def test_cli_dispatch_reaches_runtime_replay_decisions(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(**kwargs):
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(app, "cmd_runtime_replay_decisions", fake_cmd)

    assert app.main(
        [
            "runtime-replay-decisions",
            "--profile",
            "profile.json",
            "--db",
            "paper.sqlite",
            "--through-ts-list",
            "timestamps.json",
            "--out",
            "runtime.json",
        ]
    ) == 0
    assert calls == {
        "profile_path": "profile.json",
        "db_path": "paper.sqlite",
        "through_ts_list_path": "timestamps.json",
        "out_path": "runtime.json",
    }


def test_decision_equivalence_cli_marks_direct_lists_unverified(tmp_path, capsys) -> None:
    research_path = tmp_path / "research_list.json"
    runtime_path = tmp_path / "runtime_list.json"
    research_path.write_text(json.dumps([_decision()], sort_keys=True), encoding="utf-8")
    runtime_path.write_text(json.dumps([_decision()], sort_keys=True), encoding="utf-8")

    rc = cmd_decision_equivalence(
        research_decisions_path=str(research_path),
        runtime_decisions_path=str(runtime_path),
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["ok"] is False
    assert payload["promotion_grade_comparison"] is False
    assert payload["legacy_or_unverified_export"] is True
    assert payload["recommended_next_action"] == "regenerate_decisions_with_repo_owned_export_commands"


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _golden_manifest() -> dict[str, object]:
    return {
        "experiment_id": "golden_sma",
        "hypothesis": "flat no-dust baseline export/replay equivalence.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "golden_decision_fixture",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_WINDOW": [1],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
            "SMA_FILTER_OVEREXT_LOOKBACK": [1],
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": [0.0],
            "SMA_COST_EDGE_ENABLED": [False],
            "SMA_COST_EDGE_MIN_RATIO": [0.0],
            "ENTRY_EDGE_BUFFER_RATIO": [0.0],
            "STRATEGY_EXIT_RULES": ["opposite_cross,max_holding_time"],
            "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0.0]},
        "execution_timing": {
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "candle_close_legacy",
            "allow_same_candle_close_fill": True,
        },
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
            "final_holdout_required_for_promotion": False,
        },
    }


def _write_golden_db(path: Path) -> None:
    conn = ensure_db(str(path))
    try:
        closes = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0, 94.0, 93.0, 92.0, 91.0]
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            for index, close in enumerate(closes):
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close, close, close),
                )
        conn.commit()
    finally:
        conn.close()


def _write_golden_profile(tmp_path: Path, manifest_payload: dict[str, object], db_path: Path) -> tuple[Path, str, str]:
    manifest = parse_manifest(manifest_payload)
    snapshot = load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation")
    params = {key: values[0] for key, values in manifest.parameter_space.items()}
    selected_candidate_id = candidate_id(params, 0)
    candidate = {
        "experiment_id": manifest.experiment_id,
        "manifest_hash": manifest.manifest_hash(),
        "dataset_snapshot_id": manifest.dataset.snapshot_id,
        "dataset_content_hash": snapshot.content_hash(),
        "strategy_name": manifest.strategy_name,
        "parameter_candidate_id": selected_candidate_id,
        "parameter_values": params,
        "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["downtrend_normal_vol_unknown", "downtrend_low_vol_unknown"],
        "blocked_live_regimes": [],
    }
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    promotion = {
        "strategy_name": manifest.strategy_name,
        "strategy_profile_id": f"{manifest.experiment_id}_{selected_candidate_id}",
        "strategy_profile_source_experiment": manifest.experiment_id,
        "strategy_profile_hash": candidate["candidate_profile_hash"],
        "candidate_id": selected_candidate_id,
        "manifest_hash": manifest.manifest_hash(),
        "dataset_snapshot_id": manifest.dataset.snapshot_id,
        "dataset_content_hash": snapshot.content_hash(),
        "market": manifest.market,
        "interval": manifest.interval,
        "repository_version": "test",
        "candidate_profile": build_candidate_profile(candidate),
        "candidate_profile_hash": candidate["candidate_profile_hash"],
        "verified_candidate_profile_hash": candidate["candidate_profile_hash"],
        "gate_result": "PASS",
        "live_regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": candidate["allowed_live_regimes"],
            "blocked_regimes": [],
            "missing_policy_behavior": "fail_closed",
        },
        "generated_at": "2026-05-04T00:00:00+00:00",
    }
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    promotion_path = tmp_path / "promotion.json"
    promotion_path.write_text(json.dumps(promotion, sort_keys=True), encoding="utf-8")
    profile = build_approved_profile(
        promotion=promotion,
        mode="paper",
        source_promotion_path=str(promotion_path),
        market=manifest.market,
        interval=manifest.interval,
        generated_at="2026-05-04T00:00:00+00:00",
    )
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(profile, sort_keys=True), encoding="utf-8")
    return profile_path, selected_candidate_id, snapshot.content_hash()


def test_research_export_profile_binding_rejects_candidate_identity_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "candles.sqlite"
    _write_golden_db(db_path)
    manifest_payload = _golden_manifest()
    profile_path, _selected_candidate_id, _data_fingerprint = _write_golden_profile(
        tmp_path,
        manifest_payload,
        db_path,
    )
    manifest = parse_manifest(manifest_payload)
    snapshot = load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    params = {key: values[0] for key, values in manifest.parameter_space.items()}

    try:
        _validate_research_export_profile_binding(
            manifest=manifest,
            snapshot=snapshot,
            params=params,
            candidate_id_value="candidate_other",
            profile=profile,
        )
    except ValueError as exc:
        assert str(exc) == "research_export_profile_candidate_id_mismatch"
    else:
        raise AssertionError("candidate id mismatch was accepted")


def test_research_export_profile_binding_rejects_candidate_profile_hash_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "candles.sqlite"
    _write_golden_db(db_path)
    manifest_payload = _golden_manifest()
    profile_path, selected_candidate_id, _data_fingerprint = _write_golden_profile(
        tmp_path,
        manifest_payload,
        db_path,
    )
    manifest = parse_manifest(manifest_payload)
    snapshot = load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    profile["candidate_profile_hash"] = "sha256:bad"
    params = {key: values[0] for key, values in manifest.parameter_space.items()}

    try:
        _validate_research_export_profile_binding(
            manifest=manifest,
            snapshot=snapshot,
            params=params,
            candidate_id_value=selected_candidate_id,
            profile=profile,
        )
    except ValueError as exc:
        assert str(exc) == "research_export_profile_candidate_profile_hash_mismatch"
    else:
        raise AssertionError("candidate profile hash mismatch was accepted")


def test_repo_owned_export_replay_artifacts_can_pass_positive_equivalence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "candles.sqlite"
    _write_golden_db(db_path)
    manifest_payload = _golden_manifest()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, sort_keys=True), encoding="utf-8")
    profile_path, selected_candidate_id, data_fingerprint = _write_golden_profile(
        tmp_path,
        manifest_payload,
        db_path,
    )
    research_path = tmp_path / "research_decisions.json"
    runtime_path = tmp_path / "runtime_decisions.json"
    through_ts_path = tmp_path / "through_ts.json"
    through_ts_path.write_text(
        json.dumps({"through_ts_list": [_ts("2023-01-02", minute) for minute in range(5, 10)]}),
        encoding="utf-8",
    )
    source = {
        key: "chance_doc"
        for key in (
            "market_id",
            "bid_min_total_krw",
            "ask_min_total_krw",
            "bid_price_unit",
            "ask_price_unit",
            "order_types",
            "bid_types",
            "ask_types",
            "order_sides",
            "bid_fee",
            "ask_fee",
            "maker_bid_fee",
            "maker_ask_fee",
        )
    }
    rules = RuleResolution(
        rules=DerivedOrderConstraints(
            market_id="KRW-BTC",
            order_types=("limit", "price", "market"),
            bid_types=("price",),
            ask_types=("limit", "market"),
            order_sides=("bid", "ask"),
            bid_fee=0.0,
            ask_fee=0.0,
            maker_bid_fee=0.0,
            maker_ask_fee=0.0,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
        ),
        source=source,
        fallback_used=False,
        source_mode="exchange",
    )
    monkeypatch.setattr(profile_cli, "get_effective_order_rules", lambda _market: rules)
    monkeypatch.setattr("bithumb_bot.strategy.sma.get_effective_order_rules", lambda _market: rules)
    old_db_path = profile_cli.settings.DB_PATH
    object.__setattr__(profile_cli.settings, "DB_PATH", str(db_path))
    try:
        assert app.cmd_research_export_decisions(
            manifest_path=str(manifest_path),
            candidate_id_value=selected_candidate_id,
            split="validation",
            out_path=str(research_path),
            profile_path=str(profile_path),
        ) == 0
        assert app.cmd_runtime_replay_decisions(
            profile_path=str(profile_path),
            db_path=str(db_path),
            through_ts_list_path=str(through_ts_path),
            out_path=str(runtime_path),
        ) == 0
    finally:
        object.__setattr__(profile_cli.settings, "DB_PATH", old_db_path)

    research_artifact = load_decision_export_artifact(research_path, expected_source="research")
    runtime_artifact = load_decision_export_artifact(runtime_path, expected_source="runtime_replay")
    profile_hash = str(load_decision_export_artifact(research_path, expected_source="research").profile_content_hash)
    result = compare_decision_export_artifacts(
        research_artifact=research_artifact,
        runtime_artifact=runtime_artifact,
        profile_hash=profile_hash,
        market="KRW-BTC",
        interval="1m",
        data_fingerprint=data_fingerprint,
    )

    assert research_artifact.source == "research"
    assert runtime_artifact.source == "runtime_replay"
    assert research_artifact.content_hash.startswith("sha256:")
    assert runtime_artifact.content_hash.startswith("sha256:")
    assert {decision["profile_content_hash"] for decision in research_artifact.decisions} == {profile_hash}
    assert {decision["profile_content_hash"] for decision in runtime_artifact.decisions} == {profile_hash}
    assert result.ok is True, result.report
    assert result.report["promotion_grade_comparison"] is True
    assert result.report["legacy_or_unverified_export"] is False
    assert result.report["mismatch_count"] == 0
    assert result.report["missing_research_decisions"] == []
    assert result.report["missing_runtime_decisions"] == []
    assert result.report["research_export_content_hash"].startswith("sha256:")
    assert result.report["runtime_export_content_hash"].startswith("sha256:")
