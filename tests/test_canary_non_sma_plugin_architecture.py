from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot import profile_cli, runtime_strategy_decision
from bithumb_bot.canonical_decision import (
    build_runtime_replay_execution_plan_bundle,
    export_runtime_replay_decisions,
    validate_canonical_decision_payload,
)
from bithumb_bot.config import LiveModeValidationError, settings, validate_live_strategy_selection
from bithumb_bot.db_core import ensure_schema
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin


def _seed_runtime_db(path: Path) -> int:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        base_ts = 1_700_000_000_000
        for index in range(4):
            close = 10.0 + index
            conn.execute(
                """
                INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    base_ts + index * 60_000,
                    settings.PAIR,
                    settings.INTERVAL,
                    close,
                    close,
                    close,
                    close,
                    1.0,
                ),
            )
        conn.commit()
        return base_ts + 3 * 60_000
    finally:
        conn.close()


def _canary_snapshot() -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id="canary_unit",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=tuple(
            Candle(
                ts=1_700_000_000_000 + index * 60_000,
                open=10.0 + index,
                high=10.0 + index,
                low=10.0 + index,
                close=10.0 + index,
                volume=1.0,
            )
            for index in range(4)
        ),
    )


def test_canary_non_sma_plugin_runtime_envelope_and_planner(tmp_path: Path) -> None:
    db_path = tmp_path / "paper.sqlite"
    through_ts = _seed_runtime_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        adapter = runtime_strategy_decision.get_runtime_decision_adapter("canary_non_sma")
        assert adapter is not None
        from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec

        request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(strategy_name="canary_non_sma"),
            through_ts_ms=through_ts,
        )
        result = adapter.decide(conn, request)

        assert runtime_strategy_decision.is_runtime_strategy_decision_result(result)
        assert result is not None
        assert result.decision.strategy_name == "canary_non_sma"
        assert result.decision.final_signal == "HOLD"
        assert "curr_s" not in result.base_context
        assert "curr_l" not in result.base_context
        assert result.policy_hashes.as_dict()["policy_contract_hash"].startswith("sha256:")
        assert result.replay_fingerprint["policy_input_hash"]

        envelope = DecisionEnvelope.from_runtime_result(result)
        bundle = build_runtime_replay_execution_plan_bundle(
            conn,
            result,
            readiness_payload={
                "residual_inventory_policy_allows_run": True,
                "residual_inventory_policy_allows_buy": True,
                "residual_inventory_policy_allows_sell": True,
                "execution_preflight_ok": True,
                "runtime_health_ok": True,
                "startup_reconcile_ok": True,
                "unresolved_order_gate_ok": True,
                "halt_active": False,
                "cash_available": 100_000.0,
                "target_exposure_krw": None,
                "current_effective_exposure_krw": None,
            },
        )

        assert bundle.summary is not None
        assert bundle.persistence_context["decision_authority_source"] == "DecisionEnvelope.strategy_decision"
        assert bundle.persistence_context["persistence_context_authoritative"] == 0
        assert bundle.persistence_context["policy_contract_hash"].startswith("sha256:")
    finally:
        conn.close()


def test_canary_non_sma_live_real_order_fails_closed_by_plugin_capability() -> None:
    from dataclasses import replace

    plugin = resolve_research_strategy_plugin("canary_non_sma")
    assert plugin.runtime_capabilities.promotion_runtime_decisions_supported is True
    assert plugin.runtime_capabilities.live_dry_run_allowed is True
    assert plugin.runtime_capabilities.live_real_order_allowed is False
    assert runtime_strategy_decision.get_runtime_decision_adapter("canary_non_sma") is not None

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_strategy_selection(
            replace(
                settings,
                MODE="live",
                STRATEGY_NAME="canary_non_sma",
                LIVE_DRY_RUN=False,
                LIVE_REAL_ORDER_ARMED=True,
                APPROVED_STRATEGY_PROFILE_PATH="/tmp/profile.json",
            )
        )

    message = str(exc.value)
    assert "live_strategy_capability_validation_failed" in message
    assert "live_real_order_not_allowed_for_strategy:canary_non_sma" in message
    assert "canary_non_sma_live_real_order_not_allowed" in message


def test_canary_non_sma_runtime_replay_is_promotion_grade_and_read_only(tmp_path: Path) -> None:
    db_path = tmp_path / "paper.sqlite"
    through_ts = _seed_runtime_db(db_path)
    plugin = resolve_research_strategy_plugin("canary_non_sma")
    strategy = plugin.runtime_replay_builder(
        {"market": settings.PAIR, "interval": settings.INTERVAL},
        None,
    )
    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        before_changes = conn.total_changes
        decisions = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy,
            through_ts_list=[through_ts],
            market=settings.PAIR,
            interval=settings.INTERVAL,
            profile_content_hash="sha256:profile",
            dataset_content_hash="sha256:dataset",
            db_data_fingerprint="sha256:db",
            candle_basis="closed_candle",
            execution_timing_policy_hash="sha256:timing",
            strategy_version=plugin.version,
            strategy_decision_contract_version=plugin.decision_contract_version,
        )
        after_changes = conn.total_changes
    finally:
        conn.close()

    assert after_changes == before_changes
    assert len(decisions) == 1
    decision = decisions[0]
    validation = validate_canonical_decision_payload(decision)
    assert validation.promotion_grade is True
    assert decision["strategy_name"] == "canary_non_sma"
    assert decision["policy_contract_hash"].startswith("sha256:")
    assert decision["policy_input_hash"].startswith("sha256:")
    assert decision["policy_decision_hash"].startswith("sha256:")
    assert decision["replay_fingerprint_hash"].startswith("sha256:")
    assert "curr_s" not in decision
    assert "curr_l" not in decision


def test_canary_non_sma_single_replay_command_uses_plugin(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "paper.sqlite"
    through_ts = _seed_runtime_db(db_path)

    rc = profile_cli.cmd_replay_decision(
        db_path=str(db_path),
        strategy_name="canary_non_sma",
        candle_ts=through_ts,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["ok"] is True
    assert payload["strategy"] == "canary_non_sma"
    assert payload["bundle"]["read_only_replay"] is True
    assert payload["bundle"]["post_decision_total_changes_delta"] == 0
    assert payload["bundle"]["policy_hashes"]["policy_contract_hash"].startswith("sha256:")
    assert payload["bundle"]["replay_fingerprint_hash"].startswith("sha256:")


def test_canary_non_sma_research_export_command_uses_generic_plugin_normalization(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    snapshot = _canary_snapshot()
    manifest = SimpleNamespace(
        strategy_name="canary_non_sma",
        market="KRW-BTC",
        interval="1m",
        parameter_space={"CANARY_DECISION_START_INDEX": [0], "CANARY_REASON": ["canary_non_sma_no_order_contract"]},
        execution_model=SimpleNamespace(scenarios=[SimpleNamespace(fee_rate=0.0, slippage_bps=0.0)]),
        execution_timing=None,
    )
    monkeypatch.setattr(profile_cli, "load_manifest", lambda _path: manifest)
    monkeypatch.setattr(profile_cli, "load_dataset_split", lambda **_kwargs: snapshot)
    monkeypatch.setattr(
        profile_cli,
        "_candidate_params_from_manifest",
        lambda _manifest, _candidate_id: {
            "CANARY_DECISION_START_INDEX": 0,
            "CANARY_REASON": "canary_non_sma_no_order_contract",
        },
    )
    monkeypatch.setattr(profile_cli, "_research_export_profile_hash", lambda **_kwargs: "sha256:profile")
    monkeypatch.setattr(
        profile_cli,
        "load_approved_profile",
        lambda _path: {
            "profile_content_hash": "sha256:profile",
            "candidate_profile_hash": "sha256:candidate",
            "strategy_parameters": {
                "CANARY_DECISION_START_INDEX": 0,
                "CANARY_REASON": "canary_non_sma_no_order_contract",
            },
            "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        },
    )
    out_path = tmp_path / "research.json"

    rc = profile_cli.cmd_research_export_decisions(
        manifest_path="manifest.json",
        candidate_id_value="candidate_000",
        split="validation",
        profile_path="profile.json",
        out_path=str(out_path),
    )

    stdout = json.JSONDecoder().raw_decode(capsys.readouterr().out.split("{", 1)[1].join(("{", "")))[0]
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert rc == 0
    assert stdout["ok"] is True
    assert payload["strategy_plugin_contract"]["name"] == "canary_non_sma"
    assert payload["strategy_plugin_contract_hash"].startswith("sha256:")
    assert payload["decisions"]
    assert payload["decisions"][0]["strategy_name"] == "canary_non_sma"
    assert payload["decisions"][0]["policy_contract_hash"].startswith("sha256:")
    assert "curr_s" not in payload["decisions"][0]
    assert "curr_l" not in payload["decisions"][0]


def test_generic_platform_files_have_no_canary_or_replay_strategy_branches() -> None:
    profile_source = Path("src/bithumb_bot/profile_cli.py").read_text(encoding="utf-8")
    profile_tree = ast.parse(profile_source)
    replay_func = next(
        node
        for node in ast.walk(profile_tree)
        if isinstance(node, ast.FunctionDef) and node.name == "cmd_replay_decision"
    )
    replay_source = ast.get_source_segment(profile_source, replay_func) or ""

    assert "sma_with_filter" not in replay_source
    assert "canary_non_sma" not in replay_source
    assert "plugin.runtime_replay_builder" in replay_source
    assert "plugin.single_replay_bundle_builder" in replay_source
    assert "build_sma_with_filter_replay_bundle" not in profile_source

    engine_source = Path("src/bithumb_bot/engine.py").read_text(encoding="utf-8")
    assert "curr_s" not in engine_source
    assert "curr_l" not in engine_source
    assert "SMA{" not in engine_source
    assert "canary_non_sma" not in engine_source

    registry_source = Path("src/bithumb_bot/research/strategy_registry.py").read_text(encoding="utf-8")
    assert "canary_non_sma" not in registry_source

    base_source = Path("src/bithumb_bot/strategy/base.py").read_text(encoding="utf-8")
    assert "SmaPolicyConfig" not in base_source
