from __future__ import annotations

import json

from bithumb_bot.h74_equivalence_manifest import (
    build_h74_equivalence_manifest,
    compare_h74_equivalence,
)


def test_fee_mismatch_marks_experiment_equivalence_mismatch(tmp_path) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {
                    "fee_rate": 0.0004,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )
    manifest = build_h74_equivalence_manifest(
        source_artifact_path=source,
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    result = compare_h74_equivalence(
        manifest,
        current_fee_rate=0.0025,
        current_fee_authority_source="chance_doc",
        current_order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert result["experiment_equivalence_status"] == "mismatch"
    assert result["fee_comparison"]["match"] is False


def test_h74_manifest_binds_time_window_and_exit_policy() -> None:
    manifest = build_h74_equivalence_manifest(
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert manifest["time_window"] == {
        "timezone": "Asia/Seoul",
        "start_hour_kst": 9,
        "end_hour_kst": 11,
    }
    assert manifest["exit_policy"]["rules"] == "max_holding_time"
    assert manifest["exit_policy"]["max_holding_min"] == 74
    assert manifest["order_rules"]["min_qty"] == 0.0001
    assert manifest["order_rules"]["min_notional_krw"] == 5000.0


def test_missing_original_artifact_does_not_pass_equivalence() -> None:
    manifest = build_h74_equivalence_manifest(
        source_artifact_path="/tmp/definitely-missing-h74-source-artifact.json",
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    result = compare_h74_equivalence(
        manifest,
        current_fee_rate=0.0004,
        current_fee_authority_source="runtime_fee_authority",
        current_order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert result["experiment_equivalence_status"] == "unknown_source_artifact_missing"
    assert manifest["source_artifact_status"] == "missing"
    assert manifest["source_artifact_schema"] == "missing"
    assert manifest["source_candidate_id"] is None
    assert manifest["source_backtest_report_hash"] is None
    assert manifest["fee_rate"] is None
    assert manifest["slippage_bps"] is None


def test_source_candidate_artifact_fee_slippage_loaded_from_real_schema(tmp_path) -> None:
    source = tmp_path / "candidate_9738b8d6.json"
    source.write_text(
        json.dumps(
            {
                "candidate_id": "candidate_9738b8d6",
                "backtest_report_hash": "sha256:source-report",
                "cost_model": {
                    "fee_rate": 0.0004,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )

    manifest = build_h74_equivalence_manifest(
        source_artifact_path=source,
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert manifest["source_artifact_status"] == "loaded"
    assert manifest["source_candidate_id"] == "candidate_9738b8d6"
    assert manifest["source_backtest_report_hash"] == "sha256:source-report"
    assert manifest["source_artifact_schema"] == "cost_model"
    assert manifest["source_artifact_hash"].startswith("sha256:")
    assert manifest["source_assumption_status"] == "valid"
    assert manifest["fee_rate"] == 0.0004
    assert manifest["slippage_bps"] == 10.0
    assert manifest["candle_timing"] == "closed_candle_kst"


def test_missing_source_artifact_never_passes_equivalence(tmp_path) -> None:
    manifest = build_h74_equivalence_manifest(
        source_artifact_path=tmp_path / "missing-candidate_9738b8d6.json",
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )
    result = compare_h74_equivalence(
        manifest,
        current_fee_rate=0.0004,
        current_fee_authority_source="runtime_fee_authority",
        current_order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert manifest["source_artifact_status"] == "missing"
    assert result["experiment_equivalence_status"] == "unknown_source_artifact_missing"


def test_source_missing_slippage_or_candle_timing_never_passes_equivalence(tmp_path) -> None:
    source = tmp_path / "candidate_9738b8d6.json"
    source.write_text(json.dumps({"cost_model": {"fee_rate": 0.0004}}), encoding="utf-8")
    manifest = build_h74_equivalence_manifest(
        source_artifact_path=source,
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )
    result = compare_h74_equivalence(
        manifest,
        current_fee_rate=0.0004,
        current_fee_authority_source="runtime_fee_authority",
        current_order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert manifest["source_assumption_status"] == "missing_required_fields"
    assert manifest["source_missing_assumption_fields"] == ["slippage_bps", "candle_timing"]
    assert manifest["slippage_bps"] is None
    assert manifest["candle_timing"] is None
    assert result["experiment_equivalence_status"] == "unknown_source_assumption_missing"


def test_runtime_base_cost_schema_binds_source_hash_fee_slippage_and_candle_timing(tmp_path) -> None:
    source = tmp_path / "candidate_9738b8d6-runtime-base.json"
    source.write_text(
        json.dumps(
            {
                "candidate_id": "candidate_9738b8d6",
                "backtest_report_hash": "sha256:runtime-base-report",
                "runtime_base_cost_assumption": {
                    "fee_rate": 0.0004,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )

    manifest = build_h74_equivalence_manifest(
        source_artifact_path=source,
        order_rules={"min_qty": 0.0001, "min_notional_krw": 5000.0},
    )

    assert manifest["source_artifact_status"] == "loaded"
    assert manifest["source_artifact_schema"] == "runtime_base_cost_assumption"
    assert manifest["source_artifact_hash"].startswith("sha256:")
    assert manifest["source_candidate_id"] == "candidate_9738b8d6"
    assert manifest["source_backtest_report_hash"] == "sha256:runtime-base-report"
    assert manifest["fee_rate"] == 0.0004
    assert manifest["slippage_bps"] == 10.0
    assert manifest["candle_timing"] == "closed_candle_kst"
    assert manifest["manifest_hash"].startswith("sha256:")
