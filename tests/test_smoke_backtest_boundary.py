from __future__ import annotations

from pathlib import Path

import pytest

import backtest
from bithumb_bot.approved_profile import ApprovedProfileError, verify_promotion_artifact
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.promotion_gate import validate_backtest_candidate_for_promotion
from tools import diagnostic_smoke_backtest as smoke_backtest


def test_root_backtest_output_is_diagnostic_only_and_non_promotable(monkeypatch) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)

    result = smoke_backtest.backtest(short_n=2, long_n=4, entry="cross")

    assert result["diagnostic_only"] is True
    assert result["scope_badge"] == "DIAGNOSTIC_ONLY"
    assert result["non_promotable"] is True
    assert result["promotion_grade"] is False
    assert result["evidence_scope"] == "smoke_only_not_manifest_backed"
    assert result["standalone_backtest_not_full_validation"] is True


def test_root_backtest_default_refuses_to_run_smoke_backtest(capsys) -> None:
    assert backtest.main(["--short", "2", "--long", "4"]) == 2
    captured = capsys.readouterr()

    assert "diagnostic_only=true" in captured.err
    assert "non_promotable=true" in captured.err
    assert "promotion_grade=false" in captured.err
    assert "evidence_scope=smoke_only_not_manifest_backed" in captured.err
    assert "standalone_backtest_not_full_validation=true" in captured.err
    assert "reason_code=standalone_backtest_not_full_validation" in captured.err
    assert "operator_next_action=use_manifest_backed_research_validation" in captured.err
    assert "uv run bithumb-bot research-validate --manifest <path>" in captured.err


def test_direct_smoke_backtest_main_without_ack_refuses(monkeypatch, capsys) -> None:
    monkeypatch.delenv("SMOKE_BACKTEST_ACK", raising=False)

    assert smoke_backtest.main(["--short", "2", "--long", "4"]) == 2
    captured = capsys.readouterr()

    assert "diagnostic_only=true" in captured.err
    assert "non_promotable=true" in captured.err
    assert "promotion_grade=false" in captured.err
    assert "evidence_scope=smoke_only_not_manifest_backed" in captured.err
    assert "reason_code=standalone_backtest_not_full_validation" in captured.err
    assert "operator_next_action=use_manifest_backed_research_validation" in captured.err
    assert "uv run bithumb-bot research-validate --manifest <path>" in captured.err


def test_direct_smoke_backtest_main_with_flag_runs(monkeypatch, capsys) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.delenv("SMOKE_BACKTEST_ACK", raising=False)
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)

    assert smoke_backtest.main(["--diagnostic-smoke-only", "--short", "2", "--long", "4"]) == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "diagnostic_only=true" in output
    assert "non_promotable=true" in output
    assert "promotion_grade=false" in output


def test_direct_smoke_backtest_main_with_env_ack_runs(monkeypatch, capsys) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.setenv("SMOKE_BACKTEST_ACK", "diagnostic_only")
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)

    assert smoke_backtest.main(["--short", "2", "--long", "4"]) == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "diagnostic_only=true" in output
    assert "non_promotable=true" in output
    assert "promotion_grade=false" in output


def test_root_backtest_refusal_lines_are_generated_from_shared_payload() -> None:
    payload = backtest.ROOT_BACKTEST_REFUSAL
    lines = "\n".join(backtest.root_backtest_refusal_lines())

    assert f"diagnostic_only={str(payload['diagnostic_only']).lower()}" in lines
    assert f"non_promotable={str(payload['non_promotable']).lower()}" in lines
    assert f"promotion_grade={str(payload['promotion_grade']).lower()}" in lines
    assert f"evidence_scope={payload['evidence_scope']}" in lines
    assert f"reason_code={payload['reason_code']}" in lines
    assert f"operator_next_action={payload['operator_next_action']}" in lines
    assert str(payload["promotion_command"]) in lines


def test_root_backtest_diagnostic_opt_in_runs_real_wrapper_path(monkeypatch, capsys) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)

    assert backtest.main(["--diagnostic-smoke-only", "--short", "2", "--long", "4"]) == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "diagnostic_only=true" in output
    assert "non_promotable=true" in output
    assert "promotion_grade=false" in output
    assert "evidence_scope=smoke_only_not_manifest_backed" in output
    assert "standalone_backtest_not_full_validation=true" in output


def test_direct_smoke_artifact_is_rejected_by_promotion_candidate_gate(monkeypatch) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)

    allowed, reasons = validate_backtest_candidate_for_promotion(
        smoke_backtest.backtest(short_n=2, long_n=4, entry="cross")
    )

    assert allowed is False
    assert "smoke_backtest_artifact_not_promotable" in reasons
    assert "backtest_smoke_backtest_artifact_not_promotable" in reasons
    assert "diagnostic_only_evidence_artifact" in reasons


def test_direct_smoke_artifact_is_rejected_by_promotion_artifact_verifier(monkeypatch) -> None:
    candles = [(index * 60_000, float(100 + index)) for index in range(20)]
    monkeypatch.setattr(smoke_backtest, "load_candles", lambda limit: candles)
    smoke_artifact = smoke_backtest.backtest(short_n=2, long_n=4, entry="cross")
    promotion = {
        **smoke_artifact,
        "candidate_profile": dict(smoke_artifact),
    }
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))

    with pytest.raises(ApprovedProfileError, match="promotion_smoke_evidence_not_promotable"):
        verify_promotion_artifact(promotion)


def test_promotion_and_live_paths_do_not_import_legacy_or_smoke_boundaries() -> None:
    repo = Path(__file__).resolve().parents[1]
    forbidden = (
        "SmaCrossStrategy",
        "LegacySmaWithFilterDbAdapter",
        "smoke_backtest",
        "import backtest",
    )
    checked_paths = (
        "src/bithumb_bot/approved_profile.py",
        "src/bithumb_bot/research/promotion_gate.py",
        "src/bithumb_bot/run_loop_execution_planner.py",
        "src/bithumb_bot/decision_envelope.py",
        "src/bithumb_bot/broker/live.py",
    )

    for relative_path in checked_paths:
        source = (repo / relative_path).read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in source, f"{relative_path} imports or names {marker}"


def test_package_level_smoke_backtest_module_is_not_available() -> None:
    import importlib.util

    assert importlib.util.find_spec("bithumb_bot.smoke_backtest") is None


def test_legacy_execution_decision_summary_wrapper_is_non_promotion_grade() -> None:
    summary = build_execution_decision_summary(
        decision_context={"signal": "BUY", "cash_available": 100_000.0},
        raw_signal="BUY",
        final_signal="BUY",
    )
    payload = summary.as_dict()

    assert payload.get("promotion_grade") is not True
    assert payload.get("execution_submit_plan_hash") is None
    assert payload.get("submit_plan_authority") is None
