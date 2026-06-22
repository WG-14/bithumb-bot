from __future__ import annotations

from bithumb_bot.cli.registry import command_registry


def test_live_pipeline_smoke_commands_registered_with_metadata() -> None:
    registry = command_registry()

    smoke = registry["live-pipeline-smoke"]
    authority = registry["live-pipeline-smoke-authority"]

    assert smoke.guard_policy == "operator_live_pipeline_smoke"
    assert smoke.requires_live is True
    assert smoke.mutating is True
    assert smoke.writes_db is True
    assert smoke.uses_broker is True
    assert smoke.requires_confirmation is True
    assert smoke.read_only is False
    assert smoke.json_output_supported is True

    assert authority.guard_policy == "operator_live_pipeline_smoke_authority"
    assert authority.produces_artifact is True
    assert authority.uses_broker is False
    assert authority.writes_db is False


def test_live_pipeline_smoke_plan_payload_is_bounded() -> None:
    from bithumb_bot.live_pipeline_smoke import build_live_pipeline_smoke_plan

    payload = build_live_pipeline_smoke_plan(
        cycles=5,
        max_orders=10,
        max_notional_krw=10_000.0,
        market="KRW-BTC",
    )

    assert payload["status"] == "plan"
    assert payload["orders_expected"] == 10
    assert payload["buy_expected"] == 5
    assert payload["sell_expected"] == 5
    assert payload["allowed_sequence"] == ["BUY", "SELL"] * 5
    assert payload["requires_confirmation"] == "LIVE_PIPELINE_SMOKE_5X_10000"


def test_smoke_artifact_not_normal_h74_readiness() -> None:
    from bithumb_bot.live_pipeline_smoke import build_live_pipeline_smoke_plan

    payload = build_live_pipeline_smoke_plan(
        cycles=5,
        max_orders=10,
        max_notional_krw=10_000.0,
        market="KRW-BTC",
    )

    assert payload["readiness_scope"] == "operator_pipeline_only"
    assert payload["normal_h74_readiness"] is False
