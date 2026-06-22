from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.operator_commands import _print_residual_operator_fields
from bithumb_bot.reporting import _format_residual_report_fields
from bithumb_bot.runtime.public_api import RuntimeHealthQuery
from bithumb_bot.residual_disposition import build_residual_disposition


def _payload(*, mismatch: bool = False) -> dict[str, object]:
    qty = 0.00009996
    verdict = build_residual_disposition(
        residual_inventory=SimpleNamespace(residual_qty=qty, exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=True,
        residual_policy_allows_buy=True,
        residual_policy_allows_sell=False,
        position_state=SimpleNamespace(
            normalized_exposure=SimpleNamespace(
                has_executable_exposure=False,
                sellable_executable_lot_count=0,
            )
        ),
        authority_assessment={},
        projection_convergence={
            "converged": not mismatch,
            "portfolio_qty": qty,
            "projected_total_qty": qty if not mismatch else qty + 0.0001,
        },
        broker_position_evidence={"broker_qty_known": True, "broker_qty": qty},
        lot_definition=SimpleNamespace(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
            source_mode="ledger",
        ),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )
    return {
        "operator_action_required": verdict.operator_action_required,
        "recommended_action": verdict.recommended_action,
        "manual_exchange_action_required": verdict.manual_exchange_action_required,
        "residual_disposition": verdict.disposition,
        "residual_reason_code": verdict.reason_codes[0],
        "quantity_rule_authority": verdict.quantity_rule_authority,
        "broker_local_projection_state": verdict.broker_local_projection_state,
    }


def test_reports_do_not_recommend_manual_app_sell_for_tracked_sub_min_residual():
    payload = _payload()

    assert payload["residual_disposition"] == "TRACKED_NON_EXECUTABLE"
    assert payload["operator_action_required"] is False
    assert payload["manual_exchange_action_required"] is False
    assert payload["recommended_action"] == "none"


def test_reports_recommend_review_for_projection_mismatch():
    payload = _payload(mismatch=True)

    assert payload["residual_disposition"] == "BLOCKING_INCONSISTENT"
    assert payload["recommended_action"] == "review_recovery_report"
    assert payload["manual_exchange_action_required"] is False


def test_reports_include_quantity_rule_authority():
    payload = _payload()

    assert payload["quantity_rule_authority"] == "persisted_exchange_snapshot"
    assert payload["residual_reason_code"] == "sub_min_qty_residual_tracked"


class _Conn:
    def close(self) -> None:
        return None


class _RecoveryController:
    def evaluate_clearance(self, **_kwargs):
        return SimpleNamespace(allowed=False)

    def apply_clearance(self, _clearance) -> None:
        return None


class _App:
    runtime_gate_api = SimpleNamespace(startup_safety_gate=lambda: None)
    recovery_controller = _RecoveryController()


class _State:
    def __getattr__(self, _name: str):
        return None


def test_health_reports_tracked_residual_manual_action_false(monkeypatch):
    payload = _payload()
    monkeypatch.setattr("bithumb_bot.runtime.public_api.ensure_db", lambda: _Conn())
    monkeypatch.setattr(
        "bithumb_bot.runtime.public_api.compute_runtime_readiness_snapshot",
        lambda _conn: SimpleNamespace(
            as_dict=lambda: {
                "residual_disposition": {"disposition": payload["residual_disposition"]},
                "residual_reason_code": payload["residual_reason_code"],
                "manual_exchange_action_required": payload["manual_exchange_action_required"],
                "quantity_rule_authority": payload["quantity_rule_authority"],
                "broker_local_projection_state": payload["broker_local_projection_state"],
            }
        ),
    )

    health = RuntimeHealthQuery(state_snapshot=lambda: _State(), app_factory=lambda: _App()).get_status()

    assert health["residual_disposition"] == "TRACKED_NON_EXECUTABLE"
    assert health["manual_exchange_action_required"] is False
    assert health["quantity_rule_authority"] == "persisted_exchange_snapshot"


def test_recovery_report_text_includes_residual_disposition(capsys):
    payload = _payload()

    _print_residual_operator_fields("    ", payload)

    out = capsys.readouterr().out
    assert "residual_disposition=TRACKED_NON_EXECUTABLE" in out
    assert "manual_exchange_action_required=0" in out
    assert "quantity_rule_authority=persisted_exchange_snapshot" in out


def test_ops_report_includes_manual_exchange_action_required(capsys):
    payload = _payload()

    print(_format_residual_report_fields(payload))

    out = capsys.readouterr().out
    assert "residual_disposition=TRACKED_NON_EXECUTABLE" in out
    assert "manual_exchange_action_required=0" in out
    assert "quantity_rule_authority=persisted_exchange_snapshot" in out
