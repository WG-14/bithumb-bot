from __future__ import annotations

import json
from types import SimpleNamespace

from bithumb_bot.notifier import AlertSeverity, NotificationResult
from bithumb_bot.research import cli as research_cli


def _successful_body(monkeypatch):
    monkeypatch.setattr(research_cli, "load_manifest", lambda path: SimpleNamespace(deployment_tier="research_only"))
    monkeypatch.setattr(research_cli, "_print_report_summary", lambda label, report: None)
    monkeypatch.setattr(
        research_cli,
        "run_research_backtest",
        lambda **kwargs: {
            "deployment_tier": "research_only",
            "promotion_eligibility_gate_result": "FAIL",
            "promotion_blocking_reasons": ["diagnostic_failure"],
            "diagnostic_only": True,
        },
    )


def _result(status: str) -> NotificationResult:
    delivered = ("ntfy",) if status == "delivered" else ()
    attempted = ("ntfy",) if status in {"delivered", "failed"} else ()
    return NotificationResult(
        message="sent",
        severity=AlertSeverity.INFO,
        enabled=True,
        configured=True,
        attempted_transports=attempted,
        delivered_transports=delivered,
        final_status=status,
    )


def test_research_backtest_best_effort_keeps_existing_exit_code(monkeypatch) -> None:
    _successful_body(monkeypatch)
    monkeypatch.setattr(research_cli, "notify", lambda *args, **kwargs: _result("failed"))
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)

    rc = research_cli.cmd_research_backtest(
        manifest_path="manifest.json",
        notification_policy="best_effort",
    )

    assert rc == 0


def test_research_backtest_require_delivery_fails_when_notifier_unconfigured(monkeypatch) -> None:
    monkeypatch.setattr(research_cli, "is_configured", lambda: False)

    rc = research_cli.cmd_research_backtest(
        manifest_path="manifest.json",
        notification_policy="require_delivery",
    )

    assert rc == 1


def test_research_backtest_require_delivery_fails_when_delivery_failed(monkeypatch) -> None:
    _successful_body(monkeypatch)
    monkeypatch.setattr(research_cli, "is_configured", lambda: True)
    monkeypatch.setattr(research_cli, "notify", lambda *args, **kwargs: _result("failed"))
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)

    rc = research_cli.cmd_research_backtest(
        manifest_path="manifest.json",
        notification_policy="require_delivery",
    )

    assert rc == 1


def test_research_backtest_disabled_notification_does_not_attempt_transport(monkeypatch) -> None:
    _successful_body(monkeypatch)

    def _unexpected_notify(*args, **kwargs):
        raise AssertionError("disabled policy must not call notify")

    monkeypatch.setattr(research_cli, "notify", _unexpected_notify)
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)

    rc = research_cli.cmd_research_backtest(
        manifest_path="manifest.json",
        notification_policy="disabled",
    )

    assert rc == 0


def test_research_backtest_records_notification_result(monkeypatch, managed_runtime_env) -> None:
    _successful_body(monkeypatch)
    monkeypatch.setattr(research_cli, "notify", lambda *args, **kwargs: _result("failed"))

    rc = research_cli.cmd_research_backtest(
        manifest_path="manifest.json",
        notification_policy="best_effort",
    )

    assert rc == 0
    path = research_cli.PATH_MANAGER.notification_events_path()
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["source_command"] == "research-backtest"
    assert rows[-1]["event_name"] == "research_command_finished"
    assert rows[-1]["final_status"] == "failed"
