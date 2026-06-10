from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from bithumb_bot import notifier


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            exc = RuntimeError(f"HTTP {self.status_code}")
            exc.response = self
            raise exc


@pytest.mark.notification_transport_mock
def test_transport_contract_2xx_delivered_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("NTFY_TOPIC", "topic-123")
    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(post=lambda *args, **kwargs: FakeResponse(204)))

    result = notifier.notify("probe")

    assert result.final_status == "delivered"
    assert result.delivered_transports == ("ntfy",)
    assert result.attempts[0].http_status == 204


@pytest.mark.notification_transport_mock
def test_transport_contract_429_failed_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("NTFY_TOPIC", "topic-123")
    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(post=lambda *args, **kwargs: FakeResponse(429)))

    result = notifier.notify("probe")

    assert result.final_status == "failed"
    assert result.delivered_transports == ()
    assert result.attempts[0].http_status == 429


@pytest.mark.notification_transport_mock
def test_transport_contract_timeout_failed_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout(*args, **kwargs):
        raise TimeoutError("timed out")

    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("NTFY_TOPIC", "topic-123")
    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(post=timeout))

    result = notifier.notify("probe")

    assert result.final_status == "failed"
    assert result.delivered_transports == ()
    assert result.attempts[0].failure_class == "TimeoutError"
