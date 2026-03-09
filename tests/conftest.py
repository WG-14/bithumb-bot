from __future__ import annotations

import socket
import sys
import types

import pytest


try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    mod = types.ModuleType("httpx")

    class RequestError(Exception):
        pass

    class HTTPStatusError(Exception):
        def __init__(self, message: str, request=None, response=None):
            super().__init__(message)
            self.request = request
            self.response = response

    class Request:
        def __init__(self, method: str, url: str):
            self.method = method
            self.url = url

    class Response:
        def __init__(self, status_code: int, request: Request | None = None, json=None):
            self.status_code = status_code
            self.request = request
            self._json = json

        def raise_for_status(self) -> None:
            if int(self.status_code) >= 400:
                raise HTTPStatusError(
                    f"HTTP {self.status_code}",
                    request=self.request,
                    response=self,
                )

        def json(self):
            return self._json

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    mod.RequestError = RequestError
    mod.HTTPStatusError = HTTPStatusError
    mod.Request = Request
    mod.Response = Response
    mod.Client = Client

    sys.modules["httpx"] = mod


@pytest.fixture(autouse=True)
def _block_external_network(monkeypatch):
    def _deny(*args, **kwargs):
        raise RuntimeError("external network is disabled in tests")

    monkeypatch.setattr(socket, "create_connection", _deny)
