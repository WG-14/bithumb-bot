from __future__ import annotations

import socket
import sys
import types
from pathlib import Path

import pytest

from bithumb_bot.config import settings
from bithumb_bot.paths import PathManager


_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if _SRC.is_dir():
    src_path = str(_SRC)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


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


@pytest.fixture
def managed_runtime_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, str]:
    """Inject managed runtime roots/DB under pytest tmp_path (never repo-local)."""
    project_root = _ROOT.resolve()
    runtime_root = (tmp_path / "runtime").resolve()
    assert project_root not in runtime_root.parents

    monkeypatch.setenv("MODE", "paper")
    monkeypatch.setenv("ENV_ROOT", str(runtime_root / "env"))
    monkeypatch.setenv("RUN_ROOT", str(runtime_root / "run"))
    monkeypatch.setenv("DATA_ROOT", str(runtime_root / "data"))
    monkeypatch.setenv("LOG_ROOT", str(runtime_root / "logs"))
    monkeypatch.setenv("BACKUP_ROOT", str(runtime_root / "backup"))

    manager = PathManager.from_env(project_root=project_root)
    db_path = manager.primary_db_path()
    monkeypatch.setenv("DB_PATH", str(db_path))

    return {
        "project_root": str(project_root),
        "runtime_root": str(runtime_root),
        "db_path": str(db_path),
    }


@pytest.fixture
def relaxed_test_order_rules() -> None:
    original_rules = {
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    }
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    try:
        yield
    finally:
        for key, value in original_rules.items():
            object.__setattr__(settings, key, value)


@pytest.fixture(autouse=True)
def _restore_settings_state():
    """Keep direct settings mutations from leaking across test modules."""
    from bithumb_bot.broker import order_rules as _order_rules

    keys = [
        "MODE",
        "DB_PATH",
        "START_CASH_KRW",
        "BUY_FRACTION",
        "MAX_ORDER_KRW",
        "FEE_RATE",
        "LIVE_FEE_RATE_ESTIMATE",
        "MAX_ORDERBOOK_SPREAD_BPS",
        "MAX_MARKET_SLIPPAGE_BPS",
        "MIN_ORDER_NOTIONAL_KRW",
        "PRETRADE_BALANCE_BUFFER_BPS",
        "LIVE_DRY_RUN",
        "LIVE_REAL_ORDER_ARMED",
        "BITHUMB_API_KEY",
        "BITHUMB_API_SECRET",
        "MAX_DAILY_LOSS_KRW",
        "KILL_SWITCH",
        "MAX_OPEN_ORDER_AGE_SEC",
        "LIVE_MIN_ORDER_QTY",
        "LIVE_ORDER_QTY_STEP",
        "LIVE_ORDER_MAX_QTY_DECIMALS",
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS",
        "LIVE_PRICE_REFERENCE_MAX_AGE_SEC",
        "LIVE_FILL_FEE_STRICT_MODE",
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW",
        "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW",
        "LIVE_ALLOW_ORDER_RULE_FALLBACK",
        "PAIR",
    ]
    original = {key: getattr(settings, key) for key in keys if hasattr(settings, key)}
    _order_rules._cached_rules.clear()
    try:
        yield
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)
        _order_rules._cached_rules.clear()
