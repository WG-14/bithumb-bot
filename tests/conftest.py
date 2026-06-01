from __future__ import annotations

import socket
import sys
import types
from dataclasses import fields
from pathlib import Path

import pytest

import bithumb_bot.config as _config_module
from bithumb_bot.config import settings
from bithumb_bot.compat.sma_runtime_compat import legacy_default_strategy_name
from bithumb_bot.paths import PathConfig, PathManager


_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
def _path_manager_for_runtime_root(runtime_root: Path) -> PathManager:
    return PathManager(
        project_root=_ROOT.resolve(),
        config=PathConfig(
            mode="paper",
            env_root=runtime_root / "env",
            run_root=runtime_root / "run",
            data_root=runtime_root / "data",
            log_root=runtime_root / "logs",
            backup_root=runtime_root / "backup",
            archive_root=runtime_root / "archive",
        ),
    )


_BASE_RUNTIME_ROOT = Path("/tmp/bithumb-bot-pytest-runtime").resolve()
_BASE_PATH_MANAGER = _path_manager_for_runtime_root(_BASE_RUNTIME_ROOT)
if _SRC.is_dir():
    src_path = str(_SRC)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def _sync_config_singletons(path_manager=None) -> None:
    manager = _BASE_PATH_MANAGER if path_manager is None else path_manager
    _config_module.settings = settings
    _config_module.PATH_MANAGER = manager
    for module_name, module in tuple(sys.modules.items()):
        if (
            module_name.startswith("bithumb_bot")
            and getattr(module, "settings", None) is not settings
            and hasattr(module, "settings")
        ):
            setattr(module, "settings", settings)
        if getattr(module, "PATH_MANAGER", None) is not manager and hasattr(module, "PATH_MANAGER"):
            setattr(module, "PATH_MANAGER", manager)


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
    object.__setattr__(settings, "DB_PATH", str(db_path))
    _sync_config_singletons(manager)

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
def _restore_global_settings_state(tmp_path: Path):
    """Keep direct settings mutations from leaking across test modules."""
    from bithumb_bot.broker import order_rules as _order_rules
    from bithumb_bot.research import strategy_registry as _strategy_registry
    from bithumb_bot.research import validation_protocol as _validation_protocol

    keys = [field.name for field in fields(type(settings))]
    test_path_manager = _path_manager_for_runtime_root((tmp_path / "runtime-default").resolve())
    _sync_config_singletons(test_path_manager)
    _strategy_registry.reload_research_strategy_plugins_for_tests()
    _validation_protocol._CANDIDATE_SCENARIO_WORKER_CONTEXT = None
    object.__setattr__(settings, "DB_PATH", str(test_path_manager.primary_db_path()))
    object.__setattr__(settings, "STRATEGY_NAME", legacy_default_strategy_name())
    original = {key: getattr(settings, key) for key in keys if hasattr(settings, key)}
    _order_rules._cached_rules.clear()
    try:
        yield
    finally:
        _sync_config_singletons(test_path_manager)
        for key, value in original.items():
            object.__setattr__(settings, key, value)
        _sync_config_singletons(test_path_manager)
        _order_rules._cached_rules.clear()
        _strategy_registry.reload_research_strategy_plugins_for_tests()
        _validation_protocol._CANDIDATE_SCENARIO_WORKER_CONTEXT = None
