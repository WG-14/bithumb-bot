from __future__ import annotations

import ast
from pathlib import Path

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.bithumb import BithumbBroker, BithumbPrivateAPI
from bithumb_bot.config import (
    live_execution_contract_fingerprint,
    live_execution_contract_summary,
    runtime_code_provenance,
    settings,
)


pytestmark = pytest.mark.fast_regression

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "bithumb_bot"
FORBIDDEN_ALTERNATE_POST_ENDPOINTS = {"/v1/order", "/v1/orders"}


def test_private_api_rejects_legacy_order_submit_post_routes() -> None:
    api = BithumbPrivateAPI(
        api_key="k",
        api_secret="s",
        base_url="https://api.bithumb.com",
        dry_run=False,
    )

    for endpoint in sorted(FORBIDDEN_ALTERNATE_POST_ENDPOINTS):
        with pytest.raises(BrokerRejectError, match="alternate order submit route is disabled"):
            api.request("POST", endpoint, json_body={"market": "KRW-BTC"})


def test_private_api_rejects_direct_v2_order_submit_even_with_client_order_id() -> None:
    api = BithumbPrivateAPI(
        api_key="k",
        api_secret="s",
        base_url="https://api.bithumb.com",
        dry_run=False,
    )

    with pytest.raises(BrokerRejectError, match="direct /v2/orders private request is disabled"):
        api.request(
            "POST",
            "/v2/orders",
            json_body={
                "market": "KRW-BTC",
                "side": "bid",
                "order_type": "price",
                "price": "10000",
                "client_order_id": "direct-bypass",
            },
        )


def test_broker_private_request_rejects_legacy_order_submit_post_routes() -> None:
    original_live_dry_run = settings.LIVE_DRY_RUN
    try:
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        broker = BithumbBroker()

        for endpoint in sorted(FORBIDDEN_ALTERNATE_POST_ENDPOINTS):
            with pytest.raises(BrokerRejectError, match="alternate order submit route is disabled"):
                broker._request_private("POST", endpoint, json_body={"market": "KRW-BTC"})
    finally:
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)


def test_source_does_not_add_static_alternate_order_submit_posts() -> None:
    offenders: list[str] = []

    for path in sorted(SRC_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute):
                continue
            name = func.attr
            if name in {"request", "_request_private"}:
                if len(node.args) < 2:
                    continue
                method = node.args[0]
                endpoint = node.args[1]
                if (
                    isinstance(method, ast.Constant)
                    and str(method.value).upper() == "POST"
                    and isinstance(endpoint, ast.Constant)
                    and endpoint.value in FORBIDDEN_ALTERNATE_POST_ENDPOINTS
                ):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
            elif name == "_post_private":
                if not node.args:
                    continue
                endpoint = node.args[0]
                if isinstance(endpoint, ast.Constant) and endpoint.value in FORBIDDEN_ALTERNATE_POST_ENDPOINTS:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")

    assert offenders == []


def test_live_execution_contract_fingerprint_includes_explicit_env_provenance() -> None:
    original_mode = settings.MODE
    base_env = {
        "source_key": "BITHUMB_ENV_FILE_LIVE",
        "env_file": "/runtime/env/live.env",
        "loaded": True,
        "exists": True,
        "override": False,
    }
    drifted_env = {
        **base_env,
        "env_file": "/runtime/env/live-canary.env",
    }

    try:
        object.__setattr__(settings, "MODE", "live")
        base_summary = live_execution_contract_summary(settings, env_summary=base_env)
        drifted_summary = live_execution_contract_summary(settings, env_summary=drifted_env)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert base_summary["explicit_env"] == base_env
    assert live_execution_contract_fingerprint(base_summary) != live_execution_contract_fingerprint(
        drifted_summary
    )


def test_runtime_code_provenance_accepts_explicit_deploy_commit_env(monkeypatch) -> None:
    runtime_code_provenance.cache_clear()
    monkeypatch.setenv("BITHUMB_DEPLOY_COMMIT_SHA", "abc123live")
    monkeypatch.setenv("BITHUMB_DEPLOY_DIRTY", "true")
    try:
        provenance = runtime_code_provenance()
    finally:
        runtime_code_provenance.cache_clear()

    assert provenance == {
        "commit_sha": "abc123live",
        "working_tree_dirty": True,
        "source": "env",
        "git_available": False,
    }
