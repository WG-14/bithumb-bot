from __future__ import annotations

import pytest

from bithumb_bot.research import process_runtime
from bithumb_bot.research.process_runtime import (
    ResearchProcessRuntimeError,
    resolve_research_process_runtime,
)


def test_auto_safe_prefers_forkserver_when_available(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["fork", "spawn", "forkserver"])

    runtime = resolve_research_process_runtime(requested_max_workers=2)

    assert runtime.requested_start_method == "auto_safe"
    assert runtime.effective_start_method == "forkserver"
    assert runtime.max_workers_effective == 2


def test_auto_safe_falls_back_to_spawn_without_forkserver(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["spawn", "fork"])

    runtime = resolve_research_process_runtime(requested_max_workers=2)

    assert runtime.effective_start_method == "spawn"


def test_invalid_method_fails_with_available_methods(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["spawn"])

    with pytest.raises(ResearchProcessRuntimeError, match="auto_safe, auto, forkserver, spawn, fork"):
        resolve_research_process_runtime(requested_start_method="bogus", requested_max_workers=2)


def test_explicit_fork_requires_diagnostic_override(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["fork", "spawn"])
    monkeypatch.setattr(process_runtime, "_parent_thread_count", lambda: 2)

    with pytest.raises(ResearchProcessRuntimeError, match="requires BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK=1"):
        resolve_research_process_runtime(requested_start_method="fork", requested_max_workers=2)

    runtime = resolve_research_process_runtime(
        requested_start_method="fork",
        requested_max_workers=2,
        allow_unsafe_fork=True,
    )

    assert runtime.effective_start_method == "fork"
    assert runtime.unsafe_fork_allowed is True
    assert runtime.parent_thread_count == 2


def test_environment_start_method_override(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["forkserver", "spawn"])
    monkeypatch.setenv("BITHUMB_RESEARCH_MP_START_METHOD", "spawn")

    runtime = resolve_research_process_runtime(requested_start_method="forkserver", requested_max_workers=2)

    assert runtime.requested_start_method == "spawn"
    assert runtime.effective_start_method == "spawn"


def test_research_max_workers_env_caps_effective_workers(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["forkserver", "spawn"])
    monkeypatch.setenv("BITHUMB_RESEARCH_MAX_WORKERS", "2")

    runtime = resolve_research_process_runtime(requested_max_workers=4)

    assert runtime.max_workers_requested == 4
    assert runtime.max_workers_effective == 2
    assert runtime.process_budget["research_max_workers_env_cap"] == 2


def test_pytest_xdist_outer_context_detection(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["forkserver", "spawn"])
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw1")
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "3")

    runtime = resolve_research_process_runtime(requested_max_workers=2)

    assert runtime.outer_parallel_context == "pytest-xdist"
    assert runtime.process_budget["outer_worker_id"] == "gw1"
    assert runtime.process_budget["outer_worker_count"] == 3


def test_total_process_budget_caps_inner_workers_when_outer_count_known(monkeypatch) -> None:
    monkeypatch.setattr(process_runtime.mp, "get_all_start_methods", lambda: ["forkserver", "spawn"])
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "2")
    monkeypatch.setenv("BITHUMB_TOTAL_PROCESS_BUDGET", "6")

    runtime = resolve_research_process_runtime(requested_max_workers=8)

    assert runtime.max_workers_effective == 3
    assert runtime.process_budget["total_process_budget"] == 6
