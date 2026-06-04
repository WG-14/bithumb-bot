from __future__ import annotations

import multiprocessing as mp
import os
import platform
import threading
from dataclasses import dataclass
from typing import Any


ALLOWED_RESEARCH_START_METHODS = ("auto_safe", "auto", "forkserver", "spawn", "fork")


class ResearchProcessRuntimeError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchProcessRuntime:
    requested_start_method: str
    effective_start_method: str
    available_start_methods: tuple[str, ...]
    max_workers_requested: int
    max_workers_effective: int
    parent_pid: int
    parent_thread_count: int
    platform_system: str
    outer_parallel_context: str | None
    unsafe_fork_allowed: bool
    process_budget: dict[str, object]

    def mp_context(self) -> mp.context.BaseContext:
        return mp.get_context(self.effective_start_method)

    def observability_payload(self) -> dict[str, Any]:
        return {
            "requested_process_start_method": self.requested_start_method,
            "effective_process_start_method": self.effective_start_method,
            "available_process_start_methods": list(self.available_start_methods),
            "parent_pid": self.parent_pid,
            "parent_thread_count_at_pool_create": self.parent_thread_count,
            "platform_system": self.platform_system,
            "outer_parallel_context": self.outer_parallel_context,
            "unsafe_fork_allowed": self.unsafe_fork_allowed,
            "research_max_workers_requested": self.max_workers_requested,
            "research_max_workers_effective": self.max_workers_effective,
            "process_budget": dict(self.process_budget),
        }


def resolve_research_process_runtime(
    *,
    requested_start_method: str = "auto_safe",
    requested_max_workers: int,
    allow_unsafe_fork: bool = False,
    unavailable_start_methods: tuple[str, ...] = (),
) -> ResearchProcessRuntime:
    available = tuple(mp.get_all_start_methods())
    requested = _normalize_requested_start_method(
        os.environ.get("BITHUMB_RESEARCH_MP_START_METHOD") or requested_start_method,
        available_start_methods=available,
    )
    parent_thread_count = _parent_thread_count()
    unsafe_fork_allowed = allow_unsafe_fork or os.environ.get("BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK") == "1"
    selectable = tuple(method for method in available if method not in set(unavailable_start_methods))
    effective = _resolve_effective_start_method(
        requested=requested,
        available=selectable,
        unsafe_fork_allowed=unsafe_fork_allowed,
        parent_thread_count=parent_thread_count,
    )
    max_workers_requested = _positive_int(requested_max_workers, "requested_max_workers")
    max_workers_effective, budget = _resolve_worker_budget(max_workers_requested)
    return ResearchProcessRuntime(
        requested_start_method=requested,
        effective_start_method=effective,
        available_start_methods=available,
        max_workers_requested=max_workers_requested,
        max_workers_effective=max_workers_effective,
        parent_pid=os.getpid(),
        parent_thread_count=parent_thread_count,
        platform_system=platform.system(),
        outer_parallel_context=_outer_parallel_context(),
        unsafe_fork_allowed=unsafe_fork_allowed,
        process_budget={
            **budget,
            "unavailable_process_start_methods": list(unavailable_start_methods),
        },
    )


def process_policy_observability(*, requested_start_method: str, requested_max_workers: int) -> dict[str, Any]:
    available = tuple(mp.get_all_start_methods())
    return {
        "requested_process_start_method": _normalize_requested_start_method(
            requested_start_method,
            available_start_methods=available,
        ),
        "available_process_start_methods": list(available),
        "research_max_workers_requested": _positive_int(requested_max_workers, "requested_max_workers"),
        "outer_parallel_context": _outer_parallel_context(),
        "process_budget": _process_budget_metadata(_positive_int(requested_max_workers, "requested_max_workers")),
    }


def _resolve_effective_start_method(
    *,
    requested: str,
    available: tuple[str, ...],
    unsafe_fork_allowed: bool,
    parent_thread_count: int,
) -> str:
    if requested in {"auto_safe", "auto"}:
        if "forkserver" in available:
            return "forkserver"
        if "spawn" in available:
            return "spawn"
        if "fork" in available and unsafe_fork_allowed:
            return "fork"
        raise ResearchProcessRuntimeError(
            "research multiprocessing cannot choose a safe start method; "
            f"available methods: {', '.join(available) or '<none>'}; "
            "set BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK=1 only for diagnostic fork runs"
        )
    if requested not in available:
        raise ResearchProcessRuntimeError(
            f"research multiprocessing start method {requested!r} is not available; "
            f"available methods: {', '.join(available) or '<none>'}"
        )
    if requested == "fork" and not unsafe_fork_allowed:
        raise ResearchProcessRuntimeError(
            "research multiprocessing start method 'fork' is unsafe and requires "
            "BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK=1 for diagnostic use"
        )
    if requested == "fork" and parent_thread_count > 1 and not unsafe_fork_allowed:
        raise ResearchProcessRuntimeError(
            "research multiprocessing start method 'fork' from a multithreaded parent is unsafe"
        )
    return requested


def _resolve_worker_budget(requested_workers: int) -> tuple[int, dict[str, object]]:
    caps: list[int] = []
    env_cap_raw = os.environ.get("BITHUMB_RESEARCH_MAX_WORKERS")
    if env_cap_raw:
        caps.append(_positive_int(env_cap_raw, "BITHUMB_RESEARCH_MAX_WORKERS"))
    total_budget_raw = os.environ.get("BITHUMB_TOTAL_PROCESS_BUDGET")
    outer_worker_count = _outer_worker_count()
    if total_budget_raw:
        total_budget = _positive_int(total_budget_raw, "BITHUMB_TOTAL_PROCESS_BUDGET")
        if isinstance(outer_worker_count, int) and outer_worker_count > 0:
            caps.append(max(1, total_budget // outer_worker_count))
        else:
            caps.append(total_budget)
    effective = min([requested_workers, *caps]) if caps else requested_workers
    return effective, _process_budget_metadata(requested_workers, effective_workers=effective)


def _process_budget_metadata(
    requested_workers: int,
    *,
    effective_workers: int | None = None,
) -> dict[str, object]:
    outer_worker_count = _outer_worker_count()
    total_budget_raw = os.environ.get("BITHUMB_TOTAL_PROCESS_BUDGET")
    research_cap_raw = os.environ.get("BITHUMB_RESEARCH_MAX_WORKERS")
    return {
        "schema_version": 1,
        "outer_parallel_context": _outer_parallel_context(),
        "outer_worker_id": os.environ.get("PYTEST_XDIST_WORKER"),
        "outer_worker_count": outer_worker_count,
        "research_max_workers_requested": requested_workers,
        "research_max_workers_effective": effective_workers if effective_workers is not None else None,
        "research_max_workers_env_cap": int(research_cap_raw) if _is_positive_int(research_cap_raw) else None,
        "total_process_budget": int(total_budget_raw) if _is_positive_int(total_budget_raw) else None,
    }


def _normalize_requested_start_method(
    value: str,
    *,
    available_start_methods: tuple[str, ...] | None = None,
) -> str:
    normalized = str(value or "auto_safe").strip().lower()
    if normalized not in ALLOWED_RESEARCH_START_METHODS:
        available = (
            f"; available methods: {', '.join(available_start_methods) or '<none>'}"
            if available_start_methods is not None
            else ""
        )
        raise ResearchProcessRuntimeError(
            "research multiprocessing start method must be one of: "
            f"{', '.join(ALLOWED_RESEARCH_START_METHODS)}{available}"
        )
    return normalized


def _outer_parallel_context() -> str | None:
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return "pytest-xdist"
    return None


def _outer_worker_count() -> int | str | None:
    for name in ("PYTEST_XDIST_WORKER_COUNT", "PYTEST_XDIST_WORKERS"):
        value = os.environ.get(name)
        if _is_positive_int(value):
            return int(str(value))
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return "unknown"
    return None


def _parent_thread_count() -> int:
    return threading.active_count()


def _positive_int(value: object, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ResearchProcessRuntimeError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ResearchProcessRuntimeError(f"{name} must be a positive integer")
    return parsed


def _is_positive_int(value: object) -> bool:
    try:
        return int(str(value)) > 0
    except (TypeError, ValueError):
        return False
