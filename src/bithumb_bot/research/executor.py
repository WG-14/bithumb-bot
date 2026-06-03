from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from .execution_plan import ResearchWorkUnit
from .hashing import sha256_prefixed


@dataclass(frozen=True)
class ResearchWorkResult:
    work_unit: ResearchWorkUnit
    work_unit_hash: str
    candidate_index: int
    candidate_id: str
    scenario_index: int
    scenario_id: str
    status: str
    base_result: dict[str, Any] | None = None
    failure_reason: str | None = None
    failure_evidence: dict[str, Any] | None = None
    observability: dict[str, Any] | None = None
    content_hash: str | None = None

    def __post_init__(self) -> None:
        if self.content_hash is not None:
            return
        object.__setattr__(
            self,
            "content_hash",
            sha256_prefixed(
                {
                    "work_unit_hash": self.work_unit_hash,
                    "work_result_input_hash": self.work_unit.work_result_input_hash,
                    "candidate_index": self.candidate_index,
                    "candidate_id": self.candidate_id,
                    "scenario_index": self.scenario_index,
                    "scenario_id": self.scenario_id,
                    "status": self.status,
                    "failure_reason": self.failure_reason,
                    "failure_evidence_hash": (
                        sha256_prefixed(self.failure_evidence) if self.failure_evidence is not None else None
                    ),
                }
            ),
        )

    def observability_payload(self) -> dict[str, Any]:
        payload = dict(self.observability or {})
        payload.setdefault("work_unit", self.work_unit.as_dict())
        payload.setdefault("status", self.status)
        payload.setdefault("worker_process_evidence", _worker_process_evidence(self))
        if self.failure_reason is not None:
            payload.setdefault("failure_reason", self.failure_reason)
        if self.failure_evidence is not None:
            payload.setdefault("resource_guard", self.failure_evidence)
        payload.setdefault("content_hash", self.content_hash)
        return payload


ResearchWorker = Callable[[Any], ResearchWorkResult]


def _worker_process_evidence(result: ResearchWorkResult) -> dict[str, Any]:
    observability = dict(result.observability or {})
    work_unit = result.work_unit.as_dict()
    worker_pid = observability.get("worker_pid")
    input_hash = result.work_unit.work_result_input_hash or result.work_unit_hash
    output_hash = result.content_hash
    exit_status = 0 if result.status == "completed" else 1
    resource_guard = result.failure_evidence if result.failure_evidence is not None else {}
    return {
        "schema_version": 1,
        "worker_pid": worker_pid,
        "callable_identity": "bithumb_bot.research.validation_protocol._candidate_scenario_worker",
        "command_or_callable_identity": "bithumb_bot.research.validation_protocol._candidate_scenario_worker",
        "input_hash": input_hash,
        "output_hash": output_hash,
        "exit_status": exit_status,
        "status": result.status,
        "timeout_status": resource_guard.get("timeout_status", "not_reported"),
        "resource_status": resource_guard.get("status", result.status),
        "terminal_audit_trace_status": (
            "present"
            if any(str(key).endswith("audit_trace_index") for key in work_unit)
            else "not_applicable"
        ),
        "work_unit_hash": result.work_unit_hash,
        "work_result_input_hash": result.work_unit.work_result_input_hash,
    }


def execute_research_work_units_serial(
    *,
    tasks: Iterable[Any],
    worker: ResearchWorker,
) -> list[ResearchWorkResult]:
    return [worker(task) for task in tasks]


def execute_research_work_units_parallel(
    *,
    tasks: Iterable[Any],
    worker: ResearchWorker,
    max_workers: int,
    initializer: Callable[..., None] | None = None,
    initargs: tuple[Any, ...] = (),
) -> list[ResearchWorkResult]:
    results: list[ResearchWorkResult] = []
    task_list = list(tasks)
    with ProcessPoolExecutor(max_workers=int(max_workers), initializer=initializer, initargs=initargs) as pool:
        future_to_task = {pool.submit(worker, task): task for task in task_list}
        for completion_order, future in enumerate(as_completed(future_to_task)):
            task = future_to_task[future]
            try:
                result = future.result()
            except Exception as exc:
                result = _future_exception_result(task=task, exc=exc)
            observability = dict(result.observability or {})
            observability["completion_order"] = completion_order
            results.append(
                ResearchWorkResult(
                    work_unit=result.work_unit,
                    work_unit_hash=result.work_unit_hash,
                    candidate_index=result.candidate_index,
                    candidate_id=result.candidate_id,
                    scenario_index=result.scenario_index,
                    scenario_id=result.scenario_id,
                    status=result.status,
                    base_result=result.base_result,
                    failure_reason=result.failure_reason,
                    failure_evidence=result.failure_evidence,
                    observability=observability,
                    content_hash=result.content_hash,
                )
            )
    return results


def _future_exception_result(*, task: Any, exc: Exception) -> ResearchWorkResult:
    work_unit = _task_work_unit(task)
    candidate_index = int(_task_value(task, "candidate_index", work_unit.candidate_index))
    scenario_index = int(_task_value(task, "scenario_index", work_unit.scenario_index))
    evidence = {
        "status": "ERROR",
        "exception_type": type(exc).__name__,
        "message": str(exc),
        "phase": "future_result",
        "candidate_index": candidate_index,
        "candidate_id": str(_task_value(task, "candidate_id", work_unit.candidate_id)),
        "scenario_index": scenario_index,
        "scenario_id": str(_task_value(task, "scenario_id", work_unit.scenario_id)),
        "work_unit_hash": work_unit.work_unit_hash,
    }
    return ResearchWorkResult(
        work_unit=work_unit,
        work_unit_hash=work_unit.work_unit_hash,
        candidate_index=candidate_index,
        candidate_id=str(_task_value(task, "candidate_id", work_unit.candidate_id)),
        scenario_index=scenario_index,
        scenario_id=str(_task_value(task, "scenario_id", work_unit.scenario_id)),
        status="failed",
        failure_reason="parallel_executor_exception",
        failure_evidence=evidence,
        observability={
            "work_unit": work_unit.as_dict(),
            "status": "failed",
            "failure_reason": "parallel_executor_exception",
            "resource_guard": evidence,
        },
    )


def _task_work_unit(task: Any) -> ResearchWorkUnit:
    if isinstance(task, dict) and isinstance(task.get("work_unit"), ResearchWorkUnit):
        return task["work_unit"]
    raise TypeError("parallel task failure cannot be mapped without ResearchWorkUnit")


def _task_value(task: Any, key: str, default: Any) -> Any:
    if isinstance(task, dict):
        return task.get(key, default)
    return default


def sort_work_results_deterministically(results: Iterable[ResearchWorkResult]) -> list[ResearchWorkResult]:
    return sorted(
        results,
        key=lambda result: (
            int(result.scenario_index),
            int(result.candidate_index),
            str(result.work_unit.split_name),
        ),
    )


def canonical_work_results_payload(results: Iterable[ResearchWorkResult]) -> list[dict[str, Any]]:
    return [
        {
            "work_unit_hash": result.work_unit_hash,
            "work_result_input_hash": result.work_unit.work_result_input_hash,
            "candidate_index": int(result.candidate_index),
            "candidate_id": result.candidate_id,
            "scenario_index": int(result.scenario_index),
            "scenario_id": result.scenario_id,
            "split_name": result.work_unit.split_name,
            "status": result.status,
            "failure_reason": result.failure_reason,
            "failure_evidence_hash": (
                sha256_prefixed(result.failure_evidence) if result.failure_evidence is not None else None
            ),
            "content_hash": result.content_hash,
        }
        for result in sort_work_results_deterministically(results)
    ]


def canonical_work_results_content_hash(results: Iterable[ResearchWorkResult]) -> str:
    return sha256_prefixed(canonical_work_results_payload(results))
