from __future__ import annotations

from dataclasses import dataclass

import pytest

from bithumb_bot.research import executor
from bithumb_bot.research.execution_plan import ResearchWorkUnit
from bithumb_bot.research.executor import ResearchWorkResult, sort_work_results_deterministically


def _work_unit(index: int) -> ResearchWorkUnit:
    return ResearchWorkUnit(
        candidate_index=index,
        candidate_id=f"candidate_{index}",
        scenario_index=index % 3,
        scenario_id=f"scenario_{index % 3}",
        split_name="validation",
        parameter_values={"i": index},
        dataset_content_hash="sha256:dataset",
        portfolio_policy_hash="sha256:portfolio",
        simulation_policy_hash="sha256:simulation",
        execution_model_hash="sha256:execution",
        execution_timing_hash="sha256:timing",
        seed_context={"candidate_id": f"candidate_{index}"},
        work_unit_hash=f"sha256:work-{index}",
        work_result_input_hash=f"sha256:input-{index}",
    )


def _task(index: int) -> dict[str, object]:
    work_unit = _work_unit(index)
    return {
        "work_unit": work_unit,
        "candidate_index": index,
        "candidate_id": work_unit.candidate_id,
        "scenario_index": work_unit.scenario_index,
        "scenario_id": work_unit.scenario_id,
    }


def _worker(task: dict[str, object]) -> ResearchWorkResult:
    work_unit = task["work_unit"]
    assert isinstance(work_unit, ResearchWorkUnit)
    return ResearchWorkResult(
        work_unit=work_unit,
        work_unit_hash=work_unit.work_unit_hash,
        candidate_index=work_unit.candidate_index,
        candidate_id=work_unit.candidate_id,
        scenario_index=work_unit.scenario_index,
        scenario_id=work_unit.scenario_id,
        status="completed",
        base_result={
            "index": work_unit.candidate_index,
            "validation_closed_trades": [{"trade": work_unit.candidate_index}],
        },
    )


@dataclass
class _Runtime:
    max_workers_effective: int = 4

    def mp_context(self):  # type: ignore[no-untyped-def]
        return None


class _FakeFuture:
    def __init__(self, pool: "_FakePool", worker, task):  # type: ignore[no-untyped-def]
        self._pool = pool
        self._worker = worker
        self._task = task

    def result(self):  # type: ignore[no-untyped-def]
        type(self._pool).active -= 1
        return self._worker(self._task)


class _FakePool:
    active = 0
    peak_active = 0
    submit_count = 0

    def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs

    def __enter__(self):  # type: ignore[no-untyped-def]
        type(self).active = 0
        type(self).peak_active = 0
        type(self).submit_count = 0
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
        return False

    def submit(self, worker, task):  # type: ignore[no-untyped-def]
        type(self).submit_count += 1
        type(self).active += 1
        type(self).peak_active = max(type(self).peak_active, type(self).active)
        return _FakeFuture(self, worker, task)


def _fake_wait(futures, return_when=None):  # type: ignore[no-untyped-def]
    del return_when
    first = next(iter(futures))
    return {first}, set(futures) - {first}


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.resource_guard
def test_parallel_executor_never_exceeds_max_in_flight_tasks(monkeypatch) -> None:
    monkeypatch.setattr(executor, "ProcessPoolExecutor", _FakePool)
    monkeypatch.setattr(executor, "wait", _fake_wait)

    results = executor._execute_with_runtime(
        task_list=[_task(index) for index in range(100)],
        worker=_worker,
        initializer=None,
        initargs=(),
        runtime=_Runtime(),
        max_in_flight_tasks=8,
    )

    assert len(results) == 100
    assert _FakePool.submit_count == 100
    assert _FakePool.peak_active <= 8


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.resource_guard
def test_bounded_executor_preserves_deterministic_result_order_after_sort(monkeypatch) -> None:
    monkeypatch.setattr(executor, "ProcessPoolExecutor", _FakePool)
    monkeypatch.setattr(executor, "wait", _fake_wait)

    results = executor._execute_with_runtime(
        task_list=[_task(index) for index in reversed(range(12))],
        worker=_worker,
        initializer=None,
        initargs=(),
        runtime=_Runtime(),
        max_in_flight_tasks=3,
    )

    sorted_results = sort_work_results_deterministically(results)
    assert [(item.scenario_index, item.candidate_index) for item in sorted_results] == sorted(
        (item.scenario_index, item.candidate_index) for item in results
    )


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.resource_guard
def test_bounded_executor_handles_worker_exception_with_task_mapping(monkeypatch) -> None:
    monkeypatch.setattr(executor, "ProcessPoolExecutor", _FakePool)
    monkeypatch.setattr(executor, "wait", _fake_wait)

    def failing_worker(task):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    results = executor._execute_with_runtime(
        task_list=[_task(1)],
        worker=failing_worker,
        initializer=None,
        initargs=(),
        runtime=_Runtime(),
        max_in_flight_tasks=1,
    )

    assert results[0].status == "failed"
    assert results[0].failure_reason == "parallel_executor_exception"
    assert results[0].failure_evidence["candidate_id"] == "candidate_1"


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.resource_guard
def test_bounded_executor_streams_results_to_callback_without_retaining_base_result(monkeypatch) -> None:
    monkeypatch.setattr(executor, "ProcessPoolExecutor", _FakePool)
    monkeypatch.setattr(executor, "wait", _fake_wait)
    streamed: list[ResearchWorkResult] = []

    results = executor._execute_with_runtime(
        tasks=(_task(index) for index in range(20)),
        worker=_worker,
        initializer=None,
        initargs=(),
        runtime=_Runtime(),
        max_in_flight_tasks=4,
        result_callback=streamed.append,
    )

    assert results == []
    assert len(streamed) == 20
    assert all((result.base_result or {}).get("validation_closed_trades") for result in streamed)
    assert _FakePool.peak_active <= 4
