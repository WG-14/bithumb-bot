from __future__ import annotations

from pathlib import Path

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.resource_planner import detect_resource_contract, plan_research_resources
from tests.test_research_backtest_reproducibility import _manifest


def _mock_linux_limits(root: Path, *, cpu_quota: str = "800000 100000", memory_bytes: int = 12 * 1024 * 1024 * 1024) -> tuple[Path, Path]:
    cgroup = root / "sys" / "fs" / "cgroup"
    proc = root / "proc"
    cgroup.mkdir(parents=True)
    proc.mkdir()
    (cgroup / "cpu.max").write_text(cpu_quota, encoding="utf-8")
    (cgroup / "memory.max").write_text(str(memory_bytes), encoding="utf-8")
    (cgroup / "memory.swap.max").write_text("max", encoding="utf-8")
    (proc / "cpuinfo").write_text("".join(f"processor\t: {idx}\n" for idx in range(16)), encoding="utf-8")
    (proc / "meminfo").write_text("MemTotal:       16384000 kB\n", encoding="utf-8")
    return cgroup, proc


def _parallel_manifest(max_workers: int = 16):
    payload = _manifest()
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": max_workers}}
    return parse_manifest(payload)


def test_resource_planner_detects_mocked_wsl_cpu_and_memory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("BITHUMB_RESEARCH_MAX_WORKERS", raising=False)
    cgroup, proc = _mock_linux_limits(tmp_path)

    contract = detect_resource_contract(cgroup_root=cgroup, proc_root=proc)
    plan = plan_research_resources(
        manifest=_parallel_manifest(16),
        candidate_count=16,
        scenario_count=1,
        split_count=2,
        resource_contract=contract,
    )

    assert contract.cpu_limit == 8
    assert contract.memory_limit_mb == 12 * 1024
    assert plan.effective_max_workers == 8


def test_resource_planner_respects_env_worker_cap(tmp_path: Path, monkeypatch) -> None:
    cgroup, proc = _mock_linux_limits(tmp_path)
    monkeypatch.setenv("BITHUMB_RESEARCH_MAX_WORKERS", "4")

    contract = detect_resource_contract(cgroup_root=cgroup, proc_root=proc)
    plan = plan_research_resources(
        manifest=_parallel_manifest(8),
        candidate_count=16,
        scenario_count=1,
        split_count=2,
        resource_contract=contract,
    )

    assert contract.env_worker_cap == 4
    assert plan.effective_max_workers == 4


def test_resource_planner_records_fallback_when_cgroup_unavailable(tmp_path: Path, monkeypatch) -> None:
    proc = tmp_path / "proc"
    proc.mkdir()
    (proc / "cpuinfo").write_text("processor\t: 0\nprocessor\t: 1\n", encoding="utf-8")
    (proc / "meminfo").write_text("MemTotal:       2048000 kB\n", encoding="utf-8")
    monkeypatch.delenv("BITHUMB_RESEARCH_MAX_WORKERS", raising=False)

    contract = detect_resource_contract(cgroup_root=tmp_path / "missing", proc_root=proc)

    assert contract.cpu_limit == 2
    assert contract.detected_source
    assert "cgroup_cpu_limit_unavailable" in contract.fallback_reasons
    assert "cgroup_memory_limit_unavailable" in contract.fallback_reasons


def test_resource_plan_is_hash_stable_for_same_inputs(tmp_path: Path, monkeypatch) -> None:
    from bithumb_bot.research.hashing import sha256_prefixed

    cgroup, proc = _mock_linux_limits(tmp_path)
    monkeypatch.delenv("BITHUMB_RESEARCH_MAX_WORKERS", raising=False)
    contract = detect_resource_contract(cgroup_root=cgroup, proc_root=proc)
    kwargs = {
        "manifest": _parallel_manifest(16),
        "candidate_count": 16,
        "scenario_count": 1,
        "split_count": 2,
        "resource_contract": contract,
    }

    assert sha256_prefixed(plan_research_resources(**kwargs).as_dict()) == sha256_prefixed(
        plan_research_resources(**kwargs).as_dict()
    )
