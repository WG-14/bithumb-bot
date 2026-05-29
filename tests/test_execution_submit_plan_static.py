from __future__ import annotations

import ast
from pathlib import Path

from bithumb_bot.execution_service import ExecutionSubmitPlan


def test_execution_submit_plan_dict_facade_not_used_in_runtime() -> None:
    assert not hasattr(ExecutionSubmitPlan, "__getitem__")
    assert not hasattr(ExecutionSubmitPlan, "get")
    assert not hasattr(ExecutionSubmitPlan, "__contains__")

    offenders: list[str] = []
    root = Path("src/bithumb_bot")
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute):
                if node.value.attr in {
                    "target_submit_plan",
                    "residual_submit_plan",
                    "buy_submit_plan",
                    "submit_plan",
                }:
                    offenders.append(f"{path}:{node.lineno}:{node.value.attr}")

    assert offenders == []
