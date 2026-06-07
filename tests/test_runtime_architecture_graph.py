from __future__ import annotations

import ast
from pathlib import Path


RUNTIME_SHELL_PATHS = (
    Path("src/bithumb_bot/runtime/runner.py"),
    Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
    Path("src/bithumb_bot/engine.py"),
)


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8-sig"))


def _import_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            modules.add("." * int(node.level) + str(node.module or ""))
    return modules


def _normalized_import_module(path: Path, node: ast.ImportFrom) -> str:
    if node.level == 0:
        return str(node.module or "")
    package = "bithumb_bot.runtime" if path.parts[-2:] and path.parts[-2] == "runtime" else "bithumb_bot"
    parts = package.split(".")
    if node.level > 1:
        parts = parts[: -(node.level - 1)]
    if node.module:
        parts.extend(str(node.module).split("."))
    return ".".join(part for part in parts if part)


def _imported_names(path: Path) -> dict[str, set[str]]:
    imports: dict[str, set[str]] = {}
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.ImportFrom):
            module = _normalized_import_module(path, node)
            imports.setdefault(module, set()).update(alias.name for alias in node.names)
    return imports


def test_runner_imports_only_runtime_shell_dependencies() -> None:
    modules = _import_modules(Path("src/bithumb_bot/runtime/runner.py"))
    forbidden = {
        "bithumb_bot.strategy_plugins",
        "bithumb_bot.research.strategy_spec",
        "bithumb_bot.broker.live",
    }
    assert modules.isdisjoint(forbidden)


def test_runner_contains_no_sql_or_db_execute_calls() -> None:
    path = Path("src/bithumb_bot/runtime/runner.py")
    source = path.read_text(encoding="utf-8-sig")
    assert "conn.execute(" not in source
    for sql in ('"SELECT ', '"UPDATE ', '"INSERT ', "'SELECT ", "'UPDATE ", "'INSERT "):
        assert sql not in source


def test_runtime_shell_does_not_import_strategy_plugins() -> None:
    for path in RUNTIME_SHELL_PATHS:
        modules = _import_modules(path)
        assert not any("strategy_plugins" in module for module in modules), path
        assert not any("research.strategy_spec" in module for module in modules), path


def test_submit_boundary_is_only_execution_service_or_execution_coordinator() -> None:
    forbidden_names = {
        "live_execute_signal",
        "paper_execute",
        "build_signal_execution_service",
        "record_harmless_dust_exit_suppression",
        "build_signal_execution_request",
    }
    allowlisted = {
        Path("src/bithumb_bot/runtime/execution_coordinator.py"),
        Path("src/bithumb_bot/execution_service.py"),
        Path("src/bithumb_bot/runtime/app_container.py"),
    }
    for path in (
        Path("src/bithumb_bot/runtime/runner.py"),
        Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
        Path("src/bithumb_bot/engine.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/run_loop_execution_planner.py"),
    ):
        if path in allowlisted:
            continue
        source = path.read_text(encoding="utf-8-sig")
        for name in forbidden_names:
            assert name not in source, path


def test_runtime_shell_does_not_import_execution_service_submit_symbols() -> None:
    forbidden = {
        "live_execute_signal",
        "paper_execute",
        "build_signal_execution_service",
        "record_harmless_dust_exit_suppression",
        "build_signal_execution_request",
    }
    for path in (
        Path("src/bithumb_bot/runtime/runner.py"),
        Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
    ):
        imports = _imported_names(path)
        assert "bithumb_bot.execution_service" not in imports, path
        assert forbidden.isdisjoint(imports.get("bithumb_bot.runtime.execution_coordinator", set())), path


def test_runtime_shell_contains_no_raw_sql_strings() -> None:
    for path in RUNTIME_SHELL_PATHS:
        source = path.read_text(encoding="utf-8-sig")
        assert "conn.execute(" not in source
        for token in ("SELECT ", "UPDATE ", "INSERT "):
            assert token not in source, path
