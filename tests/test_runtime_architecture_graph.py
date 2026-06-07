from __future__ import annotations

import ast
from pathlib import Path


RUNTIME_SHELL_PATHS = (
    Path("src/bithumb_bot/runtime/runner.py"),
    Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
    Path("src/bithumb_bot/engine.py"),
)

RUNTIME_SUBMIT_BOUNDARY_NAMES = {
    "live_execute_signal",
    "paper_execute",
    "build_signal_execution_service",
    "record_harmless_dust_exit_suppression",
    "build_signal_execution_request",
}


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


def _module_level_getattr_functions(path: Path) -> list[ast.FunctionDef]:
    tree = _tree(path)
    return [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "__getattr__"
    ]


def _calls_getattr_on_execution_coordinator(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    if not isinstance(node.func, ast.Name) or node.func.id != "getattr":
        return False
    if not node.args:
        return False
    target = node.args[0]
    return isinstance(target, ast.Name) and target.id == "_execution_coordinator"


def _returns_submit_boundary_symbol(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id in RUNTIME_SUBMIT_BOUNDARY_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in RUNTIME_SUBMIT_BOUNDARY_NAMES
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return any(name in node.value for name in RUNTIME_SUBMIT_BOUNDARY_NAMES)
    if _calls_getattr_on_execution_coordinator(node):
        return True
    return any(_returns_submit_boundary_symbol(child) for child in ast.iter_child_nodes(node))


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
        for name in RUNTIME_SUBMIT_BOUNDARY_NAMES:
            assert name not in source, path


def test_runtime_shell_does_not_import_execution_service_submit_symbols() -> None:
    for path in (
        Path("src/bithumb_bot/runtime/runner.py"),
        Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
    ):
        imports = _imported_names(path)
        assert "bithumb_bot.execution_service" not in imports, path
        assert RUNTIME_SUBMIT_BOUNDARY_NAMES.isdisjoint(
            imports.get("bithumb_bot.runtime.execution_coordinator", set())
        ), path


def test_runtime_shell_does_not_expose_dynamic_submit_builder_alias() -> None:
    shell_paths = (
        Path("src/bithumb_bot/runtime/runner.py"),
        Path("src/bithumb_bot/runtime/cycle_pipeline.py"),
    )
    forbidden_source_tokens = {
        "build_signal_execution_",
        "compat_request_builder",
        "getattr(_execution_coordinator",
    }
    forbidden_import_modules = {
        "bithumb_bot.execution_service",
    }
    for path in shell_paths:
        source = path.read_text(encoding="utf-8-sig")
        for token in forbidden_source_tokens:
            assert token not in source, path

        imports = _imported_names(path)
        assert forbidden_import_modules.isdisjoint(imports), path
        assert RUNTIME_SUBMIT_BOUNDARY_NAMES.isdisjoint(
            imports.get("bithumb_bot.runtime.execution_coordinator", set())
        ), path

        for function in _module_level_getattr_functions(path):
            returns = [
                node.value
                for node in ast.walk(function)
                if isinstance(node, ast.Return) and node.value is not None
            ]
            assert not any(_returns_submit_boundary_symbol(node) for node in returns), path


def test_runtime_shell_contains_no_raw_sql_strings() -> None:
    for path in RUNTIME_SHELL_PATHS:
        source = path.read_text(encoding="utf-8-sig")
        assert "conn.execute(" not in source
        for token in ("SELECT ", "UPDATE ", "INSERT "):
            assert token not in source, path
