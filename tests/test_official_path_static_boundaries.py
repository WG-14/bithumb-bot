from __future__ import annotations

import json
from pathlib import Path
import re
import ast

from bithumb_bot.research.strategy_registry import list_research_strategy_plugins


REPO = Path(__file__).resolve().parents[1]
ARCHITECTURE_BOUNDARY_MANIFEST = REPO / "docs" / "architecture-boundaries.json"

OFFICIAL_PATHS = (
    "src/bithumb_bot/approved_profile.py",
    "src/bithumb_bot/config.py",
    "src/bithumb_bot/evidence_chain.py",
    "src/bithumb_bot/engine.py",
    "src/bithumb_bot/execution_service.py",
    "src/bithumb_bot/profile_cli.py",
    "src/bithumb_bot/runtime_adapter_bootstrap.py",
    "src/bithumb_bot/runtime_strategy_decision.py",
    "src/bithumb_bot/runtime_strategy_set.py",
    "src/bithumb_bot/execution_service.py",
    "src/bithumb_bot/run_loop_execution_planner.py",
    "src/bithumb_bot/decision_envelope.py",
    "src/bithumb_bot/decision_equivalence.py",
    "src/bithumb_bot/broker/live.py",
    "src/bithumb_bot/research",
    "src/bithumb_bot/runtime_sma_snapshot.py",
    "src/bithumb_bot/runtime_sma_snapshot_builder.py",
)

FORBIDDEN_MARKERS = (
    "import backtest",
    "from backtest",
    "smoke_backtest",
    "diagnostic_smoke_backtest",
    "SmaCrossStrategy",
    "LegacySmaWithFilterDbAdapter",
    "bithumb_bot.strategy.registry",
    ".strategy.registry",
    "register_strategy(",
    "register_legacy_strategy(",
    "create_legacy_strategy",
    "create_strategy(",
    "build_execution_decision_summary(",
)

EXPLICIT_COMPATIBILITY_PATHS = {
    "src/bithumb_bot/strategy/registry.py",
}

STRATEGY_NAME_BRANCH_NAMES = frozenset(
    {
        "key",
        "name",
        "selected_strategy_name",
        "strategy",
        "strategy_key",
        "strategy_name",
    }
)

STRATEGY_NAME_BRANCH_ATTRIBUTES = frozenset(
    {
        "name",
        "selected_strategy_name",
        "strategy",
        "strategy_key",
        "strategy_name",
    }
)

STRATEGY_SPECIFIC_CORE_REASON = (
    "strategy-specific logic belongs in plugin/adapter/projector/contract code, "
    "not strategy-neutral runtime/decision core"
)


def _load_architecture_boundary_manifest() -> dict[str, object]:
    payload = json.loads(ARCHITECTURE_BOUNDARY_MANIFEST.read_text(encoding="utf-8"))
    if int(payload.get("schema_version") or 0) != 1:
        raise AssertionError("architecture boundary manifest schema_version must be 1")
    core_paths = payload.get("strategy_neutral_core_paths")
    if not isinstance(core_paths, list) or not all(isinstance(item, str) for item in core_paths):
        raise AssertionError("architecture manifest strategy_neutral_core_paths must be a list of strings")
    exceptions = payload.get("strategy_literal_exceptions", {})
    if not isinstance(exceptions, dict):
        raise AssertionError("architecture manifest strategy_literal_exceptions must be an object")
    for relative, literal_reasons in exceptions.items():
        if not isinstance(relative, str) or not isinstance(literal_reasons, dict):
            raise AssertionError("architecture manifest exceptions must map path to literal reason mapping")
        for literal, reason in literal_reasons.items():
            if not isinstance(literal, str) or not str(reason).strip():
                raise AssertionError("architecture manifest exception entries require concrete reasons")
    policy = payload.get("strategy_literal_exception_policy", {})
    if not isinstance(policy, dict) or policy.get("default") != "deny":
        raise AssertionError("architecture manifest must use default-deny strategy literal policy")
    return payload


def _strategy_neutral_core_paths() -> tuple[str, ...]:
    manifest = _load_architecture_boundary_manifest()
    return tuple(str(item) for item in manifest["strategy_neutral_core_paths"])


def _strategy_literal_exception_allowlist() -> dict[str, dict[str, str]]:
    manifest = _load_architecture_boundary_manifest()
    exceptions = manifest.get("strategy_literal_exceptions", {})
    return {
        str(relative): {str(literal): str(reason) for literal, reason in dict(literals).items()}
        for relative, literals in dict(exceptions).items()
    }


def _iter_official_files() -> list[Path]:
    files: list[Path] = []
    for relative in OFFICIAL_PATHS:
        path = REPO / relative
        if path.is_dir():
            files.extend(sorted(path.rglob("*.py")))
        else:
            files.append(path)
    return [
        path
        for path in files
        if path.exists()
        and path.relative_to(REPO).as_posix() not in EXPLICIT_COMPATIBILITY_PATHS
    ]


def _marker_allowed(relative: str, line: str, marker: str) -> bool:
    stripped = line.strip()
    if marker == "build_execution_decision_summary(":
        return (
            relative == "src/bithumb_bot/execution_service.py"
            and stripped.startswith("def build_execution_decision_summary(")
        )
    if marker == "create_strategy(":
        return relative.endswith("strategy_registry.py")
    return False


def _line_has_marker(line: str, marker: str) -> bool:
    if marker == "import backtest":
        return bool(re.search(r"\bimport\s+backtest\b", line))
    if marker == "from backtest":
        return bool(re.search(r"\bfrom\s+backtest\b", line))
    return marker in line


def _is_strategy_name_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id in STRATEGY_NAME_BRANCH_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in STRATEGY_NAME_BRANCH_ATTRIBUTES
    return False


def _registered_strategy_names() -> frozenset[str]:
    return frozenset(
        str(plugin.name or "").strip().lower()
        for plugin in list_research_strategy_plugins()
        if str(plugin.name or "").strip()
    )


def _strategy_literal_allowed(*, relative: str, literal: str) -> bool:
    return literal in _strategy_literal_exception_allowlist().get(relative, {})


def _forbidden_strategy_literal(node: ast.AST, forbidden_literals: frozenset[str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        if node.value in forbidden_literals:
            return node.value
    return None


def _literal_collection_values(node: ast.AST, forbidden_literals: frozenset[str]) -> tuple[str, ...]:
    if not isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        return ()
    values: list[str] = []
    for element in node.elts:
        value = _forbidden_strategy_literal(element, forbidden_literals)
        if value is not None:
            values.append(value)
    return tuple(values)


def _source_segment(source: str, node: ast.AST) -> str:
    segment = ast.get_source_segment(source, node)
    if segment is not None:
        return segment
    return ast.unparse(node)


def _strategy_literal_failures(
    path: Path,
    source: str,
    *,
    forbidden_literals: frozenset[str],
) -> list[str]:
    relative = path.relative_to(REPO).as_posix()
    failures: list[str] = []
    for line_no, line in enumerate(source.splitlines(), start=1):
        for literal in sorted(forbidden_literals):
            if literal in line and not _strategy_literal_allowed(relative=relative, literal=literal):
                failures.append(
                    f"{relative}:{line_no}: forbidden strategy literal {literal!r}; "
                    f"{STRATEGY_SPECIFIC_CORE_REASON}"
                )
    return failures


def _strategy_specific_branch_failures(
    path: Path,
    source: str,
    *,
    forbidden_literals: frozenset[str],
) -> list[str]:
    relative = path.relative_to(REPO).as_posix()
    tree = ast.parse(source, filename=relative)
    failures: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        comparisons = zip((node.left, *node.comparators[:-1]), node.ops, node.comparators)
        for left, op, right in comparisons:
            if isinstance(op, (ast.Eq, ast.NotEq)):
                left_literal = _forbidden_strategy_literal(left, forbidden_literals)
                right_literal = _forbidden_strategy_literal(right, forbidden_literals)
                if _is_strategy_name_expression(left) and right_literal is not None:
                    failures.append(
                        f"{relative}:{node.lineno}: forbidden strategy-specific branch "
                        f"{_source_segment(source, node)!r} references {right_literal!r}; "
                        f"{STRATEGY_SPECIFIC_CORE_REASON}"
                    )
                if left_literal is not None and _is_strategy_name_expression(right):
                    failures.append(
                        f"{relative}:{node.lineno}: forbidden strategy-specific branch "
                        f"{_source_segment(source, node)!r} references {left_literal!r}; "
                        f"{STRATEGY_SPECIFIC_CORE_REASON}"
                    )
            if isinstance(op, (ast.In, ast.NotIn)) and _is_strategy_name_expression(left):
                literals = _literal_collection_values(right, forbidden_literals)
                if literals:
                    failures.append(
                        f"{relative}:{node.lineno}: forbidden strategy-specific membership branch "
                        f"{_source_segment(source, node)!r} references {', '.join(repr(v) for v in literals)}; "
                        f"{STRATEGY_SPECIFIC_CORE_REASON}"
                    )
    return failures


def test_strategy_neutral_core_files_do_not_contain_strategy_specific_special_cases() -> None:
    forbidden_literals = _registered_strategy_names()
    assert forbidden_literals
    failures: list[str] = []
    for relative in _strategy_neutral_core_paths():
        path = REPO / relative
        source = path.read_text(encoding="utf-8-sig")
        failures.extend(
            _strategy_literal_failures(path, source, forbidden_literals=forbidden_literals)
        )
        failures.extend(
            _strategy_specific_branch_failures(
                path,
                source,
                forbidden_literals=forbidden_literals,
            )
        )

    assert failures == []


def test_architecture_boundary_manifest_defines_strategy_neutral_core() -> None:
    manifest = _load_architecture_boundary_manifest()
    paths = _strategy_neutral_core_paths()

    assert ARCHITECTURE_BOUNDARY_MANIFEST.exists()
    assert "src/bithumb_bot/config.py" in paths
    assert "src/bithumb_bot/runtime_risk_engine.py" in paths
    assert manifest["strategy_literal_exception_policy"]["default"] == "deny"
    for relative in paths:
        assert (REPO / relative).exists(), relative


def test_strategy_name_guard_uses_registered_plugin_inventory_for_literals() -> None:
    literal = sorted(_registered_strategy_names())[0]
    failures = _strategy_literal_failures(
        REPO / "src/bithumb_bot/config.py",
        f'STRATEGY_NAME = "{literal}"\n',
        forbidden_literals=frozenset({literal}),
    )

    assert failures
    assert literal in failures[0]


def test_strategy_name_guard_uses_registered_plugin_inventory_for_branches() -> None:
    literal = sorted(_registered_strategy_names())[0]
    source = f'if strategy_name == "{literal}":\n    pass\n'
    failures = _strategy_specific_branch_failures(
        REPO / "src/bithumb_bot/config.py",
        source,
        forbidden_literals=frozenset({literal}),
    )

    assert failures
    assert "forbidden strategy-specific branch" in failures[0]
    assert literal in failures[0]


def test_official_paths_do_not_cross_smoke_or_legacy_authority_boundaries() -> None:
    failures: list[str] = []
    for path in _iter_official_files():
        relative = path.relative_to(REPO).as_posix()
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for marker in FORBIDDEN_MARKERS:
                if _line_has_marker(line, marker) and not _marker_allowed(relative, line, marker):
                    failures.append(f"{relative}:{line_no}: forbidden marker {marker!r}")

    assert failures == []


def test_legacy_sma_db_bound_implementation_is_owned_by_compat_namespace() -> None:
    compat_source = (REPO / "src/bithumb_bot/compat/sma_legacy_adapter.py").read_text(encoding="utf-8")
    strategy_shim_source = (REPO / "src/bithumb_bot/strategy/sma_legacy_adapter.py").read_text(
        encoding="utf-8"
    )
    strategy_sma_source = (REPO / "src/bithumb_bot/strategy/sma.py").read_text(encoding="utf-8")

    for marker in (
        "class SmaCrossStrategy",
        "class LegacySmaWithFilterDbAdapter",
        "def create_sma_strategy",
        "def create_legacy_sma_with_filter_db_adapter",
        "LEGACY_DB_BOUND_STRATEGY_STATUS",
    ):
        assert marker in compat_source
    for marker in (
        "class SmaCrossStrategy",
        "class LegacySmaWithFilterDbAdapter",
        "def create_sma_strategy",
        "def create_legacy_sma_with_filter_db_adapter",
    ):
        assert marker not in strategy_shim_source
        assert marker not in strategy_sma_source
    assert "from bithumb_bot.compat.sma_legacy_adapter import" in strategy_shim_source
    assert "__all__" in strategy_sma_source
    assert "SmaCrossStrategy" not in strategy_sma_source
    assert "LegacySmaWithFilterDbAdapter" not in strategy_sma_source


def test_runtime_source_does_not_import_strategy_sma_legacy_adapter() -> None:
    violations: list[str] = []
    for path in (REPO / "src/bithumb_bot").rglob("*.py"):
        relative = path.relative_to(REPO).as_posix()
        if relative == "src/bithumb_bot/strategy/sma_legacy_adapter.py":
            continue
        source = path.read_text(encoding="utf-8-sig")
        if "bithumb_bot.strategy.sma_legacy_adapter" in source:
            violations.append(relative)
        if "from .sma_legacy_adapter" in source or "from ..strategy.sma_legacy_adapter" in source:
            violations.append(relative)

    assert violations == []


def test_promotion_runtime_adapters_use_strategy_decision_service_boundary() -> None:
    adapter_paths = (
        REPO / "src/bithumb_bot/runtime_adapters/sma_with_filter.py",
        REPO / "src/bithumb_bot/runtime_adapters/safe_hold.py",
        REPO / "src/bithumb_bot/strategy_plugins/canary_non_sma.py",
    )
    failures: list[str] = []
    for path in adapter_paths:
        source = path.read_text(encoding="utf-8-sig")
        if path.name == "sma_with_filter.py" and "compute_strategy_decision_after_normalization" in source:
            continue
        if "StrategyDecisionService" not in source or "StrategyEvaluationRequest" not in source:
            failures.append(path.relative_to(REPO).as_posix())
    assert failures == []


def test_promotion_runtime_adapters_do_not_construct_strategy_decision_v2_outside_policy_classes() -> None:
    allowed_class_suffix = ("Policy", "Strategy")
    failures: list[str] = []
    for path in (REPO / "src/bithumb_bot").rglob("*.py"):
        relative = path.relative_to(REPO).as_posix()
        if relative.startswith("src/bithumb_bot/compat/") or "/tests/" in relative:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=relative)
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            if name != "StrategyDecisionV2":
                continue
            owner = parents.get(node)
            while owner is not None and not isinstance(owner, ast.ClassDef):
                owner = parents.get(owner)
            if isinstance(owner, ast.ClassDef) and owner.name.endswith(allowed_class_suffix):
                continue
            if relative == "src/bithumb_bot/core/sma_policy.py":
                continue
            failures.append(f"{relative}:{node.lineno}")
    assert failures == []


def test_engine_is_thin_runtime_entrypoint() -> None:
    source = (REPO / "src/bithumb_bot/engine.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]

    assert len(functions) <= 10
    assert "operator_next_action" not in source
    assert "operator_hint_command" not in source
    assert "cancel_open_orders_with_broker" not in source
    assert "flatten_position" not in source
    assert "LIVE_EXECUTION_BROKER_ERROR" not in source
    assert "STARTUP_SAFETY_GATE" not in source


def test_runtime_recovery_controller_evaluate_phase_has_no_mutation_or_delivery_calls() -> None:
    source = (REPO / "src/bithumb_bot/runtime/recovery_controller.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    evaluate_nodes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "evaluate_clearance"
    ]
    assert len(evaluate_nodes) == 1
    evaluate_source = ast.get_source_segment(source, evaluate_nodes[0]) or ""
    forbidden = (
        "disable_trading_until",
        "enter_halt",
        "set_resume_gate",
        "notify",
        "send_event",
        "send_message",
        "cancel_open_orders",
        "flatten",
        "get_balance",
    )
    assert all(marker not in evaluate_source for marker in forbidden)


def test_notification_composer_has_no_runtime_state_side_effects() -> None:
    source = (REPO / "src/bithumb_bot/runtime/operator_event_composer.py").read_text(encoding="utf-8")
    assert "runtime_state" not in source
    assert "notify" not in source
    assert "send_message" not in source
    assert "send_event" not in source


def test_runtime_runner_delegates_safety_recovery_and_execution_boundaries() -> None:
    source = (REPO / "src/bithumb_bot/runtime/runner.py").read_text(encoding="utf-8-sig")
    app_container_source = (REPO / "src/bithumb_bot/runtime/app_container.py").read_text(encoding="utf-8")

    assert "RecoveryController(" in source
    assert "SafetyController(" in source
    assert "StartupController(" in source
    assert "ExecutionCoordinator(" in app_container_source
    assert "cancel_open_orders_with_broker(broker)" not in source
    assert "flatten_status = str(flatten_outcome.get" not in source
