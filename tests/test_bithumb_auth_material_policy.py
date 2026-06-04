from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pytest

from bithumb_bot.config_spec import (
    JWT_HS256_MIN_SECRET_BYTES,
    JWT_HS256_SECRET_VALIDATION_KIND,
    SPEC_BY_NAME,
)
from tests.support.live_auth import TEST_BITHUMB_API_SECRET


pytestmark = pytest.mark.fast_regression

TESTS_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class ShortSecretUse:
    path: str
    function: str
    context: str
    line: int
    byte_length: int
    value_hash_prefix: str


def _secret_hash_prefix(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


SHORT_SECRET_ALLOWLIST_CONTEXTS = {
    (
        "test_bithumb_private_api.py",
        "test_private_api_rejects_short_secret_before_jwt_signing",
        "BithumbPrivateAPI.api_secret",
    ): (1, _secret_hash_prefix("s")),
    (
        "test_live_preflight.py",
        "test_live_preflight_rejects_short_bithumb_api_secret",
        "settings.BITHUMB_API_SECRET",
    ): (24, _secret_hash_prefix("short-secret-lint-redact")),
}


def _call_name(node: ast.Call) -> str:
    parts: list[str] = []
    current: ast.AST = node.func
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _literal_arg(node: ast.Call, index: int, literal_bindings: dict[str, str]) -> str | None:
    if len(node.args) <= index:
        return None
    arg = node.args[index]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.Name):
        return literal_bindings.get(arg.id)
    return None


def _short_secret_literal(value: str) -> bool:
    return bool(value) and len(value.encode("utf-8")) < JWT_HS256_MIN_SECRET_BYTES


class _ShortBithumbSecretVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.function_stack: list[str] = ["<module>"]
        self.literal_binding_scopes: list[dict[str, str]] = [{}]
        self.uses: list[ShortSecretUse] = []

    @property
    def _function_name(self) -> str:
        return self.function_stack[-1]

    @property
    def _literal_bindings(self) -> dict[str, str]:
        return self.literal_binding_scopes[-1]

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.literal_binding_scopes.append({})
        self.generic_visit(node)
        self.literal_binding_scopes.pop()
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._literal_bindings[target.id] = node.value.value
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if (
            isinstance(node.target, ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            self._literal_bindings[node.target.id] = node.value.value
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        call_name = _call_name(node)
        candidates: list[tuple[str, str, int]] = []
        if call_name.endswith("BithumbPrivateAPI"):
            for keyword in node.keywords:
                if (
                    keyword.arg == "api_secret"
                    and isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ):
                    candidates.append(
                        ("BithumbPrivateAPI.api_secret", keyword.value.value, keyword.value.lineno)
                    )
        elif call_name.endswith("object.__setattr__"):
            key = _literal_arg(node, 1, self._literal_bindings)
            value = _literal_arg(node, 2, self._literal_bindings)
            if key == "BITHUMB_API_SECRET" and value is not None:
                candidates.append(("settings.BITHUMB_API_SECRET", value, node.lineno))
        elif call_name.endswith(".setenv"):
            key = _literal_arg(node, 0, self._literal_bindings)
            value = _literal_arg(node, 1, self._literal_bindings)
            if key == "BITHUMB_API_SECRET" and value is not None:
                candidates.append(("env.BITHUMB_API_SECRET", value, node.lineno))

        for context, value, line in candidates:
            if not _short_secret_literal(value):
                continue
            self.uses.append(
                ShortSecretUse(
                    path=str(self.path.relative_to(TESTS_ROOT)),
                    function=self._function_name,
                    context=context,
                    line=line,
                    byte_length=len(value.encode("utf-8")),
                    value_hash_prefix=_secret_hash_prefix(value),
                )
            )
        self.generic_visit(node)


def _short_secret_uses(path: Path) -> list[ShortSecretUse]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return _short_secret_uses_from_tree(path, tree)


def _short_secret_uses_from_tree(path: Path, tree: ast.AST) -> list[ShortSecretUse]:
    visitor = _ShortBithumbSecretVisitor(path)
    visitor.visit(tree)
    return visitor.uses


def _unauthorized_short_secret_uses(path: Path) -> list[ShortSecretUse]:
    offenders: list[ShortSecretUse] = []
    for use in _short_secret_uses(path):
        allow_key = (use.path, use.function, use.context)
        expected = SHORT_SECRET_ALLOWLIST_CONTEXTS.get(allow_key)
        observed = (use.byte_length, use.value_hash_prefix)
        if expected != observed:
            offenders.append(use)
    return offenders


def test_tests_do_not_use_short_bithumb_jwt_secrets() -> None:
    offenders: list[ShortSecretUse] = []
    for path in sorted(TESTS_ROOT.rglob("*.py")):
        offenders.extend(_unauthorized_short_secret_uses(path))

    rendered = [
        f"{use.path}:{use.line} function={use.function} context={use.context} "
        f"actual_bytes={use.byte_length}"
        for use in offenders
    ]
    assert rendered == []


def test_static_policy_detects_required_short_secret_contexts() -> None:
    tree = ast.parse(
        """
def test_probe(monkeypatch, settings):
    BithumbPrivateAPI(api_key="k", api_secret="secret", base_url="https://api.bithumb.com")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    monkeypatch.setenv("BITHUMB_API_SECRET", "s")
"""
    )

    uses = _short_secret_uses_from_tree(TESTS_ROOT / "_synthetic_policy_probe.py", tree)

    assert [(use.context, use.byte_length) for use in uses] == [
        ("BithumbPrivateAPI.api_secret", len("secret".encode("utf-8"))),
        ("settings.BITHUMB_API_SECRET", len("test-secret".encode("utf-8"))),
        ("env.BITHUMB_API_SECRET", len("s".encode("utf-8"))),
    ]


def test_shared_bithumb_test_secret_satisfies_hs256_policy() -> None:
    assert len(TEST_BITHUMB_API_SECRET.encode("utf-8")) >= JWT_HS256_MIN_SECRET_BYTES


def test_config_spec_declares_bithumb_jwt_secret_quality_policy() -> None:
    spec = SPEC_BY_NAME["BITHUMB_API_SECRET"]
    assert spec.secret is True
    assert spec.required_in_live is True
    assert spec.validation_kind == JWT_HS256_SECRET_VALIDATION_KIND
    assert spec.min_live_bytes == JWT_HS256_MIN_SECRET_BYTES
