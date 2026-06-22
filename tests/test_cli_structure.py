from __future__ import annotations

import ast
import argparse
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot import config
from bithumb_bot.cli.context import AppContext
from bithumb_bot.cli.dispatch import dispatch
from bithumb_bot.cli.parser import build_parser
from bithumb_bot.cli.registry import CommandSpec, command_registry
from bithumb_bot import runtime_strategy_set


EXPECTED_COMMANDS = {
    "ticker",
    "candles",
    "sync",
    "sync-orderbook-top",
    "backfill-candles",
    "signal",
    "explain",
    "status",
    "health",
    "audit",
    "check",
    "audit-ledger",
    "validate-db",
    "config-dump",
    "notification-diagnose",
    "orders",
    "fills",
    "trades",
    "run",
    "live-dry-run",
    "pause",
    "resume",
    "reconcile",
    "broker-diagnose",
    "target-delta-dry-run",
    "smoke-buy",
    "live-pipeline-smoke",
    "live-pipeline-smoke-authority",
    "h74-live-rehearsal",
    "h74-readiness-certificate",
    "h74-long-run-preflight",
    "exchange-submit-diagnose",
    "panic-stop",
    "flatten-position",
    "cancel-open-orders",
    "target-closeout",
    "recovery-report",
    "repair-plan",
    "restart-checklist",
    "residual-closeout-plan",
    "diagnose-fill-trade-linkage",
    "recover-order",
    "backfill-broker-order",
    "fee-gap-accounting-repair",
    "fee-pending-accounting-repair",
    "rebuild-position-authority",
    "record-external-cash-adjustment",
    "manual-flat-accounting-repair",
    "external-position-accounting-repair",
    "report",
    "ops-report",
    "risk-report",
    "fee-diagnostics",
    "strategy-report",
    "h74-observation-report",
    "strategy-plugin-inventory",
    "strategy-plugin-validate",
    "experiment-report",
    "cash-drift-report",
    "decision-telemetry",
    "decision-attribution",
    "execution-quality-report",
    "research-backtest",
    "research-batch",
    "research-workload-estimate",
    "research-verify-audit",
    "research-validate",
    "research-freeze-dataset",
    "research-readiness",
    "research-forward-diagnostics",
    "research-walk-forward",
    "research-promote-candidate",
    "research-reproduce",
    "research-registry-inspect",
    "research-registry-validate",
    "research-mark-attempt-aborted",
    "research-export-decisions",
    "runtime-replay-decisions",
    "runtime-strategy-set-dump",
    "runtime-strategy-set-lint",
    "risk-layer-replay",
    "replay-decision",
    "promotion-provenance-verify",
    "promotion-verify",
    "h74-observation-authority-generate",
    "h74-observation-authority-verify",
    "h74-source-observation-authority-generate",
    "h74-source-observation-authority-verify",
    "decision-equivalence",
    "candidate-regime-policy-equivalence-evidence",
    "profile-generate",
    "profile-diff",
    "profile-verify",
    "profile-promote",
    "evidence-bundle-create",
    "evidence-bundle-verify",
    "research-missing-candles",
    "retry-missing-candles",
    "probe-missing-candles",
    "classify-persistent-missing-candles",
    "find-clean-candle-segments",
    "strategy-sweep",
}


def test_cli_help_builds_from_registry(capsys: pytest.CaptureFixture[str]) -> None:
    sys.modules.pop("bithumb_bot.app_impl", None)
    parser = build_parser(command_registry())

    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "bithumb-bot" in output
    assert "recovery-report" in output
    assert "strategy-sweep" in output
    assert "bithumb_bot.app_impl" not in sys.modules


def test_command_registration_contains_expected_major_groups() -> None:
    registry = command_registry()

    assert set(registry) == EXPECTED_COMMANDS
    assert len(registry) == len(set(registry))
    assert {
        "marketdata",
        "runtime",
        "live_ops",
        "recovery",
        "repairs",
        "reports",
        "research",
        "profile",
        "strategy",
        "data_plane",
    } <= {spec.domain for spec in registry.values()}
    assert registry["run"].guard_policy == "live_run_loop"
    assert registry["live-dry-run"].guard_policy == "live_dry_run_loop"
    assert registry["smoke-buy"].guard_policy == "operator_execution_smoke"
    assert registry["live-pipeline-smoke"].guard_policy == "operator_live_pipeline_smoke"
    assert registry["live-pipeline-smoke-authority"].guard_policy == "operator_live_pipeline_smoke_authority"
    assert registry["panic-stop"].guard_policy == "operator_risk_reduction"
    assert registry["flatten-position"].guard_policy == "operator_risk_reduction"
    assert registry["cancel-open-orders"].guard_policy == "operator_risk_reduction"
    assert registry["target-closeout"].guard_policy == "operator_risk_reduction"
    assert registry["broker-diagnose"].guard_policy == "read_only_broker_diagnostic"
    assert registry["recover-order"].guard_policy == "operator_recovery"
    assert registry["run"].mutating is True
    assert registry["run"].uses_broker is True
    assert registry["recover-order"].requires_confirmation is True
    assert registry["fee-gap-accounting-repair"].writes_db is True


def test_h74_long_run_preflight_registered() -> None:
    registry = command_registry()

    spec = registry["h74-long-run-preflight"]
    assert spec.domain == "live_ops"
    assert spec.read_only is True
    assert spec.requires_live is True
    assert spec.json_output_supported is True


@pytest.mark.parametrize(
    ("command", "options"),
    [
        ("strategy-sweep", ["--short", "--long", "--edge-buffer", "--min-expected-edge", "--slippage-bps", "--json"]),
        ("strategy-plugin-validate", ["--strategy", "--target", "--json"]),
        ("recover-order", ["--client-order-id", "--exchange-order-id", "--dry-run", "--yes"]),
        ("fee-gap-accounting-repair", ["--apply", "--yes", "--note"]),
        ("recovery-report", ["--json"]),
        ("research-backtest", ["--manifest", "--execution-calibration"]),
        ("notification-diagnose", ["--json", "--probe", "--notification-policy"]),
        ("research-forward-diagnostics", ["--manifest", "--split", "--features", "--horizons", "--bucket", "--entry-price", "--min-bucket-count", "--out", "--json"]),
        ("profile-promote", ["--profile", "--mode", "--out", "--paper-validation-evidence", "--live-readiness-evidence"]),
        ("backfill-candles", ["--market", "--interval", "--start", "--end", "--batch-size", "--dry-run"]),
        ("flatten-position", ["--dry-run", "--json"]),
        ("live-pipeline-smoke", ["--plan", "--apply", "--yes", "--cycles", "--max-orders", "--max-notional-krw", "--authority-path", "--confirm", "--json"]),
        ("live-pipeline-smoke-authority", ["--out", "--cycles", "--max-orders", "--max-notional-krw", "--expires-min"]),
        ("h74-long-run-preflight", ["--certificate", "--json"]),
    ],
)
def test_important_command_help_exposes_owned_options(
    command: str,
    options: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser(command_registry())

    with pytest.raises(SystemExit) as exc:
        parser.parse_args([command, "--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    for option in options:
        assert option in output


def test_selected_commands_parse_with_real_options() -> None:
    parser = build_parser(command_registry())

    args = parser.parse_args(
        [
            "strategy-sweep",
            "--short",
            "5,7",
            "--long",
            "20",
            "--edge-buffer",
            "0.01",
            "--min-expected-edge",
            "0.02",
            "--slippage-bps",
            "5",
            "--json",
        ]
    )

    assert args.cmd == "strategy-sweep"
    assert args.short == (5, 7)
    assert args.json is True


def test_flatten_position_supports_json_dry_run() -> None:
    registry = command_registry()
    parser = build_parser(registry)

    args = parser.parse_args(["flatten-position", "--dry-run", "--json"])

    assert args.cmd == "flatten-position"
    assert args.dry_run is True
    assert args.json is True
    assert registry["flatten-position"].json_output_supported is True


def test_dispatch_uses_spec_handler_with_context() -> None:
    calls: list[tuple[str, list[str]]] = []

    def _handler(args: argparse.Namespace, context: AppContext) -> int:
        calls.append((args.cmd, list(context.argv or [])))
        return 7

    spec = CommandSpec(
        name="fake",
        domain="runtime",
        handler=_handler,
        register_parser=lambda subparsers: subparsers.add_parser("fake"),
    )

    rc = dispatch(
        argparse.Namespace(cmd="fake"),
        AppContext(argv=["fake", "--flag"]),
        {"fake": spec},
    )

    assert rc == 7
    assert calls == [("fake", ["fake", "--flag"])]


def test_live_guard_policy_is_metadata_driven(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    handler_called = False

    def _handler(_args: argparse.Namespace, _context: AppContext) -> int:
        nonlocal handler_called
        handler_called = True
        return 0

    spec = CommandSpec(
        name="guarded",
        domain="runtime",
        handler=_handler,
        register_parser=lambda subparsers: subparsers.add_parser("guarded"),
        guard_policy="live_preflight",
    )
    monkeypatch.setattr(
        "bithumb_bot.config.validate_live_mode_preflight",
        lambda _settings: calls.append("preflight"),
    )

    rc = dispatch(
        argparse.Namespace(cmd="guarded"),
        AppContext(settings=SimpleNamespace(MODE="live")),
        {"guarded": spec},
    )

    assert rc == 0
    assert calls == ["preflight"]
    assert handler_called is True


@pytest.mark.parametrize(
    ("command", "policy"),
    [
        ("smoke-buy", "operator_execution_smoke"),
        ("live-pipeline-smoke", "operator_live_pipeline_smoke"),
        ("live-pipeline-smoke-authority", "operator_live_pipeline_smoke_authority"),
    ],
)
def test_operator_smoke_guard_does_not_call_strategy_preflight(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    policy: str,
) -> None:
    calls: list[str] = []

    for validator_name in (
        "validate_live_mode_preflight",
        "validate_live_strategy_selection",
        "validate_runtime_strategy_set_selection",
        "validate_runtime_profile_bindings_for_live_startup",
    ):
        monkeypatch.setattr(
            f"bithumb_bot.config.{validator_name}",
            lambda *_args, _name=validator_name, **_kwargs: calls.append(_name),
        )
    monkeypatch.setattr(
        "bithumb_bot.operator_smoke_preflight.validate_operator_smoke_cli_guard",
        lambda _settings: calls.append("operator_smoke"),
    )

    spec = CommandSpec(
        name=command,
        domain="live_ops",
        handler=lambda _args, _context: 0,
        register_parser=lambda subparsers: subparsers.add_parser(command),
        guard_policy=policy,
    )

    rc = dispatch(
        argparse.Namespace(cmd=command),
        AppContext(settings=SimpleNamespace(MODE="live")),
        {command: spec},
    )

    assert rc == 0
    assert calls == ["operator_smoke"]


def test_live_ops_commands_have_intent_specific_guard_policies() -> None:
    registry = command_registry()

    assert registry["run"].guard_policy == "live_run_loop"
    assert registry["live-dry-run"].guard_policy == "live_dry_run_loop"
    assert registry["reconcile"].guard_policy == "operator_recovery"
    assert registry["smoke-buy"].guard_policy == "operator_execution_smoke"
    assert registry["flatten-position"].guard_policy == "operator_risk_reduction"
    assert registry["broker-diagnose"].guard_policy == "read_only_broker_diagnostic"
    assert registry["recover-order"].guard_policy == "operator_recovery"
    assert registry["rebuild-position-authority"].guard_policy == "operator_recovery"


def test_rebuild_position_authority_json_flag_matches_command_spec() -> None:
    registry = command_registry()
    parser = build_parser(registry)

    assert registry["rebuild-position-authority"].json_output_supported is True
    args = parser.parse_args(["rebuild-position-authority", "--flat-stale-projection-repair", "--json"])
    assert args.cmd == "rebuild-position-authority"
    assert args.flat_stale_projection_repair is True
    assert args.json is True
    historical_args = parser.parse_args([
        "rebuild-position-authority",
        "--historical-fragmentation-projection-repair",
        "--json",
    ])
    assert historical_args.cmd == "rebuild-position-authority"
    assert historical_args.historical_fragmentation_projection_repair is True
    assert historical_args.json is True


def test_legacy_operator_closeout_evidence_enrichment_cli_supports_json_preview() -> None:
    registry = command_registry()
    parser = build_parser(registry)

    args = parser.parse_args([
        "rebuild-position-authority",
        "--flat-stale-projection-repair",
        "--enrich-legacy-operator-closeout-evidence",
        "--json",
    ])

    assert args.cmd == "rebuild-position-authority"
    assert args.flat_stale_projection_repair is True
    assert args.enrich_legacy_operator_closeout_evidence is True
    assert args.json is True
    assert args.apply is False
    assert args.yes is False


def test_legacy_operator_closeout_evidence_enrichment_requires_apply_yes_for_mutation() -> None:
    registry = command_registry()
    parser = build_parser(registry)

    preview = parser.parse_args([
        "rebuild-position-authority",
        "--flat-stale-projection-repair",
        "--enrich-legacy-operator-closeout-evidence",
        "--json",
    ])
    apply_without_yes = parser.parse_args([
        "rebuild-position-authority",
        "--flat-stale-projection-repair",
        "--enrich-legacy-operator-closeout-evidence",
        "--apply",
        "--json",
    ])
    apply_with_yes = parser.parse_args([
        "rebuild-position-authority",
        "--flat-stale-projection-repair",
        "--enrich-legacy-operator-closeout-evidence",
        "--apply",
        "--yes",
        "--json",
    ])

    assert preview.apply is False and preview.yes is False
    assert apply_without_yes.apply is True and apply_without_yes.yes is False
    assert apply_with_yes.apply is True and apply_with_yes.yes is True


def test_operator_recovery_guard_uses_basic_guard_not_strategy_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    handler_called = False

    def _handler(_args: argparse.Namespace, _context: AppContext) -> int:
        nonlocal handler_called
        handler_called = True
        calls.append("handler")
        return 0

    def _forbidden(name: str):
        def _inner(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"{name} must not run for operator_recovery")

        return _inner

    monkeypatch.setattr(config, "validate_live_run_startup_contract", _forbidden("live_run_startup"))
    monkeypatch.setattr(config, "validate_runtime_profile_bindings_for_live_startup", _forbidden("profile_bindings"))
    monkeypatch.setattr(config, "validate_live_strategy_selection", _forbidden("live_strategy_selection"))
    monkeypatch.setattr(config, "validate_runtime_strategy_set_selection", _forbidden("strategy_set_selection"))
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", _forbidden("load_approved_profile"))
    monkeypatch.setattr(
        runtime_strategy_set.RuntimeDecisionRequestBuilder,
        "materialize_instance",
        _forbidden("materialize_instance"),
    )
    monkeypatch.setattr(
        "bithumb_bot.operator_smoke_preflight.validate_live_operator_basic_guard",
        lambda _settings: calls.append("basic_guard"),
    )

    spec = CommandSpec(
        name="rebuild-position-authority",
        domain="repairs",
        handler=_handler,
        register_parser=lambda subparsers: subparsers.add_parser("rebuild-position-authority"),
        guard_policy="operator_recovery",
    )

    rc = dispatch(
        argparse.Namespace(cmd="rebuild-position-authority"),
        AppContext(settings=SimpleNamespace(MODE="live")),
        {"rebuild-position-authority": spec},
    )

    assert rc == 0
    assert calls == ["basic_guard", "handler"]
    assert handler_called is True


@pytest.mark.parametrize("command", ["reconcile", "rebuild-position-authority"])
def test_operator_recovery_basic_guard_failure_blocks_handler(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    from bithumb_bot.config import LiveModeValidationError

    def _blocked(_settings):
        raise LiveModeValidationError("live operator command guard validation failed: DB_PATH required")

    def _handler(_args: argparse.Namespace, _context: AppContext) -> int:
        raise AssertionError("handler bypassed operator recovery guard")

    monkeypatch.setattr("bithumb_bot.operator_smoke_preflight.validate_live_operator_basic_guard", _blocked)

    spec = CommandSpec(
        name=command,
        domain="live_ops" if command == "reconcile" else "repairs",
        handler=_handler,
        register_parser=lambda subparsers: subparsers.add_parser(command),
        guard_policy="operator_recovery",
    )

    with pytest.raises(SystemExit) as exc:
        dispatch(
            argparse.Namespace(cmd=command, apply=True, yes=True),
            AppContext(settings=SimpleNamespace(MODE="live"), printer=lambda _message: None),
            {command: spec},
        )

    assert exc.value.code == 1


@pytest.mark.parametrize(
    ("command", "policy", "validator_name"),
    [
        ("run", "live_run_loop", "validate_live_run_startup_contract"),
        ("live-dry-run", "live_dry_run_loop", "validate_live_dry_run_loop_startup_contract"),
        ("panic-stop", "live_preflight", "validate_live_mode_preflight"),
    ],
)
def test_live_guard_failure_blocks_handler_before_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    policy: str,
    validator_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from bithumb_bot.config import LiveModeValidationError

    def _blocked(_settings):
        raise LiveModeValidationError("blocked by test")

    def _handler(_args: argparse.Namespace, _context: AppContext) -> int:
        raise AssertionError("handler bypassed live guard")

    monkeypatch.setattr(f"bithumb_bot.config.{validator_name}", _blocked)
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda message: notifications.append(message))

    spec = CommandSpec(
        name=command,
        domain="runtime",
        handler=_handler,
        register_parser=lambda subparsers: subparsers.add_parser(command),
        guard_policy=policy,
    )

    with pytest.raises(SystemExit) as exc:
        dispatch(
            argparse.Namespace(cmd=command),
            AppContext(settings=SimpleNamespace(MODE="live")),
            {command: spec},
        )

    assert exc.value.code == 1
    assert "[LIVE-COMMAND-GUARD] blocked by test" in capsys.readouterr().out
    assert bool(notifications) is (policy == "live_run_loop")


def test_non_live_mode_skips_live_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    def _blocked(_settings):
        raise AssertionError("non-live guard should not run")

    monkeypatch.setattr("bithumb_bot.config.validate_live_mode_preflight", _blocked)

    spec = CommandSpec(
        name="panic-stop",
        domain="live_ops",
        handler=lambda _args, _context: 9,
        register_parser=lambda subparsers: subparsers.add_parser("panic-stop"),
        guard_policy="live_preflight",
    )

    assert dispatch(argparse.Namespace(cmd="panic-stop"), AppContext(settings=SimpleNamespace(MODE="paper")), {"panic-stop": spec}) == 9


def test_cli_composition_modules_do_not_import_domain_internals() -> None:
    guarded = [
        Path("src/bithumb_bot/cli/main.py"),
        Path("src/bithumb_bot/cli/parser.py"),
        Path("src/bithumb_bot/cli/registry.py"),
        Path("src/bithumb_bot/cli/dispatch.py"),
        Path("src/bithumb_bot/cli/guards.py"),
    ]
    forbidden = (
        "bithumb_bot.broker",
        "bithumb_bot.db_core",
        "bithumb_bot.recovery",
        "bithumb_bot.runtime_state",
        "bithumb_bot.flatten",
        "bithumb_bot.fee_",
        "bithumb_bot.research",
        "bithumb_bot.profile_cli",
        "bithumb_bot.strategy_sweep",
        "bithumb_bot.app_impl",
    )

    for path in guarded:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            module = None
            if isinstance(node, ast.ImportFrom):
                module = _resolve_import_from(path, node)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith(forbidden), f"{path}: {alias.name}"
            if module is not None:
                assert not module.startswith(forbidden), f"{path}: {module}"


def test_legacy_command_module_cannot_dispatch_to_app_impl() -> None:
    source = Path("src/bithumb_bot/cli/commands/_legacy.py").read_text(encoding="utf-8")

    assert "app_impl.main" not in source
    assert "app_impl.legacy_main" not in source
    assert "legacy_main(argv)" not in source


def test_app_module_remains_tiny_compatibility_shim() -> None:
    path = Path("src/bithumb_bot/app.py")
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    defs = [node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]

    assert defs == ["__getattr__", "legacy_main"]
    assert len(source.splitlines()) <= 25
    assert "from .cli.main import main" in source


def test_app_impl_module_remains_deprecated_compatibility_facade() -> None:
    path = Path("src/bithumb_bot/app_impl.py")
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    defs = [node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    forbidden = (
        "bithumb_bot.broker",
        "bithumb_bot.db_core",
        "bithumb_bot.recovery",
        "bithumb_bot.runtime_state",
        "bithumb_bot.flatten",
        "bithumb_bot.fee_",
        "bithumb_bot.research",
        "bithumb_bot.profile_cli",
        "bithumb_bot.strategy_sweep",
        "bithumb_bot.reporting",
    )

    assert defs == ["__getattr__", "main"]
    assert len(source.splitlines()) <= 30
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = _resolve_import_from(path, node)
            assert not module.startswith(forbidden), f"{path}: {module}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith(forbidden), f"{path}: {alias.name}"


def test_cli_command_modules_do_not_depend_on_app_impl_or_call_helper() -> None:
    for path in Path("src/bithumb_bot/cli/commands").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        assert "call_app_impl" not in source, f"{path}: call_app_impl"
        assert "bithumb_bot.app_impl" not in source, f"{path}: bithumb_bot.app_impl"
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = _resolve_import_from(path, node)
                assert module != "bithumb_bot.app_impl", f"{path}: {module}"
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "bithumb_bot.app_impl", f"{path}: {alias.name}"


def test_app_main_compatibility_smoke() -> None:
    from bithumb_bot.app import main
    from bithumb_bot.cli.main import main as cli_main

    assert main is cli_main


def test_backtest2_does_not_import_app_main_directly() -> None:
    source = Path("backtest2.py").read_text(encoding="utf-8")

    assert "bithumb_bot.app import main" not in source
    assert "run_cli" in source
    assert "bootstrap" in source


def test_root_backtest_remains_smoke_only_fail_closed() -> None:
    source = Path("backtest.py").read_text(encoding="utf-8")

    assert "--diagnostic-smoke-only" in source
    assert "SMOKE-BACKTEST REFUSED" in source
    assert "tools.diagnostic_smoke_backtest" in source


def test_notification_diagnose_command_registered() -> None:
    registry = command_registry()

    assert "notification-diagnose" in registry
    assert registry["notification-diagnose"].json_output_supported is True


def test_app_impl_main_compatibility_smoke(capsys: pytest.CaptureFixture[str]) -> None:
    from bithumb_bot.app_impl import main

    with pytest.raises(SystemExit) as exc:
        main(["--help"])

    assert exc.value.code == 0
    assert "bithumb-bot" in capsys.readouterr().out


def _resolve_import_from(path: Path, node: ast.ImportFrom) -> str:
    module = node.module or ""
    if not node.level:
        return module
    package = path.with_suffix("").parts
    base = ".".join(package[1 : len(package) - node.level + 1])
    return f"{base}.{module}" if module else base
