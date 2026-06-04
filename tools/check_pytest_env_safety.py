#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from bithumb_bot.config_spec import PYTEST_INHERITANCE_UNSAFE_ENV_KEYS  # noqa: E402


RUNNERS = (
    PROJECT_ROOT / "scripts" / "run_full_pytest_tests.sh",
    PROJECT_ROOT / "scripts" / "run_fast_pr_tests.sh",
    PROJECT_ROOT / "scripts" / "run_research_nightly_tests.sh",
    PROJECT_ROOT / "scripts" / "run_parallel_research_safety_tests.sh",
)
HELPER = PROJECT_ROOT / "scripts" / "lib" / "pytest_workspace.sh"
CONFTEST = PROJECT_ROOT / "tests" / "conftest.py"
REQUIRED_UNSAFE_ENV_KEYS = {
    "NTFY_TOPIC",
    "NOTIFIER_WEBHOOK_URL",
    "SLACK_WEBHOOK_URL",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "BITHUMB_API_KEY",
    "BITHUMB_API_SECRET",
}
BROKER_PRIVATE_ENV_KEYS = {"BITHUMB_API_KEY", "BITHUMB_API_SECRET"}
EXTERNAL_NOTIFICATION_ENV_KEYS = REQUIRED_UNSAFE_ENV_KEYS - BROKER_PRIVATE_ENV_KEYS


def _runner_unset_keys(text: str) -> set[str]:
    return set(re.findall(r"^\s*unset\s+([A-Z0-9_]+)\s*$", text, flags=re.MULTILINE))


def _array_keys(text: str, array_name: str) -> set[str]:
    match = re.search(rf"^{array_name}=\(\n(?P<body>.*?)^\)", text, flags=re.MULTILINE | re.DOTALL)
    if not match:
        return set()
    return set(re.findall(r"^\s*([A-Z0-9_]+)\s*$", match.group("body"), flags=re.MULTILINE))


def _extract_function_body(text: str, function_name: str) -> str | None:
    marker = f"{function_name}() {{"
    try:
        start = text.index(marker)
    except ValueError:
        return None
    body_start = text.index("{", start) + 1
    depth = 1
    index = body_start
    while index < len(text):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[body_start:index]
        index += 1
    return None


def _script_command_index(text: str) -> int | None:
    markers = [
        "bithumb_pytest_run_preflight",
        "uv run python",
        "uv run pytest",
        '"${pytest_cmd[@]}"',
        "$PYTEST_BIN",
        "pytest ",
    ]
    indexes = [text.index(marker) for marker in markers if marker in text]
    return min(indexes) if indexes else None


def _failures() -> list[str]:
    failures: list[str] = []
    helper_text = HELPER.read_text(encoding="utf-8")
    conftest_text = CONFTEST.read_text(encoding="utf-8")

    unsafe_keys = set(PYTEST_INHERITANCE_UNSAFE_ENV_KEYS)
    helper_body = _extract_function_body(helper_text, "bithumb_pytest_sanitize_unsafe_env")
    if helper_body is None:
        failures.append("pytest workspace helper lacks bithumb_pytest_sanitize_unsafe_env")
        helper_body = ""

    if "BITHUMB_PYTEST_ALLOW_EXTERNAL_NOTIFICATIONS" not in helper_body:
        failures.append("pytest env sanitizer lacks explicit external-notification opt-in guard")
    if "export NOTIFIER_ENABLED=false" not in helper_body:
        failures.append("pytest env sanitizer does not disable notifier delivery by default")
    if "BITHUMB_PYTEST_BROKER_PRIVATE_ENV_KEYS" not in helper_text:
        failures.append("pytest env sanitizer does not declare broker-private env separately")
    if "BITHUMB_PYTEST_EXTERNAL_NOTIFICATION_ENV_KEYS" not in helper_text:
        failures.append("pytest env sanitizer does not declare external notification env separately")

    broker_keys = _array_keys(helper_text, "BITHUMB_PYTEST_BROKER_PRIVATE_ENV_KEYS")
    notification_keys = _array_keys(helper_text, "BITHUMB_PYTEST_EXTERNAL_NOTIFICATION_ENV_KEYS")
    missing_helper_keys = sorted(unsafe_keys - (broker_keys | notification_keys))
    if missing_helper_keys:
        failures.append("pytest env sanitizer does not declare pytest-inheritance-unsafe env: " + ", ".join(missing_helper_keys))
    if broker_keys != BROKER_PRIVATE_ENV_KEYS:
        failures.append(
            "pytest env sanitizer broker-private env list mismatch: "
            f"expected {sorted(BROKER_PRIVATE_ENV_KEYS)}, got {sorted(broker_keys)}"
        )
    if not EXTERNAL_NOTIFICATION_ENV_KEYS <= notification_keys:
        failures.append(
            "pytest env sanitizer external-notification env list missing: "
            + ", ".join(sorted(EXTERNAL_NOTIFICATION_ENV_KEYS - notification_keys))
        )
    if 'unset "$key"' not in helper_body:
        failures.append("pytest env sanitizer does not unset declared unsafe env keys")

    try:
        broker_loop_index = helper_body.index("BITHUMB_PYTEST_BROKER_PRIVATE_ENV_KEYS")
        broker_unset_index = helper_body.index('unset "$key"', broker_loop_index)
        opt_in_index = helper_body.index("BITHUMB_PYTEST_ALLOW_EXTERNAL_NOTIFICATIONS")
    except ValueError as exc:
        failures.append(f"pytest env sanitizer missing expected broker/opt-in marker: {exc}")
    else:
        if broker_unset_index > opt_in_index:
            failures.append("pytest env sanitizer must clear broker-private env before external-notification opt-in branch")

    for key in sorted(BROKER_PRIVATE_ENV_KEYS):
        if key not in helper_text:
            failures.append(f"pytest env sanitizer missing broker-private key: {key}")
    for key in sorted(EXTERNAL_NOTIFICATION_ENV_KEYS):
        if key not in helper_text:
            failures.append(f"pytest env sanitizer missing external-notification key: {key}")

    for runner in RUNNERS:
        runner_text = runner.read_text(encoding="utf-8")
        runner_label = runner.relative_to(PROJECT_ROOT).as_posix()
        if "scripts/lib/pytest_workspace.sh" not in runner_text:
            failures.append(f"{runner_label} does not source pytest workspace helper")
        if "bithumb_pytest_sanitize_unsafe_env" not in runner_text:
            failures.append(f"{runner_label} does not call pytest unsafe env sanitizer")
            continue
        try:
            pythonpath_index = runner_text.index('export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"')
            safety_index = runner_text.index("bithumb_pytest_sanitize_unsafe_env")
        except ValueError as exc:
            failures.append(f"{runner_label} missing expected sanitizer ordering marker: {exc}")
            continue
        command_index = _script_command_index(runner_text[safety_index + 1 :])
        if command_index is None:
            failures.append(f"{runner_label} has no recognized preflight/pytest command after sanitizer")
            continue
        command_index += safety_index + 1
        if not (pythonpath_index < safety_index < command_index):
            failures.append(f"{runner_label} must sanitize unsafe env after PYTHONPATH and before preflight/pytest")

    if "PYTEST_INHERITANCE_UNSAFE_ENV_KEYS" not in conftest_text:
        failures.append("pytest conftest does not use the config-spec unsafe inheritance key set")
    try:
        unsafe_import_index = conftest_text.index("from bithumb_bot.config_spec import PYTEST_INHERITANCE_UNSAFE_ENV_KEYS")
        import_config_index = conftest_text.index("import bithumb_bot.config as _config_module")
        import_settings_index = conftest_text.index("from bithumb_bot.config import settings")
        top_level_clear_index = conftest_text.index("os.environ.pop(_unsafe_env_key, None)")
        top_level_disable_index = conftest_text.index('os.environ["NOTIFIER_ENABLED"] = "false"')
    except ValueError as exc:
        failures.append(f"pytest conftest missing expected unsafe-env import-order marker: {exc}")
    else:
        if not (
            unsafe_import_index
            < top_level_clear_index
            < top_level_disable_index
            < import_config_index
            < import_settings_index
        ):
            failures.append("pytest conftest must clear unsafe env before importing config/settings")
    if "monkeypatch.delenv(key" not in conftest_text:
        failures.append("pytest conftest does not clear unsafe inherited env")
    if "monkeypatch.setenv(\"NOTIFIER_ENABLED\", \"false\")" not in conftest_text:
        failures.append("pytest conftest does not disable notifier delivery by default")
    if "_post_json" not in conftest_text or "_post_ntfy" not in conftest_text:
        failures.append("pytest conftest does not guard notifier transport functions")
    if "PytestNotificationSafetyViolation" not in conftest_text:
        failures.append("pytest conftest notifier transport blockers do not raise the fail-closed safety sentinel")

    missing_specs = sorted(REQUIRED_UNSAFE_ENV_KEYS - unsafe_keys)
    if missing_specs:
        failures.append("config spec does not classify required pytest-unsafe env: " + ", ".join(missing_specs))

    notifier_text = (PROJECT_ROOT / "src" / "bithumb_bot" / "notifier.py").read_text(encoding="utf-8")
    if "class PytestNotificationSafetyViolation" not in notifier_text:
        failures.append("notifier lacks explicit pytest safety violation sentinel")
    if "except PytestNotificationSafetyViolation:" not in notifier_text:
        failures.append("notifier.notify does not re-raise pytest safety violations")

    return failures


def main() -> int:
    failures = _failures()
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    print("pytest env safety check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
