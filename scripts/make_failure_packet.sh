#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
WORK_DIR="${CODEX_PYTEST_WORK_DIR:-${TMPDIR:-/tmp}/bithumb-bot-codex-pytest}"
ITERATION="${CODEX_PYTEST_ITERATION:-manual}"
LATEST_LOG_FILE="${WORK_DIR}/latest_full_suite_log"
PYTHON_BIN="${PYTHON:-python3}"

cd "${PROJECT_ROOT}"

if [[ $# -gt 1 ]]; then
  echo "[FAILURE-PACKET] usage: $0 [full-suite-log-path]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  log_file="$1"
else
  if [[ ! -f "${LATEST_LOG_FILE}" ]]; then
    echo "[FAILURE-PACKET] latest log pointer is missing: ${LATEST_LOG_FILE}" >&2
    exit 1
  fi
  log_file="$(<"${LATEST_LOG_FILE}")"
fi

if [[ ! -f "${log_file}" ]]; then
  echo "[FAILURE-PACKET] full-suite log is missing: ${log_file}" >&2
  exit 1
fi

emit_head_tail() {
  local file="$1"
  local head_lines="${2:-240}"
  local tail_lines="${3:-240}"
  local line_count

  line_count="$(wc -l < "${file}")"
  if [[ "${line_count}" -le "${head_lines}" ]]; then
    sed -n "1,${head_lines}p" "${file}"
  elif [[ "${line_count}" -le $((head_lines + tail_lines)) ]]; then
    sed -n "1,${line_count}p" "${file}"
  else
    sed -n "1,${head_lines}p" "${file}"
    echo
    echo "[FAILURE-PACKET] truncated middle; showing tail below. See packet file for full evidence."
    echo
    tail -n "${tail_lines}" "${file}"
  fi
}

timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
packet_dir="${WORK_DIR}/packets/${timestamp}_iter${ITERATION}"
mkdir -p "${packet_dir}"

echo "[FAILURE-PACKET] creating packet in ${packet_dir}" >&2
cp "${log_file}" "${packet_dir}/full_suite.log"

failed_tests_file="${packet_dir}/failed_tests.txt"
preflight_file="${packet_dir}/preflight_failure.txt"
collection_file="${packet_dir}/first_collection_import_config_error.txt"
artifact_file="${packet_dir}/runtime_artifact_failure.txt"
workspace_file="${packet_dir}/pytest_workspace_summary.txt"
pytest_failure_sections_file="${packet_dir}/pytest_failure_sections.txt"
pytest_short_summary_file="${packet_dir}/pytest_short_summary.txt"
pytest_failure_context_file="${packet_dir}/pytest_failure_context.txt"
preflight_json_file="${packet_dir}/preflight_failure.json"
diagnostic_artifact_file="${packet_dir}/diagnostic_runtime_artifact_check.txt"
failure_signature_material_file="${packet_dir}/failure_signature_material.txt"

grep -E '^(FAILED|ERROR) tests/[^[:space:]]+' "${log_file}" \
  | awk '{print $2}' \
  | sed 's/[[:space:]].*$//' \
  | sort -u > "${failed_tests_file}" || true

grep -E '\[PYTEST-PREFLIGHT\] failed|\[PYTEST-PREFLIGHT\] pytest did not start|preflight_failure\.json' \
  "${log_file}" > "${preflight_file}" || true

awk '
  /ERROR collecting|ImportError|ModuleNotFoundError|ConftestImportFailure|ConfigError|INTERNALERROR|pytest UsageError/ {
    if (!seen) {
      seen=1
      remaining=80
    }
  }
  seen && remaining > 0 {
    print
    remaining--
  }
' "${log_file}" > "${collection_file}"

awk '
  /\[RUNTIME-ARTIFACT-CHECK\] repo-local runtime\/test artifacts detected/ {
    seen=1
    remaining=80
  }
  seen && remaining > 0 {
    print
    remaining--
  }
' "${log_file}" > "${artifact_file}"

grep -E '\[PYTEST-WORKSPACE\]|retained_size_bytes|large_file_bytes|keeping workspace' \
  "${log_file}" > "${workspace_file}" || true

"${PYTHON_BIN}" - "${log_file}" "${packet_dir}" <<'PY'
import json
import re
import shutil
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
packet_dir = Path(sys.argv[2])
text = log_path.read_text(encoding="utf-8", errors="replace")
lines = text.splitlines()


def bound_text(content, limit, source):
    if len(content) <= limit:
        return content
    head_len = limit // 2
    tail_len = limit - head_len
    marker = (
        "\n\n[TRUNCATED: output exceeded packet limit; "
        f"preserved head and tail. See {source} for complete evidence.]\n\n"
    )
    return content[:head_len].rstrip() + marker + content[-tail_len:].lstrip()


def write_bounded(name, content, limit):
    path = packet_dir / name
    content = content.strip("\n")
    if not content:
        content = f"No matching evidence was extracted from {log_path}."
    path.write_text(
        bound_text(content, limit, "full_suite.log") + "\n",
        encoding="utf-8",
    )


heading_re = re.compile(r"^=+\s+(.+?)\s+=+$")


def extract_named_sections(names):
    sections = []
    index = 0
    while index < len(lines):
        match = heading_re.match(lines[index])
        if not match or match.group(1).strip().upper() not in names:
            index += 1
            continue
        start = index
        index += 1
        while index < len(lines):
            next_heading = heading_re.match(lines[index])
            if next_heading:
                break
            index += 1
        sections.append("\n".join(lines[start:index]))
    return "\n\n".join(sections)


write_bounded(
    "pytest_failure_sections.txt",
    extract_named_sections({"FAILURES", "ERRORS"}),
    60000,
)
write_bounded(
    "pytest_short_summary.txt",
    extract_named_sections({"SHORT TEST SUMMARY INFO"}),
    24000,
)

marker_patterns = [
    r"FAILED tests/",
    r"ERROR tests/",
    r"Traceback \(most recent call last\)",
    r"AssertionError",
    r"ExceptionGroup",
    r"ERROR collecting",
    r"INTERNALERROR",
    r"ImportError",
    r"ModuleNotFoundError",
    r"ConftestImportFailure",
    r"ConfigError",
    r"pytest UsageError",
]
marker_re = re.compile("|".join(f"(?:{pattern})" for pattern in marker_patterns))
windows = []
before = 35
after = 90
for line_number, line in enumerate(lines):
    if marker_re.search(line):
        windows.append((max(0, line_number - before), min(len(lines), line_number + after + 1)))

merged = []
for start, end in windows:
    if merged and start <= merged[-1][1]:
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    else:
        merged.append((start, end))

context_parts = []
for start, end in merged:
    context_parts.append(f"[context lines {start + 1}-{end} from full_suite.log]")
    context_parts.extend(lines[start:end])
write_bounded("pytest_failure_context.txt", "\n".join(context_parts), 60000)

report_path = None
report_re = re.compile(
    r"\[PYTEST-PREFLIGHT\].*?report=(?:\"([^\"]+preflight_failure\.json)\"|'([^']+preflight_failure\.json)'|([^ \t\r\n]+preflight_failure\.json))"
)
for line in lines:
    match = report_re.search(line)
    if not match:
        continue
    raw_path = next(group for group in match.groups() if group)
    report_path = Path(raw_path)
    if not report_path.is_absolute():
        report_path = (log_path.parent / report_path).resolve()
    break

target_json = packet_dir / "preflight_failure.json"
if report_path and report_path.is_file():
    shutil.copyfile(report_path, target_json)
else:
    placeholder = {
        "status": "missing",
        "message": "No preflight_failure.json report was found at packet generation time.",
        "source_log": str(log_path),
        "parsed_report_path": str(report_path) if report_path else None,
    }
    target_json.write_text(json.dumps(placeholder, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

{
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] Diagnostic evidence only."
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] The WSL wrapper remains the validation authority."
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] Command: ./scripts/check_repo_runtime_artifacts.sh"
  echo
} > "${diagnostic_artifact_file}"
set +e
if [[ ! -e "${SCRIPT_DIR}/check_repo_runtime_artifacts.sh" ]]; then
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] skipped: scripts/check_repo_runtime_artifacts.sh is missing." >> "${diagnostic_artifact_file}"
  diagnostic_artifact_exit_code=127
elif [[ ! -x "${SCRIPT_DIR}/check_repo_runtime_artifacts.sh" ]]; then
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] skipped: scripts/check_repo_runtime_artifacts.sh is not executable." >> "${diagnostic_artifact_file}"
  diagnostic_artifact_exit_code=126
else
  "${SCRIPT_DIR}/check_repo_runtime_artifacts.sh" >> "${diagnostic_artifact_file}" 2>&1
  diagnostic_artifact_exit_code=$?
fi
set -e
{
  echo
  echo "[DIAGNOSTIC-RUNTIME-ARTIFACT-CHECK] exit_code=${diagnostic_artifact_exit_code}"
} >> "${diagnostic_artifact_file}"

git status --porcelain=v1 --untracked-files=all > "${packet_dir}/git_status.txt"
git diff --stat > "${packet_dir}/git_diff_stat.txt"
git diff --binary > "${packet_dir}/git_diff.patch"

{
  echo "# Repro Commands"
  echo
  echo "Wrapper-only full-suite validation command. Codex must not run this command:"
  echo
  echo '```bash'
  echo 'PYTEST_XDIST_WORKERS=8 PYTEST_XDIST_DIST=worksteal ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh'
  echo '```'
  echo
  echo "Focused examples derived from the failure packet:"
  echo
  if [[ -s "${failed_tests_file}" ]]; then
    while IFS= read -r selector; do
      [[ -z "${selector}" ]] && continue
      test_file="${selector%%::*}"
      echo '```bash'
      if [[ "${selector}" == *"::"* ]]; then
        echo "uv run pytest ${selector} -q"
      else
        echo "uv run pytest ${test_file} -q"
      fi
      echo '```'
    done < "${failed_tests_file}"
  else
    echo "No focused pytest selectors were extracted. Inspect the collection, preflight, and artifact excerpts before choosing any focused command."
  fi
} > "${packet_dir}/repro_commands.txt"

cat > "${packet_dir}/constraints.md" <<'EOF'
# Constraints

- Preserve existing system purpose, operational intent, and repository safety contracts.
- Do not weaken fail-close behavior, live safety guards, recovery correctness, state integrity, accounting correctness, path/storage contracts, exposure authority, reconciliation, or operator-facing reporting.
- Do not add pytest skip, skipif, xfail, or loosened assertions to force a pass.
- Do not add unrealistic mocks to hide production behavior.
- Do not run full-suite validation.
- Do not run selector-less pytest.
- Do not run broad `tests` or `tests/` pytest targets.
- Run only focused pytest commands derived from the failure packet.
- If a test expectation conflicts with a repository safety contract, stop and report the conflict instead of weakening production behavior.
EOF

"${PYTHON_BIN}" - "${packet_dir}" "${PROJECT_ROOT}" "${WORK_DIR}" <<'PY'
import hashlib
import re
import sys
from pathlib import Path

packet_dir = Path(sys.argv[1])
project_root = sys.argv[2]
work_dir = sys.argv[3]

source_files = [
    "failed_tests.txt",
    "pytest_short_summary.txt",
    "pytest_failure_sections.txt",
    "first_collection_import_config_error.txt",
    "preflight_failure.txt",
    "preflight_failure.json",
    "runtime_artifact_failure.txt",
    "diagnostic_runtime_artifact_check.txt",
]


def normalize(content):
    replacements = [
        (project_root, "<PROJECT_ROOT>"),
        (work_dir, "<PYTEST_WORK_DIR>"),
    ]
    for needle, replacement in replacements:
        if needle:
            content = re.sub(
                re.escape(needle) + r"(?=$|[\s'\"`<>)\]])",
                replacement,
                content,
            )
    content = re.sub(r"/tmp/[^\s'\"`<>)\]]+", "<TMP_PATH>", content)
    content = re.sub(r"\b20\d{6}T\d{6}Z\b", "<UTC_TIMESTAMP>", content)
    content = re.sub(
        r"\b20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?\b",
        "<UTC_TIMESTAMP>",
        content,
    )
    content = re.sub(r"\b0x[0-9a-fA-F]+\b", "<MEMORY_ADDRESS>", content)
    content = re.sub(
        r"\b\d+(?:\.\d+)?\s*(?:seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h)\b",
        "<DURATION>",
        content,
        flags=re.IGNORECASE,
    )
    content = re.sub(r"\bin \d+(?:\.\d+)?s\b", "in <DURATION>", content)
    content = re.sub(r"\b\d+(?:\.\d+)?s\b", "<DURATION>", content)
    return content


parts = []
for name in source_files:
    path = packet_dir / name
    if path.exists():
        raw = path.read_text(encoding="utf-8", errors="replace")
    else:
        raw = f"[missing packet file: {name}]\n"
    parts.append(f"===== {name} =====\n{normalize(raw).rstrip()}\n")

material = "\n".join(parts)
(packet_dir / "failure_signature_material.txt").write_text(material, encoding="utf-8")
digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
(packet_dir / "failure_signature.sha256").write_text(digest + "\n", encoding="utf-8")
PY

codex_input="${packet_dir}/codex_input.md"
{
  echo "# WSL-Owned Pytest Repair Packet"
  echo
  echo "Codex is the repair agent, not the validation authority."
  echo "The WSL wrapper is the only authority allowed to run the full-suite command."
  echo "Use the failure packet as baseline evidence."
  echo "Run only focused pytest commands derived from the packet."
  echo "Do not run the full suite."
  echo
  echo "The wrapper-owned success command is:"
  echo
  echo '```bash'
  echo 'PYTEST_XDIST_WORKERS=8 PYTEST_XDIST_DIST=worksteal ./scripts/run_full_pytest_tests.sh && ./scripts/check_repo_runtime_artifacts.sh'
  echo '```'
  echo
  echo "Codex must not run that command, \`./scripts/full_suite.sh\`, \`./scripts/run_full_pytest_tests.sh\`, \`./scripts/check_repo_runtime_artifacts.sh\`, selector-less pytest, broad \`tests\` pytest targets, or raw \`uv run pytest -q\`."
  echo
  echo "## Repository Repair Prompt"
  echo
  if [[ -s "${SCRIPT_DIR}/codex_pytest_repair_prompt.md" ]]; then
    cat "${SCRIPT_DIR}/codex_pytest_repair_prompt.md"
  else
    echo "scripts/codex_pytest_repair_prompt.md is missing or empty."
  fi
  echo
  echo "## Packet Metadata"
  echo
  echo "- project_root: ${PROJECT_ROOT}"
  echo "- packet_dir: ${packet_dir}"
  echo "- full_suite_log: ${log_file}"
  echo "- iteration: ${ITERATION}"
  echo "- failure_signature: $(<"${packet_dir}/failure_signature.sha256")"
  echo
  echo "## Failed Tests"
  echo
  if [[ -s "${failed_tests_file}" ]]; then
    sed 's/^/- /' "${failed_tests_file}"
  else
    echo "No FAILED/ERROR test selectors were extracted."
  fi
  echo
  echo "## Pytest Short Summary"
  echo '```text'
  sed -n '1,220p' "${pytest_short_summary_file}"
  echo '```'
  echo
  echo "## Pytest Failure Sections"
  echo '```text'
  emit_head_tail "${pytest_failure_sections_file}" 360 360
  echo '```'
  echo
  echo "## Failure Context Around Markers"
  echo '```text'
  emit_head_tail "${pytest_failure_context_file}" 360 360
  echo '```'
  echo
  echo "## Preflight Failure JSON"
  echo '```json'
  sed -n '1,220p' "${preflight_json_file}"
  echo '```'
  echo
  echo "## Diagnostic Runtime Artifact Check"
  echo '```text'
  sed -n '1,220p' "${diagnostic_artifact_file}"
  echo '```'
  echo
  echo "## Failure Signature Material"
  echo '```text'
  emit_head_tail "${failure_signature_material_file}" 320 320
  echo '```'
  echo
  echo "## Preflight Failure Excerpt"
  echo '```text'
  sed -n '1,120p' "${preflight_file}"
  echo '```'
  echo
  echo "## First Collection Import Config Error Excerpt"
  echo '```text'
  sed -n '1,120p' "${collection_file}"
  echo '```'
  echo
  echo "## Runtime Artifact Failure Excerpt"
  echo '```text'
  sed -n '1,120p' "${artifact_file}"
  echo '```'
  echo
  echo "## Pytest Workspace Summary"
  echo '```text'
  sed -n '1,160p' "${workspace_file}"
  echo '```'
  echo
  echo "## Git Diff Stat"
  echo '```text'
  sed -n '1,160p' "${packet_dir}/git_diff_stat.txt"
  echo '```'
  echo
  echo "## Git Diff Patch Excerpt"
  echo '```diff'
  emit_head_tail "${packet_dir}/git_diff.patch" 260 260
  echo '```'
  echo
  echo "## Repro Commands"
  echo '```text'
  sed -n '1,220p' "${packet_dir}/repro_commands.txt"
  echo '```'
  echo
  echo "## Recent Full-Suite Log Tail"
  echo '```text'
  tail -n 220 "${log_file}"
  echo '```'
  echo
  echo "## Required Behavior"
  echo
  cat "${packet_dir}/constraints.md"
} > "${codex_input}"

echo "[FAILURE-PACKET] wrote ${codex_input}" >&2
printf '%s\n' "${codex_input}"
