#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.policy.research_runner_policy import discover_expensive_research_tests, load_inventory


DURATION_RE = re.compile(r"^\s*(?P<seconds>\d+(?:\.\d+)?)s\s+(?P<phase>\S+)\s+(?P<nodeid>.+?)(?:\s+\([^)]*\))?\s*$")
STATUS_OK = "OK"
STATUS_OVER_BUDGET = "OVER_BUDGET"
STATUS_OVER_LAST_MEASURED_2X = "OVER_LAST_MEASURED_2X"
STATUS_MISSING_INVENTORY = "MISSING_INVENTORY"
STATUS_UNPARSED_DURATION_LINE = "UNPARSED_DURATION_LINE"


@dataclass(frozen=True)
class DurationRow:
    actual_seconds: float
    phase: str
    nodeid: str


@dataclass(frozen=True)
class DurationInventoryResult:
    nodeid: str
    actual_seconds: float | None
    phase: str | None
    duration_budget_seconds: float | None
    last_measured_seconds: float | None
    status: list[str]
    raw_line: str | None = None


def parse_pytest_duration_lines(lines: Iterable[str]) -> tuple[list[DurationRow], list[str]]:
    rows: list[DurationRow] = []
    unparsed: list[str] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        lower = line.lower()
        if (
            lower.startswith("=")
            or lower.startswith("slowest durations")
            or lower.startswith("(")
            or lower.startswith("no durations")
        ):
            continue
        match = DURATION_RE.match(line)
        if match:
            nodeid = match.group("nodeid").strip()
            if "::" not in nodeid:
                unparsed.append(line)
                continue
            rows.append(
                DurationRow(
                    actual_seconds=float(match.group("seconds")),
                    phase=match.group("phase"),
                    nodeid=nodeid,
                )
            )
            continue
        if ".py" in line or "s " in line:
            unparsed.append(line)
    return rows, unparsed


def parse_pytest_duration_file(path: Path) -> tuple[list[DurationRow], list[str]]:
    return parse_pytest_duration_lines(path.read_text(encoding="utf-8").splitlines())


def compare_duration_inventory(
    *,
    durations_file: Path,
    inventory_path: Path,
) -> tuple[list[DurationInventoryResult], list[str]]:
    rows, unparsed = parse_pytest_duration_file(durations_file)
    inventory = load_inventory(inventory_path)
    results: list[DurationInventoryResult] = []
    for row in rows:
        entry = inventory.get(row.nodeid)
        if entry is None:
            results.append(
                DurationInventoryResult(
                    nodeid=row.nodeid,
                    actual_seconds=row.actual_seconds,
                    phase=row.phase,
                    duration_budget_seconds=None,
                    last_measured_seconds=None,
                    status=[STATUS_MISSING_INVENTORY],
                )
            )
            continue
        budget = float(entry["duration_budget_seconds"])
        last_measured = float(entry["last_measured_seconds"])
        status: list[str] = []
        if row.actual_seconds > budget:
            status.append(STATUS_OVER_BUDGET)
        if last_measured > 0 and row.actual_seconds > last_measured * 2:
            status.append(STATUS_OVER_LAST_MEASURED_2X)
        if not status:
            status.append(STATUS_OK)
        results.append(
            DurationInventoryResult(
                nodeid=row.nodeid,
                actual_seconds=row.actual_seconds,
                phase=row.phase,
                duration_budget_seconds=budget,
                last_measured_seconds=last_measured,
                status=status,
            )
        )
    for line in unparsed:
        results.append(
            DurationInventoryResult(
                nodeid="<unparsed>",
                actual_seconds=None,
                phase=None,
                duration_budget_seconds=None,
                last_measured_seconds=None,
                status=[STATUS_UNPARSED_DURATION_LINE],
                raw_line=line,
            )
        )
    return results, unparsed


def _strict_new_violations(results: Iterable[DurationInventoryResult], test_root: Path) -> list[str]:
    expensive_nodeids = {test.nodeid for test in discover_expensive_research_tests(test_root)}
    return [
        result.nodeid
        for result in results
        if STATUS_MISSING_INVENTORY in result.status and result.nodeid in expensive_nodeids
    ]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--durations-file", required=True, type=Path)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--strict-new", action="store_true")
    parser.add_argument("--test-root", type=Path, default=Path("tests"))
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    if not args.durations_file.exists():
        print(f"duration inventory check failed: missing durations file: {args.durations_file}", file=sys.stderr)
        return 1
    try:
        results, unparsed = compare_duration_inventory(
            durations_file=args.durations_file,
            inventory_path=args.inventory,
        )
    except (OSError, json.JSONDecodeError, AssertionError) as exc:
        print(f"duration inventory check failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps([asdict(result) for result in results], indent=2, sort_keys=True))
    else:
        for result in results:
            status = ",".join(result.status)
            print(
                f"nodeid={result.nodeid} actual_seconds={result.actual_seconds} "
                f"phase={result.phase} duration_budget_seconds={result.duration_budget_seconds} "
                f"last_measured_seconds={result.last_measured_seconds} status={status}"
            )
            if result.raw_line is not None:
                print(f"raw_line={result.raw_line}")

    if unparsed:
        print("duration inventory check failed: unparsed duration lines present", file=sys.stderr)
        return 1
    if args.strict_new:
        strict_missing = _strict_new_violations(results, args.test_root)
        if strict_missing:
            for nodeid in strict_missing:
                print(f"new expensive duration nodeid missing inventory entry: {nodeid}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
