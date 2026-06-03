#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from scripts.check_fast_test_durations import TestDuration, parse_pytest_durations
from tests.policy.research_runner_policy import INVENTORY_PATH, load_inventory


@dataclass(frozen=True)
class InventoryDurationViolation:
    nodeid: str
    phase: str
    seconds: float
    budget_seconds: float


def inventory_duration_violations(
    durations: list[TestDuration],
    *,
    inventory_path: Path = INVENTORY_PATH,
) -> list[InventoryDurationViolation]:
    inventory = load_inventory(inventory_path)
    budget_by_nodeid = {
        nodeid: float(entry["duration_budget_seconds"])
        for nodeid, entry in inventory.items()
    }
    violations: list[InventoryDurationViolation] = []
    for duration in durations:
        budget = budget_by_nodeid.get(duration.nodeid)
        if budget is None or duration.seconds <= budget:
            continue
        violations.append(
            InventoryDurationViolation(
                nodeid=duration.nodeid,
                phase=duration.phase,
                seconds=duration.seconds,
                budget_seconds=budget,
            )
        )
    return sorted(
        violations,
        key=lambda violation: (
            -violation.seconds,
            violation.nodeid,
            violation.phase,
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail when inventoried research E2E pytest durations exceed their inventory budgets."
    )
    parser.add_argument("duration_log", type=Path)
    parser.add_argument(
        "--inventory",
        type=Path,
        default=INVENTORY_PATH,
        help="research E2E inventory JSON path",
    )
    args = parser.parse_args(argv)

    durations = parse_pytest_durations(args.duration_log.read_text(encoding="utf-8"))
    violations = inventory_duration_violations(durations, inventory_path=args.inventory)
    if violations:
        print("research E2E inventory duration budget exceeded:", file=sys.stderr)
        for violation in violations:
            print(
                f"- {violation.seconds:.2f}s {violation.phase} {violation.nodeid} "
                f"> budget {violation.budget_seconds:g}s",
                file=sys.stderr,
            )
        return 1
    print(f"research E2E inventory duration guard: ok ({len(durations)} reported durations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
