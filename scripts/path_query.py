#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print PathManager-managed path for operational scripts"
    )
    parser.add_argument("--project-root", default=None, help="repository root (default: auto-detect)")
    parser.add_argument("--mode", default=None, help="runtime mode override (paper/live)")
    parser.add_argument("--kind", required=True, help="managed path kind to print")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    project_root = (
        Path(args.project_root).expanduser().resolve()
        if args.project_root
        else Path(__file__).resolve().parents[1]
    )

    if args.mode:
        os.environ["MODE"] = args.mode

    sys.path.insert(0, str(project_root / "src"))
    from bithumb_bot.paths import PathManager, resolve_managed_path  # noqa: PLC0415

    manager = PathManager.from_env(project_root)
    print(resolve_managed_path(args.kind, manager))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
