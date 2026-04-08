from __future__ import annotations

import os
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ALLOWED_MODES = {"paper", "live"}
ALLOWED_LOG_KINDS = {"app", "strategy", "orders", "fills", "errors", "audit"}
MODE_SCOPED_ROOT_SEGMENTS = {"paper", "live", "dryrun"}


class PathPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class PathConfig:
    mode: str
    env_root: Path
    run_root: Path
    data_root: Path
    log_root: Path
    backup_root: Path
    archive_root: Path | None = None


@dataclass(frozen=True)
class PathManager:
    project_root: Path
    config: PathConfig

    @classmethod
    def from_env(cls, project_root: Path) -> "PathManager":
        normalized_mode = str(os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
        if normalized_mode not in ALLOWED_MODES:
            raise PathPolicyError(f"invalid MODE={normalized_mode!r}; allowed values: paper, live")

        default_runtime_root = cls._default_runtime_root(project_root)
        env_root = cls._resolve_root("ENV_ROOT", default_runtime_root / "env", normalized_mode, project_root)
        run_root = cls._resolve_root("RUN_ROOT", default_runtime_root / "run", normalized_mode, project_root)
        data_root = cls._resolve_root("DATA_ROOT", default_runtime_root / "data", normalized_mode, project_root)
        log_root = cls._resolve_root("LOG_ROOT", default_runtime_root / "logs", normalized_mode, project_root)
        backup_root = cls._resolve_root(
            "BACKUP_ROOT", default_runtime_root / "backup", normalized_mode, project_root
        )

        archive_raw = os.getenv("ARCHIVE_ROOT")
        archive_root: Path | None
        if archive_raw is None or not archive_raw.strip():
            archive_root = default_runtime_root / "archive"
        else:
            archive_root = cls._resolve_explicit_root("ARCHIVE_ROOT", archive_raw, normalized_mode, project_root)

        cls._validate_mode_neutral_roots(
            {
                "ENV_ROOT": env_root,
                "RUN_ROOT": run_root,
                "DATA_ROOT": data_root,
                "LOG_ROOT": log_root,
                "BACKUP_ROOT": backup_root,
                "ARCHIVE_ROOT": archive_root,
            }
        )

        return cls(
            project_root=project_root,
            config=PathConfig(
                mode=normalized_mode,
                env_root=env_root,
                run_root=run_root,
                data_root=data_root,
                log_root=log_root,
                backup_root=backup_root,
                archive_root=archive_root,
            ),
        )

    @staticmethod
    def _default_runtime_root(project_root: Path) -> Path:
        state_home = os.getenv("XDG_STATE_HOME")
        if state_home and state_home.strip():
            return (Path(state_home).expanduser() / "bithumb-bot").resolve()
        return (Path.home() / ".local" / "state" / "bithumb-bot").resolve()

    @classmethod
    def _resolve_root(cls, key: str, default_path: Path, mode: str, project_root: Path) -> Path:
        raw = os.getenv(key)
        if raw is None or not raw.strip():
            if mode == "live":
                raise PathPolicyError(
                    f"{key} must be explicitly set as an absolute path when MODE=live"
                )
            return default_path.resolve()
        return cls._resolve_explicit_root(key, raw, mode, project_root)

    @staticmethod
    def _resolve_explicit_root(key: str, raw: str, mode: str, project_root: Path) -> Path:
        path = Path(raw).expanduser()
        if path.is_absolute():
            resolved = path.resolve()
        else:
            if mode == "live":
                raise PathPolicyError(
                    f"{key} must be an absolute path when MODE=live (got relative: {raw!r})"
                )
            resolved = (project_root / path).resolve()

        if mode == "live" and PathManager._is_within(resolved, project_root.resolve()):
            raise PathPolicyError(
                f"{key} must be outside repository when MODE=live (got: {resolved})"
            )
        if mode == "live" and PathManager._contains_segment(resolved, "paper"):
            raise PathPolicyError(
                f"{key} must not contain a paper-scoped path segment when MODE=live (got: {resolved})"
            )
        return resolved

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    @staticmethod
    def _contains_segment(path: Path, segment: str) -> bool:
        normalized = str(segment or "").strip().lower()
        if not normalized:
            return False
        return normalized in {part.lower() for part in path.parts}

    @classmethod
    def _validate_mode_neutral_roots(cls, roots: dict[str, Path | None]) -> None:
        mode_segments = sorted(MODE_SCOPED_ROOT_SEGMENTS)
        for key, path in roots.items():
            if path is None:
                continue
            offending = [segment for segment in mode_segments if cls._contains_segment(path, segment)]
            if offending:
                segments_text = ", ".join(offending)
                raise PathPolicyError(
                    f"{key} must not contain mode-scoped path segment(s) when roots are managed by PathManager; "
                    f"offending_segments={segments_text} path={path}"
                )

    @staticmethod
    def _day_or_today(day: str | None) -> str:
        return day or datetime.now(timezone.utc).date().isoformat()

    def run_dir(self) -> Path:
        return self.config.run_root / self.config.mode

    def run_dir_for_mode(self, mode: str | None = None) -> Path:
        normalized_mode = str(mode or self.config.mode or "paper").strip().lower() or "paper"
        return self.config.run_root / normalized_mode

    def data_dir(self) -> Path:
        return self.config.data_root / self.config.mode

    def data_dir_for_mode(self, mode: str | None = None) -> Path:
        normalized_mode = str(mode or self.config.mode or "paper").strip().lower() or "paper"
        return self.config.data_root / normalized_mode

    def log_dir(self) -> Path:
        return self.config.log_root / self.config.mode

    def log_dir_for_mode(self, mode: str | None = None) -> Path:
        normalized_mode = str(mode or self.config.mode or "paper").strip().lower() or "paper"
        return self.config.log_root / normalized_mode

    def run_lock_path(self) -> Path:
        return self.run_lock_path_for_mode(self.config.mode)

    def run_lock_path_for_mode(self, mode: str | None = None) -> Path:
        return self.run_dir_for_mode(mode) / "bithumb-bot.lock"

    def pid_path(self) -> Path:
        return self.run_dir() / "bithumb-bot.pid"

    def runtime_state_path(self) -> Path:
        return self.run_dir() / "runtime_state.json"

    def primary_db_path(self) -> Path:
        return self.data_dir() / "trades" / f"{self.config.mode}.sqlite"

    def primary_db_path_for_mode(self, mode: str | None = None) -> Path:
        normalized_mode = str(mode or self.config.mode or "paper").strip().lower() or "paper"
        return self.data_dir_for_mode(normalized_mode) / "trades" / f"{normalized_mode}.sqlite"

    def raw_path(self, topic: str, day: str | None = None, ext: str = "jsonl") -> Path:
        d = self._day_or_today(day)
        return self.data_dir() / "raw" / topic / f"{topic}_{d}.{ext}"

    def derived_path(self, topic: str, day: str | None = None, ext: str = "jsonl") -> Path:
        d = self._day_or_today(day)
        return self.data_dir() / "derived" / topic / f"{topic}_{d}.{ext}"

    def trade_data_path(self, topic: str, day: str | None = None, ext: str = "jsonl") -> Path:
        d = self._day_or_today(day)
        return self.data_dir() / "trades" / topic / f"{topic}_{d}.{ext}"

    def report_path(self, topic: str, day: str | None = None, ext: str = "json") -> Path:
        d = self._day_or_today(day)
        return self.data_dir() / "reports" / topic / f"{topic}_{d}.{ext}"

    def log_path(self, kind: str, day: str | None = None, ext: str = "log") -> Path:
        normalized_kind = str(kind or "").strip().lower()
        if normalized_kind not in ALLOWED_LOG_KINDS:
            allowed = ", ".join(sorted(ALLOWED_LOG_KINDS))
            raise PathPolicyError(f"invalid log kind={kind!r}; allowed values: {allowed}")
        d = self._day_or_today(day)
        return self.log_dir() / normalized_kind / f"{normalized_kind}_{d}.{ext}"

    def app_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("app", day=day, ext=ext)

    def strategy_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("strategy", day=day, ext=ext)

    def orders_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("orders", day=day, ext=ext)

    def fills_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("fills", day=day, ext=ext)

    def error_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("errors", day=day, ext=ext)

    def audit_log_path(self, day: str | None = None, ext: str = "log") -> Path:
        return self.log_path("audit", day=day, ext=ext)

    def orders_artifact_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.trade_data_path("orders", day=day, ext=ext)

    def fills_artifact_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.trade_data_path("fills", day=day, ext=ext)

    def balance_snapshot_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.trade_data_path("balance_snapshots", day=day, ext=ext)

    def portfolio_snapshot_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.trade_data_path("portfolio_snapshots", day=day, ext=ext)

    def reconcile_event_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.trade_data_path("reconcile_events", day=day, ext=ext)

    def ops_report_path(self, day: str | None = None, ext: str = "json") -> Path:
        return self.report_path("ops_report", day=day, ext=ext)

    def strategy_validation_report_path(self, day: str | None = None, ext: str = "json") -> Path:
        return self.report_path("strategy_validation", day=day, ext=ext)

    def fee_diagnostics_report_path(self, day: str | None = None, ext: str = "json") -> Path:
        return self.report_path("fee_diagnostics", day=day, ext=ext)

    def recovery_report_path(self, day: str | None = None, ext: str = "json") -> Path:
        return self.report_path("recovery_report", day=day, ext=ext)

    def cash_drift_report_path(self, day: str | None = None, ext: str = "json") -> Path:
        return self.report_path("cash_drift_report", day=day, ext=ext)

    def feature_snapshot_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.derived_path("feature_snapshot", day=day, ext=ext)

    def signal_trace_path(self, day: str | None = None, ext: str = "jsonl") -> Path:
        return self.derived_path("signal_trace", day=day, ext=ext)

    def backup_db_path(self, timestamp: str | None = None) -> Path:
        ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return self.config.backup_root / self.config.mode / "db" / f"{self.config.mode}.sqlite.{ts}.sqlite"

    def ensure_parent_dir(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)


def validate_runtime_root_separation(config: PathConfig) -> None:
    roots: list[tuple[str, Path]] = [
        ("ENV_ROOT", config.env_root),
        ("RUN_ROOT", config.run_root),
        ("DATA_ROOT", config.data_root),
        ("LOG_ROOT", config.log_root),
        ("BACKUP_ROOT", config.backup_root),
    ]
    if config.archive_root is not None:
        roots.append(("ARCHIVE_ROOT", config.archive_root))

    resolved_roots = [(name, path.resolve()) for name, path in roots]
    for index, (left_name, left_path) in enumerate(resolved_roots):
        for right_name, right_path in resolved_roots[index + 1 :]:
            if left_path == right_path or PathManager._is_within(left_path, right_path) or PathManager._is_within(right_path, left_path):
                raise PathPolicyError(
                    "runtime roots must not overlap or share parent/child paths when MODE=live; "
                    f"{left_name}={left_path} {right_name}={right_path}"
                )


def resolve_managed_path(kind: str, manager: PathManager) -> Path:
    normalized = str(kind or "").strip().lower()
    mapping = {
        "run-dir": manager.run_dir(),
        "run-lock": manager.run_lock_path(),
        "pid": manager.pid_path(),
        "runtime-state": manager.runtime_state_path(),
        "data-dir": manager.data_dir(),
        "primary-db": manager.primary_db_path(),
        "reports-ops-dir": manager.data_dir() / "reports" / "ops",
        "reports-dir": manager.data_dir() / "reports",
        "trades-dir": manager.data_dir() / "trades",
        "derived-dir": manager.data_dir() / "derived",
        "raw-dir": manager.data_dir() / "raw",
        "backup-mode-dir": manager.config.backup_root / manager.config.mode,
        "backup-db-dir": manager.config.backup_root / manager.config.mode / "db",
        "backup-snapshots-dir": manager.config.backup_root / manager.config.mode / "snapshots",
        "log-dir": manager.log_dir(),
        "log-app-dir": manager.log_dir() / "app",
        "log-strategy-dir": manager.log_dir() / "strategy",
        "log-orders-dir": manager.log_dir() / "orders",
        "log-fills-dir": manager.log_dir() / "fills",
        "log-errors-dir": manager.log_dir() / "errors",
        "log-audit-dir": manager.log_dir() / "audit",
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        allowed = ", ".join(sorted(mapping.keys()))
        raise PathPolicyError(f"invalid kind={kind!r}; allowed values: {allowed}") from exc


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Print PathManager-managed paths")
    p.add_argument("--project-root", default=".", help="repository root directory (default: .)")
    p.add_argument("--mode", choices=sorted(ALLOWED_MODES), help="override MODE for this command")
    p.add_argument(
        "--kind",
        required=True,
        help=(
            "path kind to print: run-dir, run-lock, pid, runtime-state, data-dir, "
            "primary-db, reports-ops-dir, reports-dir, trades-dir, derived-dir, raw-dir, "
            "backup-mode-dir, backup-db-dir, backup-snapshots-dir, log-dir, "
            "log-app-dir, log-strategy-dir, log-orders-dir, log-fills-dir, "
            "log-errors-dir, log-audit-dir"
        ),
    )
    return p


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.mode:
        os.environ["MODE"] = args.mode
    project_root = Path(args.project_root).expanduser().resolve()
    manager = PathManager.from_env(project_root)
    print(resolve_managed_path(args.kind, manager))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
