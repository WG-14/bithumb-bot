from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import logging
import os
import socket
import tempfile
import time
from typing import Iterator

try:
    import fcntl  # type: ignore[attr-defined]
except ModuleNotFoundError:
    fcntl = None


class RunLockError(RuntimeError):
    pass


LOGGER = logging.getLogger(__name__)
STALE_LOCK_MAX_AGE_SECONDS = 60 * 10


@dataclass
class _LockFileState:
    pid: int | None
    hostname: str | None
    created_at: str | None
    age_seconds: float | None
    owner_text: str | None

    @property
    def is_stale_candidate(self) -> bool:
        return (
            (self.pid is None or not _pid_is_running(self.pid))
            and self.age_seconds is not None
            and self.age_seconds >= STALE_LOCK_MAX_AGE_SECONDS
        )


@dataclass(frozen=True)
class RunLockStatus:
    lock_path: Path
    owner_pid: int | None
    owner_hostname: str | None
    created_at: str | None
    age_seconds: float | None
    is_stale_candidate: bool

    @property
    def owner_state_text(self) -> str:
        if self.owner_pid is None:
            return "stale candidate" if self.is_stale_candidate else "unknown owner"
        if _pid_is_running(self.owner_pid):
            return "live owner"
        return "stale candidate" if self.is_stale_candidate else "dead owner"

    def to_human_text(self) -> str:
        owner_text = str(self.owner_pid) if self.owner_pid is not None else "unknown"
        host_text = self.owner_hostname or "unknown"
        created_text = self.created_at or "unknown"
        age_text = f"{self.age_seconds:.1f}s" if self.age_seconds is not None else "unknown"
        stale_text = "yes" if self.is_stale_candidate else "no"
        return (
            f"path={self.lock_path} owner_pid={owner_text} host={host_text} "
            f"created_at={created_text} age={age_text} "
            f"stale_candidate={stale_text} ({self.owner_state_text})"
        )


def _default_lock_path() -> Path:
    return Path(tempfile.gettempdir()) / "bithumb-bot-run.lock"


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _parse_lock_owner_text(raw: str) -> tuple[int | None, str | None, str | None]:
    value = raw.strip()
    if not value:
        return None, None, None

    # Backward-compatible legacy format: "<pid>".
    try:
        return int(value), None, None
    except ValueError:
        pass

    fields: dict[str, str] = {}
    for token in value.split():
        if "=" not in token:
            continue
        key, field_value = token.split("=", 1)
        fields[key] = field_value

    pid: int | None = None
    pid_raw = fields.get("pid")
    if pid_raw:
        try:
            pid = int(pid_raw)
        except ValueError:
            pid = None

    return pid, fields.get("host"), fields.get("created_at")


def _read_lock_file_state(path: Path, fd: int) -> _LockFileState:
    pid: int | None = None
    hostname: str | None = None
    created_at: str | None = None
    age_seconds: float | None = None
    owner_text: str | None = None

    try:
        raw = os.pread(fd, 256, 0).decode("utf-8", errors="ignore").strip()
        if raw:
            owner_text = raw
            pid, hostname, created_at = _parse_lock_owner_text(raw)
    except OSError:
        pid = None

    try:
        stat = path.stat()
        age_seconds = max(0.0, time.time() - stat.st_mtime)
    except OSError:
        age_seconds = None

    return _LockFileState(
        pid=pid,
        hostname=hostname,
        created_at=created_at,
        age_seconds=age_seconds,
        owner_text=owner_text,
    )


def _format_lock_conflict_details(path: Path, state: _LockFileState) -> str:
    owner_pid = state.pid if state.pid is not None else "unknown"
    owner_host = state.hostname or "unknown"
    owner_created_at = state.created_at or "unknown"
    owner_age = f"{state.age_seconds:.0f}s" if state.age_seconds is not None else "unknown"
    reclaim_hint = (
        "maybe (metadata looks stale; reclaim can only happen after the current holder exits)"
        if state.is_stale_candidate
        else "no (lock is actively held by another process)"
    )
    return (
        f"lock={path} owner_pid={owner_pid} owner_host={owner_host} "
        f"owner_created_at={owner_created_at} lock_age={owner_age} "
        f"reclaim_possible={reclaim_hint}"
    )


def read_run_lock_status(lock_path: Path | None = None) -> RunLockStatus:
    path = lock_path or _default_lock_path()

    if not path.exists():
        return RunLockStatus(
            lock_path=path,
            owner_pid=None,
            owner_hostname=None,
            created_at=None,
            age_seconds=None,
            is_stale_candidate=False,
        )

    fd = os.open(path, os.O_RDONLY)
    try:
        state = _read_lock_file_state(path, fd)
    finally:
        os.close(fd)

    return RunLockStatus(
        lock_path=path,
        owner_pid=state.pid,
        owner_hostname=state.hostname,
        created_at=state.created_at,
        age_seconds=state.age_seconds,
        is_stale_candidate=state.is_stale_candidate,
    )


@contextmanager
def acquire_run_lock(lock_path: Path | None = None) -> Iterator[None]:
    if fcntl is None:
        raise RunLockError("run lock is not supported on this platform; use WSL or Linux")

    path = lock_path or _default_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        previous_state = _read_lock_file_state(path, fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            state = _read_lock_file_state(path, fd)
            raise RunLockError(
                "another bot run loop is already running; lock acquisition failed. "
                "Current lock context: "
                f"{_format_lock_conflict_details(path, state)}"
            ) from exc

        if previous_state.is_stale_candidate:
            LOGGER.warning(
                "reclaimed stale run lock file at %s (previous_pid=%s previous_host=%s previous_created_at=%s age=%.0fs); "
                "prior owner appears inactive and file lock was free",
                path,
                previous_state.pid,
                previous_state.hostname,
                previous_state.created_at,
                previous_state.age_seconds,
            )

        os.ftruncate(fd, 0)
        owner_record = (
            f"pid={os.getpid()} host={socket.gethostname()} "
            f"created_at={datetime.now(timezone.utc).isoformat()}"
        )
        os.write(fd, owner_record.encode("utf-8"))
        os.fsync(fd)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
