from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
from pathlib import Path
import logging
import os
import socket
import time
from typing import Iterator

from .config import default_run_lock_path, resolve_run_lock_path

try:
    import fcntl  # type: ignore[attr-defined]
except ModuleNotFoundError:
    fcntl = None

try:
    import msvcrt  # type: ignore[attr-defined]
except ModuleNotFoundError:
    msvcrt = None


class RunLockError(RuntimeError):
    pass


LOGGER = logging.getLogger(__name__)
STALE_LOCK_MAX_AGE_SECONDS = 60 * 10
_LOCK_BYTES = 1


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
    owner_text: str | None
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
        raw_owner_text = self.owner_text or "unknown"
        return (
            f"path={self.lock_path} owner_pid={owner_text} host={host_text} "
            f"created_at={created_text} age={age_text} "
            f"stale_candidate={stale_text} status={self.owner_state_text} owner_text={raw_owner_text}"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "lock_path": str(self.lock_path),
            "owner_pid": self.owner_pid,
            "owner_hostname": self.owner_hostname,
            "created_at": self.created_at,
            "age_seconds": self.age_seconds,
            "owner_text": self.owner_text,
            "owner_state_text": self.owner_state_text,
            "is_stale_candidate": self.is_stale_candidate,
            "human_text": self.to_human_text(),
        }


def _default_lock_path() -> Path:
    configured_path = os.getenv("RUN_LOCK_PATH")
    if configured_path:
        return Path(resolve_run_lock_path(configured_path))

    mode = (os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
    return Path(resolve_run_lock_path(default_run_lock_path(mode)))


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return False
    except PermissionError:
        return True
    return True


def _read_fd_text(fd: int, size: int = 256) -> str:
    try:
        current_offset = os.lseek(fd, 0, os.SEEK_CUR)
    except OSError:
        current_offset = None
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, size)
        return raw.decode("utf-8", errors="ignore").strip()
    finally:
        if current_offset is not None:
            try:
                os.lseek(fd, current_offset, os.SEEK_SET)
            except OSError:
                pass


def _is_lock_conflict_error(exc: OSError) -> bool:
    winerror = getattr(exc, "winerror", None)
    if winerror in {32, 33, 36, 158}:
        return True
    return exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}


def _lock_fd_exclusive(fd: int) -> None:
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return
    if msvcrt is not None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, _LOCK_BYTES)
        return
    raise RunLockError("run lock is not supported on this platform; use WSL or Linux")


def _unlock_fd_exclusive(fd: int) -> None:
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return
    if msvcrt is not None:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, _LOCK_BYTES)
        except OSError:
            pass
        return
    raise RunLockError("run lock is not supported on this platform; use WSL or Linux")


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
        raw = _read_fd_text(fd, 256)
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
            owner_text=None,
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
        owner_text=state.owner_text,
        is_stale_candidate=state.is_stale_candidate,
    )


@contextmanager
def acquire_run_lock(lock_path: Path | None = None) -> Iterator[None]:
    path = lock_path or _default_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        previous_state = _read_lock_file_state(path, fd)
        try:
            _lock_fd_exclusive(fd)
        except BlockingIOError as exc:
            state = _read_lock_file_state(path, fd)
            raise RunLockError(
                "another bot run loop is already running for this runtime storage; "
                "single-instance lock acquisition failed. "
                "Current lock context: "
                f"{_format_lock_conflict_details(path, state)}"
            ) from exc
        except OSError as exc:
            if not _is_lock_conflict_error(exc):
                raise
            state = _read_lock_file_state(path, fd)
            raise RunLockError(
                "another bot run loop is already running for this runtime storage; "
                "single-instance lock acquisition failed. "
                "Current lock context: "
                f"{_format_lock_conflict_details(path, state)}"
            ) from exc

        if previous_state.is_stale_candidate:
            LOGGER.warning(
                "reclaimed stale run lock file at %s (previous_pid=%s previous_host=%s previous_created_at=%s age=%.0fs owner_text=%s); "
                "prior owner appears inactive and file lock was free",
                path,
                previous_state.pid,
                previous_state.hostname,
                previous_state.created_at,
                previous_state.age_seconds,
                previous_state.owner_text or "-",
            )

        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        owner_record = (
            f"pid={os.getpid()} host={socket.gethostname()} "
            f"created_at={datetime.now(timezone.utc).isoformat()}"
        )
        os.write(fd, owner_record.encode("utf-8"))
        os.fsync(fd)
        yield
    finally:
        try:
            _unlock_fd_exclusive(fd)
        finally:
            os.close(fd)
