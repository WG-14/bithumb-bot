from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import fcntl
import logging
import os
import tempfile
import time
from typing import Iterator


class RunLockError(RuntimeError):
    pass


LOGGER = logging.getLogger(__name__)
STALE_LOCK_MAX_AGE_SECONDS = 60 * 10


@dataclass
class _LockFileState:
    pid: int | None
    age_seconds: float | None

    @property
    def is_stale_candidate(self) -> bool:
        return (
            self.pid is not None
            and not _pid_is_running(self.pid)
            and self.age_seconds is not None
            and self.age_seconds >= STALE_LOCK_MAX_AGE_SECONDS
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


def _read_lock_file_state(path: Path, fd: int) -> _LockFileState:
    pid: int | None = None
    age_seconds: float | None = None

    try:
        raw = os.pread(fd, 256, 0).decode("utf-8", errors="ignore").strip()
        if raw:
            pid = int(raw)
    except (ValueError, OSError):
        pid = None

    try:
        stat = path.stat()
        age_seconds = max(0.0, time.time() - stat.st_mtime)
    except OSError:
        age_seconds = None

    return _LockFileState(pid=pid, age_seconds=age_seconds)


@contextmanager
def acquire_run_lock(lock_path: Path | None = None) -> Iterator[None]:
    path = lock_path or _default_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        previous_state = _read_lock_file_state(path, fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            state = _read_lock_file_state(path, fd)
            stale_hint = ""
            if state.is_stale_candidate:
                stale_hint = (
                    "; stale lock candidate detected "
                    f"(pid={state.pid}, age={state.age_seconds:.0f}s). "
                    "Auto-reclaim is only allowed when lock acquisition succeeds"
                )
            raise RunLockError(
                "another bot run loop is already running "
                f"(lock: {path}, owner_pid={state.pid}){stale_hint}"
            ) from exc

        if previous_state.is_stale_candidate:
            LOGGER.warning(
                "reclaiming stale run lock file at %s (pid=%s age=%.0fs)",
                path,
                previous_state.pid,
                previous_state.age_seconds,
            )

        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.fsync(fd)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
