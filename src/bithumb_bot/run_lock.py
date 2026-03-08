from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import fcntl
import os
import tempfile
from typing import Iterator


class RunLockError(RuntimeError):
    pass


def _default_lock_path() -> Path:
    return Path(tempfile.gettempdir()) / "bithumb-bot-run.lock"


@contextmanager
def acquire_run_lock(lock_path: Path | None = None) -> Iterator[None]:
    path = lock_path or _default_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunLockError(
                f"another bot run loop is already running (lock: {path})"
            ) from exc

        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.fsync(fd)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
