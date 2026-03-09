from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from bithumb_bot.run_lock import (
    STALE_LOCK_MAX_AGE_SECONDS,
    RunLockError,
    acquire_run_lock,
)


def test_second_acquire_fails_while_first_is_held(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    message = str(exc.value)
    assert "already running" in message
    assert "owner_pid=" in message


def test_lock_can_be_reacquired_after_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        pass

    with acquire_run_lock(lock_path):
        pass


def test_stale_lock_is_reclaimed_when_not_actively_held(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("999999\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())

    assert "reclaiming stale run lock file" in caplog.text


def test_stale_lock_with_non_pid_owner_text_is_reclaimed(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("legacy-owner\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())

    assert "reclaiming stale run lock file" in caplog.text

def test_error_message_includes_lock_owner_context_on_collision(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    message = str(exc.value)
    assert f"lock: {lock_path}" in message
    assert "owner_pid=" in message


def test_abnormal_termination_stale_lock_file_is_reclaimed(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    child_code = textwrap.dedent(
        """
        import os
        import signal
        import sys
        from pathlib import Path

        from bithumb_bot.run_lock import acquire_run_lock

        lock_path = Path(sys.argv[1])
        with acquire_run_lock(lock_path):
            print("LOCKED", flush=True)
            os.kill(os.getpid(), signal.SIGKILL)
        """
    )

    proc = subprocess.Popen(
        [sys.executable, "-c", child_code, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
    )
    stdout, _stderr = proc.communicate(timeout=10)

    assert "LOCKED" in stdout
    assert proc.returncode not in (0, None)

    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())
