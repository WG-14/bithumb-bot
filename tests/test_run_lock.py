from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

import bithumb_bot.run_lock as run_lock
from bithumb_bot import config
from bithumb_bot.run_lock import (
    STALE_LOCK_MAX_AGE_SECONDS,
    RunLockError,
    acquire_run_lock,
    read_run_lock_status,
)


def test_default_lock_path_is_mode_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_LOCK_PATH", raising=False)
    monkeypatch.setenv("MODE", "paper")
    assert "/paper/" in str(run_lock._default_lock_path()).replace("\\", "/")

    monkeypatch.setenv("MODE", "live")
    assert "/live/" in str(run_lock._default_lock_path()).replace("\\", "/")


def test_default_lock_path_prefers_run_lock_path_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUN_LOCK_PATH", "run/live/custom-run.lock")
    monkeypatch.setenv("MODE", "live")

    assert run_lock._default_lock_path() == Path(config.resolve_run_lock_path("run/live/custom-run.lock"))


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
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text
        assert "host=" in owner_text
        assert "created_at=" in owner_text

    assert "reclaimed stale run lock file" in caplog.text


def test_stale_lock_with_non_pid_owner_text_is_reclaimed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("legacy-owner\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text
        assert "host=" in owner_text
        assert "created_at=" in owner_text

    assert "reclaimed stale run lock file" in caplog.text


def test_error_message_includes_lock_owner_context_on_collision(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    message = str(exc.value)
    assert f"lock={lock_path}" in message
    assert "owner_pid=" in message
    assert "owner_host=" in message
    assert "owner_created_at=" in message
    assert "lock_age=" in message
    assert "reclaim_possible=" in message


def test_collision_message_flags_stale_metadata_when_lock_is_still_held(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text(
        "pid=999999 host=ghost created_at=2020-01-01T00:00:00+00:00\n",
        encoding="utf-8",
    )
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    fd = os.open(lock_path, os.O_RDWR)
    try:
        import fcntl  # type: ignore[attr-defined]

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass
    finally:
        os.close(fd)

    message = str(exc.value)
    assert "owner_pid=999999" in message
    assert "reclaim_possible=maybe" in message


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
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text


def test_read_run_lock_status_reports_live_owner(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        status = read_run_lock_status(lock_path)

    assert status.lock_path == lock_path
    assert status.owner_pid == os.getpid()
    assert status.owner_hostname is not None
    assert status.created_at is not None
    assert status.age_seconds is not None
    assert status.is_stale_candidate is False
    assert "live owner" in status.to_human_text()
    assert "host=" in status.to_human_text()
    assert "created_at=" in status.to_human_text()


def test_read_run_lock_status_reports_stale_candidate(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("999999\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    status = read_run_lock_status(lock_path)

    assert status.lock_path == lock_path
    assert status.owner_pid == 999999
    assert status.owner_hostname is None
    assert status.created_at is None
    assert status.age_seconds is not None
    assert status.is_stale_candidate is True
    assert "stale candidate" in status.to_human_text()


def test_read_run_lock_status_parses_rich_owner_record(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text(
        "pid=1234 host=test-host created_at=2026-01-01T00:00:00+00:00\n",
        encoding="utf-8",
    )

    status = read_run_lock_status(lock_path)

    assert status.owner_pid == 1234
    assert status.owner_hostname == "test-host"
    assert status.created_at == "2026-01-01T00:00:00+00:00"
