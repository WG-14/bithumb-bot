from __future__ import annotations

import os
import multiprocessing
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from bithumb_bot import config
import bithumb_bot.run_lock as run_lock
from bithumb_bot.run_lock import (
    STALE_LOCK_MAX_AGE_SECONDS,
    RunLockError,
    acquire_run_lock,
    read_run_lock_status,
)


def _set_runtime_env(monkeypatch: pytest.MonkeyPatch, root: Path, mode: str) -> None:
    monkeypatch.setenv("MODE", mode)
    monkeypatch.setenv("ENV_ROOT", str((root / "env").resolve()))
    monkeypatch.setenv("RUN_ROOT", str((root / "run").resolve()))
    monkeypatch.setenv("DATA_ROOT", str((root / "data").resolve()))
    monkeypatch.setenv("LOG_ROOT", str((root / "logs").resolve()))
    monkeypatch.setenv("BACKUP_ROOT", str((root / "backup").resolve()))
    if mode == "live":
        monkeypatch.setenv("DB_PATH", str((root / "data" / "live" / "trades" / "live.sqlite").resolve()))


def _hold_lock_worker(lock_path: str, ready, release, result_queue) -> None:
    try:
        with acquire_run_lock(Path(lock_path)):
            ready.set()
            result_queue.put(("held", lock_path))
            release.wait(10)
    except Exception as exc:  # pragma: no cover - exercised via multiprocessing
        result_queue.put(("failed", f"{type(exc).__name__}: {exc}"))


def _hold_lock_with_stale_metadata_worker(lock_path: str, ready, release, result_queue) -> None:
    try:
        with acquire_run_lock(Path(lock_path)):
            stale_path = Path(lock_path)
            stale_path.write_text(
                "pid=999999 host=ghost created_at=2020-01-01T00:00:00+00:00\n",
                encoding="utf-8",
            )
            stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
            os.utime(stale_path, (stale_mtime, stale_mtime))
            ready.set()
            result_queue.put(("held", lock_path))
            release.wait(10)
    except Exception as exc:  # pragma: no cover - exercised via multiprocessing
        result_queue.put(("failed", f"{type(exc).__name__}: {exc}"))


def _attempt_lock_worker(lock_path: str, result_queue) -> None:
    try:
        with acquire_run_lock(Path(lock_path)):
            result_queue.put(("acquired", lock_path))
    except Exception as exc:  # pragma: no cover - exercised via multiprocessing
        result_queue.put(("failed", f"{type(exc).__name__}: {exc}"))


def _spawn_process(ctx, target, *args):
    proc = ctx.Process(target=target, args=args)
    proc.start()
    return proc


def test_default_lock_path_is_mode_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_LOCK_PATH", raising=False)
    monkeypatch.setenv("MODE", "paper")
    assert "/paper/" in str(run_lock._default_lock_path()).replace("\\", "/")

    monkeypatch.setenv("MODE", "live")
    assert "/live/" in str(run_lock._default_lock_path()).replace("\\", "/")


def test_default_lock_path_prefers_run_lock_path_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    lock_path = (tmp_path / "run" / "live" / "custom-run.lock").resolve()
    monkeypatch.setenv("RUN_LOCK_PATH", str(lock_path))
    monkeypatch.setenv("MODE", "live")

    assert run_lock._default_lock_path() == lock_path


def test_default_lock_path_supports_dryrun_mode_scoping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_LOCK_PATH", raising=False)
    monkeypatch.setenv("MODE", "dryrun")

    dryrun_lock = run_lock._default_lock_path()

    assert "/dryrun/" in str(dryrun_lock).replace("\\", "/")
    assert dryrun_lock.name == "bithumb-bot.lock"


def test_default_lock_path_rejects_live_relative_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUN_LOCK_PATH", "run/live/custom-run.lock")
    monkeypatch.setenv("MODE", "live")

    with pytest.raises(ValueError, match="absolute path"):
        config.resolve_run_lock_path("run/live/custom-run.lock", mode="live")


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


def test_same_runtime_storage_second_process_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_runtime_env(monkeypatch, tmp_path, "paper")
    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    result_queue = ctx.Queue()
    lock_path = str((tmp_path / "run" / "paper" / "bithumb-bot.lock").resolve())

    first = _spawn_process(ctx, _hold_lock_worker, lock_path, ready, release, result_queue)
    try:
        assert ready.wait(timeout=10) is True

        contender_queue = ctx.Queue()
        second = _spawn_process(ctx, _attempt_lock_worker, lock_path, contender_queue)
        try:
            second_result = contender_queue.get(timeout=10)
        finally:
            second.join(timeout=5)
            if second.is_alive():
                second.terminate()
                second.join(timeout=5)

        assert second_result[0] == "failed"
        assert "single-instance lock acquisition failed" in second_result[1]
        assert "runtime storage" in second_result[1]
    finally:
        release.set()
        first.join(timeout=5)
        if first.is_alive():
            first.terminate()
            first.join(timeout=5)


def test_different_mode_runtime_storages_do_not_conflict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch, tmp_path, "paper")
    ctx = multiprocessing.get_context("spawn")

    paper_ready = ctx.Event()
    paper_release = ctx.Event()
    paper_result = ctx.Queue()
    paper_lock_path = str((tmp_path / "run" / "paper" / "bithumb-bot.lock").resolve())
    paper_proc = _spawn_process(ctx, _hold_lock_worker, paper_lock_path, paper_ready, paper_release, paper_result)

    live_ready = ctx.Event()
    live_release = ctx.Event()
    live_result = ctx.Queue()
    live_lock_path = str((tmp_path / "run" / "live" / "bithumb-bot.lock").resolve())
    live_proc = _spawn_process(ctx, _hold_lock_worker, live_lock_path, live_ready, live_release, live_result)
    try:
        assert paper_ready.wait(timeout=10) is True
        assert live_ready.wait(timeout=10) is True

        assert paper_result.get(timeout=1)[0] == "held"
        assert live_result.get(timeout=1)[0] == "held"
    finally:
        paper_release.set()
        live_release.set()
        paper_proc.join(timeout=5)
        live_proc.join(timeout=5)
        if paper_proc.is_alive():
            paper_proc.terminate()
            paper_proc.join(timeout=5)
        if live_proc.is_alive():
            live_proc.terminate()
            live_proc.join(timeout=5)


def test_stale_lock_is_reclaimed_when_not_actively_held(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("999999\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        pass

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
        pass

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


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Windows lock metadata collision is platform-specific")
def test_collision_message_flags_stale_metadata_when_lock_is_still_held(
    tmp_path: Path,
) -> None:
    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    result_queue = ctx.Queue()
    lock_path = str((tmp_path / "run.lock").resolve())

    holder = _spawn_process(
        ctx,
        _hold_lock_with_stale_metadata_worker,
        lock_path,
        ready,
        release,
        result_queue,
    )
    try:
        assert ready.wait(timeout=10) is True

        contender_queue = ctx.Queue()
        contender = _spawn_process(ctx, _attempt_lock_worker, lock_path, contender_queue)
        try:
            result = contender_queue.get(timeout=10)
        finally:
            contender.join(timeout=5)
            if contender.is_alive():
                contender.terminate()
                contender.join(timeout=5)

        assert result[0] == "failed"
        assert "owner_pid=999999" in result[1]
        assert "reclaim_possible=maybe" in result[1]
    finally:
        release.set()
        holder.join(timeout=5)
        if holder.is_alive():
            holder.terminate()
            holder.join(timeout=5)


def test_abnormal_termination_stale_lock_file_is_reclaimed(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    child_code = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from bithumb_bot.run_lock import acquire_run_lock

        lock_path = Path(sys.argv[1])
        lock = acquire_run_lock(lock_path)
        lock.__enter__()
        print("LOCKED", flush=True)
        time.sleep(10)
        """
    )

    proc = subprocess.Popen(
        [sys.executable, "-c", child_code, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "LOCKED"
    proc.kill()
    proc.communicate(timeout=10)
    assert proc.returncode not in (0, None)

    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        pass

    owner_text = lock_path.read_text(encoding="utf-8").strip()
    assert f"pid={os.getpid()}" in owner_text


def test_read_run_lock_status_reports_live_owner(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        pass

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
