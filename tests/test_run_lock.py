from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.run_lock import RunLockError, acquire_run_lock


def test_second_acquire_fails_while_first_is_held(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    assert "already running" in str(exc.value)


def test_lock_can_be_reacquired_after_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        pass

    with acquire_run_lock(lock_path):
        pass
