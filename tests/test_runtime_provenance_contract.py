from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.config import validate_runtime_code_provenance_for_live_real_order


def _cfg(**overrides: object) -> SimpleNamespace:
    payload = {"MODE": "live", "LIVE_REAL_ORDER_ARMED": True, "LIVE_DRY_RUN": False}
    payload.update(overrides)
    return SimpleNamespace(**payload)


def test_live_real_order_rejects_dirty_tree_without_diff_artifact() -> None:
    result = validate_runtime_code_provenance_for_live_real_order(
        _cfg(),
        code_provenance={"commit_sha": "abc", "working_tree_dirty": True},
    )

    assert result["ok"] is False
    assert result["reason_code"] == "DIRTY_RUNTIME_PROVENANCE_MISSING_DIFF_ARTIFACT"


def test_live_real_order_accepts_clean_tree() -> None:
    result = validate_runtime_code_provenance_for_live_real_order(
        _cfg(),
        code_provenance={"commit_sha": "abc", "working_tree_dirty": False},
    )

    assert result["ok"] is True


def test_dirty_tree_requires_diff_hash_in_contract() -> None:
    result = validate_runtime_code_provenance_for_live_real_order(
        _cfg(),
        code_provenance={
            "commit_sha": "abc",
            "working_tree_dirty": True,
            "runtime_git_diff_hash": "sha256:" + "d" * 64,
            "runtime_git_diff_artifact_path": "/runtime/diff.patch",
            "source_archive_hash": "sha256:" + "e" * 64,
            "operator_dirty_runtime_ack": "ack",
        },
    )

    assert result["ok"] is True
    assert result["runtime_git_diff_hash"].startswith("sha256:")
