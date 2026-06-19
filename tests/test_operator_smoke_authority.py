from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import ApprovedProfileError, validate_approved_profile
from bithumb_bot.execution_authority import execution_authority_from_payload, require_authority_operation
from bithumb_bot.storage_io import write_json_atomic
from bithumb_bot.operator_smoke_authority import (
    OperatorSmokeAuthorityError,
    build_operator_smoke_authority_payload,
    load_operator_smoke_authority,
    verify_operator_smoke_authority,
)


def test_smoke_authority_declares_not_promotion_evidence() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )

    assert payload["promotion_evidence"] is False
    assert payload["approved_profile_evidence"] is False
    assert payload["strategy_performance_evidence"] is False
    assert payload["promotion_grade"] is False


def test_operator_smoke_authority_not_accepted_as_approved_profile() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )

    with pytest.raises(ApprovedProfileError):
        validate_approved_profile(payload)


def test_operator_smoke_authority_not_accepted_by_profile_generate() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )

    with pytest.raises(ApprovedProfileError):
        validate_approved_profile(payload)


def test_smoke_authority_expired_blocks_smoke_buy() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    with pytest.raises(OperatorSmokeAuthorityError, match="operator_smoke_authority_expired"):
        verify_operator_smoke_authority(payload, now=datetime.now(timezone.utc), side="BUY", notional_krw=50_000)


def test_operator_smoke_authority_binds_market_db_account_and_commit(tmp_path: Path) -> None:
    db_path = tmp_path / "live.sqlite"
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        market="KRW-BTC",
        db_path=str(db_path),
        account_key="operator-key",
        code_commit_sha="abc123",
    )

    verify_operator_smoke_authority(
        payload,
        now=datetime.now(timezone.utc),
        side="BUY",
        notional_krw=50_000,
        market="KRW-BTC",
        db_path=str(db_path),
        account_key="operator-key",
        code_commit_sha="abc123",
    )
    with pytest.raises(OperatorSmokeAuthorityError, match="market_mismatch"):
        verify_operator_smoke_authority(payload, market="KRW-ETH")
    with pytest.raises(OperatorSmokeAuthorityError, match="db_path_mismatch"):
        verify_operator_smoke_authority(payload, db_path=str(tmp_path / "other.sqlite"))
    with pytest.raises(OperatorSmokeAuthorityError, match="account_mismatch"):
        verify_operator_smoke_authority(payload, account_key="other")
    with pytest.raises(OperatorSmokeAuthorityError, match="code_commit_mismatch"):
        verify_operator_smoke_authority(payload, code_commit_sha="other")


def test_operator_smoke_authority_rejects_reuse(tmp_path: Path) -> None:
    authority_path = tmp_path / "smoke-authority.json"
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        db_path=str(tmp_path / "live.sqlite"),
        account_key="operator-key",
        code_commit_sha="abc123",
    )
    write_json_atomic(authority_path, payload)

    loaded = load_operator_smoke_authority(authority_path)
    loaded.consume(
        consumed_at=datetime.now(timezone.utc),
        side="BUY",
        notional_krw=50_000,
        market="KRW-BTC",
        db_path=str(tmp_path / "live.sqlite"),
        account_key="operator-key",
        code_commit_sha="abc123",
    )

    with pytest.raises(OperatorSmokeAuthorityError, match="operator_smoke_authority_reused"):
        load_operator_smoke_authority(authority_path).verify(now=datetime.now(timezone.utc))


def test_operator_smoke_authority_rejects_hash_mismatch() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )
    payload["max_notional_krw"] = 1.0

    with pytest.raises(OperatorSmokeAuthorityError, match="operator_smoke_authority_hash_mismatch"):
        verify_operator_smoke_authority(payload, now=datetime.now(timezone.utc), notional_krw=1.0)


def test_operator_smoke_authority_rejects_promotion_evidence_true() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )
    payload["promotion_evidence"] = True

    with pytest.raises(OperatorSmokeAuthorityError, match="promotion_evidence_must_be_false"):
        verify_operator_smoke_authority(payload, now=datetime.now(timezone.utc))


def test_smoke_buy_rejects_approved_profile_as_operator_authority() -> None:
    authority = execution_authority_from_payload(
        {
            "artifact_type": "approved_profile",
            "profile_content_hash": "sha256:" + "a" * 64,
            "market": "KRW-BTC",
        }
    )

    with pytest.raises(ValueError, match="operation_not_allowed"):
        require_authority_operation(authority, "operator_smoke_buy")
