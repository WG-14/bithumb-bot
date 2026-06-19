from __future__ import annotations

import inspect
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot import operator_commands
from bithumb_bot.cli.context import AppContext
from bithumb_bot.cli.dispatch import dispatch
from bithumb_bot.cli.parser import build_parser
from bithumb_bot.cli.registry import command_registry
from bithumb_bot.approved_profile import ApprovedProfileError, validate_approved_profile
from bithumb_bot.db_core import ensure_db
from bithumb_bot.storage_io import write_json_atomic
from bithumb_bot.operator_smoke import (
    OPERATOR_SMOKE_STRATEGY_NAME,
    SMOKE_BUY_CONFIRMATION_TOKEN,
    OperatorSmokeError,
    build_smoke_buy_plan,
    execute_smoke_buy,
    validate_smoke_buy_request,
)
from bithumb_bot.operator_smoke_authority import (
    build_operator_smoke_authority_payload,
)
from bithumb_bot.runtime.daily_participation_claims import (
    DailyParticipationClaimKey,
    ensure_daily_participation_claims_schema,
    pending_daily_participation_claim_count,
)
import bithumb_bot.operator_smoke_preflight as smoke_preflight


def _live_settings(db_path: Path, **overrides):
    base = replace(
        __import__("bithumb_bot.config", fromlist=["settings"]).settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        KILL_SWITCH=False,
        DB_PATH=str(db_path),
        PAIR="KRW-BTC",
        BITHUMB_API_KEY="operator-key",
        BITHUMB_API_SECRET="x" * 64,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )
    return replace(base, **overrides)


def _live_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    for key, dirname in {
        "ENV_ROOT": "envroot",
        "RUN_ROOT": "runroot",
        "DATA_ROOT": "dataroot",
        "LOG_ROOT": "logroot",
        "BACKUP_ROOT": "backuproot",
    }.items():
        monkeypatch.setenv(key, str(tmp_path / dirname))
    db_path = tmp_path / "dataroot" / "live" / "trades" / "live.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    return db_path


class _SmokeBroker:
    def get_open_orders(self):
        return []

    def get_balance(self):
        return SimpleNamespace(
            cash_available=100_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )


class _FakeSmokeAuthority:
    def __init__(self) -> None:
        self.payload = build_operator_smoke_authority_payload(
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
            db_path="/tmp/fake-smoke.sqlite",
            account_key="operator-key",
            code_commit_sha="abc123",
        )
        self.verified = False
        self.consumed = False

    def verify(self, **_kwargs) -> None:
        self.verified = True

    def consume(self, **_kwargs) -> None:
        self.consumed = True


def _readiness_snapshot(
    *,
    broker_qty_known: bool = True,
    broker_qty: float = 0.0,
    portfolio_qty: float = 0.0,
    projected_qty: float = 0.0,
    projection_converged: bool = True,
    recovery_required_count: int = 0,
    fee_pending_count: int = 0,
    active_fee_accounting_blocker: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        broker_position_evidence={
            "broker_qty_known": broker_qty_known,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
            "missing_evidence_fields": [] if broker_qty_known else ["broker_asset_qty"],
        },
        projection_convergence={
            "converged": projection_converged,
            "portfolio_qty": portfolio_qty,
            "projected_total_qty": projected_qty,
            "reason": "converged" if projection_converged else "projection_non_converged",
        },
        recovery_required_count=recovery_required_count,
        fee_pending_count=fee_pending_count,
        active_fee_accounting_blocker=active_fee_accounting_blocker,
    )


def _authority_path(tmp_path: Path, *, db_path: Path, commit: str = "abc123", market: str = "KRW-BTC") -> Path:
    path = tmp_path / f"smoke-authority-{market}.json"
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        market=market,
        db_path=str(db_path),
        account_key="operator-key",
        code_commit_sha=commit,
    )
    write_json_atomic(path, payload)
    return path


def test_smoke_buy_requires_live_mode() -> None:
    with pytest.raises(OperatorSmokeError, match="smoke_buy_requires_live_mode"):
        validate_smoke_buy_request(
            mode="paper",
            live_real_order_armed=True,
            kill_switch=False,
            krw=50_000,
            confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
        )


def test_smoke_buy_requires_real_order_armed() -> None:
    with pytest.raises(OperatorSmokeError, match="smoke_buy_requires_live_real_order_armed"):
        validate_smoke_buy_request(
            mode="live",
            live_real_order_armed=False,
            kill_switch=False,
            krw=50_000,
            confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
        )


def test_smoke_buy_requires_confirmation_token() -> None:
    with pytest.raises(OperatorSmokeError, match="smoke_buy_requires_confirmation_token"):
        validate_smoke_buy_request(
            mode="live",
            live_real_order_armed=True,
            kill_switch=False,
            krw=50_000,
            confirm="",
        )


def test_smoke_buy_caps_krw_at_50000() -> None:
    with pytest.raises(OperatorSmokeError, match="smoke_buy_krw_above_50000_cap"):
        validate_smoke_buy_request(
            mode="live",
            live_real_order_armed=True,
            kill_switch=False,
            krw=50_001,
            confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
        )


def test_smoke_buy_uses_operator_execution_smoke_identity() -> None:
    plan = build_smoke_buy_plan(market="KRW-BTC", krw=50_000, run_id="run123")

    assert plan.strategy_name == "operator_execution_smoke"
    assert plan.strategy_instance_id == "operator_execution_smoke:run123"
    assert plan.origin == "operator_smoke"


def test_smoke_buy_does_not_satisfy_approved_profile_required() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=__import__("datetime").datetime(2099, 1, 1, tzinfo=__import__("datetime").timezone.utc)
    )

    with pytest.raises(ApprovedProfileError):
        validate_approved_profile(payload)


def test_smoke_buy_not_counted_as_daily_participation_event(tmp_path: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(tmp_path / "smoke.sqlite")
    conn.row_factory = sqlite3.Row
    ensure_daily_participation_claims_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_participation_claims(
            strategy_instance_id, pair, kst_day, participation_policy_hash,
            status, retry_allowed, created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, 0, 1, 1)
        """,
        (f"{OPERATOR_SMOKE_STRATEGY_NAME}:run123", "KRW-BTC", "2026-06-19", "sha256:policy", "submitted"),
    )
    conn.commit()

    count = pending_daily_participation_claim_count(
        conn,
        key=DailyParticipationClaimKey(
            strategy_instance_id="daily_participation_sma:KRW-BTC:1m",
            pair="KRW-BTC",
            kst_day="2026-06-19",
            participation_policy_hash="sha256:policy",
        ),
    )

    assert count == 0


def test_smoke_buy_cli_handler_does_not_call_broker_create_order_directly() -> None:
    source = inspect.getsource(operator_commands.cmd_smoke_buy)

    assert "create_order" not in source


def test_execute_smoke_buy_does_not_call_validate_live_mode_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    captured: dict[str, object] = {}
    cfg = _live_settings(db_path)
    monkeypatch.setattr(smoke, "settings", cfg)
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(
        "bithumb_bot.config.validate_live_mode_preflight",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("strategy preflight called")),
    )
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=lambda: {"min_notional_krw": 5_000, "min_qty": 0.0, "qty_step": 0.0}),
    )
    monkeypatch.setattr(
        smoke,
        "build_live_submit_plan",
        lambda **kwargs: SimpleNamespace(
            intent=SimpleNamespace(market=kwargs["market"]),
            submitted_qty=kwargs["qty"],
            rules=kwargs["effective_rules"],
            submit_qty_authority="unit",
            exchange_order_type="market",
            internal_lot_qty=kwargs["qty"],
            qty_split=SimpleNamespace(lot_count=1),
        ),
    )

    def _submit(**kwargs):
        captured["request"] = kwargs["request"]
        return object()

    monkeypatch.setattr(smoke, "submit_live_order_and_confirm", _submit)
    try:
        execute_smoke_buy(
            conn=conn,
            broker=_SmokeBroker(),
            krw=50_000,
            market="KRW-BTC",
            confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
            authority_path=str(_authority_path(tmp_path, db_path=db_path)),
            reference_price=100_000_000.0,
            now_ms=1_800_000_000_000,
        )
    finally:
        conn.close()

    assert captured["request"].strategy_name == "operator_execution_smoke"


def test_smoke_buy_requires_authority_for_real_submit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    try:
        with pytest.raises(OperatorSmokeError, match="smoke_buy_requires_authority_path"):
            execute_smoke_buy(
                conn=conn,
                broker=_SmokeBroker(),
                krw=50_000,
                market="KRW-BTC",
                confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
                reference_price=100_000_000.0,
            )
    finally:
        conn.close()


def test_smoke_buy_rejects_market_mismatch_with_settings_pair(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path, PAIR="KRW-BTC"))
    try:
        with pytest.raises(OperatorSmokeError, match="smoke_buy_market_mismatch_with_settings_pair"):
            execute_smoke_buy(
                conn=conn,
                broker=_SmokeBroker(),
                krw=50_000,
                market="KRW-ETH",
                confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
                authority_path=str(_authority_path(tmp_path, db_path=db_path, market="KRW-ETH")),
                reference_price=1_000_000.0,
            )
    finally:
        conn.close()


def test_smoke_buy_without_reference_price_does_not_create_one_btc_intent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    try:
        with pytest.raises(OperatorSmokeError, match="smoke_buy_reference_price_required"):
            execute_smoke_buy(
                conn=conn,
                broker=_SmokeBroker(),
                krw=50_000,
                market="KRW-BTC",
                confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
                authority_path=str(_authority_path(tmp_path, db_path=db_path)),
                reference_price=None,
            )
    finally:
        conn.close()


def test_smoke_buy_dispatch_to_submit_without_approved_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    cfg = _live_settings(db_path, APPROVED_STRATEGY_PROFILE_PATH="")
    captured: dict[str, object] = {}
    monkeypatch.setattr(smoke, "settings", cfg)
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(operator_commands, "ensure_db", lambda: conn)
    monkeypatch.setattr(operator_commands, "build_broker_with_auth_diagnostics", lambda: _SmokeBroker())
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=lambda: {"min_notional_krw": 5_000, "min_qty": 0.0, "qty_step": 0.0}),
    )
    monkeypatch.setattr(
        smoke,
        "build_live_submit_plan",
        lambda **kwargs: SimpleNamespace(
            intent=SimpleNamespace(market=kwargs["market"]),
            submitted_qty=kwargs["qty"],
            rules=kwargs["effective_rules"],
            submit_qty_authority="unit",
            exchange_order_type="market",
            internal_lot_qty=kwargs["qty"],
            qty_split=SimpleNamespace(lot_count=1),
        ),
    )

    def _submit(**kwargs):
        captured["request"] = kwargs["request"]
        return object()

    monkeypatch.setattr(smoke, "submit_live_order_and_confirm", _submit)
    registry = command_registry()
    parser = build_parser(registry)
    args = parser.parse_args(
        [
            "smoke-buy",
            "--krw",
            "50000",
            "--market",
            "KRW-BTC",
            "--confirm",
            SMOKE_BUY_CONFIRMATION_TOKEN,
            "--authority-path",
            str(_authority_path(tmp_path, db_path=db_path)),
            "--reference-price",
            "100000000",
        ]
    )

    rc = dispatch(args, AppContext(settings=cfg), registry)

    assert rc == 0
    request = captured["request"]
    assert request.strategy_name == "operator_execution_smoke"
    assert request.decision_reason == "operator_smoke"
    assert request.submit_plan.intent.market == "KRW-BTC"


def test_execute_smoke_buy_rejects_broker_local_mismatch_before_authority_consume(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    fake_authority = _FakeSmokeAuthority()
    submitted: list[object] = []
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke_preflight, "validate_market_preflight", lambda _cfg: None)
    monkeypatch.setattr(
        smoke_preflight,
        "compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_snapshot(broker_qty=0.01, portfolio_qty=0.0, projected_qty=0.0),
    )
    monkeypatch.setattr(smoke, "load_operator_smoke_authority", lambda _path: fake_authority)
    monkeypatch.setattr(smoke, "submit_live_order_and_confirm", lambda **kwargs: submitted.append(kwargs))
    try:
        with pytest.raises(Exception, match="broker_local_mismatch"):
            execute_smoke_buy(
                conn=conn,
                broker=_SmokeBroker(),
                krw=50_000,
                market="KRW-BTC",
                confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
                authority_path=str(_authority_path(tmp_path, db_path=db_path)),
                reference_price=100_000_000.0,
                now_ms=1_800_000_000_000,
            )
    finally:
        conn.close()

    assert submitted == []
    assert fake_authority.consumed is False


def test_execute_smoke_buy_allows_flat_broker_local_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    fake_authority = _FakeSmokeAuthority()
    captured: dict[str, object] = {}
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke_preflight, "validate_market_preflight", lambda _cfg: None)
    monkeypatch.setattr(
        smoke_preflight,
        "compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_snapshot(),
    )
    monkeypatch.setattr(smoke, "load_operator_smoke_authority", lambda _path: fake_authority)
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=lambda: {"min_notional_krw": 5_000, "min_qty": 0.0, "qty_step": 0.0}),
    )
    monkeypatch.setattr(
        smoke,
        "build_live_submit_plan",
        lambda **kwargs: SimpleNamespace(
            intent=SimpleNamespace(market=kwargs["market"]),
            submitted_qty=kwargs["qty"],
            rules=kwargs["effective_rules"],
            submit_qty_authority="unit",
            exchange_order_type="market",
            internal_lot_qty=kwargs["qty"],
            qty_split=SimpleNamespace(lot_count=1),
        ),
    )

    def _submit(**kwargs):
        captured["request"] = kwargs["request"]
        return object()

    monkeypatch.setattr(smoke, "submit_live_order_and_confirm", _submit)
    try:
        result = execute_smoke_buy(
            conn=conn,
            broker=_SmokeBroker(),
            krw=50_000,
            market="KRW-BTC",
            confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
            authority_path=str(_authority_path(tmp_path, db_path=db_path)),
            reference_price=100_000_000.0,
            now_ms=1_800_000_000_000,
        )
    finally:
        conn.close()

    assert result["status"] == "submitted"
    assert captured["request"].strategy_name == "operator_execution_smoke"
    assert fake_authority.verified is True
    assert fake_authority.consumed is True
