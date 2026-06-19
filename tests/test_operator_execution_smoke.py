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
    def __init__(self, *, recent_orders: list[object] | None = None) -> None:
        self.recent_orders = list(recent_orders or [])
        self.recovery_calls: list[dict[str, object]] = []
        self.open_order_calls: list[dict[str, object]] = []

    def get_recent_orders_for_recovery(self, *, market: str, limit: int = 100, **kwargs):
        self.recovery_calls.append({"market": market, "limit": limit, **kwargs})
        return list(self.recent_orders)

    def get_open_orders(self, **kwargs):
        self.open_order_calls.append(dict(kwargs))
        if not kwargs.get("exchange_order_ids") and not kwargs.get("client_order_ids"):
            raise AssertionError("identifier-free get_open_orders must not be used by operator smoke")
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


def _planner_rule_dict() -> dict[str, object]:
    return {
        "market": "KRW-BTC",
        "min_qty": 0.0001,
        "qty_step": 0.0001,
        "min_notional_krw": 5_000.0,
        "bid_min_total_krw": 5_000.0,
        "ask_min_total_krw": 5_000.0,
        "bid_price_unit": 1.0,
        "ask_price_unit": 1.0,
        "order_types": ["limit", "price", "market"],
        "bid_types": ["limit", "price", "market"],
        "ask_types": ["limit", "price", "market"],
        "order_sides": ["bid", "ask"],
        "bid_fee": 0.0025,
        "ask_fee": 0.0025,
        "maker_bid_fee": 0.0020,
        "maker_ask_fee": 0.0020,
        "max_qty_decimals": 8,
    }


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


def _patch_smoke_buy_submit_dependencies(
    monkeypatch: pytest.MonkeyPatch, smoke, captured: dict[str, object]
) -> None:
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
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


def test_cmd_smoke_buy_constructs_broker_with_caller(monkeypatch: pytest.MonkeyPatch) -> None:
    import bithumb_bot.operator_smoke as smoke

    class _Conn:
        closed = False

        def close(self) -> None:
            self.closed = True

    conn = _Conn()
    captured: dict[str, object] = {}

    def _build_broker(*, caller: str):
        captured["caller"] = caller
        return _SmokeBroker(), {"caller": caller}

    def _execute_smoke_buy(**kwargs) -> None:
        captured["broker"] = kwargs["broker"]

    monkeypatch.setattr(operator_commands, "ensure_db", lambda: conn)
    monkeypatch.setattr(operator_commands, "build_broker_with_auth_diagnostics", _build_broker)
    monkeypatch.setattr(smoke, "execute_smoke_buy", _execute_smoke_buy)

    operator_commands.cmd_smoke_buy(
        krw=50_000,
        market="KRW-BTC",
        confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
        authority_path="/tmp/operator-smoke-authority.json",
        reference_price=100_000_000.0,
    )

    assert captured["caller"] == "operator_commands.cmd_smoke_buy"
    assert isinstance(captured["broker"], _SmokeBroker)
    assert conn.closed is True


def test_broker_open_order_count_uses_market_scoped_recovery_recent_orders() -> None:
    import bithumb_bot.operator_smoke as smoke

    broker = _SmokeBroker(
        recent_orders=[
            SimpleNamespace(status="NEW"),
            SimpleNamespace(status="PARTIAL"),
            SimpleNamespace(status="FILLED"),
            SimpleNamespace(status="CANCELED"),
        ]
    )

    count = smoke._broker_open_order_count(broker, market="KRW-BTC")

    assert count == 2
    assert broker.recovery_calls == [{"market": "KRW-BTC", "limit": 30}]
    assert broker.open_order_calls == []


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
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
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


def test_execute_smoke_buy_blocks_when_recovery_recent_orders_include_unresolved_open(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    captured: dict[str, object] = {}
    broker = _SmokeBroker(recent_orders=[SimpleNamespace(status="NEW")])
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(
        smoke,
        "submit_live_order_and_confirm",
        lambda **kwargs: captured.setdefault("request", kwargs["request"]),
    )
    try:
        with pytest.raises(OperatorSmokeError, match="smoke_buy_blocked_by_open_broker_orders"):
            execute_smoke_buy(
                conn=conn,
                broker=broker,
                krw=50_000,
                market="KRW-BTC",
                confirm=SMOKE_BUY_CONFIRMATION_TOKEN,
                authority_path=str(_authority_path(tmp_path, db_path=db_path)),
                reference_price=100_000_000.0,
                now_ms=1_800_000_000_000,
            )
    finally:
        conn.close()

    assert broker.recovery_calls == [{"market": "KRW-BTC", "limit": 30}]
    assert broker.open_order_calls == []
    assert captured == {}


def test_execute_smoke_buy_submits_when_recovery_recent_orders_are_terminal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    captured: dict[str, object] = {}
    broker = _SmokeBroker(
        recent_orders=[
            SimpleNamespace(status="FILLED"),
            SimpleNamespace(status="CANCELED"),
            SimpleNamespace(status="FAILED"),
        ]
    )
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    _patch_smoke_buy_submit_dependencies(monkeypatch, smoke, captured)
    try:
        result = execute_smoke_buy(
            conn=conn,
            broker=broker,
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
    assert broker.recovery_calls == [{"market": "KRW-BTC", "limit": 30}]
    assert broker.open_order_calls == []


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
    broker_factory_call: dict[str, object] = {}
    monkeypatch.setattr(smoke, "settings", cfg)
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(operator_commands, "ensure_db", lambda: conn)

    def _build_broker(*, caller: str):
        broker_factory_call["caller"] = caller
        return _SmokeBroker(), {"caller": caller}

    monkeypatch.setattr(operator_commands, "build_broker_with_auth_diagnostics", _build_broker)
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
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
    assert broker_factory_call["caller"] == "operator_commands.cmd_smoke_buy"


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
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
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


def test_execute_smoke_buy_passes_planner_complete_smoke_rules_to_submit_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    captured: dict[str, object] = {}
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
    )

    def _build_live_submit_plan(**kwargs):
        captured["effective_rules"] = kwargs["effective_rules"]
        return SimpleNamespace(
            intent=SimpleNamespace(market=kwargs["market"]),
            submitted_qty=kwargs["qty"],
            rules=kwargs["effective_rules"],
            submit_qty_authority="unit",
            exchange_order_type="price",
            internal_lot_qty=kwargs["qty"],
            qty_split=SimpleNamespace(lot_count=1),
        )

    monkeypatch.setattr(smoke, "build_live_submit_plan", _build_live_submit_plan)
    monkeypatch.setattr(smoke, "submit_live_order_and_confirm", lambda **_kwargs: object())
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

    rules = captured["effective_rules"]
    for field in (
        "bid_price_unit",
        "ask_price_unit",
        "ask_min_total_krw",
        "max_qty_decimals",
        "order_types",
        "bid_types",
        "ask_types",
        "order_sides",
    ):
        assert hasattr(rules, field)
    assert rules.bid_price_unit == 1.0
    assert rules.ask_price_unit == 1.0


def test_execute_smoke_buy_builds_submit_plan_without_bid_price_unit_attribute_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import bithumb_bot.operator_smoke as smoke

    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    captured: dict[str, object] = {}
    monkeypatch.setattr(smoke, "settings", _live_settings(db_path))
    monkeypatch.setattr(smoke, "runtime_code_provenance", lambda: {"commit_sha": "abc123"})
    monkeypatch.setattr(smoke, "validate_operator_smoke_preflight", lambda **_kwargs: None)
    monkeypatch.setattr(
        smoke,
        "resolve_execution_order_rules",
        lambda market: SimpleNamespace(as_order_rules=_planner_rule_dict),
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

    submit_plan = captured["request"].submit_plan
    assert submit_plan.submit_price_tick_policy.price_unit == 1.0
    assert submit_plan.exchange_order_type == "price"
