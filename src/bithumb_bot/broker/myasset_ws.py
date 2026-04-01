from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable, Protocol

from .accounts_v1 import PairBalances, to_broker_balance
from .balance_source import BalanceSnapshot
from .base import BrokerBalance, BrokerSchemaError, BrokerTemporaryError


class MyAssetSchemaMismatchError(BrokerSchemaError):
    """MyAsset websocket payload does not match documented schema."""


class MyAssetStreamStaleError(BrokerTemporaryError):
    """MyAsset websocket stream is connected but has no fresh snapshot."""


@dataclass(frozen=True)
class MyAssetEnvelope:
    stream_type: str
    timestamp_ms: int
    asset_timestamp_ms: int
    pair_balances: PairBalances


class MyAssetWsConnection(Protocol):
    def open(self) -> None: ...

    def send_json(self, payload: dict[str, object]) -> None: ...

    def recv_json(self, *, timeout_sec: float) -> object: ...

    def close(self) -> None: ...


def build_myasset_subscribe_request(*, ticket: str | None = None) -> dict[str, object]:
    payload: dict[str, object] = {"type": "myAsset"}
    if ticket:
        payload["ticket"] = str(ticket)
    return payload


def _required_non_negative_decimal(payload: dict[str, object], key: str, *, context: str) -> Decimal:
    raw = payload.get(key)
    if raw in (None, ""):
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: missing required numeric field '{key}'")
    try:
        parsed = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
    if not parsed.is_finite() or parsed < 0:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: invalid non-negative numeric field '{key}'={raw}")
    return parsed


def _required_non_negative_int(payload: dict[str, object], key: str, *, context: str) -> int:
    raw = payload.get(key)
    if raw in (None, ""):
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: missing required integer field '{key}'")
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: invalid integer field '{key}'={raw}") from exc
    if parsed < 0:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: negative integer field '{key}'={raw}")
    return parsed


def parse_myasset_message(
    payload: object,
    *,
    order_currency: str,
    payment_currency: str,
) -> MyAssetEnvelope:
    context = "myAsset"
    if not isinstance(payload, dict):
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: expected object payload, got {type(payload).__name__}")

    message_type = str(payload.get("type") or "").strip()
    if message_type != "myAsset":
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: unexpected type={message_type!r}")

    stream_type = str(payload.get("stream_type") or "").strip().lower()
    if stream_type not in {"snapshot", "realtime"}:
        raise MyAssetSchemaMismatchError(
            f"{context} schema mismatch: invalid stream_type={payload.get('stream_type')!r}"
        )

    timestamp_ms = _required_non_negative_int(payload, "timestamp", context=context)
    asset_timestamp_ms = _required_non_negative_int(payload, "asset_timestamp", context=context)

    assets = payload.get("assets")
    if not isinstance(assets, list):
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: assets must be array")

    balances: dict[str, tuple[Decimal, Decimal]] = {}
    for index, row in enumerate(assets):
        row_context = f"{context}.assets[{index}]"
        if not isinstance(row, dict):
            raise MyAssetSchemaMismatchError(f"{row_context} schema mismatch: expected object row")
        currency = str(row.get("currency") or "").strip().upper()
        if not currency:
            raise MyAssetSchemaMismatchError(f"{row_context} schema mismatch: missing required text field 'currency'")
        if currency in balances:
            raise MyAssetSchemaMismatchError(f"{row_context} schema mismatch: duplicate currency row '{currency}'")
        balance = _required_non_negative_decimal(row, "balance", context=row_context)
        locked = _required_non_negative_decimal(row, "locked", context=row_context)
        balances[currency] = (balance, locked)

    base = str(order_currency or "").strip().upper()
    quote = str(payment_currency or "").strip().upper()
    if quote not in balances:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: missing quote currency row '{quote}'")
    if base not in balances:
        raise MyAssetSchemaMismatchError(f"{context} schema mismatch: missing base currency row '{base}'")
    cash_balance, cash_locked = balances[quote]
    asset_balance, asset_locked = balances[base]

    return MyAssetEnvelope(
        stream_type=stream_type,
        timestamp_ms=timestamp_ms,
        asset_timestamp_ms=asset_timestamp_ms,
        pair_balances=PairBalances(
            cash_balance=cash_balance,
            cash_locked=cash_locked,
            asset_balance=asset_balance,
            asset_locked=asset_locked,
        ),
    )


class MyAssetWsBalanceSource:
    SOURCE_ID = "myasset_ws_private_stream"

    def __init__(
        self,
        *,
        connection_factory: Callable[[], MyAssetWsConnection],
        order_currency: str,
        payment_currency: str,
        now_ms: Callable[[], int],
        stale_after_ms: int,
        recv_timeout_sec: float,
        subscribe_ticket: str | None = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._order_currency = str(order_currency).strip().upper()
        self._payment_currency = str(payment_currency).strip().upper()
        self._now_ms = now_ms
        self._stale_after_ms = max(0, int(stale_after_ms))
        self._recv_timeout_sec = max(0.1, float(recv_timeout_sec))
        self._subscribe_ticket = subscribe_ticket
        self._conn: MyAssetWsConnection | None = None
        self._last_snapshot: BalanceSnapshot | None = None
        self._diag: dict[str, object] = {
            "reason": "not_checked",
            "failure_category": "none",
            "source": self.SOURCE_ID,
            "last_observed_ts_ms": None,
            "last_asset_ts_ms": None,
            "last_success_ts_ms": None,
            "last_failure_ts_ms": None,
            "last_failure_reason": None,
            "stale": False,
        }

    def get_validation_diagnostics(self) -> dict[str, object]:
        return dict(self._diag)

    def _connect_and_subscribe(self) -> MyAssetWsConnection:
        conn = self._connection_factory()
        conn.open()
        conn.send_json(build_myasset_subscribe_request(ticket=self._subscribe_ticket))
        return conn

    def _ensure_connection(self) -> MyAssetWsConnection:
        if self._conn is None:
            self._conn = self._connect_and_subscribe()
        return self._conn

    def _reconnect(self) -> MyAssetWsConnection:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = self._connect_and_subscribe()
        return self._conn

    def _is_snapshot_fresh(self, snapshot: BalanceSnapshot, *, now_ms: int) -> bool:
        if self._stale_after_ms <= 0:
            return True
        return now_ms - int(snapshot.observed_ts_ms) <= self._stale_after_ms

    def _read_snapshot_from_stream(self, *, conn: MyAssetWsConnection) -> BalanceSnapshot:
        raw = conn.recv_json(timeout_sec=self._recv_timeout_sec)
        parsed = parse_myasset_message(
            raw,
            order_currency=self._order_currency,
            payment_currency=self._payment_currency,
        )
        snapshot = BalanceSnapshot(
            source_id=self.SOURCE_ID,
            observed_ts_ms=int(parsed.timestamp_ms),
            asset_ts_ms=int(parsed.asset_timestamp_ms),
            balance=to_broker_balance(parsed.pair_balances),
        )
        self._diag = {
            **self._diag,
            "reason": "ok",
            "failure_category": "none",
            "last_observed_ts_ms": int(parsed.timestamp_ms),
            "last_asset_ts_ms": int(parsed.asset_timestamp_ms),
            "last_success_ts_ms": int(self._now_ms()),
            "stale": False,
        }
        self._last_snapshot = snapshot
        return snapshot

    def fetch_snapshot(self) -> BalanceSnapshot:
        now_ms = self._now_ms()
        if self._last_snapshot is not None and self._is_snapshot_fresh(self._last_snapshot, now_ms=now_ms):
            return self._last_snapshot

        first_exc: Exception | None = None
        for _ in range(2):
            conn = self._ensure_connection()
            try:
                snapshot = self._read_snapshot_from_stream(conn=conn)
                if self._is_snapshot_fresh(snapshot, now_ms=self._now_ms()):
                    return snapshot
                raise MyAssetStreamStaleError(
                    f"myAsset stream stale: observed_ts_ms={snapshot.observed_ts_ms} stale_after_ms={self._stale_after_ms}"
                )
            except Exception as exc:
                failure_category = (
                    "schema_mismatch"
                    if isinstance(exc, BrokerSchemaError)
                    else "stale_source"
                    if isinstance(exc, MyAssetStreamStaleError)
                    else "transport_failure"
                )
                self._diag = {
                    **self._diag,
                    "reason": str(exc).strip() or type(exc).__name__,
                    "failure_category": failure_category,
                    "last_failure_reason": str(exc).strip() or type(exc).__name__,
                    "last_failure_ts_ms": int(self._now_ms()),
                    "stale": True if failure_category == "stale_source" else bool(self._diag.get("stale")),
                }
                if first_exc is None:
                    first_exc = exc
                self._reconnect()

        if self._last_snapshot is not None and self._is_snapshot_fresh(self._last_snapshot, now_ms=self._now_ms()):
            return self._last_snapshot

        if isinstance(first_exc, BrokerSchemaError):
            raise first_exc
        if isinstance(first_exc, MyAssetStreamStaleError):
            raise first_exc
        raise BrokerTemporaryError(f"myAsset websocket snapshot fetch failed: {type(first_exc).__name__}: {first_exc}")

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None
