from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from bithumb_bot.core.sma_policy import _stable_hash


DailyParticipationCountBasis = Literal[
    "intent",
    "submit_expected",
    "submitted",
    "filled",
    "closed_trade",
]

VALID_COUNT_BASIS: tuple[str, ...] = (
    "intent",
    "submit_expected",
    "submitted",
    "filled",
    "closed_trade",
)

TIMESTAMP_FIELD_BY_BASIS: dict[str, str] = {
    "intent": "decision_ts",
    "submit_expected": "decision_ts",
    "submitted": "submitted_ts",
    "filled": "fill_ts",
    "closed_trade": "close_ts",
}


@dataclass(frozen=True)
class DailyParticipationPolicyConfig:
    enabled: bool
    timezone: str
    count_basis: DailyParticipationCountBasis
    window_start_hour: int
    window_end_hour: int
    buy_fraction: float
    max_order_krw: float

    def __post_init__(self) -> None:
        if self.timezone not in {"Asia/Seoul", "KST"}:
            ZoneInfo(self.timezone)
        if str(self.count_basis) not in VALID_COUNT_BASIS:
            raise ValueError("daily_participation_count_basis_invalid")
        if not 0 <= int(self.window_start_hour) <= 23:
            raise ValueError("daily_participation_window_start_hour_invalid")
        if not 0 <= int(self.window_end_hour) <= 24:
            raise ValueError("daily_participation_window_end_hour_invalid")
        if int(self.window_start_hour) >= int(self.window_end_hour):
            raise ValueError("daily_participation_window_invalid")
        if float(self.buy_fraction) <= 0.0 or float(self.buy_fraction) > 1.0:
            raise ValueError("daily_participation_buy_fraction_invalid")
        if float(self.max_order_krw) <= 0.0:
            raise ValueError("daily_participation_max_order_krw_invalid")

    def policy_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "enabled": bool(self.enabled),
            "timezone": self.timezone,
            "count_basis": self.count_basis,
            "timestamp_field": TIMESTAMP_FIELD_BY_BASIS[self.count_basis],
            "window_start_hour": int(self.window_start_hour),
            "window_end_hour": int(self.window_end_hour),
            "buy_fraction": float(self.buy_fraction),
            "max_order_krw": float(self.max_order_krw),
        }

    def policy_hash(self) -> str:
        return _stable_hash(self.policy_payload())


@dataclass(frozen=True)
class DailyParticipationStateSnapshot:
    decision_ts: int
    count_for_kst_day: int
    position_open: bool
    entry_allowed: bool = True
    market_open: bool = True
    daily_count_snapshot_hash: str = "sha256:missing"
    basis_timestamp: int | None = None
    fail_closed_reason: str = ""

    def snapshot_payload(self, *, config: DailyParticipationPolicyConfig) -> dict[str, object]:
        return {
            "schema_version": 1,
            "timezone": config.timezone,
            "count_basis": config.count_basis,
            "kst_day": kst_day(self.decision_ts, config.timezone),
            "timestamp_field": TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
            "decision_ts": int(self.decision_ts),
            "basis_timestamp": int(self.basis_timestamp) if self.basis_timestamp is not None else None,
            "count_for_kst_day": int(self.count_for_kst_day),
            "position_open": bool(self.position_open),
            "entry_allowed": bool(self.entry_allowed),
            "market_open": bool(self.market_open),
            "daily_count_snapshot_hash": self.daily_count_snapshot_hash,
            "fail_closed_reason": self.fail_closed_reason,
        }

    def snapshot_hash(self, *, config: DailyParticipationPolicyConfig) -> str:
        return _stable_hash(self.snapshot_payload(config=config))


@dataclass(frozen=True)
class DailyParticipationPolicyResult:
    allowed: bool
    reason_code: str
    count_basis: DailyParticipationCountBasis
    kst_day: str
    entry_signal_source: str
    timestamp_field: str
    daily_count_snapshot_hash: str
    participation_policy_hash: str
    participation_input_hash: str
    participation_decision_hash: str
    fail_closed_reason: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def kst_day(ts_ms: int, timezone_name: str = "Asia/Seoul") -> str:
    tz = ZoneInfo("Asia/Seoul" if timezone_name == "KST" else timezone_name)
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone(tz).date().isoformat()


def evaluate_daily_participation_policy(
    *,
    config: DailyParticipationPolicyConfig,
    state: DailyParticipationStateSnapshot,
) -> DailyParticipationPolicyResult:
    day = kst_day(state.decision_ts, config.timezone)
    input_payload = {
        "policy": config.policy_payload(),
        "state": state.snapshot_payload(config=config),
    }
    input_hash = _stable_hash(input_payload)
    allowed = False
    reason_code = "daily_participation_disabled"
    if not config.enabled:
        reason_code = "daily_participation_disabled"
    elif state.fail_closed_reason:
        reason_code = state.fail_closed_reason
    elif state.count_for_kst_day > 0:
        reason_code = "daily_participation_already_counted"
    elif state.position_open:
        reason_code = "position_open"
    elif not state.entry_allowed:
        reason_code = "entry_blocked_by_position_state"
    elif not state.market_open:
        reason_code = "market_closed"
    else:
        hour = datetime.fromtimestamp(int(state.decision_ts) / 1000.0, tz=timezone.utc).astimezone(
            ZoneInfo("Asia/Seoul" if config.timezone == "KST" else config.timezone)
        ).hour
        if not (int(config.window_start_hour) <= hour < int(config.window_end_hour)):
            reason_code = "outside_daily_participation_window"
        else:
            allowed = True
            reason_code = "daily_participation_fallback_allowed"
    decision_payload: dict[str, Any] = {
        "allowed": allowed,
        "reason_code": reason_code,
        "count_basis": config.count_basis,
        "kst_day": day,
        "entry_signal_source": "daily_participation_fallback" if allowed else "hold",
        "participation_input_hash": input_hash,
        "daily_count_snapshot_hash": state.daily_count_snapshot_hash,
    }
    return DailyParticipationPolicyResult(
        allowed=allowed,
        reason_code=reason_code,
        count_basis=config.count_basis,
        kst_day=day,
        entry_signal_source="daily_participation_fallback" if allowed else "hold",
        timestamp_field=TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
        daily_count_snapshot_hash=state.daily_count_snapshot_hash,
        participation_policy_hash=config.policy_hash(),
        participation_input_hash=input_hash,
        participation_decision_hash=_stable_hash(decision_payload),
        fail_closed_reason=state.fail_closed_reason,
    )


@dataclass(frozen=True)
class DailyParticipationCountSnapshot:
    count_basis: DailyParticipationCountBasis
    timezone: str
    kst_day: str
    count_for_kst_day: int
    timestamp_field: str
    source: str
    rows: tuple[dict[str, object], ...]
    fail_closed_reason: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "count_basis": self.count_basis,
            "timezone": self.timezone,
            "kst_day": self.kst_day,
            "count_for_kst_day": int(self.count_for_kst_day),
            "timestamp_field": self.timestamp_field,
            "source": self.source,
            "rows": [dict(row) for row in self.rows],
            "fail_closed_reason": self.fail_closed_reason,
        }

    @property
    def snapshot_hash(self) -> str:
        if self.fail_closed_reason:
            return "sha256:missing"
        return _stable_hash(
            {
                "schema_version": 1,
                "count_basis": self.count_basis,
                "timezone": self.timezone,
                "kst_day": self.kst_day,
                "count_for_kst_day": int(self.count_for_kst_day),
                "timestamp_field": self.timestamp_field,
            }
        )

    def state_snapshot(self, *, decision_ts: int, position_open: bool, entry_allowed: bool, market_open: bool = True) -> DailyParticipationStateSnapshot:
        return DailyParticipationStateSnapshot(
            decision_ts=int(decision_ts),
            count_for_kst_day=int(self.count_for_kst_day),
            position_open=bool(position_open),
            entry_allowed=bool(entry_allowed),
            market_open=bool(market_open),
            daily_count_snapshot_hash=self.snapshot_hash,
            fail_closed_reason=self.fail_closed_reason,
        )


def build_research_daily_count_snapshot(
    *,
    config: DailyParticipationPolicyConfig,
    decision_ts: int,
    decision_records: tuple[dict[str, Any], ...] = (),
    trade_records: tuple[dict[str, Any], ...] = (),
) -> DailyParticipationCountSnapshot:
    day = kst_day(decision_ts, config.timezone)
    rows: list[dict[str, object]] = []
    if config.count_basis in {"intent", "submit_expected"}:
        for record in decision_records:
            if str(record.get("final_signal") or record.get("signal") or "").upper() != "BUY":
                continue
            ts = _coerce_int(record.get("decision_ts") or record.get("ts") or record.get("candle_ts"))
            if ts is None or ts >= int(decision_ts) or kst_day(ts, config.timezone) != day:
                continue
            rows.append({"basis": config.count_basis, "ts": ts, "entry_signal_source": _entry_source_from_record(record)})
    elif config.count_basis in {"submitted", "filled"}:
        for trade in trade_records:
            if str(trade.get("side") or "").upper() != "BUY":
                continue
            if config.count_basis == "filled" and not bool(trade.get("is_execution_filled")):
                continue
            ts = _coerce_int(
                trade.get("submit_ts_assumption")
                if config.count_basis == "submitted"
                else trade.get("fill_ts") or trade.get("portfolio_effective_ts") or trade.get("ts")
            )
            if ts is None or ts >= int(decision_ts) or kst_day(ts, config.timezone) != day:
                continue
            rows.append({"basis": config.count_basis, "ts": ts, "entry_signal_source": _entry_source_from_record(trade)})
    elif config.count_basis == "closed_trade":
        for trade in trade_records:
            if str(trade.get("side") or "").upper() != "SELL":
                continue
            ts = _coerce_int(trade.get("portfolio_effective_ts") or trade.get("fill_ts") or trade.get("ts"))
            if ts is None or ts >= int(decision_ts) or kst_day(ts, config.timezone) != day:
                continue
            rows.append({"basis": config.count_basis, "ts": ts, "entry_signal_source": _entry_source_from_record(trade)})
    return DailyParticipationCountSnapshot(
        count_basis=config.count_basis,
        timezone=config.timezone,
        kst_day=day,
        count_for_kst_day=len(rows),
        timestamp_field=TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
        source="research_backtest_ledger_and_decision_records",
        rows=tuple(rows),
    )


def require_runtime_comparable_daily_count_snapshot(snapshot: DailyParticipationCountSnapshot | DailyParticipationStateSnapshot) -> None:
    snapshot_hash = (
        snapshot.snapshot_hash if isinstance(snapshot, DailyParticipationCountSnapshot) else snapshot.daily_count_snapshot_hash
    )
    reason = snapshot.fail_closed_reason if isinstance(snapshot, DailyParticipationStateSnapshot) else snapshot.fail_closed_reason
    if str(snapshot_hash or "") == "sha256:missing" or reason:
        raise ValueError(reason or "daily_count_snapshot_hash_missing")


def _entry_source_from_record(record: dict[str, Any]) -> str:
    trace = record.get("trace") if isinstance(record.get("trace"), dict) else {}
    execution = record.get("execution") if isinstance(record.get("execution"), dict) else {}
    return str(record.get("entry_signal_source") or trace.get("entry_signal_source") or execution.get("entry_signal_source") or "")


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

