from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from bithumb_bot.core.sma_policy import _stable_hash


SOURCE_CONTRACT_VERSION = "daily_participation_event.v1"


@dataclass(frozen=True)
class ParticipationEvent:
    event_id: str
    strategy_instance_id: str
    strategy_name: str
    pair: str
    side: str
    lifecycle_stage: str
    event_ts: int
    count_basis: str
    client_order_id: str = ""
    order_id: str = ""
    fill_id: str = ""
    source: str = ""
    source_contract_version: str = SOURCE_CONTRACT_VERSION
    authoritative: bool = True

    def __post_init__(self) -> None:
        normalized = {
            "event_id": str(self.event_id or "").strip(),
            "strategy_instance_id": str(self.strategy_instance_id or "").strip(),
            "strategy_name": str(self.strategy_name or "").strip().lower(),
            "pair": str(self.pair or "").strip(),
            "side": str(self.side or "").strip().upper(),
            "lifecycle_stage": str(self.lifecycle_stage or "").strip().lower(),
            "count_basis": str(self.count_basis or "").strip().lower(),
            "client_order_id": str(self.client_order_id or "").strip(),
            "order_id": str(self.order_id or "").strip(),
            "fill_id": str(self.fill_id or "").strip(),
            "source": str(self.source or "").strip(),
            "source_contract_version": str(self.source_contract_version or "").strip(),
        }
        for key, value in normalized.items():
            object.__setattr__(self, key, value)
        object.__setattr__(self, "event_ts", int(self.event_ts))
        object.__setattr__(self, "authoritative", bool(self.authoritative))

    def validate_live_scope(self) -> None:
        missing = [
            key
            for key in (
                "event_id",
                "strategy_instance_id",
                "strategy_name",
                "pair",
                "side",
                "lifecycle_stage",
                "count_basis",
                "source",
                "source_contract_version",
            )
            if not str(getattr(self, key) or "").strip()
        ]
        if missing:
            raise ValueError("daily_participation_event_scope_missing:" + ",".join(missing))
        if self.lifecycle_stage == "filled" and not (self.fill_id or self.client_order_id or self.order_id):
            raise ValueError("daily_participation_event_identity_missing")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def participation_event_set_hash(events: Iterable[ParticipationEvent]) -> str:
    rows = [event.as_dict() for event in events]
    return _stable_hash({"schema_version": 1, "events": rows})


def source_contract_hash(*, source: str, source_contract_version: str) -> str:
    source_value = str(source or "").strip()
    version_value = str(source_contract_version or "").strip()
    if not source_value:
        raise ValueError("daily_participation_event_source_missing")
    if not version_value:
        raise ValueError("daily_participation_event_source_contract_version_missing")
    return _stable_hash(
        {
            "schema_version": 1,
            "source": source_value,
            "source_contract_version": version_value,
        }
    )


def normalize_research_participation_events(
    *,
    count_basis: str,
    records: Iterable[dict[str, Any]],
    strategy_instance_id: str,
    strategy_name: str,
    pair: str,
    source: str = "research_backtest_ledger_and_decision_records",
    source_contract_version: str = SOURCE_CONTRACT_VERSION,
) -> tuple[ParticipationEvent, ...]:
    events: list[ParticipationEvent] = []
    basis = str(count_basis or "").strip().lower()
    for index, record in enumerate(records):
        side = str(record.get("side") or "").upper()
        if basis in {"intent", "submit_expected"}:
            signal = str(record.get("final_signal") or record.get("signal") or "").upper()
            if signal != "BUY":
                continue
            event_ts = _coerce_int(record.get("decision_ts") or record.get("ts") or record.get("candle_ts"))
            lifecycle_stage = basis
        elif basis == "submitted":
            if side != "BUY":
                continue
            event_ts = _coerce_int(record.get("submit_ts_assumption") or record.get("submitted_ts") or record.get("ts"))
            lifecycle_stage = "submitted"
        elif basis == "filled":
            if side != "BUY" or not bool(record.get("is_execution_filled", True)):
                continue
            event_ts = _coerce_int(record.get("fill_ts") or record.get("portfolio_effective_ts") or record.get("ts"))
            lifecycle_stage = "filled"
        elif basis == "closed_trade":
            if side != "SELL":
                continue
            event_ts = _coerce_int(record.get("close_ts") or record.get("portfolio_effective_ts") or record.get("fill_ts") or record.get("ts"))
            lifecycle_stage = "closed_trade"
        else:
            continue
        if event_ts is None:
            continue
        event_id = str(record.get("event_id") or record.get("fill_id") or record.get("client_order_id") or "")
        if not event_id:
            event_id = f"{source}:{strategy_instance_id}:{pair}:{basis}:{event_ts}:{index}"
        events.append(
            ParticipationEvent(
                event_id=event_id,
                strategy_instance_id=strategy_instance_id,
                strategy_name=strategy_name,
                pair=pair,
                side=side or "BUY",
                lifecycle_stage=lifecycle_stage,
                event_ts=event_ts,
                count_basis=basis,
                client_order_id=str(record.get("client_order_id") or ""),
                order_id=str(record.get("order_id") or ""),
                fill_id=str(record.get("fill_id") or ""),
                source=source,
                source_contract_version=source_contract_version,
                authoritative=True,
            )
        )
    return tuple(events)


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
