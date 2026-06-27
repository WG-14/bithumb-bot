from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from .h74_cycle_state import H74_CYCLE_STATE_HOLDING, ensure_h74_cycle_schema
from .h74_position_ownership import (
    H74PositionOwnershipContract,
    H74PositionOwnershipError,
    h74_position_ownership_contract_from_payload,
)


class H74SubmitIdentityError(ValueError):
    pass


@dataclass(frozen=True)
class H74SubmitIdentity:
    cycle_id: str
    h74_cycle_id: str
    strategy_instance_id: str
    authority_hash: str
    probe_run_id: str
    h74_entry_plan_client_order_id: str
    h74_position_ownership_contract_hash: str
    h74_position_ownership_contract: dict[str, object]

    def __post_init__(self) -> None:
        self.validate_complete()

    @classmethod
    def from_ownership_contract(cls, contract: H74PositionOwnershipContract) -> "H74SubmitIdentity":
        return cls(
            cycle_id=contract.cycle_id,
            h74_cycle_id=contract.h74_cycle_id,
            strategy_instance_id=contract.strategy_instance_id,
            authority_hash=contract.authority_hash,
            probe_run_id=contract.probe_run_id,
            h74_entry_plan_client_order_id=contract.entry_plan_id,
            h74_position_ownership_contract_hash=contract.contract_hash,
            h74_position_ownership_contract=contract.as_dict(),
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "H74SubmitIdentity":
        contract_payload = payload.get("h74_position_ownership_contract")
        if not isinstance(contract_payload, Mapping):
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_position_ownership_contract")
        cycle_id = str(payload.get("cycle_id") or "").strip()
        h74_cycle_id = str(payload.get("h74_cycle_id") or "").strip()
        if not cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:cycle_id")
        if not h74_cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_cycle_id")
        if cycle_id != h74_cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:cycle_id")
        contract_hash = str(payload.get("h74_position_ownership_contract_hash") or "").strip()
        if not contract_hash:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_position_ownership_contract_hash")
        try:
            contract = h74_position_ownership_contract_from_payload(
                {
                    **dict(contract_payload),
                    "cycle_id": cycle_id,
                    "h74_cycle_id": h74_cycle_id,
                    "strategy_instance_id": payload.get("strategy_instance_id"),
                    "authority_hash": payload.get("authority_hash"),
                    "probe_run_id": payload.get("probe_run_id")
                    or payload.get("h74_execution_path_probe_run_id"),
                    "h74_position_ownership_contract_hash": contract_hash,
                }
            )
        except H74PositionOwnershipError as exc:
            raise H74SubmitIdentityError(f"h74_submit_identity_invalid_contract:{exc}") from exc
        entry_plan_id = str(payload.get("h74_entry_plan_client_order_id") or "").strip()
        if not entry_plan_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_entry_plan_client_order_id")
        if entry_plan_id != contract.entry_plan_id:
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_entry_plan_client_order_id")
        return cls.from_ownership_contract(contract)

    def validate_complete(self) -> None:
        missing = [
            field
            for field, value in (
                ("cycle_id", self.cycle_id),
                ("h74_cycle_id", self.h74_cycle_id),
                ("strategy_instance_id", self.strategy_instance_id),
                ("authority_hash", self.authority_hash),
                ("probe_run_id", self.probe_run_id),
                ("h74_entry_plan_client_order_id", self.h74_entry_plan_client_order_id),
                ("h74_position_ownership_contract_hash", self.h74_position_ownership_contract_hash),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise H74SubmitIdentityError("h74_submit_identity_missing:" + ",".join(missing))
        if str(self.cycle_id).strip() != str(self.h74_cycle_id).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:cycle_id")
        contract = h74_position_ownership_contract_from_payload(
            {
                **dict(self.h74_position_ownership_contract),
                "cycle_id": self.cycle_id,
                "h74_cycle_id": self.h74_cycle_id,
                "strategy_instance_id": self.strategy_instance_id,
                "authority_hash": self.authority_hash,
                "probe_run_id": self.probe_run_id,
                "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            }
        )
        if contract.contract_hash != str(self.h74_position_ownership_contract_hash).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_position_ownership_contract_hash")
        if contract.entry_plan_id != str(self.h74_entry_plan_client_order_id).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_entry_plan_client_order_id")

    def as_evidence_dict(self) -> dict[str, object]:
        return {
            "cycle_id": self.cycle_id,
            "h74_cycle_id": self.h74_cycle_id,
            "strategy_instance_id": self.strategy_instance_id,
            "authority_hash": self.authority_hash,
            "probe_run_id": self.probe_run_id,
            "h74_execution_path_probe_run_id": self.probe_run_id,
            "h74_entry_plan_client_order_id": self.h74_entry_plan_client_order_id,
            "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            "h74_position_ownership_contract": dict(self.h74_position_ownership_contract),
        }

    def as_order_metadata(self) -> dict[str, object]:
        return {
            "strategy_instance_id": self.strategy_instance_id,
            "cycle_id": self.cycle_id,
            "authority_hash": self.authority_hash,
            "probe_run_id": self.probe_run_id,
            "h74_cycle_id": self.h74_cycle_id,
            "h74_entry_plan_client_order_id": self.h74_entry_plan_client_order_id,
            "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            "h74_position_ownership_contract": dict(self.h74_position_ownership_contract),
        }


def _row_value(row: object, key: str, index: int) -> object:
    return row[key] if hasattr(row, "keys") else row[index]  # type: ignore[index]


def resolve_h74_sell_identity(
    conn: object,
    payload: Mapping[str, Any],
    *,
    pair: str = "",
) -> H74SubmitIdentity:
    ensure_h74_cycle_schema(conn)  # type: ignore[arg-type]
    cycle_id = str(payload.get("h74_cycle_id") or payload.get("cycle_id") or "").strip()
    if cycle_id:
        cycle = conn.execute(  # type: ignore[attr-defined]
            """
            SELECT cycle_id, authority_hash, strategy_instance_id, pair, state,
                   acquired_qty, sold_qty, locked_exit_qty, contract_hash,
                   h74_entry_plan_client_order_id, entry_client_order_id
            FROM h74_cycle_state
            WHERE cycle_id=? AND state=?
            """,
            (cycle_id, H74_CYCLE_STATE_HOLDING),
        ).fetchone()
    else:
        strategy_instance_id = str(payload.get("strategy_instance_id") or "").strip()
        authority_hash = str(payload.get("authority_hash") or payload.get("h74_source_authority_hash") or "").strip()
        resolved_pair = str(pair or payload.get("runtime_pair") or payload.get("pair") or "").strip()
        if not strategy_instance_id or not authority_hash or not resolved_pair:
            raise H74SubmitIdentityError("h74_sell_identity_open_cycle_lookup_missing")
        rows = conn.execute(  # type: ignore[attr-defined]
            """
            SELECT cycle_id, authority_hash, strategy_instance_id, pair, state,
                   acquired_qty, sold_qty, locked_exit_qty, contract_hash,
                   h74_entry_plan_client_order_id, entry_client_order_id
            FROM h74_cycle_state
            WHERE strategy_instance_id=? AND authority_hash=? AND pair=? AND state=?
            ORDER BY updated_ts ASC, cycle_id ASC
            """,
            (strategy_instance_id, authority_hash, resolved_pair, H74_CYCLE_STATE_HOLDING),
        ).fetchall()
        if len(rows) != 1:
            raise H74SubmitIdentityError(
                "h74_sell_identity_open_cycle_missing" if not rows else "h74_sell_identity_multiple_open_cycles"
            )
        cycle = rows[0]
    if cycle is None:
        raise H74SubmitIdentityError("h74_sell_identity_open_cycle_missing")

    acquired_qty = float(_row_value(cycle, "acquired_qty", 5) or 0.0)
    sold_qty = float(_row_value(cycle, "sold_qty", 6) or 0.0)
    locked_exit_qty = float(_row_value(cycle, "locked_exit_qty", 7) or 0.0)
    if acquired_qty - sold_qty - locked_exit_qty <= 1e-12:
        raise H74SubmitIdentityError("h74_sell_identity_cycle_not_holding_remaining")

    cycle_payload = {
        "cycle_id": str(_row_value(cycle, "cycle_id", 0) or "").strip(),
        "h74_cycle_id": str(_row_value(cycle, "cycle_id", 0) or "").strip(),
        "authority_hash": str(_row_value(cycle, "authority_hash", 1) or "").strip(),
        "strategy_instance_id": str(_row_value(cycle, "strategy_instance_id", 2) or "").strip(),
        "pair": str(_row_value(cycle, "pair", 3) or "").strip(),
        "contract_hash": str(_row_value(cycle, "contract_hash", 8) or "").strip(),
        "h74_position_ownership_contract_hash": str(_row_value(cycle, "contract_hash", 8) or "").strip(),
        "h74_entry_plan_client_order_id": str(_row_value(cycle, "h74_entry_plan_client_order_id", 9) or "").strip(),
        "entry_client_order_id": str(_row_value(cycle, "entry_client_order_id", 10) or "").strip(),
    }
    missing_cycle = [
        key
        for key in (
            "cycle_id",
            "h74_cycle_id",
            "h74_entry_plan_client_order_id",
            "h74_position_ownership_contract_hash",
            "strategy_instance_id",
            "authority_hash",
        )
        if not str(cycle_payload.get(key) or "").strip()
    ]
    if missing_cycle:
        raise H74SubmitIdentityError("h74_sell_identity_cycle_missing:" + ",".join(missing_cycle))

    entry_order = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT client_order_id, cycle_id, strategy_instance_id, authority_hash,
               h74_entry_plan_client_order_id, h74_position_ownership_contract_hash,
               h74_position_ownership_contract
        FROM orders
        WHERE side='BUY'
          AND cycle_id=?
          AND h74_entry_plan_client_order_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (cycle_payload["cycle_id"], cycle_payload["h74_entry_plan_client_order_id"]),
    ).fetchone()
    if entry_order is None:
        raise H74SubmitIdentityError("h74_sell_identity_entry_buy_order_missing")

    entry_contract_raw = _row_value(entry_order, "h74_position_ownership_contract", 6)
    try:
        entry_contract = json.loads(str(entry_contract_raw or "{}"))
    except json.JSONDecodeError as exc:
        raise H74SubmitIdentityError("h74_sell_identity_entry_buy_contract_invalid_json") from exc
    if not isinstance(entry_contract, Mapping):
        raise H74SubmitIdentityError("h74_sell_identity_entry_buy_contract_not_object")

    checks = (
        ("cycle_id", cycle_payload["cycle_id"], _row_value(entry_order, "cycle_id", 1)),
        ("strategy_instance_id", cycle_payload["strategy_instance_id"], _row_value(entry_order, "strategy_instance_id", 2)),
        ("authority_hash", cycle_payload["authority_hash"], _row_value(entry_order, "authority_hash", 3)),
        (
            "h74_entry_plan_client_order_id",
            cycle_payload["h74_entry_plan_client_order_id"],
            _row_value(entry_order, "h74_entry_plan_client_order_id", 4),
        ),
        (
            "h74_position_ownership_contract_hash",
            cycle_payload["h74_position_ownership_contract_hash"],
            _row_value(entry_order, "h74_position_ownership_contract_hash", 5),
        ),
    )
    for field, expected, actual in checks:
        if str(expected or "").strip() != str(actual or "").strip():
            raise H74SubmitIdentityError(f"h74_sell_identity_entry_buy_mismatch:{field}")

    payload_contract_hash = str(
        payload.get("h74_position_ownership_contract_hash")
        or payload.get("contract_hash")
        or ""
    ).strip()
    if payload_contract_hash and payload_contract_hash != cycle_payload["h74_position_ownership_contract_hash"]:
        raise H74SubmitIdentityError("h74_sell_identity_payload_mismatch:h74_position_ownership_contract_hash")
    payload_entry_plan_id = str(payload.get("h74_entry_plan_client_order_id") or "").strip()
    if payload_entry_plan_id and payload_entry_plan_id != cycle_payload["h74_entry_plan_client_order_id"]:
        raise H74SubmitIdentityError("h74_sell_identity_payload_mismatch:h74_entry_plan_client_order_id")

    identity_payload = {
        **cycle_payload,
        "probe_run_id": entry_contract.get("probe_run_id") or entry_contract.get("h74_execution_path_probe_run_id"),
        "h74_position_ownership_contract": dict(entry_contract),
    }
    return H74SubmitIdentity.from_mapping(identity_payload)


__all__ = ["H74SubmitIdentity", "H74SubmitIdentityError", "resolve_h74_sell_identity"]
