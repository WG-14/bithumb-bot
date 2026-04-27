from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import (
    ensure_db,
    get_fee_gap_accounting_repair_summary,
    portfolio_asset_total,
    summarize_fill_accounting_incident_projection,
)
from .dust import build_dust_display_context, build_position_state_model
from .external_position_repair import build_external_position_accounting_repair_preview
from .fee_gap_policy import classify_fee_gap_incident_verdict, matching_fee_gap_repair_present
from .lifecycle import (
    ResidualInventorySnapshot,
    summarize_non_executable_residuals,
    summarize_position_lots,
    summarize_reserved_exit_qty,
)
from .markets import normalize_market_id
from .position_authority_state import build_lot_projection_convergence, build_position_authority_assessment
from .recovery_policy import (
    build_tradeability_operator_fields,
    classify_canonical_recovery_state,
    classify_canonical_tradeability_state,
)

_POSITION_REBUILD_LOCKED_ASSET_EPSILON = 1e-12


@dataclass(frozen=True)
class RuntimeReadinessSnapshot:
    recovery_stage: str
    resume_ready: bool
    resume_blockers: tuple[str, ...]
    blocker_categories: tuple[str, ...]
    operator_next_action: str
    recommended_command: str
    position_state: Any
    lot_snapshot: Any
    reconcile_metadata: dict[str, object]
    fee_pending_count: int
    auto_recovery_count: int
    fill_accounting_incident_summary: dict[str, object]
    fee_gap_recovery_required: bool
    fee_gap_resume_blocking: bool
    fee_gap_resume_policy: str
    fee_gap_closeout_blocking: bool
    fee_gap_adjustment_count: int
    material_zero_fee_fill_count: int
    fee_gap_incident: Any
    open_order_count: int
    unresolved_open_order_count: int
    recovery_required_count: int
    submit_unknown_count: int
    position_authority_assessment: dict[str, object]
    projection_convergence: dict[str, object]
    canonical_state: str
    residual_class: str
    run_loop_allowed: bool
    position_management_allowed: bool
    new_entry_allowed: bool
    closeout_allowed: bool
    effective_flat: bool
    operator_action_required: bool
    why_not: str
    execution_flat: bool
    accounting_flat: bool
    tradeability: Any
    tradeability_operator_fields: dict[str, object]
    residual_inventory: ResidualInventorySnapshot
    residual_inventory_mode: str
    residual_inventory_state: str
    residual_inventory_policy_allows_run: bool
    residual_inventory_policy_allows_buy: bool
    residual_inventory_policy_allows_sell: bool
    total_effective_exposure_qty: float
    total_effective_exposure_notional_krw: float | None
    residual_sell_candidate: dict[str, object] | None
    residual_proof_min_qty: float | None
    residual_proof_min_notional_krw: float | None
    residual_proof_locked_qty: float
    active_fee_accounting_blocker: bool
    accounting_projection_ok: bool
    idempotency_scope: str
    structured_blockers: tuple[dict[str, object], ...]
    authority_truth_model: dict[str, object]
    broker_position_evidence: dict[str, object]
    inspect_only_mode: bool

    def as_dict(self) -> dict[str, object]:
        primary_reason = str(self.resume_blockers[0]) if self.resume_blockers else (
            str(self.residual_class or "NONE") if not self.run_loop_allowed else "none"
        )
        projection_reason = (
            "converged"
            if bool(self.projection_convergence.get("converged"))
            else str(self.projection_convergence.get("reason") or "projection_non_converged")
        )
        tradeability_reason = (
            str(self.residual_class or "NONE")
            if (not self.run_loop_allowed or str(self.residual_class or "") == "RESIDUAL_INVENTORY_TRACKED")
            else "none"
        )
        return {
            "recovery_stage": self.recovery_stage,
            "resume_ready": bool(self.resume_ready),
            "resume_blockers": list(self.resume_blockers),
            "blocker_categories": list(self.blocker_categories),
            "operator_next_action": self.operator_next_action,
            "recommended_command": self.recommended_command,
            "position_authority_summary": self.position_state.normalized_exposure.position_authority_summary,
            "normalized_exposure": self.position_state.normalized_exposure.as_dict(),
            "lot_snapshot": self.lot_snapshot.as_dict(),
            "fee_pending_count": int(self.fee_pending_count),
            "auto_recovery_count": int(self.auto_recovery_count),
            "fill_accounting_incident_summary": dict(self.fill_accounting_incident_summary),
            "fee_gap_recovery_required": bool(self.fee_gap_recovery_required),
            "fee_gap_resume_blocking": bool(self.fee_gap_resume_blocking),
            "fee_gap_resume_policy": self.fee_gap_resume_policy,
            "fee_gap_closeout_blocking": bool(self.fee_gap_closeout_blocking),
            "fee_gap_adjustment_count": int(self.fee_gap_adjustment_count),
            "material_zero_fee_fill_count": int(self.material_zero_fee_fill_count),
            "fee_gap_incident": self.fee_gap_incident.as_dict(),
            "open_order_count": int(self.open_order_count),
            "unresolved_open_order_count": int(self.unresolved_open_order_count),
            "recovery_required_count": int(self.recovery_required_count),
            "submit_unknown_count": int(self.submit_unknown_count),
            "position_authority_assessment": dict(self.position_authority_assessment),
            "position_authority_alignment_state": str(
                self.position_authority_assessment.get("alignment_state") or "unknown"
            ),
            "position_authority_diagnostic_flags": list(
                self.position_authority_assessment.get("diagnostic_flags") or []
            ),
            "position_authority_action_state": str(
                self.position_authority_assessment.get("repair_action_state") or "unknown"
            ),
            "projection_convergence": dict(self.projection_convergence),
            "projection_converged": bool(self.projection_convergence.get("converged")),
            "projection_non_convergence_reason": str(self.projection_convergence.get("reason") or "none"),
            "projection_reason": projection_reason,
            "primary_reason": primary_reason,
            "tradeability_reason": tradeability_reason,
            "authority_truth_model": dict(self.authority_truth_model),
            "broker_position_evidence": dict(self.broker_position_evidence),
            "structured_blockers": [dict(item) for item in self.structured_blockers],
            "inspect_only_mode": bool(self.inspect_only_mode),
            "canonical_state": self.canonical_state,
            "residual_class": self.residual_class,
            "halt_recovery_can_resume": bool(self.resume_ready),
            "run_loop_can_resume": bool(self.run_loop_allowed),
            "tradeability_gate_blocked": bool(not self.run_loop_allowed),
            "tradeability_resume_safety": (
                "safe"
                if self.run_loop_allowed
                else f"policy_blocked ({tradeability_reason})"
            ),
            "run_loop_allowed": bool(self.run_loop_allowed),
            "position_management_allowed": bool(self.position_management_allowed),
            "new_entry_allowed": bool(self.new_entry_allowed),
            "closeout_allowed": bool(self.closeout_allowed),
            "effective_flat": bool(self.effective_flat),
            "operator_action_required": bool(self.operator_action_required),
            "why_not": self.why_not,
            "execution_flat": bool(self.execution_flat),
            "accounting_flat": bool(self.accounting_flat),
            "tradeability": self.tradeability.as_dict(),
            "residual_inventory": self.residual_inventory.as_dict(),
            "residual_inventory_mode": self.residual_inventory_mode,
            "residual_inventory_state": self.residual_inventory_state,
            "residual_inventory_qty": float(self.residual_inventory.residual_qty),
            "residual_inventory_notional_krw": (
                None
                if self.residual_inventory.residual_notional_krw is None
                else float(self.residual_inventory.residual_notional_krw)
            ),
            "residual_inventory_exchange_sellable": bool(self.residual_inventory.exchange_sellable),
            "residual_inventory_explainable": bool(self.residual_inventory.explainable),
            "residual_inventory_policy_allows_run": bool(self.residual_inventory_policy_allows_run),
            "residual_inventory_policy_allows_buy": bool(self.residual_inventory_policy_allows_buy),
            "residual_inventory_policy_allows_sell": bool(self.residual_inventory_policy_allows_sell),
            "total_effective_exposure_qty": float(self.total_effective_exposure_qty),
            "total_effective_exposure_notional_krw": (
                None
                if self.total_effective_exposure_notional_krw is None
                else float(self.total_effective_exposure_notional_krw)
            ),
            "residual_sell_candidate": (
                None if self.residual_sell_candidate is None else dict(self.residual_sell_candidate)
            ),
            "residual_sell_candidate_allowed": bool(
                self.residual_sell_candidate and self.residual_inventory_policy_allows_sell
            ),
            "residual_proof_min_qty": self.residual_proof_min_qty,
            "residual_proof_min_notional_krw": self.residual_proof_min_notional_krw,
            "residual_proof_locked_qty": float(self.residual_proof_locked_qty),
            "active_fee_accounting_blocker": bool(self.active_fee_accounting_blocker),
            "accounting_projection_ok": bool(self.accounting_projection_ok),
            "idempotency_scope": self.idempotency_scope,
            **self.tradeability_operator_fields,
        }


def _make_structured_blocker(
    *,
    code: str,
    category: str,
    stage: str,
    detail: str,
    operator_next_action: str,
    recommended_command: str,
    projection_convergence: dict[str, object],
    authority_truth_model: dict[str, object],
    authority_assessment: dict[str, object],
) -> dict[str, object]:
    inspect_only = bool(
        authority_truth_model.get("inspect_only")
        or str(authority_assessment.get("repair_action_state") or "") == "inspect_only"
        or code == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED"
    )
    return {
        "code": code,
        "reason_code": code,
        "category": category,
        "stage": stage,
        "detail": detail,
        "operator_next_action": operator_next_action,
        "recommended_command": recommended_command,
        "inspect_only": inspect_only,
        "canonical_asset_qty": float(authority_truth_model.get("portfolio_asset_qty") or 0.0),
        "projected_lot_qty": float(projection_convergence.get("projected_total_qty") or 0.0),
        "divergence_delta_qty": float(projection_convergence.get("portfolio_delta_qty") or 0.0),
        "projection_converged": bool(projection_convergence.get("converged")),
    }


def _build_authority_truth_model(
    *,
    projection_convergence: dict[str, object],
    authority_assessment: dict[str, object],
) -> dict[str, object]:
    truth_model = {
        "canonical_truth_source": "orders_fills_trades_plus_portfolio",
        "projection_truth_source": "open_position_lots_materialized_projection",
        "projection_role": "rebuildable_materialized_view",
        "repair_event_role": "historical_evidence_not_current_state_proof",
        "projection_publication_role": "current_state_attestation",
        "portfolio_asset_qty": float(projection_convergence.get("portfolio_qty") or 0.0),
        "projected_total_qty": float(projection_convergence.get("projected_total_qty") or 0.0),
        "projection_delta_qty": float(projection_convergence.get("portfolio_delta_qty") or 0.0),
        "projected_qty_excess": float(projection_convergence.get("projected_qty_excess") or 0.0),
        "projected_qty_shortfall": float(projection_convergence.get("projected_qty_shortfall") or 0.0),
        "projection_converged": bool(projection_convergence.get("converged")),
        "projection_non_convergence_reason": str(projection_convergence.get("reason") or "none"),
        "alignment_state": str(authority_assessment.get("alignment_state") or "projection_only"),
        "repair_action_state": str(authority_assessment.get("repair_action_state") or "not_applicable"),
        "inspect_only": bool(
            str(authority_assessment.get("repair_action_state") or "") == "inspect_only"
            or not bool(projection_convergence.get("converged"))
        ),
    }
    truth_model.update(dict(authority_assessment.get("truth_model") or {}))
    return truth_model


def _broker_portfolio_projection_match(
    *,
    broker_qty_known: bool,
    broker_qty: float,
    portfolio_qty: float,
    projected_total_qty: float,
) -> bool:
    if not broker_qty_known:
        return False
    return bool(
        abs(float(broker_qty) - float(portfolio_qty)) <= 1e-12
        and abs(float(projected_total_qty) - float(portfolio_qty)) <= 1e-12
    )


def _is_non_executable_residual_holdings_state(
    *,
    residual_inventory: ResidualInventorySnapshot,
    position_state: Any,
    projection_convergence: dict[str, object],
    authority_assessment: dict[str, object],
    broker_position_evidence: dict[str, object],
    open_order_count: int,
    recovery_required_count: int,
    fee_validation_blocked_count: int,
    unapplied_principal_pending_count: int,
    fee_gap_resume_blocking: bool,
) -> bool:
    normalized = position_state.normalized_exposure
    if open_order_count > 0 or recovery_required_count > 0:
        return False
    if unapplied_principal_pending_count > 0 or fee_validation_blocked_count > 0 or fee_gap_resume_blocking:
        return False
    if not bool(projection_convergence.get("converged")):
        return False
    if any(
        bool(authority_assessment.get(key))
        for key in (
            "needs_correction",
            "needs_residual_normalization",
            "needs_portfolio_projection_repair",
            "needs_full_projection_rebuild",
        )
    ):
        return False
    if not residual_inventory.material_residual:
        return False
    if not bool(getattr(normalized, "has_dust_only_remainder", False)):
        return False
    if bool(getattr(normalized, "has_executable_exposure", False)):
        return False
    if int(getattr(normalized, "sellable_executable_lot_count", 0) or 0) > 0:
        return False
    return _broker_portfolio_projection_match(
        broker_qty_known=bool(broker_position_evidence.get("broker_qty_known")),
        broker_qty=float(broker_position_evidence.get("broker_qty") or 0.0),
        portfolio_qty=float(projection_convergence.get("portfolio_qty") or 0.0),
        projected_total_qty=float(projection_convergence.get("projected_total_qty") or 0.0),
    )


def _metadata_dict(raw: object | None) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _residual_inventory_mode() -> str:
    mode = str(getattr(settings, "RESIDUAL_INVENTORY_MODE", "block") or "block").strip().lower()
    if mode not in {"block", "track", "ignore_tiny"}:
        return "block"
    return mode


def _ignore_tiny_policy_eligible(*, residual_inventory: ResidualInventorySnapshot) -> bool:
    return bool(
        residual_inventory.material_residual
        and not residual_inventory.exchange_sellable
        and "TRUE_DUST" in residual_inventory.residual_classes
    )


def _is_residual_inventory_trackable(
    *,
    residual_inventory: ResidualInventorySnapshot,
    projection_convergence: dict[str, object],
    authority_assessment: dict[str, object],
    broker_position_evidence: dict[str, object],
    open_order_count: int,
    recovery_required_count: int,
    fee_validation_blocked_count: int,
    unapplied_principal_pending_count: int,
    fee_gap_resume_blocking: bool,
) -> bool:
    if open_order_count > 0 or recovery_required_count > 0:
        return False
    if unapplied_principal_pending_count > 0 or fee_validation_blocked_count > 0 or fee_gap_resume_blocking:
        return False
    if not bool(projection_convergence.get("converged")):
        return False
    if not bool(residual_inventory.material_residual):
        return False
    if not bool(residual_inventory.explainable):
        return False
    if any(
        bool(authority_assessment.get(key))
        for key in (
            "needs_correction",
            "needs_residual_normalization",
            "needs_portfolio_projection_repair",
            "needs_full_projection_rebuild",
        )
    ):
        return False
    if bool(broker_position_evidence.get("balance_source_stale")):
        return False
    return _broker_portfolio_projection_match(
        broker_qty_known=bool(broker_position_evidence.get("broker_qty_known")),
        broker_qty=float(broker_position_evidence.get("broker_qty") or 0.0),
        portfolio_qty=float(projection_convergence.get("portfolio_qty") or 0.0),
        projected_total_qty=float(projection_convergence.get("projected_total_qty") or 0.0),
    )


def _build_residual_sell_candidate_summary(
    *,
    residual_inventory: ResidualInventorySnapshot,
    residual_inventory_mode: str,
    residual_inventory_state: str,
) -> dict[str, object] | None:
    if residual_inventory_mode != "track":
        return None
    if residual_inventory_state != "RESIDUAL_INVENTORY_TRACKED":
        return None
    if not residual_inventory.exchange_sellable:
        return None
    return {
        "qty": float(residual_inventory.residual_qty),
        "notional": (
            None
            if residual_inventory.residual_notional_krw is None
            else float(residual_inventory.residual_notional_krw)
        ),
        "source": "residual_inventory",
        "classes": list(residual_inventory.residual_classes),
        "exchange_sellable": True,
        "allowed_by_policy": True,
        "requires_final_pre_submit_proof": True,
    }


def _metadata_int(metadata: dict[str, object], key: str) -> int:
    try:
        return max(0, int(metadata.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


def _row_int(row: Any, key: str, default: int = 0) -> int:
    if row is None:
        return default
    try:
        keys = row.keys()
    except AttributeError:
        keys = ()
    if key not in keys:
        return default
    try:
        return int(row[key] or 0)
    except (TypeError, ValueError, KeyError):
        return default


def _to_float_or_zero(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _split_pair_currencies(pair: str) -> tuple[str | None, str | None]:
    try:
        market = normalize_market_id(str(pair or ""))
    except Exception:
        token = str(pair or "").strip().upper().replace("_", "-")
        if "-" not in token:
            return None, None
        market = token
    quote_currency, base_currency = market.split("-", 1)
    return base_currency, quote_currency


def build_broker_position_evidence(
    metadata: dict[str, object] | None,
    *,
    pair: str | None = None,
) -> dict[str, object]:
    metadata = dict(metadata or {})
    base_currency_expected, quote_currency_expected = _split_pair_currencies(str(pair or settings.PAIR))
    source = str(metadata.get("balance_source") or metadata.get("broker_qty_evidence_source") or "-")
    observed_ts_ms = _metadata_int(metadata, "balance_observed_ts_ms")
    asset_ts_ms = _metadata_int(metadata, "balance_asset_ts_ms")
    asset_available = _to_float_or_zero(metadata.get("broker_asset_available"))
    asset_locked = _to_float_or_zero(metadata.get("broker_asset_locked"))
    cash_available = _to_float_or_zero(metadata.get("broker_cash_available"))
    cash_locked = _to_float_or_zero(metadata.get("broker_cash_locked"))
    broker_qty = _to_float_or_zero(metadata.get("broker_asset_qty"))
    broker_qty_value_source = "broker_asset_qty" if "broker_asset_qty" in metadata else "missing"
    if abs(broker_qty) <= 1e-12 and broker_qty_value_source == "missing":
        broker_qty = _to_float_or_zero(metadata.get("dust_broker_qty"))
        if abs(broker_qty) > 1e-12:
            broker_qty_value_source = "dust_broker_qty_fallback"
    base_currency = str(
        metadata.get("balance_source_base_currency")
        or metadata.get("base_currency")
        or ""
    ).strip().upper()
    quote_currency = str(
        metadata.get("balance_source_quote_currency")
        or metadata.get("quote_currency")
        or ""
    ).strip().upper()
    stale = bool(metadata.get("balance_source_stale")) if "balance_source_stale" in metadata else False
    missing_evidence_fields: list[str] = []
    if observed_ts_ms <= 0:
        missing_evidence_fields.append("balance_observed_ts_ms")
    if not base_currency:
        missing_evidence_fields.append("base_currency")
    if not quote_currency:
        missing_evidence_fields.append("quote_currency")
    if "broker_asset_qty" not in metadata:
        missing_evidence_fields.append("broker_asset_qty")
    if "broker_asset_available" not in metadata:
        missing_evidence_fields.append("broker_asset_available")
    if "broker_asset_locked" not in metadata:
        missing_evidence_fields.append("broker_asset_locked")
    formal_position_fields_present = all(
        key in metadata for key in ("broker_asset_qty", "broker_asset_available", "broker_asset_locked")
    )
    currency_match = bool(
        base_currency
        and quote_currency
        and base_currency_expected
        and quote_currency_expected
        and base_currency == base_currency_expected
        and quote_currency == quote_currency_expected
    )
    if base_currency and base_currency_expected and base_currency != base_currency_expected:
        missing_evidence_fields.append("base_currency_mismatch")
    if quote_currency and quote_currency_expected and quote_currency != quote_currency_expected:
        missing_evidence_fields.append("quote_currency_mismatch")
    available_for_health = bool(source != "-" or observed_ts_ms > 0 or abs(broker_qty) > 1e-12)
    formal_position_evidence_available = bool(
        observed_ts_ms > 0
        and not stale
        and currency_match
        and formal_position_fields_present
    )
    position_rebuild_blockers: list[str] = []
    if stale:
        position_rebuild_blockers.append("balance_snapshot_stale")
    if base_currency and base_currency_expected and base_currency != base_currency_expected:
        position_rebuild_blockers.append("base_currency_mismatch")
    if quote_currency and quote_currency_expected and quote_currency != quote_currency_expected:
        position_rebuild_blockers.append("quote_currency_mismatch")
    if formal_position_evidence_available and abs(asset_locked) > _POSITION_REBUILD_LOCKED_ASSET_EPSILON:
        position_rebuild_blockers.append("broker_asset_locked_nonzero")
    balance_snapshot_available_for_position_rebuild = bool(
        formal_position_evidence_available and not position_rebuild_blockers
    )
    return {
        "broker_qty_known": formal_position_evidence_available,
        "broker_qty": broker_qty,
        "broker_qty_value_source": broker_qty_value_source,
        "broker_qty_evidence_source": source,
        "broker_qty_evidence_observed_ts_ms": observed_ts_ms,
        "balance_source": source,
        "balance_source_stale": stale,
        "balance_snapshot_available_for_health": available_for_health,
        "balance_snapshot_available_for_position_rebuild": balance_snapshot_available_for_position_rebuild,
        "missing_evidence_fields": missing_evidence_fields,
        "position_rebuild_blockers": position_rebuild_blockers,
        "base_currency": base_currency or None,
        "quote_currency": quote_currency or None,
        "asset_available": asset_available,
        "asset_locked": asset_locked,
        "cash_available": cash_available,
        "cash_locked": cash_locked,
        "asset_ts_ms": asset_ts_ms,
    }


def compute_runtime_readiness_snapshot(conn=None) -> RuntimeReadinessSnapshot:
    """Build the canonical recovery/readiness interpretation for one DB snapshot.

    This is intentionally read-only. Mutation-specific previews can depend on
    this snapshot for stage and ordering, but they still own their individual
    safety checks.
    """

    close_conn = False
    if conn is None:
        conn = ensure_db()
        close_conn = True
    try:
        state = runtime_state.snapshot()
        metadata = _metadata_dict(state.last_reconcile_metadata)
        metadata.setdefault("unresolved_open_order_count", int(state.unresolved_open_order_count or 0))
        metadata.setdefault("recovery_required_count", int(state.recovery_required_count or 0))

        open_row = conn.execute(
            """
            SELECT
                COUNT(*) AS open_order_count,
                COALESCE(SUM(CASE WHEN status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'ACCOUNTING_PENDING', 'CANCEL_REQUESTED') THEN 1 ELSE 0 END), 0)
                    AS unresolved_open_order_count,
                COALESCE(SUM(CASE WHEN status='SUBMIT_UNKNOWN' THEN 1 ELSE 0 END), 0)
                    AS submit_unknown_count,
                COALESCE(SUM(CASE WHEN status='ACCOUNTING_PENDING' THEN 1 ELSE 0 END), 0)
                    AS accounting_pending_count,
                COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0)
                    AS recovery_required_count
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN',
                             'ACCOUNTING_PENDING',
                             'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            """
        ).fetchone()
        open_order_count = _row_int(open_row, "open_order_count")
        unresolved_open_order_count = _row_int(open_row, "unresolved_open_order_count")
        submit_unknown_count = _row_int(open_row, "submit_unknown_count")
        accounting_pending_count = _row_int(open_row, "accounting_pending_count")
        recovery_required_count = _row_int(open_row, "recovery_required_count")

        portfolio_row = conn.execute(
            "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        if portfolio_row is None:
            portfolio_asset_qty = 0.0
        elif "asset_available" in portfolio_row.keys():
            portfolio_asset_qty = portfolio_asset_total(
                asset_available=float(portfolio_row["asset_available"] or 0.0),
                asset_locked=float(portfolio_row["asset_locked"] or 0.0),
            )
        else:
            portfolio_asset_qty = float(portfolio_row["asset_qty"] or 0.0)

        dust_context = build_dust_display_context(metadata)
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        residual_inventory = summarize_non_executable_residuals(conn, pair=settings.PAIR)
        lot_definition = getattr(lot_snapshot, "lot_definition", None)
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
        position_state = build_position_state_model(
            raw_qty_open=portfolio_asset_qty,
            metadata_raw=metadata,
            raw_total_asset_qty=max(
                portfolio_asset_qty,
                float(lot_snapshot.raw_total_asset_qty),
                float(dust_context.raw_holdings.broker_qty),
            ),
            open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
            dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
            open_lot_count=int(lot_snapshot.open_lot_count),
            dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
            reserved_exit_qty=reserved_exit_qty,
            internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
            min_qty=(None if lot_definition is None else lot_definition.min_qty),
            qty_step=(None if lot_definition is None else lot_definition.qty_step),
            min_notional_krw=(None if lot_definition is None else lot_definition.min_notional_krw),
            max_qty_decimals=(None if lot_definition is None else lot_definition.max_qty_decimals),
        )
        fill_accounting_incident_summary = summarize_fill_accounting_incident_projection(conn)
        unapplied_principal_pending_count = int(
            fill_accounting_incident_summary.get("unapplied_principal_pending_count") or 0
        )
        principal_applied_fee_pending_count = int(
            fill_accounting_incident_summary.get("principal_applied_fee_pending_count") or 0
        )
        fee_validation_blocked_count = int(
            fill_accounting_incident_summary.get("fee_validation_blocked_count") or 0
        )
        fee_pending_count = unapplied_principal_pending_count
        fee_gap_required = _metadata_int(metadata, "fee_gap_recovery_required") > 0
        fee_gap_adjustment_count = _metadata_int(metadata, "fee_gap_adjustment_count")
        fee_gap_adjustment_latest_event_ts = _metadata_int(metadata, "fee_gap_adjustment_latest_event_ts")
        fee_gap_adjustment_total_krw = 0.0
        try:
            fee_gap_adjustment_total_krw = float(metadata.get("fee_gap_adjustment_total_krw", 0.0) or 0.0)
        except (TypeError, ValueError):
            fee_gap_adjustment_total_krw = 0.0
        material_zero_fee_fill_count = _metadata_int(metadata, "material_zero_fee_fill_count")
        material_zero_fee_fill_latest_ts = _metadata_int(metadata, "material_zero_fee_fill_latest_ts")
        authority_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
        projection_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        broker_position_evidence = build_broker_position_evidence(metadata, pair=settings.PAIR)
        authority_truth_model = _build_authority_truth_model(
            projection_convergence=projection_convergence,
            authority_assessment=authority_assessment,
        )
        projection_non_convergence_blocking = bool(
            not bool(projection_convergence.get("converged"))
            and int(projection_convergence.get("lot_row_count") or 0) > 0
            and (
                abs(float(portfolio_asset_qty)) > 1e-12
                or _metadata_int(metadata, "balance_observed_ts_ms") > 0
            )
        )
        canonical_recovery = classify_canonical_recovery_state(
            position_state=position_state,
            lot_snapshot=lot_snapshot,
            portfolio_asset_qty=portfolio_asset_qty,
            reserved_exit_qty=reserved_exit_qty,
            projection_converged=bool(projection_convergence.get("converged")),
            projection_non_convergence_reason=str(projection_convergence.get("reason") or "projection_non_converged"),
        )
        repair_summary = get_fee_gap_accounting_repair_summary(conn)
        already_repaired_fee_gap = matching_fee_gap_repair_present(
            repair_summary=repair_summary,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            fee_gap_adjustment_total_krw=fee_gap_adjustment_total_krw,
            fee_gap_adjustment_latest_event_ts=fee_gap_adjustment_latest_event_ts,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            material_zero_fee_fill_latest_ts=material_zero_fee_fill_latest_ts,
        )
        fee_gap_reasons: list[str] = []
        external_cash_adjustment_reason = str(metadata.get("external_cash_adjustment_reason") or "none")
        if fee_gap_required and material_zero_fee_fill_count <= 0:
            fee_gap_reasons.append("material_zero_fee_fill_count=0")
        if fee_gap_required and fee_gap_adjustment_count <= 0:
            fee_gap_reasons.append("fee_gap_adjustment_count=0")
        if external_cash_adjustment_reason not in {"reconcile_fee_gap_cash_drift", "none"}:
            fee_gap_reasons.append(f"external_cash_adjustment_reason={external_cash_adjustment_reason}")
        if open_order_count > 0:
            fee_gap_reasons.append(f"open_or_unresolved_orders={open_order_count}")
        if recovery_required_count > 0:
            fee_gap_reasons.append(f"recovery_required_orders={recovery_required_count}")
        if str(state.last_reconcile_status or "").lower() != "ok":
            fee_gap_reasons.append(f"last_reconcile_status={state.last_reconcile_status or 'none'}")
        if abs(float(portfolio_asset_qty)) > 1e-12:
            fee_gap_reasons.append(f"portfolio_not_flat=asset_qty={float(portfolio_asset_qty):.12f}")
        if int(lot_snapshot.open_lot_count) > 0 or int(lot_snapshot.dust_tracking_lot_count) > 0:
            fee_gap_reasons.append(
                "lot_residue_present="
                f"open_lot_count={int(lot_snapshot.open_lot_count)},dust_tracking_lot_count={int(lot_snapshot.dust_tracking_lot_count)}"
            )
        if abs(float(reserved_exit_qty)) > 1e-12:
            fee_gap_reasons.append(f"reserved_exit_qty={float(reserved_exit_qty):.12f}")
        blocked_by_authority_rebuild = bool(
            bool(authority_assessment.get("needs_correction"))
            or bool(authority_assessment.get("needs_residual_normalization"))
            or bool(authority_assessment.get("needs_portfolio_projection_repair"))
            or not bool(projection_convergence.get("converged"))
            or str(position_state.normalized_exposure.authority_gap_reason or "")
            == "authority_missing_recovery_required"
        )
        fee_gap_incident = classify_fee_gap_incident_verdict(
            raw_recovery_required=fee_gap_required,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            material_zero_fee_fill_latest_ts=material_zero_fee_fill_latest_ts,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            fee_gap_adjustment_total_krw=fee_gap_adjustment_total_krw,
            fee_gap_adjustment_latest_event_ts=fee_gap_adjustment_latest_event_ts,
            external_cash_adjustment_reason=external_cash_adjustment_reason,
            already_repaired=already_repaired_fee_gap,
            repair_blocker_reasons=fee_gap_reasons,
            blocked_by_authority_rebuild=blocked_by_authority_rebuild,
            blocked_by_open_exposure=bool(
                int(lot_snapshot.open_lot_count) > 0 or abs(float(portfolio_asset_qty)) > 1e-12
            ),
            blocked_by_dust_residue=bool(int(lot_snapshot.dust_tracking_lot_count) > 0),
            has_executable_open_exposure=bool(
                int(lot_snapshot.open_lot_count) > 0
                and position_state.normalized_exposure.has_executable_exposure
            ),
            canonical_state=canonical_recovery.canonical_state,
            execution_flat=canonical_recovery.execution_flat,
            accounting_flat=canonical_recovery.accounting_flat,
        )
        fee_gap_policy = fee_gap_incident.policy
        replay_mismatch_preview = build_external_position_accounting_repair_preview(conn)
        residual_inventory_mode = _residual_inventory_mode()
        residual_inventory_state = "NONE"

        blockers: list[str] = []
        categories: list[str] = []
        structured_blockers: list[dict[str, object]] = []
        stage = "RESUME_READY"
        operator_next_action = "resume_now"
        recommended_command = "uv run python bot.py resume"

        if unapplied_principal_pending_count > 0:
            stage = "UNAPPLIED_PRINCIPAL_PENDING"
            blockers.append("UNAPPLIED_PRINCIPAL_PENDING")
            categories.append("accounting_latency")
            operator_next_action = "wait_for_auto_reconcile_or_review_fee_evidence"
            recommended_command = "uv run python bot.py recovery-report"
            structured_blockers.append(
                _make_structured_blocker(
                    code="UNAPPLIED_PRINCIPAL_PENDING",
                    category="accounting_latency",
                    stage=stage,
                    detail="broker fill principal is still unapplied; new submissions and closeout remain blocked until principal accounting converges",
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif fee_validation_blocked_count > 0:
            stage = "FEE_VALIDATION_BLOCKED"
            blockers.append("FEE_VALIDATION_BLOCKED")
            categories.append("accounting_truth")
            operator_next_action = "review_fee_evidence"
            recommended_command = "uv run python bot.py recovery-report"
            structured_blockers.append(
                _make_structured_blocker(
                    code="FEE_VALIDATION_BLOCKED",
                    category="accounting_truth",
                    stage=stage,
                    detail="principal is applied but fee validation is blocked; operator review is required before new submissions resume",
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif principal_applied_fee_pending_count > 0:
            stage = "FEE_FINALIZATION_PENDING"
            categories.append("accounting_latency")
            operator_next_action = "wait_for_auto_reconcile_or_review_fee_evidence"
            recommended_command = "uv run python bot.py recovery-report"
        elif bool(authority_assessment.get("needs_residual_normalization")):
            stage = "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
            blockers.append("POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = (
                "apply_rebuild_position_authority"
                if bool(authority_assessment.get("safe_to_normalize_residual"))
                else "review_position_authority_evidence"
            )
            recommended_command = (
                "uv run python bot.py rebuild-position-authority --apply --yes"
                if bool(authority_assessment.get("safe_to_normalize_residual"))
                else "uv run python bot.py rebuild-position-authority"
            )
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=str(authority_assessment.get("reason") or "partial-close residual normalization required"),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif bool(authority_assessment.get("needs_full_projection_rebuild")):
            stage = "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
            blockers.append("POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "review_position_authority_evidence"
            recommended_command = "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=str(
                        authority_assessment.get("reason")
                        or "historical fragmentation requires full projection rebuild"
                    ),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif bool(authority_assessment.get("needs_portfolio_projection_repair")):
            stage = "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING"
            blockers.append("POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "review_position_authority_evidence"
            recommended_command = "uv run python bot.py rebuild-position-authority"
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=str(authority_assessment.get("reason") or "projection/portfolio divergence requires review"),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif bool(authority_assessment.get("needs_correction")):
            stage = "AUTHORITY_CORRECTION_PENDING"
            blockers.append("POSITION_AUTHORITY_CORRECTION_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = (
                "apply_rebuild_position_authority"
                if bool(authority_assessment.get("safe_to_correct"))
                else "review_position_authority_evidence"
            )
            recommended_command = (
                "uv run python bot.py rebuild-position-authority --apply --yes"
                if bool(authority_assessment.get("safe_to_correct"))
                else "uv run python bot.py rebuild-position-authority"
            )
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_CORRECTION_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=str(authority_assessment.get("reason") or "lot authority correction required"),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif str(position_state.normalized_exposure.authority_gap_reason or "") == "authority_missing_recovery_required":
            stage = "AUTHORITY_REBUILD_PENDING"
            blockers.append("POSITION_AUTHORITY_RECOVERY_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "rebuild_position_authority"
            recommended_command = "uv run python bot.py rebuild-position-authority --apply --yes"
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_RECOVERY_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=str(position_state.normalized_exposure.authority_gap_reason or "authority missing"),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif projection_non_convergence_blocking:
            stage = "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
            blockers.append("POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "review_position_authority_evidence"
            blocking_incident_class = str(authority_assessment.get("incident_class") or "NONE")
            recommended_command = (
                "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
                if blocking_incident_class == "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"
                else "uv run python bot.py rebuild-position-authority"
            )
            structured_blockers.append(
                _make_structured_blocker(
                    code="POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED",
                    category="executable_authority",
                    stage=stage,
                    detail=(
                        "projection_convergence_required="
                        f"projected_total_qty={float(projection_convergence.get('projected_total_qty') or 0.0):.12f},"
                        f"portfolio_qty={float(projection_convergence.get('portfolio_qty') or 0.0):.12f},"
                        f"reason={projection_convergence.get('reason') or 'none'},"
                        f"incident_class={blocking_incident_class}"
                    ),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif fee_gap_policy.closeout_blocking and not fee_gap_policy.resume_blocking:
            stage = fee_gap_policy.readiness_stage
            categories.append(fee_gap_policy.blocker_category)
            operator_next_action = fee_gap_policy.operator_next_action
            recommended_command = fee_gap_policy.recommended_command
        elif fee_gap_policy.resume_blocking:
            stage = fee_gap_policy.readiness_stage
            blockers.append("FEE_GAP_RECOVERY_REQUIRED")
            categories.append(fee_gap_policy.blocker_category)
            operator_next_action = fee_gap_policy.operator_next_action
            recommended_command = fee_gap_policy.recommended_command
        elif bool(replay_mismatch_preview.get("needs_repair")) and bool(replay_mismatch_preview.get("safe_to_apply")):
            stage = "ACCOUNTING_EXTERNAL_POSITION_REPAIR_PENDING"
            blockers.append("EXTERNAL_POSITION_ACCOUNTING_REPAIR_REQUIRED")
            categories.append("accounting_truth")
            operator_next_action = "apply_external_position_accounting_repair"
            recommended_command = "uv run python bot.py external-position-accounting-repair --apply --yes"
        elif bool(replay_mismatch_preview.get("needs_repair")):
            stage = "ACCOUNTING_REPLAY_MISMATCH_PENDING"
            blockers.append("ACCOUNTING_REPLAY_MISMATCH_REVIEW_REQUIRED")
            categories.append("accounting_truth")
            operator_next_action = "review_accounting_replay_evidence"
            recommended_command = "uv run python bot.py external-position-accounting-repair"
        elif open_order_count > 0 or recovery_required_count > 0:
            stage = "RESUME_BLOCKED_BY_POLICY"
            blockers.append("ORDER_RECOVERY_REQUIRED")
            categories.append("runtime_resume_gate")
            operator_next_action = "recover_or_reconcile_orders"
            recommended_command = "uv run python bot.py recovery-report"
        elif bool(state.halt_new_orders_blocked or state.halt_state_unresolved):
            stage = "RESUME_BLOCKED_BY_POLICY"
            blockers.append("HALT_STATE_UNRESOLVED")
            categories.append("runtime_resume_gate")
            operator_next_action = "review_halt_state"
            recommended_command = "uv run python bot.py recovery-report"
        elif residual_inventory_mode == "track" and _is_residual_inventory_trackable(
            residual_inventory=residual_inventory,
            projection_convergence=projection_convergence,
            authority_assessment=authority_assessment,
            broker_position_evidence=broker_position_evidence,
            open_order_count=open_order_count,
            recovery_required_count=recovery_required_count,
            fee_validation_blocked_count=fee_validation_blocked_count,
            unapplied_principal_pending_count=unapplied_principal_pending_count,
            fee_gap_resume_blocking=fee_gap_policy.resume_blocking,
        ):
            stage = "RESIDUAL_INVENTORY_TRACKED"
            operator_next_action = "run_with_residual_inventory_tracking"
            recommended_command = "uv run python bot.py resume"
            residual_inventory_state = "RESIDUAL_INVENTORY_TRACKED"
        elif residual_inventory_mode == "track" and residual_inventory.material_residual:
            stage = "RESIDUAL_INVENTORY_UNRESOLVED"
            blockers.append("RESIDUAL_INVENTORY_UNRESOLVED")
            categories.append("tradeability_policy")
            operator_next_action = "review_residual_inventory_evidence"
            recommended_command = "uv run python bot.py recovery-report"
            residual_inventory_state = "RESIDUAL_INVENTORY_UNRESOLVED"
        elif _is_non_executable_residual_holdings_state(
            residual_inventory=residual_inventory,
            position_state=position_state,
            projection_convergence=projection_convergence,
            authority_assessment=authority_assessment,
            broker_position_evidence=broker_position_evidence,
            open_order_count=open_order_count,
            recovery_required_count=recovery_required_count,
            fee_validation_blocked_count=fee_validation_blocked_count,
            unapplied_principal_pending_count=unapplied_principal_pending_count,
            fee_gap_resume_blocking=fee_gap_policy.resume_blocking,
        ):
            stage = "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
            blockers.append("NON_EXECUTABLE_RESIDUAL_HOLDINGS")
            categories.append("tradeability_policy")
            operator_next_action = "residual_policy_review"
            recommended_command = "uv run bithumb-bot residual-closeout-plan"
            residual_inventory_state = "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
            structured_blockers.append(
                _make_structured_blocker(
                    code="NON_EXECUTABLE_RESIDUAL_HOLDINGS",
                    category="tradeability_policy",
                    stage=stage,
                    detail=(
                        "broker/portfolio/projection converge, but only tracked non-executable residual holdings remain; "
                        f"residual_qty={float(residual_inventory.residual_qty):.12f} "
                        f"exchange_sellable={1 if residual_inventory.exchange_sellable else 0} "
                        f"residual_classes={'|'.join(residual_inventory.residual_classes) or 'none'}"
                    ),
                    operator_next_action=operator_next_action,
                    recommended_command=recommended_command,
                    projection_convergence=projection_convergence,
                    authority_truth_model=authority_truth_model,
                    authority_assessment=authority_assessment,
                )
            )
        elif residual_inventory_mode == "ignore_tiny" and _ignore_tiny_policy_eligible(
            residual_inventory=residual_inventory
        ):
            residual_inventory_state = "RESIDUAL_INVENTORY_TRACKED"
        elif residual_inventory.material_residual:
            residual_inventory_state = "RESIDUAL_INVENTORY_UNRESOLVED"

        resume_ready = not blockers
        run_loop_policy_allowed = bool(
            resume_ready
            or stage in {"UNAPPLIED_PRINCIPAL_PENDING", "FEE_FINALIZATION_PENDING"}
            or str(stage).startswith("RESUME_READY")
        )
        tradeability = classify_canonical_tradeability_state(
            position_state=position_state,
            recovery_state=canonical_recovery,
            run_loop_allowed=run_loop_policy_allowed,
        )
        if stage == "UNAPPLIED_PRINCIPAL_PENDING":
            reasons = [str(tradeability.why_not or "none")]
            if "unapplied_principal_pending" not in reasons:
                reasons.append("unapplied_principal_pending")
            tradeability = replace(
                tradeability,
                new_entry_allowed=False,
                closeout_allowed=False,
                why_not=";".join(item for item in reasons if item and item != "none"),
                operator_next_action="wait_for_auto_reconcile_or_review_fee_evidence",
            )
        elif stage == "FEE_FINALIZATION_PENDING":
            reasons = [str(tradeability.why_not or "none")]
            if "fee_finalization_pending" not in reasons:
                reasons.append("fee_finalization_pending")
            tradeability = replace(
                tradeability,
                new_entry_allowed=False,
                why_not=";".join(item for item in reasons if item and item != "none"),
                operator_next_action="wait_for_auto_reconcile_or_review_fee_evidence",
            )
        elif stage == "FEE_VALIDATION_BLOCKED":
            reasons = [str(tradeability.why_not or "none")]
            if "fee_validation_blocked" not in reasons:
                reasons.append("fee_validation_blocked")
            tradeability = replace(
                tradeability,
                new_entry_allowed=False,
                closeout_allowed=False,
                why_not=";".join(item for item in reasons if item and item != "none"),
                operator_next_action="review_fee_evidence",
            )
        elif stage == "NON_EXECUTABLE_RESIDUAL_HOLDINGS":
            reasons = [str(tradeability.why_not or "none"), "non_executable_residual_holdings"]
            tradeability = replace(
                tradeability,
                residual_class="NON_EXECUTABLE_RESIDUAL_HOLDINGS",
                run_loop_allowed=False,
                position_management_allowed=False,
                new_entry_allowed=False,
                closeout_allowed=False,
                operator_action_required=True,
                why_not=";".join(item for item in reasons if item and item != "none"),
                operator_next_action="residual_policy_review",
            )
        elif stage == "RESIDUAL_INVENTORY_UNRESOLVED":
            reasons = [str(tradeability.why_not or "none"), "residual_inventory_unresolved"]
            tradeability = replace(
                tradeability,
                residual_class="RESIDUAL_INVENTORY_UNRESOLVED",
                run_loop_allowed=False,
                position_management_allowed=False,
                new_entry_allowed=False,
                closeout_allowed=False,
                operator_action_required=True,
                why_not=";".join(item for item in reasons if item and item != "none"),
                operator_next_action="review_residual_inventory_evidence",
            )
        elif stage == "RESIDUAL_INVENTORY_TRACKED":
            tradeability = replace(
                tradeability,
                residual_class="RESIDUAL_INVENTORY_TRACKED",
                run_loop_allowed=True,
                position_management_allowed=True,
                new_entry_allowed=bool(position_state.normalized_exposure.entry_allowed),
                closeout_allowed=bool(residual_inventory.exchange_sellable),
                operator_action_required=False,
                why_not="none",
                operator_next_action="run_with_residual_inventory_tracking",
            )
        residual_inventory_policy_allows_run = bool(stage == "RESIDUAL_INVENTORY_TRACKED")
        residual_inventory_policy_allows_buy = bool(
            residual_inventory_policy_allows_run and tradeability.new_entry_allowed
        )
        residual_inventory_policy_allows_sell = bool(
            residual_inventory_policy_allows_run and residual_inventory.exchange_sellable
        )
        residual_sell_candidate = _build_residual_sell_candidate_summary(
            residual_inventory=residual_inventory,
            residual_inventory_mode=residual_inventory_mode,
            residual_inventory_state=residual_inventory_state,
        )
        total_effective_exposure_qty = float(position_state.normalized_exposure.open_exposure_qty)
        total_effective_exposure_notional_krw: float | None = None
        if residual_inventory_state == "RESIDUAL_INVENTORY_TRACKED":
            total_effective_exposure_qty += float(residual_inventory.residual_qty)
            total_effective_exposure_notional_krw = residual_inventory.residual_notional_krw
        tradeability_operator_fields = build_tradeability_operator_fields(
            tradeability=tradeability,
            dust_fields=dust_context.fields,
        )

        next_action = operator_next_action
        if (
            resume_ready
            and stage != "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
            and str(tradeability.operator_next_action) == "resume_position_management"
        ):
            next_action = tradeability.operator_next_action
        elif tradeability.operator_action_required and resume_ready:
            next_action = tradeability.operator_next_action

        return RuntimeReadinessSnapshot(
            recovery_stage=stage,
            resume_ready=resume_ready,
            resume_blockers=tuple(blockers),
            blocker_categories=tuple(dict.fromkeys(categories)),
            operator_next_action=next_action,
            recommended_command=recommended_command,
            position_state=position_state,
            lot_snapshot=lot_snapshot,
            reconcile_metadata=metadata,
            fee_pending_count=fee_pending_count,
            auto_recovery_count=max(
                accounting_pending_count,
                unapplied_principal_pending_count + principal_applied_fee_pending_count,
            ),
            fill_accounting_incident_summary=fill_accounting_incident_summary,
            fee_gap_recovery_required=fee_gap_required,
            fee_gap_resume_blocking=fee_gap_policy.resume_blocking,
            fee_gap_resume_policy=fee_gap_policy.resume_policy,
            fee_gap_closeout_blocking=fee_gap_policy.closeout_blocking,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            fee_gap_incident=fee_gap_incident,
            open_order_count=open_order_count,
            unresolved_open_order_count=unresolved_open_order_count,
            recovery_required_count=recovery_required_count,
            submit_unknown_count=submit_unknown_count,
            position_authority_assessment=authority_assessment,
            projection_convergence=projection_convergence,
            canonical_state=canonical_recovery.canonical_state,
            residual_class=tradeability.residual_class,
            run_loop_allowed=tradeability.run_loop_allowed,
            position_management_allowed=tradeability.position_management_allowed,
            new_entry_allowed=tradeability.new_entry_allowed,
            closeout_allowed=tradeability.closeout_allowed,
            effective_flat=tradeability.effective_flat,
            operator_action_required=tradeability.operator_action_required,
            why_not=tradeability.why_not,
            execution_flat=canonical_recovery.execution_flat,
            accounting_flat=canonical_recovery.accounting_flat,
            tradeability=tradeability,
            tradeability_operator_fields=tradeability_operator_fields,
            residual_inventory=residual_inventory,
            residual_inventory_mode=residual_inventory_mode,
            residual_inventory_state=residual_inventory_state,
            residual_inventory_policy_allows_run=residual_inventory_policy_allows_run,
            residual_inventory_policy_allows_buy=residual_inventory_policy_allows_buy,
            residual_inventory_policy_allows_sell=residual_inventory_policy_allows_sell,
            total_effective_exposure_qty=total_effective_exposure_qty,
            total_effective_exposure_notional_krw=total_effective_exposure_notional_krw,
            residual_sell_candidate=residual_sell_candidate,
            residual_proof_min_qty=(None if lot_definition is None else _optional_float(lot_definition.min_qty)),
            residual_proof_min_notional_krw=(
                None if lot_definition is None else _optional_float(lot_definition.min_notional_krw)
            ),
            residual_proof_locked_qty=float(broker_position_evidence.get("asset_locked") or 0.0),
            active_fee_accounting_blocker=bool(
                fee_validation_blocked_count > 0
                or unapplied_principal_pending_count > 0
                or fee_gap_policy.closeout_blocking
            ),
            accounting_projection_ok=bool(projection_convergence.get("converged")),
            idempotency_scope="live_client_order_id_generator",
            structured_blockers=tuple(structured_blockers),
            authority_truth_model=authority_truth_model,
            broker_position_evidence=broker_position_evidence,
            inspect_only_mode=any(bool(item.get("inspect_only")) for item in structured_blockers),
        )
    finally:
        if close_conn:
            conn.close()
