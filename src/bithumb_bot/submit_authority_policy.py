from __future__ import annotations

from dataclasses import dataclass


TARGET_DELTA_SUBMIT_SOURCE = "target_delta"
TARGET_DELTA_SUBMIT_AUTHORITIES = frozenset(
    {
        "canonical_target_delta_sizing",
        "target_position_delta",
    }
)
RESIDUAL_SUBMIT_SOURCE = "residual_inventory"
RESIDUAL_SUBMIT_AUTHORITIES = frozenset({"residual_inventory_policy"})
LEGACY_BUY_SUBMIT_SOURCES = frozenset({"strategy_position"})
LEGACY_BUY_SUBMIT_AUTHORITIES = frozenset(
    {
        "configured_strategy_order_size",
        "residual_inventory_delta",
        "strategy_execution_intent",
        "research_compatibility_execution_intent",
    }
)


@dataclass(frozen=True)
class SubmitAuthorityPolicy:
    submit_authority_mode: str
    live_real_order_requires_target_delta: bool
    legacy_lot_native_compat_enabled: bool
    allowed_submit_plan_sources: tuple[str, ...]
    allowed_submit_plan_authorities: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "submit_authority_mode": self.submit_authority_mode,
            "live_real_order_requires_target_delta": bool(
                self.live_real_order_requires_target_delta
            ),
            "legacy_lot_native_compat_enabled": bool(self.legacy_lot_native_compat_enabled),
            "allowed_submit_plan_sources": list(self.allowed_submit_plan_sources),
            "allowed_submit_plan_authorities": list(self.allowed_submit_plan_authorities),
        }


def live_real_order_enabled(settings_obj: object) -> bool:
    return (
        str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live"
        and not bool(getattr(settings_obj, "LIVE_DRY_RUN", True))
        and bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False))
    )


def submit_authority_policy_from_settings(settings_obj: object) -> SubmitAuthorityPolicy:
    if live_real_order_enabled(settings_obj):
        return SubmitAuthorityPolicy(
            submit_authority_mode="live_real_order_target_delta_only",
            live_real_order_requires_target_delta=True,
            legacy_lot_native_compat_enabled=False,
            allowed_submit_plan_sources=(TARGET_DELTA_SUBMIT_SOURCE, RESIDUAL_SUBMIT_SOURCE),
            allowed_submit_plan_authorities=tuple(
                sorted(TARGET_DELTA_SUBMIT_AUTHORITIES | RESIDUAL_SUBMIT_AUTHORITIES)
            ),
        )
    if str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live":
        return SubmitAuthorityPolicy(
            submit_authority_mode="live_dry_run_non_submitting_compat",
            live_real_order_requires_target_delta=False,
            legacy_lot_native_compat_enabled=True,
            allowed_submit_plan_sources=tuple(
                sorted({TARGET_DELTA_SUBMIT_SOURCE, RESIDUAL_SUBMIT_SOURCE} | LEGACY_BUY_SUBMIT_SOURCES)
            ),
            allowed_submit_plan_authorities=tuple(
                sorted(
                    TARGET_DELTA_SUBMIT_AUTHORITIES
                    | RESIDUAL_SUBMIT_AUTHORITIES
                    | LEGACY_BUY_SUBMIT_AUTHORITIES
                )
            ),
        )
    return SubmitAuthorityPolicy(
        submit_authority_mode="paper_research_compat",
        live_real_order_requires_target_delta=False,
        legacy_lot_native_compat_enabled=True,
        allowed_submit_plan_sources=tuple(
            sorted(
                {
                    TARGET_DELTA_SUBMIT_SOURCE,
                    RESIDUAL_SUBMIT_SOURCE,
                    "research_backtest",
                }
                | LEGACY_BUY_SUBMIT_SOURCES
            )
        ),
        allowed_submit_plan_authorities=tuple(
            sorted(
                TARGET_DELTA_SUBMIT_AUTHORITIES
                | RESIDUAL_SUBMIT_AUTHORITIES
                | LEGACY_BUY_SUBMIT_AUTHORITIES
                | {"target_position_delta"}
            )
        ),
    )


def live_real_order_legacy_buy_submit_plan_error(
    plan: object,
    *,
    settings_obj: object,
) -> str | None:
    policy = submit_authority_policy_from_settings(settings_obj)
    if not policy.live_real_order_requires_target_delta:
        return None
    payload = plan.as_dict() if hasattr(plan, "as_dict") else dict(plan or {})
    side = str(payload.get("side") or "").strip().upper()
    source = str(payload.get("source") or "").strip()
    authority = str(payload.get("authority") or "").strip()
    if side == "BUY" and (
        source != TARGET_DELTA_SUBMIT_SOURCE
        or authority not in TARGET_DELTA_SUBMIT_AUTHORITIES
    ):
        return "live_real_order_buy_submit_plan_requires_target_delta"
    return None
