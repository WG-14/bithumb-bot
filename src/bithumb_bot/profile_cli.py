from __future__ import annotations

import json
from pathlib import Path

from .approved_profile import (
    ApprovedProfileError,
    build_approved_profile,
    default_profile_output_path,
    diff_profile_to_runtime,
    expected_profile_modes_for_runtime,
    load_approved_profile,
    parse_env_file,
    promote_profile_mode,
    runtime_contract_from_env_values,
    verify_profile_against_runtime,
    write_approved_profile_atomic,
)
from .config import PATH_MANAGER, settings
from .research.promotion_gate import PromotionGateError


def _load_json(path: str) -> dict[str, object]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ApprovedProfileError("payload_not_object")
    return payload


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def cmd_profile_generate(
    *,
    promotion_path: str,
    mode: str,
    out_path: str | None,
    market: str | None = None,
    interval: str | None = None,
) -> int:
    try:
        if str(mode or "").strip().lower() != "paper":
            raise ApprovedProfileError("profile_generate_requires_paper_mode_use_profile-promote_for_live_modes")
        promotion = _load_json(promotion_path)
        profile_market = str(market or promotion.get("market") or "").strip()
        profile_interval = str(interval or promotion.get("interval") or "").strip()
        if not profile_market:
            raise ApprovedProfileError("market_missing: pass --market for old promotion artifacts")
        if not profile_interval:
            raise ApprovedProfileError("interval_missing: pass --interval for old promotion artifacts")
        profile = build_approved_profile(
            promotion=promotion,
            mode=mode,
            source_promotion_path=promotion_path,
            market=profile_market,
            interval=profile_interval,
            manager=PATH_MANAGER,
        )
        resolved_out = Path(out_path).expanduser() if out_path else default_profile_output_path(
            manager=PATH_MANAGER,
            profile=profile,
        )
        resolved_out = write_approved_profile_atomic(resolved_out, profile, manager=PATH_MANAGER)
    except (ApprovedProfileError, PromotionGateError, OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "profile-generate"})
        return 1
    _print_json(
        {
            "ok": True,
            "command": "profile-generate",
            "profile_path": str(resolved_out.resolve()),
            "profile_hash": profile.get("profile_content_hash"),
            "profile_mode": profile.get("profile_mode"),
            "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
            "candidate_profile_hash": profile.get("candidate_profile_hash"),
            "manifest_hash": profile.get("manifest_hash"),
            "dataset_content_hash": profile.get("dataset_content_hash"),
            "next_action": "operator_review_then_profile-verify_against_target_env",
        }
    )
    return 0


def cmd_profile_diff(*, profile_path: str, target_env: str, as_json: bool) -> int:
    try:
        profile = load_approved_profile(profile_path)
        runtime = runtime_contract_from_env_values(parse_env_file(target_env))
        mismatches = diff_profile_to_runtime(profile, runtime, profile_path=profile_path)
    except (ApprovedProfileError, OSError, ValueError) as exc:
        payload = {"ok": False, "error": str(exc), "command": "profile-diff"}
        _print_json(payload) if as_json else print(f"[PROFILE-DIFF] error={exc}")
        return 1
    payload = {
        "ok": len(mismatches) == 0,
        "command": "profile-diff",
        "profile_path": str(Path(profile_path).expanduser()),
        "target_env": str(Path(target_env).expanduser()),
        "profile_hash": profile.get("profile_content_hash"),
        "mismatch_count": len(mismatches),
        "mismatches": [dict(item) for item in mismatches],
        "source_promotion_verified": False,
        "evidence_verified": False,
        "use_profile_verify_for_artifact_chain": True,
    }
    if as_json:
        _print_json(payload)
    else:
        print("[PROFILE-DIFF]")
        print(f"  profile_path={payload['profile_path']}")
        print(f"  target_env={payload['target_env']}")
        print(f"  profile_hash={payload['profile_hash']}")
        print("  source_promotion_verified=False")
        print("  evidence_verified=False")
        print("  use_profile_verify_for_artifact_chain=True")
        print(f"  mismatch_count={payload['mismatch_count']}")
        for item in mismatches:
            print(f"  mismatch field={item['field']} expected={item['expected']} actual={item['actual']}")
    return 0 if not mismatches else 1


def cmd_profile_verify(*, profile_path: str, env_path: str) -> int:
    try:
        runtime = runtime_contract_from_env_values(parse_env_file(env_path))
        expected_modes, mode_reason = expected_profile_modes_for_runtime(runtime)
        result = verify_profile_against_runtime(
            profile_path=profile_path,
            runtime=runtime,
            require_profile=True,
            expected_profile_modes=expected_modes,
            expected_profile_mode_reason=mode_reason,
            verify_source_promotion=True,
        )
    except (ApprovedProfileError, OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "profile-verify"})
        return 1
    payload = {
        "ok": result.ok,
        "command": "profile-verify",
        "reason": result.reason,
        **result.audit_fields(),
    }
    _print_json(payload)
    return 0 if result.ok else 1


def cmd_profile_promote(
    *,
    profile_path: str,
    mode: str,
    out_path: str | None,
    paper_validation_evidence: str | None,
    live_readiness_evidence: str | None,
) -> int:
    try:
        parent = load_approved_profile(profile_path)
        child = promote_profile_mode(
            parent_profile=parent,
            target_mode=mode,
            paper_validation_evidence=paper_validation_evidence,
            live_readiness_evidence=live_readiness_evidence,
            manager=PATH_MANAGER,
        )
        resolved_out = Path(out_path).expanduser() if out_path else default_profile_output_path(
            manager=PATH_MANAGER,
            profile=child,
        )
        resolved_out = write_approved_profile_atomic(resolved_out, child, manager=PATH_MANAGER)
    except (ApprovedProfileError, OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "profile-promote"})
        return 1
    _print_json(
        {
            "ok": True,
            "command": "profile-promote",
            "profile_path": str(resolved_out.resolve()),
            "profile_hash": child.get("profile_content_hash"),
            "profile_mode": child.get("profile_mode"),
            "parent_profile_hash": child.get("parent_profile_hash"),
        }
    )
    return 0
