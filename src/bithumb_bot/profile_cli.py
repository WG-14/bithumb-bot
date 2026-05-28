from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .canonical_decision import (
    export_runtime_replay_decisions,
    order_rules_snapshot_payload,
)
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
from .evidence_chain import evidence_report_failure_payload
from .evidence_chain import (
    build_candidate_regime_policy_equivalence_evidence,
    validate_candidate_regime_policy_equivalence_evidence,
)
from .decision_equivalence import (
    compare_decision_equivalence,
    compare_decision_export_artifacts,
    compute_decision_equivalence_hash,
    compute_decision_export_hash,
    load_decision_export_artifact,
    load_decision_list,
    promotion_grade_decision_equivalence_fail_reasons,
)
from .research.dataset_snapshot import load_dataset_split
from .research.backtest_engine import BacktestRunContext
from .research.experiment_manifest import load_manifest
from .research.hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from .research.decision_export_normalizers import (
    decision_export_execution_timing_policy_hash as _decision_export_execution_timing_policy_hash,
    generic_promotion_grade_research_export_decisions as _generic_promotion_grade_research_export_decisions,
)
from .research.parameter_space import candidate_id, iter_parameter_candidates
from .research.promotion_gate import PromotionGateError, build_candidate_profile
from .research.strategy_registry import resolve_research_strategy_plugin
from .research.strategy_spec import materialize_strategy_parameters
from .research.strategy_spec import strategy_spec_for_name
from .storage_io import write_json_atomic
from .broker.order_rules import get_effective_order_rules


def _load_json(path: str) -> dict[str, object]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ApprovedProfileError("payload_not_object")
    return payload


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _profile_execution_capability_summary(profile: dict[str, object]) -> dict[str, object]:
    capability = profile.get("execution_capability_contract")
    if not isinstance(capability, dict):
        return {
            "execution_capability_contract_hash": None,
            "evidence_tier": None,
            "unavailable_required_capabilities": [],
            "market_impact_required": None,
            "market_impact_model_available": None,
            "top_of_book_is_full_depth": None,
        }
    required = capability.get("strategy_required_capabilities")
    if not isinstance(required, dict):
        required = {}
    available = capability.get("available_capabilities")
    if not isinstance(available, dict):
        available = {}
    return {
        "execution_capability_contract_hash": (
            profile.get("execution_capability_contract_hash")
            or capability.get("execution_capability_contract_hash")
        ),
        "evidence_tier": capability.get("evidence_tier"),
        "unavailable_required_capabilities": list(capability.get("unavailable_required_capabilities") or []),
        "market_impact_required": bool(required.get("market_impact_model")),
        "market_impact_model_available": bool(available.get("market_impact_model")),
        "top_of_book_is_full_depth": bool(available.get("top_of_book_is_full_depth")),
    }


def _candidate_regime_policy_summary(profile: dict[str, object]) -> dict[str, object]:
    required = bool(profile.get("candidate_regime_policy_required_for_live"))
    applied = bool(profile.get("candidate_regime_policy_applied_in_research"))
    evidence_hash = profile.get("candidate_regime_policy_equivalence_evidence_hash")
    verified = str(evidence_hash or "").startswith("sha256:") and bool(
        profile.get("candidate_regime_policy_equivalence_evidence_path")
    )
    if not required or applied:
        next_action = "none"
    elif verified:
        next_action = "continue_promotion_profile_verification"
    else:
        next_action = "generate_and_bind_candidate_regime_policy_equivalence_evidence"
    return {
        "candidate_regime_policy_applied_in_research": applied,
        "candidate_regime_policy_required_for_live": required,
        "candidate_regime_policy_equivalence_required": bool(
            profile.get("candidate_regime_policy_equivalence_required")
        ),
        "candidate_regime_policy_equivalence_evidence_hash": evidence_hash,
        "candidate_regime_policy_equivalence_evidence_path": profile.get(
            "candidate_regime_policy_equivalence_evidence_path"
        ),
        "candidate_regime_policy_evidence_verified": verified,
        "candidate_regime_policy_limitation_reasons": list(
            profile.get("candidate_regime_policy_limitation_reasons") or []
        ),
        "candidate_regime_policy_next_action": next_action,
    }


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
            **_profile_execution_capability_summary(profile),
            **_candidate_regime_policy_summary(profile),
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
        artifact_path = paper_validation_evidence if paper_validation_evidence else live_readiness_evidence
        _print_json(
            evidence_report_failure_payload(
                command="profile-promote",
                error=str(exc),
                artifact_path=artifact_path,
            )
        )
        return 1
    _print_json(
        {
            "ok": True,
            "command": "profile-promote",
            "profile_path": str(resolved_out.resolve()),
            "profile_hash": child.get("profile_content_hash"),
            "profile_mode": child.get("profile_mode"),
            "parent_profile_hash": child.get("parent_profile_hash"),
            **_profile_execution_capability_summary(child),
            **_candidate_regime_policy_summary(child),
        }
    )
    return 0


def cmd_decision_equivalence(
    *,
    research_decisions_path: str,
    runtime_decisions_path: str,
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
) -> int:
    try:
        try:
            result = compare_decision_export_artifacts(
                research_artifact=load_decision_export_artifact(research_decisions_path, expected_source="research"),
                runtime_artifact=load_decision_export_artifact(runtime_decisions_path, expected_source="runtime_replay"),
                profile_hash=profile_hash,
                market=market,
                interval=interval,
                data_fingerprint=data_fingerprint,
            )
        except ValueError as artifact_exc:
            result = compare_decision_equivalence(
                research_decisions=load_decision_list(research_decisions_path),
                runtime_decisions=load_decision_list(runtime_decisions_path),
                profile_hash=profile_hash,
                market=market,
                interval=interval,
                data_fingerprint=data_fingerprint,
            )
            report = dict(result.report)
            reason_codes = sorted(set(list(report.get("reason_codes") or ()) + ["decision_export_artifact_unverified"]))
            report.update(
                {
                    "ok": False,
                    "promotion_grade_comparison": False,
                    "legacy_or_unverified_export": True,
                    "repo_owned_export_artifacts": False,
                    "export_artifact_validation_error": str(artifact_exc),
                    "reason_codes": reason_codes,
                    "outcome": "FAIL_EXPORT_BINDING",
                    "recommended_next_action": "regenerate_decisions_with_repo_owned_export_commands",
                }
            )
            report["content_hash"] = compute_decision_equivalence_hash(report)
            result = type(result)(report=report)
    except (OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "decision-equivalence"})
        return 1
    gate_reasons = promotion_grade_decision_equivalence_fail_reasons(result.report)
    if gate_reasons:
        report = dict(result.report)
        reason_codes = sorted(set(list(report.get("reason_codes") or ()) + list(gate_reasons)))
        report.update(
            {
                "ok": False,
                "promotion_grade_comparison": False,
                "promotion_gate_reason_codes": list(gate_reasons),
                "reason_codes": reason_codes,
            }
        )
        report["content_hash"] = compute_decision_equivalence_hash(report)
        result = type(result)(report=report)
    _print_json({"command": "decision-equivalence", **result.report})
    return 0 if result.ok else 1


def cmd_candidate_regime_policy_equivalence_evidence(
    *,
    backtest_report_path: str,
    candidate_id_value: str,
    decision_equivalence_report_path: str,
    out_path: str | None,
    bind: bool,
) -> int:
    try:
        report_path = Path(backtest_report_path).expanduser().resolve()
        report = _load_json(str(report_path))
        candidates = report.get("candidates")
        if not isinstance(candidates, list):
            raise ValueError("backtest_report_candidates_missing")
        candidate = next(
            (
                item
                for item in candidates
                if isinstance(item, dict)
                and str(item.get("parameter_candidate_id") or item.get("candidate_id") or "") == candidate_id_value
            ),
            None,
        )
        if not isinstance(candidate, dict):
            raise ValueError("candidate_id_not_found")
        decision_path = Path(decision_equivalence_report_path).expanduser().resolve()
        decision_report = _load_json(str(decision_path))
        candidate_contract = dict(candidate)
        for key in (
            "candidate_regime_policy_equivalence_evidence_hash",
            "candidate_regime_policy_equivalence_evidence_path",
            "candidate_regime_policy_equivalence_evidence_status",
            "candidate_profile_evidence_contract_hash",
        ):
            candidate_contract.pop(key, None)
        candidate_profile_contract_hash = sha256_prefixed(build_candidate_profile(candidate_contract))
        evidence = build_candidate_regime_policy_equivalence_evidence(
            candidate={**candidate_contract, "candidate_profile_evidence_contract_hash": candidate_profile_contract_hash},
            decision_equivalence_report=decision_report,
            candidate_profile_contract_hash=candidate_profile_contract_hash,
            decision_equivalence_report_path=decision_path,
        )
        resolved_out = (
            Path(out_path).expanduser().resolve()
            if out_path
            else report_path.parent / f"candidate_regime_policy_equivalence_{candidate_id_value}.json"
        )
        if PATH_MANAGER._is_within(resolved_out, PATH_MANAGER.project_root.resolve()):
            raise ValueError("candidate_regime_policy_equivalence_evidence_output_repo_local_not_allowed")
        write_json_atomic(resolved_out, evidence)
        validate_candidate_regime_policy_equivalence_evidence(
            evidence,
            candidate_or_profile={
                **candidate_contract,
                "candidate_profile_evidence_contract_hash": candidate_profile_contract_hash,
            },
            expected_hash=str(evidence["content_hash"]),
            evidence_path=resolved_out,
        )
        if bind:
            candidate["candidate_profile_evidence_contract_hash"] = candidate_profile_contract_hash
            candidate["candidate_regime_policy_equivalence_evidence_path"] = str(resolved_out)
            candidate["candidate_regime_policy_equivalence_evidence_hash"] = evidence["content_hash"]
            candidate["candidate_regime_policy_equivalence_evidence_status"] = "verified"
            candidate["candidate_regime_policy_equivalence_required"] = bool(
                candidate.get("candidate_regime_policy_equivalence_required")
            )
            candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
            report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
            write_json_atomic(report_path, report)
    except (OSError, ValueError) as exc:
        _print_json(
            {
                "ok": False,
                "error": str(exc),
                "command": "candidate-regime-policy-equivalence-evidence",
            }
        )
        return 1
    _print_json(
        {
            "ok": True,
            "command": "candidate-regime-policy-equivalence-evidence",
            "evidence_path": str(resolved_out),
            "content_hash": evidence["content_hash"],
            "candidate_id": candidate_id_value,
            "candidate_profile_evidence_contract_hash": candidate_profile_contract_hash,
            "bound_to_backtest_report": bind,
            "next_action": "research-promote-candidate" if bind else "rerun_with_--bind",
        }
    )
    return 0


def cmd_research_export_decisions(
    *,
    manifest_path: str,
    candidate_id_value: str,
    split: str,
    out_path: str,
    profile_path: str | None = None,
) -> int:
    try:
        manifest = load_manifest(manifest_path)
        snapshot = load_dataset_split(db_path=settings.DB_PATH, manifest=manifest, split_name=split)
        params = _candidate_params_from_manifest(manifest, candidate_id_value)
        profile = load_approved_profile(profile_path) if profile_path else None
        promotion_grade_export = profile is not None
        if promotion_grade_export:
            _require_explicit_runtime_bound_strategy_parameters(
                strategy_name=str(manifest.strategy_name),
                parameter_values=params,
            )
        profile_hash = _research_export_profile_hash(
            manifest=manifest,
            snapshot=snapshot,
            params=params,
            candidate_id_value=candidate_id_value,
            profile=profile,
        )
        plugin = resolve_research_strategy_plugin(manifest.strategy_name)
        scenario = manifest.execution_model.scenarios[0]
        run = plugin.runner(
            snapshot,
            params,
            float(scenario.fee_rate),
            float(scenario.slippage_bps),
            None,
            None,
            manifest.execution_timing,
            None,
            BacktestRunContext(
                report_detail="full",
                approved_profile=profile if isinstance(profile, dict) else None,
                candidate_regime_policy=(
                    _candidate_regime_policy_from_approved_profile(profile)
                    if isinstance(profile, dict)
                    else None
                ),
                candidate_regime_policy_drives_research_execution=(
                    bool(promotion_grade_export)
                    if isinstance(profile, dict)
                    else False
                ),
                policy_materialization_mode=(
                    "research_promotion" if promotion_grade_export else "research_exploratory"
                ),
            ),
        )
        order_rules_hash = sha256_prefixed(
            order_rules_snapshot_payload(get_effective_order_rules(manifest.market), pair=manifest.market)
        )
        raw_decisions = [
            {
                **item,
                "profile_content_hash": profile_hash,
                "candidate_profile_hash": "" if profile is None else str(profile.get("candidate_profile_hash") or ""),
                "dataset_content_hash": snapshot.content_hash(),
                "db_data_fingerprint": snapshot.content_hash(),
                "order_rules_hash": order_rules_hash,
                "strategy_version": plugin.version,
                "strategy_decision_contract_version": plugin.decision_contract_version,
            }
            for item in run.decisions
        ]
        if promotion_grade_export:
            if plugin.research_export_normalizer is None:
                decisions = _generic_promotion_grade_research_export_decisions(
                    raw_decisions=raw_decisions,
                    snapshot=snapshot,
                    params=params,
                    profile=profile or {},
                    order_rules_hash=order_rules_hash,
                )
            else:
                decisions = plugin.research_export_normalizer(
                    raw_decisions,
                    snapshot,
                    params,
                    profile or {},
                    order_rules_hash,
                )
        else:
            decisions = raw_decisions
        payload = _decision_export_payload(
            source="research" if promotion_grade_export else "research_legacy_unbound",
            profile_content_hash=profile_hash,
            data_fingerprint=snapshot.content_hash(),
            market=manifest.market,
            interval=manifest.interval,
            decisions=decisions,
            promotion_grade_export=promotion_grade_export,
            strategy_plugin_contract=plugin.contract_payload() if promotion_grade_export else None,
            strategy_plugin_contract_hash=plugin.contract_hash() if promotion_grade_export else "",
            strategy_decision_contract_version=(
                plugin.decision_contract_version if promotion_grade_export else ""
            ),
            recommended_next_action=(
                "none"
                if promotion_grade_export
                else "rerun_research_export_decisions_with_approved_profile"
            ),
        )
        write_json_atomic(Path(out_path).expanduser(), payload)
    except (OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "research-export-decisions"})
        return 1
    _print_json(
        {
            "ok": True,
            "command": "research-export-decisions",
            "out": str(Path(out_path).expanduser()),
            "decision_count": len(decisions),
            "content_hash": payload["content_hash"],
            "profile_hash": profile_hash,
            "promotion_grade_export": promotion_grade_export,
        }
    )
    return 0


def cmd_runtime_replay_decisions(
    *,
    profile_path: str,
    db_path: str,
    through_ts_list_path: str,
    out_path: str,
) -> int:
    try:
        profile = load_approved_profile(profile_path)
        through_ts_list = _load_through_ts_list(through_ts_list_path)
        compatibility_warnings: list[str] = []
        strategy_name = str(profile.get("strategy_name") or "").strip()
        if not strategy_name:
            raise ValueError("runtime_replay_profile_strategy_name_missing")
        plugin = resolve_research_strategy_plugin(strategy_name)
        if plugin.runtime_replay_builder is None:
            raise ValueError(f"runtime replay unsupported for research strategy: {strategy_name}")
        strategy = plugin.runtime_replay_builder(
            profile,
            _candidate_regime_policy_from_approved_profile(profile),
        )
        db_fingerprint = sha256_prefixed({"db_path": str(Path(db_path).expanduser().resolve()), "through_ts": through_ts_list})
        conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
        try:
            decisions = export_runtime_replay_decisions(
                conn=conn,
                strategy=strategy,
                through_ts_list=through_ts_list,
                market=str(profile.get("market") or settings.PAIR),
                interval=str(profile.get("interval") or settings.INTERVAL),
                profile_content_hash=str(profile.get("profile_content_hash") or ""),
                dataset_content_hash=str(profile.get("dataset_content_hash") or ""),
                db_data_fingerprint=db_fingerprint,
                candle_basis="closed_candle",
                execution_timing_policy_hash=_decision_export_execution_timing_policy_hash(),
                strategy_version=plugin.version,
                strategy_decision_contract_version=plugin.decision_contract_version,
            )
        finally:
            conn.close()
        payload = _decision_export_payload(
            source="runtime_replay",
            profile_content_hash=str(profile.get("profile_content_hash") or ""),
            data_fingerprint=str(profile.get("dataset_content_hash") or ""),
            market=str(profile.get("market") or settings.PAIR),
            interval=str(profile.get("interval") or settings.INTERVAL),
            decisions=decisions,
            db_data_fingerprint=db_fingerprint,
            promotion_grade_export=True,
            strategy_plugin_contract=plugin.contract_payload(),
            strategy_plugin_contract_hash=plugin.contract_hash(),
            strategy_decision_contract_version=plugin.decision_contract_version,
            recommended_next_action="none",
        )
        if compatibility_warnings:
            payload["compatibility_warnings"] = compatibility_warnings
        write_json_atomic(Path(out_path).expanduser(), payload)
    except (OSError, ValueError, sqlite3.Error) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "runtime-replay-decisions"})
        return 1
    _print_json(
        {
            "ok": True,
            "command": "runtime-replay-decisions",
            "out": str(Path(out_path).expanduser()),
            "decision_count": len(decisions),
            "content_hash": payload["content_hash"],
        }
    )
    return 0


def cmd_replay_decision(
    *,
    db_path: str,
    strategy_name: str,
    candle_ts: int,
    readiness_json_path: str | None = None,
    as_json: bool = False,
) -> int:
    try:
        selected_strategy = str(strategy_name or "").strip().lower()
        plugin = resolve_research_strategy_plugin(selected_strategy)
        if plugin.runtime_replay_builder is None or plugin.single_replay_bundle_builder is None:
            raise ValueError(f"replay_decision_unsupported_strategy:{selected_strategy or 'missing'}")
        strategy = plugin.runtime_replay_builder(
            {
                "strategy_name": selected_strategy,
                "market": str(settings.PAIR),
                "interval": str(settings.INTERVAL),
                "strategy_parameters": plugin.runtime_parameter_adapter.from_settings(settings)
                if plugin.runtime_parameter_adapter is not None
                else {},
                "cost_model": {
                    "fee_rate": float(settings.LIVE_FEE_RATE_ESTIMATE),
                    "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
                },
            },
            None,
        )
        resolved_db_path = Path(db_path).expanduser().resolve()
        readiness_payload = None
        if readiness_json_path is not None:
            readiness_path = Path(readiness_json_path).expanduser().resolve()
            raw_readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
            if not isinstance(raw_readiness, dict):
                raise ValueError("replay_decision_readiness_json_not_object")
            readiness_payload = dict(raw_readiness)
        conn = sqlite3.connect(f"file:{resolved_db_path}?mode=ro", uri=True)
        try:
            bundle = plugin.single_replay_bundle_builder(
                conn,
                strategy,
                int(candle_ts),
                readiness_payload,
            )
        finally:
            conn.close()
        if bundle is None:
            raise ValueError("replay_decision_no_decision_for_candle_ts")
    except (OSError, ValueError, sqlite3.Error) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "replay-decision"})
        return 1

    payload = {
        "ok": True,
        "command": "replay-decision",
        "db": str(resolved_db_path),
        "strategy": selected_strategy,
        "candle_ts": int(candle_ts),
        "readiness_json": None if readiness_json_path is None else str(readiness_path),
        "bundle": bundle,
    }
    if as_json:
        _print_json(payload)
    else:
        _print_json(payload)
    return 0


def _require_explicit_runtime_bound_strategy_parameters(
    *,
    strategy_name: str,
    parameter_values: dict[str, object],
) -> None:
    spec = strategy_spec_for_name(strategy_name)
    explicit = set(parameter_values)
    required = sorted(
        set(spec.behavior_affecting_parameter_names) - set(spec.research_only_parameter_names)
    )
    missing = [key for key in required if key not in explicit]
    if missing:
        raise ValueError(
            "promotion_runtime_bound_parameter_missing:" + ",".join(missing)
        )


def _candidate_params_from_manifest(manifest: object, wanted_candidate_id: str) -> dict[str, object]:
    for index, params in enumerate(iter_parameter_candidates(manifest.parameter_space)):  # type: ignore[attr-defined]
        if candidate_id(params, index) == wanted_candidate_id:
            return dict(params)
    raise ValueError("candidate_id_not_found")


def _research_export_profile_hash(
    *,
    manifest: object,
    snapshot: object,
    params: dict[str, object],
    candidate_id_value: str,
    profile: dict[str, object] | None,
) -> str:
    if profile is None:
        return sha256_prefixed(
            {
                "strategy_name": manifest.strategy_name,  # type: ignore[attr-defined]
                "candidate_id": candidate_id_value,
                "parameter_values": params,
                "market": manifest.market,  # type: ignore[attr-defined]
                "interval": manifest.interval,  # type: ignore[attr-defined]
                "dataset_content_hash": snapshot.content_hash(),  # type: ignore[attr-defined]
            }
        )
    _validate_research_export_profile_binding(
        manifest=manifest,
        snapshot=snapshot,
        params=params,
        candidate_id_value=candidate_id_value,
        profile=profile,
    )
    return str(profile.get("profile_content_hash") or "")


def _validate_research_export_profile_binding(
    *,
    manifest: object,
    snapshot: object,
    params: dict[str, object],
    candidate_id_value: str,
    profile: dict[str, object],
) -> None:
    checks = {
        "strategy_name": (profile.get("strategy_name"), manifest.strategy_name),  # type: ignore[attr-defined]
        "market": (profile.get("market"), manifest.market),  # type: ignore[attr-defined]
        "interval": (profile.get("interval"), manifest.interval),  # type: ignore[attr-defined]
        "manifest_hash": (profile.get("manifest_hash"), manifest.manifest_hash()),  # type: ignore[attr-defined]
        "dataset_content_hash": (profile.get("dataset_content_hash"), snapshot.content_hash()),  # type: ignore[attr-defined]
    }
    for field, (left, right) in checks.items():
        if str(left or "").strip() != str(right or "").strip():
            raise ValueError(f"research_export_profile_{field}_mismatch")
    source_promotion_path = str(profile.get("source_promotion_artifact_path") or "").strip()
    if not source_promotion_path:
        raise ValueError("research_export_profile_source_promotion_missing")
    source_promotion = _load_json(str(Path(source_promotion_path).expanduser()))
    if str(source_promotion.get("candidate_id") or "").strip() != str(candidate_id_value or "").strip():
        raise ValueError("research_export_profile_candidate_id_mismatch")
    if not str(profile.get("candidate_profile_hash") or "").startswith("sha256:"):
        raise ValueError("research_export_profile_candidate_profile_hash_missing")
    profile_candidate = source_promotion.get("candidate_profile")
    if not isinstance(profile_candidate, dict):
        raise ValueError("research_export_profile_candidate_profile_missing")
    if str(profile.get("candidate_profile_hash") or "").strip() != str(
        source_promotion.get("candidate_profile_hash") or ""
    ).strip():
        raise ValueError("research_export_profile_candidate_profile_hash_mismatch")
    cost = profile.get("cost_model")
    if not isinstance(cost, dict):
        raise ValueError("research_export_profile_cost_model_missing")
    effective_params = materialize_strategy_parameters(
        str(manifest.strategy_name),  # type: ignore[attr-defined]
        params,
        fee_rate=cost.get("fee_rate"),
        slippage_bps=cost.get("slippage_bps"),
    )
    profile_params = (
        profile.get("strategy_parameters")
        if isinstance(profile.get("strategy_parameters"), dict)
        else {}
    )
    for key, expected in profile_params.items():
        if key not in effective_params:
            raise ValueError(f"research_export_profile_strategy_parameter_missing:{key}")
        if str(effective_params[key]).strip() != str(expected).strip():
            try:
                if abs(float(effective_params[key]) - float(expected)) <= 1e-12:
                    continue
            except (TypeError, ValueError):
                pass
            raise ValueError(f"research_export_profile_strategy_parameter_mismatch:{key}")
    scenario = manifest.execution_model.scenarios[0]  # type: ignore[attr-defined]
    for key, actual in {
        "fee_rate": scenario.fee_rate,
        "slippage_bps": scenario.slippage_bps,
    }.items():
        if key not in cost:
            raise ValueError(f"research_export_profile_cost_model_missing:{key}")
        try:
            if abs(float(cost[key]) - float(actual)) <= 1e-12:
                continue
        except (TypeError, ValueError):
            pass
        raise ValueError(f"research_export_profile_cost_model_mismatch:{key}")


def _candidate_regime_policy_from_approved_profile(profile: dict[str, object]) -> dict[str, object]:
    return {
        "live_regime_policy": dict(profile.get("regime_policy") if isinstance(profile.get("regime_policy"), dict) else {}),
        "strategy_profile_hash": profile.get("profile_content_hash"),
        "approved_profile_hash": profile.get("profile_content_hash"),
        "approved_profile_mode": profile.get("profile_mode"),
        "approved_profile_verification_ok": True,
        "approved_profile_block_reason": "ok",
        "approved_profile_loaded": True,
        "approved_profile_schema_hash_valid": True,
        "approved_profile_source_verified": True,
        "approved_profile_evidence_verified": True,
        "approved_profile_runtime_verified": True,
        "approved_profile_contract_scope": "full_approved_profile",
        "legacy_candidate_profile_path_used": False,
        "candidate_profile_hash": profile.get("candidate_profile_hash"),
        "manifest_hash": profile.get("manifest_hash"),
        "dataset_content_hash": profile.get("dataset_content_hash"),
        "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
        "source_promotion_artifact_path": profile.get("source_promotion_artifact_path"),
        "lineage_hash": profile.get("lineage_hash"),
        "legacy_compatibility_used": bool(profile.get("legacy_compatibility_used")),
        "decision_equivalence_report_path": profile.get("decision_equivalence_report_path"),
        "decision_equivalence_content_hash": profile.get("decision_equivalence_content_hash"),
        **_candidate_regime_policy_summary(profile),
    }


def _load_through_ts_list(path: str) -> list[int]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        payload = payload.get("through_ts_list")
    if not isinstance(payload, list):
        raise ValueError("through_ts_list_not_list")
    return [int(item) for item in payload]


def _decision_export_payload(
    *,
    source: str,
    profile_content_hash: str,
    data_fingerprint: str,
    market: str,
    interval: str,
    decisions: list[dict[str, object]],
    db_data_fingerprint: str = "",
    promotion_grade_export: bool = True,
    strategy_plugin_contract: dict[str, object] | None = None,
    strategy_plugin_contract_hash: str = "",
    strategy_decision_contract_version: str = "",
    recommended_next_action: str = "none",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "decision_contract_version": 1,
        "source": source,
        "profile_content_hash": profile_content_hash,
        "dataset_content_hash": data_fingerprint,
        "db_data_fingerprint": db_data_fingerprint,
        "market": market,
        "interval": interval,
        "decision_count": len(decisions),
        "promotion_grade_export": bool(promotion_grade_export),
        "strategy_plugin_contract": strategy_plugin_contract,
        "strategy_plugin_contract_hash": strategy_plugin_contract_hash,
        "strategy_decision_contract_version": strategy_decision_contract_version,
        "recommended_next_action": recommended_next_action,
        "decisions": decisions,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload["content_hash"] = compute_decision_export_hash(payload)
    return payload


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
