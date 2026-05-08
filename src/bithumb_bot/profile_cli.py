from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .canonical_decision import export_runtime_replay_decisions
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
from .decision_equivalence import compare_decision_equivalence, load_decision_list
from .research.dataset_snapshot import load_dataset_split
from .research.experiment_manifest import load_manifest
from .research.hashing import content_hash_payload, sha256_prefixed
from .research.parameter_space import candidate_id, iter_parameter_candidates
from .research.promotion_gate import PromotionGateError
from .research.strategy_registry import resolve_research_strategy
from .storage_io import write_json_atomic
from .strategy.sma import create_sma_with_filter_strategy


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
        result = compare_decision_equivalence(
            research_decisions=load_decision_list(research_decisions_path),
            runtime_decisions=load_decision_list(runtime_decisions_path),
            profile_hash=profile_hash,
            market=market,
            interval=interval,
            data_fingerprint=data_fingerprint,
        )
    except (OSError, ValueError) as exc:
        _print_json({"ok": False, "error": str(exc), "command": "decision-equivalence"})
        return 1
    _print_json({"command": "decision-equivalence", **result.report})
    return 0 if result.ok else 1


def cmd_research_export_decisions(
    *,
    manifest_path: str,
    candidate_id_value: str,
    split: str,
    out_path: str,
) -> int:
    try:
        manifest = load_manifest(manifest_path)
        snapshot = load_dataset_split(db_path=settings.DB_PATH, manifest=manifest, split_name=split)
        params = _candidate_params_from_manifest(manifest, candidate_id_value)
        scenario = manifest.execution_model.scenarios[0]
        run = resolve_research_strategy(manifest.strategy_name)(
            snapshot,
            params,
            float(scenario.fee_rate),
            float(scenario.slippage_bps),
            None,
            None,
            manifest.execution_timing,
        )
        profile_hash = sha256_prefixed(
            {
                "strategy_name": manifest.strategy_name,
                "candidate_id": candidate_id_value,
                "parameter_values": params,
                "market": manifest.market,
                "interval": manifest.interval,
                "dataset_content_hash": snapshot.content_hash(),
            }
        )
        decisions = [
            {
                **item,
                "profile_content_hash": profile_hash,
                "dataset_content_hash": snapshot.content_hash(),
                "db_data_fingerprint": snapshot.content_hash(),
            }
            for item in run.decisions
        ]
        payload = _decision_export_payload(
            source="research",
            profile_content_hash=profile_hash,
            data_fingerprint=snapshot.content_hash(),
            market=manifest.market,
            interval=manifest.interval,
            decisions=decisions,
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
        params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
        cost = profile.get("cost_model") if isinstance(profile.get("cost_model"), dict) else {}
        through_ts_list = _load_through_ts_list(through_ts_list_path)
        strategy = create_sma_with_filter_strategy(
            short_n=int(params.get("SMA_SHORT", settings.SMA_SHORT)),
            long_n=int(params.get("SMA_LONG", settings.SMA_LONG)),
            pair=str(profile.get("market") or settings.PAIR),
            interval=str(profile.get("interval") or settings.INTERVAL),
            min_gap_ratio=float(params.get("SMA_FILTER_GAP_MIN_RATIO", settings.SMA_FILTER_GAP_MIN_RATIO)),
            volatility_window=int(params.get("SMA_FILTER_VOL_WINDOW", settings.SMA_FILTER_VOL_WINDOW)),
            min_volatility_ratio=float(params.get("SMA_FILTER_VOL_MIN_RANGE_RATIO", settings.SMA_FILTER_VOL_MIN_RANGE_RATIO)),
            overextended_lookback=int(params.get("SMA_FILTER_OVEREXT_LOOKBACK", settings.SMA_FILTER_OVEREXT_LOOKBACK)),
            overextended_max_return_ratio=float(params.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO)),
            cost_edge_enabled=_coerce_bool(params.get("SMA_COST_EDGE_ENABLED", settings.SMA_COST_EDGE_ENABLED)),
            cost_edge_min_ratio=float(params.get("SMA_COST_EDGE_MIN_RATIO", settings.SMA_COST_EDGE_MIN_RATIO)),
            entry_edge_buffer_ratio=float(params.get("ENTRY_EDGE_BUFFER_RATIO", settings.ENTRY_EDGE_BUFFER_RATIO)),
            slippage_bps=float(cost.get("slippage_bps", settings.STRATEGY_ENTRY_SLIPPAGE_BPS)),
            live_fee_rate_estimate=float(cost.get("fee_rate", settings.LIVE_FEE_RATE_ESTIMATE)),
            exit_rule_names=str(params.get("STRATEGY_EXIT_RULES", settings.STRATEGY_EXIT_RULES)).split(","),
            exit_max_holding_min=int(params.get("STRATEGY_EXIT_MAX_HOLDING_MIN", settings.STRATEGY_EXIT_MAX_HOLDING_MIN)),
            exit_min_take_profit_ratio=float(params.get("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO)),
            exit_small_loss_tolerance_ratio=float(params.get("STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO", settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO)),
            candidate_regime_policy={"strategy_profile_hash": profile.get("profile_content_hash")},
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
                execution_timing_policy_hash=sha256_prefixed({"runtime_replay": "closed_candle_through_ts"}),
            )
        finally:
            conn.close()
        payload = _decision_export_payload(
            source="runtime_replay",
            profile_content_hash=str(profile.get("profile_content_hash") or ""),
            data_fingerprint=db_fingerprint,
            market=str(profile.get("market") or settings.PAIR),
            interval=str(profile.get("interval") or settings.INTERVAL),
            decisions=decisions,
            db_data_fingerprint=db_fingerprint,
        )
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


def _candidate_params_from_manifest(manifest: object, wanted_candidate_id: str) -> dict[str, object]:
    for index, params in enumerate(iter_parameter_candidates(manifest.parameter_space)):  # type: ignore[attr-defined]
        if candidate_id(params, index) == wanted_candidate_id:
            return dict(params)
    raise ValueError("candidate_id_not_found")


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
        "decisions": decisions,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload["content_hash"] = sha256_prefixed(
        content_hash_payload({key: value for key, value in payload.items() if key not in {"content_hash", "generated_at"}})
    )
    return payload


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
