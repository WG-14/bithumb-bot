from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _backtest(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_backtest

    return int(cmd_research_backtest(manifest_path=str(args.manifest), execution_calibration_path=str(args.execution_calibration) if args.execution_calibration else None))


def _verify_audit(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_verify_audit

    return int(cmd_research_verify_audit(experiment_id=str(args.experiment_id)))


def _validate(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_validate

    return int(cmd_research_validate(manifest_path=str(args.manifest), execution_calibration_path=str(args.execution_calibration) if args.execution_calibration else None, candidate_id=str(args.candidate_id) if args.candidate_id else None, out_path=str(args.out) if args.out else None, mode=str(args.mode)))


def _readiness(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.readiness import cmd_research_readiness

    return int(cmd_research_readiness(manifest_path=str(args.manifest), execution_calibration_path=str(args.execution_calibration) if args.execution_calibration else None, missing_classification_path=str(args.missing_classification) if args.missing_classification else None, as_json=bool(args.json)))


def _research_forward_diagnostics(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_forward_diagnostics

    return int(
        cmd_research_forward_diagnostics(
            manifest_path=str(args.manifest),
            split_name=str(args.split),
            features=tuple(args.features),
            horizons=tuple(args.horizons),
            bucket=str(args.bucket),
            entry_price=str(args.entry_price),
            min_bucket_count=int(args.min_bucket_count),
            out_path=str(args.out) if args.out else None,
            as_json=bool(args.json),
        )
    )


def _walk_forward(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_walk_forward

    return int(cmd_research_walk_forward(manifest_path=str(args.manifest), execution_calibration_path=str(args.execution_calibration) if args.execution_calibration else None))


def _promote(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_promote_candidate

    return int(cmd_research_promote_candidate(experiment_id=str(args.experiment_id), candidate_id=str(args.candidate_id), allow_legacy_lineage=bool(args.allow_legacy_lineage), validation_run_path=str(args.validation_run) if args.validation_run else None))


def _reproduce(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_reproduce

    return int(cmd_research_reproduce(promotion_path=str(args.promotion)))


def _registry_inspect(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_registry_inspect

    return int(cmd_research_registry_inspect(row_hash=str(args.row_hash)))


def _registry_validate(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_registry_validate

    return int(cmd_research_registry_validate(experiment_id=str(args.experiment_id)))


def _mark_aborted(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.research.cli import cmd_research_mark_attempt_aborted

    return int(cmd_research_mark_attempt_aborted(row_hash=str(args.row_hash), reason=str(args.reason)))


def _decision_equivalence(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_decision_equivalence

    return int(cmd_decision_equivalence(research_decisions_path=str(args.research_decisions), runtime_decisions_path=str(args.runtime_decisions), profile_hash=str(args.profile_hash), market=str(args.market), interval=str(args.interval), data_fingerprint=str(args.data_fingerprint)))


def _candidate_regime(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_candidate_regime_policy_equivalence_evidence

    return int(cmd_candidate_regime_policy_equivalence_evidence(backtest_report_path=str(args.backtest_report), candidate_id_value=str(args.candidate_id), decision_equivalence_report_path=str(args.decision_equivalence_report), out_path=str(args.out) if args.out is not None else None, bind=bool(args.bind)))


def _export_decisions(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_research_export_decisions

    return int(cmd_research_export_decisions(manifest_path=str(args.manifest), candidate_id_value=str(args.candidate_id), split=str(args.split), out_path=str(args.out), profile_path=str(args.profile) if args.profile is not None else None))


def _runtime_replay(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_runtime_replay_decisions

    return int(cmd_runtime_replay_decisions(profile_path=str(args.profile), db_path=str(args.db), through_ts_list_path=str(args.through_ts_list), out_path=str(args.out)))


def _replay_decision(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_replay_decision

    return int(cmd_replay_decision(db_path=str(args.db), strategy_name=str(args.strategy), candle_ts=int(args.candle_ts), readiness_json_path=None if getattr(args, "readiness_json", None) is None else str(args.readiness_json), as_json=bool(args.json)))


def _promotion_provenance_verify(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_promotion_provenance_verify

    return int(cmd_promotion_provenance_verify(artifact_path=str(args.artifact)))


def command_specs() -> list[CommandSpec]:
    common = dict(domain="research", read_only=True, produces_artifact=True, json_output_supported=True)
    return [
        make_spec("research-backtest", handler=_backtest, help="run a reproducible research backtest from a manifest", description="Run pure replay/simulation from a research manifest. Writes deterministic candidate and report artifacts under PathManager-managed research paths.", build=_build_manifest_calibration, **common),
        make_spec("research-verify-audit", handler=_verify_audit, help="verify research audit trace manifest and JSONL hash chains", build=lambda p: p.add_argument("--experiment-id", required=True), **common),
        make_spec("research-validate", handler=_validate, help="run the fail-closed end-to-end research validation pipeline", description="Run readiness, backtest, required walk-forward, promotion generation, and reproduce from one fixed manifest and write a hash-bound ValidationRun artifact.", build=_build_validate, **common),
        make_spec("research-readiness", handler=_readiness, help="check manifest data readiness before research execution", description="Read-only manifest readiness report for configured DB candle coverage, top-of-book coverage, calibration, and walk-forward prerequisites.", build=_build_readiness, **common),
        make_spec("research-forward-diagnostics", handler=_research_forward_diagnostics, help="run diagnostic-only forward-return feature bucket analysis", description="Run DatasetSnapshot-based forward-return diagnostics for feature mining only; output is not promotion, approved-profile, live-readiness, or capital-allocation evidence.", build=_build_forward_diagnostics, **common),
        make_spec("research-walk-forward", handler=_walk_forward, help="run walk-forward validation from a research manifest", description="Run research walk-forward validation without live broker or order lifecycle coupling.", build=_build_manifest_calibration, **common),
        make_spec("research-promote-candidate", handler=_promote, help="generate an operator-reviewable promotion artifact for a passing research candidate", description="Generate a promotion artifact; this command never rewrites paper/live env files.", build=_build_promote, **common),
        make_spec("research-reproduce", handler=_reproduce, help="verify a promotion artifact and its recorded experiment lineage", build=lambda p: p.add_argument("--promotion", required=True), **common),
        make_spec("research-registry-inspect", handler=_registry_inspect, help="inspect one research attempt registry row by hash", build=lambda p: p.add_argument("--row-hash", required=True), **common),
        make_spec("research-registry-validate", handler=_registry_validate, help="validate completed registry binding for an experiment id", build=lambda p: p.add_argument("--experiment-id", required=True), **common),
        make_spec("research-mark-attempt-aborted", handler=_mark_aborted, help="append an aborted event for an incomplete counted research attempt", build=_build_mark_aborted, **common),
        make_spec("research-export-decisions", handler=_export_decisions, help="export repo-generated canonical research decisions for a manifest candidate", description="Generate canonical research decision evidence from the repository research path.", build=_build_export_decisions, **common),
        make_spec("runtime-replay-decisions", handler=_runtime_replay, help="replay runtime SMA decisions at explicit closed-candle timestamps", description="Read-only runtime decision replay from SQLite; does not call live broker APIs or submit orders.", build=_build_runtime_replay, **common),
        make_spec("replay-decision", handler=_replay_decision, help="debug one runtime SMA decision at a closed-candle timestamp", description="Read-only single-decision replay from SQLite; does not call live broker APIs or submit orders.", build=_build_replay_decision, **common),
        make_spec("promotion-provenance-verify", handler=_promotion_provenance_verify, help="verify typed promotion provenance on a canonical artifact", description="Read-only provenance check for promotion/canonical artifacts; rejects compatibility fallback evidence.", build=lambda p: p.add_argument("--artifact", required=True), **common),
        make_spec("promotion-verify", handler=_promotion_provenance_verify, help="verify typed promotion provenance on a canonical artifact", description="Read-only promotion verifier for canonical/promotion artifacts.", build=lambda p: p.add_argument("--artifact", required=True), **common),
        make_spec("decision-equivalence", handler=_decision_equivalence, help="compare research decisions against runtime/paper decision telemetry", description="Credential-free deterministic equivalence check over exported decision JSON artifacts.", build=_build_decision_equivalence, **common),
        make_spec("candidate-regime-policy-equivalence-evidence", handler=_candidate_regime, help="bind promotion-grade decision-equivalence evidence to candidate regime policy equivalence", description="Generate a typed candidate-regime-policy equivalence artifact and optionally bind it into the research backtest report candidate before promotion.", build=_build_candidate_regime, **common),
    ]


def _build_manifest_calibration(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--execution-calibration")


def _build_validate(parser: argparse.ArgumentParser) -> None:
    _build_manifest_calibration(parser)
    parser.add_argument("--candidate-id")
    parser.add_argument("--out")
    parser.add_argument("--mode", default="strict", choices=["strict"])


def _build_readiness(parser: argparse.ArgumentParser) -> None:
    _build_manifest_calibration(parser)
    parser.add_argument("--missing-classification")
    parser.add_argument("--json", action="store_true")


def _build_forward_diagnostics(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--split", default="train", choices=("train", "validation", "final_holdout"))
    parser.add_argument("--features", required=True, type=_parse_csv_strings("--features"))
    parser.add_argument("--horizons", required=True, type=_parse_csv_ints("--horizons"))
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--entry-price", default="next_open", choices=("next_open", "signal_close"))
    parser.add_argument("--min-bucket-count", type=_positive_int_arg, default=30)
    parser.add_argument("--out")
    parser.add_argument("--json", action="store_true")


def _build_promote(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--validation-run")
    parser.add_argument("--allow-legacy-lineage", action="store_true", help="explicitly allow promotion of reviewed historical reports that lack lineage")


def _build_mark_aborted(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--row-hash", required=True)
    parser.add_argument("--reason", required=True)


def _build_decision_equivalence(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--research-decisions", required=True)
    parser.add_argument("--runtime-decisions", required=True)
    parser.add_argument("--profile-hash", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--interval", required=True)
    parser.add_argument("--data-fingerprint", required=True)


def _build_candidate_regime(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backtest-report", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--decision-equivalence-report", required=True)
    parser.add_argument("--out")
    parser.add_argument("--bind", action="store_true")


def _build_export_decisions(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--split", default="validation")
    parser.add_argument("--profile")
    parser.add_argument("--out", required=True)


def _build_runtime_replay(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--through-ts-list", required=True)
    parser.add_argument("--out", required=True)


def _build_replay_decision(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--candle-ts", required=True, type=int)
    parser.add_argument("--readiness-json")
    parser.add_argument("--json", action="store_true")


def _parse_csv_values(value: str, *, option_name: str, parser):
    parts = [part.strip() for part in str(value).split(",") if part.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(f"{option_name} requires a non-empty comma-separated list")
    parsed = []
    for part in parts:
        try:
            parsed.append(parser(part))
        except (TypeError, ValueError) as exc:
            raise argparse.ArgumentTypeError(f"{option_name} contains invalid value: {part}") from exc
    return tuple(parsed)


def _parse_csv_strings(option_name: str):
    return lambda value: _parse_csv_values(value, option_name=option_name, parser=str)


def _parse_csv_ints(option_name: str):
    return lambda value: _parse_csv_values(value, option_name=option_name, parser=int)


def _positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--min-bucket-count must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--min-bucket-count must be a positive integer")
    return parsed
