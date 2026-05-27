from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _generate(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_profile_generate

    return int(cmd_profile_generate(promotion_path=str(args.promotion), mode=str(args.mode), out_path=str(args.out) if args.out is not None else None, market=str(args.market) if args.market is not None else None, interval=str(args.interval) if args.interval is not None else None))


def _diff(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_profile_diff

    return int(cmd_profile_diff(profile_path=str(args.profile), target_env=str(args.target_env), as_json=bool(args.json)))


def _verify(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_profile_verify

    return int(cmd_profile_verify(profile_path=str(args.profile), env_path=str(args.env)))


def _promote(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.profile_cli import cmd_profile_promote

    return int(cmd_profile_promote(profile_path=str(args.profile), mode=str(args.mode), out_path=str(args.out) if args.out is not None else None, paper_validation_evidence=str(args.paper_validation_evidence) if args.paper_validation_evidence is not None else None, live_readiness_evidence=str(args.live_readiness_evidence) if args.live_readiness_evidence is not None else None))


def command_specs() -> list[CommandSpec]:
    return [
        make_spec("profile-generate", domain="profile", handler=_generate, help="generate an approved profile artifact from a reviewed promotion artifact", description="Generate an approved profile; this command never rewrites paper/live env files.", build=_build_generate, produces_artifact=True, json_output_supported=True),
        make_spec("profile-diff", domain="profile", handler=_diff, help="compare an approved profile against a target env file", build=_build_diff, json_output_supported=True),
        make_spec("profile-verify", domain="profile", handler=_verify, help="verify an approved profile against a target env file and fail on drift", build=_build_verify),
        make_spec("profile-promote", domain="profile", handler=_promote, help="promote an approved profile through paper -> live_dry_run -> small_live states", build=_build_promote, produces_artifact=True, json_output_supported=True),
    ]


def _build_generate(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--promotion", required=True)
    parser.add_argument("--mode", required=True, choices=("paper",))
    parser.add_argument("--out")
    parser.add_argument("--market")
    parser.add_argument("--interval")


def _build_diff(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--json", action="store_true")


def _build_verify(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True)
    parser.add_argument("--env", required=True)


def _build_promote(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True)
    parser.add_argument("--mode", required=True, choices=("live_dry_run", "small_live"))
    parser.add_argument("--out")
    parser.add_argument("--paper-validation-evidence")
    parser.add_argument("--live-readiness-evidence")
