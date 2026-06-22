from __future__ import annotations

import argparse
import json

from bithumb_bot.cli.commands.live_ops import _h74_long_run_preflight
from bithumb_bot.h74_readiness_certificate import validate_h74_long_run_preflight


def _certificate() -> dict[str, object]:
    return {
        "status": "pass",
        "positive_rehearsal_kst_10_pass": True,
        "negative_rehearsal_kst_18_blocks_entry": True,
        "entry_authority_gate_present": True,
        "out_of_window_buy_blocked": True,
        "entry_authority_gate_hash": "sha256:entry",
    }


def test_preflight_requires_negative_rehearsal_pass() -> None:
    cert = _certificate()
    cert.pop("negative_rehearsal_kst_18_blocks_entry")

    verdict = validate_h74_long_run_preflight(cert)

    assert verdict["valid"] is False
    assert "negative_rehearsal_kst_18_blocks_entry_missing_or_false" in verdict["reasons"]


def test_preflight_blocks_when_certificate_lacks_entry_negative_fields() -> None:
    cert = _certificate()
    cert["entry_authority_gate_hash"] = ""

    verdict = validate_h74_long_run_preflight(cert)

    assert verdict["valid"] is False
    assert "entry_authority_gate_hash_missing" in verdict["reasons"]


def test_preflight_accepts_positive_and_negative_certificate() -> None:
    verdict = validate_h74_long_run_preflight(_certificate())

    assert verdict["valid"] is True
    assert verdict["status"] == "pass"


def test_preflight_command_exits_nonzero_without_negative(tmp_path, capsys) -> None:
    cert = _certificate()
    cert["negative_rehearsal_kst_18_blocks_entry"] = False
    path = tmp_path / "certificate.json"
    path.write_text(json.dumps(cert), encoding="utf-8")

    code = _h74_long_run_preflight(
        argparse.Namespace(certificate=str(path), json=True),
        None,
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["status"] == "blocked"
    assert "negative_rehearsal_kst_18_blocks_entry_missing_or_false" in payload["reasons"]
