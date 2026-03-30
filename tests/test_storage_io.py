from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.storage_io import append_jsonl, write_json_atomic, write_text_atomic


def test_append_jsonl_writes_append_only_records(tmp_path: Path) -> None:
    out = tmp_path / "data" / "paper" / "trades" / "orders" / "orders_2026-03-30.jsonl"
    append_jsonl(out, {"event": "created", "id": 1})
    append_jsonl(out, {"event": "filled", "id": 1})

    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"event": "created", "id": 1}
    assert json.loads(lines[1]) == {"event": "filled", "id": 1}


def test_write_json_atomic_replaces_content_atomically(tmp_path: Path) -> None:
    out = tmp_path / "data" / "paper" / "reports" / "ops_report" / "ops_report_2026-03-30.json"
    write_json_atomic(out, {"a": 1})
    write_json_atomic(out, {"a": 2, "b": "ok"})

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload == {"a": 2, "b": "ok"}


def test_write_text_atomic_creates_parent_and_utf8(tmp_path: Path) -> None:
    out = tmp_path / "logs" / "paper" / "audit" / "audit_2026-03-30.log"
    write_text_atomic(out, "운영자 action=resume\n")
    assert out.read_text(encoding="utf-8") == "운영자 action=resume\n"
