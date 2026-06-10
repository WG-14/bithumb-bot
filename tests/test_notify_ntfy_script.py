from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _fake_curl(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl = bin_dir / "curl"
    curl.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\" > \"${FAKE_CURL_ARGS}\"\n",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    return bin_dir


def _run_script(tmp_path: Path, *, env: dict[str, str]) -> str:
    args_path = tmp_path / "curl_args.txt"
    fake_bin = _fake_curl(tmp_path)
    run_env = {
        **os.environ,
        **env,
        "FAKE_CURL_ARGS": str(args_path),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }
    result = subprocess.run(
        ["bash", "scripts/notify_ntfy.sh", "Title", "default", "Message"],
        cwd=Path(__file__).resolve().parents[1],
        env=run_env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return args_path.read_text(encoding="utf-8")


def test_notify_ntfy_script_uses_ntfy_server(tmp_path: Path) -> None:
    output = _run_script(
        tmp_path,
        env={
            "NTFY_TOPIC": "topic-secret",
            "NTFY_SERVER": "https://server-a",
            "NTFY_URL": "https://server-b",
        },
    )

    assert "https://server-a/topic-secret" in output
    assert "https://server-b/topic-secret" not in output


def test_notify_ntfy_script_falls_back_to_ntfy_url_alias(tmp_path: Path) -> None:
    output = _run_script(
        tmp_path,
        env={
            "NTFY_TOPIC": "topic-secret",
            "NTFY_SERVER": "",
            "NTFY_URL": "https://server-b",
        },
    )

    assert "https://server-b/topic-secret" in output
