from __future__ import annotations
from pathlib import Path
import sys

SKIP_PREFIXES = ("diff --git", "index ", "--- ", "+++ ", "@@")

def undiff_text(text: str) -> str:
    lines = text.splitlines()
    # git patch 형태가 아니면 그대로 둠
    if not lines or not lines[0].startswith("diff --git"):
        return text

    out = []
    for line in lines:
        if line.startswith("\\ No newline at end of file"):
            continue
        if line.startswith(SKIP_PREFIXES):
            continue
        if line.startswith("+"):
            out.append(line[1:])
        elif line.startswith("-"):
            continue
        elif line.startswith(" "):
            out.append(line[1:])
        else:
            out.append(line)

    return ("\n".join(out)).rstrip() + "\n"

def process(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="replace")
    new = undiff_text(text)
    if new == text:
        return False
    path.write_text(new, encoding="utf-8", newline="\n")
    return True

def main(argv: list[str]) -> int:
    targets = argv[1:] or ["main.py"]
    changed = []
    for t in targets:
        if process(Path(t)):
            changed.append(t)
    print("undiff:", "cleaned " + ", ".join(changed) if changed else "nothing to do")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
