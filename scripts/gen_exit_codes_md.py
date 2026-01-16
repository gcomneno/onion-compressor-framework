#!/usr/bin/env python3
"""Generate docs/exit_codes.md from src/gcc_ocf/errors.py (single source of truth)."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo / "src"))

    from gcc_ocf import errors  # noqa: E402

    out = repo / "docs" / "exit_codes.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(errors.render_exit_codes_markdown(), encoding="utf-8")
    print(f"[gcc-ocf] wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
