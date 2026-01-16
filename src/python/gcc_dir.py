#!/usr/bin/env python3
"""Compatibility wrapper for directory workflow CLI.

Keeps historical path `src/python/gcc_dir.py`.
Real implementation: `gcc_ocf.legacy.gcc_dir`.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "src"))


def main(argv: list[str]) -> int:
    _bootstrap()
    from gcc_ocf.legacy.gcc_dir import main as legacy_main

    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
