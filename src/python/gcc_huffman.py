#!/usr/bin/env python3
"""Compatibility wrapper.

This file intentionally keeps the historical path `src/python/gcc_huffman.py`
so existing scripts (bench_all.sh, run_roundtrip.sh) keep working.

The real implementation lives in `gcc_ocf.legacy.gcc_huffman`.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    # Add <repo>/src to sys.path so `import gcc_ocf` works when run as a script.
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "src"))


def main(argv: list[str]) -> int:
    _bootstrap()
    from gcc_ocf.legacy.gcc_huffman import main as legacy_main

    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
