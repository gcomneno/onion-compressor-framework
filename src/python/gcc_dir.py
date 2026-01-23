#!/usr/bin/env python3
"""Compatibility wrapper for legacy scripts.

This file exists for backward compatibility with older tooling that expects a
`src/python/gcc_dir.py` entrypoint.

Single source of truth lives in: `gcc_ocf.legacy.gcc_dir`.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from gcc_ocf.legacy.gcc_dir import main as legacy_main

    argv = sys.argv[1:] if argv is None else argv
    return int(legacy_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
