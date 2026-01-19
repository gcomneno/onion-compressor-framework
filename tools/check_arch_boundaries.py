from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    test_path = repo_root / "tests" / "test_arch_boundaries.py"
    if not test_path.is_file():
        print("ERROR: tests/test_arch_boundaries.py not found.", file=sys.stderr)
        return 3

    ns: dict[str, object] = {"__file__": str(test_path)}  # <-- FIX: needed by the test
    try:
        code = test_path.read_text(encoding="utf-8")
        exec(compile(code, str(test_path), "exec"), ns, ns)
        fn = ns.get("test_no_low_level_imports_orchestrator")
        if not callable(fn):
            print("ERROR: test_no_low_level_imports_orchestrator not found.", file=sys.stderr)
            return 3
        fn()  # type: ignore[misc]
        print("OK: architecture boundaries respected.")
        return 0
    except AssertionError as e:
        print(str(e), file=sys.stderr)
        return 2
    except Exception as e:
        print(f"ERROR: unexpected failure: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
