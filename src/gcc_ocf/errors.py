"""Typed errors for GCC-OCF.

Single source of truth for exit codes lives here.

Policy:
- Errors are small and boring.
- The CLI maps errors to stable exit codes (see EXIT_CODE_* constants).
- docs/exit_codes.md is generated from this module (scripts/gen_exit_codes_md.py).
"""

from __future__ import annotations

from dataclasses import dataclass

# -------------------------
# Exit codes (single source)
# -------------------------

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_GENERIC = 10
EXIT_UNSUPPORTED_VERSION = 11
EXIT_MISSING_RESOURCE = 12
EXIT_HASH_MISMATCH = 13


@dataclass(frozen=True, slots=True)
class ExitCodeInfo:
    code: int
    name: str
    description: str


EXIT_CODES: tuple[ExitCodeInfo, ...] = (
    ExitCodeInfo(EXIT_OK, "OK", "Success"),
    ExitCodeInfo(EXIT_USAGE, "USAGE", "Usage/config error (invalid args, invalid pipeline spec, etc.)"),
    ExitCodeInfo(EXIT_GENERIC, "GENERIC", "Generic failure (corrupt payload, unexpected error, etc.)"),
    ExitCodeInfo(EXIT_UNSUPPORTED_VERSION, "UNSUPPORTED_VERSION", "Unsupported container/archive version"),
    ExitCodeInfo(EXIT_MISSING_RESOURCE, "MISSING_RESOURCE", "Missing required resource (e.g. bucket-level dict)"),
    ExitCodeInfo(EXIT_HASH_MISMATCH, "HASH_MISMATCH", "Integrity failure (hash/CRC mismatch, tamper detected)"),
)

# For convenience (fast lookup)
_EXIT_CODE_BY_NAME: dict[str, int] = {e.name: e.code for e in EXIT_CODES}
_EXIT_CODE_BY_CODE: dict[int, ExitCodeInfo] = {e.code: e for e in EXIT_CODES}


def exit_code_info(code: int) -> ExitCodeInfo | None:
    return _EXIT_CODE_BY_CODE.get(int(code))


def render_exit_codes_markdown() -> str:
    """Render docs/exit_codes.md content."""
    lines: list[str] = []
    lines.append("# Exit codes\n")
    lines.append("> GENERATED FILE — do not edit manually.\n")
    lines.append("> Source of truth: `src/gcc_ocf/errors.py` (EXIT_CODES).\n")
    lines.append("> Regenerate: `python scripts/gen_exit_codes_md.py`.\n\n")
    lines.append("These are the CLI exit codes you can rely on.\n\n")
    lines.append("| Code | Name | Meaning |\n")
    lines.append("|---:|---|---|\n")
    for e in sorted(EXIT_CODES, key=lambda x: x.code):
        lines.append(f"| {e.code} | `{e.name}` | {e.description} |\n")
    lines.append("\n## Notes\n")
    lines.append("- Most internal errors extend `GCCOCFError` and carry an `exit_code`.\n")
    lines.append("- `--debug` re-raises errors to show full stack traces.\n")
    lines.append(
        "- `--json` on `verify` prints a JSON object to stdout (ok) or stderr (error), and returns the same exit code.\n"
    )
    return "".join(lines)


# ---------------
# Typed exceptions
# ---------------


class GCCOCFError(Exception):
    """Base error for GCC-OCF."""

    exit_code: int = EXIT_GENERIC


class UsageError(GCCOCFError):
    exit_code = EXIT_USAGE


class CorruptPayload(GCCOCFError):
    exit_code = EXIT_GENERIC


class BadMagic(CorruptPayload):
    pass


class UnsupportedVersion(GCCOCFError):
    exit_code = EXIT_UNSUPPORTED_VERSION


class MissingResource(GCCOCFError):
    exit_code = EXIT_MISSING_RESOURCE


class HashMismatch(GCCOCFError):
    exit_code = EXIT_HASH_MISMATCH
