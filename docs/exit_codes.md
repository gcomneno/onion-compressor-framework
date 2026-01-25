# Exit codes
> GENERATED FILE — do not edit manually.
> Source of truth: `src/gcc_ocf/errors.py` (EXIT_CODES).
> Regenerate: `python scripts/gen_exit_codes_md.py`.

These are the CLI exit codes you can rely on.

| Code | Name | Meaning |
|---:|---|---|
| 0 | `OK` | Success |
| 2 | `USAGE` | Usage/config error (invalid args, invalid pipeline spec, etc.) |
| 10 | `GENERIC` | Generic failure (corrupt payload, unexpected error, etc.) |
| 11 | `UNSUPPORTED_VERSION` | Unsupported container/archive version |
| 12 | `MISSING_RESOURCE` | Missing required resource (e.g. bucket-level dict) |
| 13 | `HASH_MISMATCH` | Integrity failure (hash/CRC mismatch, tamper detected) |

## Notes
- Most internal errors extend `GCCOCFError` and carry an `exit_code`.
- `--debug` re-raises errors to show full stack traces.
- `--json` on `verify` prints a JSON object to stdout (ok) or stderr (error), and returns the same exit code.
