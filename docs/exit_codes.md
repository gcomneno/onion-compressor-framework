# Exit codes

These are the CLI exit codes you can rely on.

- **0**: success
- **2**: usage/config error (e.g. invalid pipeline spec JSON)
- **10**: generic failure (unexpected error, corrupt payload, unsupported version, etc.)
- **13**: integrity failure (**HashMismatch**) — used by verify full/light when hashes/CRCs don't match

Notes:
- Most internal errors extend `GCCOCFError` and carry an `exit_code`.
- `--debug` re-raises errors to show full stack traces.
- `--json` on `verify` prints a JSON object to stdout (ok) or stderr (error), and returns the same exit code.
