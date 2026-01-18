#!/usr/bin/env bash
set -euo pipefail

# Run P0 core invariants (fast).
# Usage: bash scripts/run_p0.sh

ruff check . --fix
pytest -q -m p0
