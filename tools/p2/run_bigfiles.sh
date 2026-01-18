#!/usr/bin/env bash
set -euo pipefail

# Bigfiles torture runner (pragmatic defaults).
#
# Usage:
#   bash tools/p2/run_bigfiles.sh /tmp/ocf_p2_data
#     [--big-mb 64]
#     [--buckets 8]
#     [--modes classic,single,mixed | mixed]
#     [--regen]
#     [--skip-verify]
#     [--skip-unpack]
#     [--timeout SEC]
#
# Default: big-mb=64, modes=mixed, skip-unpack=1, timeout=600
#
# Output dir is controlled by:
#   OCF_P2_OUT (default: bench_out/ocf_p2) used by run_bench.sh

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <out_root> [--big-mb N] [--buckets N] [--modes ...] [--regen] [--skip-verify] [--skip-unpack] [--timeout SEC]" >&2
  exit 2
fi

OUT_ROOT="$1"
shift || true

BIG_MB="64"
BUCKETS="8"
MODES="mixed"
REGEN=0
SKIP_VERIFY=0
SKIP_UNPACK=1
TIMEOUT_SEC="600"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --big-mb) BIG_MB="$2"; shift 2;;
    --buckets) BUCKETS="$2"; shift 2;;
    --modes) MODES="$2"; shift 2;;
    --mode) MODES="$2"; shift 2;; # alias
    --regen) REGEN=1; shift 1;;
    --skip-verify) SKIP_VERIFY=1; shift 1;;
    --skip-unpack) SKIP_UNPACK=1; shift 1;;
    --timeout) TIMEOUT_SEC="$2"; shift 2;;
    *) echo "ERROR: arg sconosciuto: $1" >&2; exit 2;;
  esac
done

DS_DIR="$OUT_ROOT/bigfile_single"
META="$DS_DIR/dataset.json"

if [[ "$REGEN" == "1" || ! -f "$META" ]]; then
  python3 tools/p2/bench_dataset_gen.py --out "$OUT_ROOT" --preset bigfile_single --big-mb "$BIG_MB"
else
  echo "INFO: reuse dataset (exists): $DS_DIR"
fi

# Forward to run_bench.sh (which handles OCF_P2_OUT)
args=( "$DS_DIR" --buckets "$BUCKETS" --modes "$MODES" --timeout "$TIMEOUT_SEC" )
if [[ "$SKIP_VERIFY" == "1" ]]; then args+=( --skip-verify ); fi
if [[ "$SKIP_UNPACK" == "1" ]]; then args+=( --skip-unpack ); fi

bash tools/p2/run_bench.sh "${args[@]}"
