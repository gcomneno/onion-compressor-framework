#!/usr/bin/env bash
set -euo pipefail

# Benchmark v1–v6 (legacy + v5/v6 container) for huffman-compressor.
#
# Usage:
#   PYTHON=.venv/bin/python bash scripts/bench_all.sh | tee /tmp/bench_all.txt
#
# Optional env:
#   PYTHON=python3
#   SCRIPT=src/python/gcc_huffman.py
#   DATA_DIR=tests/data
#   FILES="small.txt medium.txt large.txt"   # whitespace-separated list (NOT quoted as one arg)

PYTHON=${PYTHON:-python3}
SCRIPT=${SCRIPT:-src/python/gcc_huffman.py}
DATA_DIR=${DATA_DIR:-tests/data}

# Sanitize SCRIPT if it accidentally contains surrounding quotes (e.g. '"src/python/gcc_huffman.py"').
SCRIPT=${SCRIPT%\"}
SCRIPT=${SCRIPT#\"}
SCRIPT=${SCRIPT%\'}
SCRIPT=${SCRIPT#\'}

# Best-effort realpath (don’t fail if realpath can’t resolve).
SCRIPT_ABS=$(realpath "$SCRIPT" 2>/dev/null || printf "%s" "$SCRIPT")

# Build file list robustly (default: small/medium/large).
if [[ -n "${FILES:-}" ]]; then
  # shellcheck disable=SC2206
  FILES_ARR=($FILES)
else
  FILES_ARR=("small.txt" "medium.txt" "semantic_nums.txt" "fattura_like.txt" "large.txt")
fi

# Detect optional python zstandard (to avoid noisy failures).
HAVE_ZSTD=0
if "$PYTHON" -c "import zstandard" >/dev/null 2>&1; then
  HAVE_ZSTD=1
fi

echo "=== huffman-compressor benchmark (v1–v6 + c7) ==="
echo "Using: $PYTHON $SCRIPT_ABS"
echo

# Legacy steps (v1–v4)
LEGACY_LABELS=("c1 Step1 (bytes)" "c2 Step2 (V/C/O)" "c3 Step3 (sillabe)" "c4 Step4 (parole)")
LEGACY_CMDS=("c1" "c2" "c3" "c4")

# v5 (container bundle, huffman)
V5_LABELS=("v5 (bundle: bytes)" "v5 (bundle: sillabe)" "v5 (bundle: parole)")
V5_LAYERS=("bytes" "syllables_it" "words_it")

# v5 zstd (container)
V5_ZSTD_LABELS=("v5 (zstd: bytes)" "v5 (zstd: sillabe)" "v5 (zstd: parole)")
V5_ZSTD_LAYERS=("bytes" "syllables_it" "words_it")

# v5 auto-pick (CSV)
V5_AUTO_LABELS=("v5 (auto: layers=bytes,syllables_it,words_it codecs=huffman,zstd)")
V5_AUTO_LAYERS=("bytes,syllables_it,words_it")
V5_AUTO_CODECS=("huffman,zstd")

# v6 zstd (container v6) — same candidates as v5 zstd for direct comparison
V6_ZSTD_LABELS=("v6 (zstd: bytes)" "v6 (zstd: sillabe)" "v6 (zstd: parole)")
V6_ZSTD_LAYERS=("bytes" "syllables_it" "words_it")

# v6 auto-pick (CSV)
V6_AUTO_LABELS=("v6 (auto: layers=bytes,syllables_it,words_it codecs=huffman,zstd)")
V6_AUTO_LAYERS=("bytes,syllables_it,words_it")
V6_AUTO_CODECS=("huffman,zstd")

# c7 (v6 + MBN multi-stream)
C7_LABELS=("c7 (MBN: bytes)" "c7 (MBN: vc0)" "c7 (MBN: split_text_nums)" "c7 (MBN: tpl_lines_v0)")
C7_LAYERS=("bytes" "vc0" "split_text_nums" "tpl_lines_v0")

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

run_one () {
  local input="$1"
  local out="$2"
  shift 2
  local cmd=("$@")

  if "${cmd[@]}" >"$tmpdir/out.log" 2>"$tmpdir/err.log"; then
    local in_size out_size ratio
    in_size=$(stat -c%s "$input")
    out_size=$(stat -c%s "$out")
    ratio=$(awk -v o="$out_size" -v i="$in_size" 'BEGIN{printf "%.3f", o/i}')
    printf "%s\t%s\n" "$out_size" "$ratio"
    return 0
  else
    echo "    FAIL (compression error)"
    if [ -s "$tmpdir/err.log" ]; then
      echo "    --- stderr (tail) ---"
      tail -n 10 "$tmpdir/err.log" | sed 's/^/    /'
      echo "    ---------------------"
    fi
    return 1
  fi
}

best_by_size () {
  awk -F'\t' '
    NF>=3 {
      size=$2+0
      if (!seen || size < best) {best=size; bestline=$0; seen=1}
    }
    END{if(seen) print bestline}
  '
}

for f in "${FILES_ARR[@]}"; do
  if [[ "$f" = /* ]]; then
    input="$f"
  elif [[ -f "$f" ]]; then
    input="$f"
  else
    input="$DATA_DIR/$f"
  fi

  if [ ! -f "$input" ]; then
    echo "[WARN] File non trovato: $input (salto)"
    echo
    continue
  fi

  in_size=$(stat -c%s "$input")
  echo "--- File: $input ($in_size bytes) ---"

  results_all=""
  results_legacy=""

  # v1–v4
  for i in "${!LEGACY_LABELS[@]}"; do
    label="${LEGACY_LABELS[$i]}"
    c="${LEGACY_CMDS[$i]}"
    out="$tmpdir/$(basename "$f").${c}.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "$c" "$input" "$out"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
      results_legacy+="${label}\t${size}\t${ratio}\n"
    fi
  done

  # v5 bundle (huffman)
  for i in "${!V5_LABELS[@]}"; do
    label="${V5_LABELS[$i]}"
    layer="${V5_LAYERS[$i]}"
    out="$tmpdir/$(basename "$f").c5.${layer}.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c5" "$input" "$out" "$layer"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done

  # v5 zstd
  if [[ "$HAVE_ZSTD" -eq 1 ]]; then
  for i in "${!V5_ZSTD_LABELS[@]}"; do
    label="${V5_ZSTD_LABELS[$i]}"
    layer="${V5_ZSTD_LAYERS[$i]}"
    out="$tmpdir/$(basename "$f").c5.${layer}.zstd.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c5" "$input" "$out" "$layer" "zstd"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done
  else
    echo "  [SKIP] v5 zstd (python zstandard non disponibile)"
  fi

  # v5 auto
  if [[ "$HAVE_ZSTD" -eq 1 ]]; then
  for i in "${!V5_AUTO_LABELS[@]}"; do
    label="${V5_AUTO_LABELS[$i]}"
    layers_csv="${V5_AUTO_LAYERS[$i]}"
    codecs_csv="${V5_AUTO_CODECS[$i]}"
    out="$tmpdir/$(basename "$f").c5.auto.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c5" "$input" "$out" "$layers_csv" "$codecs_csv"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done
  else
    echo "  [SKIP] v5 auto (include zstd)"
  fi

  # v6 zstd
  if [[ "$HAVE_ZSTD" -eq 1 ]]; then
  for i in "${!V6_ZSTD_LABELS[@]}"; do
    label="${V6_ZSTD_LABELS[$i]}"
    layer="${V6_ZSTD_LAYERS[$i]}"
    out="$tmpdir/$(basename "$f").c6.${layer}.zstd.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c6" "$input" "$out" "$layer" "zstd"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done
  else
    echo "  [SKIP] v6 zstd (python zstandard non disponibile)"
  fi

  # v6 auto
  if [[ "$HAVE_ZSTD" -eq 1 ]]; then
  for i in "${!V6_AUTO_LABELS[@]}"; do
    label="${V6_AUTO_LABELS[$i]}"
    layers_csv="${V6_AUTO_LAYERS[$i]}"
    codecs_csv="${V6_AUTO_CODECS[$i]}"
    out="$tmpdir/$(basename "$f").c6.auto.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c6" "$input" "$out" "$layers_csv" "$codecs_csv"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done
  else
    echo "  [SKIP] v6 auto (include zstd)"
  fi


  # c7 (v6+MBN multi-stream)
  c7_codec="zstd_tight"
  if [[ "$HAVE_ZSTD" -ne 1 ]]; then
    c7_codec="zlib"
  fi
  for i in "${!C7_LABELS[@]}"; do
    label="${C7_LABELS[$i]}"
    layer="${C7_LAYERS[$i]}"
    out="$tmpdir/$(basename "$f").c7.${layer}.${c7_codec}.gcc"
    echo "  [$label]"
    if line=$(run_one "$input" "$out" "$PYTHON" "$SCRIPT" "c7" "$input" "$out" "$layer" "$c7_codec"); then
      size=$(printf "%s" "$line" | cut -f1)
      ratio=$(printf "%s" "$line" | cut -f2)
      echo "    OK   size=$size  ratio=$ratio"
      results_all+="${label}\t${size}\t${ratio}\n"
    fi
  done

  # Best picks
  if [ -n "$results_legacy" ]; then
    best_legacy=$(printf "%b" "$results_legacy" | best_by_size)
    bl_label=$(echo "$best_legacy" | awk -F'\t' '{print $1}')
    bl_size=$(echo "$best_legacy" | awk -F'\t' '{print $2}')
    bl_ratio=$(echo "$best_legacy" | awk -F'\t' '{print $3}')
    echo "  ==> Best legacy : [$bl_label] size=$bl_size  ratio=$bl_ratio"
  fi

  if [ -n "$results_all" ]; then
    best_all=$(printf "%b" "$results_all" | best_by_size)
    ba_label=$(echo "$best_all" | awk -F'\t' '{print $1}')
    ba_size=$(echo "$best_all" | awk -F'\t' '{print $2}')
    ba_ratio=$(echo "$best_all" | awk -F'\t' '{print $3}')
    echo "  ==> Best overall: [$ba_label] size=$ba_size  ratio=$ba_ratio"
  fi

  echo
done

echo "=== Done benchmark v1–v6 + c7 ==="
