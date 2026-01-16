#!/usr/bin/env bash
set -e

PYTHON=${PYTHON:-python3}
SCRIPT=src/python/gcc_huffman.py

DATA_DIR=tests/data

FILES_SMALL=("small.txt" "medium.txt" "semantic_nums.txt" "fattura_like.txt")
FILES_LARGE=("large.txt")

MODES=("1" "2" "3" "4")

# v5 bundle layers (per small/medium)
V5_LAYERS=("bytes" "syllables_it" "words_it")

# v6+MBN (c7) layers (keep safe: use raw/num_v0 only, no external deps)
C7_LAYERS=("bytes" "vc0" "split_text_nums" "tpl_lines_v0")

echo "=== huffman-compressor roundtrip tests ==="
echo "Using: $PYTHON $SCRIPT"
echo

run_legacy_1to4() {
  local INPUT="$1"
  local f="$2"

  for m in "${MODES[@]}"; do
    OUT_COMP="$DATA_DIR/${f}.v${m}.gcc"
    OUT_DEC="$DATA_DIR/${f}.v${m}.dec.txt"

    echo "Step v${m}: compress..."
    $PYTHON "$SCRIPT" c${m} "$INPUT" "$OUT_COMP" || {
      echo "  [ERRORE] compressione v${m} fallita, salto decompressione"
      continue
    }

    echo "Step v${m}: decompress..."
    $PYTHON "$SCRIPT" d${m} "$OUT_COMP" "$OUT_DEC"

    echo "Step v${m}: diff..."
    if diff -q "$INPUT" "$OUT_DEC" > /dev/null; then
      echo "  OK: roundtrip lossless"
    else
      echo "  [ATTENZIONE] diff non vuoto per v${m} su $f"
    fi
    echo
  done
}

run_v5_layers() {
  local INPUT="$1"
  local f="$2"

  for layer in "${V5_LAYERS[@]}"; do
    OUT_COMP="$DATA_DIR/${f}.v5.${layer}.gcc"
    OUT_DEC="$DATA_DIR/${f}.v5.${layer}.dec.txt"

    echo "Step v5 ($layer): compress..."
    $PYTHON "$SCRIPT" c5 "$INPUT" "$OUT_COMP" "$layer" || {
      echo "  [ERRORE] compressione v5 ($layer) fallita, salto decompressione"
      continue
    }

    echo "Step v5 ($layer): decompress..."
    $PYTHON "$SCRIPT" d5 "$OUT_COMP" "$OUT_DEC"

    echo "Step v5 ($layer): diff..."
    if diff -q "$INPUT" "$OUT_DEC" > /dev/null; then
      echo "  OK: roundtrip lossless"
    else
      echo "  [ATTENZIONE] diff non vuoto per v5 ($layer) su $f"
    fi
    echo
  done
}

run_c7_layers() {
  local INPUT="$1"
  local f="$2"

  for layer in "${C7_LAYERS[@]}"; do
    OUT_COMP="$DATA_DIR/${f}.v6.mbn.${layer}.gcc"
    OUT_DEC="$DATA_DIR/${f}.v6.mbn.${layer}.dec.txt"

    echo "Step c7 ($layer): compress..."
    # Usa codec raw per TEXT/MAIN, e per split_text_nums NUMS -> num_v0 (default interno)
    $PYTHON "$SCRIPT" c7 "$INPUT" "$OUT_COMP" "$layer" raw || {
      echo "  [ERRORE] compressione c7 ($layer) fallita, salto decompressione"
      continue
    }

    echo "Step d7 ($layer): decompress..."
    $PYTHON "$SCRIPT" d7 "$OUT_COMP" "$OUT_DEC"

    echo "Step c7 ($layer): diff..."
    if diff -q "$INPUT" "$OUT_DEC" > /dev/null; then
      echo "  OK: roundtrip lossless"
    else
      echo "  [ATTENZIONE] diff non vuoto per c7 ($layer) su $f"
    fi
    echo
  done
}

run_large_fast() {
  local INPUT="$1"
  local f="$2"

  # large: solo v1
  local m="1"
  OUT_COMP="$DATA_DIR/${f}.v${m}.gcc"
  OUT_DEC="$DATA_DIR/${f}.v${m}.dec.txt"

  echo "Step v${m}: compress..."
  $PYTHON "$SCRIPT" c${m} "$INPUT" "$OUT_COMP" || {
    echo "  [ERRORE] compressione v${m} fallita, salto decompressione"
    return 0
  }

  echo "Step v${m}: decompress..."
  $PYTHON "$SCRIPT" d${m} "$OUT_COMP" "$OUT_DEC"

  echo "Step v${m}: diff..."
  if diff -q "$INPUT" "$OUT_DEC" > /dev/null; then
    echo "  OK: roundtrip lossless"
  else
    echo "  [ATTENZIONE] diff non vuoto per v${m} su $f"
  fi
  echo

  # large: solo v5 bytes
  local layer="bytes"
  OUT_COMP="$DATA_DIR/${f}.v5.${layer}.gcc"
  OUT_DEC="$DATA_DIR/${f}.v5.${layer}.dec.txt"

  echo "Step v5 ($layer): compress..."
  $PYTHON "$SCRIPT" c5 "$INPUT" "$OUT_COMP" "$layer" || {
    echo "  [ERRORE] compressione v5 ($layer) fallita, salto decompressione"
    return 0
  }

  echo "Step v5 ($layer): decompress..."
  $PYTHON "$SCRIPT" d5 "$OUT_COMP" "$OUT_DEC"

  echo "Step v5 ($layer): diff..."
  if diff -q "$INPUT" "$OUT_DEC" > /dev/null; then
    echo "  OK: roundtrip lossless"
  else
    echo "  [ATTENZIONE] diff non vuoto per v5 ($layer) su $f"
  fi
  echo
}

# small/medium: full matrix
for f in "${FILES_SMALL[@]}"; do
  INPUT="$DATA_DIR/$f"
  if [ ! -f "$INPUT" ]; then
    echo "[WARN] File non trovato: $INPUT (salto)"
    continue
  fi

  echo "--- File: $INPUT ---"
  run_legacy_1to4 "$INPUT" "$f"
  run_v5_layers "$INPUT" "$f"
  run_c7_layers "$INPUT" "$f"
  echo
done

# large: fast subset
for f in "${FILES_LARGE[@]}"; do
  INPUT="$DATA_DIR/$f"
  if [ ! -f "$INPUT" ]; then
    echo "[WARN] File non trovato: $INPUT (salto)"
    continue
  fi

  echo "--- File: $INPUT (fast subset) ---"
  run_large_fast "$INPUT" "$f"
  echo
done

echo "=== Fine test roundtrip ==="
