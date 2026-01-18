# GCC Onion Compressor Framework (GCC-OCF)

[![CI](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/badge/ruff-lint-success)](https://github.com/astral-sh/ruff)

> **Nota:** “GCC” qui significa *Grande Compressione Cucita-a-mano* (by GiadaWare). **Non** è GNU GCC.

Framework “a cipolla” per compressione **lossless** con:
- layer semantici pluggabili (multi-stream)
- codec pluggabili (Huffman, zlib, num_v0/num_v1, raw, …)
- container binario v6 con payload MBN
- workflow directory-based con bucketing + autopick + archivi `.gca` (GCA1) + resources

## Quickstart

```bash
pip install -e ".[dev]"
gcc-ocf --help
gcc-ocf --version
```

Wrapper compat (NON toccare gli script legacy):

- `python3 src/python/gcc_huffman.py ...` (usato da `tests/run_roundtrip.sh`, `scripts/bench_all.sh`)
- `python3 src/python/gcc_dir.py ...`

## CLI

### File: pipeline spec (consigliato)

Per rendere un piano di compressione **riproducibile**, usa un pipeline spec JSON:

```bash
gcc-ocf file compress IN OUT --pipeline @tools/pipelines/split_text_nums_v1.json
gcc-ocf file decompress OUT BACK
gcc-ocf file verify OUT --full --json
```

Spec e schema: vedi `docs/pipeline_spec.md`.

### Directory: modalità classica (manifest + bucket .gca)

Directory mode “classico”: bucketing deterministico + autopick + archivi `.gca` (GCA1).

```bash
gcc-ocf dir pack IN_DIR OUT_DIR --buckets 16
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Schema e opzioni: vedi `docs/dir_pipeline_spec.md`.

## Single-container modes (dir pack)

Queste modalità sono “a parte”: niente `.gca`, niente `manifest.jsonl`.
L’output è una directory con 1 o 2 container `.gcc` + index JSON.

### `--single-container` (TEXT-only)

Concat deterministico dei file + pipeline vincente per testo:
`concat → split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)`

```bash
gcc-ocf dir pack --single-container IN_DIR OUT_DIR
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Se c’è anche solo un file non UTF-8/binary: errore (è voluto).

Opzionale:
- `--keep-concat` mantiene `bundle.concat` (intermedio).

Output:
- `bundle.gcc`
- `bundle_index.json`
- (opzionale) `bundle.concat`

### `--single-container-mixed` (TEXT + BIN)

Due bundle separati:
- **TEXT**: `concat → split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)`
- **BIN**: `bytes + (zstd se disponibile, altrimenti zlib)`

```bash
gcc-ocf dir pack --single-container-mixed IN_DIR OUT_DIR
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Note pratiche:
- Se la directory è tutta testo, `bundle_bin.concat` può essere vuoto (normale).
- Su file binari molto piccoli è facile peggiorare (overhead container+codec): ratio > 1 è normale.

Opzionale:
- `--keep-concat` mantiene `bundle_text.concat` e `bundle_bin.concat`.

Output:
- `bundle_text.gcc`, `bundle_text_index.json`
- `bundle_bin.gcc`, `bundle_bin_index.json`
- (opzionale) `bundle_text.concat`, `bundle_bin.concat`

## Test baseline

```bash
bash tests/run_roundtrip.sh
pytest -q
ruff check .
```

## Exit codes

Source of truth: `src/gcc_ocf/errors.py`  
Doc generata: `docs/exit_codes.md`

## Documentazione formati

Vedi `docs/formats.md`.