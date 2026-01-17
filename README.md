# GCC Onion Compressor Framework (GCC-OCF)

[![CI](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/badge/ruff-lint-success)](https://github.com/astral-sh/ruff)

> **Nota:** “GCC” qui significa *Giancarlo Compression Codec* (GiadaWare). **Non** è GNU GCC.

Framework “a cipolla” per compressione **lossless** con:
- layer semantici pluggabili (multi-stream)
- codec pluggabili (Huffman, zlib, num_v0/num_v1, raw, …)
- container binario v6 con payload MBN
- workflow directory-based con bucketing + autopick + archivi `.gca` (GCA1) + resources

## CLI

Nuova CLI (stabile, in evoluzione):

```bash
pip install -e ".[dev]"
gcc-ocf --help
```

Wrapper compat (NON toccare gli script legacy):

- `python3 src/python/gcc_huffman.py ...` (usato da `tests/run_roundtrip.sh`, `scripts/bench_all.sh`)
- `python3 src/python/gcc_dir.py ...`

## Quickstart

Install (dev):

```bash
python -m pip install -e ".[dev]"
```

File roundtrip (lossless):

```bash
gcc-ocf file compress in.txt out.gcc --layer bytes --codec zlib
gcc-ocf file verify out.gcc
gcc-ocf file decompress out.gcc back.txt
```

Pipeline spec validate + use (inline JSON):

```bash
spec='{"spec":"gcc-ocf.pipeline.v1","name":"demo","layer":"split_text_nums","codec":"zlib","mbn":true,"stream_codecs":{"TEXT":"zlib","NUMS":"num_v1"}}'
gcc-ocf file pipeline-validate "$spec"
gcc-ocf file compress in.txt out.gcc --pipeline "$spec"
```

Directory workflow:

```bash
gcc-ocf dir pack ./in_dir ./out_dir --buckets 8
gcc-ocf dir verify ./out_dir
gcc-ocf dir unpack ./out_dir ./restored_dir
```

Machine-readable verify:

```bash
gcc-ocf file verify out.gcc --json
gcc-ocf dir verify ./out_dir --json --full
```

Docs:
- `docs/pipeline_spec_v1.md`
- `docs/container_v6_mbn.md`
- `docs/gca1_format.md`

### Pipeline spec (consigliato)

Per rendere un piano di compressione **riproducibile**, usa un pipeline spec JSON:

```bash
gcc-ocf file compress IN OUT --pipeline @tools/pipelines/split_text_nums_v1.json
gcc-ocf file decompress OUT BACK
```

Spec e schema: vedi `docs/pipeline_spec.md`.

### Directory pipeline spec (dir mode)

Per fissare candidate pool + autopick + resources in directory mode:

```bash
gcc-ocf dir pipeline-validate @tools/dir_pipelines/default_v1.json
gcc-ocf dir pack IN_DIR OUT_DIR --pipeline @tools/dir_pipelines/default_v1.json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Schema: vedi `docs/dir_pipeline_spec.md`.

## Test baseline

```bash
bash tests/run_roundtrip.sh
```

## Documentazione formati
Vedi `docs/formats.md`.

## Directory pack: single-container (text-only, max compression)
Se hai una directory **solo di file UTF-8 di testo** (es. `.md`, `.txt`) e vuoi la miglior compressione,
usa `--single-container`.

Questa modalità fa automaticamente quello che a mano era risultato vincente:
- concat deterministico di tutti i file (ordine stabile)
- compressione con pipeline **split_text_nums + MBN** (TEXT:zlib, NUMS:num_v1)

### Esempio
```bash
gcc-ocf dir pack --single-container ./LeLes /tmp/leles_sc
gcc-ocf dir verify /tmp/leles_sc --json
gcc-ocf dir verify /tmp/leles_sc --json --full
gcc-ocf dir unpack /tmp/leles_sc /tmp/leles_back
diff -ru ./LeLes /tmp/leles_back && echo OK
```

### Limiti
- **Text-only**: se nella directory c'è un file binario o non UTF-8, la modalità fallisce con `UsageError`.
  In quel caso usa il workflow classico:

```bash
gcc-ocf dir pack ./DIR /tmp/out --buckets 16
```

### File prodotti
- `bundle.gcc` (container compresso)
- `bundle_index.json` (indice: offset/len/sha256 per ogni file)
- `bundle.concat` solo se usi `--keep-concat`
