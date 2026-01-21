# GCC Onion Compressor Framework (GCC-OCF)

[![CI](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/gcomneno/onion-compressor-framework/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/badge/ruff-lint-success)](https://github.com/astral-sh/ruff)

> **Nota:** “GCC” qui significa *Grande Compressione Cucita-a-mano* (by GiadaWare o Giancarlo Cicellyn Comneno, come vi pare!). **Non** è GNU GCC.

Framework “a cipolla” per compressione **lossless** progettato come **stack a strati**:
- **Layer semantici** pluggabili (anche multi-stream)
- **Codec** pluggabili (Huffman, zlib, num_v0/num_v1, raw, …)
- **Container** binario v6 con payload **MBN** (multi-stream)
- Workflow directory-based: **bucketing + autopick + archivi `.gca` (GCA1) + resources**
- Modalità directory “single-container” (1 o 2 `.gcc` + index JSON)

### Terminologia (1 riga, senza mostri)
Ogni layer tratta ciò che arriva da sopra come **bytes a scatola chiusa**: una sequenza di `bytes` che il layer *non deve interpretare*.

---

## Requisiti
- Python **>= 3.12**
- (Opzionale) `zstandard` per codec zstd (se manca, si usa zlib dove previsto)

---

## Quickstart

```bash
python -m pip install -e ".[dev]"
gcc-ocf --help
gcc-ocf --version
````

### Wrapper compat (legacy) — NON romperli

Usati da `tests/run_roundtrip.sh` e `scripts/bench_all.sh`:

* `python3 src/python/gcc_huffman.py ...`
* `python3 src/python/gcc_dir.py ...`

---

## CLI

### File: pipeline spec (consigliato)

Per rendere un piano di compressione **riproducibile**, usa un pipeline spec JSON:

```bash
gcc-ocf file compress IN OUT --pipeline @tools/pipelines/split_text_nums_v1.json
gcc-ocf file decompress OUT BACK
gcc-ocf file verify OUT --full --json
```

Schema e dettagli: vedi `docs/pipeline_spec.md`.

---

### Directory: modalità classica (manifest + bucket `.gca`)

Directory mode “classico”: bucketing deterministico + autopick + archivi `.gca` (GCA1).

```bash
gcc-ocf dir pack IN_DIR OUT_DIR --buckets 16
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Schema e opzioni: vedi `docs/dir_pipeline_spec.md`.
Formato GCA1: vedi `docs/gca1_format.md`.

---

## Single-container modes (dir pack)

Queste modalità sono “a parte”: niente `.gca`, niente `manifest.jsonl`.
Output = directory con 1 o 2 container `.gcc` + index JSON.

> Nota architetturale utile: qui il “framing” è il container (GCC) e i confini “file-per-file” sono definiti dall’index come **slice** `(offset,length)` sul concat **decompresso**.

### `--single-container` (TEXT-only)

Concat deterministico dei file + pipeline vincente per testo:
`concat → split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)`

```bash
gcc-ocf dir pack --single-container IN_DIR OUT_DIR
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Se c’è anche solo un file non UTF-8/binary: errore (voluto).

Opzionale:

* `--keep-concat` mantiene `bundle.concat` (intermedio).

Output:

* `bundle.gcc`
* `bundle_index.json`
* (opzionale) `bundle.concat`

### `--single-container-mixed` (TEXT + BIN)

Due bundle separati:

* **TEXT**: `concat → split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)`
* **BIN**: `bytes + (zstd se disponibile, altrimenti zlib)`

```bash
gcc-ocf dir pack --single-container-mixed IN_DIR OUT_DIR
gcc-ocf dir verify OUT_DIR --full --json
gcc-ocf dir unpack OUT_DIR RESTORED_DIR
```

Note pratiche:

* Se la directory è tutta testo, il bundle BIN può risultare “vuoto” (normale).
* Su file binari molto piccoli è facile peggiorare (overhead container+codec): ratio > 1 è normale.

Opzionale:

* `--keep-concat` mantiene `bundle_text.concat` e `bundle_bin.concat`.

Output:

* `bundle_text.gcc`, `bundle_text_index.json`
* `bundle_bin.gcc`, `bundle_bin_index.json`
* (opzionale) `bundle_text.concat`, `bundle_bin.concat`

---

## Verify semantics (nota breve)

- `verify` (default) fa controlli “light” (struttura e coerenza).
- `verify --full` fa controlli “forti” (integrità end-to-end via hash).
- In modalità mixed, in `--full` eventuali errori di decode/decompress vengono trattati come **tamper** (HashMismatch): su codec tipo zstd può fallire prima dell’hash, ed è la scelta più robusta lato sicurezza.

Exit codes: vedi `docs/exit_codes.md` (doc generata; source of truth: `src/gcc_ocf/errors.py`).

---

## Architettura e documentazione

- Architettura a strati: vedi `docs/arch.md`
- Roadmap: **`ROADMAP.md`** (in root)
- Formati: `docs/formats.md`
- Container v6 + MBN: `docs/container_v6_mbn.md`
- GCA1: `docs/gca1_format.md`
- Benchmark notes: `docs/benchmarks.md`
- Design notes: `docs/design-notes.md`

---

## Dev / test baseline

```bash
bash tests/run_roundtrip.sh
python -m pytest -q
ruff check .
```

Guardrail architettura (stratificazione enforced):

```bash
python -m pytest -q tests/test_arch_boundaries.py
python tools/check_arch_boundaries.py
```

---
