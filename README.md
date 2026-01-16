# GCC Onion Compressor Framework (GCC-OCF)
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
