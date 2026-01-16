# Pipeline Spec (v1)

Obiettivo: rendere **riproducibile** e **portabile** un piano di compressione **lossless** (layer + codec + mapping per-stream).

La CLI semantica accetta un pipeline spec con:

```bash
gcc-ocf file compress IN OUT --pipeline @tools/pipelines/split_text_nums_v1.json
```

Quando `--pipeline` è presente, i flag `--layer`, `--codec`, `--stream-codecs`, `--mbn` vengono **ignorati**.

## Schema

Formato: JSON object.

Campi:

- `spec` (obbligatorio): deve essere **esattamente** `"gcc-ocf.pipeline.v1"`.
- `name` (opzionale): nome leggibile (log/debug).
- `layer` (obbligatorio): id del layer (es. `bytes`, `vc0`, `split_text_nums`, `tpl_lines_v0`).
- `codec` (opzionale, default `zlib`): codec principale (per stream non numerici).
- `stream_codecs` (opzionale): mappa per-stream `{ "TEXT": "zlib", "NUMS": "num_v1", ... }`.
- `mbn` (opzionale):
  - `true` forza MBN
  - `false` forza single-stream
  - assente = auto (MBN se layer multi-stream o se `stream_codecs` è presente)

Chiavi non riconosciute: **errore** (spec volutamente “stretta”).

## Esempi

### Single-stream (bytes + zlib)

```json
{
  "spec": "gcc-ocf.pipeline.v1",
  "name": "bytes+zlib",
  "layer": "bytes",
  "codec": "zlib"
}
```

### Multi-stream (split_text_nums)

```json
{
  "spec": "gcc-ocf.pipeline.v1",
  "name": "split_text_nums",
  "layer": "split_text_nums",
  "codec": "zlib",
  "stream_codecs": {
    "TEXT": "zlib",
    "NUMS": "num_v1"
  }
}
```

## Note

- Questo spec **non** descrive (ancora) la parte directory/autopick/bucket: per quello arriverà un *directory pipeline spec* separato.
- L’obiettivo qui è: file-mode deterministico, testabile, CI-friendly.

## Validazione

```bash
gcc-ocf file pipeline-validate @tools/pipelines/split_text_nums_v1.json
```

Se il file è valido → exit code 0 e stampa `OK`.
Se è invalido → exit code 2 con errore chiaro.
