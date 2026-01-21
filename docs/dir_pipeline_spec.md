# GCC-OCF Directory Pipeline Spec (v1)
Ultimo aggiornamento: 2026-01-21

Questo documento definisce lo **schema JSON** per controllare il workflow directory-based
(`gcc-ocf dir pack` / `gcc-ocf dir unpack`) in modo riproducibile.

Lo spec directory è separato da:
- **file pipeline spec** → come comprimere un singolo file (layer, codec, MBN, stream-codecs)
- **dir pipeline spec** → come bucketizzare + autopick (K=2) + candidate pool + knob principali
- **single/mixed index spec** → schema dell’indice per single-container dir (vedi `dir_index.py` e `bundle_*_index.json`)

---

## Identificatore di schema

Ogni documento DEVE avere:
```json
{"spec":"gcc-ocf.dir_pipeline.v1"}
````

Se `spec` non coincide, il validatore rifiuta il file.

---

## Struttura (overview)

Campi supportati (chiavi sconosciute = errore):
- `spec` (string, obbligatorio)
- `buckets` (int, opzionale) → numero buckets (default: arg CLI, altrimenti 16)
- `archive` (bool, opzionale) → usa `.gca` per bucket (default: come legacy)
- `autopick` (object, opzionale)
- `candidate_pools` (object, opzionale)
- `resources` (object, opzionale)

### `autopick`

```json
"autopick": {
  "enabled": true,
  "sample_n": 3,
  "top_k": 2,
  "top_db_max": 12,
  "refresh_top": false
}
```

* `enabled` (bool) → se `false` usa la heuristic legacy
* `sample_n` (1..8) → quanti file per bucket usare per stimare ratio
* `top_k` → K=2 (vincolo progetto; se metti altro, verrà clamped a 2)
* `top_db_max` → grandezza storico (TOP db)
* `refresh_top` → se `true` ignora il TOP db e prova il pool completo

### `candidate_pools`

Mappa `bucket_type -> lista di piani`.

Bucket types attuali:
- `textish`
- `mixed_text_nums`
- `binaryish`

Ogni piano ha:

```json
{
  "layer": "split_text_nums",
  "codec": "zlib",
  "stream_codecs": {"TEXT":"zlib","NUMS":"num_v1"},
  "note": "split_text_nums+(TEXT=zlib)+num_v1"
}
```

Campi:
- `layer` (string, obbligatorio)
- `codec` (string, obbligatorio) → codec per stream principali / text-ish
- `stream_codecs` (object, opzionale) → mappa `STREAM_NAME -> codec_id`
- `note` (string, opzionale)

Stream names supportati:
`MAIN`, `TEXT`, `NUMS`, `IDS`, `TPL`, `META`, `CONS`, `VOWELS`, `MASK`

### `resources`

```json
"resources": {
  "num_dict_v1": {"enabled": true, "k": 64}
}
```

Attualmente implementato:
- `num_dict_v1` (bucket-level) usato da `num_v1` in MODE_SHARED

---

## Esempio completo

```json
{
  "spec": "gcc-ocf.dir_pipeline.v1",
  "buckets": 16,
  "archive": true,
  "autopick": {
    "enabled": true,
    "sample_n": 3,
    "top_k": 2,
    "top_db_max": 12,
    "refresh_top": false
  },
  "candidate_pools": {
    "binaryish": [
      {"layer":"bytes","codec":"zlib","note":"bytes+zlib"}
    ],
    "textish": [
      {"layer":"bytes","codec":"zlib","note":"bytes+zlib"},
      {"layer":"vc0","codec":"zlib","note":"vc0+zlib"},
      {"layer":"split_text_nums","codec":"zlib","stream_codecs":{"TEXT":"zlib","NUMS":"num_v1"}},
      {"layer":"tpl_lines_v0","codec":"zlib","stream_codecs":{"TPL":"zlib","IDS":"num_v1","NUMS":"num_v1"}}
    ],
    "mixed_text_nums": [
      {"layer":"split_text_nums","codec":"zlib","stream_codecs":{"TEXT":"zlib","NUMS":"num_v1"}},
      {"layer":"tpl_lines_v0","codec":"zlib","stream_codecs":{"TPL":"zlib","IDS":"num_v1","NUMS":"num_v1"}}
    ]
  },
  "resources": {
    "num_dict_v1": {"enabled": true, "k": 64}
  }
}
```

---

## Validazione

```bash
gcc-ocf dir pipeline-validate @tools/dir_pipelines/default_v1.json
```

Se il file è valido → exit code 0.
Se è invalido → exit code 2 con errore chiaro.

````

---
