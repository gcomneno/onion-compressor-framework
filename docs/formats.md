# Formati
Ultimo aggiornamento: 2026-01-21

Questo repo contiene **formati legacy (v1–v4)** e container moderni (v5/v6) che combinano layer+codec.

> Nota: qui descrivo **cosa** c’è e **come è inteso**.
> I dettagli byte-per-byte vivono nei documenti dedicati e nel codice:
> - `docs/container_v6_mbn.md`
> - `docs/gca1_format.md`
> - `src/gcc_ocf/engine/container_v6.py`
> - `src/gcc_ocf/core/legacy_payloads.py`
> - `src/gcc_ocf/core/huffman_bundle.py`
> - `src/gcc_ocf/core/mbn_bundle.py`

---

## Concetti base

- **Layer**: trasforma `input_bytes` in `symbols` + (opzionale) `meta`.
- **Codec**: comprime/decomprime i simboli (e, nei bundle, anche meta/vocab come stream separati).
- **Container (framing)**: impacchetta header + meta minimale + payload del codec.

Terminologia:
- “bytes a scatola chiusa” = `bytes` che un layer trasporta/incapsula senza interpretarli.

---

## Legacy v1–v4

Gli “step” storici chiamati dalla CLI:

- **v1 (bytes)**: Huffman direttamente sui byte.
- **v2 (V/C/O)**: mapping simbolico (vocali/consonanti/altro) + Huffman.
- **v3 (sillabe)**: tokenizzazione in sillabe italiane → IDs di vocabolario + Huffman.
- **v4 (parole)**: tokenizzazione in parole → IDs di vocabolario + Huffman.

Nei legacy, il vocabolario poteva finire nel meta (JSON) oppure nel payload (a seconda della variante).

---

## Container v5 (GCC v5)

v5 non è “un nuovo algoritmo”: è un **involucro** che permette di combinare:

- `layer_id` (es. `bytes`, `syllables_it`, `words_it`)
- `codec_id` (oggi: `huffman`)
- `meta` (JSON minimale)
- `payload` (tipicamente un bundle di stream)

### Payload v5: Huffman bundle (raccomandato)

Il payload è un **bundle auto-descrittivo** di stream compressi:

- uno stream `__meta__` (bytes) per eventuale meta binaria del layer (può essere vuoto)
- uno o più stream di simboli (bytes o ids), a seconda del layer
- eventuali stream aggiuntivi (dizionari, side-channel, ecc.)

Ogni stream può essere:
- `raw` (non compresso)
- `huffman` (compresso con Huffman)

### Payload legacy v5 (compatibilità)

Per compatibilità con vecchi file, `legacy_payloads.py` supporta ancora payload “a kind”:

- `KIND_BYTES`: Huffman su bytes (simile a v1)
- `KIND_IDS_META_VOCAB`: IDs + vocabolario in meta
- `KIND_IDS_INLINE_VOCAB`: IDs + vocabolario inline nel payload

Se il payload **non** è un bundle riconosciuto, si prova la via “legacy payload”.

---

## Container v6 + payload MBN (multi-stream)

v6 è l'evoluzione “compatta” del framework:

- header corto (ID numerici `u8` per `layer/codec`)
- meta omessa se vuota
- `payload_len` omesso (payload = resto del file, default)

Il payload consigliato è **MBN**: bundle multi-stream dove ogni stream dichiara esplicitamente il codec usato.
È la base per layer come `split_text_nums` e `tpl_lines_*`, che producono più stream e permettono codec diversi per testo e numeri.

---

## Bucket archive GCA1 (directory mode classico)

Nel workflow directory (`gcc-ocf dir pack` classico) ogni bucket può essere archiviato in:

- `bucket_XX.gca` (GCA1)

GCA1 **non** è un nuovo codec: è un wrapper append-friendly che concatena blob già self-contained
(tipicamente container v6) + un index JSONL compresso + trailer fisso.

Layout:
- `[blob0][blob1]...[index_zlib][TRAILER]`

Indice:
- JSONL UTF-8 compresso zlib
- entry minime: `rel`, `offset`, `length`
- meta facoltativa: `blob_sha256`, `blob_crc32`, ecc.

Resource bucket-level:
- entry speciali con `rel="__res__/NAME"` (es. dict condivisi)

Dettagli byte-level: `docs/gca1_format.md`.
---

## Single-container dir (TEXT e MIXED)

Questa è una modalità **separata** dal workflow bucket+GCA1:

- TEXT-only: `bundle.gcc + bundle_index.json`
- MIXED: `bundle_text.*` + `bundle_bin.*`

Punto chiave:
- il “framing” è il container (GCC)
- i confini per-file sono definiti dall’index come slice `(offset,length)` sul concat **decompresso**.

Schema index: `gcc-ocf.dir_bundle_index.v1` (vedi `src/gcc_ocf/dir_index.py`).
````

---
