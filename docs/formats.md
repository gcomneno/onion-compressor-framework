# Formati

Ultimo aggiornamento: 2026-01-12

Questo repo contiene **formati legacy (v1–v4)** e un **container v5** che generalizza layer+codec.

> Nota: qui descrivo **cosa** c’è e **come è inteso**. I dettagli byte-per-byte vivono nel codice (`src/python/engine/container.py`, `src/python/core/legacy_payloads.py`, `src/python/core/huffman_bundle.py`).

## Concetti base

- **Layer**: trasforma `input_bytes` in `symbols` + (opzionale) `meta`.
- **Codec**: comprime/decomprime i simboli (e, in v5 bundle, anche meta/vocab come stream separati).
- **Container**: impacchetta header + meta minimale + payload del codec.

## Legacy v1–v4

Questi sono gli “step” storici chiamati dalla CLI:

- **v1 (bytes)**: Huffman direttamente sui byte.
- **v2 (V/C/O)**: mapping simbolico (vocali/consonanti/altro) + Huffman.
- **v3 (sillabe)**: tokenizzazione in sillabe italiane → IDs di vocabolario + Huffman.
- **v4 (parole)**: tokenizzazione in parole → IDs di vocabolario + Huffman.

Nei legacy, il vocabolario poteva finire nel meta (JSON) oppure nel payload (a seconda della variante).

## Container v5 (GCC v5)

v5 non è “un nuovo algoritmo”: è un **involucro** che permette di combinare:

- `layer_id` (es. `bytes`, `syllables_it`, `words_it`)
- `codec_id` (oggi: `huffman`)
- `meta` (JSON minimale)
- `payload` (tipicamente un **bundle** di stream)

### Payload v5: Huffman bundle (raccomandato)

Il payload è un **bundle auto-descrittivo** di stream compressi:

- uno stream `__meta__` (bytes) per eventuale meta binaria del layer (può essere vuoto)
- uno o più stream di simboli (bytes o ids), a seconda del layer
- eventuali stream aggiuntivi in futuro (es. dizionari, indici, side-channel)

Ogni stream può essere:
- `raw` (non compresso)
- `huffman` (compresso con Huffman)

### Payload legacy v5 (compatibilità)

Per compatibilità con test/vecchi file, `legacy_payloads.py` supporta ancora payload “a kind”:

- `KIND_BYTES`: Huffman su bytes (simile a v1)
- `KIND_IDS_META_VOCAB`: IDs + vocabolario in meta (vecchio)
- `KIND_IDS_INLINE_VOCAB`: IDs + vocabolario inline nel payload (nuovo)

In pratica: se il payload **non** è un bundle riconosciuto, si prova la via “legacy payload”.

## Container v6 + payload MBN (multi-stream)

v6 è l'evoluzione "compatta" del framework:

- header più corto (id numerici `u8` per `layer/codec`)
- meta omessa se vuota
- `payload_len` omesso (il payload è "il resto del file")

Il payload consigliato è **MBN (Multi Bundle)**: un bundle di *stream*.
Ogni stream dichiara esplicitamente quale codec lo ha compresso (per-stream routing).

Questa è la base per layer come `split_text_nums` e `tpl_lines_v0`, che producono più stream
e permettono di usare codec diversi per testo e numeri.

## Bucket archive GCA1 (directory mode)

Per la modalità directory (`gcc_dir.py`), il tool può creare un file archivio **per bucket**:

- `bucket_XX.gca`

Questo **non** è un nuovo container di compressione: è un wrapper che concatena molti blob
già self-contained (tipicamente container v6) e aggiunge un indice.

Layout:

- `[blob0][blob1]...[index_zlib][TRAILER]`
- `TRAILER` (16 byte): magic `GCA1` + `index_len` (u64 little-endian) + `crc32`.

L'indice è JSONL zlib-compresso e contiene per entry almeno: `rel`, `offset`, `length`.

Campi aggiuntivi tipici (facoltativi):

- `blob_sha256`: sha256 del blob (hex)
- `blob_crc32`: crc32 del blob (u32)

Le resources di bucket (es. `num_dict_v1`, `tpl_dict_v0`) sono entry con `rel` riservato `__res__/NAME`.
