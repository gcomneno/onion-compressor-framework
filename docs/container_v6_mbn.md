# GCC-OCF – Container v6 + Payload MBN

Ultimo aggiornamento: 2026-01-21

Questo documento descrive il **formato** del container v6 (`GCC` + versione `6`) e del payload multi-stream **MBN**.

**Nota terminologica (stratificazione):**
- “Payload” qui significa sempre **bytes trattati come blob opaco** dal layer che li trasporta: il layer non interpreta i contenuti interni, li incapsula/trasporta soltanto.
- MBN è un modo per impacchettare **più stream** *dentro* il payload del container (Multi Bundle).

**Riferimenti implementativi:**
- `src/gcc_ocf/engine/container_v6.py`
- `src/gcc_ocf/core/mbn_bundle.py`

---

## 1) Container v6 (file)

### 1.1 Layout byte-level

Il file v6 è:

```
+----------------+--------------------+
| HEADER (min 7) | PAYLOAD (resto)    |
+----------------+--------------------+
```

#### Header minimo (7 byte)

| Ofs | Sze | Campo      | Tipo | Valore / Note |
|---:|---:|------------|------|---------------|
| 0  | 3  | magic      | by   | `b"GCC"` |
| 3  | 1  | version    | u8   | `6` |
| 4  | 1  | flags      | u8   | bitmask |
| 5  | 1  | layer_code | u8   | vedi tabella |
| 6  | 1  | codec_code | u8   | vedi tabella |

#### Campi opzionali

Dopo i 7 byte:

- se `flags & 0x01` (**F_HAS_META**) → `varint(meta_len)` + `meta_bytes`
- se `flags & 0x02` (**F_HAS_PAYLOAD_LEN**) → `varint(payload_len)` + `payload`
  - altrimenti il payload è “resto del file”

**Varint:** unsigned LEB128.

#### Flags

| Flag                | Bit    | Significato |
|---------------------|-------:|-------------|
| `F_HAS_META`        | `0x01` | presente un blocco meta subito dopo header |
| `F_HAS_PAYLOAD_LEN` | `0x02` | presente `payload_len` varint (non usato di default) |
| `F_KIND_EXTRACT`    | `0x80` | payload “extract” (lossy): non usare `decompress`, usare `extract-show` |

---

### 1.2 Mappature stabili (layer_code / codec_code)

> Queste mappature sono **stabili**: cambiare i codici rompe la compatibilità dei file scritti.

#### layer_code

| layer_id | code |
|----------|-----:|
| `bytes`  | 0 |
| `syllables_it` | 1 |
| `words_it` | 2 |
| `vc0` | 3 |
| `lines_dict` | 4 |
| `lines_rle` | 5 |
| `split_text_nums` | 6 |
| `tpl_lines_v0` | 7 |
| `tpl_lines_shared_v0` | 8 |

#### codec_code

| codec_id | code |
|---|---:|
| `huffman` | 0 |
| `zstd` | 1 |
| `zstd_tight` | 2 |
| `raw` | 3 |
| `mbn` | 4 |
| `num_v0` | 5 |
| `zlib` | 6 |
| `num_v1` | 7 |

---

## 2) Payload MBN (multi-stream bundle)

### 2.1 Scopo

MBN impacchetta **più stream** nello stesso container v6.

Ogni stream dichiara:
- **stype**: tipo stream (u8)
- **codec**: codec usato per comprimere quello stream
- **ulen**: lunghezza uncompressed (prima della compressione)
- **comp**: bytes compressi
- **meta**: meta per-stream (attualmente opzionale e tipicamente vuoto)

---

### 2.2 Layout byte-level

```
MBN:
magic: 3B  "MBN"
nstreams: varint
repeat nstreams times:
  stype: u8
  codec: u8
  ulen: varint
  clen: varint
  mlen: varint
  meta: mlen bytes
  comp: clen bytes
```

- `codec` usa **gli stessi codec_code** del container v6.
- `ulen` è la lunghezza del raw stream *prima* della compressione.

---

### 2.3 Stream types (stype)

> Anche questi codici sono **stabili**.

| stype | nome | significato |
|---:|---|---|
| 0 | `MAIN` | stream “principale” (bytes) |
| 1 | `MASK` | per layer `vc0` |
| 2 | `VOWELS` | per layer `vc0` |
| 3 | `CONS` | per layer `vc0` |
| 10 | `TEXT` | per layer `split_text_nums` |
| 11 | `NUMS` | per layer `split_text_nums` e tpl-lines |
| 20 | `TPL` | per layer `tpl_lines_v0` / `tpl_lines_shared_v0` |
| 21 | `IDS` | per layer `tpl_lines_v0` / `tpl_lines_shared_v0` |
| 250 | `META` | stream riservato `__meta__` (layer meta serializzata) |

---

### 2.4 Regole pratiche di decodifica (semantica)

- Se nel payload v6 trovi `magic == "MBN"`, il decoder:
  1) decodifica tutti gli stream (header + `meta` + `comp`)
  2) se esiste lo stream `META` e il layer implementa `unpack_meta`, ricostruisce il dizionario meta
  3) ricostruisce `symbols` per `layer.decode(symbols, meta)` in base al layer:

     - `vc0`: `(MASK, VOWELS, CONS)`
     - `split_text_nums`: `(TEXT, NUMS)`
     - `tpl_lines_v0` / `tpl_lines_shared_v0`: `(TPL, IDS, NUMS)`
     - altri: `MAIN` (o fallback al primo stream non-meta)

---

### 2.5 Invarianti (garanzie del formato)

Queste sono le regole che **un writer MBN deve garantire** e **un reader MBN deve validare**.

**Struttura:**
- `nstreams` deve essere >= 1.
- Ogni stream deve avere campi completi; un varint troncato è payload corrotto.
- `mlen` e `clen` devono essere tali che `meta` e `comp` stiano nel buffer (niente out-of-bounds).

**Coerenza:**
- `stype` deve essere un u8 (0..255). Se sconosciuto: lo stream è comunque parsabile, ma potrebbe essere ignorato dal layer.
- `codec` deve essere un codec_code valido per l’implementazione corrente; se sconosciuto → decodifica stream fallisce.
- `ulen` è la lunghezza attesa *dopo decompress*; se decompress produce una lunghezza diversa → payload corrotto.

**Namespace / collisioni:**
- Se più stream hanno lo stesso `stype`, la semantica deve essere esplicita (consigliato: vietare duplicati).  
  *Nota:* la politica effettiva (vietato vs “ultimo vince”) deve essere coerente con `mbn_bundle.py`.

---

### 2.6 Error semantics (Corrupt vs Tamper)

Questo documento usa due categorie, coerenti con la semantica “verify”:

- **CorruptPayload**: struttura/varint/troncamenti/length mismatch/codec sconosciuto/stream incompleto.
- **HashMismatch (tamper)**: quando l’integrità (hash) non torna dopo una decodifica riuscita.  
  *Nota pratica:* alcuni codec (es. zstd) possono fallire **prima** dell’hash; in quel caso l’errore può essere trattato come “tamper” in modalità full.

---

### 2.7 CLI mapping

- `gcc-ocf file compress ... --mbn` forza MBN
- `gcc-ocf file compress ... --stream-codecs 'TEXT:zlib,NUMS:num_v1'` abilita MBN e sceglie codec per-stream
- pipeline spec (`gcc-ocf.pipeline.v1`) controlla `mbn` e `stream_codecs`

---

### 2.8 Esempi byte-level (golden vectors)

Questi esempi sono **deliberatamente piccoli** e servono a “inchiodare” il formato MBN in modo testabile.
Nel repo sono coperti da `tests/test_mbn_vectors.py`.

#### Esempio A — 1 stream (MAIN, codec=raw)

Hex:

```
4d 42 4e  01  00 03  03 03 00  61 62 63
```

Breakdown:

- `4d 42 4e` = `"MBN"`
- `01` = `nstreams=1`
- Stream #1:
  - `00` = `stype=MAIN`
  - `03` = `codec=raw` (codec_code=3)
  - `03` = `ulen=3`
  - `03` = `clen=3`
  - `00` = `mlen=0`
  - `comp` = `61 62 63` (`b"abc"`)

#### Esempio B — 2 stream (TEXT + NUMS)

Hex:

```
4d 42 4e  02
  0a 06  05 02 00  01 02
  0b 07  04 01 01  ff  aa
```

Breakdown:

- `4d 42 4e` = `"MBN"`
- `02` = `nstreams=2`
- Stream #1:
  - `0a` = `stype=TEXT` (10)
  - `06` = `codec=zlib` (codec_code=6)
  - `05` = `ulen=5`
  - `02` = `clen=2`
  - `00` = `mlen=0`
  - `comp` = `01 02`
- Stream #2:
  - `0b` = `stype=NUMS` (11)
  - `07` = `codec=num_v1` (codec_code=7)
  - `04` = `ulen=4`
  - `01` = `clen=1`
  - `01` = `mlen=1`
  - `meta` = `ff`
  - `comp` = `aa`

**Nota importante:** in MBN `codec` è un *code* (u8) che riusa la tabella `codec_code` del container v6.
Il significato semantico (TEXT/NUMS/TPL/IDS ecc.) è determinato dallo `stype` e dal layer che lo consuma.


## 3) Compatibilità

- Il decoder “universale” (d7) gestisce v1–v6 e v6+MBN.
- I file marcati `F_KIND_EXTRACT` sono **lossy**: vanno letti con `extract-show` (non con `decompress`).
