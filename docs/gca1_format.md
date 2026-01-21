# GCC-OCF – GCA1 (Bucket Archive) Format
Ultimo aggiornamento: 2026-01-21

Questo documento descrive il formato **GCA1** usato nel workflow directory-based classico (`gcc-ocf dir pack/unpack`).

GCA1 è un “framing container” per bucket:
- non inventa un nuovo codec
- tratta ogni blob come **bytes a scatola chiusa**
- aggiunge un index JSONL compresso e un trailer fisso

Riferimenti implementativi:
- `src/gcc_ocf/core/gca.py`
- `src/gcc_ocf/verify.py`

---

# 1) Obiettivo
GCA1 è un contenitore **append-friendly** per un bucket.
Ogni entry è un blob già compresso (tipicamente un container v6).

Workflow tipico:
1) scrivi blobs (entries)
2) scrivi index JSONL (compresso)
3) scrivi trailer fisso

---

# 2) Layout file (byte-level)

```
[blob0][blob1]...[blobN-1][index_zlib][TRAILER]
````

- I blob sono concatenati “a pacco”, senza header per-entry.
- `index_zlib` è un unico blob compresso con zlib che contiene UTF-8 JSONL.
- `TRAILER` è fisso a 16 byte.

---

# 3) Trailer (16 bytes)

| Offset (dal fondo) | Size | Campo | Tipo | Descrizione |
|---:|---:|---|---|---|
| -16 | 4 | magic | bytes | `b"GCA1"` |
| -12 | 8 | index_len | uint64 LE | lunghezza in bytes di `index_zlib` |
| -4 | 4 | index_crc32 | uint32 LE | CRC32 su `index_zlib` |

Validazione minima (reader):
- magic deve essere `GCA1`
- `index_len` deve stare dentro al file
- CRC32 di `index_zlib` deve combaciare

---

# 4) Index (`index_zlib`) – JSONL compresso
`index_zlib` (dopo zlib-decompress) è **UTF-8 JSONL**: una riga = un oggetto JSON.

## 4.1 Record entry (standard)
Per ogni entry viene scritto un record con almeno:

| Campo | Tipo | Richiesto | Note |
|---|---|---:|---|
| `rel` | string | sì | path relativo (es. `a.txt`) oppure risorsa `__res__/NAME` |
| `offset` | int | sì | offset byte dall’inizio del file dove parte il blob |
| `length` | int | sì | lunghezza in bytes del blob |

### Meta per-entry

Il writer può includere meta aggiuntiva come campi extra.
Campi tipici:

| Campo | Tipo | Note |
|---|---|---|
| `blob_sha256` | string hex | sha256 del blob |
| `blob_crc32` | int | crc32 del blob |

## 4.2 Record trailer (ultima riga)

L’ultima riga dell’index è un record trailer con:

| Campo | Valore |
|---|---|
| `kind` | `"trailer"` |
| `schema` | `"gca.index_trailer.v1"` |
| `index_body_sha256` | sha256 dell’**index body** (tutte le righe entry, con `\n`) |
| `entries` | numero entries |

L’hash `index_body_sha256` protegge dall’alterazione dell’index (oltre al CRC32 del blob compresso).

---

# 5) Resources bucket-level

Le resources sono blob “speciali” scritti come entry normali ma con path riservato:

- `rel = "__res__/NAME"`

e meta che include tipicamente:
- `kind = "resource"`
- `res_name = "NAME"`

`GCAReader.load_resources()` restituisce:
- `NAME -> { "blob": <bytes>, "meta": <dict> }`

---

# 6) Verify (light vs full)

## 6.1 verify light

- valida trailer (magic, index_len, CRC32 index_zlib)
- valida `index_body_sha256` del trailer JSONL (se presente)
- cross-check `manifest.jsonl` ↔ index GCA:
  - join per `(archive_offset, archive_length)` del manifest (robusto)
  - `rel` usato come best-effort
- valida presenza resources richieste da `bucket_summary`

## 6.2 verify full

In aggiunta al light:
- ricalcola sha256 + crc32 dei blob (streaming, chunked)
- confronta con `blob_sha256` / `blob_crc32` presenti nell’index
- ricalcola hash dei blob resources quando è disponibile `blob_sha256` in meta

## 6.3 Exit code (CLI)

Vedi `docs/exit_codes.md` per la tabella completa.

Errori tipici:
- payload/manifest/index corrotti → `CorruptPayload` → 10
- versione non supportata → `UnsupportedVersion` → 11
- resource mancante → `MissingResource` → 12
- hash mismatch (tamper) → `HashMismatch` → 13

---

# 7) Esempio minimo (intuizione)

1) pack:

```bash
gcc-ocf dir pack ./IN ./OUT --buckets 8
````

2. verify:

```bash
gcc-ocf dir verify ./OUT
# oppure
gcc-ocf dir verify ./OUT --full
```

3. unpack:

```bash
gcc-ocf dir unpack ./OUT ./RESTORED
```

---

## 9) `docs/exit_codes.md` (GENERATO) — COSA FARE

Questo file dice esplicitamente “GENERATED FILE — do not edit manually”. :contentReference[oaicite:1]{index=1}  
Quindi: **non lo tocco**. Il lavoro “coerente” lo faccio negli altri docs: ora puntano a lui come fonte unica.

---
