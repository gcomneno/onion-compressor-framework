# Roadmap (GCC-OCF)
Ultimo aggiornamento: 2026-01-21

Questa roadmap vive in root perché è “developer-facing”: deve stare sotto gli occhi.
La documentazione di dettaglio resta in `docs/`.

---

## Stato attuale (fatto)

### Core: codec/container
- [x] Legacy v1–v4: compressione/decompressione lossless
- [x] Container v5: `layer_id + codec_id + meta + payload`
- [x] Container v6: header compatto con ID numerici (u8) + meta opzionale + payload_len omesso
- [x] Payload multi-stream: bundle (v5) + MBN (v6)
- [x] Codec `zstd` + `zstd_tight` (frame overhead ridotto)
- [x] c7/d7 “universale”: decode v1–v6 + v6+MBN
- [x] Layer lossless `split_text_nums` (TEXT/NUMS) con meta versionata
- [x] Codec numerico `num_v0` e `num_v1`

### Directory workflow (classico + single/mixed)
- [x] Tool directory legacy (`src/python/gcc_dir.py`): bucketing + piano per-bucket + batch compress/decompress
- [x] GCA1: bucket archive append-friendly + index JSONL compresso
- [x] Mini-autopick per bucket (sample + top-k deterministico e capped)
- [x] Modalità `--single-container` (TEXT-only): `bundle.gcc + bundle_index.json` (+ `--keep-concat`)
- [x] Modalità `--single-container-mixed`: `bundle_text.*` + `bundle_bin.*` (+ `--keep-concat`)
- [x] Verify “full” per mixed: errori di decode/decompress trattati come tamper (HashMismatch) per robustezza
- [x] Hygiene: decoder universale silenzioso per single-container (niente log rumorosi)

### Qualità / riproducibilità
- [x] Roundtrip test: `tests/run_roundtrip.sh`
- [x] Benchmark: `scripts/bench_all.sh` (skip zstd se mancante)
- [x] Stratificazione enforced: `tests/test_arch_boundaries.py` + `tools/check_arch_boundaries.py`
- [x] Index layer “da manuale” per single/mixed: `src/gcc_ocf/dir_index.py` (put/get/serialize/deserialize)

---

## Prossimi micro-step (senza refactor totale)

### Packaging / report
- [ ] Mini-report aggregato per `dir pack` classico: top bucket, top estensioni, saving per plan, “perché” del piano scelto.
- [ ] Rendere espliciti nel report: differenza tra “framing container” e “slice via index” per single/mixed.

### Numeri (codec)
- [ ] Potenziare `num_v1` (delta/zigzag/RLE) mantenendo compatibilità via meta-version (non “silenziosamente diverso”).
- [ ] Test vectors deterministici per `num_v1` (encode/decode).

### MBN
- [ ] Stabilizzare e documentare MBN: campi, stream types, invarianti, test vectors (byte-level), esempi reali.

### Verify semantics
- [ ] Documentare formalmente: quando un errore è `CorruptPayload` vs `HashMismatch` (tamper).
- [ ] “verify --json”: schema output stabile (ok+error).

---

## Refactor grosso (quando ci mettiamo davvero)

- [ ] Spezzare legacy: separare “framework/engine” da wrapper e vecchie funzioni (senza rompere gli script legacy).
- [ ] Registry dinamico layer/codec (plugin) invece di mapping statici (solo quando hai i test che lo blindano).
- [ ] Spec byte-level completa per tutti i formati con migrazioni (v1–v6, GCA1, MBN).

---

## Script standard

- Benchmark: `bash scripts/bench_all.sh`
- Sanity lossless: `bash tests/run_roundtrip.sh`
- Guardrail architettura: `python -m pytest -q tests/test_arch_boundaries.py`
