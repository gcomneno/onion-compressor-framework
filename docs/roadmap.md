# Roadmap

Ultimo aggiornamento: 2026-01-11

## Fatto (stato attuale)

- [x] Step legacy v1–v4: compressione/decompressione lossless
- [x] Container v5: `layer_id + codec_id + meta + payload`
- [x] Payload “bundle”: supporto stream multipli (v5: HBN2/ZBN2/ZRAW1)
- [x] Container v6: header compatto con ID numerici (u8) + meta opzionale + payload_len omesso
- [x] Codec zstd + variante `zstd_tight` (frame overhead ridotto: no content size, no checksum)
- [x] c5/d5 (v5), c6/d6 (v6) + auto-pick su CSV (layer e codec)
- [x] c7/d7 (universale): v6 + MBN “Multi Bundle” multi-stream (bytes/vc0/split_text_nums)
- [x] Nuovo layer lossless `split_text_nums`: TEXT/NUMS con regole semantiche standard e meta versionata
- [x] Codec `num_v0` (base) per stream di int (varint)
- [x] Lossy separato: comando `extract` (+ `extract-show`) per estrazioni mirate (non decompressabile lossless)
- [x] Roundtrip test `tests/run_roundtrip.sh` (v1–v6 + c7 subset)
- [x] Benchmark unico `scripts/bench_all.sh` (v1–v6 + c7), con skip automatico zstd se `zstandard` non è installato
- [x] Analyzer: fingerprint (simhash) + bucketization (fallback modulo, plugin Turbo-Bucketizer opzionale via `TB_MODULE`)
- [x] Tool directory: `src/python/gcc_dir.py` (`packdir`/`unpackdir`) = bucketing + piano per-bucket + batch compress/decompress (lossless)
- [x] `packdir` mini-autopick per bucket: prova pipeline candidate su un campione e sceglie per size (deterministico e capped)

## Prossimi micro-step (senza refactor totale)

- [ ] Aggiungere un mini-report aggregato per `packdir` (top bucket, top estensioni, saving per plan).
- [ ] Potenziare `num_v0` (delta/zigzag/RLE) mantenendo compatibilità (nuovo `num_v1` o meta-version nel codec).
- [ ] Aggiungere un layer “fattura-like” dedicato (solo se porta vantaggio reale): template + stream variabili.
- [ ] Stabilizzare e documentare la spec MBN (campi, stream types, meta, invarianti) con test vectors.

## Refactor grosso (quando ci mettiamo davvero)

- [ ] Spezzare `gcc_huffman.py` (legacy + CLI + framework) in moduli puliti.
- [ ] Registry dinamico layer/codec (plugin) invece di mapping statici.
- [ ] Spec byte-level completa dei formati (con compatibilità e migrazioni).

## Script

- `scripts/bench_all.sh` è lo script standard.
- `tests/run_roundtrip.sh` è il sanity-check lossless.
