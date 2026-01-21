# Benchmarks
Ultimo aggiornamento: 2026-01-21

Questi numeri servono **solo** come termometro (proto). Cambieranno al cambiare di layer, codec e packaging. Non sono “performance claims”, sono *sanity metrics*.

---

## Come eseguire

Benchmark (solo compressione, misura size/ratio):
```bash
bash scripts/bench_all.sh
````

Roundtrip lossless (compressione + decompressione + diff):
```bash
bash tests/run_roundtrip.sh
```

Note:
- Lo script benchmark può fare skip automatico di zstd se il modulo Python non è installato.
- Il roundtrip è il “gate” vero: se fallisce, la ratio non interessa.

---

## Risultati (estratto)

### small.txt (1038 B)
| Variante                 | Size (B) | Ratio |
| ------------------------ | -------: | ----: |
| v1 (bytes)               |      831 | 0.801 |
| v2 (V/C/O)               |     3790 | 3.651 |
| v3 (sillabe)             |     1728 | 1.665 |
| v4 (parole)              |     1795 | 1.729 |
| v5 bundle (bytes)        |      754 | 0.726 |
| v5 bundle (syllables_it) |     1266 | 1.220 |
| v5 bundle (words_it)     |     1265 | 1.219 |

Migliore legacy: **v1 (bytes)**.
Migliore overall: **v5 bundle (bytes)**.

### medium.txt (4615 B)
| Variante                 | Size (B) | Ratio |
| ------------------------ | -------: | ----: |
| v1 (bytes)               |     2900 | 0.628 |
| v2 (V/C/O)               |     6046 | 1.310 |
| v3 (sillabe)             |     4623 | 1.002 |
| v4 (parole)              |     6408 | 1.389 |
| v5 bundle (bytes)        |     2775 | 0.601 |
| v5 bundle (syllables_it) |     3544 | 0.768 |
| v5 bundle (words_it)     |     4127 | 0.894 |

Migliore legacy: **v1 (bytes)**.
Migliore overall: **v5 bundle (bytes)**.

### large.txt (628014 B)
| Variante                 | Size (B) | Ratio |
| ------------------------ | -------: | ----: |
| v1 (bytes)               |   348839 | 0.555 |
| v2 (V/C/O)               |   401769 | 0.640 |
| v3 (sillabe)             |   272731 | 0.434 |
| v4 (parole)              |   148262 | 0.236 |
| v5 bundle (bytes)        |   348769 | 0.555 |
| v5 bundle (syllables_it) |   271797 | 0.433 |
| v5 bundle (words_it)     |   146214 | 0.233 |

Migliore legacy: **v4 (parole)**.
Migliore overall: **v5 bundle (words_it)**.

---

## Note veloci
- Su file piccoli, l’overhead del “vocabolario” e del packaging pesa tantissimo: è normale vedere ratio peggiori per layer semantici.
- Su file grandi, **words_it** vince perché riduce l’entropia “visibile” al codec: il vocabolario compresso diventa un costo marginale.
- I bundle (v5/v6+MBN) aiutano perché comprimono anche meta/vocab come stream, invece di lasciarli “fuori banda”.

````

---
