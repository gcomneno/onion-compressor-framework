# tests/ – How to play with huffman-compressor

Questa directory contiene:
- file di test (`testdata/`)
- script (eventuali) per provare compressione/decompressione
- idee per benchmark manuali

L’obiettivo non è avere una test-suite super formale, ma una **palestra** per vedere come si comportano gli step (v1–v4) su testi diversi.

---

## 1. Struttura consigliata

```text
tests/
├─ README.md          # questo file
└─ testdata/
   ├─ small.txt       # file di test piccolo (1–10 KB)
   ├─ medium.txt      # file di test medio (10–200 KB)
   ├─ large.txt       # opzionale, per futuri test >200 KB
   └─ ...             # altri che vuoi aggiungere
```

I nomi sono solo suggeriti, puoi chiamarli come ti pare.

---

## 2. Che file usare in `testdata/`

Qualche idea:

* `small.txt`

  * 1–5 KB
  * qualcosa di breve: un paio di pagine di appunti, un estratto di testo, ecc.
  * serve a vedere **quanto pesa l’header** e come *peggiora* la compressione.

* `medium.txt`

  * 20–200 KB
  * un capitolo di libro, documentazione tecnica, un articolo lungo.
  * utile per vedere se Step3/Step4 iniziano a dare qualche segnale interessante.

* `large.txt` (opzionale per ora)

  * 200 KB – qualche MB
  * più avanti servirà per test “seri” sulle differenze tra v1–v4.

Consigli:

* usare **testi in italiano** (visto il focus),
* evitare PDF binari, immagini ecc. → solo `.txt`.

---

## 3. Test manuali: roundtrip

Dalla root del progetto:

```bash
# Esempio con small.txt
python3 src/python/gcc_huffman.py c1 tests/testdata/small.txt tests/testdata/small.v1.gcc
python3 src/python/gcc_huffman.py d1 tests/testdata/small.v1.gcc tests/testdata/small.v1.dec.txt
diff tests/testdata/small.txt tests/testdata/small.v1.dec.txt

python3 src/python/gcc_huffman.py c2 tests/testdata/small.txt tests/testdata/small.v2.gcc
python3 src/python/gcc_huffman.py d2 tests/testdata/small.v2.gcc tests/testdata/small.v2.dec.txt
diff tests/testdata/small.txt tests/testdata/small.v2.dec.txt

python3 src/python/gcc_huffman.py c3 tests/testdata/small.txt tests/testdata/small.v3.gcc
python3 src/python/gcc_huffman.py d3 tests/testdata/small.v3.gcc tests/testdata/small.v3.dec.txt
diff tests/testdata/small.txt tests/testdata/small.v3.dec.txt

python3 src/python/gcc_huffman.py c4 tests/testdata/small.txt tests/testdata/small.v4.gcc
python3 src/python/gcc_huffman.py d4 tests/testdata/small.v4.gcc tests/testdata/small.v4.dec.txt
diff tests/testdata/small.txt tests/testdata/small.v4.dec.txt
```

Se i `diff` non stampano niente, la compressione è **lossless** (come deve essere).

Puoi ripetere lo stesso schema con `medium.txt` e `large.txt`.

---

## 4. Leggere le statistiche

Ogni comando `c1`/`c2`/`c3`/`c4` stampa qualcosa tipo:

```text
=== GCC Huffman stats (StepX ...) ===
File originale : ... (N byte)
File compresso : ... (M byte)
Rapporto       : R (1.0 = nessuna compressione)
Bit/simbolo    : B (8.0 = non compresso)
===============================
```

Interpretazione rapida:

* `Rapporto`:

  * < 1.0  → stai effettivamente comprimendo,
  * = 1.0  → neutro,
  * > 1.0  → la “compressione” ti sta facendo ingrassare il file.
* `Bit/simbolo`:

  * 8.0  = livello “non compresso” (1 byte = 8 bit per simbolo),
  * < 8.0 = compressione,
  * > 8.0 = espansione.

Suggerimenti di lettura:

* su **file molto piccoli**, aspettati spesso rapporti > 1.0 → gli header dominano.
* usa `medium.txt` per confrontare v1 vs v3 vs v4:

  * v1 = baseline,
  * v3 = layer sillabe,
  * v4 = layer parole.

---

## 5. Piccolo “gioco” di benchmark

Quando avrai voglia, puoi farti un mini benchmark “artigianale”:

```bash
# su medium.txt
python3 src/python/gcc_huffman.py c1 tests/testdata/medium.txt tests/testdata/medium.v1.gcc
python3 src/python/gcc_huffman.py c3 tests/testdata/medium.txt tests/testdata/medium.v3.gcc
python3 src/python/gcc_huffman.py c4 tests/testdata/medium.txt tests/testdata/medium.v4.gcc

ls -l tests/testdata/medium*
```

Domande da porsi:

* chi tra v1 / v3 / v4 produce il file più piccolo?
* lo scarto è enorme o minimo?
* lo strato extra (sillabe/parole) sembra promettente o solo decorativo?

Annotarsi questi esperimenti può aiutare a decidere **dove investire tempo** (ottimizzare v1, potenziare v4, lavorare sui lemmi, ecc.).

---

## 6. Futuri test automatici (idea)

In futuro, si potrebbe aggiungere:

* un piccolo script Python in `tests/` che:

  * gira su tutti i file in `testdata/`,
  * per ogni formato (v1–v4):

    * comprime, decomprime,
    * controlla il roundtrip,
    * registra le statistiche in una tabellina.

Per ora, i test manuali sopra bastano per giocare e capire il comportamento della huffman. 🍝
