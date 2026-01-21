# Design notes
Ultimo aggiornamento: 2026-01-21

## Obiettivo: “cipolla” ma senza farsi venire pianti!

La pipeline “pulita” è:
1. **Layer (semantico)**: `input_bytes -> symbols + meta (opzionale)`
2. **Codec (compressione)**: comprime **tutto ciò che serve** (symbols + meta/vocab) secondo l’algoritmo scelto
3. **Container (framing)**: impacchetta `(layer_id, codec_id, meta minimale, payload già compresso)`

Regola d’oro: **la compressione è responsabilità del codec**.

Il container non deve “inventarsi” compressioni extra.

Terminologia usata:
- “bytes a scatola chiusa” = `bytes` che un layer trasporta/incapsula senza interpretarli.

## Meta opzionale (lossy / semantica pura)

Un layer può decidere di produrre solo `symbols` (meta vuoto).
L’engine non deve impallarsi: `meta` può essere `None` / `{}` / `b""` e la pipeline deve continuare.

## v5/v6: bundle per evitare “vocab immenso non compresso”

Il costo vero, nei layer a vocabolario (sillabe/parole), non è solo la sequenza di IDs, ma anche:
- vocabolario (lista token)
- mapping / info di decodifica

Con i bundle (v5) e con MBN (v6):
- vocabolario/meta diventano **stream dedicati** (es. `META` / `__meta__`)
- lo stesso codec li può comprimere come gli altri stream
- niente hack tipo “meta in JSON base64” che gonfia su file piccoli

## Nota di naming (importante)
Nel vecchio design c’era l’idea “v5 = lemmi + tag”. Ora **v5/v6** sono formati container/payload.
Quella cosa (se la faremo) deve avere un altro nome specifico (es. `layer_id=lemmas_it`).
Se chiamiamo tutto “v5”, poi ci spariamo nei piedi e piangiamo veramente!

## Stratificazione: non è fede, è enforcement

La stratificazione non deve dipendere dalla disciplina:
- i moduli “orchestrator” (CLI/profili) stanno sopra
- core/engine/legacy stanno sotto
- gli import “in salita” fanno fallire i test

Guardrail:
- `tests/test_arch_boundaries.py`
- `tools/check_arch_boundaries.py`

Bonus: l’Index layer per single/mixed è stato estratto in un modulo dedicato (`dir_index.py`), così non resta “logica di indice” sparsa negli orchestrator.
```

---
