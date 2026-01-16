# Design notes

Ultimo aggiornamento: 2026-01-09

## Obiettivo: “cipolla / lasagna” ma senza farsi male

La pipeline “pulita” è:

1. **Layer** (semantico): `bytes -> symbols + meta (opzionale)`
2. **Codec** (compressione): prende **tutto ciò che serve** (symbols + meta/vocab) e lo comprime secondo l’algoritmo scelto
3. **Container**: impacchetta (layer_id, codec_id, meta minimale, payload già compresso)

Quindi: *la compressione è responsabilità del codec*. Il container non deve “inventarsi” compressioni extra.

## Meta opzionale (lossy / semantica pura)

Un layer può anche decidere di produrre solo `symbols` (meta vuoto).  
L’engine non deve impallarsi: `meta` può essere `None` / `{}` / `b""` e la pipeline deve continuare.

## v5: container + bundle (per evitare il “vocab immenso non compresso”)

Il costo vero, nei layer a vocabolario (sillabe/parole), non è solo la sequenza di IDs, ma anche:

- vocabolario (lista token)
- mapping / info di decodifica

Con il **bundle** v5:
- il vocabolario/meta diventano **stream dedicati** (es. `__meta__`)
- lo stesso codec (Huffman) li può comprimere, come fa per gli altri stream
- niente hack “meta in JSON base64” che gonfia a dismisura su file piccoli

## Nota di naming (importante)

Nel vecchio design c’era l’idea “v5 = lemmi + tag”.  
Ora **v5 è il formato container/bundle**, quindi quella cosa (se la faremo) deve avere un altro nome (es. `layer_id=lemmas_it` o una futura v6).  
Se chiamiamo tutto “v5”, poi ci spariamo nei piedi.
