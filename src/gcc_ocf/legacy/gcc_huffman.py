#!/usr/bin/env python3
"""
GCC - Grande Compressione Cucita-a-mano (oppure Giancarlo Cicellyn Comneno) :-)

Core: Huffman su byte, riusato da:
- Step 1: un solo stream (v1)
- Step 2: 3 stream (mask / vocali / consonanti+altro) (v2)
- Step 3: token "sillabe" + blocchi non-lettera, Huffman sugli ID dei token (v3)
- Step 4: token "parole intere" + blocchi non-lettera, Huffman sugli ID dei token (v4)
"""
from __future__ import annotations

from typing import Dict, List, Tuple
from pathlib import Path

from gcc_ocf.core.codec_huffman import CodecHuffman
from gcc_ocf.core.bundle import SymbolStream, EncodedStream
from gcc_ocf.core.codec_huffman import (
    huffman_compress_core,
    huffman_decompress_core,
    huffman_compress_ids,
    huffman_decompress_ids,
)

from gcc_ocf.layers.bytes import LayerBytes
from gcc_ocf.layers.vc0 import LayerVC0
from gcc_ocf.layers.syllables_it import LayerSyllablesIT
from gcc_ocf.layers.words_it import LayerWordsIT

from gcc_ocf.engine.container import Engine
from gcc_ocf.engine.container_v6 import (
    compress_v6,
    decompress_v6,
    compress_v6_mbn,
    pack_container_v6,
    CODEC_TO_CODE,
    unpack_v6_mbn_raw,
)

from gcc_ocf.core.mbn_bundle import (
    pack_mbn,
    MBNStream,
    ST_MAIN,
    ST_MASK,
    ST_VOWELS,
    ST_CONS,
    ST_TEXT,
    ST_NUMS,
    ST_TPL,
    ST_IDS,
    ST_META,
)

from gcc_ocf.core.num_stream import encode_ints, decode_ints

import json
import re

import sys

MAGIC = b"GCC"
BUNDLE_MAGIC = b"HBN1"  # Huffman Bundle v1

VERSION_STEP1 = 1  # byte/bit stream
VERSION_STEP2 = 2  # maschera + vocali + resto
VERSION_STEP3 = 3  # sillabe
VERSION_STEP4 = 4  # parole intere
VERSION_STEP5 = 5  # (concettuale) – lemmi + tag morfologici (“vocabolario mentale”)

# -------------------
# Step 1: formato v1 (un solo stream)
# -------------------
def compress_bytes_v1(data: bytes) -> bytes:
    """
    Formato v1 (ottimizzato):
    [ MAGIC(3) | VERSION(1) | N(8) | NUM_SYMS(2)
      | (SYMBOL(1) + FREQ(4)) * NUM_SYMS
      | LASTBITS(1)
      | DATA(...) = bitstream Huffman ]
    """
    layer = LayerBytes()
    symbols, _layer_meta = layer.encode(data)

    N = len(data)

    codec = CodecHuffman()
    freq, lastbits, bitstream = codec.compress_bytes(symbols)

    # Caso particolare: file vuoto
    if N == 0:
        header = bytearray()
        header += MAGIC
        header.append(VERSION_STEP1)          # di solito = 1
        header += (0).to_bytes(8, "big")      # N = 0
        header += (0).to_bytes(2, "big")      # NUM_SYMS = 0
        header.append(0)                      # LASTBITS = 0
        # nessun bitstream
        return bytes(header)

    # Simboli effettivamente usati (freq > 0)
    used = [(sym, f) for sym, f in enumerate(freq) if f > 0]
    num_syms = len(used)
    if num_syms > 0xFFFF:
        raise ValueError("Troppi simboli distinti per NUM_SYMS (u16)")

    header = bytearray()
    header += MAGIC
    header.append(VERSION_STEP1)
    header += N.to_bytes(8, "big")
    header += num_syms.to_bytes(2, "big")

    for sym, f in used:
        header.append(sym)                 # SYMBOL (u8)
        header += f.to_bytes(4, "big")     # FREQ (u32)

    header.append(lastbits)

    return bytes(header) + bitstream

def decompress_bytes_v1(comp: bytes) -> bytes:
    """
    Decodifica formato v1 ottimizzato:

    [ MAGIC(3) | VERSION(1) | N(8) | NUM_SYMS(2)
      | (SYMBOL(1) + FREQ(4)) * NUM_SYMS
      | LASTBITS(1)
      | DATA(...) ]
    """
    idx = 0
    # header minimo: MAGIC(3) + VERSION(1) + N(8) + NUM_SYMS(2) + LASTBITS(1)
    min_header = 3 + 1 + 8 + 2 + 1
    if len(comp) < min_header:
        raise ValueError("Dati troppo corti per GCC v1 (header minimale)")

    magic = comp[idx:idx+3]
    idx += 3
    if magic != MAGIC:
        raise ValueError("Magic number non valido")
    version = comp[idx]
    idx += 1
    if version != VERSION_STEP1:
        raise ValueError(f"Versione Step1 inattesa: {version}")

    N = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8
    num_syms = int.from_bytes(comp[idx:idx+2], "big")
    idx += 2

    freq = [0] * 256
    for _ in range(num_syms):
        if idx + 1 + 4 > len(comp):
            raise ValueError("File troncato: freq table incompleta")
        sym = comp[idx]
        idx += 1
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq[sym] = f

    if idx >= len(comp):
        raise ValueError("File troncato: manca LASTBITS")

    lastbits = comp[idx]
    idx += 1

    bitstream = comp[idx:]

    codec = CodecHuffman()
    symbols = codec.decompress_bytes(freq, bitstream, N, lastbits)

    layer = LayerBytes()
    return layer.decode(symbols, {})

# -------------------
# Step 2: formato v2 (maschera + vocali + resto)
# -------------------
VOWELS = set("aeiouAEIOU")

def split_streams_v2(data: bytes) -> Tuple[bytes, bytes, bytes]:
    layer = LayerVC0()
    (mask, vowels, cons), _meta = layer.encode(data)
    return mask, vowels, cons

def merge_streams_v2(mask: bytes, vowels: bytes, cons: bytes) -> bytes:
    layer = LayerVC0()
    return layer.decode((mask, vowels, cons), {})

def compress_bytes_v2(data: bytes) -> bytes:
    """
    Formato v2 (Step 2):

    [ MAGIC(3) | VERSION(1)=2 | N(8) | LEN_V(8) | LEN_C(8)
      | FREQ_MASK[256]*4 | LASTBITS_MASK(1) | BSIZE_MASK(8)
      | FREQ_V[256]*4    | LASTBITS_V(1)    | BSIZE_V(8)
      | FREQ_C[256]*4    | LASTBITS_C(1)    | BSIZE_C(8)
      | DATA_MASK | DATA_V | DATA_C ]
    """
    N = len(data)
    mask, vowels, cons = split_streams_v2(data)

    freq_m, last_m, bs_m = huffman_compress_core(mask)
    freq_v, last_v, bs_v = huffman_compress_core(vowels)
    freq_c, last_c, bs_c = huffman_compress_core(cons)

    header = bytearray()
    header += MAGIC
    header.append(VERSION_STEP2)
    header += N.to_bytes(8, "big")

    # lunghezze dei flussi originali
    header += len(vowels).to_bytes(8, "big")  # LEN_V
    header += len(cons).to_bytes(8, "big")    # LEN_C
    # mask length = N, lo sappiamo già

    # FREQ + lastbits + dimensioni bitstream per ciascun flusso
    # MASK
    for f in freq_m:
        header += f.to_bytes(4, "big")
    header.append(last_m)
    header += len(bs_m).to_bytes(8, "big")

    # VOWELS
    for f in freq_v:
        header += f.to_bytes(4, "big")
    header.append(last_v)
    header += len(bs_v).to_bytes(8, "big")

    # CONS
    for f in freq_c:
        header += f.to_bytes(4, "big")
    header.append(last_c)
    header += len(bs_c).to_bytes(8, "big")

    return bytes(header) + bs_m + bs_v + bs_c

def decompress_bytes_v2(comp: bytes) -> bytes:
    """
    Decodifica formato v2 (Step 2).
    """
    # Controllo minimo: almeno header base
    min_header_base = 3 + 1 + 8 + 8 + 8   # MAGIC+VER+N+LEN_V+LEN_C
    if len(comp) < min_header_base:
        raise ValueError("Dati troppo corti per GCC v2 (base header)")

    idx = 0
    magic = comp[idx:idx+3]
    idx += 3
    if magic != MAGIC:
        raise ValueError("Magic non valido")
    version = comp[idx]
    idx += 1
    if version != VERSION_STEP2:
        raise ValueError(f"Versione v2 richiesta, trovato {version}")

    N = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8
    len_v = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8
    len_c = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8
    # mask length = N

    # MASK: freq + lastbits + bsize
    freq_m = []
    for _ in range(256):
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq_m.append(f)
    last_m = comp[idx]
    idx += 1
    bsize_m = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8

    # VOWELS
    freq_v = []
    for _ in range(256):
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq_v.append(f)
    last_v = comp[idx]
    idx += 1
    bsize_v = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8

    # CONS
    freq_c = []
    for _ in range(256):
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq_c.append(f)
    last_c = comp[idx]
    idx += 1
    bsize_c = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8

    # Bitstream per i tre flussi
    end_m = idx + bsize_m
    bs_m = comp[idx:end_m]
    idx = end_m

    end_v = idx + bsize_v
    bs_v = comp[idx:end_v]
    idx = end_v

    end_c = idx + bsize_c
    bs_c = comp[idx:end_c]

    # Decodifica i tre flussi
    mask = huffman_decompress_core(freq_m, bs_m, N, last_m)
    vowels = huffman_decompress_core(freq_v, bs_v, len_v, last_v)
    cons = huffman_decompress_core(freq_c, bs_c, len_c, last_c)

    # Ricostruisci il testo
    return merge_streams_v2(mask, vowels, cons)

# -------------------
# Step 3: formato v3 (sillabe + blocchi non-lettera)
# -------------------
def _is_ascii_letter(b: int) -> bool:
    return (65 <= b <= 90) or (97 <= b <= 122)  # A-Z a-z

_VOWEL_BYTES = {ord(c) for c in "aeiouAEIOU"}

def _is_vowel_byte(b: int) -> bool:
    return b in _VOWEL_BYTES

def split_word_into_syllables(word: bytes) -> List[bytes]:
    """
    Spezzettamento grezzo di una "parola" (solo lettere) in pseudo-sillabe:
    - accumula caratteri
    - spezza dopo ogni vocale
    Non è foneticamente perfetto, ma basta per sperimentare.
    """
    syllables: List[bytes] = []
    current = bytearray()
    for b in word:
        current.append(b)
        if _is_vowel_byte(b):
            syllables.append(bytes(current))
            current.clear()
    if current:
        syllables.append(bytes(current))
    return syllables

def tokenize_syllables_and_other(data: bytes) -> List[bytes]:
    """
    Trasforma il testo in una lista di token:
    - sequenze di lettere -> spezzate in pseudo-sillabe
    - sequenze di non-lettere -> tenute come blocchi separati
    """
    tokens: List[bytes] = []
    i = 0
    n = len(data)

    while i < n:
        b = data[i]
        if _is_ascii_letter(b):
            # raccogli sequenza di lettere
            start = i
            i += 1
            while i < n and _is_ascii_letter(data[i]):
                i += 1
            word = data[start:i]
            sylls = split_word_into_syllables(word)
            tokens.extend(sylls)
        else:
            # raccogli sequenza di non-lettere
            start = i
            i += 1
            while i < n and not _is_ascii_letter(data[i]):
                i += 1
            tokens.append(data[start:i])
    return tokens

def compress_bytes_v3(data: bytes) -> bytes:
    """
    Formato v3 (Step 3: sillabe):

    [ MAGIC(3) | VERSION(1)=3
      | N_TOKENS(8)
      | VOCAB_SIZE(4)
      | VOCAB:
          per i=0..VOCAB_SIZE-1:
             LEN(2) | TOKEN_BYTES
      | FREQ_ID[VOCAB_SIZE]*4
      | LASTBITS(1)
      | BITSTREAM_IDs(...) ]
    """
    # Tokenizzazione: pseudo-sillabe + blocchi non-lettera
    layer = LayerSyllablesIT()
    id_stream, meta = layer.encode(data)

    vocab_list = meta["vocab_list"]
    N_tokens = len(id_stream)
    vocab_size = len(vocab_list)

    # Huffman sugli ID
    codec = CodecHuffman()
    freq, lastbits, bitstream = codec.compress_ids(id_stream, vocab_size)

    # Header
    header = bytearray()
    header += MAGIC
    header.append(VERSION_STEP3)
    header += N_tokens.to_bytes(8, "big")

    # VOCAB_SIZE (4 byte)
    header += vocab_size.to_bytes(4, "big")

    # VOCAB
    for tok_bytes in vocab_list:
        L = len(tok_bytes)
        if L > 0xFFFF:
            raise ValueError("Token troppo lungo per LEN(2 byte)")
        header += L.to_bytes(2, "big")
        header += tok_bytes

    # FREQ_ID[VOCAB_SIZE]*4
    for f in freq:
        header += f.to_bytes(4, "big")

    # LASTBITS
    header.append(lastbits)

    return bytes(header) + bitstream

def decompress_bytes_v3(comp: bytes) -> bytes:
    """
    Decodifica formato v3 (Step 3: sillabe) con VOCAB_SIZE variabile.
    """
    idx = 0
    # MAGIC(3) + VERSION(1) + N_TOKENS(8) + VOCAB_SIZE(4)
    min_header_base = 3 + 1 + 8 + 4
    if len(comp) < min_header_base:
        raise ValueError("Dati troppo corti per GCC v3 (base header)")

    magic = comp[idx:idx+3]
    idx += 3
    if magic != MAGIC:
        raise ValueError("Magic non valido")

    version = comp[idx]
    idx += 1
    if version != VERSION_STEP3:
        raise ValueError(f"Versione v3 richiesta, trovato {version}")

    N_tokens = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8

    vocab_size = int.from_bytes(comp[idx:idx+4], "big")
    idx += 4

    # VOCAB
    vocab_list: List[bytes] = []
    for _ in range(vocab_size):
        if idx + 2 > len(comp):
            raise ValueError("File troncato (LEN token)")
        L = int.from_bytes(comp[idx:idx+2], "big")
        idx += 2
        if idx + L > len(comp):
            raise ValueError("File troncato (TOKEN)")
        tok = comp[idx:idx+L]
        idx += L
        vocab_list.append(tok)

    # FREQ_ID[VOCAB_SIZE]*4 + LASTBITS(1)
    freq_bytes = vocab_size * 4
    if idx + freq_bytes + 1 > len(comp):
        raise ValueError("File troncato (FREQ_ID o LASTBITS)")

    freq: List[int] = []
    for _ in range(vocab_size):
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq.append(f)

    lastbits = comp[idx]
    idx += 1

    bitstream = comp[idx:]

    if N_tokens == 0:
        return b""

    ids = huffman_decompress_ids(freq, N_tokens, lastbits, bitstream)

    # Ricostruisci il testo concatenando i token
    layer = LayerSyllablesIT()
    return layer.decode(ids, {"vocab_list": vocab_list})

def compress_bytes_v4(data: bytes) -> bytes:
    """
    Formato v4 (Step 4: parole intere + blocchi non-lettera):

    [ MAGIC(3) | VERSION(1)=4
      | N_TOKENS(8)
      | VOCAB_SIZE(4)
      | VOCAB:
          per i=0..VOCAB_SIZE-1:
             LEN(2) | TOKEN_BYTES
      | FREQ_ID[VOCAB_SIZE]*4
      | LASTBITS(1)
      | BITSTREAM_IDs(...) ]
    """
    # Tokenizzazione: parole intere + blocchi non-lettera
    layer = LayerWordsIT()
    id_stream, meta = layer.encode(data)

    N_tokens = len(id_stream)

    vocab_list = meta["vocab_list"]
    vocab_size = len(vocab_list)

    codec = CodecHuffman()
    freq, lastbits, bitstream = codec.compress_ids(id_stream, vocab_size)

    # Caso particolare: nessun token
    if N_tokens == 0:
        header = bytearray()
        header += MAGIC
        header.append(VERSION_STEP4)
        header += (0).to_bytes(8, "big")   # N_TOKENS
        header += (0).to_bytes(4, "big")   # VOCAB_SIZE
        header.append(0)                   # LASTBITS
        return bytes(header)

    # Header
    header = bytearray()
    header += MAGIC
    header.append(VERSION_STEP4)
    header += N_tokens.to_bytes(8, "big")

    # VOCAB_SIZE (4 byte)
    header += vocab_size.to_bytes(4, "big")

    # VOCAB
    for tok_bytes in vocab_list:
        L = len(tok_bytes)
        if L > 0xFFFF:
            raise ValueError("Token troppo lungo per LEN(2 byte)")
        header += L.to_bytes(2, "big")
        header += tok_bytes

    # FREQ_ID[VOCAB_SIZE]*4
    for f in freq:
        header += f.to_bytes(4, "big")

    # LASTBITS
    header.append(lastbits)

    return bytes(header) + bitstream

def decompress_bytes_v4(comp: bytes) -> bytes:
    """
    Decodifica formato v4 (Step 4: parole intere + blocchi non-lettera) con VOCAB_SIZE variabile.
    """
    idx = 0
    # MAGIC(3) + VERSION(1) + N_TOKENS(8) + VOCAB_SIZE(4)
    min_header_base = 3 + 1 + 8 + 4
    if len(comp) < min_header_base:
        raise ValueError("Dati troppo corti per GCC v4 (base header)")

    magic = comp[idx:idx+3]
    idx += 3
    if magic != MAGIC:
        raise ValueError("Magic non valido")

    version = comp[idx]
    idx += 1
    if version != VERSION_STEP4:
        raise ValueError(f"Versione v4 richiesta, trovato {version}")

    N_tokens = int.from_bytes(comp[idx:idx+8], "big")
    idx += 8

    vocab_size = int.from_bytes(comp[idx:idx+4], "big")
    idx += 4

    # VOCAB
    vocab_list: List[bytes] = []
    for _ in range(vocab_size):
        if idx + 2 > len(comp):
            raise ValueError("File troncato (LEN token)")
        L = int.from_bytes(comp[idx:idx+2], "big")
        idx += 2
        if idx + L > len(comp):
            raise ValueError("File troncato (TOKEN)")
        tok = comp[idx:idx+L]
        idx += L
        vocab_list.append(tok)

    # FREQ_ID[VOCAB_SIZE]*4 + LASTBITS(1)
    freq_bytes = vocab_size * 4
    if idx + freq_bytes + 1 > len(comp):
        raise ValueError("File troncato (FREQ_ID o LASTBITS)")

    freq: List[int] = []
    for _ in range(vocab_size):
        f = int.from_bytes(comp[idx:idx+4], "big")
        idx += 4
        freq.append(f)

    lastbits = comp[idx]
    idx += 1

    bitstream = comp[idx:]

    if N_tokens == 0:
        return b""

    ids = huffman_decompress_ids(freq, N_tokens, lastbits, bitstream)

    layer = LayerWordsIT()
    return layer.decode(ids, {"vocab_list": vocab_list})

def _parse_csv(value: str) -> list[str]:
    parts = [p.strip() for p in value.split(",")]
    return [p for p in parts if p]


def compress_file_v5(input_path: str, output_path: str, layer_id: str = "bytes", codec_id: str = "huffman") -> None:
    """
    v5 compress.

    layer_id and codec_id can be:
      - a single id (e.g. "bytes", "zstd")
      - a CSV list of candidates (e.g. "bytes,words_it" and/or "huffman,zstd").
        In CSV mode we try all combinations and write the smallest output, storing
        the chosen (layer_id, codec_id) into the container header.
    """
    data = Path(input_path).read_bytes()
    eng = Engine.default()

    layer_candidates = _parse_csv(layer_id) if ("," in layer_id) else [layer_id]
    codec_candidates = _parse_csv(codec_id) if ("," in codec_id) else [codec_id]

    # Validate candidates against what the engine exposes (keeps this future-proof).
    known_layers = set(getattr(eng, "layers", {}).keys())
    known_codecs = set(getattr(eng, "codecs", {}).keys())

    bad_layers = [x for x in layer_candidates if x not in known_layers]
    bad_codecs = [x for x in codec_candidates if x not in known_codecs]

    if bad_layers:
        raise SystemExit(f"[ERR] layer_id non valido: {bad_layers} (validi: {', '.join(sorted(known_layers))})")
    if bad_codecs:
        raise SystemExit(f"[ERR] codec_id non valido: {bad_codecs} (validi: {', '.join(sorted(known_codecs))})")

    best_blob: bytes | None = None
    best_layer: str | None = None
    best_codec: str | None = None

    for lid in layer_candidates:
        for cid in codec_candidates:
            blob = eng.compress(data, layer_id=lid, codec_id=cid)
            if best_blob is None or len(blob) < len(best_blob):
                best_blob = blob
                best_layer = lid
                best_codec = cid

    assert best_blob is not None and best_layer is not None and best_codec is not None

    Path(output_path).write_bytes(best_blob)

    # stats quick & dirty
    orig_n = len(data)
    comp_n = len(best_blob)
    ratio = (comp_n / orig_n) if orig_n else 0.0
    print(f"=== GCC Container v5 ===")
    if len(layer_candidates) > 1 or len(codec_candidates) > 1:
        print(f"Candidates     : layers={','.join(layer_candidates)}  codecs={','.join(codec_candidates)}")
    print(f"Layer/Codec    : {best_layer} / {best_codec}")
    print(f"File originale : {input_path} ({orig_n} byte)")
    print(f"File compresso : {output_path} ({comp_n} byte)")
    print(f"Rapporto       : {ratio:.3f} (1.0 = nessuna compressione)")
    print("========================")


def decompress_file_v5(input_path: str, output_path: str) -> None:
    blob = Path(input_path).read_bytes()
    eng = Engine.default()
    data = eng.decompress(blob)
    Path(output_path).write_bytes(data)
    print(f"Decompressione v5 completata: {output_path}")

def compress_file_v1(input_path: str | Path, output_path: str | Path) -> None:
    data = Path(input_path).read_bytes()
    comp = compress_bytes_v1(data)
    Path(output_path).write_bytes(comp)

def decompress_file_v1(input_path: str | Path, output_path: str | Path) -> None:
    comp = Path(input_path).read_bytes()
    data = decompress_bytes_v1(comp)
    Path(output_path).write_bytes(data)

def compress_file_v2(input_path: str | Path, output_path: str | Path) -> None:
    data = Path(input_path).read_bytes()
    comp = compress_bytes_v2(data)
    Path(output_path).write_bytes(comp)

def decompress_file_v2(input_path: str | Path, output_path: str | Path) -> None:
    comp = Path(input_path).read_bytes()
    data = decompress_bytes_v2(comp)
    Path(output_path).write_bytes(data)

def compress_file_v3(input_path: str | Path, output_path: str | Path) -> None:
    data = Path(input_path).read_bytes()
    comp = compress_bytes_v3(data)
    Path(output_path).write_bytes(comp)

def decompress_file_v3(input_path: str | Path, output_path: str | Path) -> None:
    comp = Path(input_path).read_bytes()
    data = decompress_bytes_v3(comp)
    Path(output_path).write_bytes(data)

def compress_file_v4(input_path: str | Path, output_path: str | Path) -> None:
    data = Path(input_path).read_bytes()
    comp = compress_bytes_v4(data)
    Path(output_path).write_bytes(comp)

def decompress_file_v4(input_path: str | Path, output_path: str | Path) -> None:
    comp = Path(input_path).read_bytes()
    data = decompress_bytes_v4(comp)
    Path(output_path).write_bytes(data)

def _freq_to_used(freq: List[int]) -> List[tuple[int, int]]:
    return [(i, f) for i, f in enumerate(freq) if f > 0]

def _used_to_freq(used: List[tuple[int, int]], alphabet_size: int) -> List[int]:
    freq = [0] * alphabet_size
    for sym, f in used:
        if sym < 0 or sym >= alphabet_size:
            raise ValueError("freq_used contiene sym fuori range")
        freq[sym] = f
    return freq

def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def compress_file_v6(input_path: str, output_path: str, layer_id: str = "bytes", codec_id: str = "huffman") -> None:
    from pathlib import Path
    from gcc_ocf.engine.container import Engine

    data = Path(input_path).read_bytes()
    eng = Engine.default()

    layers = _split_csv(layer_id) or ["bytes"]
    codecs = _split_csv(codec_id) or ["huffman"]

    best_blob = None
    best_layer = None
    best_codec = None

    if len(layers) > 1 or len(codecs) > 1:
        print(f"=== GCC Container v6 ===")
        print(f"Candidates     : layers={','.join(layers)}  codecs={','.join(codecs)}")

    for lid in layers:
        for cid in codecs:
            blob = compress_v6(eng, data, layer_id=lid, codec_id=cid)
            if best_blob is None or len(blob) < len(best_blob):
                best_blob = blob
                best_layer = lid
                best_codec = cid

    assert best_blob is not None and best_layer is not None and best_codec is not None

    Path(output_path).write_bytes(best_blob)

    in_size = len(data)
    out_size = len(best_blob)
    ratio = out_size / in_size if in_size else 0.0

    print("=== GCC Container v6 ===")
    print(f"Layer/Codec    : {best_layer} / {best_codec}")
    print(f"File originale : {input_path} ({in_size} byte)")
    print(f"File compresso : {output_path} ({out_size} byte)")
    print(f"Rapporto       : {ratio:.3f} (1.0 = nessuna compressione)")
    print("========================")

def decompress_file_v6(input_path: str, output_path: str) -> None:
    from pathlib import Path
    from gcc_ocf.engine.container import Engine

    blob = Path(input_path).read_bytes()
    eng = Engine.default()
    data = decompress_v6(eng, blob)
    Path(output_path).write_bytes(data)
    print(f"Decompressione v6 completata: {output_path}")


def _parse_stream_codecs_spec(spec: str | None) -> dict[int, str] | None:
    """Parsa una mappa per-stream codec: 'MAIN:zstd_tight,MASK:zstd,CONS:huffman'.

    - key: nome stream (MAIN/MASK/VOWELS/CONS/TEXT/NUMS/META) oppure numero (0..255)
    - value: codec_id (es: zstd_tight, num_v1, num_v0, raw, ...)
    """
    if not spec:
        return None

    name_to_code = {
        "MAIN": ST_MAIN,
        "MASK": ST_MASK,
        "VOWELS": ST_VOWELS,
        "CONS": ST_CONS,
        "TEXT": ST_TEXT,
        "NUMS": ST_NUMS,
        "TPL": ST_TPL,
        "IDS": ST_IDS,
        "META": ST_META,
        "__META__": ST_META,
    }

    out: dict[int, str] = {}
    parts = [p.strip() for p in re.split(r"[;,]", spec) if p.strip()]
    for p in parts:
        if ":" not in p:
            raise ValueError(f"stream-codecs: token senza ':': {p!r}")
        k, v = [x.strip() for x in p.split(":", 1)]
        if not v:
            raise ValueError(f"stream-codecs: codec vuoto per {k!r}")
        kk = k.upper()
        if kk.isdigit():
            code = int(kk)
        else:
            if kk not in name_to_code:
                raise ValueError(f"stream-codecs: stream sconosciuto: {k!r}")
            code = int(name_to_code[kk])

        out[code] = v
    return out

def compress_file_v7(input_path: str, output_path: str, layer_id: str = "bytes", codec_id: str = "zstd_tight", stream_codecs_spec: str | None = None) -> None:
    """c7: v6 + payload MBN (multi-stream).

    Layer supportati (attuali):
      - bytes
      - vc0
      - split_text_nums (lossless: TEXT/NUMS)
      - tpl_lines_v0 (lossless: TPL/IDS/NUMS)
    """
    data = Path(input_path).read_bytes()
    eng = Engine.default()

    if layer_id not in ("bytes", "vc0", "split_text_nums", "tpl_lines_v0"):
        raise ValueError("c7 per ora supporta solo layer_id=bytes/vc0/split_text_nums/tpl_lines_v0")
    if codec_id not in ("zstd", "zstd_tight", "zlib", "raw", "num_v0", "num_v1"):
        raise ValueError("c7 per ora supporta codec_id=zstd/zstd_tight/zlib/raw/num_v0/num_v1")

    stream_codecs = _parse_stream_codecs_spec(stream_codecs_spec)
    # default smart routing per split_text_nums: NUMS -> num_v1, TEXT -> codec_id
    if layer_id == "split_text_nums" and stream_codecs is None:
        stream_codecs = {
            ST_TEXT: codec_id,
            ST_NUMS: "num_v1",
        }
    # default smart routing per tpl_lines_v0: TPL -> codec_id, IDS/NUMS -> num_v1
    if layer_id == "tpl_lines_v0" and stream_codecs is None:
        stream_codecs = {
            ST_TPL: codec_id,
            ST_IDS: "num_v1",
            ST_NUMS: "num_v1",
        }
    # Nota: ST_META (se presente) viene sempre forzato a 'raw' nel container.
    blob = compress_v6_mbn(eng, data, layer_id=layer_id, codec_id=codec_id, stream_codecs=stream_codecs)
    Path(output_path).write_bytes(blob)

    in_size = len(data)
    out_size = len(blob)
    ratio = out_size / in_size if in_size else 0.0
    print("=== GCC Container v6 + MBN (c7) ===")
    print(f"Layer/Codec    : {layer_id} / {codec_id}")
    if stream_codecs_spec:
        print(f"Stream codecs  : {stream_codecs_spec}")
    print(f"File originale : {input_path} ({in_size} byte)")
    print(f"File compresso : {output_path} ({out_size} byte)")
    print(f"Rapporto       : {ratio:.3f} (1.0 = nessuna compressione)")
    print("===============================")


def decompress_file_v7(input_path: str, output_path: str) -> None:
    """d7: decompress 'universale' (v1..v6 + c7 MBN)."""
    blob = Path(input_path).read_bytes()
    if len(blob) < 4 or blob[:3] != MAGIC:
        raise ValueError("File non GCC (magic mancante)")

    ver = blob[3]
    if ver == VERSION_STEP1:
        data = decompress_bytes_v1(blob)
    elif ver == VERSION_STEP2:
        data = decompress_bytes_v2(blob)
    elif ver == VERSION_STEP3:
        data = decompress_bytes_v3(blob)
    elif ver == VERSION_STEP4:
        data = decompress_bytes_v4(blob)
    elif ver == 5:
        eng = Engine.default()
        data = eng.decompress(blob)
    elif ver == 6:
        eng = Engine.default()
        data = decompress_v6(eng, blob)
    else:
        raise ValueError(f"Versione GCC non supportata: {ver}")

    Path(output_path).write_bytes(data)
    print(f"Decompressione d7 completata: {output_path}")


def extract_numbers_only(input_path: str, output_path: str) -> None:
    """Lossy: estrae solo i numeri interi da un file e salva un container v6 (EXTRACT).

    NOTA: il risultato NON è decompressabile in modo lossless.
    Va letto con 'extract-show'.
    """
    src = Path(input_path).read_bytes()
    text = src.decode("utf-8", errors="ignore")
    nums = [int(x) for x in re.findall(r"-?\d+", text)]

    eng = Engine.default()
    raw_nums = encode_ints(nums)
    numc = eng.codecs["num_v1"]
    rawc = eng.codecs["raw"]

    comp_nums = numc.compress(raw_nums)
    meta_obj = {
        "extractor": "numbers_only",
        "count": len(nums),
        "src_bytes": len(src),
    }
    meta_b = json.dumps(meta_obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    comp_meta = rawc.compress(meta_b)

    streams = [
        MBNStream(
            stype=ST_NUMS,
            codec=CODEC_TO_CODE["num_v1"],
            ulen=len(raw_nums),
            comp=comp_nums,
            meta=b"",
        ),
        MBNStream(
            stype=ST_META,
            codec=CODEC_TO_CODE["raw"],
            ulen=len(meta_b),
            comp=comp_meta,
            meta=b"",
        ),
    ]

    payload = pack_mbn(streams)
    blob = pack_container_v6(payload, layer_id="bytes", codec_id="mbn", meta=b"", is_extract=True)
    Path(output_path).write_bytes(blob)
    print(f"EXTRACT scritto: {output_path} (nums={len(nums)})")


def extract_show(input_path: str) -> None:
    """Mostra il contenuto di un file EXTRACT (lossy)."""
    blob = Path(input_path).read_bytes()
    eng = Engine.default()
    raw_pairs = unpack_v6_mbn_raw(eng, blob, allow_extract=True)

    meta = {}
    nums: list[int] = []

    for stype, raw in raw_pairs:
        if stype == ST_META:
            try:
                meta = json.loads(raw.decode("utf-8"))
            except Exception:
                meta = {"meta_raw_utf8": raw.decode("utf-8", errors="replace")}
        elif stype == ST_NUMS:
            nums = decode_ints(raw)

    print("=== EXTRACT-SHOW ===")
    print(json.dumps({"meta": meta, "nums": nums[:200], "nums_total": len(nums)}, ensure_ascii=False, indent=2))


def huffman_encode_stream(stream: SymbolStream) -> EncodedStream:
    if stream.kind == "bytes":
        freq, lastbits, bitstream = huffman_compress_core(stream.data)  # type: ignore[arg-type]
        return EncodedStream(
            name=stream.name,
            kind="bytes",
            alphabet_size=256,
            n=stream.n,
            encoding="huffman",
            freq_used=_freq_to_used(freq),
            lastbits=lastbits,
            bitstream=bitstream,
        )

    if stream.kind == "ids":
        ids: List[int] = stream.data  # type: ignore[assignment]
        vocab_size = stream.alphabet_size
        freq, lastbits, bitstream = huffman_compress_ids(ids, vocab_size)
        return EncodedStream(
            name=stream.name,
            kind="ids",
            alphabet_size=vocab_size,
            n=stream.n,
            encoding="huffman",
            freq_used=_freq_to_used(freq),
            lastbits=lastbits,
            bitstream=bitstream,
        )

    raise NotImplementedError("kind non supportato")

def huffman_decode_stream(enc: EncodedStream) -> SymbolStream:
    if enc.encoding == "raw":
        if enc.raw is None:
            raise ValueError("EncodedStream raw senza raw bytes")
        if enc.kind != "bytes":
            raise ValueError("raw supportato solo per bytes (per ora)")
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(enc.raw), data=enc.raw)

    # huffman
    if enc.freq_used is None or enc.lastbits is None or enc.bitstream is None:
        raise ValueError("EncodedStream huffman incompleto")

    freq = _used_to_freq(enc.freq_used, enc.alphabet_size)

    if enc.kind == "bytes":
        data = huffman_decompress_core(freq, enc.bitstream, enc.n, enc.lastbits)
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(data), data=data)

    if enc.kind == "ids":
        ids = huffman_decompress_ids(freq, enc.n, enc.lastbits, enc.bitstream)
        return SymbolStream(name=enc.name, kind="ids", alphabet_size=enc.alphabet_size, n=len(ids), data=ids)

    raise NotImplementedError("kind non supportato")

def pack_encoded_stream(enc: EncodedStream) -> bytes:
    name_b = enc.name.encode("utf-8")
    if len(name_b) > 0xFF:
        raise ValueError("stream name troppo lungo (max 255)")

    out = bytearray()
    out.append(0 if enc.encoding == "raw" else 1)          # enc
    out.append(0 if enc.kind == "bytes" else 1)            # kind
    out.append(len(name_b))
    out += name_b
    out += enc.alphabet_size.to_bytes(4, "big")
    out += enc.n.to_bytes(4, "big")

    if enc.encoding == "raw":
        raw = enc.raw or b""
        out += len(raw).to_bytes(4, "big")
        out += raw
        return bytes(out)

    used = enc.freq_used or []
    out += len(used).to_bytes(4, "big")
    for sym, f in used:
        out += sym.to_bytes(4, "big")
        out += f.to_bytes(4, "big")
    out.append(int(enc.lastbits or 0) & 0xFF)
    bs = enc.bitstream or b""
    out += len(bs).to_bytes(4, "big")
    out += bs
    return bytes(out)

def unpack_encoded_stream(blob: bytes, idx: int) -> tuple[EncodedStream, int]:
    if idx + 1 + 1 + 1 + 4 + 4 > len(blob):
        raise ValueError("bundle troncato (header stream)")

    enc_flag = blob[idx]; idx += 1
    kind_flag = blob[idx]; idx += 1
    name_len = blob[idx]; idx += 1

    if idx + name_len > len(blob):
        raise ValueError("bundle troncato (name)")
    name = blob[idx:idx+name_len].decode("utf-8")
    idx += name_len

    alphabet_size = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    n = int.from_bytes(blob[idx:idx+4], "big"); idx += 4

    encoding = "raw" if enc_flag == 0 else "huffman"
    kind = "bytes" if kind_flag == 0 else "ids"

    if encoding == "raw":
        raw_len = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        if idx + raw_len > len(blob):
            raise ValueError("bundle troncato (raw)")
        raw = blob[idx:idx+raw_len]; idx += raw_len
        return EncodedStream(name=name, kind=kind, alphabet_size=alphabet_size, n=n, encoding="raw", raw=raw), idx

    num_used = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    used: List[tuple[int, int]] = []
    for _ in range(num_used):
        if idx + 8 > len(blob):
            raise ValueError("bundle troncato (freq entries)")
        sym = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        f = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        used.append((sym, f))

    if idx >= len(blob):
        raise ValueError("bundle troncato (lastbits)")
    lastbits = blob[idx]; idx += 1

    bs_len = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    if idx + bs_len > len(blob):
        raise ValueError("bundle troncato (bitstream)")
    bitstream = blob[idx:idx+bs_len]; idx += bs_len

    return EncodedStream(
        name=name,
        kind=kind,
        alphabet_size=alphabet_size,
        n=n,
        encoding="huffman",
        freq_used=used,
        lastbits=lastbits,
        bitstream=bitstream,
    ), idx

def pack_huffman_bundle(encoded_streams: List[EncodedStream]) -> bytes:
    if len(encoded_streams) > 0xFF:
        raise ValueError("troppi stream (max 255)")
    out = bytearray()
    out += BUNDLE_MAGIC
    out.append(len(encoded_streams))
    for s in encoded_streams:
        sb = pack_encoded_stream(s)
        out += len(sb).to_bytes(4, "big")
        out += sb
    return bytes(out)

def unpack_huffman_bundle(payload: bytes) -> List[EncodedStream]:
    if len(payload) < 5 or payload[:4] != BUNDLE_MAGIC:
        raise ValueError("payload non è un Huffman bundle")
    idx = 4
    n_streams = payload[idx]; idx += 1
    streams: List[EncodedStream] = []
    for _ in range(n_streams):
        if idx + 4 > len(payload):
            raise ValueError("bundle troncato (len)")
        L = int.from_bytes(payload[idx:idx+4], "big"); idx += 4
        if idx + L > len(payload):
            raise ValueError("bundle troncato (stream blob)")
        s_blob = payload[idx:idx+L]; idx += L
        s, _ = unpack_encoded_stream(s_blob, 0)
        streams.append(s)
    return streams

# -------------------
# Statistiche
# -------------------
def print_stats(original_path: str | Path, compressed_path: str | Path, label: str) -> None:
    original_path = Path(original_path)
    compressed_path = Path(compressed_path)

    size_orig = original_path.stat().st_size
    size_comp = compressed_path.stat().st_size

    print(f"=== GCC Huffman stats ({label}) ===")
    print(f"File originale : {original_path} ({size_orig} byte)")
    print(f"File compresso : {compressed_path} ({size_comp} byte)")

    if size_orig == 0:
        print("File originale vuoto: niente statistiche sensate 🙂")
        print("===============================")
        return

    ratio = size_comp / size_orig
    bps = (size_comp * 8) / size_orig

    print(f"Rapporto       : {ratio:.3f} (1.0 = nessuna compressione)")
    print(f"Bit/simbolo    : {bps:.3f} (8.0 = non compresso)")
    print("===============================")

# -------------------
# CLI
# -------------------
def main(argv: List[str]) -> int:
    if len(argv) < 2 or argv[1] not in (
        "c1","d1","c2","d2","c3","d3","c4","d4","c5","d5","c6","d6","c7","d7","extract","extract-show"
    ):
        print(f"Uso:")
        print(f"  {argv[0]} c1 input.txt output.gcc1   (compress Step1)")
        print(f"  {argv[0]} d1 input.gcc1 output.txt   (decompress Step1)")
        print(f"  {argv[0]} c2 input.txt output.gcc2   (compress Step2 V/C/O)")
        print(f"  {argv[0]} d2 input.gcc2 output.txt   (decompress Step2)")
        print(f"  {argv[0]} c3 input.txt output.gcc3   (compress Step3 sillabe)")
        print(f"  {argv[0]} d3 input.gcc3 output.txt   (decompress Step3)")
        print(f"  {argv[0]} c4 input.txt output.gcc4   (compress Step4 parole)")
        print(f"  {argv[0]} d4 input.gcc4 output.txt   (decompress Step4)")
        print(f"  {argv[0]} c5 input.txt output.gcc5 [layer_id_csv] [codec_id_csv]   (compress Container v5)")
        print(f"  {argv[0]} d5 input.gcc5 output.txt                         (decompress Container v5)")
        print(f"  {argv[0]} c6 input.txt output.gcc6 [layer_id_csv] [codec_id_csv]   (compress Container v6)")
        print(f"  {argv[0]} d6 input.gcc6 output.txt                         (decompress Container v6)")
        print(f"  {argv[0]} c7 input.txt output.gcc6 [layer_id] [codec_id] [stream_codecs] (compress v6+MBN multi-stream)")
        print(f"    stream_codecs: es. MAIN:zstd_tight,MASK:zstd_tight,VOWELS:zstd_tight,CONS:zstd_tight")
        print(f"  {argv[0]} d7 input.gccX output.txt                         (decompress universale v1..v6+MBN)")
        print(f"  {argv[0]} extract input.any output.gcc6                    (lossy: estrae solo numeri)")
        print(f"  {argv[0]} extract-show input.gcc6                          (mostra un EXTRACT)")
        print(f"  layer_id_csv: bytes | syllables_it | words_it | ... (oppure CSV: es. bytes,words_it -> prova e sceglie il migliore)")
        print(f"  codec_id_csv: huffman | zstd (oppure CSV: es. huffman,zstd -> prova e sceglie il migliore)")
        return 1

    mode = argv[1]

    # comandi senza output file
    if mode == "extract-show":
        if len(argv) < 3:
            raise ValueError("extract-show: manca input")
        extract_show(argv[2])
        return 0

    if len(argv) < 4:
        raise ValueError("mancano argomenti: input/output")

    inp = argv[2]
    out = argv[3]

    if mode == "c1":
        compress_file_v1(inp, out)
        print_stats(inp, out, "Step1")
    elif mode == "d1":
        decompress_file_v1(inp, out)
        print(f"Decompressione Step1 completata: {out}")
    elif mode == "c2":
        compress_file_v2(inp, out)
        print_stats(inp, out, "Step2")
    elif mode == "d2":
        decompress_file_v2(inp, out)
        print(f"Decompressione Step2 completata: {out}")
    elif mode == "c3":
        compress_file_v3(inp, out)
        print_stats(inp, out, "Step3 (sillabe)")
    elif mode == "d3":
        decompress_file_v3(inp, out)
        print(f"Decompressione Step3 completata: {out}")
    elif mode == "c4":
        compress_file_v4(inp, out)
        print_stats(inp, out, "Step4 (parole)")
    elif mode == "d4":
        decompress_file_v4(inp, out)
        print(f"Decompressione Step4 completata: {out}")
    elif mode == "c5":
        layer_id = argv[4] if len(argv) >= 5 else "bytes"
        codec_id = argv[5] if len(argv) >= 6 else "huffman"
        compress_file_v5(inp, out, layer_id=layer_id, codec_id=codec_id)
    elif mode == "d5":
        decompress_file_v5(inp, out)
    elif mode == "c6":
        layer_id = argv[4] if len(argv) >= 5 else "bytes"
        codec_id = argv[5] if len(argv) >= 6 else "huffman"
        compress_file_v6(inp, out, layer_id=layer_id, codec_id=codec_id)
    elif mode == "d6":
        decompress_file_v6(inp, out)
    elif mode == "c7":
        layer_id = argv[4] if len(argv) >= 5 else "bytes"
        codec_id = argv[5] if len(argv) >= 6 else "zstd_tight"
        stream_codecs = argv[6] if len(argv) >= 7 else None
        compress_file_v7(inp, out, layer_id=layer_id, codec_id=codec_id, stream_codecs_spec=stream_codecs)
    elif mode == "d7":
        decompress_file_v7(inp, out)
    elif mode == "extract":
        extract_numbers_only(inp, out)

    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
