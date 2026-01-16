from __future__ import annotations

from typing import Tuple

from gcc_ocf.core.codec_zstd import CodecZstd

ZRAW1_MAGIC = b"ZRAW1"

def _enc_varint(x: int) -> bytes:
    if x < 0:
        raise ValueError("varint negativo non supportato")
    out = bytearray()
    while True:
        b = x & 0x7F
        x >>= 7
        if x:
            out.append(0x80 | b)
        else:
            out.append(b)
            break
    return bytes(out)

def _dec_varint(buf: bytes, idx: int) -> Tuple[int, int]:
    shift = 0
    x = 0
    while True:
        if idx >= len(buf):
            raise ValueError("varint troncato")
        b = buf[idx]
        idx += 1
        x |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            raise ValueError("varint troppo grande")
    return x, idx

def pack_zstd_raw(data: bytes, codec: CodecZstd) -> bytes:
    """
    Layout:
      ZRAW1_MAGIC + varint(uncompressed_len) + zstd(compressed bytes)
    """
    raw = bytes(data)
    comp = codec.compress(raw)
    return ZRAW1_MAGIC + _enc_varint(len(raw)) + comp

def unpack_zstd_raw(blob: bytes, codec: CodecZstd) -> bytes:
    if len(blob) < 5 or blob[:5] != ZRAW1_MAGIC:
        raise ValueError("ZRAW1 magic non valido")
    n, idx = _dec_varint(blob, 5)
    comp = blob[idx:]
    raw = codec.decompress(comp, out_size=n)
    if len(raw) != n:
        raise ValueError("ZRAW1: uncompressed_len mismatch (file corrotto?)")
    return raw
