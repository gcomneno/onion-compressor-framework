from __future__ import annotations

from typing import List, Tuple


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


def _zigzag_enc(n: int) -> int:
    return (n << 1) if n >= 0 else ((-n << 1) - 1)


def _zigzag_dec(u: int) -> int:
    return (u >> 1) if (u & 1) == 0 else -(u >> 1) - 1


def encode_ints(ints: List[int]) -> bytes:
    """Encode lista di int come concatenazione di uvarint(zigzag(int))."""
    out = bytearray()
    for n in ints:
        out += _enc_varint(_zigzag_enc(int(n)))
    return bytes(out)


def decode_ints(raw: bytes) -> List[int]:
    """Decode concatenazione uvarint(zigzag(int)) fino a EOF."""
    out: List[int] = []
    idx = 0
    b = bytes(raw)
    while idx < len(b):
        u, idx = _dec_varint(b, idx)
        out.append(_zigzag_dec(u))
    return out
