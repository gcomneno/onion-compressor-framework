from __future__ import annotations

from typing import List, Tuple

# ------------------------------------------------------------
# Vocab blob encoding
#
# v1 (legacy):  [u32 count BE] + repeat (u32 len BE + bytes)
# v2 (VB2\0):   b"VB2\0" + varint(count) + repeat(varint(len)+bytes)
#
# unpack_vocab_list() accetta sia v1 che v2 (auto-detect).
# pack_vocab_list() produce SEMPRE v2.
# ------------------------------------------------------------

MAGIC_VB2 = b"VB2\0"


def _enc_varint(n: int) -> bytes:
    if n < 0:
        raise ValueError("varint: n < 0")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _dec_varint(buf: bytes, idx: int) -> Tuple[int, int]:
    n = 0
    shift = 0
    while True:
        if idx >= len(buf):
            raise ValueError("varint: buffer troncato")
        b = buf[idx]
        idx += 1
        n |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return n, idx
        shift += 7
        if shift > 63:
            raise ValueError("varint: overflow")


def pack_vocab_list(vocab_list: List[bytes]) -> bytes:
    # VB2 format
    out = bytearray()
    out += MAGIC_VB2
    out += _enc_varint(len(vocab_list))
    for tok in vocab_list:
        if not isinstance(tok, (bytes, bytearray)):
            raise TypeError("vocab_list deve contenere bytes")
        tok_b = bytes(tok)
        out += _enc_varint(len(tok_b))
        out += tok_b
    return bytes(out)


def unpack_vocab_list(blob: bytes) -> List[bytes]:
    if not isinstance(blob, (bytes, bytearray)):
        raise TypeError("blob deve essere bytes")

    buf = bytes(blob)

    # v2
    if len(buf) >= 4 and buf[:4] == MAGIC_VB2:
        idx = 4
        n, idx = _dec_varint(buf, idx)
        vocab: List[bytes] = []
        for _ in range(n):
            L, idx = _dec_varint(buf, idx)
            if idx + L > len(buf):
                raise ValueError("vocab VB2 troncato (data)")
            vocab.append(buf[idx:idx+L])
            idx += L
        if idx != len(buf):
            raise ValueError("vocab VB2 con trailing garbage")
        return vocab

    # v1 legacy (u32 BE)
    idx = 0
    if len(buf) < 4:
        raise ValueError("vocab v1 troppo corto")
    n = int.from_bytes(buf[idx:idx+4], "big")
    idx += 4

    vocab: List[bytes] = []
    for _ in range(n):
        if idx + 4 > len(buf):
            raise ValueError("vocab v1 troncato (len)")
        L = int.from_bytes(buf[idx:idx+4], "big")
        idx += 4
        if idx + L > len(buf):
            raise ValueError("vocab v1 troncato (data)")
        vocab.append(buf[idx:idx+L])
        idx += L

    if idx != len(buf):
        raise ValueError("vocab v1 con trailing garbage")
    return vocab
