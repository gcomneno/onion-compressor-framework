from __future__ import annotations

from dataclasses import dataclass

MBN_MAGIC = b"MBN"  # 3 bytes


# Stream types (u8). Keep these stable.
ST_MAIN = 0
ST_MASK = 1
ST_VOWELS = 2
ST_CONS = 3
ST_TEXT = 10
ST_NUMS = 11

# Template mining (line-based)
ST_TPL = 20
ST_IDS = 21

ST_META = 250  # "__meta__" stream


def _enc_varint(x: int) -> bytes:
    """Unsigned LEB128."""
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


def _dec_varint(buf: bytes, idx: int) -> tuple[int, int]:
    """Unsigned LEB128 decode."""
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


@dataclass(frozen=True)
class MBNStream:
    stype: int
    codec: int
    ulen: int
    comp: bytes
    meta: bytes = b""


def is_mbn(payload: bytes) -> bool:
    return len(payload) >= 3 and payload[:3] == MBN_MAGIC


def pack_mbn(streams: list[MBNStream]) -> bytes:
    out = bytearray()
    out += MBN_MAGIC
    out += _enc_varint(len(streams))

    for s in streams:
        if not (0 <= s.stype <= 255):
            raise ValueError(f"MBN: stype fuori range u8: {s.stype}")
        if not (0 <= s.codec <= 255):
            raise ValueError(f"MBN: codec fuori range u8: {s.codec}")
        if s.ulen < 0:
            raise ValueError("MBN: ulen negativo")

        out.append(s.stype)
        out.append(s.codec)
        out += _enc_varint(int(s.ulen))
        out += _enc_varint(len(s.comp))
        out += _enc_varint(len(s.meta))
        if s.meta:
            out += s.meta
        out += s.comp

    return bytes(out)


def unpack_mbn(payload: bytes) -> list[MBNStream]:
    if not is_mbn(payload):
        raise ValueError("MBN: magic non valido")

    idx = 3
    n, idx = _dec_varint(payload, idx)
    if n > 10_000:
        raise ValueError("MBN: nstreams troppo grande (sanity check)")

    streams: list[MBNStream] = []
    for _ in range(n):
        if idx + 2 > len(payload):
            raise ValueError("MBN: header stream troncato")
        stype = payload[idx]
        codec = payload[idx + 1]
        idx += 2

        ulen, idx = _dec_varint(payload, idx)
        clen, idx = _dec_varint(payload, idx)
        mlen, idx = _dec_varint(payload, idx)

        if idx + mlen + clen > len(payload):
            raise ValueError("MBN: stream troncato (meta/comp)")

        meta = payload[idx : idx + mlen] if mlen else b""
        idx += mlen
        comp = payload[idx : idx + clen]
        idx += clen

        streams.append(MBNStream(stype=stype, codec=codec, ulen=ulen, comp=comp, meta=meta))

    return streams
