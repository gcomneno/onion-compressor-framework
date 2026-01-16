from __future__ import annotations

from typing import List, Optional, Tuple

from gcc_ocf.core.bundle import SymbolStream
from gcc_ocf.core.codec_zstd import CodecZstd

ZBN1_MAGIC = b"ZBN1"  # legacy: frame per-stream
ZBN2_MAGIC = b"ZBN2"  # new: single zstd frame for whole bundle


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


def _pack_ids_varint(ids: List[int]) -> bytes:
    out = bytearray()
    for v in ids:
        if v < 0:
            raise ValueError("ids negativi non supportati")
        out += _enc_varint(int(v))
    return bytes(out)


def _unpack_ids_varint(data: bytes, n: int) -> List[int]:
    ids: List[int] = []
    idx = 0
    for _ in range(n):
        v, idx = _dec_varint(data, idx)
        ids.append(v)
    if idx != len(data):
        raise ValueError("ids varint: bytes residui (n mismatch o payload corrotto)")
    return ids


# -------------------------
# ZBN1 (legacy): per-stream compression
# -------------------------
def pack_zstd_bundle(streams: List[SymbolStream], codec: Optional[CodecZstd] = None) -> bytes:
    if codec is None:
        codec = CodecZstd()

    out = bytearray()
    out += ZBN1_MAGIC
    out += _enc_varint(len(streams))

    for s in streams:
        name_b = s.name.encode("utf-8")
        if len(name_b) > 255:
            raise ValueError("stream name troppo lungo (max 255)")
        out.append(len(name_b))
        out += name_b

        if s.kind == "bytes":
            kind = 0
            raw = bytes(s.data)  # type: ignore[arg-type]
            if s.n != len(raw):
                raise ValueError("SymbolStream.n mismatch (bytes)")
            payload = raw
        elif s.kind == "ids":
            kind = 1
            ids = s.data  # type: ignore[assignment]
            if not isinstance(ids, list):
                raise ValueError("ids stream deve avere data=list[int]")
            if s.n != len(ids):
                raise ValueError("SymbolStream.n mismatch (ids)")
            payload = _pack_ids_varint(ids)
        else:
            raise NotImplementedError(f"kind non supportato: {s.kind}")

        out.append(kind)
        out += int(s.alphabet_size).to_bytes(4, "big")
        out += int(s.n).to_bytes(4, "big")

        comp = codec.compress(payload)
        out += _enc_varint(len(comp))
        out += comp

    return bytes(out)


def unpack_zstd_bundle(blob: bytes, codec: Optional[CodecZstd] = None) -> List[SymbolStream]:
    if codec is None:
        codec = CodecZstd()

    if len(blob) < 4 or blob[:4] != ZBN1_MAGIC:
        raise ValueError("ZBN1 magic non valido")

    idx = 4
    n_streams, idx = _dec_varint(blob, idx)

    streams: List[SymbolStream] = []
    for _ in range(n_streams):
        if idx >= len(blob):
            raise ValueError("bundle troncato (name_len)")
        name_len = blob[idx]
        idx += 1
        if idx + name_len > len(blob):
            raise ValueError("bundle troncato (name)")
        name = blob[idx:idx + name_len].decode("utf-8")
        idx += name_len

        if idx >= len(blob):
            raise ValueError("bundle troncato (kind)")
        kind_b = blob[idx]
        idx += 1

        if idx + 4 + 4 > len(blob):
            raise ValueError("bundle troncato (sizes)")
        alphabet_size = int.from_bytes(blob[idx:idx + 4], "big")
        idx += 4
        n = int.from_bytes(blob[idx:idx + 4], "big")
        idx += 4

        comp_len, idx = _dec_varint(blob, idx)
        if idx + comp_len > len(blob):
            raise ValueError("bundle troncato (comp bytes)")
        comp = blob[idx:idx + comp_len]
        idx += comp_len

        payload = codec.decompress(comp)

        if kind_b == 0:
            data = payload
            if len(data) != n:
                raise ValueError("bundle corrotto: n mismatch (bytes)")
            streams.append(SymbolStream(name=name, kind="bytes", alphabet_size=256, n=n, data=data))
        elif kind_b == 1:
            ids = _unpack_ids_varint(payload, n)
            streams.append(SymbolStream(name=name, kind="ids", alphabet_size=alphabet_size, n=n, data=ids))
        else:
            raise ValueError(f"kind byte sconosciuto: {kind_b}")

    return streams


# -------------------------
# ZBN2 (new): single-frame compression for all streams together
# -------------------------
def _pack_inner(streams: List[SymbolStream]) -> bytes:
    inner = bytearray()
    inner += _enc_varint(len(streams))

    for s in streams:
        name_b = s.name.encode("utf-8")
        if len(name_b) > 255:
            raise ValueError("stream name troppo lungo (max 255)")
        inner.append(len(name_b))
        inner += name_b

        if s.kind == "bytes":
            kind = 0
            raw = bytes(s.data)  # type: ignore[arg-type]
            if s.n != len(raw):
                raise ValueError("SymbolStream.n mismatch (bytes)")
            payload = raw
        elif s.kind == "ids":
            kind = 1
            ids = s.data  # type: ignore[assignment]
            if not isinstance(ids, list):
                raise ValueError("ids stream deve avere data=list[int]")
            if s.n != len(ids):
                raise ValueError("SymbolStream.n mismatch (ids)")
            payload = _pack_ids_varint(ids)
        else:
            raise NotImplementedError(f"kind non supportato: {s.kind}")

        inner.append(kind)
        inner += int(s.alphabet_size).to_bytes(4, "big")
        inner += int(s.n).to_bytes(4, "big")
        inner += _enc_varint(len(payload))
        inner += payload

    return bytes(inner)


def _unpack_inner(inner: bytes) -> List[SymbolStream]:
    idx = 0
    n_streams, idx = _dec_varint(inner, idx)

    streams: List[SymbolStream] = []
    for _ in range(n_streams):
        if idx >= len(inner):
            raise ValueError("inner troncato (name_len)")
        name_len = inner[idx]
        idx += 1
        if idx + name_len > len(inner):
            raise ValueError("inner troncato (name)")
        name = inner[idx:idx + name_len].decode("utf-8")
        idx += name_len

        if idx >= len(inner):
            raise ValueError("inner troncato (kind)")
        kind_b = inner[idx]
        idx += 1

        if idx + 4 + 4 > len(inner):
            raise ValueError("inner troncato (sizes)")
        alphabet_size = int.from_bytes(inner[idx:idx + 4], "big")
        idx += 4
        n = int.from_bytes(inner[idx:idx + 4], "big")
        idx += 4

        payload_len, idx = _dec_varint(inner, idx)
        if idx + payload_len > len(inner):
            raise ValueError("inner troncato (payload)")
        payload = inner[idx:idx + payload_len]
        idx += payload_len

        if kind_b == 0:
            if len(payload) != n:
                raise ValueError("inner corrotto: n mismatch (bytes)")
            streams.append(SymbolStream(name=name, kind="bytes", alphabet_size=256, n=n, data=payload))
        elif kind_b == 1:
            ids = _unpack_ids_varint(payload, n)
            streams.append(SymbolStream(name=name, kind="ids", alphabet_size=alphabet_size, n=n, data=ids))
        else:
            raise ValueError(f"kind byte sconosciuto: {kind_b}")

    if idx != len(inner):
        raise ValueError("inner: bytes residui (corruzione o mismatch)")
    return streams


def pack_zstd_bundle2(streams: List[SymbolStream], codec: Optional[CodecZstd] = None) -> bytes:
    if codec is None:
        codec = CodecZstd()
    inner = _pack_inner(streams)
    comp = codec.compress(inner)
    out = bytearray()
    out += ZBN2_MAGIC
    out += _enc_varint(len(inner))
    out += comp
    return bytes(out)


def unpack_zstd_bundle2(blob: bytes, codec: Optional[CodecZstd] = None) -> List[SymbolStream]:
    if codec is None:
        codec = CodecZstd()

    if len(blob) < 4 or blob[:4] != ZBN2_MAGIC:
        raise ValueError("ZBN2 magic non valido")

    idx = 4
    inner_len, idx = _dec_varint(blob, idx)
    comp = blob[idx:]
    inner = codec.decompress(comp)
    if len(inner) != inner_len:
        # strict: se non matcha, file corrotto o codec errato
        raise ValueError("ZBN2: inner_len mismatch (file corrotto?)")
    return _unpack_inner(inner)
