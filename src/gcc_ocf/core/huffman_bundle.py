from __future__ import annotations

from gcc_ocf.core.bundle import EncodedStream, SymbolStream
from gcc_ocf.core.codec_huffman import CodecHuffman

# -------------------------------------------------------------------
# Huffman Bundle
#
# V1: magic b"HBN1", uses u32 lengths + (sym,u32 freq) pairs.
# V2: magic b"HBN2", uses varint lengths + (sym-delta,varint freq) pairs.
#
# Decoder supports BOTH. Encoder emits V2.
# -------------------------------------------------------------------

BUNDLE_MAGIC_V1 = b"HBN1"
BUNDLE_MAGIC_V2 = b"HBN2"

# Default magic used by pack_huffman_bundle()
BUNDLE_MAGIC = BUNDLE_MAGIC_V2

# Magics accepted by container detection
BUNDLE_MAGICS = (BUNDLE_MAGIC_V1, BUNDLE_MAGIC_V2)


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


def _dec_varint(buf: bytes, idx: int) -> tuple[int, int]:
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


def _norm_triplet(ret) -> tuple[list[int], int, bytes]:
    """Normalizza output di compress_*: (freq_list, lastbits_int, bitstream_bytes)."""
    if not isinstance(ret, tuple) or len(ret) != 3:
        raise TypeError("compress_* deve ritornare una tupla (freq, lastbits, bitstream)")

    freq = None
    lastbits = None
    bitstream = None

    for x in ret:
        if isinstance(x, list):
            freq = x
        elif isinstance(x, int):
            lastbits = x
        elif isinstance(x, (bytes, bytearray)):
            bitstream = bytes(x)

    if freq is None or lastbits is None or bitstream is None:
        raise TypeError("impossibile normalizzare tripla Huffman (tipi inattesi)")

    return freq, int(lastbits), bitstream


def _freq_to_used(freq: list[int]) -> list[tuple[int, int]]:
    return [(i, f) for i, f in enumerate(freq) if f > 0]


def _used_to_freq(used: list[tuple[int, int]], alphabet_size: int) -> list[int]:
    freq = [0] * alphabet_size
    for sym, f in used:
        if sym < 0 or sym >= alphabet_size:
            raise ValueError("freq_used contiene sym fuori range")
        freq[sym] = f
    return freq


def huffman_encode_stream(stream: SymbolStream, codec: CodecHuffman | None = None) -> EncodedStream:
    if codec is None:
        codec = CodecHuffman()

    if stream.kind == "bytes":
        data_b = stream.data
        if not isinstance(data_b, (bytes, bytearray)):
            raise TypeError("SymbolStream bytes ma data non bytes")
        freq, lastbits, bitstream = _norm_triplet(codec.compress_bytes(bytes(data_b)))
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
        ids = stream.data
        if not isinstance(ids, list):
            raise TypeError("SymbolStream ids ma data non list[int]")
        vocab_size = stream.alphabet_size
        freq, lastbits, bitstream = _norm_triplet(codec.compress_ids(ids, vocab_size))
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

    raise NotImplementedError(f"kind non supportato: {stream.kind}")


def huffman_decode_stream(enc: EncodedStream, codec: CodecHuffman | None = None) -> SymbolStream:
    if codec is None:
        codec = CodecHuffman()

    if enc.encoding == "raw":
        raw = enc.raw or b""
        if enc.kind != "bytes":
            raise ValueError("raw supportato solo per bytes")
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(raw), data=raw)

    if enc.freq_used is None or enc.lastbits is None or enc.bitstream is None:
        raise ValueError("EncodedStream huffman incompleto")

    freq = _used_to_freq(enc.freq_used, enc.alphabet_size)

    if enc.kind == "bytes":
        data = codec.decompress_bytes(freq, enc.bitstream, enc.n, enc.lastbits)
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(data), data=data)

    if enc.kind == "ids":
        ids = codec.decompress_ids(freq, enc.n, enc.lastbits, enc.bitstream)
        return SymbolStream(
            name=enc.name, kind="ids", alphabet_size=enc.alphabet_size, n=len(ids), data=ids
        )

    raise NotImplementedError(f"kind non supportato: {enc.kind}")


# ---------------------------
# Stream packing/unpacking V1
# ---------------------------


def _pack_encoded_stream_v1(enc: EncodedStream) -> bytes:
    name_b = enc.name.encode("utf-8")
    if len(name_b) > 0xFF:
        raise ValueError("stream name troppo lungo (max 255)")

    out = bytearray()
    out.append(0 if enc.encoding == "raw" else 1)  # encoding flag
    out.append(0 if enc.kind == "bytes" else 1)  # kind flag
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


def _unpack_encoded_stream_v1(blob: bytes, idx: int) -> tuple[EncodedStream, int]:
    if idx + 1 + 1 + 1 + 4 + 4 > len(blob):
        raise ValueError("bundle troncato (header stream)")

    enc_flag = blob[idx]
    idx += 1
    kind_flag = blob[idx]
    idx += 1
    name_len = blob[idx]
    idx += 1

    if idx + name_len > len(blob):
        raise ValueError("bundle troncato (name)")
    name = blob[idx : idx + name_len].decode("utf-8")
    idx += name_len

    alphabet_size = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4
    n = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4

    encoding = "raw" if enc_flag == 0 else "huffman"
    kind = "bytes" if kind_flag == 0 else "ids"

    if encoding == "raw":
        raw_len = int.from_bytes(blob[idx : idx + 4], "big")
        idx += 4
        if idx + raw_len > len(blob):
            raise ValueError("bundle troncato (raw)")
        raw = blob[idx : idx + raw_len]
        idx += raw_len
        return EncodedStream(
            name=name, kind=kind, alphabet_size=alphabet_size, n=n, encoding="raw", raw=raw
        ), idx

    num_used = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4
    used: list[tuple[int, int]] = []
    for _ in range(num_used):
        if idx + 8 > len(blob):
            raise ValueError("bundle troncato (freq entries)")
        sym = int.from_bytes(blob[idx : idx + 4], "big")
        idx += 4
        f = int.from_bytes(blob[idx : idx + 4], "big")
        idx += 4
        used.append((sym, f))

    if idx >= len(blob):
        raise ValueError("bundle troncato (lastbits)")
    lastbits = blob[idx]
    idx += 1

    bs_len = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4
    if idx + bs_len > len(blob):
        raise ValueError("bundle troncato (bitstream)")
    bitstream = blob[idx : idx + bs_len]
    idx += bs_len

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


# ---------------------------
# Stream packing/unpacking V2
# ---------------------------


def _pack_encoded_stream_v2(enc: EncodedStream) -> bytes:
    name_b = enc.name.encode("utf-8")
    if len(name_b) > 0xFF:
        raise ValueError("stream name troppo lungo (max 255)")

    out = bytearray()
    out.append(0 if enc.encoding == "raw" else 1)  # encoding flag
    out.append(0 if enc.kind == "bytes" else 1)  # kind flag
    out.append(len(name_b))
    out += name_b
    out += enc.alphabet_size.to_bytes(4, "big")
    out += enc.n.to_bytes(4, "big")

    if enc.encoding == "raw":
        raw = enc.raw or b""
        out += _enc_varint(len(raw))
        out += raw
        return bytes(out)

    used = enc.freq_used or []
    # Store used entries sorted by sym, with delta sym (varint) and varint freq
    used_sorted = sorted(used, key=lambda t: t[0])
    out += _enc_varint(len(used_sorted))

    prev = 0
    first = True
    for sym, f in used_sorted:
        if first:
            delta = sym
            first = False
        else:
            delta = sym - prev
            if delta < 0:
                raise ValueError("used_sorted non monotono")
        prev = sym
        out += _enc_varint(delta)
        out += _enc_varint(f)

    out.append(int(enc.lastbits or 0) & 0xFF)
    bs = enc.bitstream or b""
    out += _enc_varint(len(bs))
    out += bs
    return bytes(out)


def _unpack_encoded_stream_v2(blob: bytes, idx: int) -> tuple[EncodedStream, int]:
    if idx + 1 + 1 + 1 + 4 + 4 > len(blob):
        raise ValueError("bundle troncato (header stream)")

    enc_flag = blob[idx]
    idx += 1
    kind_flag = blob[idx]
    idx += 1
    name_len = blob[idx]
    idx += 1

    if idx + name_len > len(blob):
        raise ValueError("bundle troncato (name)")
    name = blob[idx : idx + name_len].decode("utf-8")
    idx += name_len

    alphabet_size = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4
    n = int.from_bytes(blob[idx : idx + 4], "big")
    idx += 4

    encoding = "raw" if enc_flag == 0 else "huffman"
    kind = "bytes" if kind_flag == 0 else "ids"

    if encoding == "raw":
        raw_len, idx = _dec_varint(blob, idx)
        if idx + raw_len > len(blob):
            raise ValueError("bundle troncato (raw)")
        raw = blob[idx : idx + raw_len]
        idx += raw_len
        return EncodedStream(
            name=name, kind=kind, alphabet_size=alphabet_size, n=n, encoding="raw", raw=raw
        ), idx

    num_used, idx = _dec_varint(blob, idx)
    used: list[tuple[int, int]] = []
    sym = 0
    first = True
    for _ in range(num_used):
        delta, idx = _dec_varint(blob, idx)
        if first:
            sym = delta
            first = False
        else:
            sym = sym + delta
        f, idx = _dec_varint(blob, idx)
        used.append((sym, f))

    if idx >= len(blob):
        raise ValueError("bundle troncato (lastbits)")
    lastbits = blob[idx]
    idx += 1

    bs_len, idx = _dec_varint(blob, idx)
    if idx + bs_len > len(blob):
        raise ValueError("bundle troncato (bitstream)")
    bitstream = blob[idx : idx + bs_len]
    idx += bs_len

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


# ---------------------------
# Bundle packing/unpacking
# ---------------------------


def pack_huffman_bundle(encoded_streams: list[EncodedStream]) -> bytes:
    """Serializza una lista di EncodedStream (multi-stream) in un payload bundle (V2)."""
    if len(encoded_streams) > 0xFF:
        raise ValueError("troppi stream (max 255)")
    out = bytearray()
    out += BUNDLE_MAGIC_V2
    out.append(len(encoded_streams))
    for s in encoded_streams:
        sb = _pack_encoded_stream_v2(s)
        out += _enc_varint(len(sb))
        out += sb
    return bytes(out)


def unpack_huffman_bundle(payload: bytes) -> list[EncodedStream]:
    """Deserializza un payload bundle (V1 o V2) in lista di EncodedStream."""
    if len(payload) < 5:
        raise ValueError("payload troppo corto per bundle")

    magic = payload[:4]
    if magic not in BUNDLE_MAGICS:
        raise ValueError("payload non è un Huffman bundle")

    idx = 4
    n_streams = payload[idx]
    idx += 1
    streams: list[EncodedStream] = []

    if magic == BUNDLE_MAGIC_V1:
        # V1 lengths are u32
        for _ in range(n_streams):
            if idx + 4 > len(payload):
                raise ValueError("bundle V1 troncato (len)")
            L = int.from_bytes(payload[idx : idx + 4], "big")
            idx += 4
            if idx + L > len(payload):
                raise ValueError("bundle V1 troncato (stream blob)")
            s_blob = payload[idx : idx + L]
            idx += L
            s, _ = _unpack_encoded_stream_v1(s_blob, 0)
            streams.append(s)
        return streams

    # V2 lengths are varint
    for _ in range(n_streams):
        L, idx = _dec_varint(payload, idx)
        if idx + L > len(payload):
            raise ValueError("bundle V2 troncato (stream blob)")
        s_blob = payload[idx : idx + L]
        idx += L
        s, _ = _unpack_encoded_stream_v2(s_blob, 0)
        streams.append(s)

    return streams
