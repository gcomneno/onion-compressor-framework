from __future__ import annotations

from typing import Any, Dict, List, Optional

from gcc_ocf.core.bundle import SymbolStream
from gcc_ocf.core.codec_huffman import CodecHuffman
from gcc_ocf.core.codec_zstd import CodecZstd
from gcc_ocf.core.huffman_bundle import (
    BUNDLE_MAGICS,
    pack_huffman_bundle,
    unpack_huffman_bundle,
    huffman_encode_stream,
    huffman_decode_stream,
)
from gcc_ocf.core.zstd_bundle import (
    ZBN1_MAGIC,
    ZBN2_MAGIC,
    pack_zstd_bundle2,
    unpack_zstd_bundle,
    unpack_zstd_bundle2,
)
from gcc_ocf.core.legacy_payloads import (
    KIND_IDS_META_VOCAB,
    KIND_IDS_INLINE_VOCAB,
    unpack_huffman_payload_bytes,
    unpack_huffman_payload_ids,
    unpack_huffman_payload_ids_inline_vocab,
)
from gcc_ocf.core.zstd_raw import ZRAW1_MAGIC, pack_zstd_raw, unpack_zstd_raw


def _symbols_to_streams(layer_id: str, symbols: Any, meta: Dict[str, Any]) -> List[SymbolStream]:
    # bytes
    if isinstance(symbols, (bytes, bytearray)):
        b = bytes(symbols)
        return [SymbolStream(name="main", kind="bytes", alphabet_size=256, n=len(b), data=b)]

    # ids
    if isinstance(symbols, list) and (len(symbols) == 0 or isinstance(symbols[0], int)):
        vocab_list = meta.get("vocab_list")
        if vocab_list is None:
            raise ValueError("symbols=ids ma manca meta['vocab_list']")
        vocab_size = len(vocab_list)
        return [SymbolStream(name="main", kind="ids", alphabet_size=vocab_size, n=len(symbols), data=symbols)]

    # multi-stream bytes (eventuale vc0 o simili)
    if isinstance(symbols, tuple) and all(isinstance(x, (bytes, bytearray)) for x in symbols):
        names = ("mask", "vowels", "cons") if layer_id == "vc0" and len(symbols) == 3 else tuple(
            f"s{i}" for i in range(len(symbols))
        )
        out: List[SymbolStream] = []
        for name, part in zip(names, symbols):
            pb = bytes(part)
            out.append(SymbolStream(name=name, kind="bytes", alphabet_size=256, n=len(pb), data=pb))
        return out

    raise NotImplementedError("symbols non supportati per v5")


def _streams_to_symbols(layer_id: str, streams: List[SymbolStream]) -> Any:
    # ids: stream main (o qualunque ids)
    for s in streams:
        if s.kind == "ids":
            return s.data  # list[int]

    # vc0
    if layer_id == "vc0":
        parts = {s.name: s.data for s in streams}
        return (parts.get("mask", b""), parts.get("vowels", b""), parts.get("cons", b""))

    # default bytes: stream main
    for s in streams:
        if s.name == "main" and s.kind == "bytes":
            return s.data
    for s in streams:
        if s.kind == "bytes":
            return s.data

    raise ValueError("streams vuoti o non riconosciuti")


def _decode_streams_with_optional_meta(layer_id: str, layer: Any, streams: List[SymbolStream]) -> bytes:
    """
    streams: lista di SymbolStream già decodificati (bytes/ids) e potenzialmente uno "__meta__" (bytes).
    """
    meta_bytes = None
    symbol_streams: List[SymbolStream] = []

    for s in streams:
        if s.name == "__meta__":
            if not isinstance(s.data, (bytes, bytearray)):
                raise ValueError("__meta__ stream non è bytes")
            if s.data:
                meta_bytes = bytes(s.data)
        else:
            symbol_streams.append(s)

    if meta_bytes is not None and hasattr(layer, "unpack_meta"):
        layer_meta = layer.unpack_meta(meta_bytes)
    else:
        layer_meta = {}

    symbols = _streams_to_symbols(layer_id, symbol_streams)
    return layer.decode(symbols, layer_meta)


def encode_v5_payload(input_bytes: bytes, layer_id: str, layer: Any, codec: Any) -> bytes:
    """
    raw -> layer.encode -> streams (+__meta__ opzionale) -> codec bundle (Huffman o Zstd)
    """
    ret = layer.encode(input_bytes)

    if isinstance(ret, tuple) and len(ret) == 2:
        symbols, layer_meta = ret
    else:
        symbols, layer_meta = ret, {}

    layer_meta = layer_meta or {}

    streams = _symbols_to_streams(layer_id, symbols, layer_meta)

    # Optional meta stream
    meta_bytes = None
    if layer_meta and hasattr(layer, "pack_meta"):
        mb = layer.pack_meta(layer_meta)
        if mb:
            meta_bytes = bytes(mb)

    if meta_bytes is not None:
        streams.append(SymbolStream(name="__meta__", kind="bytes", alphabet_size=256, n=len(meta_bytes), data=meta_bytes))

    codec_id = getattr(codec, "codec_id", "huffman")

    if codec_id == "huffman":
        huff = codec if isinstance(codec, CodecHuffman) else CodecHuffman()
        enc_streams = [huffman_encode_stream(s, huff) for s in streams]
        return pack_huffman_bundle(enc_streams)

    if codec_id == "zstd":
        zc = codec if isinstance(codec, CodecZstd) else CodecZstd()

        # ZRAW1 fast-path:
        # se abbiamo UN SOLO stream bytes "main" e NON c'è meta, evitiamo il bundle.
        if (
            len(streams) == 1
            and streams[0].name == "main"
            and streams[0].kind == "bytes"
        ):
            return pack_zstd_raw(streams[0].data, zc)  # type: ignore[arg-type]

        return pack_zstd_bundle2(streams, zc)

    raise ValueError(f"codec_id non supportato in v5: {codec_id!r}")


def decode_v5_payload(payload: bytes, container_meta: Dict[str, Any], layer_id: str, layer: Any, codec: Any) -> bytes:
    """
    payload -> bundle (Huffman o Zstd) OR legacy v5 -> layer.decode -> raw bytes
    """
    codec_id = getattr(codec, "codec_id", "huffman")

    # --- ZRAW1 fast-path (bytes+zstd) ---
    if len(payload) >= 5 and payload[:5] == ZRAW1_MAGIC:
        zc = codec if isinstance(codec, CodecZstd) else CodecZstd()
        raw = unpack_zstd_raw(payload, zc)
        return layer.decode(raw, {})

    # --- Bundle payloads ---
    if len(payload) >= 4 and payload[:4] in BUNDLE_MAGICS:
        huff = codec if isinstance(codec, CodecHuffman) else CodecHuffman()
        enc_streams = unpack_huffman_bundle(payload)
        decoded = [huffman_decode_stream(es, huff) for es in enc_streams]
        return _decode_streams_with_optional_meta(layer_id, layer, decoded)

    if len(payload) >= 4 and payload[:4] == ZBN1_MAGIC:
        zc = codec if isinstance(codec, CodecZstd) else CodecZstd()
        decoded = unpack_zstd_bundle(payload, zc)
        return _decode_streams_with_optional_meta(layer_id, layer, decoded)

    if len(payload) >= 4 and payload[:4] == ZBN2_MAGIC:
        zc = codec if isinstance(codec, CodecZstd) else CodecZstd()
        decoded = unpack_zstd_bundle2(payload, zc)
        return _decode_streams_with_optional_meta(layer_id, layer, decoded)

    # --- Legacy v5 payload (fallback) ---
    symbol_kind = container_meta.get("symbol_kind")
    layer_meta = container_meta.get("layer_meta", {}) or {}

    # Legacy bytes
    if symbol_kind == "bytes":
        n = int(container_meta.get("n", 0))
        freq, lastbits, bitstream = unpack_huffman_payload_bytes(payload)
        huff = codec if isinstance(codec, CodecHuffman) else CodecHuffman()
        symbols = huff.decompress_bytes(freq, bitstream, n, lastbits)
        return layer.decode(symbols, layer_meta)

    # Legacy ids
    if symbol_kind == "ids":
        n_symbols = int(container_meta.get("n_symbols", 0))
        kind = payload[0] if payload else None
        huff = codec if isinstance(codec, CodecHuffman) else CodecHuffman()

        if kind == KIND_IDS_INLINE_VOCAB:
            vocab_list, freq, lastbits, bitstream = unpack_huffman_payload_ids_inline_vocab(payload)
            ids = huff.decompress_ids(freq, n_symbols, lastbits, bitstream)
            layer_meta2 = dict(layer_meta)
            layer_meta2["vocab_list"] = vocab_list
            return layer.decode(ids, layer_meta2)

        if kind == KIND_IDS_META_VOCAB:
            vocab_list = layer_meta.get("vocab_list")
            if vocab_list is None:
                raise ValueError("v5 legacy ids: manca vocab_list nel meta")
            vocab_size = len(vocab_list)

            vs, freq, lastbits, bitstream = unpack_huffman_payload_ids(payload)
            if vs != vocab_size:
                raise ValueError("Mismatch vocab_size tra meta e payload (legacy)")

            ids = huff.decompress_ids(freq, n_symbols, lastbits, bitstream)
            return layer.decode(ids, layer_meta)

        raise ValueError(f"payload kind ids non riconosciuto: {kind}")

    raise ValueError("payload v5 non riconosciuto (né bundle né legacy)")
