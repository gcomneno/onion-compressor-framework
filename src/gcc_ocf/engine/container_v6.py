from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gcc_ocf.core.mbn_bundle import (
    MBN_MAGIC,
    ST_CONS,
    ST_IDS,
    ST_MAIN,
    ST_MASK,
    ST_META,
    ST_NUMS,
    ST_TEXT,
    ST_TPL,
    ST_VOWELS,
    MBNStream,
    pack_mbn,
    unpack_mbn,
)
from gcc_ocf.core.v5_dispatch import decode_v5_payload, encode_v5_payload

MAGIC = b"GCC"
VER_V6 = 6

# v6 uses numeric IDs (u8) to avoid string overhead.
# IMPORTANT: keep these mappings stable forever once you start writing v6 files.
LAYER_TO_CODE: dict[str, int] = {
    "bytes": 0,
    "syllables_it": 1,
    "words_it": 2,
    "vc0": 3,
    "lines_dict": 4,
    "lines_rle": 5,
    "split_text_nums": 6,
    "tpl_lines_v0": 7,
    "tpl_lines_shared_v0": 8,
}
CODE_TO_LAYER: dict[int, str] = {v: k for k, v in LAYER_TO_CODE.items()}

CODEC_TO_CODE: dict[str, int] = {
    "huffman": 0,
    "zstd": 1,
    "zstd_tight": 2,
    "raw": 3,
    "mbn": 4,
    "num_v0": 5,
    "zlib": 6,
    "num_v1": 7,
}
CODE_TO_CODEC: dict[int, str] = {v: k for k, v in CODEC_TO_CODE.items()}

# flags
F_HAS_META = 0x01
F_HAS_PAYLOAD_LEN = 0x02
F_KIND_EXTRACT = 0x80  # lossy, decode via extract-show (non via decompress)


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


def _dec_varint(buf: bytes, idx: int) -> tuple[int, int]:
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


def is_container_v6(blob: bytes) -> bool:
    return len(blob) >= 5 and blob[:3] == MAGIC and blob[3] == VER_V6


@dataclass(frozen=True)
class V6Header:
    layer_id: str
    codec_id: str
    is_extract: bool
    meta: bytes
    payload: bytes


def pack_container_v6(
    payload: bytes,
    *,
    layer_id: str,
    codec_id: str,
    meta: bytes = b"",
    is_extract: bool = False,
) -> bytes:
    if layer_id not in LAYER_TO_CODE:
        raise ValueError(f"v6: layer_id non mappato: {layer_id!r}")
    if codec_id not in CODEC_TO_CODE:
        raise ValueError(f"v6: codec_id non mappato: {codec_id!r}")

    flags = F_KIND_EXTRACT if is_extract else 0
    chunks = [
        MAGIC,
        bytes([VER_V6]),
        bytes([flags]),
        bytes([LAYER_TO_CODE[layer_id]]),
        bytes([CODEC_TO_CODE[codec_id]]),
    ]

    # meta: omitted if empty
    if meta:
        flags |= F_HAS_META
        chunks[2] = bytes([flags])
        chunks.append(_enc_varint(len(meta)))
        chunks.append(meta)

    # payload_len: omitted (payload is rest-of-file)
    chunks.append(payload)
    return b"".join(chunks)


def unpack_container_v6(blob: bytes) -> V6Header:
    if len(blob) < 7:
        raise ValueError("v6: blob troppo corto")
    if blob[:3] != MAGIC:
        raise ValueError("v6: magic non valido")
    ver = blob[3]
    if ver != VER_V6:
        raise ValueError(f"v6: version non supportata: {ver}")

    flags = blob[4]
    layer_code = blob[5]
    codec_code = blob[6]

    layer_id = CODE_TO_LAYER.get(layer_code)
    if layer_id is None:
        raise ValueError(f"v6: layer_code sconosciuto: {layer_code}")
    codec_id = CODE_TO_CODEC.get(codec_code)
    if codec_id is None:
        raise ValueError(f"v6: codec_code sconosciuto: {codec_code}")

    idx = 7
    meta = b""
    if flags & F_HAS_META:
        mlen, idx = _dec_varint(blob, idx)
        meta = blob[idx : idx + mlen]
        if len(meta) != mlen:
            raise ValueError("v6: meta troncata")
        idx += mlen

    if flags & F_HAS_PAYLOAD_LEN:
        plen, idx = _dec_varint(blob, idx)
        payload = blob[idx : idx + plen]
        if len(payload) != plen:
            raise ValueError("v6: payload troncato")
    else:
        payload = blob[idx:]

    return V6Header(
        layer_id=layer_id,
        codec_id=codec_id,
        is_extract=bool(flags & F_KIND_EXTRACT),
        meta=meta,
        payload=payload,
    )


def compress_v6(engine: Any, data: bytes, *, layer_id: str, codec_id: str) -> bytes:
    """
    Produce un blob v6: header compatto + payload (bundle/codec).
    Il payload è lo stesso formato bundle già usato (HBN2/ZBN2/ZRAW1), quindi riusiamo il dispatch v5.
    """
    layer = engine.layers[layer_id]
    codec = engine.codecs[codec_id]
    payload = encode_v5_payload(data, layer_id, layer, codec)
    return pack_container_v6(payload, layer_id=layer_id, codec_id=codec_id, meta=b"")


def _layer_to_mbn_raw_streams(
    layer_id: str, layer: Any, data: bytes
) -> tuple[list[tuple[int, bytes]], bytes | None]:
    """Esegue layer.encode e normalizza in una lista di stream raw bytes.

    Per ora supporta SOLO:
      - bytes
      - vc0 (mask/vowels/cons)
      - split_text_nums (text/nums)
      - tpl_lines_v0 (tpl/ids/nums)
      - tpl_lines_shared_v0 (tpl/ids/nums, compatibile MBN)
    """
    ret = layer.encode(data)
    if isinstance(ret, tuple) and len(ret) == 2:
        symbols, layer_meta = ret
    else:
        symbols, layer_meta = ret, {}

    layer_meta = layer_meta or {}
    meta_bytes: bytes | None = None
    if layer_meta and hasattr(layer, "pack_meta"):
        mb = layer.pack_meta(layer_meta)
        if mb:
            meta_bytes = bytes(mb)

    # bytes
    if isinstance(symbols, (bytes, bytearray)):
        return [(ST_MAIN, bytes(symbols))], meta_bytes

    # vc0
    if (
        layer_id == "vc0"
        and isinstance(symbols, tuple)
        and len(symbols) == 3
        and all(isinstance(x, (bytes, bytearray)) for x in symbols)
    ):
        mask, vowels, cons = (bytes(symbols[0]), bytes(symbols[1]), bytes(symbols[2]))
        return [(ST_MASK, mask), (ST_VOWELS, vowels), (ST_CONS, cons)], meta_bytes

    # split_text_nums
    if (
        layer_id == "split_text_nums"
        and isinstance(symbols, tuple)
        and len(symbols) == 2
        and all(isinstance(x, (bytes, bytearray)) for x in symbols)
    ):
        text_b, nums_b = bytes(symbols[0]), bytes(symbols[1])
        return [(ST_TEXT, text_b), (ST_NUMS, nums_b)], meta_bytes

    # tpl_lines_v0 / tpl_lines_shared_v0
    if (
        layer_id in ("tpl_lines_v0", "tpl_lines_shared_v0")
        and isinstance(symbols, tuple)
        and len(symbols) == 3
        and all(isinstance(x, (bytes, bytearray)) for x in symbols)
    ):
        tpl_b, ids_b, nums_b = bytes(symbols[0]), bytes(symbols[1]), bytes(symbols[2])
        return [(ST_TPL, tpl_b), (ST_IDS, ids_b), (ST_NUMS, nums_b)], meta_bytes

    raise NotImplementedError(
        "MBN per ora supporta solo layer bytes/vc0/split_text_nums/tpl_lines_v0/tpl_lines_shared_v0"
    )


def compress_v6_mbn(
    engine: Any,
    data: bytes,
    *,
    layer_id: str,
    codec_id: str,
    stream_codecs: dict[int, str] | None = None,
) -> bytes:
    """Container v6 + payload MBN (multi-stream), per ora solo bytes/vc0.

    - `codec_id` è il default applicato agli stream.
    - `stream_codecs` (opzionale) permette di specificare un codec diverso per tipo stream:
        {ST_MAIN: "zstd_tight", ST_MASK: "zstd", ...}
      Lo stream `ST_META` (se presente) usa sempre `raw`.
    """
    if codec_id not in engine.codecs:
        raise ValueError(f"codec non supportato: {codec_id}")
    if codec_id not in CODEC_TO_CODE:
        raise ValueError(f"v6: codec_id non mappato: {codec_id!r}")

    layer = engine.layers[layer_id]
    raw_streams, meta_bytes = _layer_to_mbn_raw_streams(layer_id, layer, data)

    # Normalizza stream_codecs
    sc: dict[int, str] = {}
    if stream_codecs:
        for k, v in stream_codecs.items():
            sc[int(k)] = str(v)

    records: list[MBNStream] = []

    for stype, raw in raw_streams:
        # codec per-stream, fallback al default
        cid = sc.get(int(stype), codec_id)

        if cid not in engine.codecs:
            raise ValueError(f"MBN: codec non supportato per stype={stype}: {cid!r}")
        if cid not in CODEC_TO_CODE:
            raise ValueError(f"MBN: codec_id non mappato: {cid!r} (stype={stype})")

        codec = engine.codecs[cid]
        comp = codec.compress(raw)
        records.append(
            MBNStream(
                stype=int(stype),
                codec=CODEC_TO_CODE[cid],
                ulen=len(raw),
                comp=comp,
                meta=b"",
            )
        )

    # optional meta stream (codec raw)
    if meta_bytes is not None:
        rawc = engine.codecs.get("raw")
        if rawc is None:
            raise ValueError("engine: manca codec 'raw' per __meta__")
        comp_meta = rawc.compress(meta_bytes)
        records.append(
            MBNStream(
                stype=ST_META,
                codec=CODEC_TO_CODE["raw"],
                ulen=len(meta_bytes),
                comp=comp_meta,
                meta=b"",
            )
        )

    payload = pack_mbn(records)
    # codec_id nel container = "mbn" (dispatch basato sul payload)
    return pack_container_v6(payload, layer_id=layer_id, codec_id="mbn", meta=b"")


def _decode_mbn_payload_to_raw(engine: Any, payload: bytes) -> list[tuple[int, bytes]]:
    streams = unpack_mbn(payload)
    out: list[tuple[int, bytes]] = []
    for s in streams:
        cid = CODE_TO_CODEC.get(int(s.codec))
        if cid is None:
            raise ValueError(f"MBN: codec_code sconosciuto: {s.codec}")
        codec = engine.codecs.get(cid)
        if codec is None:
            raise ValueError(f"engine: codec non registrato: {cid!r}")
        raw = codec.decompress(s.comp, out_size=int(s.ulen))
        out.append((int(s.stype), raw))
    return out


def unpack_v6_mbn_raw(
    engine: Any, blob: bytes, *, allow_extract: bool = True
) -> list[tuple[int, bytes]]:
    """Ritorna gli stream raw (già decompressi) per container v6+MBN.

    Serve a extract-show e diagnostica.
    """
    h = unpack_container_v6(blob)
    if h.is_extract and not allow_extract:
        raise ValueError("Questo file è EXTRACT (lossy). Usa 'extract-show'.")
    if not (len(h.payload) >= 3 and h.payload[:3] == MBN_MAGIC):
        raise ValueError("payload non è MBN")
    return _decode_mbn_payload_to_raw(engine, h.payload)


def decompress_v6(engine: Any, blob: bytes, *, allow_extract: bool = False) -> bytes:
    h = unpack_container_v6(blob)
    if h.is_extract and not allow_extract:
        raise ValueError("Questo file è EXTRACT (lossy). Usa 'extract-show'.")

    layer = engine.layers[h.layer_id]

    # MBN (multi-stream)
    if len(h.payload) >= 3 and h.payload[:3] == MBN_MAGIC:
        raw_pairs = _decode_mbn_payload_to_raw(engine, h.payload)

        meta_bytes = None
        by_type: dict[int, bytes] = {}
        for stype, raw in raw_pairs:
            if stype == ST_META:
                meta_bytes = raw
            else:
                by_type[stype] = raw

        layer_meta = {}
        if meta_bytes is not None and hasattr(layer, "unpack_meta"):
            layer_meta = layer.unpack_meta(meta_bytes) or {}

        if h.layer_id == "vc0":
            symbols = (
                by_type.get(ST_MASK, b""),
                by_type.get(ST_VOWELS, b""),
                by_type.get(ST_CONS, b""),
            )
        elif h.layer_id == "split_text_nums":
            symbols = (
                by_type.get(ST_TEXT, b""),
                by_type.get(ST_NUMS, b""),
            )
        elif h.layer_id in ("tpl_lines_v0", "tpl_lines_shared_v0"):
            symbols = (
                by_type.get(ST_TPL, b""),
                by_type.get(ST_IDS, b""),
                by_type.get(ST_NUMS, b""),
            )
        else:
            symbols = by_type.get(ST_MAIN)
            if symbols is None:
                # fallback: primo stream non-meta
                for k in sorted(by_type.keys()):
                    symbols = by_type[k]
                    break
                if symbols is None:
                    symbols = b""

        return layer.decode(symbols, layer_meta)

    # fallback: payload v5 (HBN2/ZBN2/ZRAW1 ecc.)
    codec = engine.codecs[h.codec_id]
    return decode_v5_payload(h.payload, {}, h.layer_id, layer, codec)
