from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

import base64
import json

from gcc_ocf.layers.bytes import LayerBytes
from gcc_ocf.layers.vc0 import LayerVC0
from gcc_ocf.layers.syllables_it import LayerSyllablesIT
from gcc_ocf.layers.words_it import LayerWordsIT
from gcc_ocf.layers.lines_dict import LayerLinesDict
from gcc_ocf.layers.lines_rle import LayerLinesRLE
from gcc_ocf.layers.split_text_nums import LayerSplitTextNums
from gcc_ocf.layers.tpl_lines_v0 import LayerTplLinesV0
from gcc_ocf.layers.tpl_lines_shared_v0 import LayerTplLinesSharedV0

from gcc_ocf.core.codec_huffman import CodecHuffman
from gcc_ocf.core.codec_zstd import CodecZstd
from gcc_ocf.core.codec_zlib import CodecZlib
from gcc_ocf.core.codec_raw import CodecRaw
from gcc_ocf.core.codec_num_v0 import CodecNumV0
from gcc_ocf.core.codec_num_v1 import CodecNumV1
from gcc_ocf.core.v5_dispatch import encode_v5_payload, decode_v5_payload
from gcc_ocf.core.legacy_payloads import (
    KIND_BYTES,
    KIND_IDS,
    KIND_IDS_META_VOCAB,
    KIND_IDS_INLINE_VOCAB,
    pack_huffman_payload_bytes,
    unpack_huffman_payload_bytes,
    pack_huffman_payload_ids,
    unpack_huffman_payload_ids,
    pack_huffman_payload_ids_inline_vocab,
    unpack_huffman_payload_ids_inline_vocab,
)

MAGIC = b"GCC"
VERSION_CONTAINER_V5 = 5

# -------------------
# Meta encoding (JSON + base64 per bytes)
# -------------------
def _meta_to_jsonable(obj: Any) -> Any:
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, (bytes, bytearray)):
        b = bytes(obj)
        return {"__t": "bytes", "b64": base64.b64encode(b).decode("ascii")}
    if isinstance(obj, list):
        return [_meta_to_jsonable(x) for x in obj]
    if isinstance(obj, tuple):
        return {"__t": "tuple", "items": [_meta_to_jsonable(x) for x in obj]}
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            out[str(k)] = _meta_to_jsonable(v)
        return out
    raise TypeError(f"meta non serializzabile in JSON: {type(obj)}")


def _meta_from_jsonable(obj: Any) -> Any:
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, list):
        return [_meta_from_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        t = obj.get("__t")
        if t == "bytes":
            return base64.b64decode(obj["b64"].encode("ascii"))
        if t == "tuple":
            return tuple(_meta_from_jsonable(x) for x in obj["items"])
        return {k: _meta_from_jsonable(v) for k, v in obj.items()}
    raise TypeError(f"meta JSON inatteso: {type(obj)}")


def encode_meta(meta: Dict[str, Any]) -> bytes:
    jsonable = _meta_to_jsonable(meta)
    return json.dumps(jsonable, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_meta(meta_bytes: bytes) -> Dict[str, Any]:
    if not meta_bytes:
        return {}
    obj = json.loads(meta_bytes.decode("utf-8"))
    meta = _meta_from_jsonable(obj)
    if not isinstance(meta, dict):
        raise ValueError("meta root deve essere un dict")
    return meta


# -------------------
# Container v5
# [MAGIC(3)|VER(1)|LAYERLEN(1)|LAYER|CODECLEN(1)|CODEC|META_LEN(u32)|META|PAYLOAD_LEN(u32)|PAYLOAD]
# -------------------
def pack_container_v5(layer_id: str, codec_id: str, meta: Dict[str, Any], payload: bytes) -> bytes:
    layer_b = layer_id.encode("utf-8")
    codec_b = codec_id.encode("utf-8")
    if len(layer_b) > 0xFF or len(codec_b) > 0xFF:
        raise ValueError("layer_id/codec_id troppo lunghi (max 255 byte UTF-8)")

    meta_b = encode_meta(meta)
    if len(meta_b) > 0xFFFFFFFF or len(payload) > 0xFFFFFFFF:
        raise ValueError("meta/payload troppo grandi (u32 overflow)")

    out = bytearray()
    out += MAGIC
    out.append(VERSION_CONTAINER_V5)
    out.append(len(layer_b))
    out += layer_b
    out.append(len(codec_b))
    out += codec_b
    out += len(meta_b).to_bytes(4, "big")
    out += meta_b
    out += len(payload).to_bytes(4, "big")
    out += payload
    return bytes(out)


def unpack_container_v5(blob: bytes) -> Tuple[str, str, Dict[str, Any], bytes]:
    if len(blob) < 3 + 1 + 1 + 1 + 4 + 4:
        raise ValueError("blob troppo corto per container v5")

    idx = 0
    if blob[idx:idx + 3] != MAGIC:
        raise ValueError("Magic number non valido")
    idx += 3

    ver = blob[idx]
    idx += 1
    if ver != VERSION_CONTAINER_V5:
        raise ValueError(f"Versione container inattesa: {ver}")

    layer_len = blob[idx]
    idx += 1
    layer_id = blob[idx:idx + layer_len].decode("utf-8")
    idx += layer_len

    codec_len = blob[idx]
    idx += 1
    codec_id = blob[idx:idx + codec_len].decode("utf-8")
    idx += codec_len

    meta_len = int.from_bytes(blob[idx:idx + 4], "big")
    idx += 4
    meta_b = blob[idx:idx + meta_len]
    idx += meta_len

    payload_len = int.from_bytes(blob[idx:idx + 4], "big")
    idx += 4
    payload = blob[idx:idx + payload_len]
    idx += payload_len

    meta = decode_meta(meta_b)
    return layer_id, codec_id, meta, payload


# -------------------
# Engine
# -------------------
@dataclass
class Engine:
    layers: Dict[str, Any]
    codecs: Dict[str, Any]

    @classmethod
    def default(cls) -> "Engine":
        layers = {
            "bytes": LayerBytes(),
            "vc0": LayerVC0(),
            "syllables_it": LayerSyllablesIT(),
            "words_it": LayerWordsIT(),
            "lines_dict": LayerLinesDict(),
            "lines_rle": LayerLinesRLE(),
            "split_text_nums": LayerSplitTextNums(),
            "tpl_lines_v0": LayerTplLinesV0(),
            "tpl_lines_shared_v0": LayerTplLinesSharedV0(),
        }

        codecs = {
            "huffman": CodecHuffman(),
            "zstd": CodecZstd(level=19, tight=False),
            "zstd_tight": CodecZstd(level=19, tight=True),
            "zlib": CodecZlib(level=9),
            "raw": CodecRaw(),
            "num_v0": CodecNumV0(),
            "num_v1": CodecNumV1(),
        }

        return cls(layers=layers, codecs=codecs)

    def compress(self, input_bytes: bytes, layer_id: str = "bytes", codec_id: str = "huffman") -> bytes:
        if layer_id not in self.layers:
            raise ValueError(f"Layer non supportato: {layer_id}")
        if codec_id not in self.codecs:
            raise ValueError(f"Codec non supportato: {codec_id}")

        layer = self.layers[layer_id]
        codec = self.codecs[codec_id]

        payload = encode_v5_payload(input_bytes, layer_id=layer_id, layer=layer, codec=codec)

        # Container meta minimale (non dipende più da vocab_list ecc.)
        meta = {"meta_v": 4, "bundle": True}
        return pack_container_v5(layer_id, codec_id, meta, payload)

    def decompress(self, container_blob: bytes) -> bytes:
        layer_id, codec_id, meta, payload = unpack_container_v5(container_blob)

        if layer_id not in self.layers:
            raise ValueError(f"Layer non supportato: {layer_id}")
        if codec_id not in self.codecs:
            raise ValueError(f"Codec non supportato: {codec_id}")

        layer = self.layers[layer_id]
        codec = self.codecs[codec_id]

        return decode_v5_payload(payload, container_meta=meta, layer_id=layer_id, layer=layer, codec=codec)
