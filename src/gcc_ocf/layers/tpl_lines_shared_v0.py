"""tpl_lines_shared_v0

Lossless layer like ``tpl_lines_v0``, with optional bucket-level shared template dictionary.

Streams (raw, before codecs):
  - TPL: template dictionary
  - IDS: template_id per line (ints)
  - NUMS: numbers per line (ints)

If a shared base dict is configured (via ``set_shared_dict``):
  - TPL contains only *delta* templates not found in base.
  - IDS refers to (base + delta) template space.
  - META includes ``base_n`` and ``base_tag8`` to validate the base dict.

If no shared base dict is configured:
  - Behaves like ``tpl_lines_v0`` (TPL contains the full dictionary, base_n=0).

Resource (archive-only): ``tpl_dict_v0``
  blob = b"TPLD" + ver(u8=1) + fmt(u8) + tok(u8) + rsv(u8=0) + tpl_raw
  tag8 = sha256(blob)[:8]
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from gcc_ocf.core.num_stream import decode_ints, encode_ints
from gcc_ocf.layers.tpl_lines_v0 import LayerTplLinesV0, _pack_templates, _unpack_templates


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
    b = bytes(buf)
    while True:
        if idx >= len(b):
            raise ValueError("varint troncato")
        bb = b[idx]
        idx += 1
        x |= (bb & 0x7F) << shift
        if (bb & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            raise ValueError("varint troppo grande")
    return x, idx


def _tag8(blob: bytes) -> bytes:
    return hashlib.sha256(blob).digest()[:8]


TPLD_MAGIC = b"TPLD"
TPLD_VER = 1


def pack_tpl_dict_v0_resource(
    templates: list[list[bytes]], *, fmt: int = 1, tok: int = 1
) -> tuple[bytes, dict[str, Any]]:
    """Pack bucket-level template dictionary resource.

    Returns (blob, meta).
    """
    tpl_raw = _pack_templates(templates)
    hdr = TPLD_MAGIC + bytes([TPLD_VER & 0xFF, int(fmt) & 0xFF, int(tok) & 0xFF, 0])
    blob = hdr + tpl_raw
    t8 = _tag8(blob)
    meta = {
        "ver": int(TPLD_VER),
        "fmt": int(fmt),
        "tok": int(tok),
        "k": int(len(templates)),
        "tag8_hex": t8.hex(),
    }
    return blob, meta


def unpack_tpl_dict_v0_resource(blob: bytes) -> tuple[list[list[bytes]], dict[str, Any]]:
    b = bytes(blob)
    if len(b) < 8:
        raise ValueError("tpl_dict_v0: blob troppo corto")
    if b[:4] != TPLD_MAGIC:
        raise ValueError("tpl_dict_v0: magic non valido")
    ver = int(b[4])
    if ver != TPLD_VER:
        raise ValueError(f"tpl_dict_v0: ver non supportato: {ver}")
    fmt = int(b[5])
    tok = int(b[6])
    tpl_raw = b[8:]
    templates = _unpack_templates(tpl_raw)
    t8 = _tag8(b)
    meta = {
        "ver": ver,
        "fmt": fmt,
        "tok": tok,
        "k": int(len(templates)),
        "tag8": t8,
        "tag8_hex": t8.hex(),
    }
    return templates, meta


@dataclass
class LayerTplLinesSharedV0:
    id: str = "tpl_lines_shared_v0"

    # Keep in sync with tpl_lines_v0 meta versioning
    FMT_VERSION = 1
    TOK_RULES = 1

    FLAG_EMPTY = 0x01

    def __post_init__(self) -> None:
        self._base_templates: list[list[bytes]] | None = None
        self._base_tag8: bytes | None = None

    def set_shared_dict(self, templates: list[list[bytes]], *, tag8: bytes) -> None:
        self._base_templates = [list(x) for x in templates]
        self._base_tag8 = bytes(tag8)

    def clear_shared_dict(self) -> None:
        self._base_templates = None
        self._base_tag8 = None

    def pack_meta(self, meta: dict[str, Any]) -> bytes:
        fmt = int(meta.get("fmt", self.FMT_VERSION)) & 0xFF
        tok = int(meta.get("tok", self.TOK_RULES)) & 0xFF
        flags = int(meta.get("flags", 0)) & 0xFF
        base_n = int(meta.get("base_n", 0))
        out = bytearray([fmt, tok, flags])
        out += _enc_varint(base_n)
        if base_n > 0:
            tag8 = meta.get("base_tag8")
            if not isinstance(tag8, (bytes, bytearray)) or len(tag8) != 8:
                raise ValueError("tpl_lines_shared_v0: base_tag8 mancante o invalido")
            out += bytes(tag8)
        return bytes(out)

    def unpack_meta(self, meta_bytes: bytes) -> dict[str, Any]:
        b = bytes(meta_bytes)
        if not b:
            return {}
        if len(b) < 3:
            raise ValueError("tpl_lines_shared_v0: meta troppo corta")
        fmt = int(b[0])
        tok = int(b[1])
        flags = int(b[2])
        idx = 3
        base_n, idx = _dec_varint(b, idx)
        out: dict[str, Any] = {"fmt": fmt, "tok": tok, "flags": flags, "base_n": int(base_n)}
        if base_n > 0:
            if idx + 8 > len(b):
                raise ValueError("tpl_lines_shared_v0: meta troncata (tag8)")
            out["base_tag8"] = b[idx : idx + 8]
            idx += 8
        if idx != len(b):
            raise ValueError("tpl_lines_shared_v0: bytes extra in meta")
        if flags & self.FLAG_EMPTY:
            out["empty"] = True
        return out

    def encode(self, data: bytes) -> tuple[tuple[bytes, bytes, bytes], dict[str, Any]]:
        # Reuse tpl_lines_v0 semantic tokenizer and NUMS encoding
        v0 = LayerTplLinesV0()
        (tpl_raw_full, ids_raw_full, nums_raw), meta0 = v0.encode(data)

        meta: dict[str, Any] = {
            "fmt": int(meta0.get("fmt", self.FMT_VERSION)),
            "tok": int(meta0.get("tok", self.TOK_RULES)),
        }

        if meta0.get("empty"):
            # Keep empty encoding self-contained
            meta["flags"] = int(self.FLAG_EMPTY)
            meta["base_n"] = 0
            return (tpl_raw_full, ids_raw_full, nums_raw), meta

        base = self._base_templates
        base_tag8 = self._base_tag8
        if not base or not base_tag8:
            meta["base_n"] = 0
            return (tpl_raw_full, ids_raw_full, nums_raw), meta

        full_templates = _unpack_templates(tpl_raw_full)
        base_index: dict[tuple[bytes, ...], int] = {tuple(t): i for i, t in enumerate(base)}

        # Build delta templates, map full template id -> new global id
        delta: list[list[bytes]] = []
        delta_index: dict[tuple[bytes, ...], int] = {}
        tid_map: dict[int, int] = {}

        for tid, tpl in enumerate(full_templates):
            key = tuple(tpl)
            if key in base_index:
                tid_map[tid] = int(base_index[key])
            else:
                di = delta_index.get(key)
                if di is None:
                    di = len(delta)
                    delta_index[key] = di
                    delta.append(list(tpl))
                tid_map[tid] = int(len(base) + di)

        # Remap IDS
        ids = decode_ints(ids_raw_full)
        ids2 = [int(tid_map.get(int(x), 0)) for x in ids]
        ids_raw = encode_ints(ids2)

        tpl_raw = _pack_templates(delta)  # delta only

        meta["base_n"] = int(len(base))
        meta["base_tag8"] = bytes(base_tag8)
        return (tpl_raw, ids_raw, nums_raw), meta

    def decode(self, symbols: tuple[bytes, bytes, bytes], layer_meta: dict[str, Any]) -> bytes:
        if not (isinstance(symbols, tuple) and len(symbols) == 3):
            raise ValueError("tpl_lines_shared_v0: symbols attesi: (TPL, IDS, NUMS)")
        tpl_raw, ids_raw, nums_raw = symbols

        meta = layer_meta or {}
        fmt = int(meta.get("fmt", self.FMT_VERSION))
        if fmt != 1:
            raise ValueError(f"tpl_lines_shared_v0: fmt non supportato: {fmt}")

        base_n = int(meta.get("base_n", 0) or 0)
        if base_n > 0:
            base = self._base_templates
            tag8 = self._base_tag8
            expected_tag8 = meta.get("base_tag8")
            if not base or not tag8:
                raise ValueError("tpl_lines_shared_v0: base dict richiesta ma non configurata")
            if len(base) != base_n:
                raise ValueError("tpl_lines_shared_v0: base_n mismatch")
            if not isinstance(expected_tag8, (bytes, bytearray)) or bytes(expected_tag8) != bytes(
                tag8
            ):
                raise ValueError("tpl_lines_shared_v0: tag8 mismatch")
            delta = _unpack_templates(tpl_raw)
            templates = list(base) + list(delta)
        else:
            templates = _unpack_templates(tpl_raw)

        ids = decode_ints(ids_raw)
        nums = decode_ints(nums_raw)
        if not nums:
            raise ValueError("tpl_lines_shared_v0: NUMS stream vuoto")

        idx = 0
        n_lines = int(nums[idx])
        idx += 1

        if n_lines != len(ids):
            # allow the special empty-file encoding
            if not (meta.get("empty") and n_lines == 1 and len(ids) == 1):
                raise ValueError("tpl_lines_shared_v0: mismatch n_lines vs IDS")

        out = bytearray()
        for li in range(n_lines):
            if idx >= len(nums):
                raise ValueError("tpl_lines_shared_v0: NUMS troncato")
            n_nums = int(nums[idx])
            idx += 1

            tid = int(ids[li]) if li < len(ids) else 0
            if tid < 0 or tid >= len(templates):
                raise ValueError(f"tpl_lines_shared_v0: template id fuori range: {tid}")
            chunks = templates[tid]
            expected = max(0, len(chunks) - 1)
            if n_nums != expected:
                raise ValueError(
                    f"tpl_lines_shared_v0: n_nums mismatch (got={n_nums} expected={expected})"
                )

            out += chunks[0]
            for ni in range(n_nums):
                if idx + 3 > len(nums):
                    raise ValueError("tpl_lines_shared_v0: NUMS troncato (triple)")
                sign_code = int(nums[idx])
                digits_len = int(nums[idx + 1])
                magnitude = int(nums[idx + 2])
                idx += 3

                if sign_code == LayerTplLinesV0.SIGN_PLUS:
                    out.append(ord("+"))
                elif sign_code == LayerTplLinesV0.SIGN_MINUS:
                    out.append(ord("-"))
                elif sign_code != LayerTplLinesV0.SIGN_NONE:
                    raise ValueError(f"tpl_lines_shared_v0: sign_code invalido: {sign_code}")

                if digits_len < 1:
                    raise ValueError("tpl_lines_shared_v0: digits_len invalido")
                ds = str(magnitude)
                if len(ds) < digits_len:
                    ds = ds.zfill(digits_len)
                out += ds.encode("ascii")
                out += chunks[ni + 1]

        if idx != len(nums):
            raise ValueError("tpl_lines_shared_v0: NUMS stream contiene dati extra")
        return bytes(out)
