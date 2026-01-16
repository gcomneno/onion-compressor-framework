from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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


@dataclass
class LayerLinesRLE:
    """
    Lossless layer:
      - splitlines(keepends=True)
      - dizionario righe uniche (vocab_list: list[bytes])
      - stream principale: bytes con coppie (id,varint)(run,varint)
        cioè RLE sulla sequenza degli id.

    Meta (compresso in __meta__):
      - n_lines (varint)
      - vocab_blob (pack_vocab_list)
    """

    layer_id: str = "lines_rle"

    def encode(self, data: bytes) -> tuple[bytes, dict[str, Any]]:
        lines: list[bytes] = data.splitlines(keepends=True)

        vocab: list[bytes] = []
        index: dict[bytes, int] = {}
        ids: list[int] = []

        for ln in lines:
            j = index.get(ln)
            if j is None:
                j = len(vocab)
                vocab.append(ln)
                index[ln] = j
            ids.append(j)

        # RLE ids -> bytes (coppie id,run)
        out = bytearray()
        if ids:
            cur = ids[0]
            run = 1
            for v in ids[1:]:
                if v == cur:
                    run += 1
                else:
                    out += _enc_varint(cur)
                    out += _enc_varint(run)
                    cur = v
                    run = 1
            out += _enc_varint(cur)
            out += _enc_varint(run)

        meta = {"vocab_list": vocab, "n_lines": len(lines)}
        return bytes(out), meta

    def decode(self, symbols: bytes, layer_meta: dict[str, Any]) -> bytes:
        vocab = layer_meta.get("vocab_list")
        n_lines = layer_meta.get("n_lines")

        if vocab is None or n_lines is None:
            raise ValueError("lines_rle: meta incompleto (serve vocab_list e n_lines)")
        if not isinstance(vocab, list) or (vocab and not isinstance(vocab[0], (bytes, bytearray))):
            raise TypeError("lines_rle: vocab_list deve essere list[bytes]")
        if not isinstance(n_lines, int) or n_lines < 0:
            raise TypeError("lines_rle: n_lines deve essere int >= 0")

        # parse RLE pairs
        ids: list[int] = []
        idx = 0
        b = bytes(symbols)
        while idx < len(b):
            vid, idx = _dec_varint(b, idx)
            run, idx = _dec_varint(b, idx)
            if vid >= len(vocab):
                raise ValueError("lines_rle: id fuori range")
            if run <= 0:
                raise ValueError("lines_rle: run non valido")
            ids.extend([vid] * run)

        if len(ids) != n_lines:
            raise ValueError("lines_rle: n_lines mismatch (file corrotto?)")

        out = bytearray()
        for i in ids:
            out += bytes(vocab[i])
        return bytes(out)

    def pack_meta(self, layer_meta: dict[str, Any]) -> bytes:
        from gcc_ocf.layers.vocab_blob import pack_vocab_list

        vocab = layer_meta.get("vocab_list", [])
        n_lines = int(layer_meta.get("n_lines", 0))

        vocab_b = [bytes(x) for x in vocab]
        blob = pack_vocab_list(vocab_b)

        return _enc_varint(n_lines) + blob

    def unpack_meta(self, meta_bytes: bytes) -> dict[str, Any]:
        from gcc_ocf.layers.vocab_blob import unpack_vocab_list

        n_lines, idx = _dec_varint(meta_bytes, 0)
        vocab_blob = meta_bytes[idx:]
        vocab = unpack_vocab_list(vocab_blob)
        return {"vocab_list": vocab, "n_lines": n_lines}
