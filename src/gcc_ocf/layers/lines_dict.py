from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Any


@dataclass
class LayerLinesDict:
    """
    Lossless layer:
      - split in righe preservando i newline (keepends=True)
      - dizionario di righe uniche -> vocab_list (list[bytes])
      - stream principale: ids (list[int]) che indicano la riga nel dizionario

    Meta:
      - vocab_list: list[bytes] (righe bytes, includono '\n' se presente)
    """
    layer_id: str = "lines_dict"

    def encode(self, data: bytes) -> Tuple[List[int], Dict[str, Any]]:
        # splitlines(keepends=True) è la via corretta: preserva esattamente i newline
        # e NON inventa righe extra quando il file termina con '\n'.
        lines: List[bytes] = data.splitlines(keepends=True)

        vocab: List[bytes] = []
        index: Dict[bytes, int] = {}
        ids: List[int] = []

        for ln in lines:
            j = index.get(ln)
            if j is None:
                j = len(vocab)
                vocab.append(ln)
                index[ln] = j
            ids.append(j)

        meta = {"vocab_list": vocab}
        return ids, meta

    def decode(self, symbols: List[int], layer_meta: Dict[str, Any]) -> bytes:
        vocab = layer_meta.get("vocab_list")
        if vocab is None:
            raise ValueError("lines_dict: manca vocab_list nel meta")
        if not isinstance(vocab, list) or (vocab and not isinstance(vocab[0], (bytes, bytearray))):
            raise TypeError("lines_dict: vocab_list deve essere list[bytes]")

        out = bytearray()
        for i in symbols:
            if i < 0 or i >= len(vocab):
                raise ValueError("lines_dict: id fuori range")
            out += bytes(vocab[i])
        return bytes(out)

    def pack_meta(self, layer_meta: Dict[str, Any]) -> bytes:
        from gcc_ocf.layers.vocab_blob import pack_vocab_list
        vocab = layer_meta.get("vocab_list", [])
        if not isinstance(vocab, list):
            raise TypeError("lines_dict: vocab_list deve essere list")
        vocab_b = [bytes(x) for x in vocab]
        return pack_vocab_list(vocab_b)

    def unpack_meta(self, meta_bytes: bytes) -> Dict[str, Any]:
        from gcc_ocf.layers.vocab_blob import unpack_vocab_list
        vocab = unpack_vocab_list(meta_bytes)
        return {"vocab_list": vocab}
