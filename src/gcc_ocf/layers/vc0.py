from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Keep in sync con gcc_huffman.py (v2)
_VOWELS = set("aeiouAEIOU")


@dataclass(frozen=True)
class LayerVC0:
    """
    Layer v2: split in 3 stream:
      - mask: 'V','C','O'
      - vowels: solo vocali
      - cons: consonanti + tutto il resto
    symbols = (mask_bytes, vowels_bytes, cons_bytes)
    """

    id: str = "vc0"

    def encode(self, data: bytes) -> tuple[tuple[bytes, bytes, bytes], dict[str, Any]]:
        mask = bytearray()
        vowels = bytearray()
        cons = bytearray()

        for b in data:
            ch = chr(b)
            if ch in _VOWELS:
                mask.append(ord("V"))
                vowels.append(b)
            elif ch.isalpha():
                mask.append(ord("C"))
                cons.append(b)
            else:
                mask.append(ord("O"))
                cons.append(b)

        return (bytes(mask), bytes(vowels), bytes(cons)), {}

    def decode(self, symbols: tuple[bytes, bytes, bytes], layer_meta: dict[str, Any]) -> bytes:
        mask, vowels, cons = symbols

        out = bytearray()
        iv = 0
        ic = 0
        for m in mask:
            if m == ord("V"):
                out.append(vowels[iv])
                iv += 1
            else:
                out.append(cons[ic])
                ic += 1

        return bytes(out)

    def pack_meta(self, meta: dict) -> bytes:
        return b""

    def unpack_meta(self, meta_bytes: bytes) -> dict:
        return {}
