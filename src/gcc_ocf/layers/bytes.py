from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LayerBytes:
    """
    Layer v1: identità.
    - symbols: bytes (uguale all'input)
    - layer_meta: vuoto (non serializzato in v1 per compatibilità)
    """

    id: str = "bytes"

    def encode(self, data: bytes) -> tuple[bytes, dict[str, Any]]:
        return data, {}

    def decode(self, symbols: bytes, layer_meta: dict[str, Any]) -> bytes:
        return symbols

    def pack_meta(self, meta: dict) -> bytes:
        return b""

    def unpack_meta(self, meta_bytes: bytes) -> dict:
        return {}
