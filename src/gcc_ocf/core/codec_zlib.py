from __future__ import annotations

import zlib


class CodecZlib:
    """zlib/DEFLATE byte codec (no external deps)."""

    def __init__(self, level: int = 9):
        if not (0 <= level <= 9):
            raise ValueError(f"zlib level must be 0..9, got {level}")
        self.level = level

    def compress(self, data: bytes) -> bytes:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")
        return zlib.compress(bytes(data), self.level)

    def decompress(self, comp: bytes, out_size: int | None = None) -> bytes:
        # out_size is ignored for zlib
        if not isinstance(comp, (bytes, bytearray)):
            raise TypeError("comp must be bytes")
        return zlib.decompress(bytes(comp))
