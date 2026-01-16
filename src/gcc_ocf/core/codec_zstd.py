from __future__ import annotations

from dataclasses import dataclass

try:
    import zstandard as zstd  # type: ignore
except Exception:  # pragma: no cover
    zstd = None


@dataclass
class CodecZstd:
    """
    Codec byte-compressor pluggabile.
    Nota: lavora su bytes (non Huffman symbols).

    "tight" prova a minimizzare l'overhead del frame zstd:
      - no content size nel frame
      - no checksum
    """

    level: int = 19
    codec_id: str = "zstd"
    tight: bool = False

    def _require(self) -> None:
        if zstd is None:
            raise RuntimeError(
                "Modulo 'zstandard' non disponibile. Installa con: python3 -m pip install zstandard"
            )

    def compress(self, data: bytes) -> bytes:
        self._require()

        if self.tight:
            # Tenta di ridurre il frame overhead (più simile a "raw" minimale)
            c = zstd.ZstdCompressor(
                level=int(self.level),
                write_content_size=False,
                write_checksum=False,
            )
        else:
            c = zstd.ZstdCompressor(level=int(self.level))

        return c.compress(data)

    def decompress(self, data: bytes, out_size: int | None = None) -> bytes:
        self._require()
        d = zstd.ZstdDecompressor()
        if out_size is None:
            return d.decompress(data)
        return d.decompress(data, max_output_size=int(out_size))
