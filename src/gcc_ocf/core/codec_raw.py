from __future__ import annotations


class CodecRaw:
    """
    Codec identity: utile per stream piccoli (meta) e per debug.
    """

    codec_id: str = "raw"

    def compress(self, data: bytes) -> bytes:
        return bytes(data)

    def decompress(self, data: bytes, out_size: int | None = None) -> bytes:
        b = bytes(data)
        if out_size is not None and len(b) != int(out_size):
            # non è un errore fatale per tutti gli usi, ma qui preferiamo essere severi
            raise ValueError(f"raw: out_size mismatch: got={len(b)} expected={out_size}")
        return b
