from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Sequence, Tuple

class Codec(ABC):
    """
    Interfaccia minima per codec pluggabili.

    NOTA: teniamo due API distinte:
      - bytes (v1) ha semantica lastbits storica
      - ids (v3/v4) usa la semantica attuale del progetto
    """
    codec_id: str

    @abstractmethod
    def compress_bytes(self, data: bytes) -> Tuple[List[int], int, bytes]:
        """Return (freq, lastbits, bitstream)."""
        raise NotImplementedError

    @abstractmethod
    def decompress_bytes(self, freq: List[int], bitstream: bytes, n: int, lastbits: int) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def compress_ids(self, id_stream: Sequence[int], vocab_size: int) -> Tuple[List[int], int, bytes]:
        raise NotImplementedError

    @abstractmethod
    def decompress_ids(self, freq: List[int], n_symbols: int, lastbits: int, bitstream: bytes) -> List[int]:
        raise NotImplementedError
