from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union, List, Tuple

StreamKind = Literal["bytes", "ids"]
EncodingKind = Literal["raw", "huffman"]

@dataclass(frozen=True)
class SymbolStream:
    name: str
    kind: StreamKind
    alphabet_size: int  # per bytes: 256, per ids: vocab_size
    n: int              # len(bytes) o numero simboli ids
    data: Union[bytes, List[int]]

@dataclass(frozen=True)
class EncodedStream:
    name: str
    kind: StreamKind
    alphabet_size: int
    n: int
    encoding: EncodingKind
    # raw:
    raw: bytes | None = None
    # huffman:
    freq_used: List[Tuple[int, int]] | None = None  # (sym, freq)
    lastbits: int | None = None
    bitstream: bytes | None = None
