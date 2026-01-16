from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from gcc_ocf.layers.vocab_blob import pack_vocab_list, unpack_vocab_list


def _is_ascii_letter(b: int) -> bool:
    return (65 <= b <= 90) or (97 <= b <= 122)  # A-Z a-z


_VOWEL_BYTES = {ord(c) for c in "aeiouAEIOU"}


def _is_vowel_byte(b: int) -> bool:
    return b in _VOWEL_BYTES


def _split_word_into_syllables(word: bytes) -> list[bytes]:
    """
    Identico alla logica legacy:
    - accumula caratteri
    - spezza dopo ogni vocale
    """
    syllables: list[bytes] = []
    current = bytearray()
    for b in word:
        current.append(b)
        if _is_vowel_byte(b):
            syllables.append(bytes(current))
            current.clear()
    if current:
        syllables.append(bytes(current))
    return syllables


def _tokenize_syllables_and_other(data: bytes) -> list[bytes]:
    """
    Identico alla logica legacy:
    - sequenze di lettere -> spezzate in pseudo-sillabe
    - sequenze di non-lettere -> blocchi separati
    """
    tokens: list[bytes] = []
    i = 0
    n = len(data)

    while i < n:
        b = data[i]
        if _is_ascii_letter(b):
            start = i
            i += 1
            while i < n and _is_ascii_letter(data[i]):
                i += 1
            word = data[start:i]
            tokens.extend(_split_word_into_syllables(word))
        else:
            start = i
            i += 1
            while i < n and not _is_ascii_letter(data[i]):
                i += 1
            tokens.append(data[start:i])

    return tokens


@dataclass(frozen=True)
class LayerSyllablesIT:
    """
    Layer v3 (pseudo-sillabe IT):
    encode: bytes -> (id_stream, meta{vocab_list})
    decode: id_stream + vocab_list -> bytes
    """

    id: str = "syllables_it"

    def encode(self, data: bytes) -> tuple[list[int], dict[str, Any]]:
        tokens = _tokenize_syllables_and_other(data)

        vocab: dict[bytes, int] = {}
        vocab_list: list[bytes] = []
        id_stream: list[int] = []

        # IMPORTANTISSIMO: ordine "first seen" identico alla versione legacy
        for tok in tokens:
            if tok not in vocab:
                vocab[tok] = len(vocab_list)
                vocab_list.append(tok)
            id_stream.append(vocab[tok])

        return id_stream, {"vocab_list": vocab_list}

    def decode(self, id_stream: Sequence[int], layer_meta: dict[str, Any]) -> bytes:
        vocab_list = layer_meta.get("vocab_list")
        if vocab_list is None:
            raise ValueError("LayerSyllablesIT.decode: manca vocab_list in layer_meta")

        out = bytearray()
        for sid in id_stream:
            if sid < 0 or sid >= len(vocab_list):
                # stesso messaggio legacy
                raise ValueError("ID token fuori range")
            out += vocab_list[sid]
        return bytes(out)

    def pack_meta(self, meta: dict[str, Any]) -> bytes:
        vocab_list = meta.get("vocab_list")
        if vocab_list is None:
            return b""
        return pack_vocab_list(vocab_list)

    def unpack_meta(self, meta_bytes: bytes) -> dict[str, Any]:
        if not meta_bytes:
            return {"vocab_list": []}
        return {"vocab_list": unpack_vocab_list(meta_bytes)}
