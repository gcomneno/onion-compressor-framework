from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gcc_ocf.core.num_stream import decode_ints, encode_ints


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
    b = bytes(buf)
    while True:
        if idx >= len(b):
            raise ValueError("varint troncato")
        bb = b[idx]
        idx += 1
        x |= (bb & 0x7F) << shift
        if (bb & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            raise ValueError("varint troppo grande")
    return x, idx


def _pack_templates(templates: list[list[bytes]]) -> bytes:
    """TPL stream raw format (v0):

    [n_templates(varint)]
      for each template:
        [n_chunks(varint)]
          for each chunk:
            [len(varint)][chunk bytes]
    """
    out = bytearray()
    out += _enc_varint(len(templates))
    for chunks in templates:
        out += _enc_varint(len(chunks))
        for c in chunks:
            cb = bytes(c)
            out += _enc_varint(len(cb))
            out += cb
    return bytes(out)


def _unpack_templates(raw: bytes) -> list[list[bytes]]:
    b = bytes(raw)
    idx = 0
    n, idx = _dec_varint(b, idx)
    if n > 1_000_000:
        raise ValueError("tpl_lines_v0: troppi template (sanity)")
    out: list[list[bytes]] = []
    for _ in range(n):
        n_chunks, idx = _dec_varint(b, idx)
        if n_chunks < 1 or n_chunks > 1_000_000:
            raise ValueError("tpl_lines_v0: n_chunks invalido")
        chunks: list[bytes] = []
        for _j in range(n_chunks):
            ln, idx = _dec_varint(b, idx)
            if idx + ln > len(b):
                raise ValueError("tpl_lines_v0: chunk troncato")
            chunks.append(b[idx : idx + ln])
            idx += ln
        out.append(chunks)
    if idx != len(b):
        # tolerate trailing garbage? no, keep strict
        raise ValueError("tpl_lines_v0: bytes extra nel TPL stream")
    return out


@dataclass(frozen=True)
class LayerTplLinesV0:
    """Layer sperimentale: line template mining (lossless).

    Output streams (raw, prima dei codec):
      - TPL: dizionario di template (chunks statici per linea)
      - IDS: template_id per linea (come lista di int zigzag-varint)
      - NUMS: numeri per linea (lista di int zigzag-varint) con struttura:
          [n_lines,
           n_nums(line0), (sign,dlen,mag)*,
           n_nums(line1), ...]

    Token numerico (regole semantiche standard):
      - sequenza di cifre ASCII
      - oppure segno unario (+|-) seguito da cifra, solo se a inizio linea
        o dopo whitespace/separatori tipici di valore.

    Nota: separatori come '-' nelle date/range restano nei chunk (TEXT),
    evitando di trasformare "2024-01-01" in numeri negativi.
    """

    id: str = "tpl_lines_v0"

    SIGN_NONE = 0
    SIGN_PLUS = 1
    SIGN_MINUS = 2

    # Meta versioning: evoluzione isolabile del layer
    FMT_VERSION = 1
    TOK_RULES = 1

    def pack_meta(self, meta: dict[str, Any]) -> bytes:
        fmt = int(meta.get("fmt", self.FMT_VERSION)) & 0xFF
        tok = int(meta.get("tok", self.TOK_RULES)) & 0xFF
        return bytes([fmt, tok])

    def unpack_meta(self, meta_bytes: bytes) -> dict[str, Any]:
        b = bytes(meta_bytes)
        if not b:
            return {}
        if len(b) < 2:
            raise ValueError("tpl_lines_v0: meta troppo corta")
        return {"fmt": int(b[0]), "tok": int(b[1])}

    @staticmethod
    def _is_digit(x: int) -> bool:
        return 48 <= x <= 57

    @staticmethod
    def _is_unary_sign(line: bytes, pos: int) -> bool:
        # conservative: unary only in standard "value" contexts
        if pos <= 0:
            return True
        prev = line[pos - 1]
        if prev in (9, 10, 13, 32):
            return True
        if prev in (ord("("), ord("["), ord("{"), ord("<"), ord("="), ord(":"), ord(","), ord(";")):
            return True
        return False

    def _split_line(self, line: bytes) -> tuple[list[bytes], list[tuple[int, int, int]]]:
        """Return (chunks, nums_meta) for a single line.

        chunks length = n_nums + 1.
        nums_meta items = (sign_code, digits_len, magnitude).
        """
        b = bytes(line)
        n = len(b)
        i = 0
        last = 0
        chunks: list[bytes] = []
        nums_meta: list[tuple[int, int, int]] = []

        while i < n:
            c = b[i]
            start = -1
            sign_code = self.SIGN_NONE
            j = i

            if (
                c in (43, 45)
                and (i + 1) < n
                and self._is_digit(b[i + 1])
                and self._is_unary_sign(b, i)
            ):
                start = i
                sign_code = self.SIGN_PLUS if c == 43 else self.SIGN_MINUS
                j = i + 1
            elif self._is_digit(c):
                start = i
                j = i
            else:
                i += 1
                continue

            while j < n and self._is_digit(b[j]):
                j += 1

            token = b[start:j]
            chunks.append(b[last:start])
            last = j

            if token and token[0] in (43, 45):
                digits = token[1:]
            else:
                digits = token

            if not digits:
                i = j
                continue

            digits_len = len(digits)
            magnitude = int(digits.decode("ascii"))
            nums_meta.append((int(sign_code), int(digits_len), int(magnitude)))

            i = j

        chunks.append(b[last:])
        return chunks, nums_meta

    def encode(self, data: bytes) -> tuple[tuple[bytes, bytes, bytes], dict[str, Any]]:
        b = bytes(data)
        lines = b.splitlines(keepends=True)

        # Special case: empty file
        if not lines and b == b"":
            tpl_raw = _pack_templates([[b""]])
            ids_raw = encode_ints([0])  # 1 "line" placeholder
            nums_raw = encode_ints([1, 0])  # n_lines=1, n_nums=0
            return (tpl_raw, ids_raw, nums_raw), {
                "fmt": self.FMT_VERSION,
                "tok": self.TOK_RULES,
                "empty": True,
            }

        templates: list[list[bytes]] = []
        tpl_index: dict[tuple[bytes, ...], int] = {}

        ids: list[int] = []
        nums_ints: list[int] = []
        nums_ints.append(len(lines))

        for line in lines:
            chunks, nums_meta = self._split_line(line)
            key = tuple(chunks)
            tid = tpl_index.get(key)
            if tid is None:
                tid = len(templates)
                tpl_index[key] = tid
                templates.append(chunks)
            ids.append(int(tid))

            nums_ints.append(len(nums_meta))
            for sign_code, digits_len, magnitude in nums_meta:
                nums_ints.extend([int(sign_code), int(digits_len), int(magnitude)])

        tpl_raw = _pack_templates(templates)
        ids_raw = encode_ints(ids)
        nums_raw = encode_ints(nums_ints)

        return (tpl_raw, ids_raw, nums_raw), {"fmt": self.FMT_VERSION, "tok": self.TOK_RULES}

    def decode(self, symbols: tuple[bytes, bytes, bytes], layer_meta: dict[str, Any]) -> bytes:
        if not (isinstance(symbols, tuple) and len(symbols) == 3):
            raise ValueError("tpl_lines_v0: symbols attesi: (TPL, IDS, NUMS)")
        tpl_raw, ids_raw, nums_raw = symbols

        meta = layer_meta or {}
        fmt = int(meta.get("fmt", self.FMT_VERSION))
        if fmt != 1:
            raise ValueError(f"tpl_lines_v0: fmt non supportato: {fmt}")

        templates = _unpack_templates(tpl_raw)
        ids = decode_ints(ids_raw)
        nums = decode_ints(nums_raw)
        if not nums:
            raise ValueError("tpl_lines_v0: NUMS stream vuoto")

        idx = 0
        n_lines = int(nums[idx])
        idx += 1

        if n_lines != len(ids):
            # allow the special empty-file encoding
            if not (meta.get("empty") and n_lines == 1 and len(ids) == 1):
                raise ValueError("tpl_lines_v0: mismatch n_lines vs IDS")

        out = bytearray()
        for li in range(n_lines):
            if idx >= len(nums):
                raise ValueError("tpl_lines_v0: NUMS troncato")
            n_nums = int(nums[idx])
            idx += 1

            tid = int(ids[li]) if li < len(ids) else 0
            if tid < 0 or tid >= len(templates):
                raise ValueError(f"tpl_lines_v0: template id fuori range: {tid}")
            chunks = templates[tid]
            expected = max(0, len(chunks) - 1)
            if n_nums != expected:
                raise ValueError(
                    f"tpl_lines_v0: n_nums mismatch (got={n_nums} expected={expected})"
                )

            out += chunks[0]
            for ni in range(n_nums):
                if idx + 3 > len(nums):
                    raise ValueError("tpl_lines_v0: NUMS troncato (triple)")
                sign_code = int(nums[idx])
                digits_len = int(nums[idx + 1])
                magnitude = int(nums[idx + 2])
                idx += 3

                if sign_code == self.SIGN_PLUS:
                    out.append(ord("+"))
                elif sign_code == self.SIGN_MINUS:
                    out.append(ord("-"))
                elif sign_code != self.SIGN_NONE:
                    raise ValueError(f"tpl_lines_v0: sign_code invalido: {sign_code}")

                if digits_len < 1:
                    raise ValueError("tpl_lines_v0: digits_len invalido")
                ds = str(magnitude)
                if len(ds) < digits_len:
                    ds = ds.zfill(digits_len)
                out += ds.encode("ascii")
                out += chunks[ni + 1]

        if idx != len(nums):
            # strict: no garbage
            raise ValueError("tpl_lines_v0: NUMS stream contiene dati extra")
        return bytes(out)
