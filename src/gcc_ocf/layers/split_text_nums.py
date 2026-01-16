from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gcc_ocf.core.num_stream import decode_ints, encode_ints


@dataclass(frozen=True)
class LayerSplitTextNums:
    """Layer sperimentale: split lossless TEXT/NUMS.

    Idea:
      - TEXT stream = concatenazione dei "chunk" tra i token numerici
      - NUMS stream = sequenza di int (zigzag+varint) che codifica:
          [n_numbers,
           chunk_len_0..chunk_len_n,
           (sign_code, digits_len, magnitude) * n_numbers]

    Token numerico riconosciuto (byte-level, regole "semantiche" standard):
      - una sequenza di cifre ASCII
      - oppure un segno unario (+|-) seguito da almeno una cifra ASCII, *solo* in contesti tipici
        (inizio file/linea, dopo whitespace o dopo separatori come "(", "[", "{", "=", ":", ",", ";").

    Nota: questo evita di interpretare come "numero negativo" casi comuni come date/range:
      - 2024-01-01  -> tokens: 2024, 01, 01 (il '-' resta nello stream TEXT)
      - 10-12       -> tokens: 10, 12 (il '-' resta nello stream TEXT)

    Lossless anche con zeri iniziali e segno '+' esplicito.
    """

    id: str = "split_text_nums"

    # sign codes (uvarint)
    SIGN_NONE = 0
    SIGN_PLUS = 1
    SIGN_MINUS = 2

    # Meta versioning: permette di evolvere SOLO questo layer (tokenizer/format) senza rompere altro.
    # fmt=1: formato NUMS attuale (chunk lens + triples per numero)
    # tok=1: regole semantiche standard per il segno unario
    FMT_VERSION = 1
    TOK_RULES = 1

    def encode(self, data: bytes) -> tuple[tuple[bytes, bytes], dict[str, Any]]:
        b = bytes(data)

        chunks: list[bytes] = []
        nums_meta: list[tuple[int, int, int]] = []  # (sign_code, digits_len, magnitude)

        i = 0
        last = 0
        n = len(b)

        def is_digit(x: int) -> bool:
            return 48 <= x <= 57

        def is_unary_sign(pos: int) -> bool:
            """True se b[pos] (+/-) è da interpretare come segno unario.

            Regola standard (conservativa): il segno è unario solo se:
              - è a inizio buffer, oppure
              - il byte precedente è whitespace, oppure
              - il byte precedente è un separatore tipico di "valore" (apertura parentesi, '=' ecc.).

            Questo impedisce di trattare '-' come segno in date/range/operatori: "2024-01-01", "10-12", "x-1".
            """
            if pos <= 0:
                return True
            prev = b[pos - 1]
            # whitespace
            if prev in (9, 10, 13, 32):
                return True
            # common "value" separators
            if prev in (
                ord("("),
                ord("["),
                ord("{"),
                ord("<"),
                ord("="),
                ord(":"),
                ord(","),
                ord(";"),
            ):
                return True
            return False

        while i < n:
            c = b[i]

            # Detect start of number token
            start = -1
            sign_code = self.SIGN_NONE
            if c in (43, 45) and (i + 1) < n and is_digit(b[i + 1]) and is_unary_sign(i):
                # '+' or '-' followed by digit, in unary-sign context
                start = i
                sign_code = self.SIGN_PLUS if c == 43 else self.SIGN_MINUS
                j = i + 1
            elif is_digit(c):
                start = i
                j = i
            else:
                i += 1
                continue

            # Consume digits
            while j < n and is_digit(b[j]):
                j += 1

            token = b[start:j]

            # chunk before token
            chunks.append(b[last:start])
            last = j

            # parse digits part
            if token and token[0] in (43, 45):
                digits = token[1:]
            else:
                digits = token

            if not digits:
                # should not happen (we require at least 1 digit)
                i = j
                continue

            digits_len = len(digits)
            # magnitude as int (leading zeros ok)
            magnitude = int(digits.decode("ascii"))
            nums_meta.append((sign_code, digits_len, magnitude))

            i = j

        # tail chunk
        chunks.append(b[last:])

        # Build numeric stream payload as a list of non-negative ints
        n_numbers = len(nums_meta)
        seq: list[int] = [n_numbers]
        seq.extend(len(ch) for ch in chunks)  # n_numbers + 1 entries
        for sign_code, digits_len, magnitude in nums_meta:
            seq.extend([int(sign_code), int(digits_len), int(magnitude)])

        text_stream = b"".join(chunks)
        nums_stream = encode_ints(seq)
        return (text_stream, nums_stream), {"fmt": self.FMT_VERSION, "tok": self.TOK_RULES}

    def decode(self, symbols: tuple[bytes, bytes], layer_meta: dict[str, Any]) -> bytes:
        # Versioning: oggi il decoder supporta fmt 0 (legacy/no-meta) e fmt 1 (attuale).
        fmt = int((layer_meta or {}).get("fmt", 0) or 0)
        if fmt not in (0, self.FMT_VERSION):
            raise ValueError(f"split_text_nums: fmt non supportato: {fmt}")

        text_stream, nums_stream = symbols
        seq = decode_ints(nums_stream)
        if not seq:
            # no numbers, whole payload is text
            return bytes(text_stream)

        n_numbers = int(seq[0])
        if n_numbers < 0:
            raise ValueError("split_text_nums: n_numbers negativo")

        need = 1 + (n_numbers + 1) + (3 * n_numbers)
        if len(seq) < need:
            raise ValueError(
                f"split_text_nums: NUMS stream troppo corto: have={len(seq)} need>={need}"
            )

        # chunk lengths
        chunk_lens = [int(x) for x in seq[1 : 1 + n_numbers + 1]]
        if any(x < 0 for x in chunk_lens):
            raise ValueError("split_text_nums: chunk_len negativo")

        # split text_stream
        ts = bytes(text_stream)
        chunks: list[bytes] = []
        pos = 0
        for ln in chunk_lens:
            ln = int(ln)
            chunks.append(ts[pos : pos + ln])
            pos += ln
        if pos != len(ts):
            raise ValueError(
                f"split_text_nums: chunk_len sum mismatch: sum={pos} text_len={len(ts)}"
            )

        # numbers triples
        idx = 1 + (n_numbers + 1)
        nums: list[bytes] = []
        for _ in range(n_numbers):
            sign_code = int(seq[idx])
            digits_len = int(seq[idx + 1])
            magnitude = int(seq[idx + 2])
            idx += 3

            if digits_len <= 0:
                raise ValueError("split_text_nums: digits_len <= 0")
            if magnitude < 0:
                raise ValueError("split_text_nums: magnitude negativo")

            s = b""
            if sign_code == self.SIGN_PLUS:
                s = b"+"
            elif sign_code == self.SIGN_MINUS:
                s = b"-"
            elif sign_code != self.SIGN_NONE:
                raise ValueError(f"split_text_nums: sign_code sconosciuto: {sign_code}")

            digits = str(magnitude).encode("ascii")
            if len(digits) > digits_len:
                raise ValueError(
                    f"split_text_nums: digits_len troppo piccolo: {digits_len} < {len(digits)}"
                )
            if len(digits) < digits_len:
                digits = b"0" * (digits_len - len(digits)) + digits
            nums.append(s + digits)

        # interleave chunks and numbers
        out = bytearray()
        for i in range(n_numbers):
            out += chunks[i]
            out += nums[i]
        out += chunks[n_numbers]
        return bytes(out)

    def pack_meta(self, meta: dict) -> bytes:
        # Meta compatta: 2 byte (fmt, tok). Se assenti, decoder assume legacy.
        fmt = int(meta.get("fmt", 0) or 0)
        tok = int(meta.get("tok", 0) or 0)
        if fmt <= 0 and tok <= 0:
            return b""
        if not (0 <= fmt <= 255 and 0 <= tok <= 255):
            raise ValueError("split_text_nums: meta fuori range (0..255)")
        return bytes([fmt & 0xFF, tok & 0xFF])

    def unpack_meta(self, meta_bytes: bytes) -> dict:
        if not meta_bytes:
            # legacy/no-meta
            return {"fmt": 0, "tok": 0}
        if len(meta_bytes) < 2:
            raise ValueError("split_text_nums: meta troppo corta")
        fmt = int(meta_bytes[0])
        tok = int(meta_bytes[1])
        return {"fmt": fmt, "tok": tok}
