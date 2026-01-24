from __future__ import annotations

import hashlib
from dataclasses import dataclass

from gcc_ocf.core.num_stream import decode_ints, encode_ints


def _enc_varint(x: int) -> bytes:
    if x < 0:
        raise ValueError("varint negativo non supportato")
    out = bytearray()
    out_append = out.append
    while True:
        b = x & 0x7F
        x >>= 7
        if x:
            out_append(0x80 | b)
        else:
            out_append(b)
            break
    return bytes(out)


def _dec_varint(buf: bytes, idx: int) -> tuple[int, int]:
    shift = 0
    x = 0
    n = len(buf)
    while True:
        if idx >= n:
            raise ValueError("varint troncato")
        b = buf[idx]
        idx += 1
        x |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            raise ValueError("varint troppo grande")
    return x, idx


def _zigzag_enc(n: int) -> int:
    return (n << 1) if n >= 0 else ((-n << 1) - 1)


def _zigzag_dec(u: int) -> int:
    return (u >> 1) if (u & 1) == 0 else -(u >> 1) - 1


@dataclass
class CodecNumV1:
    """Codec numerico v1 (PTC-inspired): dizionario top-K + escape.

    Input/Output: bytes nel formato num_stream (concatenazione uvarint(zigzag(int))).

    Modalità:
      - RAW: payload = raw input
      - DICT: payload = uvarint(K) + dict(K ints zigzag-varint) + code-stream
      - SHARED: payload = tag8 + code-stream

    Code-stream:
      - uvarint(code)
      - code==0: segue uvarint(zigzag(int)) (escape)
      - code in [1..K]: valore = dict[code-1]

    Idea: sui dataset tipo fatture/log, alcuni valori (0,1,22,30,100, ecc.) e/o
    ripetizioni di importi/codici possono essere molto frequenti.
    """

    codec_id: str = "num_v1"

    MODE_RAW = 0
    MODE_DICT = 1
    MODE_SHARED = 2

    MAGIC = b"NV1"  # 3B

    # optional shared dictionary (bucket-level)
    _shared_vals: list[int] | None = None
    _shared_tag8: bytes | None = None

    @staticmethod
    def dict_tag8(dict_vals: list[int]) -> bytes:
        """Compute a stable 8-byte tag for a dict."""
        raw = encode_ints(list(dict_vals))
        return hashlib.sha256(raw).digest()[:8]

    def set_shared_dict(self, dict_vals: list[int], tag8: bytes | None = None) -> None:
        vals = list(dict_vals)
        if not vals:
            self._shared_vals = None
            self._shared_tag8 = None
            return
        t = bytes(tag8) if tag8 is not None else self.dict_tag8(vals)
        if len(t) != 8:
            raise ValueError("num_v1: shared tag8 deve essere lungo 8")
        self._shared_vals = vals
        self._shared_tag8 = t

    # Candidate K values (kept small: dictionary overhead matters on short streams)
    K_CANDIDATES = (8, 16, 32, 64, 128)

    def _encode_dict(self, ints: list[int], dict_vals: list[int]) -> bytes:
        # payload = K + dict_raw + codes
        K = len(dict_vals)
        if K <= 0:
            raise ValueError("num_v1: K deve essere > 0")

        idx_map: dict[int, int] = {v: i for i, v in enumerate(dict_vals)}
        dict_raw = encode_ints(dict_vals)

        codes = bytearray()
        codes_extend = codes.extend
        enc = _enc_varint
        zz = _zigzag_enc

        for n in ints:
            j = idx_map.get(n)
            if j is not None:
                codes_extend(enc(j + 1))
            else:
                codes_extend(enc(0))
                codes_extend(enc(zz(int(n))))

        # Build in a single pass to avoid quadratic concatenation.
        out = bytearray()
        out.extend(enc(K))
        out.extend(dict_raw)
        out.extend(codes)
        return bytes(out)

    def _encode_codes(self, ints: list[int], dict_vals: list[int]) -> bytes:
        """Encode only the code-stream using the provided dict."""
        idx_map: dict[int, int] = {v: i for i, v in enumerate(dict_vals)}
        codes = bytearray()
        codes_extend = codes.extend
        enc = _enc_varint
        zz = _zigzag_enc

        for n in ints:
            j = idx_map.get(n)
            if j is not None:
                codes_extend(enc(j + 1))
            else:
                codes_extend(enc(0))
                codes_extend(enc(zz(int(n))))
        return bytes(codes)

    def compress(self, data: bytes) -> bytes:
        raw = bytes(data)

        # RAW candidate (always valid)
        best_blob = self.MAGIC + bytes([self.MODE_RAW]) + raw
        best_len = len(best_blob)

        # Decode ints; if stream is tiny, dict overhead usually loses
        ints = decode_ints(raw)
        if len(ints) < 8:
            return best_blob

        # SHARED candidate (bucket-level dict): only if configured
        if self._shared_vals and self._shared_tag8:
            try:
                codes = self._encode_codes(ints, self._shared_vals)
                blob = self.MAGIC + bytes([self.MODE_SHARED]) + self._shared_tag8 + codes
                if len(blob) < best_len:
                    best_blob = blob
                    best_len = len(blob)
            except Exception:
                pass

        # Frequency table
        freq: dict[int, int] = {}
        freq_get = freq.get
        for n in ints:
            freq[n] = freq_get(n, 0) + 1

        # If not enough variety or repetition, dict won't help
        if len(freq) < 4:
            return best_blob

        # Sort by frequency desc, then by absolute value / value for stability
        # (PTC-style: deterministico)
        ordered = sorted(freq.items(), key=lambda kv: (-kv[1], abs(kv[0]), kv[0]))
        unique_vals = [k for k, _ in ordered]

        for K in self.K_CANDIDATES:
            if K >= len(unique_vals):
                dict_vals = unique_vals[:]  # all
            else:
                dict_vals = unique_vals[:K]

            if len(dict_vals) < 4:
                continue

            payload = self._encode_dict(ints, dict_vals)
            blob = self.MAGIC + bytes([self.MODE_DICT]) + payload
            if len(blob) < best_len:
                best_blob = blob
                best_len = len(blob)

        return best_blob

    def decompress(self, data: bytes, out_size: int | None = None) -> bytes:
        blob = bytes(data)
        if len(blob) < 4 or blob[:3] != self.MAGIC:
            raise ValueError("num_v1: magic non valido")
        mode = blob[3]
        payload = blob[4:]

        dec = _dec_varint
        zz = _zigzag_dec

        if mode == self.MODE_RAW:
            out = payload
        elif mode == self.MODE_DICT:
            idx = 0
            K, idx = dec(payload, idx)
            if K <= 0 or K > 1_000_000:
                raise ValueError(f"num_v1: K non valido: {K}")

            # decode K dict ints
            dict_vals: list[int] = []
            dict_append = dict_vals.append
            for _ in range(int(K)):
                u, idx = dec(payload, idx)
                dict_append(zz(u))

            ints: list[int] = []
            ints_append = ints.append
            # parse codes until EOF
            while idx < len(payload):
                code, idx = dec(payload, idx)
                if code == 0:
                    u, idx = dec(payload, idx)
                    ints_append(zz(u))
                else:
                    j = int(code) - 1
                    if j < 0 or j >= len(dict_vals):
                        raise ValueError(f"num_v1: code fuori dizionario: {code}")
                    ints_append(dict_vals[j])

            out = encode_ints(ints)
        elif mode == self.MODE_SHARED:
            if len(payload) < 8:
                raise ValueError("num_v1: SHARED payload troppo corto")
            tag8 = payload[:8]
            codes_payload = payload[8:]
            if self._shared_vals is None or self._shared_tag8 is None:
                raise ValueError("num_v1: shared dict mancante")
            if tag8 != self._shared_tag8:
                raise ValueError("num_v1: shared dict tag mismatch")
            dict_vals = self._shared_vals
            ints: list[int] = []
            ints_append = ints.append
            idx = 0
            while idx < len(codes_payload):
                code, idx = dec(codes_payload, idx)
                if code == 0:
                    u, idx = dec(codes_payload, idx)
                    ints_append(zz(u))
                else:
                    j = int(code) - 1
                    if j < 0 or j >= len(dict_vals):
                        raise ValueError(f"num_v1: code fuori dizionario: {code}")
                    ints_append(dict_vals[j])
            out = encode_ints(ints)
        else:
            raise ValueError(f"num_v1: mode sconosciuto: {mode}")

        if out_size is not None and len(out) != int(out_size):
            raise ValueError(f"num_v1: out_size mismatch: got={len(out)} expected={out_size}")

        return out
