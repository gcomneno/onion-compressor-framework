from __future__ import annotations

from dataclasses import dataclass


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
    while True:
        if idx >= len(buf):
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
    # Python-friendly zigzag
    return (n << 1) if n >= 0 else ((-n << 1) - 1)


def _zigzag_dec(u: int) -> int:
    return (u >> 1) if (u & 1) == 0 else -(u >> 1) - 1


def _decode_ints_from_raw(raw: bytes) -> list[int]:
    # raw = concatenazione di uvarint(zigzag(int))
    out: list[int] = []
    idx = 0
    while idx < len(raw):
        u, idx = _dec_varint(raw, idx)
        out.append(_zigzag_dec(u))
    return out


def _encode_ints_to_raw(ints: list[int]) -> bytes:
    out = bytearray()
    for n in ints:
        out += _enc_varint(_zigzag_enc(int(n)))
    return bytes(out)


@dataclass
class CodecNumV0:
    """
    Codec numerico sperimentale.

    Input/Output: bytes che rappresentano una sequenza di int come
    concatenazione di uvarint(zigzag(int)).
    """

    codec_id: str = "num_v0"

    # modes
    MODE_RAW = 0
    MODE_DELTA = 1

    MAGIC = b"NV0"  # 3B

    def compress(self, data: bytes) -> bytes:
        raw = bytes(data)

        # Tentiamo DELTA solo se conviene.
        ints = _decode_ints_from_raw(raw)
        if len(ints) <= 1:
            return self.MAGIC + bytes([self.MODE_RAW]) + raw

        deltas: list[int] = [ints[0]]
        for i in range(1, len(ints)):
            deltas.append(ints[i] - ints[i - 1])

        raw_delta = _encode_ints_to_raw(deltas)

        if len(raw_delta) + 4 < len(raw) + 4:  # stessa header size
            return self.MAGIC + bytes([self.MODE_DELTA]) + raw_delta

        return self.MAGIC + bytes([self.MODE_RAW]) + raw

    def decompress(self, data: bytes, out_size: int | None = None) -> bytes:
        blob = bytes(data)
        if len(blob) < 4 or blob[:3] != self.MAGIC:
            raise ValueError("num_v0: magic non valido")
        mode = blob[3]
        payload = blob[4:]

        if mode == self.MODE_RAW:
            out = payload
        elif mode == self.MODE_DELTA:
            deltas = _decode_ints_from_raw(payload)
            if not deltas:
                out = b""
            else:
                ints: list[int] = [deltas[0]]
                for i in range(1, len(deltas)):
                    ints.append(ints[-1] + deltas[i])
                out = _encode_ints_to_raw(ints)
        else:
            raise ValueError(f"num_v0: mode sconosciuto: {mode}")

        if out_size is not None and len(out) != int(out_size):
            raise ValueError(f"num_v0: out_size mismatch: got={len(out)} expected={out_size}")

        return out
