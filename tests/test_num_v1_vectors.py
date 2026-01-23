from __future__ import annotations

import pytest

from gcc_ocf.core.codec_num_v1 import CodecNumV1
from gcc_ocf.core.num_stream import decode_ints, encode_ints


def _b(hexstr: str) -> bytes:
    return bytes.fromhex(hexstr)


# Golden vectors (byte-level)
# NOTE: These tests intentionally "pin" the current num_v1 bitstream format.
# If you introduce a new behavior, do it under a new codec_id / versioned meta,
# not by silently changing num_v1 output.
NV1_EMPTY_BLOB_HEX = "4e563100"

# ints_small = [0, 1, -1, 2, -2, 127, -128]
# raw_small (num_stream) = 00 02 01 04 03 fe01 ff01  (len=9)
NV1_SMALL_RAW_BLOB_HEX = "4e5631000002010403fe01ff01"

# ints_big = [100000, 100001, 100002, 100003] * 50  (len=200)
# This must pick MODE_DICT with K=4 (no shared dict configured).
NV1_BIG_DICT_BLOB_HEX = "4e56310104c09a0cc29a0cc49a0cc69a0c" + ("01020304" * 50)

# Same ints_big, but with shared dict configured: must pick MODE_SHARED.
# shared tag8 = sha256(encode_ints(dict_vals))[:8]
NV1_SHARED_TAG8_HEX = "4d3c6bf064ce99a5"
NV1_BIG_SHARED_BLOB_HEX = "4e563102" + NV1_SHARED_TAG8_HEX + ("01020304" * 50)


def test_num_stream_vectors_roundtrip_and_hex() -> None:
    assert encode_ints([]).hex() == ""
    assert encode_ints([0]).hex() == "00"
    assert encode_ints([1]).hex() == "02"
    assert encode_ints([-1]).hex() == "01"
    assert encode_ints([0, 1, -1]).hex() == "000201"
    assert encode_ints([127]).hex() == "fe01"  # zigzag(127)=254
    assert encode_ints([-128]).hex() == "ff01"  # zigzag(-128)=255

    ints = [0, 1, -1, 2, -2, 127, -128, 128, -129, 10_000, -10_000]
    raw = encode_ints(ints)
    assert decode_ints(raw) == ints


def test_num_v1_empty_raw_vector() -> None:
    c = CodecNumV1()
    raw = encode_ints([])
    blob = c.compress(raw)

    assert blob.hex() == NV1_EMPTY_BLOB_HEX
    assert blob[:3] == b"NV1"
    assert blob[3] == c.MODE_RAW
    assert c.decompress(blob, out_size=len(raw)) == raw


def test_num_v1_small_picks_raw_vector() -> None:
    c = CodecNumV1()
    ints_small = [0, 1, -1, 2, -2, 127, -128]
    raw = encode_ints(ints_small)
    assert len(ints_small) < 8  # compress() must early-return RAW

    blob = c.compress(raw)
    assert blob.hex() == NV1_SMALL_RAW_BLOB_HEX
    assert blob[3] == c.MODE_RAW
    assert c.decompress(blob, out_size=len(raw)) == raw


def test_num_v1_big_picks_dict_vector() -> None:
    c = CodecNumV1()
    ints_big = [100_000, 100_001, 100_002, 100_003] * 50
    raw = encode_ints(ints_big)
    blob = c.compress(raw)

    assert blob.hex() == NV1_BIG_DICT_BLOB_HEX
    assert blob[:3] == b"NV1"
    assert blob[3] == c.MODE_DICT
    assert c.decompress(blob, out_size=len(raw)) == raw

    # Deterministic: same input, same output.
    assert c.compress(raw) == blob


def test_num_v1_big_picks_shared_vector() -> None:
    c = CodecNumV1()
    dict_vals = [100_000, 100_001, 100_002, 100_003]
    c.set_shared_dict(dict_vals)

    ints_big = dict_vals * 50
    raw = encode_ints(ints_big)
    blob = c.compress(raw)

    assert blob.hex() == NV1_BIG_SHARED_BLOB_HEX
    assert blob[:3] == b"NV1"
    assert blob[3] == c.MODE_SHARED

    tag8 = blob[4:12]
    assert tag8.hex() == NV1_SHARED_TAG8_HEX
    assert tag8 == CodecNumV1.dict_tag8(dict_vals)

    assert c.decompress(blob, out_size=len(raw)) == raw


def test_num_v1_decompress_error_cases() -> None:
    c = CodecNumV1()

    # bad magic
    with pytest.raises(ValueError, match="magic"):
        c.decompress(b"BAD" + b"\x00" + b"")

    # out_size mismatch
    raw = encode_ints([0, 1, -1, 2, -2, 127, -128])
    blob = c.compress(raw)
    with pytest.raises(ValueError, match="out_size mismatch"):
        c.decompress(blob, out_size=len(raw) + 1)

    # shared dict missing
    blob_shared = _b(NV1_BIG_SHARED_BLOB_HEX)
    with pytest.raises(ValueError, match="shared dict mancante"):
        c.decompress(blob_shared)

    # shared tag mismatch
    c2 = CodecNumV1()
    c2.set_shared_dict([1, 2, 3, 4])  # different tag
    with pytest.raises(ValueError, match="tag mismatch"):
        c2.decompress(blob_shared)
