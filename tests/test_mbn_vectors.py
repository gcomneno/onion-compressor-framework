from __future__ import annotations

import pytest

from gcc_ocf.core.mbn_bundle import (
    MBN_MAGIC,
    ST_MAIN,
    ST_NUMS,
    ST_TEXT,
    MBNStream,
    is_mbn,
    pack_mbn,
    unpack_mbn,
)


def _b(hexstr: str) -> bytes:
    return bytes.fromhex(hexstr)


def test_is_mbn() -> None:
    assert is_mbn(MBN_MAGIC + b"\x00")
    assert not is_mbn(b"")
    assert not is_mbn(b"MB")
    assert not is_mbn(b"XXX")


def test_mbn_golden_vector_single_stream() -> None:
    # 1 stream: TEXT (10), zlib (6), ulen=5, comp=b"abc", meta=b""
    streams = [MBNStream(stype=ST_TEXT, codec=6, ulen=5, comp=b"abc", meta=b"")]
    blob = pack_mbn(streams)

    # magic "MBN" + nstreams(1) + stype + codec + ulen + clen + mlen + comp
    assert blob.hex() == "4d424e010a06050300616263"
    assert unpack_mbn(blob) == streams

    # Determinism: same input list -> same bytes
    assert pack_mbn(streams) == blob


def test_mbn_golden_vector_two_streams_with_meta() -> None:
    # 2 streams, second has per-stream meta.
    streams = [
        MBNStream(stype=ST_MAIN, codec=3, ulen=0, comp=b"", meta=b""),
        MBNStream(stype=ST_NUMS, codec=7, ulen=4, comp=b"\x01\x02", meta=b"\xff"),
    ]
    blob = pack_mbn(streams)

    # magic + nstreams(2)
    # stream0: 00 03 00 00 00
    # stream1: 0b 07 04 02 01 ff 0102
    assert blob.hex() == "4d424e0200030000000b07040201ff0102"
    assert unpack_mbn(blob) == streams

    # Determinism: same input list -> same bytes
    assert pack_mbn(streams) == blob


def test_mbn_pack_validation() -> None:
    with pytest.raises(ValueError, match="stype fuori range"):
        pack_mbn([MBNStream(stype=256, codec=0, ulen=0, comp=b"")])

    with pytest.raises(ValueError, match="codec fuori range"):
        pack_mbn([MBNStream(stype=0, codec=999, ulen=0, comp=b"")])

    with pytest.raises(ValueError, match="ulen negativo"):
        pack_mbn([MBNStream(stype=0, codec=0, ulen=-1, comp=b"")])


def test_mbn_unpack_error_cases() -> None:
    # bad magic
    with pytest.raises(ValueError, match="magic"):
        unpack_mbn(b"BAD\x00\x00\x00")

    # header stream truncated (needs stype+codec)
    with pytest.raises(ValueError, match="header stream troncato"):
        unpack_mbn(_b("4d424e01") + b"\x00")  # nstreams=1, then only 1 byte

    # varint truncated while reading sizes
    with pytest.raises(ValueError, match="varint troncato"):
        # magic + nstreams=1 + stype+codec + start ulen varint that never ends (0x80, no terminator)
        unpack_mbn(_b("4d424e01") + b"\x0a\x06\x80")

    # stream truncated (meta/comp lengths exceed buffer)
    with pytest.raises(ValueError, match="stream troncato"):
        # nstreams=1, stype=0, codec=0, ulen=0, clen=10, mlen=0, but no comp bytes
        unpack_mbn(_b("4d424e01") + b"\x00\x00\x00\x0a\x00")

    # sanity check: nstreams too large (10001)
    with pytest.raises(ValueError, match="nstreams troppo grande"):
        unpack_mbn(_b("4d424e") + b"\x91\x4e")  # 0x4e91 (LEB128) = 10001
