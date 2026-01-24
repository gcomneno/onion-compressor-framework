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


# Golden vectors (byte-level)
#
# IMPORTANT: These tests pin the exact MBN wire format produced by pack_mbn().
# Any format change must be versioned (new magic / new schema), not a silent change.
#
# Layout:
#   "MBN" + varint(nstreams) +
#   repeat:
#     stype(u8) + codec(u8) + varint(ulen) + varint(clen) + varint(mlen) + meta + comp
MBN_ONE_MAIN_HEX = "4d424e010003030300616263"
MBN_TWO_STREAMS_HEX = "4d424e020a0605020001020b07040101ffaa"


def test_is_mbn() -> None:
    assert is_mbn(b"") is False
    assert is_mbn(b"MB") is False
    assert is_mbn(b"MBN") is True
    assert is_mbn(b"MBN\x00") is True
    assert is_mbn(b"XYZ") is False


def test_mbn_golden_one_main() -> None:
    streams = [MBNStream(stype=ST_MAIN, codec=3, ulen=3, comp=b"abc", meta=b"")]
    blob = pack_mbn(streams)
    assert blob.hex() == MBN_ONE_MAIN_HEX

    got = unpack_mbn(blob)
    assert got == streams


def test_mbn_golden_two_streams() -> None:
    streams = [
        MBNStream(stype=ST_TEXT, codec=6, ulen=5, comp=b"\x01\x02", meta=b""),
        MBNStream(stype=ST_NUMS, codec=7, ulen=4, comp=b"\xaa", meta=b"\xff"),
    ]
    blob = pack_mbn(streams)
    assert blob.hex() == MBN_TWO_STREAMS_HEX

    got = unpack_mbn(blob)
    assert got == streams


def test_mbn_varint_multibyte_lengths_prefix() -> None:
    # Pin varint encoding for multi-byte values without embedding a huge full hex blob.
    # ulen=300 -> LEB128 0xAC 0x02 ; clen=128 -> 0x80 0x01 ; mlen=0 -> 0x00
    comp = b"\x00" * 128
    streams = [MBNStream(stype=ST_MAIN, codec=3, ulen=300, comp=comp, meta=b"")]
    blob = pack_mbn(streams)

    # Expected header prefix (up to mlen):
    # magic "MBN"
    # nstreams=1
    # stype=0
    # codec=3
    # ulen=ac02
    # clen=8001
    # mlen=00
    assert blob[:3] == MBN_MAGIC
    assert blob.hex().startswith("4d424e010003ac02800100")

    got = unpack_mbn(blob)
    assert got[0].stype == ST_MAIN
    assert got[0].codec == 3
    assert got[0].ulen == 300
    assert got[0].meta == b""
    assert got[0].comp == comp


def test_mbn_error_bad_magic() -> None:
    with pytest.raises(ValueError, match="magic"):
        unpack_mbn(b"XYZ\x01\x00")


def test_mbn_error_truncated_stream_header() -> None:
    # magic + nstreams=1 but no room for stype/codec
    with pytest.raises(ValueError, match="header stream troncato"):
        unpack_mbn(b"MBN\x01")


def test_mbn_error_nstreams_sanity() -> None:
    # nstreams encoded as very large number (here: 10001)
    # varint(10001) = 0x91 0x4E
    with pytest.raises(ValueError, match="nstreams troppo grande"):
        unpack_mbn(_b("4d424e914e"))


def test_mbn_error_truncated_meta_or_comp() -> None:
    # nstreams=1, stype=0 codec=3, ulen=1, clen=2, mlen=0, but only 1 byte comp present.
    blob = _b("4d424e010003010200aa")
    with pytest.raises(ValueError, match="stream troncato"):
        unpack_mbn(blob)
