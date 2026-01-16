from __future__ import annotations


def test_resolve_codec_id_falls_back_to_zlib_when_zstd_missing() -> None:
    from gcc_ocf.legacy.gcc_dir import _resolve_codec_id

    assert _resolve_codec_id("zstd_tight", have_zstd=False) == "zlib"
    assert _resolve_codec_id("zstd", have_zstd=False) == "zlib"
    assert _resolve_codec_id("zlib", have_zstd=False) == "zlib"


def test_resolve_codec_id_keeps_zstd_when_available() -> None:
    from gcc_ocf.legacy.gcc_dir import _resolve_codec_id

    assert _resolve_codec_id("zstd_tight", have_zstd=True) == "zstd_tight"
    assert _resolve_codec_id("zstd", have_zstd=True) == "zstd"
