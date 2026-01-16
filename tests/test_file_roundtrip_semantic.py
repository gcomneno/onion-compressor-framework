from __future__ import annotations

from pathlib import Path


def test_file_roundtrip_split_text_nums_mbn(tmp_path: Path) -> None:
    """Lossless roundtrip for a real multi-stream layer (TEXT/NUMS via MBN)."""
    from gcc_ocf.engine.container_v6 import compress_v6_mbn, decompress_v6
    from gcc_ocf.engine.container import Engine
    from gcc_ocf.core.mbn_bundle import ST_TEXT, ST_NUMS

    data = (
        "FATTURA 1001\n"
        "RIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\n"
        "RIGA ARTICOLO: dado M3 qty=7 prezzo=0.80\n"
        "TOTALE 17.60\n"
    ).encode("utf-8")

    eng = Engine.default()
    blob = compress_v6_mbn(
        eng,
        data,
        layer_id="split_text_nums",
        codec_id="zlib",
        stream_codecs={
            ST_TEXT: "zlib",
            ST_NUMS: "num_v1",
        },
    )

    out = decompress_v6(eng, blob)
    assert out == data


def test_file_roundtrip_tpl_lines_shared_self_contained(tmp_path: Path) -> None:
    """tpl_lines_shared_v0 must be usable even without bucket resources."""
    from gcc_ocf.engine.container_v6 import compress_v6_mbn, decompress_v6
    from gcc_ocf.engine.container import Engine
    from gcc_ocf.core.mbn_bundle import ST_TPL, ST_IDS, ST_NUMS

    # Repetitive, line-structured text with numbers.
    lines = []
    for i in range(25):
        lines.append(f"RIGA ARTICOLO: vite M3 qty={i+1} prezzo=1.20 TOT={(i+1)*1.2:.2f}")
    data = ("FATTURA 2002\n" + "\n".join(lines) + "\n").encode("utf-8")

    eng = Engine.default()
    blob = compress_v6_mbn(
        eng,
        data,
        layer_id="tpl_lines_shared_v0",
        codec_id="zlib",
        stream_codecs={
            ST_TPL: "zlib",
            ST_IDS: "num_v1",
            ST_NUMS: "num_v1",
        },
    )
    out = decompress_v6(eng, blob)
    assert out == data
