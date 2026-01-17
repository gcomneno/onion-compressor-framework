from __future__ import annotations

from pathlib import Path


def _sample_text_bytes() -> bytes:
    # Testo "umano": lettere, numeri, accenti, righe lunghe → esercita un po' tutto.
    # Nota: per v3/v4 (sillabe/parole) è meglio stare su testo sensato.
    long_line = "RIGA_LUNGA=" + ("x" * 5000)
    s = (
        "LeLe TEST\n"
        "FATTURA N. 2\n"
        "RIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\n"
        "TOTALE 12.00\n"
        "caffè già così — ok?\n"
        f"{long_line}\n"
    )
    return s.encode("utf-8")


def _roundtrip_one(src_txt: Path, comp: Path, out_txt: Path) -> None:
    from gcc_ocf.legacy.gcc_huffman import decompress_file_v7

    decompress_file_v7(str(comp), str(out_txt))
    assert out_txt.read_bytes() == src_txt.read_bytes()


def test_d7_universal_decoder_roundtrips_all_versions(tmp_path: Path) -> None:
    """
    Obiettivo:
      - Non dipendere da tests/data (che può essere ignorato/purgato).
      - Generare on-the-fly payload v1..v6 + v6+MBN (c7) via legacy encoder.
      - Verificare che d7 decodifichi tutto a bytes identici all'originale.
    """
    from gcc_ocf.legacy.gcc_huffman import (
        compress_file_v1,
        compress_file_v2,
        compress_file_v3,
        compress_file_v4,
        compress_file_v5,
        compress_file_v6,
        compress_file_v7,
    )

    src = tmp_path / "src.txt"
    src.write_bytes(_sample_text_bytes())

    # v1..v4 (formati "storici")
    comp1 = tmp_path / "a.gcc1"
    comp2 = tmp_path / "a.gcc2"
    comp3 = tmp_path / "a.gcc3"
    comp4 = tmp_path / "a.gcc4"

    compress_file_v1(src, comp1)
    compress_file_v2(src, comp2)
    compress_file_v3(src, comp3)
    compress_file_v4(src, comp4)

    _roundtrip_one(src, comp1, tmp_path / "out_v1.txt")
    _roundtrip_one(src, comp2, tmp_path / "out_v2.txt")
    _roundtrip_one(src, comp3, tmp_path / "out_v3.txt")
    _roundtrip_one(src, comp4, tmp_path / "out_v4.txt")

    # v5/v6 (container engine-based) – scegliamo zlib per non dipendere da zstd.
    comp5 = tmp_path / "a.gcc5"
    comp6 = tmp_path / "a.gcc6"

    compress_file_v5(str(src), str(comp5), layer_id="bytes", codec_id="zlib")
    compress_file_v6(str(src), str(comp6), layer_id="bytes", codec_id="zlib")

    _roundtrip_one(src, comp5, tmp_path / "out_v5.txt")
    _roundtrip_one(src, comp6, tmp_path / "out_v6.txt")

    # c7: v6 + payload MBN (multi-stream) – pipeline vincente text/numbers
    comp7 = tmp_path / "a.gcc7"
    compress_file_v7(
        str(src),
        str(comp7),
        layer_id="split_text_nums",
        codec_id="zlib",
        stream_codecs_spec=None,  # smart default: TEXT:zlib, NUMS:num_v1
    )
    _roundtrip_one(src, comp7, tmp_path / "out_v7.txt")
