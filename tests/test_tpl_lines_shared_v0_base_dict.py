from __future__ import annotations

from pathlib import Path


def _make_fattura_lines(*, n: int = 10) -> bytes:
    lines = ["FATTURA 3003"]
    for i in range(n):
        lines.append(f"RIGA ARTICOLO: vite M3 qty={i+1} prezzo=1.20 TOT={(i+1)*1.2:.2f}")
    lines.append("TOTALE 66.00")
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_tpl_lines_shared_base_dict_produces_delta_and_roundtrips(tmp_path: Path) -> None:
    from gcc_ocf.layers.tpl_lines_shared_v0 import LayerTplLinesSharedV0, pack_tpl_dict_v0_resource, unpack_tpl_dict_v0_resource

    # Build a base dict from a first sample (bucket-level resource simulation)
    base_data = _make_fattura_lines(n=12)
    layer0 = LayerTplLinesSharedV0()
    (tpl_raw_full, ids_raw_full, nums_raw_full), meta0 = layer0.encode(base_data)
    assert meta0.get("base_n", 0) == 0  # self-contained

    # Resource stores templates (packed as tpl_raw)
    # Decode the templates from tpl_raw using the resource helper: pack->unpack path
    # We pack templates extracted from tpl_raw_full as the base resource.
    # NOTE: templates is a list[list[bytes]]
    from gcc_ocf.layers.tpl_lines_v0 import _unpack_templates

    base_templates = _unpack_templates(tpl_raw_full)
    blob, rmeta = pack_tpl_dict_v0_resource(base_templates)
    templates2, rmeta2 = unpack_tpl_dict_v0_resource(blob)
    assert templates2 == base_templates
    assert rmeta2["tag8_hex"] == rmeta["tag8_hex"]

    # Configure a shared layer with the base dict
    shared = LayerTplLinesSharedV0()
    shared.set_shared_dict(templates2, tag8=bytes.fromhex(rmeta2["tag8_hex"]))

    # New data should produce a (possibly small) delta TPL
    data = _make_fattura_lines(n=15)
    (tpl_raw, ids_raw, nums_raw), meta = shared.encode(data)
    assert int(meta.get("base_n", 0)) == len(base_templates)
    assert "base_tag8" in meta
    # delta payload should generally be smaller than full dict (sanity)
    assert len(tpl_raw) <= len(tpl_raw_full)

    out = shared.decode((tpl_raw, ids_raw, nums_raw), meta)
    assert out == data


def test_tpl_lines_shared_fails_on_tag_mismatch(tmp_path: Path) -> None:
    from gcc_ocf.layers.tpl_lines_shared_v0 import LayerTplLinesSharedV0

    base_data = _make_fattura_lines(n=8)
    from gcc_ocf.layers.tpl_lines_v0 import LayerTplLinesV0
    v0 = LayerTplLinesV0()
    (tpl_raw_full, _, _), _ = v0.encode(base_data)
    from gcc_ocf.layers.tpl_lines_v0 import _unpack_templates
    base_templates = _unpack_templates(tpl_raw_full)

    shared = LayerTplLinesSharedV0()
    shared.set_shared_dict(base_templates, tag8=b"12345678")  # wrong tag

    data = _make_fattura_lines(n=9)
    (tpl_raw, ids_raw, nums_raw), meta = shared.encode(data)

    # Force expected tag8 different from configured one
    meta["base_tag8"] = b"ABCDEFGH"

    try:
        shared.decode((tpl_raw, ids_raw, nums_raw), meta)
        assert False, "expected tag8 mismatch error"
    except ValueError as e:
        assert "tag8 mismatch" in str(e)
