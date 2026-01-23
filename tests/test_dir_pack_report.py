from __future__ import annotations

import json
from pathlib import Path


def test_packdir_writes_aggregated_report(tmp_path: Path) -> None:
    from gcc_ocf.legacy.gcc_dir import packdir

    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    in_dir.mkdir()
    out_dir.mkdir()

    # A couple of small files: text compresses, bin may not.
    (in_dir / "a.txt").write_text("hello\n" * 2000, encoding="utf-8")
    (in_dir / "b.txt").write_text("world\n" * 1500, encoding="utf-8")
    (in_dir / "c.bin").write_bytes(bytes(range(256)) * 40)
    (in_dir / "d").write_text("noext\n" * 500, encoding="utf-8")

    packdir(in_dir, out_dir, buckets=4, jobs=1)

    rep_json = out_dir / "pack_report.json"
    rep_txt = out_dir / "pack_report.txt"
    assert rep_json.is_file(), "pack_report.json not written"
    assert rep_txt.is_file(), "pack_report.txt not written"

    rep = json.loads(rep_json.read_text(encoding="utf-8"))
    assert rep["schema"] == "gcc-ocf.dir_pack_report.v1"
    assert rep["mode"] == "classic_gca1"
    assert rep["buckets"] == 4
    assert rep["files_ok"] >= 1
    assert rep["total_in"] > 0
    assert rep["total_out"] > 0
    assert isinstance(rep["top_extensions"], list)
    keys = {row["key"] for row in rep["top_extensions"] if "key" in row}
    assert ".txt" in keys
