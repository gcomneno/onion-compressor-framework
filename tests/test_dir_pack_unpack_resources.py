from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_fattura_like(dir_: Path, *, n: int = 8) -> None:
    dir_.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        day = 10 + (i % 9)
        lines = [
            f"FATTURA {1000 + i}",
            f"DATA 2026-01-{day:02d}",
            "CLIENTE ACME SRL",
            f"RIGA {i} QTA 2 PREZZO 19.99 TOT 39.98",
            f"RIGA {i} QTA 1 PREZZO 5.50 TOT 5.50",
            "IVA 22% IMP 45.48 TOT 55.49",
            f"CODICE ART 000{i:02d} LOTTO 202601{day:02d}",
            "",
        ]
        (dir_ / f"fattura_{i:02d}.txt").write_text("\n".join(lines), encoding="utf-8")


def test_pack_unpack_dir_writes_tpl_resource_and_roundtrips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from gcc_ocf.core.gca import GCAReader
    from gcc_ocf.legacy import gcc_dir as gd

    # Isolate TOP db writes away from the real repo
    fake_repo = tmp_path / "fake_repo"
    (fake_repo / "tools").mkdir(parents=True)
    (fake_repo / "tools" / "top_pipelines.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gd, "_repo_root", lambda: fake_repo)

    input_dir = tmp_path / "in"
    _write_fattura_like(input_dir, n=8)

    out_dir = tmp_path / "out"
    gd.packdir(input_dir, out_dir, buckets=1)

    # Manifest has bucket_summary lines (ignored by unpackdir)
    manifest = out_dir / "manifest.jsonl"
    first = json.loads(manifest.read_text(encoding="utf-8").splitlines()[0])
    assert first.get("kind") == "bucket_summary"

    # Autopick report must exist
    report = json.loads((out_dir / "autopick_report.json").read_text(encoding="utf-8"))
    assert report.get("schema") == "gcc-ocf.autopick_report.v1"
    assert "00" in report.get("buckets", {})

    # The single-bucket archive must contain tpl_dict_v0 resource
    arch = out_dir / "bucket_00.gca"
    assert arch.is_file()
    with GCAReader(arch) as rd:
        res = rd.load_resources()
    assert "tpl_dict_v0" in res

    # Roundtrip unpackdir
    restore_dir = tmp_path / "restore"
    gd.unpackdir(out_dir, restore_dir)

    for p in sorted(input_dir.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(input_dir)
        r2 = restore_dir / rel
        assert r2.is_file()
        assert r2.read_bytes() == p.read_bytes()
