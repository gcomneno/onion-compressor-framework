from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_fattura_like_dense(dir_: Path, *, n_files: int = 24, n_lines: int = 40) -> None:
    """Create a dataset where tpl_lines_shared_v0 should beat tpl_lines_v0.

    We want many files sharing the *same* templates with varying numeric fields.
    """
    dir_.mkdir(parents=True, exist_ok=True)
    for i in range(n_files):
        day = 10 + (i % 9)
        inv = 2000 + i
        out = []
        out.append(f"FATTURA {inv}")
        out.append(f"DATA 2026-01-{day:02d}")
        out.append("CLIENTE ACME SRL")
        out.append("P.IVA 01234567890")
        # Repeated template lines with varying numbers
        for j in range(n_lines):
            qta = (j % 9) + 1
            prezzo = 0.75 + (j % 7) * 0.10
            tot = qta * prezzo
            out.append(f"RIGA ARTICOLO: vite M3 QTA {qta} PREZZO {prezzo:.2f} TOT {tot:.2f}")
        imponibile = sum(( (j % 9) + 1) * (0.75 + (j % 7) * 0.10) for j in range(n_lines))
        iva = imponibile * 0.22
        totale = imponibile + iva
        out.append(f"IMPONIBILE {imponibile:.2f}")
        out.append(f"IVA 22% {iva:.2f}")
        out.append(f"TOTALE {totale:.2f}")
        out.append(f"CODICE LOTTO 202601{day:02d}")
        (dir_ / f"fattura_dense_{i:02d}.txt").write_text("\n".join(out) + "\n", encoding="utf-8")


def test_autopick_prefers_tpl_lines_shared_when_pool_restricted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Force candidate pool to tpl_lines_shared_v0 first, tpl_lines_v0 fallback.

    Expectation:
      - bucket_type = mixed_text_nums
      - chosen layer = tpl_lines_shared_v0
      - archive contains tpl_dict_v0 resource
    """
    from gcc_ocf.dir_pipeline_spec import load_dir_pipeline_spec
    from gcc_ocf.legacy import gcc_dir as gd
    from gcc_ocf.core.gca import GCAReader

    # Isolate TOP db writes away from the real repo
    fake_repo = tmp_path / "fake_repo"
    (fake_repo / "tools").mkdir(parents=True)
    (fake_repo / "tools" / "top_pipelines.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gd, "_repo_root", lambda: fake_repo)

    input_dir = tmp_path / "in"
    _write_fattura_like_dense(input_dir, n_files=24, n_lines=40)

    # Custom dir spec: restrict mixed pool to tpl shared first, tpl v0 fallback.
    spec_path = tmp_path / "dir_spec.json"
    spec_obj = {
        "spec": "gcc-ocf.dir_pipeline.v1",
        "buckets": 1,
        "archive": True,
        "autopick": {"enabled": True, "sample_n": 4, "top_k": 2, "refresh_top": True},
        "candidate_pools": {
            "mixed_text_nums": [
                {"layer": "tpl_lines_shared_v0", "codec": "zlib"},
                {"layer": "tpl_lines_v0", "codec": "zlib"},
            ]
        },
        "resources": {
            "tpl_dict_v0": {"enabled": True, "k": 128},
            "num_dict_v1": {"enabled": True, "k": 64},
        },
    }
    spec_path.write_text(json.dumps(spec_obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    dir_spec = load_dir_pipeline_spec("@" + str(spec_path))

    out_dir = tmp_path / "out"
    gd.packdir(input_dir, out_dir, buckets=1, dir_spec=dir_spec)

    # Autopick report must exist and report chosen tpl_lines_shared_v0
    rep = json.loads((out_dir / "autopick_report.json").read_text(encoding="utf-8"))
    b0 = rep["buckets"]["00"]
    assert b0.get("bucket_type") == "mixed_text_nums"
    chosen = b0.get("chosen")
    assert isinstance(chosen, dict)
    assert chosen.get("layer_id") == "tpl_lines_shared_v0"

    # Archive contains tpl_dict_v0 resource
    arch = out_dir / "bucket_00.gca"
    with GCAReader(arch) as rd:
        res = rd.load_resources()
    assert "tpl_dict_v0" in res

    # Roundtrip
    restore_dir = tmp_path / "restore"
    gd.unpackdir(out_dir, restore_dir)
    for p in sorted(input_dir.rglob("*")):
        if p.is_file():
            rel = p.relative_to(input_dir)
            r2 = restore_dir / rel
            assert r2.is_file()
            assert r2.read_bytes() == p.read_bytes()
