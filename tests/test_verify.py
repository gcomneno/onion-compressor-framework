from __future__ import annotations

import json
from pathlib import Path

import pytest


def _read_manifest(path: Path) -> list[dict]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def test_verify_dir_light_and_full(tmp_path: Path) -> None:
    from gcc_ocf.legacy.gcc_dir import packdir, unpackdir
    from gcc_ocf.verify import verify_packed_dir

    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    back_dir = tmp_path / "back"
    in_dir.mkdir()
    (in_dir / "a.txt").write_text(
        "FATTURA N. 1\nRIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\nTOTALE 12.00\n", encoding="utf-8"
    )
    (in_dir / "b.txt").write_text(
        "FATTURA N. 2\nRIGA ARTICOLO: dado M3 qty=7 prezzo=0.80\nTOTALE 5.60\n", encoding="utf-8"
    )

    packdir(in_dir, out_dir, buckets=8, dir_spec=None)
    # light
    verify_packed_dir(out_dir, full=False)
    # full
    verify_packed_dir(out_dir, full=True)

    # roundtrip still ok
    unpackdir(out_dir, back_dir)
    assert (back_dir / "a.txt").read_text(encoding="utf-8") == (in_dir / "a.txt").read_text(
        encoding="utf-8"
    )


def test_verify_detects_tamper(tmp_path: Path) -> None:
    from gcc_ocf.errors import HashMismatch
    from gcc_ocf.legacy.gcc_dir import packdir
    from gcc_ocf.verify import verify_packed_dir

    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    in_dir.mkdir()
    (in_dir / "x.txt").write_text("HELLO 123\n", encoding="utf-8")
    (in_dir / "y.txt").write_text("HELLO 124\n", encoding="utf-8")
    packdir(in_dir, out_dir, buckets=4, dir_spec=None)

    mf = out_dir / "manifest.jsonl"
    recs = _read_manifest(mf)
    # find first file record that points to an archive
    file_rec = next(r for r in recs if r.get("rel") and r.get("archive"))
    arch = out_dir / str(file_rec["archive"])
    off = int(file_rec["archive_offset"])
    ln = int(file_rec["archive_length"])

    # flip one byte inside the blob area (not in index/trailer)
    with arch.open("r+b") as fp:
        fp.seek(off + min(10, max(0, ln - 1)))
        b = fp.read(1)
        fp.seek(fp.tell() - 1)
        fp.write(bytes([(b[0] ^ 0x01) if b else 0x01]))

    with pytest.raises(HashMismatch):
        verify_packed_dir(out_dir, full=True)
