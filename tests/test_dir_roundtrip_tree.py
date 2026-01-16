from __future__ import annotations

import hashlib
from pathlib import Path


def _sha256_file(p: Path, *, chunk_size: int = 256 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _tree_digest(root: Path) -> dict[str, str]:
    """Return {relative_posix_path: sha256} for all files under root."""
    out: dict[str, str] = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            out[rel] = _sha256_file(p)
    return out


def test_dir_roundtrip_tree_pack_verify_unpack(tmp_path: Path) -> None:
    """End-to-end dir workflow: pack -> verify -> unpack -> tree equality (paths+bytes)."""
    from gcc_ocf.legacy.gcc_dir import packdir, unpackdir
    from gcc_ocf.verify import verify_packed_dir

    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    back_dir = tmp_path / "back"

    # Build a small deterministic tree
    (in_dir / "sub").mkdir(parents=True)
    (in_dir / "sub2" / "deep").mkdir(parents=True)

    (in_dir / "a.txt").write_text("HELLO 123\n", encoding="utf-8")
    (in_dir / "b.bin").write_bytes(b"\x00\x01\x02\x03\xff" * 1000)
    (in_dir / "sub" / "c.txt").write_text(
        "FATTURA N. 42\nRIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\nTOTALE 12.00\n",
        encoding="utf-8",
    )
    (in_dir / "sub2" / "deep" / "d.txt").write_text("line1\nline2\nline3\n", encoding="utf-8")

    # Pack (bucketing deterministic; autopick inside packdir)
    packdir(in_dir, out_dir, buckets=8, dir_spec=None)

    # Verify (light + full)
    verify_packed_dir(out_dir, full=False)
    verify_packed_dir(out_dir, full=True)

    # Unpack
    unpackdir(out_dir, back_dir)

    # Compare tree digests
    dig_in = _tree_digest(in_dir)
    dig_back = _tree_digest(back_dir)

    assert sorted(dig_in.keys()) == sorted(dig_back.keys())
    for k in sorted(dig_in.keys()):
        assert dig_in[k] == dig_back[k], f"Mismatch for {k}"
