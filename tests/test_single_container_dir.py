from __future__ import annotations

from pathlib import Path

import pytest


def _read_tree_bytes(root: Path) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.resolve().relative_to(root.resolve()))
            out[rel] = p.read_bytes()
    return out


def test_single_container_roundtrip_and_verify(tmp_path: Path) -> None:
    from gcc_ocf.single_container_dir import (
        is_single_container_dir,
        pack_single_container_dir,
        unpack_single_container_dir,
        verify_single_container_dir,
    )

    src = tmp_path / "src"
    out = tmp_path / "out"
    back = tmp_path / "back"
    (src / "sub").mkdir(parents=True)

    (src / "a.md").write_text("CIAO 123\nRIGA LUNGA: " + ("x" * 5000) + "\n", encoding="utf-8")
    (src / "sub" / "b.md").write_text("FATTURA N. 2\nTOTALE 5.60\n", encoding="utf-8")
    (src / "sub" / "c.txt").write_text("HELLO 124\n", encoding="utf-8")

    pack_single_container_dir(src, out)
    assert is_single_container_dir(out)

    verify_single_container_dir(out, full=False)
    verify_single_container_dir(out, full=True)

    unpack_single_container_dir(out, back)
    assert _read_tree_bytes(src) == _read_tree_bytes(back)


def test_single_container_rejects_binary(tmp_path: Path) -> None:
    from gcc_ocf.errors import UsageError
    from gcc_ocf.single_container_dir import pack_single_container_dir

    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir()
    # invalid UTF-8
    (src / "bin.dat").write_bytes(b"\xff\xfe\x00\x00")

    with pytest.raises(UsageError):
        pack_single_container_dir(src, out)
