from __future__ import annotations

import json
from pathlib import Path

import pytest


def _read_tree_bytes(root: Path) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            out[rel] = p.read_bytes()
    return out


def _flip_one_byte_safely(path: Path) -> None:
    size = path.stat().st_size
    if size <= 2:
        raise RuntimeError(f"File too small to tamper: {path} ({size} bytes)")

    pos = size // 2
    if size > 256:
        pos = max(64, min(pos, size - 64))

    with path.open("r+b") as fp:
        fp.seek(pos)
        b = fp.read(1)
        if not b:
            raise RuntimeError("Failed to read byte for tamper")
        fp.seek(pos)
        fp.write(bytes([b[0] ^ 0x01]))


def test_single_container_mixed_roundtrip_and_verify(tmp_path: Path) -> None:
    from gcc_ocf.single_container_mixed_dir import (
        BUNDLE_BIN_GCC,
        BUNDLE_BIN_INDEX,
        BUNDLE_TEXT_GCC,
        BUNDLE_TEXT_INDEX,
        is_single_container_mixed_dir,
        pack_single_container_mixed_dir,
        unpack_single_container_mixed_dir,
        verify_single_container_mixed_dir,
    )

    src = tmp_path / "src"
    out = tmp_path / "out"
    back = tmp_path / "back"
    (src / "sub").mkdir(parents=True)

    (src / "a.md").write_text("CIAO 123\nRIGA LUNGA: " + ("x" * 5000) + "\n", encoding="utf-8")
    (src / "sub" / "b.md").write_text("FATTURA N. 2\nTOTALE 5.60\n", encoding="utf-8")

    # Binary: force BIN classification (contains NUL + 0xFF)
    (src / "sub" / "c.bin").write_bytes(b"\x00\xff\x00\xff" * 256)

    pack_single_container_mixed_dir(src, out, keep_concat=False)
    assert is_single_container_mixed_dir(out)

    verify_single_container_mixed_dir(out, full=False)
    verify_single_container_mixed_dir(out, full=True)

    idx_bin = json.loads((out / BUNDLE_BIN_INDEX).read_text(encoding="utf-8"))
    assert idx_bin["kind"] == "bin"
    assert idx_bin["layer_used"] == "bytes"
    assert idx_bin["codec_used"] in {"zlib", "zstd"}

    idx_text = json.loads((out / BUNDLE_TEXT_INDEX).read_text(encoding="utf-8"))
    assert idx_text["kind"] == "text"
    assert idx_text["layer_used"] == "split_text_nums"
    assert idx_text["codec_used"] == "zlib"

    unpack_single_container_mixed_dir(out, back)
    assert _read_tree_bytes(src) == _read_tree_bytes(back)

    assert (out / BUNDLE_TEXT_GCC).is_file()
    assert (out / BUNDLE_BIN_GCC).is_file()


def test_single_container_mixed_detects_tamper(tmp_path: Path) -> None:
    from gcc_ocf.errors import HashMismatch
    from gcc_ocf.single_container_mixed_dir import (
        BUNDLE_BIN_GCC,
        pack_single_container_mixed_dir,
        verify_single_container_mixed_dir,
    )

    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir()

    (src / "t.txt").write_text("HELLO 123\n", encoding="utf-8")
    # Binary: force BIN classification
    (src / "b.bin").write_bytes(b"\x00\xff\x00\xff" * 1024)

    pack_single_container_mixed_dir(src, out, keep_concat=False)

    _flip_one_byte_safely(out / BUNDLE_BIN_GCC)

    with pytest.raises(HashMismatch):
        verify_single_container_mixed_dir(out, full=True)
