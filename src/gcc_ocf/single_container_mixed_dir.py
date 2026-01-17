"""
Single-container MIXED directory packing.

Mode for mixed directories (UTF-8 text + binary files) while keeping a "single-container" UX.

Output layout (stable)
----------------------
<out_dir>/
  bundle_text.gcc
  bundle_text_index.json
  bundle_bin.gcc
  bundle_bin_index.json
  bundle_text.concat   (optional; only if keep_concat=True)
  bundle_bin.concat    (optional; only if keep_concat=True)

Policy
------
- TEXT bundle: "textish" UTF-8 (decoded) AND does NOT contain NUL bytes.
- BIN bundle: everything else.

Compression plan
----------------
- TEXT: split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)
- BIN: bytes + (zstd if available else zlib)

Index schema: gcc-ocf.dir_bundle_index.v1
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from gcc_ocf.errors import CorruptPayload, HashMismatch, UsageError
from gcc_ocf.verify import verify_container_file

SPEC_INDEX_V1: Final[str] = "gcc-ocf.dir_bundle_index.v1"

BUNDLE_TEXT_GCC: Final[str] = "bundle_text.gcc"
BUNDLE_TEXT_INDEX: Final[str] = "bundle_text_index.json"
BUNDLE_TEXT_CONCAT: Final[str] = "bundle_text.concat"

BUNDLE_BIN_GCC: Final[str] = "bundle_bin.gcc"
BUNDLE_BIN_INDEX: Final[str] = "bundle_bin_index.json"
BUNDLE_BIN_CONCAT: Final[str] = "bundle_bin.concat"


@dataclass(frozen=True)
class _IndexEntry:
    rel: str
    offset: int
    length: int
    sha256: str


def is_single_container_mixed_dir(out_dir: Path) -> bool:
    out = Path(out_dir)
    return (
        (out / BUNDLE_TEXT_GCC).is_file()
        and (out / BUNDLE_TEXT_INDEX).is_file()
        and (out / BUNDLE_BIN_GCC).is_file()
        and (out / BUNDLE_BIN_INDEX).is_file()
    )


def _iter_files_deterministic(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*") if p.is_file()]
    files.sort(key=lambda p: p.relative_to(root).as_posix())
    return files


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _is_textish_utf8(data: bytes) -> bool:
    """
    Minimal, deterministic "textish" heuristic:
      - must decode as UTF-8
      - must NOT contain NUL bytes
    This avoids classifying many binary blobs (that still decode) as TEXT.
    """
    if b"\x00" in data:
        return False
    try:
        data.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def _choose_bin_codec_id() -> str:
    """
    Policy: zstd if available, else zlib.
    "Available" means the optional dependency can be imported and the codec is usable.
    """
    try:
        from gcc_ocf.core.codec_zstd import zstd as _zstd  # type: ignore
    except Exception:
        return "zlib"
    return "zstd" if _zstd is not None else "zlib"


def pack_single_container_mixed_dir(
    input_dir: Path,
    output_dir: Path,
    *,
    keep_concat: bool = False,
) -> None:
    inp = Path(input_dir)
    out = Path(output_dir)
    if not inp.is_dir():
        raise UsageError(f"input_dir non è una directory: {inp}")

    out.mkdir(parents=True, exist_ok=True)

    text_concat_path = out / BUNDLE_TEXT_CONCAT
    bin_concat_path = out / BUNDLE_BIN_CONCAT

    text_entries: list[_IndexEntry] = []
    bin_entries: list[_IndexEntry] = []
    text_off = 0
    bin_off = 0

    with text_concat_path.open("wb") as f_text, bin_concat_path.open("wb") as f_bin:
        for p in _iter_files_deterministic(inp):
            rel = p.relative_to(inp).as_posix()
            data = p.read_bytes()
            sha = _sha256_bytes(data)

            if _is_textish_utf8(data):
                f_text.write(data)
                text_entries.append(
                    _IndexEntry(rel=rel, offset=text_off, length=len(data), sha256=sha)
                )
                text_off += len(data)
            else:
                f_bin.write(data)
                bin_entries.append(_IndexEntry(rel=rel, offset=bin_off, length=len(data), sha256=sha))
                bin_off += len(data)

    text_concat_bytes = text_concat_path.read_bytes()
    bin_concat_bytes = bin_concat_path.read_bytes()

    text_index: dict[str, Any] = {
        "spec": SPEC_INDEX_V1,
        "root": inp.name,
        "kind": "text",
        "count": len(text_entries),
        "files": [e.__dict__ for e in text_entries],
        "concat_sha256": _sha256_bytes(text_concat_bytes),
        "layer_used": "split_text_nums",
        "codec_used": "zlib",
        "stream_codecs_used": "TEXT:zlib,NUMS:num_v1",
    }
    (out / BUNDLE_TEXT_INDEX).write_text(
        json.dumps(text_index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    bin_codec_id = _choose_bin_codec_id()
    bin_index: dict[str, Any] = {
        "spec": SPEC_INDEX_V1,
        "root": inp.name,
        "kind": "bin",
        "count": len(bin_entries),
        "files": [e.__dict__ for e in bin_entries],
        "concat_sha256": _sha256_bytes(bin_concat_bytes),
        "layer_used": "bytes",
        "codec_used": bin_codec_id,
    }
    (out / BUNDLE_BIN_INDEX).write_text(
        json.dumps(bin_index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    from gcc_ocf.legacy.gcc_huffman import compress_file_v6, compress_file_v7

    compress_file_v7(
        str(text_concat_path),
        str(out / BUNDLE_TEXT_GCC),
        layer_id="split_text_nums",
        codec_id="zlib",
        stream_codecs_spec=None,
    )

    compress_file_v6(
        str(bin_concat_path),
        str(out / BUNDLE_BIN_GCC),
        layer_id="bytes",
        codec_id=bin_codec_id,
    )

    if not keep_concat:
        for p in (text_concat_path, bin_concat_path):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _load_index(path: Path, *, expected_kind: str) -> dict[str, Any]:
    if not path.is_file():
        raise CorruptPayload(f"bundle index non trovato: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise CorruptPayload(f"bundle index JSON invalido: {path}: {e}") from e

    if not isinstance(data, dict) or data.get("spec") != SPEC_INDEX_V1:
        raise CorruptPayload(f"bundle index spec invalida: {path}")

    if data.get("kind") != expected_kind:
        raise CorruptPayload(f"bundle index kind invalido (atteso {expected_kind}): {path}")

    files = data.get("files")
    if not isinstance(files, list):
        raise CorruptPayload(f"bundle index senza 'files': {path}")

    return data


def _extract_concat_bytes(bundle_gcc: Path) -> bytes:
    if not bundle_gcc.is_file():
        raise CorruptPayload(f"bundle .gcc non trovato: {bundle_gcc}")

    from gcc_ocf.legacy.gcc_huffman import decompress_file_v7

    with tempfile.TemporaryDirectory(prefix="gcc-ocf-bundle-") as td:
        tmp = Path(td) / "concat.bin"
        decompress_file_v7(str(bundle_gcc), str(tmp))
        return tmp.read_bytes()


def verify_single_container_mixed_dir(output_dir: Path, *, full: bool = False) -> None:
    out = Path(output_dir)
    if not is_single_container_mixed_dir(out):
        raise CorruptPayload(f"non è una single-container mixed dir: {out}")

    try:
        verify_container_file(out / BUNDLE_TEXT_GCC, full=full)
        verify_container_file(out / BUNDLE_BIN_GCC, full=full)
    except Exception as e:
        if full:
            raise HashMismatch("tamper detected (container verify failed)") from e
        raise

    idx_text = _load_index(out / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = _load_index(out / BUNDLE_BIN_INDEX, expected_kind="bin")

    text_concat = _extract_concat_bytes(out / BUNDLE_TEXT_GCC)
    bin_concat = _extract_concat_bytes(out / BUNDLE_BIN_GCC)

    if idx_text.get("concat_sha256") != _sha256_bytes(text_concat):
        raise CorruptPayload("bundle_text concat sha256 mismatch (index vs payload)")
    if idx_bin.get("concat_sha256") != _sha256_bytes(bin_concat):
        raise CorruptPayload("bundle_bin concat sha256 mismatch (index vs payload)")

    if not full:
        return

    def _check_files(idx: dict[str, Any], concat_bytes: bytes) -> None:
        for raw in idx["files"]:
            if not isinstance(raw, dict):
                raise CorruptPayload(f"bundle index entry invalida: {raw}")

            rel = raw.get("rel")
            off = int(raw.get("offset", -1))
            ln = int(raw.get("length", -1))
            sha = raw.get("sha256")

            if not rel or off < 0 or ln < 0 or not sha:
                raise CorruptPayload(f"bundle index entry invalida: {raw}")

            blob = concat_bytes[off : off + ln]
            if len(blob) != ln:
                raise CorruptPayload(f"bundle slice fuori range: {rel}")

            if _sha256_bytes(blob) != sha:
                raise HashMismatch(f"bundle file hash mismatch: {rel}")

    _check_files(idx_text, text_concat)
    _check_files(idx_bin, bin_concat)


def unpack_single_container_mixed_dir(input_dir: Path, restore_dir: Path) -> None:
    inp = Path(input_dir)
    if not is_single_container_mixed_dir(inp):
        raise CorruptPayload(f"non è una single-container mixed dir: {inp}")

    restore = Path(restore_dir)
    restore.mkdir(parents=True, exist_ok=True)

    idx_text = _load_index(inp / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = _load_index(inp / BUNDLE_BIN_INDEX, expected_kind="bin")

    text_concat = _extract_concat_bytes(inp / BUNDLE_TEXT_GCC)
    bin_concat = _extract_concat_bytes(inp / BUNDLE_BIN_GCC)

    def _restore(idx: dict[str, Any], concat_bytes: bytes) -> None:
        for raw in idx["files"]:
            if not isinstance(raw, dict):
                raise CorruptPayload(f"bundle index entry invalida: {raw}")

            rel = raw.get("rel")
            off = int(raw.get("offset", -1))
            ln = int(raw.get("length", -1))
            if not rel or off < 0 or ln < 0:
                raise CorruptPayload(f"bundle index entry invalida: {raw}")

            data = concat_bytes[off : off + ln]
            if len(data) != ln:
                raise CorruptPayload(f"bundle slice fuori range: {rel}")

            outp = restore / rel
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(data)

    _restore(idx_text, text_concat)
    _restore(idx_bin, bin_concat)
