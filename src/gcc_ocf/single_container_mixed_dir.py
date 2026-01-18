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
- TEXT bundle: must decode as UTF-8 AND must NOT contain NUL bytes.
- BIN bundle: everything else.

Compression plan
----------------
- TEXT: split_text_nums + MBN (TEXT:zlib, NUMS:num_v1)
- BIN: bytes + (zstd if available else zlib)

Index schema: gcc-ocf.dir_bundle_index.v1
"""

from __future__ import annotations

import codecs
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Final, Protocol

from gcc_ocf.errors import CorruptPayload, HashMismatch, UsageError
from gcc_ocf.verify import verify_container_file

SPEC_INDEX_V1: Final[str] = "gcc-ocf.dir_bundle_index.v1"

BUNDLE_TEXT_GCC: Final[str] = "bundle_text.gcc"
BUNDLE_TEXT_INDEX: Final[str] = "bundle_text_index.json"
BUNDLE_TEXT_CONCAT: Final[str] = "bundle_text.concat"

BUNDLE_BIN_GCC: Final[str] = "bundle_bin.gcc"
BUNDLE_BIN_INDEX: Final[str] = "bundle_bin_index.json"
BUNDLE_BIN_CONCAT: Final[str] = "bundle_bin.concat"

_CHUNK: Final[int] = 1024 * 1024  # 1 MiB


class _Hash(Protocol):
    def update(self, data: bytes) -> None: ...


@dataclass(frozen=True)
class _IndexEntry:
    rel: str
    offset: int
    length: int
    sha256: str


@dataclass(frozen=True)
class _FileInfo:
    path: Path
    rel: str
    is_text: bool
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


def _analyze_file_textish_and_sha256(p: Path) -> tuple[bool, int, str]:
    """Stream-read p to compute (is_textish_utf8, size_bytes, sha256_hex) without loading full file."""
    h = hashlib.sha256()
    size = 0

    decoder = codecs.getincrementaldecoder("utf-8")()
    is_text = True

    with p.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            size += len(chunk)
            h.update(chunk)

            if is_text:
                if b"\x00" in chunk:
                    is_text = False
                else:
                    try:
                        decoder.decode(chunk, final=False)
                    except UnicodeDecodeError:
                        is_text = False

        if is_text:
            try:
                decoder.decode(b"", final=True)
            except UnicodeDecodeError:
                is_text = False

    return is_text, size, h.hexdigest()


def _copy_file_to(fp: Path, out_f: BinaryIO, *, concat_hash: _Hash) -> None:
    """Stream copy fp into out_f, updating concat_hash, without loading full file."""
    with fp.open("rb") as src:
        while True:
            chunk = src.read(_CHUNK)
            if not chunk:
                break
            out_f.write(chunk)
            concat_hash.update(chunk)


def _choose_bin_codec_id() -> str:
    try:
        from gcc_ocf.core.codec_zstd import zstd as _zstd  # type: ignore
    except Exception:
        return "zlib"
    return "zstd" if _zstd is not None else "zlib"


def _decompress_gcc_universal(blob: bytes) -> bytes:
    """Universal decoder (silent): v1..v6 + MBN (d7 behaviour) -> raw bytes."""
    from gcc_ocf.engine.container import Engine
    from gcc_ocf.engine.container_v6 import decompress_v6
    from gcc_ocf.legacy.gcc_huffman import (
        MAGIC,
        VERSION_STEP1,
        VERSION_STEP2,
        VERSION_STEP3,
        VERSION_STEP4,
        decompress_bytes_v1,
        decompress_bytes_v2,
        decompress_bytes_v3,
        decompress_bytes_v4,
    )

    if len(blob) < 4 or blob[:3] != MAGIC:
        raise CorruptPayload("bundle.gcc non GCC (magic mancante)")

    ver = blob[3]
    if ver == VERSION_STEP1:
        return decompress_bytes_v1(blob)
    if ver == VERSION_STEP2:
        return decompress_bytes_v2(blob)
    if ver == VERSION_STEP3:
        return decompress_bytes_v3(blob)
    if ver == VERSION_STEP4:
        return decompress_bytes_v4(blob)
    if ver == 5:
        return Engine.default().decompress(blob)
    if ver == 6:
        eng = Engine.default()
        return decompress_v6(eng, blob)

    raise CorruptPayload(f"Versione GCC non supportata: {ver}")


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

    infos: list[_FileInfo] = []
    for p in _iter_files_deterministic(inp):
        rel = p.relative_to(inp).as_posix()
        is_text, size, sha = _analyze_file_textish_and_sha256(p)
        infos.append(_FileInfo(path=p, rel=rel, is_text=is_text, length=size, sha256=sha))

    text_entries: list[_IndexEntry] = []
    bin_entries: list[_IndexEntry] = []
    text_off = 0
    bin_off = 0

    text_concat_hash = hashlib.sha256()
    bin_concat_hash = hashlib.sha256()

    with text_concat_path.open("wb") as f_text, bin_concat_path.open("wb") as f_bin:
        for info in infos:
            if info.is_text:
                _copy_file_to(info.path, f_text, concat_hash=text_concat_hash)
                text_entries.append(
                    _IndexEntry(rel=info.rel, offset=text_off, length=info.length, sha256=info.sha256)
                )
                text_off += info.length
            else:
                _copy_file_to(info.path, f_bin, concat_hash=bin_concat_hash)
                bin_entries.append(
                    _IndexEntry(rel=info.rel, offset=bin_off, length=info.length, sha256=info.sha256)
                )
                bin_off += info.length

    text_index: dict[str, Any] = {
        "spec": SPEC_INDEX_V1,
        "root": inp.name,
        "kind": "text",
        "count": len(text_entries),
        "files": [e.__dict__ for e in text_entries],
        "concat_sha256": text_concat_hash.hexdigest(),
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
        "concat_sha256": bin_concat_hash.hexdigest(),
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
    return _decompress_gcc_universal(bundle_gcc.read_bytes())


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
        raise CorruptPayload(f"verify container fallita: {e}") from e

    idx_text = _load_index(out / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = _load_index(out / BUNDLE_BIN_INDEX, expected_kind="bin")

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

    # TEXT first
    try:
        text_concat = _extract_concat_bytes(out / BUNDLE_TEXT_GCC)
    except Exception as e:
        if full:
            raise HashMismatch("tamper detected (decode failed)") from e
        raise CorruptPayload(f"decode fallita: {e}") from e

    if idx_text.get("concat_sha256") != _sha256_bytes(text_concat):
        msg = "bundle_text concat sha256 mismatch (index vs payload)"
        if full:
            raise HashMismatch(msg)
        raise CorruptPayload(msg)

    if full:
        _check_files(idx_text, text_concat)

    del text_concat

    # BIN second
    try:
        bin_concat = _extract_concat_bytes(out / BUNDLE_BIN_GCC)
    except Exception as e:
        if full:
            raise HashMismatch("tamper detected (decode failed)") from e
        raise CorruptPayload(f"decode fallita: {e}") from e

    if idx_bin.get("concat_sha256") != _sha256_bytes(bin_concat):
        msg = "bundle_bin concat sha256 mismatch (index vs payload)"
        if full:
            raise HashMismatch(msg)
        raise CorruptPayload(msg)

    if full:
        _check_files(idx_bin, bin_concat)


def unpack_single_container_mixed_dir(input_dir: Path, restore_dir: Path) -> None:
    inp = Path(input_dir)
    if not is_single_container_mixed_dir(inp):
        raise CorruptPayload(f"non è una single-container mixed dir: {inp}")

    restore = Path(restore_dir)
    restore.mkdir(parents=True, exist_ok=True)

    idx_text = _load_index(inp / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = _load_index(inp / BUNDLE_BIN_INDEX, expected_kind="bin")

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

    text_concat = _extract_concat_bytes(inp / BUNDLE_TEXT_GCC)
    _restore(idx_text, text_concat)
    del text_concat

    bin_concat = _extract_concat_bytes(inp / BUNDLE_BIN_GCC)
    _restore(idx_bin, bin_concat)
