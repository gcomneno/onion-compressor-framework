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
import tempfile
from pathlib import Path
from typing import IO, Final

from gcc_ocf.dir_index import DirBundleIndexV1, SPEC_INDEX_V1
from gcc_ocf.errors import CorruptPayload, HashMismatch, UsageError
from gcc_ocf.verify import verify_container_file

SPEC_INDEX_V1_LOCAL: Final[str] = SPEC_INDEX_V1

BUNDLE_TEXT_GCC: Final[str] = "bundle_text.gcc"
BUNDLE_TEXT_INDEX: Final[str] = "bundle_text_index.json"
BUNDLE_TEXT_CONCAT: Final[str] = "bundle_text.concat"

BUNDLE_BIN_GCC: Final[str] = "bundle_bin.gcc"
BUNDLE_BIN_INDEX: Final[str] = "bundle_bin_index.json"
BUNDLE_BIN_CONCAT: Final[str] = "bundle_bin.concat"

_CHUNK: Final[int] = 1024 * 1024  # 1 MiB


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


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _choose_bin_codec_id() -> str:
    try:
        from gcc_ocf.core.codec_zstd import zstd as _zstd  # type: ignore
    except Exception:
        return "zlib"
    return "zstd" if _zstd is not None else "zlib"


def _copy_and_hash(src: IO[bytes], dst: IO[bytes], h: hashlib._Hash) -> int:
    """Copy src->dst in chunks updating hash; returns copied bytes."""
    total = 0
    while True:
        chunk = src.read(_CHUNK)
        if not chunk:
            break
        dst.write(chunk)
        h.update(chunk)
        total += len(chunk)
    return total


def _spool_classify_and_hash(path: Path) -> tuple[bool, str, int, IO[bytes]]:
    """
    Stream the file once:
      - write to a temp spool file
      - compute sha256 incrementally
      - classify as TEXT if: no NUL and valid UTF-8 for the entire stream
    Returns: (is_textish, sha256_hex, length, spool_file positioned at start)
    """
    sha = hashlib.sha256()
    total = 0

    has_nul = False
    utf8_ok = True
    dec = codecs.getincrementaldecoder("utf-8")()

    tf = tempfile.TemporaryFile()  # binary
    with path.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            tf.write(chunk)
            sha.update(chunk)
            total += len(chunk)

            if not has_nul and b"\x00" in chunk:
                has_nul = True
                utf8_ok = False  # by policy, NUL => BIN

            if utf8_ok:
                try:
                    # validate stream; we discard decoded text immediately
                    dec.decode(chunk, final=False)
                except UnicodeDecodeError:
                    utf8_ok = False

    if utf8_ok:
        try:
            dec.decode(b"", final=True)
        except UnicodeDecodeError:
            utf8_ok = False

    tf.seek(0)
    return (utf8_ok and not has_nul), sha.hexdigest(), total, tf


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

    idx_text = DirBundleIndexV1(
        root=inp.name,
        kind="text",
        concat_sha256="",
        layer_used="split_text_nums",
        codec_used="zlib",
        files=[],
        stream_codecs_used="TEXT:zlib,NUMS:num_v1",
    )

    idx_bin = DirBundleIndexV1(
        root=inp.name,
        kind="bin",
        concat_sha256="",
        layer_used="bytes",
        codec_used="",
        files=[],
        stream_codecs_used=None,
    )

    text_off = 0
    bin_off = 0

    text_concat_sha = hashlib.sha256()
    bin_concat_sha = hashlib.sha256()

    with text_concat_path.open("wb") as f_text, bin_concat_path.open("wb") as f_bin:
        for p in _iter_files_deterministic(inp):
            rel = p.relative_to(inp).as_posix()

            is_text, file_sha, ln, spool = _spool_classify_and_hash(p)
            try:
                if is_text:
                    off = text_off
                    _copy_and_hash(spool, f_text, text_concat_sha)
                    idx_text.put(rel, offset=off, length=ln, sha256=file_sha)
                    text_off += ln
                else:
                    off = bin_off
                    _copy_and_hash(spool, f_bin, bin_concat_sha)
                    idx_bin.put(rel, offset=off, length=ln, sha256=file_sha)
                    bin_off += ln
            finally:
                try:
                    spool.close()
                except Exception:
                    pass

    idx_text.concat_sha256 = text_concat_sha.hexdigest()
    idx_text.write(out / BUNDLE_TEXT_INDEX, indent=2)

    bin_codec_id = _choose_bin_codec_id()
    idx_bin.codec_used = bin_codec_id
    idx_bin.concat_sha256 = bin_concat_sha.hexdigest()
    idx_bin.write(out / BUNDLE_BIN_INDEX, indent=2)

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


def _extract_concat_bytes(bundle_gcc: Path) -> bytes:
    if not bundle_gcc.is_file():
        raise CorruptPayload(f"bundle .gcc non trovato: {bundle_gcc}")
    return _decompress_gcc_universal(bundle_gcc.read_bytes())


def verify_single_container_mixed_dir(output_dir: Path, *, full: bool = False) -> None:
    out = Path(output_dir)
    if not is_single_container_mixed_dir(out):
        raise CorruptPayload(f"non è una single-container mixed dir: {out}")

    # In full mode, any decode/decompress error is treated as tamper (HashMismatch),
    # because a single flipped bit can break codec frames before we can compute hashes.
    try:
        verify_container_file(out / BUNDLE_TEXT_GCC, full=full)
        verify_container_file(out / BUNDLE_BIN_GCC, full=full)
    except Exception as e:
        if full:
            raise HashMismatch("tamper detected (container verify failed)") from e
        raise CorruptPayload(f"verify container fallita: {e}") from e

    idx_text = DirBundleIndexV1.read(out / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = DirBundleIndexV1.read(out / BUNDLE_BIN_INDEX, expected_kind="bin")

    try:
        text_concat = _extract_concat_bytes(out / BUNDLE_TEXT_GCC)
        bin_concat = _extract_concat_bytes(out / BUNDLE_BIN_GCC)
    except Exception as e:
        if full:
            raise HashMismatch("tamper detected (decode failed)") from e
        raise CorruptPayload(f"decode fallita: {e}") from e

    if idx_text.concat_sha256 != _sha256_hex(text_concat):
        if full:
            raise HashMismatch("bundle_text concat sha256 mismatch (index vs payload)")
        raise CorruptPayload("bundle_text concat sha256 mismatch (index vs payload)")

    if idx_bin.concat_sha256 != _sha256_hex(bin_concat):
        if full:
            raise HashMismatch("bundle_bin concat sha256 mismatch (index vs payload)")
        raise CorruptPayload("bundle_bin concat sha256 mismatch (index vs payload)")

    if not full:
        return

    def _check_files(idx: DirBundleIndexV1, concat_bytes: bytes) -> None:
        for e in idx.iter_entries():
            blob = concat_bytes[e.offset : e.offset + e.length]
            if len(blob) != e.length:
                raise CorruptPayload(f"bundle slice fuori range: {e.rel}")
            if _sha256_hex(blob) != e.sha256:
                raise HashMismatch(f"bundle file hash mismatch: {e.rel}")

    _check_files(idx_text, text_concat)
    _check_files(idx_bin, bin_concat)


def unpack_single_container_mixed_dir(input_dir: Path, restore_dir: Path) -> None:
    inp = Path(input_dir)
    if not is_single_container_mixed_dir(inp):
        raise CorruptPayload(f"non è una single-container mixed dir: {inp}")

    restore = Path(restore_dir)
    restore.mkdir(parents=True, exist_ok=True)

    idx_text = DirBundleIndexV1.read(inp / BUNDLE_TEXT_INDEX, expected_kind="text")
    idx_bin = DirBundleIndexV1.read(inp / BUNDLE_BIN_INDEX, expected_kind="bin")

    text_concat = _extract_concat_bytes(inp / BUNDLE_TEXT_GCC)
    bin_concat = _extract_concat_bytes(inp / BUNDLE_BIN_GCC)

    def _restore(idx: DirBundleIndexV1, concat_bytes: bytes) -> None:
        for e in idx.iter_entries():
            data = concat_bytes[e.offset : e.offset + e.length]
            if len(data) != e.length:
                raise CorruptPayload(f"bundle slice fuori range: {e.rel}")

            outp = restore / e.rel
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(data)

    _restore(idx_text, text_concat)
    _restore(idx_bin, bin_concat)
