"""
Single-container directory packing.

Goal
----
Provide a directory packing mode that behaves like the "winning" approach for text corpora:
  concat (deterministic) + split_text_nums + MBN (TEXT:zlib, NUMS:num_v1) -> strong compression

Output layout (stable)
----------------------
<out_dir>/
  bundle.gcc          # GCC container (v1..v6 + MBN supported by d7)
  bundle_index.json   # JSON index describing each original file slice inside the concat
  bundle.concat       # optional; only if keep_concat=True

Constraints
-----------
- Text-only: every file must be valid UTF-8.
- No format changes to existing packed-dir (manifest + buckets). This is a separate mode.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from gcc_ocf.errors import CorruptPayload, HashMismatch, UsageError
from gcc_ocf.verify import verify_container_file

BUNDLE_GCC: Final[str] = "bundle.gcc"
BUNDLE_INDEX: Final[str] = "bundle_index.json"
BUNDLE_CONCAT: Final[str] = "bundle.concat"

# Bundle format version (only for the index schema, not the container itself)
BUNDLE_INDEX_SPEC: Final[str] = "gcc-ocf.dir_bundle_index.v1"


@dataclass(frozen=True)
class _IndexEntry:
    rel: str
    offset: int
    length: int
    sha256: str


def is_single_container_dir(out_dir: Path) -> bool:
    out = Path(out_dir)
    return (out / BUNDLE_GCC).is_file() and (out / BUNDLE_INDEX).is_file()


def _iter_files_deterministic(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*") if p.is_file()]
    files.sort(key=lambda p: p.relative_to(root).as_posix())
    return files


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_utf8_bytes(p: Path) -> bytes:
    b = p.read_bytes()
    try:
        b.decode("utf-8")
    except UnicodeDecodeError as e:
        raise UsageError(
            f"single-container: file non UTF-8/binary: {p} "
            f"(pos={e.start}). Usa 'gcc-ocf dir pack' classico per dati binari."
        ) from e
    return b


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


def pack_single_container_dir(input_dir: Path, output_dir: Path, *, keep_concat: bool = False) -> None:
    inp = Path(input_dir)
    out = Path(output_dir)
    if not inp.is_dir():
        raise UsageError(f"input_dir non è una directory: {inp}")

    out.mkdir(parents=True, exist_ok=True)

    concat_path = out / BUNDLE_CONCAT
    index_path = out / BUNDLE_INDEX
    gcc_path = out / BUNDLE_GCC

    entries: list[_IndexEntry] = []
    offset = 0

    with concat_path.open("wb") as fp:
        for p in _iter_files_deterministic(inp):
            rel = p.relative_to(inp).as_posix()
            data = _read_utf8_bytes(p)

            fp.write(data)
            entries.append(
                _IndexEntry(rel=rel, offset=offset, length=len(data), sha256=_sha256_bytes(data))
            )
            offset += len(data)

    payload: dict[str, Any] = {
        "spec": BUNDLE_INDEX_SPEC,
        "root": inp.name,
        "kind": "text",
        "count": len(entries),
        "files": [e.__dict__ for e in entries],
        "concat_sha256": _sha256_bytes(concat_path.read_bytes()),
        "layer_used": "split_text_nums",
        "codec_used": "zlib",
        "stream_codecs_used": "TEXT:zlib,NUMS:num_v1",
    }
    index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    from gcc_ocf.legacy.gcc_huffman import compress_file_v7

    compress_file_v7(
        str(concat_path),
        str(gcc_path),
        layer_id="split_text_nums",
        codec_id="zlib",
        stream_codecs_spec=None,  # smart default TEXT:zlib, NUMS:num_v1
    )

    if not keep_concat:
        try:
            concat_path.unlink(missing_ok=True)
        except Exception:
            pass


def _load_index(out_dir: Path) -> dict[str, Any]:
    p = Path(out_dir) / BUNDLE_INDEX
    if not p.is_file():
        raise CorruptPayload(f"bundle index non trovato: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise CorruptPayload(f"bundle index JSON invalido: {p}: {e}") from e

    if not isinstance(data, dict) or data.get("spec") != BUNDLE_INDEX_SPEC:
        raise CorruptPayload(f"bundle index spec invalida: {p}")

    if "files" not in data or not isinstance(data["files"], list):
        raise CorruptPayload(f"bundle index senza 'files': {p}")

    return data


def _extract_concat_bytes(out_dir: Path) -> bytes:
    out = Path(out_dir)
    gcc_path = out / BUNDLE_GCC
    if not gcc_path.is_file():
        raise CorruptPayload(f"bundle.gcc non trovato: {gcc_path}")
    blob = gcc_path.read_bytes()
    return _decompress_gcc_universal(blob)


def verify_single_container_dir(output_dir: Path, *, full: bool = False) -> None:
    out = Path(output_dir)
    if not is_single_container_dir(out):
        raise CorruptPayload(f"non è una single-container dir: {out}")

    verify_container_file(out / BUNDLE_GCC, full=full)

    idx = _load_index(out)
    concat_bytes = _extract_concat_bytes(out)

    concat_sha = _sha256_bytes(concat_bytes)
    if idx.get("concat_sha256") != concat_sha:
        raise CorruptPayload("bundle concat sha256 mismatch (index vs payload)")

    if not full:
        return

    for raw in idx["files"]:
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


def unpack_single_container_dir(input_dir: Path, restore_dir: Path) -> None:
    inp = Path(input_dir)
    if not is_single_container_dir(inp):
        raise CorruptPayload(f"non è una single-container dir: {inp}")

    restore = Path(restore_dir)
    restore.mkdir(parents=True, exist_ok=True)

    idx = _load_index(inp)
    concat_bytes = _extract_concat_bytes(inp)

    for raw in idx["files"]:
        rel = raw["rel"]
        off = int(raw["offset"])
        ln = int(raw["length"])
        data = concat_bytes[off : off + ln]
        if len(data) != ln:
            raise CorruptPayload(f"bundle slice fuori range: {rel}")

        outp = restore / rel
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_bytes(data)
