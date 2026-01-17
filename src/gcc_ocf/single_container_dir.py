"""Single-container directory mode (text-only).

Used by: `gcc-ocf dir pack --single-container`.

Produces:
  - output_dir/bundle.gcc
  - output_dir/bundle_index.json

Text-only: non-UTF8/binary files are rejected (use normal dir pack instead).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gcc_ocf.errors import CorruptPayload, HashMismatch, UsageError

SPEC_ID = "gcc-ocf.single-container.v1"
BUNDLE_NAME = "bundle.gcc"
INDEX_NAME = "bundle_index.json"


@dataclass(frozen=True)
class BundleEntry:
    rel: str
    offset: int
    length: int
    sha256: str


def is_single_container_dir(path: Path) -> bool:
    """Return True if `path` looks like a single-container packed dir."""
    p = Path(path)
    return (p / BUNDLE_NAME).is_file() and (p / INDEX_NAME).is_file()


def _iter_files_sorted(root: Path) -> list[Path]:
    root = Path(root)
    files: list[Path] = []
    for p in root.rglob("*"):
        if p.is_file():
            files.append(p)
    files.sort(key=lambda x: str(x.resolve().relative_to(root.resolve())))
    return files


def _relpath(root: Path, p: Path) -> str:
    return str(p.resolve().relative_to(root.resolve()))


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_index(index_path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise CorruptPayload(f"index JSON non valido: {index_path}: {e}") from e
    if not isinstance(obj, dict):
        raise CorruptPayload(f"index non è un oggetto JSON: {index_path}")
    if obj.get("spec") != SPEC_ID:
        raise CorruptPayload(
            f"index spec non supportata: {obj.get('spec')!r} (attesa {SPEC_ID!r})"
        )
    files = obj.get("files")
    if not isinstance(files, list):
        raise CorruptPayload("index: campo 'files' mancante o non-list")
    return obj


def _decode_bundle_bytes(bundle_path: Path) -> bytes:
    from gcc_ocf.engine.container import Engine
    from gcc_ocf.engine.container_v6 import decompress_v6

    blob = bundle_path.read_bytes()
    eng = Engine.default()
    return decompress_v6(eng, blob, allow_extract=False)


def pack_single_container_dir(input_dir: Path, output_dir: Path) -> None:
    """Pack a directory into ONE bundle container (text-only)."""
    from gcc_ocf.legacy.gcc_huffman import compress_file_v7

    in_dir = Path(input_dir).resolve()
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _iter_files_sorted(in_dir)
    if not files:
        raise UsageError(f"directory vuota: {in_dir}")

    # 1) build concatenated blob + index (deterministic order)
    concat_path = out_dir / "bundle.concat"
    entries: list[BundleEntry] = []
    off = 0

    with concat_path.open("wb") as fp:
        for p in files:
            rel = _relpath(in_dir, p)
            data = p.read_bytes()
            try:
                data.decode("utf-8")
            except UnicodeDecodeError as e:
                raise UsageError(
                    f"--single-container: file non UTF-8/binary: {rel} (usa dir pack normale)"
                ) from e

            fp.write(data)
            entries.append(
                BundleEntry(rel=rel, offset=int(off), length=int(len(data)), sha256=_sha256_hex(data))
            )
            off += len(data)

    # 2) compress concat using the winning semantic pipeline
    bundle_path = out_dir / BUNDLE_NAME
    compress_file_v7(
        str(concat_path),
        str(bundle_path),
        layer_id="split_text_nums",
        codec_id="zlib",
        stream_codecs_spec="TEXT:zlib,NUMS:num_v1",
    )

    # 3) write index JSON
    index_obj: dict[str, Any] = {
        "spec": SPEC_ID,
        "bundle": BUNDLE_NAME,
        "concat_size": int(off),
        "pipeline": {
            "layer": "split_text_nums",
            "codec": "zlib",
            "stream_codecs": {"TEXT": "zlib", "NUMS": "num_v1"},
            "mbn": True,
        },
        "files": [
            {
                "rel": e.rel,
                "offset": int(e.offset),
                "length": int(e.length),
                "sha256": e.sha256,
            }
            for e in entries
        ],
    }
    (out_dir / INDEX_NAME).write_text(
        json.dumps(index_obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    # remove intermediate concat (keep output dir clean)
    try:
        concat_path.unlink()
    except Exception:
        pass


def verify_single_container_dir(input_dir: Path, *, full: bool = False) -> None:
    """Verify a single-container packed dir."""
    from gcc_ocf.verify import verify_container_file

    in_dir = Path(input_dir).resolve()
    index_path = in_dir / INDEX_NAME
    bundle_path = in_dir / BUNDLE_NAME

    if not index_path.is_file():
        raise CorruptPayload(f"index non trovato: {index_path}")
    if not bundle_path.is_file():
        raise CorruptPayload(f"bundle non trovato: {bundle_path}")

    obj = _load_index(index_path)
    verify_container_file(bundle_path, full=full)

    if not full:
        return

    data = _decode_bundle_bytes(bundle_path)
    n = len(data)

    for rec in obj.get("files", []):
        if not isinstance(rec, dict):
            raise CorruptPayload("index: record file non-oggetto")
        rel = rec.get("rel")
        off = rec.get("offset")
        ln = rec.get("length")
        sha = rec.get("sha256")

        if not isinstance(rel, str) or not rel:
            raise CorruptPayload("index: rel mancante")
        if not isinstance(off, int) or not isinstance(ln, int) or off < 0 or ln < 0:
            raise CorruptPayload(f"index: offset/length invalidi per {rel}")
        if not isinstance(sha, str) or len(sha) < 16:
            raise CorruptPayload(f"index: sha256 invalido per {rel}")
        if off + ln > n:
            raise CorruptPayload(f"index: bounds fuori range per {rel} (off={off} len={ln} n={n})")

        chunk = data[off : off + ln]
        got = _sha256_hex(chunk)
        if got != sha:
            raise HashMismatch(f"sha256 mismatch per {rel}")


def unpack_single_container_dir(input_dir: Path, restore_dir: Path) -> None:
    """Unpack a single-container packed dir."""
    in_dir = Path(input_dir).resolve()
    out_dir = Path(restore_dir).resolve()

    index_path = in_dir / INDEX_NAME
    bundle_path = in_dir / BUNDLE_NAME

    if not index_path.is_file():
        raise CorruptPayload(f"index non trovato: {index_path}")
    if not bundle_path.is_file():
        raise CorruptPayload(f"bundle non trovato: {bundle_path}")

    obj = _load_index(index_path)
    data = _decode_bundle_bytes(bundle_path)
    n = len(data)

    out_dir.mkdir(parents=True, exist_ok=True)

    for rec in obj.get("files", []):
        if not isinstance(rec, dict):
            raise CorruptPayload("index: record file non-oggetto")
        rel = rec.get("rel")
        off = rec.get("offset")
        ln = rec.get("length")
        sha = rec.get("sha256")

        if not isinstance(rel, str) or not rel:
            raise CorruptPayload("index: rel mancante")
        if not isinstance(off, int) or not isinstance(ln, int) or off < 0 or ln < 0:
            raise CorruptPayload(f"index: offset/length invalidi per {rel}")
        if off + ln > n:
            raise CorruptPayload(f"index: bounds fuori range per {rel}")

        chunk = data[off : off + ln]
        if isinstance(sha, str) and sha:
            got = _sha256_hex(chunk)
            if got != sha:
                raise HashMismatch(f"sha256 mismatch per {rel}")

        dst = out_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(chunk)
