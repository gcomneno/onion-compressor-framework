"""Verification helpers (Step 6).

We implement:
  - dir verify: validate a packed output directory (manifest + GCA1 archives)
  - file verify: validate a single container file (v6; v1..v5 via legacy decode)

Policy (user choice): light by default, --full recomputes hashes.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from gcc_ocf.core.gca import GCAReader
from gcc_ocf.engine.container_v6 import unpack_container_v6
from gcc_ocf.errors import (
    BadMagic,
    CorruptPayload,
    HashMismatch,
    MissingResource,
    UnsupportedVersion,
)

CHUNK_SIZE_DEFAULT = 256 * 1024


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = CHUNK_SIZE_DEFAULT) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_manifest_records(manifest_path: Path) -> Iterator[dict[str, Any]]:
    with Path(manifest_path).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if isinstance(rec, dict):
                yield rec


def verify_gca(path: Path, *, full: bool = False, chunk_size: int = CHUNK_SIZE_DEFAULT) -> None:
    """Verify a single GCA1 archive.

    Light:
      - CRC ok (done by GCAReader)
      - if an entry has blob_sha256, ensure it looks like a sha256 hex
      - validate index trailer if present

    Full:
      - recompute sha256 for each entry with offset/length and compare to blob_sha256
    """
    p = Path(path)
    if not p.is_file():
        raise CorruptPayload(f"GCA non trovato: {p}")

    with GCAReader(p) as rd:
        idx_raw = rd.index_raw()
        idx = list(rd.iter_index())

        # Validate trailer record if present
        if idx_raw:
            lines = idx_raw.splitlines(keepends=True)
            if lines:
                last_line = lines[-1]
                try:
                    last = json.loads(last_line.decode("utf-8").strip())
                except Exception:
                    last = None
                if isinstance(last, dict) and str(last.get("kind")) == "trailer":
                    body = b"".join(lines[:-1])
                    body_sha = hashlib.sha256(body).hexdigest()
                    exp = str(last.get("index_body_sha256") or "")
                    if exp and exp != body_sha:
                        raise HashMismatch(f"GCA index_body_sha256 mismatch: {p.name}")

        # Verify entry hashes
        for e in idx:
            if not isinstance(e, dict):
                continue
            if str(e.get("kind")) == "trailer":
                continue
            rel = str(e.get("rel") or "")
            off = int(e.get("offset") or 0)
            ln = int(e.get("length") or 0)
            if ln <= 0:
                continue
            exp = str(e.get("blob_sha256") or "")
            if exp and (len(exp) != 64 or any(c not in "0123456789abcdef" for c in exp.lower())):
                raise CorruptPayload(f"GCA blob_sha256 malformato per {rel}")
            exp_crc = e.get("blob_crc32")
            if exp_crc is not None:
                try:
                    _ = int(exp_crc)
                except Exception:
                    raise CorruptPayload(f"GCA blob_crc32 malformato per {rel}")
            if full:
                got, got_crc = rd.sha256_crc32_blob(off, ln, chunk_size=chunk_size)
                exp_crc = e.get("blob_crc32")
                if exp and got != exp:
                    raise HashMismatch(f"GCA blob hash mismatch per {rel}")
                if exp_crc is not None:
                    try:
                        exp_crc_i = int(exp_crc)
                    except Exception:
                        raise CorruptPayload(f"GCA blob_crc32 malformato per {rel}")
                    if int(got_crc) != exp_crc_i:
                        raise HashMismatch(f"GCA blob CRC mismatch per {rel}")


def verify_packed_dir(
    output_dir: Path, *, full: bool = False, chunk_size: int = CHUNK_SIZE_DEFAULT
) -> None:
    """Verify a packed directory (manifest + GCA1 archives)."""
    out = Path(output_dir)
    manifest = out / "manifest.jsonl"
    if not manifest.is_file():
        raise CorruptPayload(f"manifest non trovato: {manifest}")

    # Collect file records + bucket summaries
    file_recs: list[dict[str, Any]] = []
    needed_archives: dict[str, list[dict[str, Any]]] = {}
    bucket_summaries: dict[int, dict[str, Any]] = {}

    for rec in _iter_manifest_records(manifest):
        if rec.get("kind") == "bucket_summary":
            try:
                b = int(rec.get("bucket") or 0)
                bucket_summaries[b] = rec
            except Exception:
                pass
            continue
        rel = rec.get("rel")
        if not rel or "error" in rec:
            continue
        file_recs.append(rec)
        arch = rec.get("archive")
        if arch:
            needed_archives.setdefault(str(arch), []).append(rec)

    # Verify each archive (index/trailer + optional full hashes)
    for arch in sorted(needed_archives.keys()):
        verify_gca(out / arch, full=full, chunk_size=chunk_size)

    # Cross-check manifest vs archive index
    for arch, recs in needed_archives.items():
        p = out / arch
        with GCAReader(p) as rd:
            idx = list(rd.iter_index())
            by_rel: dict[str, dict[str, Any]] = {}
            for e in idx:
                if not isinstance(e, dict) or str(e.get("kind")) == "trailer":
                    continue
                r = str(e.get("rel") or "")
                if r:
                    by_rel[r] = e

            for rec in recs:
                r = str(rec.get("rel") or "")
                if r not in by_rel:
                    raise CorruptPayload(f"manifest punta a entry mancante in {arch}: {r}")
                e = by_rel[r]
                exp = str(e.get("blob_sha256") or "")
                man_blob_sha = str(rec.get("blob_sha256") or "")
                if man_blob_sha and exp and man_blob_sha != exp:
                    raise HashMismatch(f"manifest/blob_sha256 mismatch: {r}")
                if full:
                    off = int(rec.get("archive_offset") or 0)
                    ln = int(rec.get("archive_length") or 0)
                    got, got_crc = rd.sha256_crc32_blob(off, ln, chunk_size=chunk_size)
                    exp_crc = e.get("blob_crc32")
                    if exp and got != exp:
                        raise HashMismatch(f"blob hash mismatch: {r}")
                    if exp_crc is not None:
                        try:
                            exp_crc_i = int(exp_crc)
                        except Exception:
                            raise CorruptPayload(f"GCA blob_crc32 malformato per {r}")
                        if int(got_crc) != exp_crc_i:
                            raise HashMismatch(f"blob CRC mismatch: {r}")

            # Resource checks (from bucket_summary)
            # Determine buckets that map to this archive.
            buckets_here = {int(rr.get("bucket") or 0) for rr in recs}
            if buckets_here:
                res = rd.load_resources()
                for b in sorted(buckets_here):
                    bs = bucket_summaries.get(b) or {}
                    declared = (bs.get("bucket_resources") or []) if isinstance(bs, dict) else []
                    meta_map = (
                        (bs.get("bucket_resources_meta") or {}) if isinstance(bs, dict) else {}
                    )
                    for name in declared:
                        if name not in res:
                            raise MissingResource(
                                f"resource mancante in {arch}: bucket={b} name={name}"
                            )
                        exp_sha = str((meta_map.get(name) or {}).get("blob_sha256") or "")
                        got_sha = str(
                            (res.get(name) or {}).get("meta", {}).get("blob_sha256") or ""
                        )
                        if exp_sha and got_sha and exp_sha != got_sha:
                            raise HashMismatch(f"resource sha mismatch: {arch} {name}")
                        if full and exp_sha:
                            # recompute
                            # find entry in index
                            for e in idx:
                                if isinstance(e, dict) and (
                                    str(e.get("rel") or "") in (f"__res__/{name}",)
                                ):
                                    off = int(e.get("offset") or 0)
                                    ln = int(e.get("length") or 0)
                                    if ln > 0:
                                        recomputed, recomputed_crc = rd.sha256_crc32_blob(
                                            off, ln, chunk_size=chunk_size
                                        )
                                        if recomputed != exp_sha:
                                            raise HashMismatch(
                                                f"resource blob hash mismatch: {arch} {name}"
                                            )
                                        exp_crc = e.get("blob_crc32")
                                        if exp_crc is not None:
                                            try:
                                                exp_crc_i = int(exp_crc)
                                            except Exception:
                                                raise CorruptPayload(
                                                    f"GCA blob_crc32 malformato per resource {name}"
                                                )
                                            if int(recomputed_crc) != exp_crc_i:
                                                raise HashMismatch(
                                                    f"resource blob CRC mismatch: {arch} {name}"
                                                )

                                    break


def verify_container_file(path: Path, *, full: bool = False) -> None:
    """Verify a single container file.

    Light:
      - parse v6 header
    Full:
      - attempt full decode (lossless) via legacy universal decoder
    """
    p = Path(path)
    if not p.is_file():
        raise CorruptPayload(f"file non trovato: {p}")
    blob = p.read_bytes()
    try:
        unpack_container_v6(blob)
    except ValueError as e:
        msg = str(e)
        if "magic" in msg:
            raise BadMagic(msg)
        if "version" in msg:
            raise UnsupportedVersion(msg)
        raise CorruptPayload(msg)

    if full:
        # Use universal decoder for deeper validation.
        from gcc_ocf.engine.container import Engine
        from gcc_ocf.engine.container_v6 import decompress_v6

        eng = Engine.default()
        _ = decompress_v6(eng, blob, allow_extract=False)
