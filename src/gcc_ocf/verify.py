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
from dataclasses import dataclass
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


def _is_sha256_hex(s: str) -> bool:
    ss = s.strip().lower()
    return len(ss) == 64 and all(c in "0123456789abcdef" for c in ss)


def _as_int(v: Any, *, where: str) -> int:
    try:
        return int(v)
    except Exception as err:
        raise CorruptPayload(where) from err


@dataclass(frozen=True)
class _GCATrailer:
    index_body_sha256: str | None


@dataclass(frozen=True)
class _GCAIndexEntry:
    kind: str
    rel: str
    offset: int
    length: int
    blob_sha256: str | None
    blob_crc32: int | None


@dataclass(frozen=True)
class _ManifestFileRec:
    rel: str
    archive: str | None
    bucket: int
    archive_offset: int
    archive_length: int
    blob_sha256: str | None


@dataclass(frozen=True)
class _BucketSummary:
    bucket: int
    bucket_resources: list[str]
    bucket_resources_meta: dict[str, dict[str, Any]]


def _parse_gca_trailer(idx_raw: bytes) -> _GCATrailer | None:
    if not idx_raw:
        return None
    lines = idx_raw.splitlines(keepends=True)
    if not lines:
        return None
    last_line = lines[-1]
    try:
        last = json.loads(last_line.decode("utf-8").strip())
    except Exception:
        return None
    if not isinstance(last, dict) or str(last.get("kind")) != "trailer":
        return None
    sha = last.get("index_body_sha256")
    if sha is None:
        return _GCATrailer(index_body_sha256=None)
    if not isinstance(sha, str) or not sha.strip():
        raise CorruptPayload("GCA trailer index_body_sha256 malformato")
    if not _is_sha256_hex(sha):
        raise CorruptPayload("GCA trailer index_body_sha256 non-hex")
    return _GCATrailer(index_body_sha256=sha.strip().lower())


def _parse_gca_index_entry(e: Any) -> _GCAIndexEntry | None:
    if not isinstance(e, dict):
        return None

    # Normal entries do NOT have a "kind" field (writer emits rel/offset/length + meta).
    # Only special records (resources, trailer) set it.
    kind = str(e.get("kind") or "entry")

    if kind == "trailer":
        return _GCAIndexEntry(
            kind=kind,
            rel="",
            offset=0,
            length=0,
            blob_sha256=None,
            blob_crc32=None,
        )

    rel = str(e.get("rel") or "")
    off = _as_int(e.get("offset") or 0, where=f"GCA offset malformato per {rel}")
    ln = _as_int(e.get("length") or 0, where=f"GCA length malformato per {rel}")

    sha = e.get("blob_sha256")
    blob_sha = None
    if sha is not None:
        if not isinstance(sha, str):
            raise CorruptPayload(f"GCA blob_sha256 malformato per {rel}")
        if sha.strip():
            if not _is_sha256_hex(sha):
                raise CorruptPayload(f"GCA blob_sha256 malformato per {rel}")
            blob_sha = sha.strip().lower()

    crc = e.get("blob_crc32")
    blob_crc = None
    if crc is not None:
        blob_crc = _as_int(crc, where=f"GCA blob_crc32 malformato per {rel}")

    return _GCAIndexEntry(
        kind=kind,
        rel=rel,
        offset=off,
        length=ln,
        blob_sha256=blob_sha,
        blob_crc32=blob_crc,
    )


def _parse_bucket_summary(rec: dict[str, Any]) -> _BucketSummary | None:
    if rec.get("kind") != "bucket_summary":
        return None
    b = _as_int(rec.get("bucket") or 0, where="bucket_summary.bucket malformato")
    declared = rec.get("bucket_resources")
    if declared is None:
        declared_list: list[str] = []
    elif isinstance(declared, list):
        declared_list = [str(x) for x in declared if str(x)]
    else:
        raise CorruptPayload("bucket_summary.bucket_resources malformato")

    meta = rec.get("bucket_resources_meta")
    if meta is None:
        meta_map: dict[str, dict[str, Any]] = {}
    elif isinstance(meta, dict):
        meta_map = {str(k): (v if isinstance(v, dict) else {}) for k, v in meta.items()}
    else:
        raise CorruptPayload("bucket_summary.bucket_resources_meta malformato")

    return _BucketSummary(bucket=b, bucket_resources=declared_list, bucket_resources_meta=meta_map)


def _parse_manifest_file_rec(rec: dict[str, Any]) -> _ManifestFileRec | None:
    # Ignore errors and non-file records.
    rel = rec.get("rel")
    if not rel or "error" in rec:
        return None

    r = str(rel)
    arch = rec.get("archive")
    a = str(arch) if arch else None
    bucket = _as_int(rec.get("bucket") or 0, where=f"manifest.bucket malformato per {r}")
    off = _as_int(
        rec.get("archive_offset") or 0, where=f"manifest.archive_offset malformato per {r}"
    )
    ln = _as_int(
        rec.get("archive_length") or 0, where=f"manifest.archive_length malformato per {r}"
    )

    sha = rec.get("blob_sha256")
    blob_sha = None
    if sha is not None:
        if not isinstance(sha, str):
            raise CorruptPayload(f"manifest.blob_sha256 malformato per {r}")
        if sha.strip():
            if not _is_sha256_hex(sha):
                raise CorruptPayload(f"manifest.blob_sha256 malformato per {r}")
            blob_sha = sha.strip().lower()

    return _ManifestFileRec(
        rel=r,
        archive=a,
        bucket=bucket,
        archive_offset=off,
        archive_length=ln,
        blob_sha256=blob_sha,
    )


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
    """Verify a single GCA1 archive."""
    p = Path(path)
    if not p.is_file():
        raise CorruptPayload(f"GCA non trovato: {p}")

    with GCAReader(p) as rd:
        idx_raw = rd.index_raw()
        trailer = _parse_gca_trailer(idx_raw)

        if trailer and trailer.index_body_sha256:
            lines = idx_raw.splitlines(keepends=True)
            body = b"".join(lines[:-1])
            body_sha = hashlib.sha256(body).hexdigest()
            if trailer.index_body_sha256 != body_sha:
                raise HashMismatch(f"GCA index_body_sha256 mismatch: {p.name}")

        for raw in rd.iter_index():
            e = _parse_gca_index_entry(raw)
            if e is None or e.kind == "trailer":
                continue
            if e.length <= 0:
                continue

            if full:
                got, got_crc = rd.sha256_crc32_blob(e.offset, e.length, chunk_size=chunk_size)
                if e.blob_sha256 and got != e.blob_sha256:
                    raise HashMismatch(f"GCA blob hash mismatch per {e.rel}")
                if e.blob_crc32 is not None and int(got_crc) != int(e.blob_crc32):
                    raise HashMismatch(f"GCA blob CRC mismatch per {e.rel}")


def verify_packed_dir(
    output_dir: Path, *, full: bool = False, chunk_size: int = CHUNK_SIZE_DEFAULT
) -> None:
    out = Path(output_dir)
    manifest = out / "manifest.jsonl"
    if not manifest.is_file():
        raise CorruptPayload(f"manifest non trovato: {manifest}")

    needed_archives: dict[str, list[_ManifestFileRec]] = {}
    bucket_summaries: dict[int, _BucketSummary] = {}

    for rec in _iter_manifest_records(manifest):
        bs = _parse_bucket_summary(rec)
        if bs is not None:
            bucket_summaries[bs.bucket] = bs
            continue

        fr = _parse_manifest_file_rec(rec)
        if fr is None:
            continue
        if fr.archive:
            needed_archives.setdefault(fr.archive, []).append(fr)

    # Verify each archive (index/trailer + optional full hashes)
    for arch in sorted(needed_archives.keys()):
        verify_gca(out / arch, full=full, chunk_size=chunk_size)

    # Cross-check manifest vs archive index
    for arch, recs in needed_archives.items():
        p = out / arch
        with GCAReader(p) as rd:
            by_rel: dict[str, _GCAIndexEntry] = {}
            by_offlen: dict[tuple[int, int], _GCAIndexEntry] = {}

            for raw in rd.iter_index():
                e = _parse_gca_index_entry(raw)
                if e is None or e.kind == "trailer":
                    continue
                if e.rel:
                    by_rel[e.rel] = e
                by_offlen[(e.offset, e.length)] = e

            for rec in recs:
                e = by_rel.get(rec.rel)

                if e is None:
                    # some writers prefix rel (e.g. "files/a.txt") -> suffix match if unique
                    suffix = "/" + rec.rel
                    candidates = [ee for rr, ee in by_rel.items() if rr.endswith(suffix)]
                    if len(candidates) == 1:
                        e = candidates[0]

                if e is None:
                    # authoritative: offset/length match
                    e = by_offlen.get((rec.archive_offset, rec.archive_length))

                if e is None:
                    raise CorruptPayload(f"manifest punta a entry mancante in {arch}: {rec.rel}")

                # If both provide sha, they must agree
                if rec.blob_sha256 and e.blob_sha256 and rec.blob_sha256 != e.blob_sha256:
                    raise HashMismatch(f"manifest/blob_sha256 mismatch: {rec.rel}")

                if full:
                    got, got_crc = rd.sha256_crc32_blob(
                        rec.archive_offset, rec.archive_length, chunk_size=chunk_size
                    )
                    if e.blob_sha256 and got != e.blob_sha256:
                        raise HashMismatch(f"blob hash mismatch: {rec.rel}")
                    if e.blob_crc32 is not None and int(got_crc) != int(e.blob_crc32):
                        raise HashMismatch(f"blob CRC mismatch: {rec.rel}")

            # Resource checks (from bucket_summary)
            buckets_here = {rr.bucket for rr in recs}
            if buckets_here:
                res = rd.load_resources()
                for b in sorted(buckets_here):
                    bs = bucket_summaries.get(b)
                    if bs is None:
                        continue
                    for name in bs.bucket_resources:
                        if name not in res:
                            raise MissingResource(
                                f"resource mancante in {arch}: bucket={b} name={name}"
                            )
                        exp_sha = str(
                            (bs.bucket_resources_meta.get(name) or {}).get("blob_sha256") or ""
                        )
                        got_sha = str(
                            (res.get(name) or {}).get("meta", {}).get("blob_sha256") or ""
                        )
                        if exp_sha and got_sha and exp_sha != got_sha:
                            raise HashMismatch(f"resource sha mismatch: {arch} {name}")

                        if full and exp_sha:
                            res_rel = f"__res__/{name}"
                            e = by_rel.get(res_rel)
                            if e is None:
                                # fallback: some writers may store resource rel differently; off/len not known here
                                raise CorruptPayload(
                                    f"resource entry mancante in {arch}: {res_rel}"
                                )

                            if e.length > 0:
                                recomputed, recomputed_crc = rd.sha256_crc32_blob(
                                    e.offset, e.length, chunk_size=chunk_size
                                )
                                if recomputed != exp_sha:
                                    raise HashMismatch(
                                        f"resource blob hash mismatch: {arch} {name}"
                                    )
                                if e.blob_crc32 is not None and int(recomputed_crc) != int(
                                    e.blob_crc32
                                ):
                                    raise HashMismatch(f"resource blob CRC mismatch: {arch} {name}")


def verify_container_file(path: Path, *, full: bool = False) -> None:
    p = Path(path)
    if not p.is_file():
        raise CorruptPayload(f"file non trovato: {p}")
    blob = p.read_bytes()
    try:
        unpack_container_v6(blob)
    except ValueError as err:
        msg = str(err)
        if "magic" in msg:
            raise BadMagic(msg) from err
        if "version" in msg:
            raise UnsupportedVersion(msg) from err
        raise CorruptPayload(msg) from err

    if full:
        from gcc_ocf.engine.container import Engine
        from gcc_ocf.engine.container_v6 import decompress_v6

        eng = Engine.default()
        _ = decompress_v6(eng, blob, allow_extract=False)
