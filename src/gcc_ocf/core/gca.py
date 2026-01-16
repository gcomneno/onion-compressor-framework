from __future__ import annotations

"""GCA: simple bucket archive wrapper.

This is *not* a new compression container.

Each entry payload is an existing, self-contained compressed blob
(currently: container v6 bytes that may embed MBN).

Layout:
  [blob0][blob1]...[blobN-1][index_zlib][TRAILER]

TRAILER (fixed 16 bytes):
  magic      4B  b"GCA1"
  index_len  8B  uint64 little endian
  index_crc  4B  uint32 little endian over index_zlib bytes

index_zlib is zlib-compressed UTF-8 JSONL.
Each line is a dict with at least: rel, offset, length.

The archive is append-friendly: write blobs first, then index+trailer.
"""

import json
import struct
import zlib
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Dict, Iterator, List, Optional


GCA_MAGIC = b"GCA1"
TRAILER_LEN = 16


@dataclass
class GCAEntry:
    rel: str
    offset: int
    length: int
    meta: Dict


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


class GCAWriter:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp: BinaryIO = self.path.open("wb")
        self._entries: List[GCAEntry] = []
        self._closed = False

    def append(self, rel: str, blob: bytes, *, meta: Optional[Dict] = None) -> GCAEntry:
        if self._closed:
            raise ValueError("GCAWriter: append su writer chiuso")
        if meta is None:
            meta = {}
        # blob integrity
        if "blob_sha256" not in meta:
            meta = dict(meta)
            meta["blob_sha256"] = hashlib.sha256(blob).hexdigest()
        if "blob_crc32" not in meta:
            meta = dict(meta)
            meta["blob_crc32"] = _crc32(blob)
        off = int(self._fp.tell())
        self._fp.write(blob)
        ent = GCAEntry(rel=str(rel), offset=off, length=len(blob), meta=dict(meta))
        self._entries.append(ent)
        return ent

    def append_resource(self, name: str, blob: bytes, *, meta: Optional[Dict] = None) -> GCAEntry:
        """Append a bucket-level resource.

        Resources are stored as regular blobs, but with a reserved rel prefix.
        They are discoverable via load_resources().
        """
        nm = str(name).strip()
        if not nm:
            raise ValueError("GCAWriter: resource name vuoto")
        m = dict(meta or {})
        m.setdefault("kind", "resource")
        m.setdefault("res_name", nm)
        return self.append(f"__res__/{nm}", blob, meta=m)

    def close(self) -> None:
        if self._closed:
            return
        # Build JSONL index
        lines: List[str] = []
        for e in self._entries:
            d = {"rel": e.rel, "offset": int(e.offset), "length": int(e.length)}
            # include meta (may contain sha256, plan, etc.)
            for k, v in (e.meta or {}).items():
                if k in d:
                    continue
                d[k] = v
            lines.append(json.dumps(d, ensure_ascii=False))
        # index body (entries only)
        idx_body = ("\n".join(lines) + "\n").encode("utf-8")
        body_sha = hashlib.sha256(idx_body).hexdigest()
        # trailer record (included as last JSONL line)
        trailer_line = json.dumps(
            {
                "kind": "trailer",
                "schema": "gca.index_trailer.v1",
                "index_body_sha256": body_sha,
                "entries": int(len(self._entries)),
            },
            ensure_ascii=False,
        )
        idx_raw = idx_body + (trailer_line + "\n").encode("utf-8")
        idx_z = zlib.compress(idx_raw, level=9)
        idx_crc = _crc32(idx_z)

        self._fp.write(idx_z)
        trailer = GCA_MAGIC + struct.pack("<Q", len(idx_z)) + struct.pack("<I", idx_crc)
        self._fp.write(trailer)
        self._fp.flush()
        self._fp.close()
        self._closed = True

    def __enter__(self) -> "GCAWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class GCAReader:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._fp: BinaryIO = self.path.open("rb")
        self._index: Optional[List[Dict]] = None
        self._index_raw: Optional[bytes] = None

    def close(self) -> None:
        try:
            self._fp.close()
        except Exception:
            pass

    def _load_index(self) -> List[Dict]:
        if self._index is not None:
            return self._index
        self._fp.seek(0, 2)
        size = int(self._fp.tell())
        if size < TRAILER_LEN:
            raise ValueError("GCAReader: file troppo corto")
        self._fp.seek(size - TRAILER_LEN)
        trailer = self._fp.read(TRAILER_LEN)
        if len(trailer) != TRAILER_LEN:
            raise ValueError("GCAReader: trailer incompleto")
        magic = trailer[:4]
        if magic != GCA_MAGIC:
            raise ValueError("GCAReader: magic non valido")
        idx_len = struct.unpack("<Q", trailer[4:12])[0]
        idx_crc = struct.unpack("<I", trailer[12:16])[0]
        if idx_len <= 0 or idx_len > (size - TRAILER_LEN):
            raise ValueError("GCAReader: index_len non valido")
        idx_off = size - TRAILER_LEN - int(idx_len)
        self._fp.seek(idx_off)
        idx_z = self._fp.read(int(idx_len))
        if _crc32(idx_z) != int(idx_crc):
            raise ValueError("GCAReader: CRC index mismatch")
        idx_raw = zlib.decompress(idx_z)
        self._index_raw = idx_raw
        out: List[Dict] = []
        for line in idx_raw.decode("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
        self._index = out
        return out

    def index_raw(self) -> bytes:
        """Return the decompressed JSONL index bytes."""
        self._load_index()
        return bytes(self._index_raw or b"")

    def index_trailer(self) -> Optional[Dict]:
        """Return the parsed trailer record (last JSONL line) if present."""
        idx = self._load_index()
        if not idx:
            return None
        last = idx[-1]
        if isinstance(last, dict) and str(last.get("kind")) == "trailer":
            return last
        return None

    def iter_index(self) -> Iterator[Dict]:
        for e in self._load_index():
            yield e

    def load_resources(self) -> Dict[str, Dict]:
        """Load bucket-level resources.

        Returns a dict: name -> {"blob": bytes, "meta": dict}.
        """
        res: Dict[str, Dict] = {}
        for e in self._load_index():
            if not isinstance(e, dict):
                continue
            rel = str(e.get("rel", ""))
            kind = str(e.get("kind", ""))
            name = str(e.get("res_name", ""))
            if not name and rel.startswith("__res__/"):
                name = rel.split("/", 1)[1]
            if (kind == "resource") or rel.startswith("__res__/"):
                try:
                    off = int(e.get("offset") or 0)
                    ln = int(e.get("length") or 0)
                    if ln <= 0:
                        continue
                    blob = self.read_blob(off, ln)
                    meta = {k: v for k, v in e.items() if k not in ("offset", "length")}
                    res[name] = {"blob": blob, "meta": meta}
                except Exception:
                    continue
        return res

    def read_blob(self, offset: int, length: int) -> bytes:
        if length < 0 or offset < 0:
            raise ValueError("GCAReader: offset/length non validi")
        self._fp.seek(int(offset))
        blob = self._fp.read(int(length))
        if len(blob) != int(length):
            raise ValueError("GCAReader: blob troncato")
        return blob

    def sha256_blob(self, offset: int, length: int, *, chunk_size: int = 256 * 1024) -> str:
        """Compute sha256 for a blob segment without loading it all in RAM."""
        if length < 0 or offset < 0:
            raise ValueError("GCAReader: offset/length non validi")
        if chunk_size <= 0:
            chunk_size = 256 * 1024
        h = hashlib.sha256()
        self._fp.seek(int(offset))
        remaining = int(length)
        while remaining > 0:
            n = chunk_size if remaining > chunk_size else remaining
            chunk = self._fp.read(n)
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
        if remaining != 0:
            raise ValueError("GCAReader: blob troncato")
        return h.hexdigest()

    def sha256_crc32_blob(self, offset: int, length: int, *, chunk_size: int = 256 * 1024) -> tuple[str, int]:
        """Compute sha256 and crc32 for a blob segment in a single streaming pass."""
        if length < 0 or offset < 0:
            raise ValueError("GCAReader: offset/length non validi")
        if chunk_size <= 0:
            chunk_size = 256 * 1024
        h = hashlib.sha256()
        crc = 0
        self._fp.seek(int(offset))
        remaining = int(length)
        while remaining > 0:
            n = chunk_size if remaining > chunk_size else remaining
            chunk = self._fp.read(n)
            if not chunk:
                break
            h.update(chunk)
            crc = zlib.crc32(chunk, crc)
            remaining -= len(chunk)
        if remaining != 0:
            raise ValueError("GCAReader: blob troncato")
        return h.hexdigest(), (crc & 0xFFFFFFFF)

    def __enter__(self) -> "GCAReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
