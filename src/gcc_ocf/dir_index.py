from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterable

from gcc_ocf.errors import CorruptPayload

SPEC_INDEX_V1: Final[str] = "gcc-ocf.dir_bundle_index.v1"


@dataclass(frozen=True)
class DirIndexEntry:
    rel: str
    offset: int
    length: int
    sha256: str

    @staticmethod
    def from_dict(raw: Any) -> "DirIndexEntry":
        if not isinstance(raw, dict):
            raise CorruptPayload(f"bundle index entry invalida (non dict): {raw}")

        rel = raw.get("rel")
        off = raw.get("offset")
        ln = raw.get("length")
        sha = raw.get("sha256")

        if not isinstance(rel, str) or not rel:
            raise CorruptPayload(f"bundle index entry invalida (rel): {raw}")

        try:
            off_i = int(off)
            ln_i = int(ln)
        except Exception as e:
            raise CorruptPayload(f"bundle index entry invalida (offset/length): {raw}") from e

        if off_i < 0 or ln_i < 0:
            raise CorruptPayload(f"bundle index entry invalida (offset/length negative): {raw}")

        if not isinstance(sha, str) or not sha:
            raise CorruptPayload(f"bundle index entry invalida (sha256): {raw}")

        return DirIndexEntry(rel=rel, offset=off_i, length=ln_i, sha256=sha)

    def to_dict(self) -> dict[str, Any]:
        return {"rel": self.rel, "offset": self.offset, "length": self.length, "sha256": self.sha256}


@dataclass
class DirBundleIndexV1:
    """
    Layer 2: Index/Namespace for single-container directory modes.

    Stable JSON schema:
      {
        "spec": "gcc-ocf.dir_bundle_index.v1",
        "root": "<dir name>",
        "kind": "text" | "bin",
        "count": <int>,
        "files": [{"rel","offset","length","sha256"}, ...],
        "concat_sha256": "<hex>",
        "layer_used": "<str>",
        "codec_used": "<str>",
        "stream_codecs_used": "<str>"   # optional (present for text bundle)
      }
    """

    root: str
    kind: str  # "text" or "bin"
    concat_sha256: str
    layer_used: str
    codec_used: str
    files: list[DirIndexEntry]
    stream_codecs_used: str | None = None

    # --- Manual-style API ---

    def put(self, name: str, offset: int, length: int, sha256: str) -> None:
        self.files.append(DirIndexEntry(rel=name, offset=offset, length=length, sha256=sha256))

    def get(self, name: str) -> DirIndexEntry | None:
        for e in self.files:
            if e.rel == name:
                return e
        return None

    def iter_entries(self) -> Iterable[DirIndexEntry]:
        return iter(self.files)

    # --- Serialization ---

    def to_dict(self) -> dict[str, Any]:
        # Keep key order stable (human-friendly diffs)
        d: dict[str, Any] = {
            "spec": SPEC_INDEX_V1,
            "root": self.root,
            "kind": self.kind,
            "count": len(self.files),
            "files": [e.to_dict() for e in self.files],
            "concat_sha256": self.concat_sha256,
            "layer_used": self.layer_used,
            "codec_used": self.codec_used,
        }
        if self.stream_codecs_used is not None:
            d["stream_codecs_used"] = self.stream_codecs_used
        return d

    def serialize(self, *, indent: int = 2) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "DirBundleIndexV1":
        try:
            raw = json.loads(data.decode("utf-8"))
        except Exception as e:
            raise CorruptPayload(f"bundle index JSON invalido: {e}") from e
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Any) -> "DirBundleIndexV1":
        if not isinstance(raw, dict):
            raise CorruptPayload("bundle index invalido (non dict)")

        if raw.get("spec") != SPEC_INDEX_V1:
            raise CorruptPayload("bundle index spec invalida")

        root = raw.get("root")
        kind = raw.get("kind")
        concat_sha256 = raw.get("concat_sha256")
        layer_used = raw.get("layer_used")
        codec_used = raw.get("codec_used")
        files_raw = raw.get("files")

        if not isinstance(root, str) or not root:
            raise CorruptPayload("bundle index invalido (root)")
        if kind not in ("text", "bin"):
            raise CorruptPayload("bundle index invalido (kind)")
        if not isinstance(concat_sha256, str) or not concat_sha256:
            raise CorruptPayload("bundle index invalido (concat_sha256)")
        if not isinstance(layer_used, str) or not layer_used:
            raise CorruptPayload("bundle index invalido (layer_used)")
        if not isinstance(codec_used, str) or not codec_used:
            raise CorruptPayload("bundle index invalido (codec_used)")
        if not isinstance(files_raw, list):
            raise CorruptPayload("bundle index invalido (files)")

        files = [DirIndexEntry.from_dict(x) for x in files_raw]

        if "count" in raw:
            try:
                cnt = int(raw.get("count"))
            except Exception:
                raise CorruptPayload("bundle index invalido (count non int)")
            if cnt != len(files):
                raise CorruptPayload("bundle index invalido (count mismatch)")

        stream_codecs_used = raw.get("stream_codecs_used")
        if stream_codecs_used is not None and not isinstance(stream_codecs_used, str):
            raise CorruptPayload("bundle index invalido (stream_codecs_used)")

        return cls(
            root=root,
            kind=kind,
            concat_sha256=concat_sha256,
            layer_used=layer_used,
            codec_used=codec_used,
            files=files,
            stream_codecs_used=stream_codecs_used,
        )

    # --- File helpers ---

    @classmethod
    def read(cls, path: Path, *, expected_kind: str | None = None) -> "DirBundleIndexV1":
        p = Path(path)
        if not p.is_file():
            raise CorruptPayload(f"bundle index non trovato: {p}")
        idx = cls.deserialize(p.read_bytes())
        if expected_kind is not None and idx.kind != expected_kind:
            raise CorruptPayload(f"bundle index kind invalido (atteso {expected_kind}): {p}")
        return idx

    def write(self, path: Path, *, indent: int = 2) -> None:
        Path(path).write_text(self.serialize(indent=indent).decode("utf-8"), encoding="utf-8")
