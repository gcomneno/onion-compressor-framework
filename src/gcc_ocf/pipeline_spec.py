"""Pipeline spec (v1) for GCC-OCF.

Goal: make encode plans reproducible and portable (CLI, dir-mode, CI).

This module intentionally stays *small* and strict:
  - JSON only
  - explicit schema id
  - unknown keys are rejected
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SPEC_ID_V1 = "gcc-ocf.pipeline.v1"


class PipelineSpecError(ValueError):
    pass


def _load_json_arg(pipeline_arg: str) -> dict[str, Any]:
    s = pipeline_arg.strip()
    if not s:
        raise PipelineSpecError("pipeline: argomento vuoto")

    if s.startswith("@"):
        p = Path(s[1:]).expanduser()
        if not p.exists() or not p.is_file():
            raise PipelineSpecError(f"pipeline: file non trovato: {p}")
        raw = p.read_text(encoding="utf-8")
        try:
            obj = json.loads(raw)
        except Exception as e:
            raise PipelineSpecError(f"pipeline: JSON non valido in {p}: {e}") from e
        if not isinstance(obj, dict):
            raise PipelineSpecError(f"pipeline: il JSON in {p} deve essere un oggetto")
        return obj

    try:
        obj = json.loads(s)
    except Exception as e:
        raise PipelineSpecError(f"pipeline: JSON inline non valido: {e}") from e
    if not isinstance(obj, dict):
        raise PipelineSpecError("pipeline: il JSON inline deve essere un oggetto")
    return obj


def _require_str(obj: dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip():
        raise PipelineSpecError(f"pipeline: campo '{key}' richiesto (string)")
    return v.strip()


def _optional_bool(obj: dict[str, Any], key: str) -> bool | None:
    if key not in obj:
        return None
    v = obj.get(key)
    if isinstance(v, bool):
        return v
    raise PipelineSpecError(f"pipeline: campo '{key}' deve essere booleano")


def _optional_stream_codecs(obj: dict[str, Any]) -> dict[str, str] | None:
    if "stream_codecs" not in obj:
        return None
    v = obj.get("stream_codecs")
    if v is None:
        return None
    if not isinstance(v, dict):
        raise PipelineSpecError("pipeline: 'stream_codecs' deve essere un oggetto {STREAM: codec}")
    out: dict[str, str] = {}
    for k, vv in v.items():
        if not isinstance(k, str) or not k.strip():
            raise PipelineSpecError("pipeline: 'stream_codecs' ha una chiave non-stringa")
        if not isinstance(vv, str) or not vv.strip():
            raise PipelineSpecError(f"pipeline: stream_codecs['{k}'] deve essere una stringa")
        out[k.strip().upper()] = vv.strip()
    return out


@dataclass(frozen=True)
class PipelineSpecV1:
    """A single lossless encode plan."""

    name: str
    layer: str
    codec: str
    stream_codecs: dict[str, str] | None = None
    mbn: bool | None = None

    def stream_codecs_spec(self) -> str | None:
        """Return the legacy 'TEXT:zlib,NUMS:num_v1' string, deterministic order."""
        if not self.stream_codecs:
            return None
        parts = [f"{k}:{self.stream_codecs[k]}" for k in sorted(self.stream_codecs.keys())]
        return ",".join(parts)


def load_pipeline_spec(pipeline_arg: str) -> PipelineSpecV1:
    """Load and validate a pipeline spec.

    pipeline_arg:
      - '@file.json'
      - inline JSON object
    """
    obj = _load_json_arg(pipeline_arg)

    # Strict key set (keep it small and stable).
    allowed = {"spec", "name", "layer", "codec", "stream_codecs", "mbn"}
    extra = sorted(set(obj.keys()) - allowed)
    if extra:
        raise PipelineSpecError(f"pipeline: chiavi non supportate: {', '.join(extra)}")

    spec_id = obj.get("spec")
    if spec_id != SPEC_ID_V1:
        raise PipelineSpecError(
            f"pipeline: spec non supportata: {spec_id!r} (attesa {SPEC_ID_V1!r})"
        )

    name = obj.get("name")
    if name is None:
        name = "pipeline"
    if not isinstance(name, str) or not name.strip():
        raise PipelineSpecError("pipeline: campo 'name' deve essere stringa")

    layer = _require_str(obj, "layer")
    codec = obj.get("codec", "zlib")
    if not isinstance(codec, str) or not codec.strip():
        raise PipelineSpecError("pipeline: campo 'codec' deve essere stringa")
    codec = codec.strip()

    stream_codecs = _optional_stream_codecs(obj)
    mbn = _optional_bool(obj, "mbn")

    return PipelineSpecV1(
        name=name.strip(), layer=layer, codec=codec, stream_codecs=stream_codecs, mbn=mbn
    )
