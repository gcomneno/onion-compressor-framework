"""Directory pipeline spec loader/validator.

This controls directory-mode behaviour (bucketing + autopick + candidate pools + resources)
in a reproducible way.

Schema id: ``gcc-ocf.dir_pipeline.v1``

Design goals:
  - Strict: unknown keys are errors
  - Deterministic: defaults mirror legacy behaviour
  - Minimal: only the knobs we actually use today
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


SCHEMA_ID = "gcc-ocf.dir_pipeline.v1"


class DirPipelineSpecError(ValueError):
    pass


_STREAM_NAME_TO_KEY = {
    "MAIN": "MAIN",
    "TEXT": "TEXT",
    "NUMS": "NUMS",
    "IDS": "IDS",
    "TPL": "TPL",
    "META": "META",
    "CONS": "CONS",
    "VOWELS": "VOWELS",
    "MASK": "MASK",
}


@dataclass(frozen=True)
class DirAutopick:
    enabled: Optional[bool] = None
    sample_n: Optional[int] = None
    top_k: Optional[int] = None
    top_db_max: Optional[int] = None
    refresh_top: Optional[bool] = None


@dataclass(frozen=True)
class DirResourceNumDictV1:
    enabled: Optional[bool] = None
    k: Optional[int] = None


@dataclass(frozen=True)
class DirResourceTplDictV0:
    """Bucket-level shared template dictionary for tpl_lines_shared_v0 (archive-only)."""

    enabled: Optional[bool] = None
    k: Optional[int] = None


@dataclass(frozen=True)
class DirPlan:
    layer: str
    codec: str
    stream_codecs: Optional[Dict[str, str]] = None
    note: str = ""


@dataclass(frozen=True)
class DirPipelineSpec:
    spec: str
    buckets: Optional[int] = None
    archive: Optional[bool] = None
    autopick: DirAutopick = DirAutopick()
    candidate_pools: Dict[str, List[DirPlan]] = None  # type: ignore[assignment]
    num_dict_v1: DirResourceNumDictV1 = DirResourceNumDictV1()
    tpl_dict_v0: DirResourceTplDictV0 = DirResourceTplDictV0()

    def __post_init__(self) -> None:
        # dataclasses with default mutable: enforce a real dict
        if self.candidate_pools is None:
            object.__setattr__(self, "candidate_pools", {})


def _read_json_text(arg: str) -> str:
    s = arg.strip()
    if not s:
        raise DirPipelineSpecError("dir pipeline spec: input vuoto")
    if s.startswith("@"):  # @file.json
        p = Path(s[1:]).expanduser()
        if not p.is_file():
            raise DirPipelineSpecError(f"dir pipeline spec: file non trovato: {p}")
        return p.read_text(encoding="utf-8")
    return s


def _expect_type(name: str, v: Any, t: type) -> Any:
    if not isinstance(v, t):
        raise DirPipelineSpecError(f"dir pipeline spec: '{name}' deve essere {t.__name__}")
    return v


def _ensure_allowed_keys(obj_name: str, obj: Mapping[str, Any], allowed: Iterable[str]) -> None:
    allowed_set = set(allowed)
    extra = [k for k in obj.keys() if k not in allowed_set]
    if extra:
        raise DirPipelineSpecError(f"dir pipeline spec: chiavi non supportate in {obj_name}: {', '.join(sorted(extra))}")


def _parse_autopick(v: Any) -> DirAutopick:
    if v is None:
        return DirAutopick()
    _expect_type("autopick", v, dict)
    _ensure_allowed_keys(
        "autopick",
        v,
        ["enabled", "sample_n", "top_k", "top_db_max", "refresh_top"],
    )
    enabled = v.get("enabled")
    sample_n = v.get("sample_n")
    top_k = v.get("top_k")
    top_db_max = v.get("top_db_max")
    refresh_top = v.get("refresh_top")

    if enabled is not None:
        _expect_type("autopick.enabled", enabled, bool)
    if sample_n is not None:
        _expect_type("autopick.sample_n", sample_n, int)
        if sample_n < 1 or sample_n > 8:
            raise DirPipelineSpecError("dir pipeline spec: autopick.sample_n deve essere tra 1 e 8")
    if top_k is not None:
        _expect_type("autopick.top_k", top_k, int)
    if top_db_max is not None:
        _expect_type("autopick.top_db_max", top_db_max, int)
        if top_db_max < 1:
            raise DirPipelineSpecError("dir pipeline spec: autopick.top_db_max deve essere >= 1")
    if refresh_top is not None:
        _expect_type("autopick.refresh_top", refresh_top, bool)
    return DirAutopick(enabled=enabled, sample_n=sample_n, top_k=top_k, top_db_max=top_db_max, refresh_top=refresh_top)


def _parse_plan(obj: Any) -> DirPlan:
    _expect_type("plan", obj, dict)
    _ensure_allowed_keys("plan", obj, ["layer", "codec", "stream_codecs", "note"])
    layer = obj.get("layer")
    codec = obj.get("codec")
    if not isinstance(layer, str) or not layer.strip():
        raise DirPipelineSpecError("dir pipeline spec: plan.layer obbligatorio")
    if not isinstance(codec, str) or not codec.strip():
        raise DirPipelineSpecError("dir pipeline spec: plan.codec obbligatorio")
    note = obj.get("note") or ""
    if not isinstance(note, str):
        raise DirPipelineSpecError("dir pipeline spec: plan.note deve essere string")
    sc = obj.get("stream_codecs")
    sc_out: Optional[Dict[str, str]] = None
    if sc is not None:
        _expect_type("plan.stream_codecs", sc, dict)
        sc_out = {}
        for k, v in sc.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise DirPipelineSpecError("dir pipeline spec: stream_codecs deve essere mappa string->string")
            k2 = k.strip().upper()
            if k2 not in _STREAM_NAME_TO_KEY:
                raise DirPipelineSpecError(f"dir pipeline spec: stream name non supportato: {k}")
            if not v.strip():
                raise DirPipelineSpecError("dir pipeline spec: codec vuoto in stream_codecs")
            sc_out[k2] = v.strip()
    return DirPlan(layer=layer.strip(), codec=codec.strip(), stream_codecs=sc_out, note=note)


def _parse_candidate_pools(v: Any) -> Dict[str, List[DirPlan]]:
    if v is None:
        return {}
    _expect_type("candidate_pools", v, dict)
    pools: Dict[str, List[DirPlan]] = {}
    for bt, lst in v.items():
        if not isinstance(bt, str) or not bt.strip():
            raise DirPipelineSpecError("dir pipeline spec: candidate_pools keys devono essere string")
        _expect_type(f"candidate_pools[{bt}]", lst, list)
        plans = [_parse_plan(x) for x in lst]
        pools[bt.strip()] = plans
    return pools


def _parse_resources(v: Any) -> Tuple[DirResourceNumDictV1, DirResourceTplDictV0]:
    if v is None:
        return DirResourceNumDictV1(), DirResourceTplDictV0()
    _expect_type("resources", v, dict)
    _ensure_allowed_keys("resources", v, ["num_dict_v1", "tpl_dict_v0"])
    # num_dict_v1
    nd = v.get("num_dict_v1")
    nd_out = DirResourceNumDictV1()
    if nd is not None:
        _expect_type("resources.num_dict_v1", nd, dict)
        _ensure_allowed_keys("resources.num_dict_v1", nd, ["enabled", "k"])
        enabled = nd.get("enabled")
        k = nd.get("k")
        if enabled is not None:
            _expect_type("resources.num_dict_v1.enabled", enabled, bool)
        if k is not None:
            _expect_type("resources.num_dict_v1.k", k, int)
            if k < 0:
                raise DirPipelineSpecError("dir pipeline spec: resources.num_dict_v1.k deve essere >= 0")
        nd_out = DirResourceNumDictV1(enabled=enabled, k=k)

    # tpl_dict_v0
    td = v.get("tpl_dict_v0")
    td_out = DirResourceTplDictV0()
    if td is not None:
        _expect_type("resources.tpl_dict_v0", td, dict)
        _ensure_allowed_keys("resources.tpl_dict_v0", td, ["enabled", "k"])
        enabled = td.get("enabled")
        k = td.get("k")
        if enabled is not None:
            _expect_type("resources.tpl_dict_v0.enabled", enabled, bool)
        if k is not None:
            _expect_type("resources.tpl_dict_v0.k", k, int)
            if k < 0:
                raise DirPipelineSpecError("dir pipeline spec: resources.tpl_dict_v0.k deve essere >= 0")
        td_out = DirResourceTplDictV0(enabled=enabled, k=k)

    return nd_out, td_out


def load_dir_pipeline_spec(arg: str) -> DirPipelineSpec:
    """Load and validate a dir pipeline spec from '@file.json' or inline JSON."""
    text = _read_json_text(arg)
    try:
        obj = json.loads(text)
    except Exception as e:
        raise DirPipelineSpecError(f"dir pipeline spec: JSON invalido: {e}") from e
    _expect_type("root", obj, dict)
    _ensure_allowed_keys("root", obj, ["spec", "buckets", "archive", "autopick", "candidate_pools", "resources"])

    spec = obj.get("spec")
    if spec != SCHEMA_ID:
        raise DirPipelineSpecError(f"dir pipeline spec: spec deve essere '{SCHEMA_ID}'")

    buckets = obj.get("buckets")
    if buckets is not None:
        _expect_type("buckets", buckets, int)
        if buckets <= 0:
            raise DirPipelineSpecError("dir pipeline spec: buckets deve essere > 0")

    archive = obj.get("archive")
    if archive is not None:
        _expect_type("archive", archive, bool)

    autopick = _parse_autopick(obj.get("autopick"))
    pools = _parse_candidate_pools(obj.get("candidate_pools"))
    nd, td = _parse_resources(obj.get("resources"))

    return DirPipelineSpec(
        spec=spec,
        buckets=buckets,
        archive=archive,
        autopick=autopick,
        candidate_pools=pools,
        num_dict_v1=nd,
        tpl_dict_v0=td,
    )
