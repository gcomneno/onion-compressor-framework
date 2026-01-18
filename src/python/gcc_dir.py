#!/usr/bin/env python3
"""Directory helpers for huffman-compressor (framework).

This is deliberately *not* a new compression container format.
It can write either:
  - one compressed file per input file (legacy behaviour)
  - OR a per-bucket archive (.gca) that concatenates many v6 blobs
    with an index (recommended default)

Commands:
  packdir   <input_dir> <output_dir> [buckets]
  unpackdir <output_dir> <restore_dir>

Notes:
  - Lossless only.
  - Uses container v6 + payload MBN (same as CLI c7/d7) with Engine.default().
  - Bucketing uses simhash64 + optional Turbo-Bucketizer plugin via TB_MODULE.

Plugin contract (optional):
  - set env TB_MODULE to a python module import path
  - module may implement: bucket_for_fingerprint(simhash64: int, buckets: int) -> int

Pipeline selection (macro-step):
  - Bucket-level *mini autopick* over a small sample.
  - Candidates (subset chosen based on bucket signals):
      - bytes + (zstd_tight|zlib)
      - vc0 + (zstd_tight|zlib)                          [text-ish buckets]
      - split_text_nums + (zstd_tight|zlib) + num_v1     [text-ish buckets]
      - tpl_lines_v0 + (zstd_tight|zlib) + num_v1 (IDS/NUMS) [text-ish buckets]
  - Autopick is deterministic and capped.

Autopick env knobs (optional):
  - GCC_AUTOPICK=0   disable autopick (fallback to old heuristic)
  - GCC_AUTOPICK_N=3 sample size per bucket

The point of this tool is to give the Analyzer a concrete "execute plan" step,
without inventing new containers or breaking bench/roundtrip.
"""

from __future__ import annotations

try:
    import zstandard as zstd  # type: ignore
except Exception:  # pragma: no cover
    zstd = None

import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gcc_ocf.analyzer.bucketize import bucketize_records, iter_files
from gcc_ocf.analyzer.simhash import fingerprint_bytes
from gcc_ocf.core.codec_num_v1 import CodecNumV1
from gcc_ocf.core.gca import GCAReader, GCAWriter
from gcc_ocf.core.mbn_bundle import (
    ST_CONS,
    ST_IDS,
    ST_MAIN,
    ST_MASK,
    ST_META,
    ST_NUMS,
    ST_TEXT,
    ST_TPL,
    ST_VOWELS,
)
from gcc_ocf.core.num_stream import decode_ints, encode_ints
from gcc_ocf.dir_pipeline_spec import DirPipelineSpec
from gcc_ocf.engine.container import Engine
from gcc_ocf.engine.container_v6 import compress_v6_mbn, decompress_v6
from gcc_ocf.layers.tpl_lines_shared_v0 import (
    LayerTplLinesSharedV0,
    pack_tpl_dict_v0_resource,
    unpack_tpl_dict_v0_resource,
)
from gcc_ocf.layers.tpl_lines_v0 import LayerTplLinesV0
from gcc_ocf.layers.tpl_lines_v0 import _unpack_templates as _tpl_v0_unpack_templates

MANIFEST_NAME = "manifest.jsonl"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"  # sha256(b"")
ARCHIVE_PREFIX = "bucket_"
ARCHIVE_SUFFIX = ".gca"
TOP_PIPELINES_REL = Path("tools") / "top_pipelines.json"
TOP_K_DEFAULT = 2
TOP_DB_MAX_DEFAULT = 12

# Step 7: performance knobs
SPOOL_THRESHOLD_BYTES = 4 * 1024 * 1024  # 4 MiB
ANALYZE_MAX_BYTES_DEFAULT = 256 * 1024  # 256 KiB


ARCHIVE_DEFAULT = True
ARCHIVE_ENV = "GCC_ARCHIVE"  # set to 0 to disable archives

NUM_DICT_ENV_K = "GCC_NUM_DICT_K"  # size of bucket-level numeric dict for num_v1
NUM_DICT_NAME = "num_dict_v1"

NUM_DICT_K_ENV = "GCC_NUM_DICT_K"
NUM_DICT_K_DEFAULT = 64
NUM_DICT_RES_NAME = "num_dict_v1"

TPL_DICT_NAME = "tpl_dict_v0"
TPL_DICT_ENV_K = "GCC_TPL_DICT_K"
TPL_DICT_K_DEFAULT = 128


@dataclass(frozen=True)
class Plan:
    layer_id: str
    codec_text: str  # codec used for MAIN/TEXT and other non-numeric streams
    stream_codecs: dict[int, str] | None = None  # optional overrides by stream type
    note: str = ""


def _repo_root() -> Path:
    """Best-effort repo root discovery.

    Historically this lived in ``src/python/gcc_dir.py``; in GCC-OCF it lives under
    ``src/gcc_ocf/legacy``. We cannot rely on a fixed ``parents[N]``.
    """
    cur = Path(__file__).resolve()
    for parent in cur.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    # Fallback: assume ``.../src/gcc_ocf/legacy/gcc_dir.py`` layout
    return cur.parents[3]


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _have_zstd() -> bool:
    return zstd is not None


def _resolve_codec_id(codec_id: str, *, have_zstd: bool) -> str:
    cid = str(codec_id)
    if cid in ("zstd", "zstd_tight") and not have_zstd:
        return "zlib"
    return cid


def _resolve_stream_codecs(sc: dict[int, str] | None, *, have_zstd: bool) -> dict[int, str] | None:
    if not sc:
        return None
    out = {}
    for st, cid in sc.items():
        out[int(st)] = _resolve_codec_id(str(cid), have_zstd=have_zstd)
    return out


def _cpu_penalty(
    plan: Plan, *, resolved_codec_text: str, resolved_sc: dict[int, str] | None
) -> float:
    """Small deterministic penalty (tie-break + avoid fragile expensive plans)."""
    layer_p = {
        "bytes": 0.000,
        "vc0": 0.010,
        "split_text_nums": 0.020,
        "tpl_lines_v0": 0.030,
        "tpl_lines_shared_v0": 0.030,
    }.get(plan.layer_id, 0.015)
    codec_p = 0.0
    if resolved_codec_text == "zstd_tight":
        codec_p += 0.005
    # num_v1 penalty if present in any numeric stream
    uses_num = any(v == "num_v1" for v in (resolved_sc or {}).values()) or plan.layer_id in (
        "split_text_nums",
        "tpl_lines_v0",
        "tpl_lines_shared_v0",
    )
    num_p = 0.005 if uses_num else 0.0
    return float(layer_p + codec_p + num_p)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _engine_with_num_shared(base: Engine, dict_vals: list[int], tag8: bytes) -> Engine:
    """Return a new Engine whose num_v1 codec is configured with a shared dict."""
    eng = Engine(layers=base.layers, codecs=dict(base.codecs))
    c = CodecNumV1()
    c.set_shared_dict(dict_vals, tag8=tag8)
    eng.codecs["num_v1"] = c
    return eng


def _engine_with_tpl_shared(base: Engine, templates: list[list[bytes]], tag8: bytes) -> Engine:
    """Return a new Engine whose tpl_lines_shared_v0 layer is configured with a shared dict."""
    layers = dict(base.layers)
    lyr = LayerTplLinesSharedV0()
    lyr.set_shared_dict(templates, tag8=tag8)
    layers[lyr.id] = lyr
    return Engine(layers=layers, codecs=dict(base.codecs))


def _numeric_density(data: bytes) -> float:
    if not data:
        return 0.0
    # density of ASCII digits (0-9) among bytes
    digits = sum(1 for b in data if 48 <= b <= 57)
    return digits / len(data)


def _bucket_signals(records: list[dict]) -> tuple[bool, float]:
    """Return (textish, avg_numeric_density) for a bucket."""
    sample = records[:10]
    if not sample:
        return False, 0.0
    is_text_votes = sum(1 for r in sample if r.get("is_text"))
    dens = sum(float(r.get("numeric_density", 0.0)) for r in sample) / len(sample)
    textish = is_text_votes >= max(1, len(sample) // 2)
    return textish, dens


BT_TEXTISH = "textish"
BT_MIXED_TEXT_NUMS = "mixed_text_nums"
BT_BINARYISH = "binaryish"


_STREAM_NAME_TO_STYPE = {
    "MAIN": ST_MAIN,
    "TEXT": ST_TEXT,
    "NUMS": ST_NUMS,
    "IDS": ST_IDS,
    "TPL": ST_TPL,
    "META": ST_META,
    "CONS": ST_CONS,
    "VOWELS": ST_VOWELS,
    "MASK": ST_MASK,
}


def _plans_from_dir_spec_pool(dir_spec: DirPipelineSpec, bucket_type: str) -> list[Plan] | None:
    """Convert a DirPipelineSpec candidate pool into internal Plan objects.

    Returns None if the spec does not define a pool for the bucket type.
    """
    pools = getattr(dir_spec, "candidate_pools", {}) or {}
    raw = pools.get(bucket_type)
    if raw is None:
        return None
    plans: list[Plan] = []
    for p in raw:
        sc = None
        if getattr(p, "stream_codecs", None):
            sc = {}
            for name, cid in (p.stream_codecs or {}).items():
                st = _STREAM_NAME_TO_STYPE.get(str(name).upper())
                if st is None:
                    continue
                sc[int(st)] = str(cid)
        plans.append(
            Plan(
                layer_id=p.layer,
                codec_text=p.codec,
                stream_codecs=sc,
                note=getattr(p, "note", "") or "",
            )
        )
    return plans


def _shannon_entropy(data: bytes) -> float:
    if not data:
        return 0.0
    counts = [0] * 256
    for b in data:
        counts[b] += 1
    n = float(len(data))
    h = 0.0
    for c in counts:
        if c:
            p = c / n
            h -= p * math.log2(p)
    return h


def _bucket_metrics(
    records: list[dict], *, max_files: int = 4, max_per_file: int = 65536
) -> dict[str, float]:
    # deterministic: prefer larger files, tie-break by rel/path
    ok = [r for r in records if r.get("path") and "error" not in r and int(r.get("size", 0)) > 0]
    ok.sort(
        key=lambda r: (-int(r.get("size", 0)), str(r.get("rel", r.get("path", "")))),
    )
    buf = bytearray()
    digit = 0
    nul = 0
    printable = 0
    nl = 0
    for r in ok[:max_files]:
        try:
            data = Path(str(r["path"])).read_bytes()
        except Exception:
            continue
        chunk = data[: max(0, int(max_per_file))]
        buf += chunk
        for b in chunk:
            if b == 0:
                nul += 1
            if 48 <= b <= 57:
                digit += 1
            if b in (9, 10, 13) or 32 <= b <= 126:
                printable += 1
            if b == 10:
                nl += 1
    data2 = bytes(buf)
    n = float(len(data2))
    if n <= 0:
        return {
            "entropy": 0.0,
            "null_ratio": 0.0,
            "printable_ratio": 0.0,
            "digit_ratio": 0.0,
            "newline_density": 0.0,
            "utf8_ok": 0.0,
        }
    try:
        data2.decode("utf-8")
        utf8_ok = 1.0
    except Exception:
        utf8_ok = 0.0
    return {
        "entropy": float(_shannon_entropy(data2)),
        "null_ratio": float(nul / n),
        "printable_ratio": float(printable / n),
        "digit_ratio": float(digit / n),
        "newline_density": float(nl / n),
        "utf8_ok": float(utf8_ok),
    }


def _bucket_type(records: list[dict]) -> tuple[str, dict[str, float]]:
    """Bucket type classification (v2).

    Decision 1B: use entropy/null/printable/utf8 + digit ratio.
    """
    m = _bucket_metrics(records)
    # binary-ish if there's a non-trivial amount of NULs OR it's high-entropy and not texty
    if m.get("null_ratio", 0.0) > 0.01:
        return BT_BINARYISH, m
    if (
        m.get("entropy", 0.0) > 6.6
        and m.get("printable_ratio", 0.0) < 0.65
        and m.get("utf8_ok", 0.0) < 0.5
    ):
        return BT_BINARYISH, m
    if m.get("digit_ratio", 0.0) >= 0.10:
        return BT_MIXED_TEXT_NUMS, m
    return BT_TEXTISH, m


def _plan_uses_num_v1(plan: Plan, *, resolved_sc: dict[int, str] | None) -> bool:
    sc = resolved_sc or {}
    if any(v == "num_v1" for v in sc.values()):
        return True
    # implicit default for known layers
    if plan.layer_id in ("split_text_nums", "tpl_lines_v0", "tpl_lines_shared_v0"):
        return True
    return False


def _extract_num_stream_ints(
    eng: Engine, layer_id: str, data: bytes, *, want_ids: bool
) -> list[int]:
    """Extract numeric ints from a layer's raw streams.

    This is lossless and deterministic; used only to build bucket-level dicts.
    """
    layer = eng.layers.get(layer_id)
    if layer is None:
        return []
    try:
        symbols, _meta = layer.encode(data)
    except Exception:
        return []

    ints: list[int] = []
    # Layer-specific stream layout
    if layer_id == "split_text_nums":
        if isinstance(symbols, tuple) and len(symbols) == 2:
            _text, nums = symbols
            ints.extend(decode_ints(nums))
        return ints
    if layer_id in ("tpl_lines_v0", "tpl_lines_shared_v0"):
        if isinstance(symbols, tuple) and len(symbols) == 3:
            _tpl, ids, nums = symbols
            if want_ids:
                ints.extend(decode_ints(ids))
            ints.extend(decode_ints(nums))
        return ints
    return []


def _build_bucket_num_dict(
    eng: Engine,
    records: list[dict],
    plan: Plan,
    *,
    k: int,
) -> tuple[list[int], bytes]:
    """Build a deterministic top-K numeric dictionary for the bucket.

    Returns (dict_vals, tag8).
    """
    # Decide whether IDS stream is numeric-coded by num_v1
    sc = plan.stream_codecs or {}
    want_ids = bool(sc.get(ST_IDS) == "num_v1")

    freq: dict[int, int] = {}
    for r in records:
        pth = r.get("path")
        if not pth:
            continue
        try:
            data = Path(pth).read_bytes()
        except Exception:
            continue
        ints = _extract_num_stream_ints(eng, plan.layer_id, data, want_ids=want_ids)
        for n in ints:
            freq[n] = freq.get(n, 0) + 1

    if not freq:
        return [], b""

    ordered = sorted(freq.items(), key=lambda kv: (-kv[1], abs(kv[0]), kv[0]))
    dict_vals = [int(x) for x, _ in ordered[: max(0, int(k))]]
    # keep only meaningful dicts
    if len(dict_vals) < 4:
        return [], b""
    tag8 = CodecNumV1.dict_tag8(dict_vals)
    return dict_vals, tag8


def _build_bucket_tpl_dict(
    records: list[dict],
    *,
    k: int,
) -> tuple[list[list[bytes]], bytes, bytes, dict[str, Any]]:
    """Build a deterministic top-K template dictionary for the bucket.

    Implementation: run tpl_lines_v0.encode on each file and count template usage
    by lines (IDS stream). Then pick the top-K templates by frequency.

    Returns (templates, tag8, blob, meta).
    """
    freq: dict[tuple[bytes, ...], int] = {}
    layer = LayerTplLinesV0()

    for r in records:
        pth = r.get("path")
        if not pth:
            continue
        try:
            data = Path(pth).read_bytes()
        except Exception:
            continue
        try:
            (tpl_raw, ids_raw, _nums_raw), _meta = layer.encode(data)
            templates = _tpl_v0_unpack_templates(tpl_raw)
            ids = decode_ints(ids_raw)
            for tid in ids:
                if tid < 0 or tid >= len(templates):
                    continue
                key = tuple(templates[int(tid)])
                freq[key] = freq.get(key, 0) + 1
        except Exception:
            continue

    if not freq:
        return [], b"", b"", {}

    ordered = sorted(freq.items(), key=lambda kv: (-kv[1], len(kv[0]), b"".join(kv[0])[:32]))
    picked = [list(k) for k, _ in ordered[: max(0, int(k))]]
    if len(picked) < 4:
        return [], b"", b"", {}

    blob, meta = pack_tpl_dict_v0_resource(picked)
    tag8 = hashlib.sha256(blob).digest()[:8]
    return picked, tag8, blob, meta


def _plan_to_dict(p: Plan) -> dict:
    sc = None
    if p.stream_codecs:
        # json keys must be strings
        sc = {str(int(k)): str(v) for k, v in p.stream_codecs.items()}
    return {
        "layer_id": p.layer_id,
        "codec_text": p.codec_text,
        "stream_codecs": sc,
        "note": p.note,
    }


def _plan_from_dict(d: dict) -> Plan:
    sc_in = d.get("stream_codecs")
    sc: dict[int, str] | None = None
    if isinstance(sc_in, dict):
        sc = {int(k): str(v) for k, v in sc_in.items()}
    return Plan(
        layer_id=str(d.get("layer_id", "bytes")),
        codec_text=str(d.get("codec_text", "raw")),
        stream_codecs=sc,
        note=str(d.get("note", "")),
    )


def _load_top_db(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_top_db(path: Path, db: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(db, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _bootstrap_plans(bucket_type: str, *, have_zstd: bool) -> list[Plan]:
    """Return a small bootstrap set (<= TOP_K_DEFAULT).

    Used when no TOP entries exist yet for a bucket type.
    """
    codec = "zstd_tight" if have_zstd else "zlib"

    if bucket_type == BT_BINARYISH:
        return [Plan(layer_id="bytes", codec_text=codec, note="bootstrap:bytes")]

    if bucket_type == BT_TEXTISH:
        # Step4 3A: textish -> split_text_nums primary, bytes fallback
        return [
            Plan(
                layer_id="split_text_nums",
                codec_text=codec,
                stream_codecs={ST_TEXT: codec, ST_NUMS: "num_v1"},
                note="bootstrap:split_text_nums",
            ),
            Plan(layer_id="bytes", codec_text=codec, note="bootstrap:bytes"),
        ]

    # mixed_text_nums
    return [
        Plan(
            layer_id="tpl_lines_shared_v0",
            codec_text=codec,
            stream_codecs={ST_TPL: codec, ST_IDS: "num_v1", ST_NUMS: "num_v1"},
            note="bootstrap:tpl_lines_shared_v0",
        ),
        Plan(
            layer_id="tpl_lines_v0",
            codec_text=codec,
            stream_codecs={ST_TPL: codec, ST_IDS: "num_v1", ST_NUMS: "num_v1"},
            note="bootstrap:tpl_lines_v0",
        ),
    ]


def _plan_sig(p: Plan) -> tuple[str, str, tuple[tuple[int, str], ...]]:
    """Signature used for dedup + diversity checks (note excluded)."""

    sc = tuple(sorted((p.stream_codecs or {}).items()))
    return (p.layer_id, p.codec_text, sc)


def _div_rank(a: Plan, b: Plan) -> int:
    """Diversity rank (higher is better).

    We prefer picking a 2nd candidate that is meaningfully different from the 1st,
    so "TOP-2" is not just two near-identical pipelines.
    """

    if a.layer_id != b.layer_id:
        return 3
    if (a.stream_codecs or {}) != (b.stream_codecs or {}):
        return 2
    if a.codec_text != b.codec_text:
        return 1
    return 0


def _pick_top_diverse(plans_sorted: list[Plan], *, top_k: int) -> list[Plan]:
    if not plans_sorted:
        return []
    if top_k <= 1 or len(plans_sorted) == 1:
        return [plans_sorted[0]]

    first = plans_sorted[0]
    # Choose the earliest plan (best score order) that has the best diversity rank.
    best2: Plan | None = None
    best_rank = -1
    for p in plans_sorted[1:]:
        r = _div_rank(first, p)
        if r > best_rank:
            best_rank = r
            best2 = p
            if r == 3:
                break  # can't do better than different layer

    if best2 is None:
        best2 = plans_sorted[1]

    out = [first, best2]
    return out[:top_k]


def _top_candidates(
    db: dict, bucket_type: str, *, have_zstd: bool, top_k: int, top_db_max: int
) -> list[Plan]:
    """Return up to top_k candidates, preferring diversity.

    The TOP database may store more than top_k entries; we scan the best ones
    and then pick a diverse top_k subset.
    """

    entries = db.get(bucket_type)
    plans: list[Plan] = []
    if isinstance(entries, list):
        for e in entries[: max(1, top_db_max)]:
            if not isinstance(e, dict):
                continue
            pd = e.get("plan")
            if isinstance(pd, dict):
                plans.append(_plan_from_dict(pd))

    # Dedup by signature, keep order (already score-sorted in DB)
    uniq: list[Plan] = []
    seen = set()
    for p in plans:
        sig = _plan_sig(p)
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append(p)

    if uniq:
        return _pick_top_diverse(uniq, top_k=top_k)

    # bootstrap when empty
    bs = _bootstrap_plans(bucket_type, have_zstd=have_zstd)
    return bs[:top_k]


def _update_top_db(
    db: dict, bucket_type: str, plan: Plan, ratio: float, *, top_db_max: int
) -> None:
    """Update db in-place with the observed ratio for a plan.

    We keep the best (lowest) ratio observed for each plan.
    """
    key = json.dumps(_plan_to_dict(plan), sort_keys=True)
    lst = db.get(bucket_type)
    if not isinstance(lst, list):
        lst = []

    # find existing
    found = None
    for e in lst:
        if isinstance(e, dict) and e.get("key") == key:
            found = e
            break

    if found is None:
        found = {"key": key, "plan": _plan_to_dict(plan), "score": float(ratio), "seen": 1}
        lst.append(found)
    else:
        found["seen"] = int(found.get("seen", 0)) + 1
        prev = float(found.get("score", ratio))
        found["score"] = min(prev, float(ratio))

    # sort ascending by score
    lst.sort(key=lambda e: float(e.get("score", 1e9)) if isinstance(e, dict) else 1e9)
    db[bucket_type] = lst[: max(1, top_db_max)]


def _candidate_plans(*, textish: bool, have_zstd: bool) -> list[Plan]:
    plans: list[Plan] = []

    # Always-available baselines (no external deps)
    plans.append(Plan(layer_id="bytes", codec_text="zlib", note="bytes+zlib"))
    if have_zstd:
        plans.append(Plan(layer_id="bytes", codec_text="zstd_tight", note="bytes+zstd_tight"))

    if textish:
        # vc0: classic vowel/consonant split (still lossless)
        plans.append(Plan(layer_id="vc0", codec_text="zlib", note="vc0+zlib"))
        if have_zstd:
            plans.append(Plan(layer_id="vc0", codec_text="zstd_tight", note="vc0+zstd_tight"))

        # split_text_nums: semantic split, numeric stream gets num_v1
        plans.append(
            Plan(
                layer_id="split_text_nums",
                codec_text="zstd_tight" if have_zstd else "zlib",
                stream_codecs={ST_TEXT: ("zstd_tight" if have_zstd else "zlib"), ST_NUMS: "num_v1"},
                note="split_text_nums+(TEXT codec)+num_v1",
            )
        )

        # tpl_lines_v0: line template mining, numeric streams use num_v1
        plans.append(
            Plan(
                layer_id="tpl_lines_shared_v0",
                codec_text="zstd_tight" if have_zstd else "zlib",
                stream_codecs={
                    ST_TPL: ("zstd_tight" if have_zstd else "zlib"),
                    ST_IDS: "num_v1",
                    ST_NUMS: "num_v1",
                },
                note="tpl_lines_shared_v0+(TPL codec)+num_v1",
            )
        )

        # tpl_lines_v0: self-contained dictionary
        plans.append(
            Plan(
                layer_id="tpl_lines_v0",
                codec_text="zstd_tight" if have_zstd else "zlib",
                stream_codecs={
                    ST_TPL: ("zstd_tight" if have_zstd else "zlib"),
                    ST_IDS: "num_v1",
                    ST_NUMS: "num_v1",
                },
                note="tpl_lines_v0+(TPL codec)+num_v1",
            )
        )

    # De-duplicate (layer_id, codec_text, stream_codecs)
    uniq: list[Plan] = []
    seen = set()
    for p in plans:
        key = (p.layer_id, p.codec_text, tuple(sorted((p.stream_codecs or {}).items())))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq


def _sample_records_for_autopick(records: list[dict], *, n: int) -> list[dict]:
    # prefer larger files: overhead dominates on tiny files
    ok = [r for r in records if r.get("path") and "error" not in r and int(r.get("size", 0)) > 0]
    ok.sort(key=lambda r: int(r.get("size", 0)), reverse=True)
    return ok[:n]


def _try_plan(
    eng: Engine, sample: list[dict], plan: Plan, *, have_zstd: bool
) -> tuple[bool, int, int, str]:
    in_total = 0
    out_total = 0
    for r in sample:
        p = Path(r["path"])
        data = p.read_bytes()
        in_total += len(data)
        codec_text = _resolve_codec_id(plan.codec_text, have_zstd=have_zstd)
        sc = _resolve_stream_codecs(plan.stream_codecs, have_zstd=have_zstd)
        blob = compress_v6_mbn(
            eng, data, layer_id=plan.layer_id, codec_id=codec_text, stream_codecs=sc
        )
        out_total += len(blob)
    return True, in_total, out_total, ""


def _choose_plan_for_bucket(
    eng: Engine,
    records: list[dict],
    *,
    bucket_type: str,
    top_db: dict,
    top_k: int,
    top_db_max: int,
    dir_spec: DirPipelineSpec | None = None,
) -> tuple[Plan, Plan | None, list[dict]]:
    """Choose per-bucket plans (chosen + runner-up) and emit a scored report.

    Step 4:
      - candidate pool is TOP-only by default (K=2) unless refresh_top is enabled
      - scoring = ratio + cpu_penalty
      - runner-up is chosen for diversity
      - zstd codecs are resolved to zlib if zstandard is not installed
    """

    have_zstd = _have_zstd()

    # Determine whether archive mode is enabled (resources only exist in archive mode).
    use_archive = _env_bool(ARCHIVE_ENV, ARCHIVE_DEFAULT)
    if dir_spec is not None and dir_spec.archive is not None:
        use_archive = bool(dir_spec.archive)

    # tpl_dict_v0 knobs (used to score tpl_lines_shared_v0 as *shared*, not self-contained).
    tpl_dict_enabled = True
    tpl_dict_k = max(0, _env_int(TPL_DICT_ENV_K, TPL_DICT_K_DEFAULT))
    if dir_spec is not None:
        if dir_spec.tpl_dict_v0.enabled is not None:
            tpl_dict_enabled = bool(dir_spec.tpl_dict_v0.enabled)
        if dir_spec.tpl_dict_v0.k is not None:
            tpl_dict_k = max(0, int(dir_spec.tpl_dict_v0.k))

    def _resolved_plan(p: Plan) -> Plan:
        ct = _resolve_codec_id(p.codec_text, have_zstd=have_zstd)
        sc = _resolve_stream_codecs(p.stream_codecs, have_zstd=have_zstd)
        return Plan(layer_id=p.layer_id, codec_text=ct, stream_codecs=sc, note=p.note)

    # Fallback heuristic (legacy)
    def _heuristic() -> tuple[Plan, Plan | None, list[dict]]:
        codec = "zstd_tight" if have_zstd else "zlib"
        # conservative default
        p = Plan(layer_id="bytes", codec_text=codec, note="heuristic:bytes")
        return _resolved_plan(p), None, []

    # Dir spec overrides env knobs.
    spec_enabled = None
    if dir_spec is not None:
        spec_enabled = dir_spec.autopick.enabled

    if spec_enabled is False:
        return _heuristic()
    if spec_enabled is None and not _env_bool("GCC_AUTOPICK", True):
        return _heuristic()

    # sample size
    if dir_spec is not None and dir_spec.autopick.sample_n is not None:
        n = max(1, min(8, int(dir_spec.autopick.sample_n)))
    else:
        n = max(1, min(8, _env_int("GCC_AUTOPICK_N", 3)))

    # Candidate pool selection
    refresh = None
    if dir_spec is not None and dir_spec.autopick.refresh_top is not None:
        refresh = bool(dir_spec.autopick.refresh_top)

    candidates = _plans_from_dir_spec_pool(dir_spec, bucket_type) if dir_spec is not None else None
    if candidates is None:
        refresh2 = bool(refresh) if refresh is not None else _env_bool("GCC_REFRESH_TOP", False)
        if refresh2:
            candidates = _candidate_plans(
                textish=(bucket_type != BT_BINARYISH), have_zstd=have_zstd
            )
        else:
            candidates = _top_candidates(
                top_db, bucket_type, have_zstd=have_zstd, top_k=top_k, top_db_max=top_db_max
            )

    sample = _sample_records_for_autopick(records, n=n)
    if not sample:
        return _heuristic()

    report: list[dict] = []
    scored: list[tuple[float, float, float, Plan]] = []  # (score, ratio, penalty, plan_resolved)

    for p in candidates:
        p_res = _resolved_plan(p)
        try:
            eng2 = eng
            # Score tpl_lines_shared_v0 using a sample-derived shared dict (approximation).
            # This matches real archive-mode behaviour (bucket-level tpl_dict_v0),
            # and prevents shared-vs-selfcontained from being unfairly compared.
            if (
                use_archive
                and tpl_dict_enabled
                and p_res.layer_id == "tpl_lines_shared_v0"
                and len(sample) >= 2
            ):
                try:
                    templates, tag8, _blob, _meta = _build_bucket_tpl_dict(
                        sample, k=int(tpl_dict_k)
                    )
                    if templates and tag8:
                        eng2 = _engine_with_tpl_shared(eng2, templates, tag8)
                except Exception:
                    # If building a sample dict fails, fall back to self-contained scoring.
                    pass

            ok, in_total, out_total, err = _try_plan(eng2, sample, p_res, have_zstd=have_zstd)
            if not ok or in_total <= 0:
                raise RuntimeError(err or "try_plan failed")
            ratio = float(out_total / in_total)
            pen = float(
                _cpu_penalty(
                    p_res, resolved_codec_text=p_res.codec_text, resolved_sc=p_res.stream_codecs
                )
            )
            score = float(ratio + pen)
            report.append(
                {
                    "plan": _plan_to_dict(p_res),
                    "ratio": ratio,
                    "cpu_penalty": pen,
                    "score": score,
                    "in_total": int(in_total),
                    "out_total": int(out_total),
                    "ok": True,
                }
            )
            scored.append((score, ratio, pen, p_res))
        except Exception as e:
            report.append({"plan": _plan_to_dict(p_res), "ok": False, "error": str(e)})

    if not scored:
        return _heuristic()

    scored.sort(key=lambda t: (t[0], t[1]))
    plans_sorted = [t[3] for t in scored]
    picked = _pick_top_diverse(plans_sorted, top_k=top_k)
    chosen = picked[0]
    runner = picked[1] if len(picked) > 1 else None

    # Persist TOP db with observed best score
    _update_top_db(top_db, bucket_type, chosen, float(scored[0][0]), top_db_max=top_db_max)
    return chosen, runner, report


def _relpath(root: Path, p: Path) -> str:
    return str(p.resolve().relative_to(root.resolve()))


def packdir(
    input_dir: Path,
    output_dir: Path,
    *,
    buckets: int = 16,
    dir_spec: DirPipelineSpec | None = None,
    jobs: int = 1,
) -> None:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    eng = Engine.default()

    # TOP pipelines db (used for "TOP-only" autopick). Kept in-repo for stability.
    top_db_path = _repo_root() / TOP_PIPELINES_REL
    top_db = _load_top_db(top_db_path)

    # Autopick knobs: allow dir spec overrides, but keep legacy defaults.
    # Project constraint: TOP-only K=2.
    top_k = max(1, _env_int("GCC_TOP_K", TOP_K_DEFAULT))
    if dir_spec is not None and dir_spec.autopick.top_k is not None:
        top_k = int(dir_spec.autopick.top_k)
    top_k = max(1, min(2, int(top_k)))

    top_db_max = max(top_k, _env_int("GCC_TOP_DB_MAX", TOP_DB_MAX_DEFAULT))
    if dir_spec is not None and dir_spec.autopick.top_db_max is not None:
        top_db_max = max(top_k, int(dir_spec.autopick.top_db_max))

    # Performance knobs (Step 7)
    # - Analyze uses at most this many bytes per file (sampling), to avoid RAM blow-ups.
    analyze_max_bytes = max(0, _env_int("GCC_ANALYZE_MAX_BYTES", 256 * 1024))
    # - Spool threshold: only files <= this size are considered for parallel jobs to keep memory bounded.
    spool_threshold = max(0, _env_int("GCC_SPOOL_THRESHOLD", 4 * 1024 * 1024))
    # - Parallelism: jobs are per-bucket and deterministic (write order preserved).
    jobs = max(1, int(jobs))

    # 1) Analyze + compute numeric density (sampled)
    records: list[dict] = []
    for p in iter_files(input_dir):
        rel = _relpath(input_dir, p)
        try:
            st_size = int(p.stat().st_size)
        except Exception:
            st_size = 0
        try:
            sampled = False
            if analyze_max_bytes and st_size and st_size > analyze_max_bytes:
                # Sample only the first chunk for fingerprinting/bucketing.
                with p.open("rb") as fp:
                    data = fp.read(int(analyze_max_bytes))
                sampled = True
            else:
                data = p.read_bytes()
        except Exception as e:
            records.append({"path": str(p), "rel": rel, "error": str(e)})
            continue

        fp = fingerprint_bytes(data)
        records.append(
            {
                "path": str(p),
                "rel": rel,
                "size": int(st_size or len(data)),
                "analyzed_bytes": int(len(data)),
                "sampled": bool(sampled),
                "algo": fp.algo,
                "simhash64": int(fp.simhash64),
                "is_text": bool(fp.is_text),
                "token_count": int(fp.token_count),
                "numeric_density": _numeric_density(data),
            }
        )

    # 2) Bucketize
    bucketed = bucketize_records(records, buckets=buckets)

    # group by bucket
    by_bucket: dict[int, list[dict]] = {}
    for r in bucketed:
        b = int(r.get("bucket", 0))
        by_bucket.setdefault(b, []).append(r)

    # 3) Plan per bucket
    plans: dict[int, Plan] = {}
    runners: dict[int, Plan | None] = {}
    bucket_types: dict[int, str] = {}
    bucket_metrics: dict[int, dict[str, float]] = {}
    bucket_autopick: dict[int, list[dict]] = {}
    for b, recs in sorted(by_bucket.items(), key=lambda x: x[0]):
        btype, met = _bucket_type(recs)
        bucket_types[b] = btype
        bucket_metrics[b] = met
        plan, runner, rep = _choose_plan_for_bucket(
            eng,
            recs,
            bucket_type=btype,
            top_db=top_db,
            top_k=top_k,
            top_db_max=top_db_max,
            dir_spec=dir_spec,
        )
        plans[b] = plan
        runners[b] = runner
        bucket_autopick[b] = rep
        # small, deterministic log
        extra = (
            f" runner={runner.note or (runner.layer_id + '+' + runner.codec_text)}"
            if runner is not None
            else ""
        )
        print(
            f"bucket[{b:02d}]({btype}): n={len(recs)} plan={plan.note or (plan.layer_id + '+' + plan.codec_text)}{extra}"
        )

    use_archive = _env_bool(ARCHIVE_ENV, ARCHIVE_DEFAULT)
    if dir_spec is not None and dir_spec.archive is not None:
        use_archive = bool(dir_spec.archive)

    # 3b) Bucket-level resources (numeric dict for num_v1)
    bucket_eng: dict[int, Engine] = {}
    bucket_num_res: dict[int, dict] = {}
    bucket_tpl_res: dict[int, dict] = {}
    if use_archive:
        # Resource knobs: allow dir spec overrides, fall back to env.
        nd_enabled = True
        nd_k = max(0, _env_int(NUM_DICT_ENV_K, 64))
        if dir_spec is not None:
            if dir_spec.num_dict_v1.enabled is not None:
                nd_enabled = bool(dir_spec.num_dict_v1.enabled)
            if dir_spec.num_dict_v1.k is not None:
                nd_k = max(0, int(dir_spec.num_dict_v1.k))
        k_num = int(nd_k)
        for b, recs in sorted(by_bucket.items(), key=lambda x: x[0]):
            plan = plans.get(b)
            if plan is None:
                continue
            if not nd_enabled:
                continue
            # resolve implicit stream_codecs for split_text_nums
            sc = plan.stream_codecs
            if plan.layer_id == "split_text_nums" and sc is None:
                sc = {ST_TEXT: plan.codec_text, ST_NUMS: "num_v1"}
            if not _plan_uses_num_v1(plan, resolved_sc=sc):
                continue
            if len(recs) < 2:
                continue
            dict_vals, tag8 = _build_bucket_num_dict(
                eng, recs, Plan(plan.layer_id, plan.codec_text, sc, plan.note), k=k_num
            )
            if not dict_vals:
                continue
            dict_raw = encode_ints(dict_vals)
            blob = bytes(tag8) + dict_raw
            blob_sha = _sha256(blob)
            bucket_num_res[b] = {
                "name": NUM_DICT_NAME,
                "tag8": tag8,
                "k": len(dict_vals),
                "blob": blob,
                "blob_sha256": blob_sha,
            }
            bucket_eng[b] = _engine_with_num_shared(eng, dict_vals, tag8)

        # tpl_dict_v0 for tpl_lines_shared_v0
        td_enabled = True
        td_k = max(0, _env_int(TPL_DICT_ENV_K, TPL_DICT_K_DEFAULT))
        if dir_spec is not None:
            if dir_spec.tpl_dict_v0.enabled is not None:
                td_enabled = bool(dir_spec.tpl_dict_v0.enabled)
            if dir_spec.tpl_dict_v0.k is not None:
                td_k = max(0, int(dir_spec.tpl_dict_v0.k))

        if td_enabled:
            for b, recs in sorted(by_bucket.items(), key=lambda x: x[0]):
                # build only if chosen or runner uses tpl_lines_shared_v0
                pl = plans.get(b)
                rn = runners.get(b)
                uses = (pl is not None and pl.layer_id == "tpl_lines_shared_v0") or (
                    rn is not None and rn.layer_id == "tpl_lines_shared_v0"
                )
                if not uses:
                    continue
                if len(recs) < 2:
                    continue
                templates, tag8, tpl_blob, tpl_meta = _build_bucket_tpl_dict(recs, k=int(td_k))
                if not templates or not tag8:
                    continue
                bucket_tpl_res[b] = {
                    "name": TPL_DICT_NAME,
                    "tag8": tag8,
                    "k": len(templates),
                    "blob": tpl_blob,
                    "meta": tpl_meta,
                    "blob_sha256": _sha256(tpl_blob),
                }
                base_eng = bucket_eng.get(b, eng)
                bucket_eng[b] = _engine_with_tpl_shared(base_eng, templates, tag8)

    # 4) Compress files + write manifest
    manifest_path = output_dir / MANIFEST_NAME
    n_ok = 0
    n_fail = 0
    in_total = 0
    out_total = 0

    writers: dict[int, GCAWriter] = {}
    res_written: dict[int, bool] = {}

    with manifest_path.open("w", encoding="utf-8") as mf:
        # Bucket summaries (ignored by unpackdir because 'rel' is missing/empty)
        for b in sorted(by_bucket.keys()):
            chosen = plans.get(b)
            runner = runners.get(b)
            res_meta: dict[str, dict] = {}
            if b in bucket_num_res:
                rr = bucket_num_res[b]
                res_meta[NUM_DICT_NAME] = {
                    "blob_sha256": str(rr.get("blob_sha256") or ""),
                    "k": int(rr.get("k", 0)),
                    "tag8_hex": bytes(rr.get("tag8", b"")).hex(),
                }
            if b in bucket_tpl_res:
                tr = bucket_tpl_res[b]
                res_meta[TPL_DICT_NAME] = {
                    "blob_sha256": str(tr.get("blob_sha256") or ""),
                    "k": int(tr.get("k", 0)),
                    "tag8_hex": bytes(tr.get("tag8", b"")).hex(),
                    **(tr.get("meta") or {}),
                }
            mf.write(
                json.dumps(
                    {
                        "kind": "bucket_summary",
                        "bucket": int(b),
                        "bucket_type": str(bucket_types.get(b, "")),
                        "metrics": dict(bucket_metrics.get(b, {})),
                        "chosen": _plan_to_dict(chosen) if chosen is not None else None,
                        "runner_up": _plan_to_dict(runner) if runner is not None else None,
                        "bucket_resources": ([NUM_DICT_NAME] if b in bucket_num_res else [])
                        + ([TPL_DICT_NAME] if b in bucket_tpl_res else []),
                        "bucket_resources_meta": res_meta,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        # Deterministic processing order
        bucketed_sorted = sorted(
            bucketed, key=lambda rr: (int(rr.get("bucket", 0)), str(rr.get("rel", "")))
        )

        from concurrent.futures import ThreadPoolExecutor

        def _compress_one(rec: dict, *, plan: Plan, eng2: Engine, btype: str) -> dict:
            """Compress one file record. Returns a dict with either {ok: True, ...} or {ok: False, error: ...}."""
            rel = str(rec.get("rel") or "")
            src_path = Path(rec.get("path"))
            b = int(rec.get("bucket", 0) or 0)
            try:
                data = src_path.read_bytes()
                sc = plan.stream_codecs
                if plan.layer_id == "split_text_nums" and sc is None:
                    sc = {ST_TEXT: plan.codec_text, ST_NUMS: "num_v1"}
                blob = compress_v6_mbn(
                    eng2,
                    data,
                    layer_id=plan.layer_id,
                    codec_id=plan.codec_text,
                    stream_codecs=sc,
                )
                in_sha = _sha256(data)
                blob_sha = _sha256(blob)
                return {
                    "ok": True,
                    "rel": rel,
                    "bucket": b,
                    "data": data,
                    "blob": blob,
                    "in_sha": in_sha,
                    "blob_sha": blob_sha,
                    "sc": sc,
                }
            except Exception as e:
                return {"ok": False, "rel": rel, "bucket": b, "error": f"compress: {e}"}

        # Process per-bucket, optionally in parallel for small files.
        for b in sorted(by_bucket.keys()):
            recs = [rr for rr in bucketed_sorted if int(rr.get("bucket", 0)) == int(b)]
            if not recs:
                continue
            plan = plans.get(b, Plan(layer_id="bytes", codec_text="raw"))
            btype = bucket_types.get(b, "")
            eng2 = bucket_eng.get(b, eng)

            # Partition by size: only "small" files are run in parallel to keep memory bounded.
            small: list[dict] = []
            large: list[dict] = []
            for rr in recs:
                rel = rr.get("rel")
                src_path = Path(rr.get("path"))
                if not rel or not src_path.exists() or not src_path.is_file() or "error" in rr:
                    # record error line and skip
                    n_fail += 1
                    mf.write(
                        json.dumps(
                            {"rel": rel, "error": rr.get("error", "missing")}, ensure_ascii=False
                        )
                        + "\n"
                    )
                    continue
                sz = int(rr.get("size") or 0)
                if jobs > 1 and spool_threshold and sz and sz <= spool_threshold:
                    small.append(rr)
                else:
                    large.append(rr)

            results: dict[str, dict] = {}

            # Run small jobs in parallel (deterministic write order preserved below)
            if jobs > 1 and len(small) > 1:
                with ThreadPoolExecutor(max_workers=int(jobs)) as ex:
                    futs = [
                        ex.submit(_compress_one, rr, plan=plan, eng2=eng2, btype=btype)
                        for rr in small
                    ]
                    for fut in futs:
                        res = fut.result()
                        results[str(res.get("rel") or "")] = res
            else:
                for rr in small:
                    res = _compress_one(rr, plan=plan, eng2=eng2, btype=btype)
                    results[str(res.get("rel") or "")] = res

            # Process large sequentially (and also any remaining small that failed to produce a result)
            for rr in large:
                res = _compress_one(rr, plan=plan, eng2=eng2, btype=btype)
                results[str(res.get("rel") or "")] = res

            # Write outputs in deterministic order
            for rr in recs:
                rel = str(rr.get("rel") or "")
                if not rel:
                    continue
                res = results.get(rel)
                if not res:
                    continue
                if not res.get("ok"):
                    n_fail += 1
                    mf.write(
                        json.dumps(
                            {
                                "rel": rel,
                                "bucket": int(b),
                                "error": str(res.get("error") or "compress: unknown"),
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    continue

                data = bytes(res["data"])
                blob = bytes(res["blob"])
                in_sha = str(res["in_sha"])
                blob_sha = str(res["blob_sha"])
                sc = res.get("sc")

                out_rel = rel + ".gcc6"  # legacy per-file output name (used when archive disabled)

                try:
                    archive_rel = None
                    archive_off = None
                    if use_archive:
                        archive_rel = f"bucket_{b:02d}.gca"
                        w = writers.get(b)
                        if w is None:
                            w = GCAWriter(output_dir / archive_rel)
                            writers[b] = w
                        # write resources (once, before first file blob)
                        if not res_written.get(b):
                            if b in bucket_num_res:
                                rrn = bucket_num_res[b]
                                w.append_resource(
                                    rrn["name"],
                                    rrn["blob"],
                                    meta={
                                        "codec": "num_v1",
                                        "k": int(rrn.get("k", 0)),
                                        "tag8_hex": bytes(rrn.get("tag8", b"")).hex(),
                                        "blob_sha256": str(rrn.get("blob_sha256") or ""),
                                    },
                                )
                            if b in bucket_tpl_res:
                                tr = bucket_tpl_res[b]
                                w.append_resource(
                                    tr["name"],
                                    tr["blob"],
                                    meta={
                                        "layer": "tpl_lines_shared_v0",
                                        "k": int(tr.get("k", 0)),
                                        "tag8_hex": bytes(tr.get("tag8", b"")).hex(),
                                        "blob_sha256": str(tr.get("blob_sha256") or ""),
                                        **(tr.get("meta") or {}),
                                    },
                                )
                            res_written[b] = True
                        ent = w.append(
                            rel,
                            blob,
                            meta={
                                "bucket": b,
                                "bucket_type": btype,
                                "layer_id": plan.layer_id,
                                "codec_text": plan.codec_text,
                                "stream_codecs": sc,
                                "plan_note": plan.note,
                                "runner_up": _plan_to_dict(runners.get(b))
                                if runners.get(b) is not None
                                else None,
                                "in_size": len(data),
                                "out_size": len(blob),
                                "sha256": in_sha,
                                "in_sha256": in_sha,
                                "blob_sha256": blob_sha,
                                "ver": 6,
                            },
                        )
                        archive_off = int(ent.offset)
                        archive_len = int(ent.length)
                    else:
                        out_path = output_dir / out_rel
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        out_path.write_bytes(blob)
                        archive_len = None

                    rec = {
                        "rel": rel,
                        "bucket": b,
                        "bucket_type": bucket_types.get(b, ""),
                        "layer_id": plan.layer_id,
                        "codec_text": plan.codec_text,
                        "stream_codecs": sc,
                        "plan_note": plan.note,
                        "runner_up": _plan_to_dict(runners.get(b))
                        if runners.get(b) is not None
                        else None,
                        "bucket_resources": ([NUM_DICT_NAME] if b in bucket_num_res else [])
                        + ([TPL_DICT_NAME] if b in bucket_tpl_res else []),
                        "out_rel": None if use_archive else out_rel,
                        "archive": archive_rel,
                        "archive_offset": archive_off,
                        "archive_length": archive_len if use_archive else None,
                        "in_size": len(data),
                        "out_size": len(blob),
                        "sha256": in_sha,
                        "in_sha256": in_sha,
                        "blob_sha256": blob_sha if use_archive else None,
                        "ver": 6,
                    }
                    mf.write(json.dumps(rec, ensure_ascii=False) + "\n")

                    n_ok += 1
                    in_total += len(data)
                    out_total += len(blob)
                except Exception as e:
                    n_fail += 1
                    mf.write(
                        json.dumps(
                            {"rel": rel, "bucket": b, "error": f"write: {e}"}, ensure_ascii=False
                        )
                        + "\n"
                    )

    ratio = (out_total / in_total) if in_total else 0.0
    print(f"packdir: files_ok={n_ok} files_fail={n_fail}")
    print(f"packdir: total_in={in_total} total_out={out_total} ratio={ratio:.3f}")
    print(f"packdir: manifest -> {manifest_path}")

    # close archives (write index + trailer)
    if writers:
        for _b, w in sorted(writers.items(), key=lambda x: x[0]):
            w.close()
        print(f"packdir: archives -> {output_dir} (buckets={len(writers)})")

    # Write per-run autopick report (bucket-level)
    try:
        report_obj = {
            "schema": "gcc-ocf.autopick_report.v1",
            "have_zstd": bool(_have_zstd()),
            "buckets": {},
        }
        for b in sorted(by_bucket.keys()):
            chosen = plans.get(b)
            runner = runners.get(b)
            report_obj["buckets"][f"{b:02d}"] = {
                "bucket": int(b),
                "bucket_type": str(bucket_types.get(b, "")),
                "metrics": dict(bucket_metrics.get(b, {})),
                "chosen": _plan_to_dict(chosen) if chosen is not None else None,
                "runner_up": _plan_to_dict(runner) if runner is not None else None,
                "candidates": bucket_autopick.get(b, []),
                "bucket_resources": ([NUM_DICT_NAME] if b in bucket_num_res else [])
                + ([TPL_DICT_NAME] if b in bucket_tpl_res else []),
            }
        (output_dir / "autopick_report.json").write_text(
            json.dumps(report_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"packdir: autopick_report -> {output_dir / 'autopick_report.json'}")
    except Exception as e:
        print(f"packdir: WARNING autopick_report non scritto: {e}")

    # Persist TOP pipelines (best-known plans) for future runs.
    _save_top_db(top_db_path, top_db)
    print(f"packdir: top_pipelines -> {top_db_path}")


def unpackdir(output_dir: Path, restore_dir: Path) -> None:
    output_dir = output_dir.resolve()
    restore_dir = restore_dir.resolve()
    restore_dir.mkdir(parents=True, exist_ok=True)

    eng = Engine.default()

    manifest_path = output_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        raise ValueError(f"unpackdir: manifest non trovato: {manifest_path}")

    n_ok = 0
    n_fail = 0

    readers: dict[str, GCAReader] = {}
    archive_eng: dict[str, Engine] = {}

    with manifest_path.open("r", encoding="utf-8") as mf:
        for line in mf:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rel = rec.get("rel")
            if not rel or "error" in rec:
                # skip failed entries
                continue

            # Empty-file invariant: always restore empty files without decoding.
            in_size = int(rec.get("in_size") or 0)
            sha = str(rec.get("sha256") or rec.get("in_sha256") or "")
            if in_size == 0 and sha == EMPTY_SHA256:
                dst_path = restore_dir / rel
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                dst_path.write_bytes(b"")
                n_ok += 1
                continue

            try:
                archive_rel = rec.get("archive")
                if archive_rel:
                    off = int(rec.get("archive_offset") or 0)
                    ln = int(rec.get("archive_length") or 0)
                    if ln <= 0:
                        raise ValueError("archive_length non valido")
                    key = str(archive_rel)
                    rd = readers.get(key)
                    if rd is None:
                        rd = GCAReader(output_dir / key)
                        readers[key] = rd
                        # load bucket-level resources (if any) and configure engine
                        try:
                            res = rd.load_resources()
                        except Exception:
                            res = {}
                        if NUM_DICT_NAME in res:
                            rblob = bytes(res[NUM_DICT_NAME]["blob"])
                            if len(rblob) >= 8:
                                tag8 = rblob[:8]
                                dict_raw = rblob[8:]
                                dict_vals = decode_ints(dict_raw)
                                archive_eng[key] = _engine_with_num_shared(eng, dict_vals, tag8)
                        if TPL_DICT_NAME in res:
                            tblob = bytes(res[TPL_DICT_NAME]["blob"])
                            templates, tmeta = unpack_tpl_dict_v0_resource(tblob)
                            tag8_hex = str(tmeta.get("tag8_hex", "")) or ""
                            tag8 = (
                                bytes.fromhex(tag8_hex)
                                if len(tag8_hex) == 16
                                else bytes(tmeta.get("tag8") or b"")
                            )
                            base = archive_eng.get(key, eng)
                            if templates and tag8 and len(tag8) == 8:
                                archive_eng[key] = _engine_with_tpl_shared(base, templates, tag8)
                    blob = rd.read_blob(off, ln)
                else:
                    out_rel = rec.get("out_rel")
                    if not out_rel:
                        raise ValueError("out_rel mancante")
                    comp_path = output_dir / str(out_rel)
                    if not comp_path.is_file():
                        raise ValueError("file compresso mancante")
                    blob = comp_path.read_bytes()
                e2 = archive_eng.get(str(archive_rel), eng) if archive_rel else eng
                data = decompress_v6(e2, blob)
                dst_path = restore_dir / rel
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                dst_path.write_bytes(data)
                n_ok += 1
            except Exception:
                n_fail += 1

    for rd in readers.values():
        try:
            rd.close()
        except Exception:
            pass

    print(f"unpackdir: files_ok={n_ok} files_fail={n_fail}")
    print(f"unpackdir: restored -> {restore_dir}")


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        print("Usage:")
        print(f"  {argv[0]} packdir <input_dir> <output_dir> [buckets]")
        print(f"  {argv[0]} unpackdir <output_dir> <restore_dir>")
        print()
        print("Env:")
        print("  TB_MODULE=<python.module>  optional Turbo-Bucketizer plugin")
        print("  GCC_AUTOPICK=0             disable autopick (use heuristic)")
        print("  GCC_AUTOPICK_N=3           sample size per bucket (1..8)")
        print("  GCC_TOP_K=2                keep top-K pipelines per bucket type")
        print(
            "  GCC_TOP_DB_MAX=12          keep up to this many scored pipelines per bucket type (history)"
        )
        print("  GCC_REFRESH_TOP=1          ignore TOP db and explore broad candidates")
        print(f"  {ARCHIVE_ENV}=0              disable per-bucket archive (.gca) output")
        print(
            f"  {NUM_DICT_ENV_K}=64          bucket-level numeric dict size for num_v1 (archive only)"
        )
        return 1

    cmd = argv[1]
    if cmd == "packdir":
        if len(argv) < 4:
            raise ValueError("packdir: args insufficienti")
        input_dir = Path(argv[2])
        output_dir = Path(argv[3])
        buckets = int(argv[4]) if len(argv) >= 5 else 16
        packdir(input_dir, output_dir, buckets=buckets)
        return 0

    if cmd == "unpackdir":
        if len(argv) < 4:
            raise ValueError("unpackdir: args insufficienti")
        output_dir = Path(argv[2])
        restore_dir = Path(argv[3])
        unpackdir(output_dir, restore_dir)
        return 0

    raise ValueError(f"comando sconosciuto: {cmd}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
