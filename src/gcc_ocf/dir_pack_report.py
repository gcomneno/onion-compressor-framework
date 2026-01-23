"""Aggregated mini-report for classic `dir pack` (manifest.jsonl + bucket_*.gca).

Determinism note:
This report is part of the classic output directory and is therefore covered by
`tests/test_p1_determinism.py`. For this reason, the serialized report MUST be
deterministic across runs given the same input content.

Concretely, we DO NOT embed:
- timestamps
- absolute paths (input_dir/output_dir)
Anything run/environment-specific would break determinism.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _safe_int(x: object, default: int = 0) -> int:
    try:
        return int(x)  # type: ignore[arg-type]
    except Exception:
        return default


def _safe_float(x: object, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)  # type: ignore[arg-type]
    except Exception:
        return default


def _safe_stream_codecs(d: object) -> dict[int, str]:
    out: dict[int, str] = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        try:
            ik = int(k)  # type: ignore[arg-type]
        except Exception:
            continue
        out[ik] = str(v)
    return out


def _norm_ext(rel: str) -> str:
    suf = Path(rel).suffix.lower()
    return suf if suf else "(none)"


def _plan_key(plan: dict[str, Any] | None) -> str:
    if not plan:
        return "(none)"
    layer_id = str(plan.get("layer_id") or plan.get("layer") or "")
    codec_text = str(plan.get("codec_text") or plan.get("codec") or "")
    sc = _safe_stream_codecs(plan.get("stream_codecs"))
    note = str(plan.get("note") or plan.get("plan_note") or "").strip()
    sc_part = ""
    if sc:
        items = sorted(sc.items(), key=lambda kv: kv[0])
        sc_part = ";streams=" + ",".join([f"{k}:{v}" for k, v in items])
    note_part = f";note={note}" if note else ""
    return f"{layer_id}+{codec_text}{sc_part}{note_part}"


def _bytes_h(n: int) -> str:
    if n < 0:
        return str(n)
    units = ["B", "KiB", "MiB", "GiB"]
    f = float(n)
    u = 0
    while f >= 1024.0 and u < len(units) - 1:
        f /= 1024.0
        u += 1
    return f"{int(f)} {units[u]}" if u == 0 else f"{f:.2f} {units[u]}"


def _load_manifest_rows(output_dir: Path) -> list[dict[str, Any]]:
    """Best-effort load of manifest.jsonl emitted by packdir."""
    path = output_dir / "manifest.jsonl"
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def build_dir_pack_report(
    *,
    input_dir: Path,
    output_dir: Path,
    buckets: int,
    files_ok: int,
    files_fail: int,
    total_in: int,
    total_out: int,
    bucket_summaries: dict[int, dict[str, Any]] | dict[str, Any],
    file_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
    autopick_candidates: dict[int, list[dict[str, Any]]] | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic aggregated report.

    If `file_rows` is empty, fall back to reading `output_dir/manifest.jsonl`.
    """

    autopick_candidates = autopick_candidates or {}
    if not isinstance(bucket_summaries, dict):
        bucket_summaries = {}
    if not isinstance(autopick_candidates, dict):
        autopick_candidates = {}

    if not file_rows:
        file_rows = _load_manifest_rows(output_dir)

    # Aggregate by extension / plan / bucket
    ext_stats: dict[str, dict[str, int]] = {}
    plan_stats: dict[str, dict[str, int]] = {}
    bucket_stats: dict[int, dict[str, Any]] = {}

    for r in file_rows:
        rel = str(r.get("rel") or r.get("path") or r.get("name") or "")
        if not rel:
            continue
        b = _safe_int(r.get("bucket"), 0)
        in_sz = _safe_int(r.get("in_size"), _safe_int(r.get("size_in"), 0))
        out_sz = _safe_int(r.get("out_size"), _safe_int(r.get("size_out"), 0))

        ext = _norm_ext(rel)
        es = ext_stats.setdefault(ext, {"files": 0, "in": 0, "out": 0})
        es["files"] += 1
        es["in"] += in_sz
        es["out"] += out_sz

        plan_obj: dict[str, Any] = {
            "layer_id": r.get("layer_id"),
            "codec_text": r.get("codec_text"),
            "stream_codecs": r.get("stream_codecs"),
            "note": r.get("plan_note") or r.get("note"),
        }
        pk = _plan_key(plan_obj)
        ps = plan_stats.setdefault(pk, {"files": 0, "in": 0, "out": 0})
        ps["files"] += 1
        ps["in"] += in_sz
        ps["out"] += out_sz

        bs = bucket_stats.setdefault(b, {"bucket": b, "files": 0, "in": 0, "out": 0})
        bs["files"] += 1
        bs["in"] += in_sz
        bs["out"] += out_sz

    def _top_rows(stats: dict[str, dict[str, int]], k: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, v in stats.items():
            in_b = _safe_int(v.get("in"), 0)
            out_b = _safe_int(v.get("out"), 0)
            saved = in_b - out_b
            ratio = (out_b / in_b) if in_b else 0.0
            rows.append(
                {
                    "key": key,
                    "files": _safe_int(v.get("files"), 0),
                    "in": in_b,
                    "out": out_b,
                    "saved": saved,
                    "ratio": float(ratio),
                }
            )
        rows.sort(
            key=lambda rr: (
                -_safe_int(rr.get("saved"), 0),
                _safe_int(rr.get("out"), 0),
                str(rr.get("key")),
            )
        )
        return rows[: max(0, int(k))]

    top_extensions = _top_rows(ext_stats, 10)
    top_plans = _top_rows(plan_stats, 10)

    # Buckets: keep deterministic ordering and omit volatile details.
    top_buckets: list[dict[str, Any]] = []
    buckets_detail: dict[str, Any] = {}

    all_bucket_ids: set[int] = set(int(k) for k in bucket_stats.keys())
    for k in bucket_summaries.keys():
        try:
            all_bucket_ids.add(int(k))  # type: ignore[arg-type]
        except Exception:
            continue

    for b in sorted(all_bucket_ids):
        bs = bucket_stats.get(b, {"bucket": b, "files": 0, "in": 0, "out": 0})
        summ = bucket_summaries.get(b) if isinstance(bucket_summaries, dict) else None
        if summ is None and isinstance(bucket_summaries, dict):
            summ = bucket_summaries.get(f"{b:02d}") or bucket_summaries.get(str(b))

        btype = str((summ or {}).get("bucket_type") or "")
        chosen = (
            (summ or {}).get("chosen") if isinstance((summ or {}).get("chosen"), dict) else None
        )

        in_b = _safe_int(bs.get("in"), 0)
        out_b = _safe_int(bs.get("out"), 0)
        saved = in_b - out_b
        ratio = (out_b / in_b) if in_b else 0.0

        buckets_detail[f"{b:02d}"] = {
            "bucket": b,
            "bucket_type": btype,
            "files": _safe_int(bs.get("files"), 0),
            "in": in_b,
            "out": out_b,
            "saved": saved,
            "ratio": float(ratio),
            "chosen": chosen,
        }

        top_buckets.append(
            {
                "bucket": b,
                "bucket_type": btype,
                "files": _safe_int(bs.get("files"), 0),
                "in": in_b,
                "out": out_b,
                "saved": saved,
                "ratio": float(ratio),
                "chosen": chosen,
            }
        )

    top_buckets.sort(
        key=lambda r: (
            -_safe_int(r.get("saved"), 0),
            _safe_int(r.get("out"), 0),
            _safe_int(r.get("bucket"), 0),
        )
    )
    top_buckets = top_buckets[:5]

    overall_ratio = (int(total_out) / int(total_in)) if int(total_in) else 0.0

    # Deterministic: omit absolute paths and timestamps.
    return {
        "schema": "gcc-ocf.dir_pack_report.v1",
        "mode": "classic_gca1",
        "buckets": int(buckets),
        "files_ok": int(files_ok),
        "files_fail": int(files_fail),
        "total_in": int(total_in),
        "total_out": int(total_out),
        "ratio": float(overall_ratio),
        "top_buckets": top_buckets,
        "top_extensions": top_extensions,
        "top_plans": top_plans,
        "buckets_detail": buckets_detail,
        "errors": list(error_rows)[:200],
    }


def render_dir_pack_report_text(rep: dict[str, Any]) -> str:
    # Deterministic text: no timestamps, no paths.
    lines: list[str] = []
    lines.append("GCC-OCF dir pack — mini-report (classic mode)\n")
    lines.append(
        f"files_ok={rep.get('files_ok')} files_fail={rep.get('files_fail')} buckets={rep.get('buckets')}\n"
    )
    lines.append(
        f"total_in={_bytes_h(_safe_int(rep.get('total_in'), 0))} total_out={_bytes_h(_safe_int(rep.get('total_out'), 0))} ratio={_safe_float(rep.get('ratio'), 0.0):.3f}\n\n"
    )

    lines.append("Top bucket (per risparmio)\n")
    tb = rep.get("top_buckets") or []
    if not tb:
        lines.append("  (nessun dato)\n\n")
    else:
        for r in tb:
            b = _safe_int(r.get("bucket"), 0)
            btype = str(r.get("bucket_type") or "")
            lines.append(
                f"  bucket[{b:02d}] {btype:10s} saved={_bytes_h(_safe_int(r.get('saved'), 0))} ratio={_safe_float(r.get('ratio'), 0.0):.3f}\n"
            )
        lines.append("\n")

    lines.append("Top estensioni (per risparmio)\n")
    te = rep.get("top_extensions") or []
    if not te:
        lines.append("  (nessun dato)\n\n")
    else:
        for r in te[:10]:
            lines.append(
                f"  {str(r.get('key')):10s} files={_safe_int(r.get('files'), 0):4d} saved={_bytes_h(_safe_int(r.get('saved'), 0))} ratio={_safe_float(r.get('ratio'), 0.0):.3f}\n"
            )
        lines.append("\n")

    lines.append("Saving per plan (top)\n")
    tp = rep.get("top_plans") or []
    if not tp:
        lines.append("  (nessun dato)\n\n")
    else:
        for r in tp[:10]:
            lines.append(
                f"  {str(r.get('key'))}\n    files={_safe_int(r.get('files'), 0)} saved={_bytes_h(_safe_int(r.get('saved'), 0))} ratio={_safe_float(r.get('ratio'), 0.0):.3f}\n"
            )
        lines.append("\n")

    return "".join(lines)
