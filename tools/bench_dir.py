#!/usr/bin/env python3
"""Directory-mode benchmark/soak tool (Step 7).

Runs pack -> verify -> unpack -> diff, collecting basic timing and peak RSS.

Usage example:
  python tools/bench_dir.py /path/in --pipeline @tools/dir_pipelines/default_v1.json --jobs 4 --iters 3

Notes:
- Uses internal APIs (no subprocess). Run inside repo venv.
- By default uses temp output/restore directories.
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import time
from pathlib import Path
from typing import Any


def _peak_rss_kb() -> int:
    # Linux: ru_maxrss is KB
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def _dir_diff_equal(a: Path, b: Path) -> bool:
    """Return True if directory trees contain identical file bytes."""
    a = Path(a)
    b = Path(b)
    for p in sorted(a.rglob("*")):
        if p.is_dir():
            continue
        rel = p.relative_to(a)
        q = b / rel
        if not q.is_file():
            return False
        if p.read_bytes() != q.read_bytes():
            return False
    # Also ensure b has no extra files
    for q in sorted(b.rglob("*")):
        if q.is_dir():
            continue
        rel = q.relative_to(b)
        if not (a / rel).is_file():
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench_dir.py", description="GCC-OCF directory benchmark")
    ap.add_argument("input_dir", type=Path)
    ap.add_argument(
        "--pipeline", default=None, help="Dir pipeline spec (@file.json or inline JSON)"
    )
    ap.add_argument("--buckets", type=int, default=None)
    ap.add_argument("--jobs", type=int, default=1)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument(
        "--output", type=Path, default=None, help="Optional output dir (will be wiped each iter)"
    )
    ap.add_argument(
        "--restore", type=Path, default=None, help="Optional restore dir (will be wiped each iter)"
    )
    ap.add_argument(
        "--full-verify", action="store_true", help="Run verify --full (sha256+crc32 over blobs)"
    )
    ns = ap.parse_args(argv)

    from gcc_ocf.dir_pipeline_spec import load_dir_pipeline_spec
    from gcc_ocf.legacy.gcc_dir import packdir, unpackdir
    from gcc_ocf.verify import verify_packed_dir

    inp = ns.input_dir.resolve()
    if not inp.is_dir():
        raise SystemExit(f"input_dir non valido: {inp}")

    dir_spec = load_dir_pipeline_spec(ns.pipeline) if ns.pipeline else None

    # Resolve buckets precedence like CLI
    buckets = (
        int(ns.buckets)
        if ns.buckets is not None
        else (int(dir_spec.buckets) if dir_spec and dir_spec.buckets is not None else 16)
    )

    out = (ns.output or Path("/tmp/gcc_ocf_bench_out")).resolve()
    rst = (ns.restore or Path("/tmp/gcc_ocf_bench_restore")).resolve()

    rows: list[dict[str, Any]] = []
    t0_all = time.perf_counter()

    for i in range(int(ns.iters)):
        # reset dirs
        if out.exists():
            shutil.rmtree(out)
        if rst.exists():
            shutil.rmtree(rst)

        rss0 = _peak_rss_kb()
        t0 = time.perf_counter()
        packdir(inp, out, buckets=buckets, dir_spec=dir_spec, jobs=max(1, int(ns.jobs)))
        t_pack = time.perf_counter() - t0
        rss1 = _peak_rss_kb()

        t1 = time.perf_counter()
        verify_packed_dir(out, full=bool(ns.full_verify))
        t_verify = time.perf_counter() - t1
        rss2 = _peak_rss_kb()

        t2 = time.perf_counter()
        unpackdir(out, rst)
        t_unpack = time.perf_counter() - t2
        rss3 = _peak_rss_kb()

        t3 = time.perf_counter()
        same = _dir_diff_equal(inp, rst)
        t_diff = time.perf_counter() - t3
        rss4 = _peak_rss_kb()

        row = {
            "iter": i + 1,
            "buckets": buckets,
            "jobs": int(ns.jobs),
            "pipeline": (str(ns.pipeline) if ns.pipeline else None),
            "times_sec": {
                "pack": t_pack,
                "verify": t_verify,
                "unpack": t_unpack,
                "diff": t_diff,
                "total": t_pack + t_verify + t_unpack + t_diff,
            },
            "peak_rss_kb": {
                "before": rss0,
                "after_pack": rss1,
                "after_verify": rss2,
                "after_unpack": rss3,
                "after_diff": rss4,
                "max": max(rss0, rss1, rss2, rss3, rss4),
            },
            "roundtrip_ok": bool(same),
        }
        rows.append(row)
        print(json.dumps(row, ensure_ascii=False))
        if not same:
            raise SystemExit("diff mismatch: roundtrip non lossless")

    total = time.perf_counter() - t0_all
    # summary
    if rows:
        avg_total = sum(r["times_sec"]["total"] for r in rows) / len(rows)
    else:
        avg_total = 0.0
    summary = {
        "schema": "gcc-ocf.bench_dir.v1",
        "iters": len(rows),
        "avg_total_sec": avg_total,
        "wall_total_sec": total,
        "max_peak_rss_kb": max((r["peak_rss_kb"]["max"] for r in rows), default=0),
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
