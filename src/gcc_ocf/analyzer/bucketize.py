from __future__ import annotations

import importlib
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from gcc_ocf.analyzer.simhash import Fingerprint, fingerprint_bytes


def iter_files(root: Path) -> Iterator[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def analyze_dir(root: Path, *, out_jsonl: Path) -> None:
    root = root.resolve()
    n = 0
    with out_jsonl.open("w", encoding="utf-8") as f:
        for p in iter_files(root):
            try:
                data = p.read_bytes()
            except Exception as e:
                rec = {"path": str(p), "error": str(e)}
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue

            fp = fingerprint_bytes(data)
            rec = {
                "path": str(p),
                "rel": str(p.relative_to(root)),
                "size": len(data),
                **asdict(fp),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    print(f"analyze-dir: wrote {n} records -> {out_jsonl}")


def _fallback_bucket(simhash64: int, buckets: int) -> int:
    return int(simhash64) % int(buckets)


def _load_tb_plugin() -> Optional[object]:
    modname = os.environ.get("TB_MODULE")
    if not modname:
        return None
    try:
        return importlib.import_module(modname)
    except Exception:
        return None


def bucketize_records(records: List[dict], *, buckets: int) -> List[dict]:
    plugin = _load_tb_plugin()
    out: List[dict] = []
    for r in records:
        if "simhash64" not in r:
            continue
        sim = int(r["simhash64"])
        b = None
        if plugin is not None and hasattr(plugin, "bucket_for_fingerprint"):
            try:
                b = int(plugin.bucket_for_fingerprint(sim, int(buckets)))
            except Exception:
                b = None
        if b is None:
            b = _fallback_bucket(sim, buckets)
        rr = dict(r)
        rr["bucket"] = b
        out.append(rr)
    return out


def bucket_dir(report_jsonl: Path, *, buckets: int, out_jsonl: Path) -> None:
    records: List[dict] = []
    with report_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    out = bucketize_records(records, buckets=buckets)
    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"bucket-dir: wrote {len(out)} records -> {out_jsonl}")
