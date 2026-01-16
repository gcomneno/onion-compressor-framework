from __future__ import annotations

import sys
from pathlib import Path

from analyzer.bucketize import analyze_dir, bucket_dir


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in ("analyze-dir", "bucket-dir"):
        print("Uso:")
        print(f"  {argv[0]} analyze-dir <root_dir> <out.jsonl>")
        print(f"  {argv[0]} bucket-dir <report.jsonl> <buckets> <out.jsonl>")
        print("")
        print("Note:")
        print("  - TB_MODULE (opzionale): modulo python con bucket_for_fingerprint(simhash64, buckets)->bucket")
        return 1

    cmd = argv[1]
    if cmd == "analyze-dir":
        if len(argv) < 4:
            raise ValueError("analyze-dir: argomenti insufficienti")
        analyze_dir(Path(argv[2]), out_jsonl=Path(argv[3]))
        return 0

    if cmd == "bucket-dir":
        if len(argv) < 5:
            raise ValueError("bucket-dir: argomenti insufficienti")
        bucket_dir(Path(argv[2]), buckets=int(argv[3]), out_jsonl=Path(argv[4]))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
