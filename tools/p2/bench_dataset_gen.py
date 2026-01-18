#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import string
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final

PRESETS: Final[set[str]] = {
    "tiny_smoke",
    "text_corpus_small",
    "mixed_corpus_small",
    "bigfile_single",
}


@dataclass(frozen=True)
class DatasetMeta:
    preset: str
    seed: int
    root: str
    note: str
    files_written: int
    bytes_written: int


def _write_bytes(path: Path, data: bytes) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return len(data)


def _write_text(path: Path, text: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    b = text.encode("utf-8")
    path.write_bytes(b)
    return len(b)


def _rand_word(rng: random.Random, min_len: int = 3, max_len: int = 12) -> str:
    n = rng.randint(min_len, max_len)
    alphabet = string.ascii_lowercase
    return "".join(rng.choice(alphabet) for _ in range(n))


def _rand_line(rng: random.Random) -> str:
    # un po' di unicode, numeri, e roba "fattura-like"
    parts = []
    parts.append(_rand_word(rng).capitalize())
    parts.append(_rand_word(rng))
    parts.append(str(rng.randint(0, 10_000_000)))
    parts.append(rng.choice(["€", "Ω", "✓", "Δ", "π", "—", "…", "漢字", "è", "à", "ù"]))
    parts.append(rng.choice(["IVA", "Totale", "Riga", "Codice", "Cliente", "Note"]))
    return " ".join(parts)


def _make_text_file(rng: random.Random, *, lines: int, long_line: bool = False) -> str:
    out = []
    for _ in range(lines):
        out.append(_rand_line(rng))
    if long_line:
        # una riga esagerata
        tail = " | ".join(_rand_line(rng) for _ in range(200))
        out.append(tail)
    return "\n".join(out) + "\n"


def _make_jsonl(rng: random.Random, *, rows: int) -> str:
    out = []
    for i in range(rows):
        obj = {
            "id": i,
            "name": _rand_word(rng),
            "amount": rng.randint(0, 1_000_000),
            "flag": rng.choice([True, False]),
            "note": _rand_line(rng),
        }
        out.append(json.dumps(obj, ensure_ascii=False))
    return "\n".join(out) + "\n"


def _write_random_bin(rng: random.Random, path: Path, size: int) -> int:
    # randbytes è veloce e deterministic
    remaining = size
    chunk = 1 << 16
    written = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        while remaining > 0:
            n = min(chunk, remaining)
            f.write(rng.randbytes(n))
            written += n
            remaining -= n
    return written


def _generate_tiny_smoke(root: Path, rng: random.Random) -> tuple[int, int, str]:
    files = 0
    bytes_ = 0
    note = "Tiny dataset per smoke benchmark (veloce)."

    bytes_ += _write_text(root / "hello.txt", "Ciao\n")
    files += 1

    bytes_ += _write_text(root / "nested" / "deep" / "unicode.txt", "Ωmega ✓\n")
    files += 1

    bytes_ += _write_bytes(root / "nested" / "deep" / "empty.txt", b"")
    files += 1

    bytes_ += _write_bytes(root / "bin" / "tiny.bin", b"\x00\x01\x02\x03")
    files += 1

    return files, bytes_, note


def _generate_text_corpus_small(root: Path, rng: random.Random) -> tuple[int, int, str]:
    files = 0
    bytes_ = 0
    note = "Corpus text small: unicode, linee lunghe, jsonl, e file vuoti."

    # 20 vuoti
    for i in range(20):
        bytes_ += _write_bytes(root / "empty" / f"e_{i:03d}.txt", b"")
        files += 1

    # 80 txt normali
    for i in range(80):
        txt = _make_text_file(rng, lines=rng.randint(10, 60))
        bytes_ += _write_text(root / "docs" / f"doc_{i:03d}.txt", txt)
        files += 1

    # 40 txt con linee lunghe
    for i in range(40):
        txt = _make_text_file(rng, lines=rng.randint(5, 20), long_line=True)
        bytes_ += _write_text(root / "long" / f"long_{i:03d}.md", txt)
        files += 1

    # 60 jsonl
    for i in range(60):
        txt = _make_jsonl(rng, rows=rng.randint(50, 200))
        bytes_ += _write_text(root / "jsonl" / f"data_{i:03d}.jsonl", txt)
        files += 1

    return files, bytes_, note


def _generate_mixed_corpus_small(root: Path, rng: random.Random) -> tuple[int, int, str]:
    files = 0
    bytes_ = 0
    note = "Corpus mixed small: testo+bin, include binari minuscoli e file vuoti."

    # parte testo (120)
    for i in range(120):
        txt = _make_text_file(rng, lines=rng.randint(5, 40), long_line=(i % 17 == 0))
        bytes_ += _write_text(root / "text" / f"t_{i:03d}.txt", txt)
        files += 1

    # vuoti (20)
    for i in range(20):
        bytes_ += _write_bytes(root / "text" / "empty" / f"e_{i:03d}.txt", b"")
        files += 1

    # binari random (50) 1..64KB
    for i in range(50):
        sz = rng.randint(1024, 64 * 1024)
        bytes_ += _write_random_bin(rng, root / "bin" / f"r_{i:03d}.bin", sz)
        files += 1

    # binari minuscoli (10) 0..32 bytes
    for i in range(10):
        sz = rng.randint(0, 32)
        bytes_ += _write_random_bin(rng, root / "bin" / "tiny" / f"tiny_{i:03d}.bin", sz)
        files += 1

    return files, bytes_, note


def _generate_bigfile_single(root: Path, rng: random.Random, big_mb: int) -> tuple[int, int, str]:
    files = 0
    bytes_ = 0
    note = f"Bigfile torture: ~{big_mb}MB random.bin + ~{big_mb}MB pseudo_text.txt (+ empty)."

    bytes_ += _write_bytes(root / "nested" / "deep" / "empty.txt", b"")
    files += 1

    # random.bin
    target = big_mb * 1024 * 1024
    bytes_ += _write_random_bin(rng, root / "big" / "random.bin", target)
    files += 1

    # pseudo_text.txt (molto comprimibile)
    path = root / "big" / "pseudo_text.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("wb") as f:
        while written < target:
            line = _make_text_file(rng, lines=50, long_line=True)
            b = line.encode("utf-8")
            take = min(len(b), target - written)
            f.write(b[:take])
            written += take
    bytes_ += written
    files += 1

    return files, bytes_, note


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate deterministic datasets for P2 bench/torture.")
    ap.add_argument("--out", required=True, help="Output root directory (datasets will be created under it).")
    ap.add_argument("--preset", required=True, choices=sorted(PRESETS))
    ap.add_argument("--seed", type=int, default=1337, help="Deterministic seed.")
    ap.add_argument("--big-mb", type=int, default=250, help="For bigfile_single: size per big file in MB.")
    args = ap.parse_args()

    out_root = Path(args.out).expanduser().resolve()
    ds_dir = out_root / args.preset / "in"
    ds_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)

    files = 0
    bytes_ = 0
    note = ""

    if args.preset == "tiny_smoke":
        files, bytes_, note = _generate_tiny_smoke(ds_dir, rng)
    elif args.preset == "text_corpus_small":
        files, bytes_, note = _generate_text_corpus_small(ds_dir, rng)
    elif args.preset == "mixed_corpus_small":
        files, bytes_, note = _generate_mixed_corpus_small(ds_dir, rng)
    elif args.preset == "bigfile_single":
        files, bytes_, note = _generate_bigfile_single(ds_dir, rng, args.big_mb)
    else:
        raise AssertionError("preset non gestito")

    meta = DatasetMeta(
        preset=args.preset,
        seed=args.seed,
        root=str(ds_dir),
        note=note,
        files_written=files,
        bytes_written=bytes_,
    )
    meta_path = out_root / args.preset / "dataset.json"
    meta_path.write_text(json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: generated preset={args.preset} root={ds_dir}")
    print(f"OK: files={files} bytes={bytes_}")
    print(f"OK: meta -> {meta_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
