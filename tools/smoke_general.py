#!/usr/bin/env python3
"""Robust general smoke tests for GCC-OCF.

Goal:
- deterministic, repeatable general tests that exercise file + dir workflows
- produce a JSON report
- fail fast (non-zero exit) on any mismatch

Adds:
- --unicode to include unicode + very long lines in generated text

Usage examples:
  python tools/smoke_general.py --iters 10
  python tools/smoke_general.py --iters 50 --seed 123 --keep
  python tools/smoke_general.py --iters 10 --unicode
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import string
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
    )


def _sha256_file(p: Path, *, chunk_size: int = 256 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _tree_digest(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in root.rglob("*"):
        if p.is_file():
            out[p.relative_to(root).as_posix()] = _sha256_file(p)
    return out


def _write_text(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")


def _write_bytes(p: Path, b: bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b)


def _rand_ascii(rng: random.Random, n: int) -> str:
    alphabet = string.ascii_letters + string.digits + " _-.,;:/@"
    return "".join(rng.choice(alphabet) for _ in range(n))


def _gen_invoice_like(rng: random.Random) -> str:
    lines: list[str] = []
    inv = rng.randint(1, 9999)
    lines.append(f"FATTURA N. {inv}")
    for _ in range(rng.randint(3, 12)):
        art = rng.choice(["vite", "dado", "rondella", "bullone", "chiave", "cavo"])
        size = rng.choice(["M3", "M4", "M5", "M6", "M8"])
        qty = rng.randint(1, 200)
        price = rng.randint(1, 500) / 100.0
        lines.append(f"RIGA ARTICOLO: {art} {size} qty={qty} prezzo={price:.2f}")
    total = rng.randint(10, 50000) / 100.0
    lines.append(f"TOTALE {total:.2f}")
    return "\n".join(lines) + "\n"


def _gen_unicode_long_text(rng: random.Random, *, long_len: int = 20000) -> str:
    # deterministic unicode mix + a very long line
    tokens = [
        "caffè",
        "☕",
        "—",
        "北京",
        "東京",
        "😀",
        "⚙️",
        "résumé",
        "naïve",
        "ﬁ",
        "€",
    ]
    header = " ".join(rng.choice(tokens) for _ in range(20))
    long_line = ("ΑβΓδ" * (long_len // 4 + 1))[:long_len]  # non-ASCII, deterministic
    body = _gen_invoice_like(rng)
    return f"{header}\n{long_line}\n{body}"


def _gen_mixed_text(rng: random.Random, *, unicode_mode: bool) -> str:
    blocks: list[str] = []
    for _ in range(rng.randint(2, 6)):
        if rng.random() < 0.5:
            blocks.append(_gen_invoice_like(rng))
        else:
            if unicode_mode and rng.random() < 0.35:
                blocks.append(_gen_unicode_long_text(rng, long_len=rng.randint(4000, 30000)))
            else:
                blocks.append(_rand_ascii(rng, rng.randint(40, 200)) + "\n")
    return "".join(blocks)


def _make_random_tree(
    root: Path, rng: random.Random, *, files: int, max_bytes: int, unicode_mode: bool
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for i in range(files):
        depth = rng.choice([0, 1, 2, 3])
        parts: list[str] = []
        for _ in range(depth):
            parts.append(rng.choice(["sub", "deep", "docs", "data", "tmp", "alpha", "beta"]))
            if rng.random() < 0.3:
                parts.append(str(rng.randint(0, 99)))
        fname = f"f{i:03d}_" + rng.choice(["a", "b", "c", "x", "y", "z"])
        ext = rng.choice([".txt", ".txt", ".txt", ".bin", ".dat"])
        rel = Path(*parts) / (fname + ext)
        p = root / rel

        if ext == ".txt":
            _write_text(p, _gen_mixed_text(rng, unicode_mode=unicode_mode))
        else:
            size = rng.randint(1, max_bytes)
            _write_bytes(p, os.urandom(size))

    # one guaranteed unicode + very long line file
    if unicode_mode:
        _write_text(root / "unicode_long.txt", _gen_unicode_long_text(rng, long_len=25000))


def _read_manifest_first_file_rec(manifest: Path) -> dict[str, Any]:
    with manifest.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(rec, dict)
                and rec.get("rel")
                and rec.get("archive")
                and rec.get("archive_offset") is not None
                and rec.get("archive_length") is not None
            ):
                return rec
    raise RuntimeError("Manifest: non trovo nessun file record con archive_offset/archive_length")


@dataclass
class StepResult:
    name: str
    ok: bool
    rc: int
    stdout: str
    stderr: str


def _ok(res: subprocess.CompletedProcess[str]) -> bool:
    return res.returncode == 0


def main() -> int:
    ap = argparse.ArgumentParser(description="GCC-OCF robust general smoke tests (file+dir)")
    ap.add_argument("--iters", type=int, default=10, help="Number of iterations (default: 10)")
    ap.add_argument("--seed", type=int, default=12345, help="Deterministic RNG seed (default: 12345)")
    ap.add_argument("--files", type=int, default=20, help="Files per directory tree (default: 20)")
    ap.add_argument("--max-bytes", type=int, default=200_000, help="Max binary file size (default: 200000)")
    ap.add_argument("--buckets", type=int, default=8, help="Buckets for dir pack (default: 8)")
    ap.add_argument("--unicode", action="store_true", help="Include unicode + very long lines in generated text")
    ap.add_argument("--keep", action="store_true", help="Keep temp workdir on exit")
    ap.add_argument("--workdir", type=Path, default=None, help="Optional workdir (default: temp)")
    ap.add_argument("--json-out", type=Path, default=None, help="Write JSON report to file")
    ap.add_argument("--python", dest="pyexe", default=sys.executable, help="Python executable to use")
    ns = ap.parse_args()

    rng = random.Random(ns.seed)

    if ns.workdir:
        wd = ns.workdir.resolve()
        wd.mkdir(parents=True, exist_ok=True)
        own_temp = False
    else:
        wd = Path(tempfile.mkdtemp(prefix="gcc-ocf-smoke-"))
        own_temp = True

    report: dict[str, Any] = {
        "ok": True,
        "seed": ns.seed,
        "iters": ns.iters,
        "files": ns.files,
        "max_bytes": ns.max_bytes,
        "buckets": ns.buckets,
        "unicode": bool(ns.unicode),
        "workdir": str(wd),
        "steps": [],
    }

    def add_step(name: str, res: subprocess.CompletedProcess[str]) -> None:
        step = StepResult(name=name, ok=_ok(res), rc=res.returncode, stdout=res.stdout, stderr=res.stderr)
        report["steps"].append(step.__dict__)
        if not step.ok:
            report["ok"] = False

    def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
        cmd = [ns.pyexe, "-m", "gcc_ocf.cli", *args]
        return _run(cmd)

    try:
        add_step("cli_version", run_cli("--version"))
        add_step("cli_help", run_cli("--help"))

        pipeline_spec = json.dumps(
            {
                "spec": "gcc-ocf.pipeline.v1",
                "name": "smoke-general",
                "layer": "split_text_nums",
                "codec": "zlib",
                "mbn": True,
                "stream_codecs": {"TEXT": "zlib", "NUMS": "num_v1"},
            },
            separators=(",", ":"),
        )
        add_step("file_pipeline_validate", run_cli("file", "pipeline-validate", pipeline_spec))

        for it in range(ns.iters):
            it_dir = wd / f"iter_{it:03d}"
            in_dir = it_dir / "in"
            out_dir = it_dir / "out"
            back_dir = it_dir / "back"
            in_dir.mkdir(parents=True, exist_ok=True)

            _make_random_tree(
                in_dir,
                rng,
                files=ns.files,
                max_bytes=ns.max_bytes,
                unicode_mode=bool(ns.unicode),
            )

            file_in = it_dir / "sample.txt"
            file_out = it_dir / "sample.gcc"
            file_back = it_dir / "sample.back.txt"
            _write_text(file_in, _gen_invoice_like(rng))

            add_step(
                f"it{it:03d}_file_compress_pipeline",
                run_cli("file", "compress", str(file_in), str(file_out), "--pipeline", pipeline_spec),
            )
            add_step(f"it{it:03d}_file_verify_json", run_cli("file", "verify", str(file_out), "--json"))
            add_step(
                f"it{it:03d}_file_verify_json_full",
                run_cli("file", "verify", str(file_out), "--json", "--full"),
            )
            add_step(f"it{it:03d}_file_decompress", run_cli("file", "decompress", str(file_out), str(file_back)))

            if file_back.is_file() and file_in.read_bytes() != file_back.read_bytes():
                report["ok"] = False
                report["steps"].append(
                    {
                        "name": f"it{it:03d}_file_diff",
                        "ok": False,
                        "rc": 1,
                        "stdout": "",
                        "stderr": "File roundtrip mismatch (bytes differ)",
                    }
                )

            add_step(
                f"it{it:03d}_dir_pack",
                run_cli("dir", "pack", str(in_dir), str(out_dir), "--buckets", str(ns.buckets)),
            )
            add_step(f"it{it:03d}_dir_verify_json", run_cli("dir", "verify", str(out_dir), "--json"))
            add_step(
                f"it{it:03d}_dir_verify_json_full",
                run_cli("dir", "verify", str(out_dir), "--json", "--full"),
            )
            add_step(f"it{it:03d}_dir_unpack", run_cli("dir", "unpack", str(out_dir), str(back_dir)))

            if back_dir.is_dir():
                dig_in = _tree_digest(in_dir)
                dig_back = _tree_digest(back_dir)
                if sorted(dig_in.keys()) != sorted(dig_back.keys()):
                    report["ok"] = False
                    report["steps"].append(
                        {
                            "name": f"it{it:03d}_dir_tree_paths",
                            "ok": False,
                            "rc": 1,
                            "stdout": "",
                            "stderr": "Directory roundtrip mismatch (paths differ)",
                        }
                    )
                else:
                    mism = [k for k in dig_in.keys() if dig_in[k] != dig_back.get(k)]
                    if mism:
                        report["ok"] = False
                        report["steps"].append(
                            {
                                "name": f"it{it:03d}_dir_tree_hash",
                                "ok": False,
                                "rc": 1,
                                "stdout": "",
                                "stderr": f"Directory roundtrip mismatch (sha256 differ): {mism[:5]}{'...' if len(mism)>5 else ''}",
                            }
                        )

            mf = out_dir / "manifest.jsonl"
            if mf.is_file():
                try:
                    rec = _read_manifest_first_file_rec(mf)
                    arch = out_dir / str(rec["archive"])
                    off = int(rec["archive_offset"])
                    ln = int(rec["archive_length"])
                    if arch.is_file() and ln > 0:
                        with arch.open("r+b") as fp:
                            fp.seek(off + min(10, max(0, ln - 1)))
                            b = fp.read(1)
                            fp.seek(fp.tell() - 1)
                            fp.write(bytes([(b[0] ^ 0x01) if b else 0x01]))
                        tam = run_cli("dir", "verify", str(out_dir), "--json", "--full")
                        ok_tam = tam.returncode == 13
                        report["steps"].append(
                            {
                                "name": f"it{it:03d}_tamper_verify_full_expect_13",
                                "ok": ok_tam,
                                "rc": tam.returncode,
                                "stdout": tam.stdout,
                                "stderr": tam.stderr,
                            }
                        )
                        if not ok_tam:
                            report["ok"] = False
                except Exception as e:
                    report["ok"] = False
                    report["steps"].append(
                        {
                            "name": f"it{it:03d}_tamper_setup",
                            "ok": False,
                            "rc": 1,
                            "stdout": "",
                            "stderr": f"Tamper test setup failed: {e}",
                        }
                    )

        if ns.json_out:
            ns.json_out.parent.mkdir(parents=True, exist_ok=True)
            ns.json_out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        print(
            json.dumps(
                {
                    "ok": report["ok"],
                    "seed": ns.seed,
                    "iters": ns.iters,
                    "unicode": bool(ns.unicode),
                    "workdir": str(wd),
                },
                ensure_ascii=False,
            )
        )
        return 0 if report["ok"] else 1

    finally:
        if own_temp and not ns.keep:
            shutil.rmtree(wd, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
