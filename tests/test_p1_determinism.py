from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.p1


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_tree_mixed(root: Path) -> None:
    # deterministico, include vuoto + unicode + bin
    (root / "a").mkdir(parents=True, exist_ok=True)
    (root / "nested/deep").mkdir(parents=True, exist_ok=True)

    (root / "hello.txt").write_text("ciao\n", encoding="utf-8")
    (root / "a" / "unicø∂e.txt").write_text("Ω\nλ\n", encoding="utf-8")
    (root / "nested/deep/empty.txt").write_bytes(b"")

    # bin con NUL
    (root / "bin").mkdir(parents=True, exist_ok=True)
    (root / "bin" / "tiny.bin").write_bytes(b"\x00\x01\x02\x03\xff")
    (root / "bin" / "random_4k.bin").write_bytes(os.urandom(4096))


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True)


def assert_ok(cp: subprocess.CompletedProcess[str], msg: str) -> None:
    if cp.returncode != 0:
        raise AssertionError(
            f"{msg}\ncmd: {cp.args}\nrc={cp.returncode}\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}\n"
        )


def collect_dir_fingerprints(out_dir: Path) -> dict[str, str]:
    files = sorted([p for p in out_dir.rglob("*") if p.is_file()])
    fp: dict[str, str] = {}
    for p in files:
        rel = p.relative_to(out_dir).as_posix()
        fp[rel] = sha256_file(p)
    return fp


@pytest.mark.p1
def test_determinism_classic_pack_manifest_and_buckets(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    write_tree_mixed(in_dir)

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"

    cp1 = run(["gcc-ocf", "dir", "pack", str(in_dir), str(out1), "--buckets", "8"])
    assert_ok(cp1, "classic pack #1 fallito")
    cp2 = run(["gcc-ocf", "dir", "pack", str(in_dir), str(out2), "--buckets", "8"])
    assert_ok(cp2, "classic pack #2 fallito")

    m1 = (out1 / "manifest.jsonl").read_bytes()
    m2 = (out2 / "manifest.jsonl").read_bytes()
    assert m1 == m2, "manifest.jsonl deve essere identico (determinismo classic)"

    fp1 = collect_dir_fingerprints(out1)
    fp2 = collect_dir_fingerprints(out2)

    # autopick_report.json può includere timestamp o path; lo escludiamo dal determinismo hard.
    fp1.pop("autopick_report.json", None)
    fp2.pop("autopick_report.json", None)

    assert fp1 == fp2, "output classic (escluso autopick_report) deve essere identico"


@pytest.mark.p1
def test_determinism_mixed_single_container_indexes(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    write_tree_mixed(in_dir)

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"

    cp1 = run(["gcc-ocf", "dir", "pack", str(in_dir), str(out1), "--single-container-mixed"])
    assert_ok(cp1, "mixed pack #1 fallito")
    cp2 = run(["gcc-ocf", "dir", "pack", str(in_dir), str(out2), "--single-container-mixed"])
    assert_ok(cp2, "mixed pack #2 fallito")

    idx1_text = (out1 / "bundle_text_index.json").read_bytes()
    idx2_text = (out2 / "bundle_text_index.json").read_bytes()
    assert idx1_text == idx2_text, (
        "bundle_text_index.json deve essere identico (determinismo mixed)"
    )

    idx1_bin = (out1 / "bundle_bin_index.json").read_bytes()
    idx2_bin = (out2 / "bundle_bin_index.json").read_bytes()
    assert idx1_bin == idx2_bin, "bundle_bin_index.json deve essere identico (determinismo mixed)"
