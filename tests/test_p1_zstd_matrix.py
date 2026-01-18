from __future__ import annotations

import hashlib
import json
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


def tree_digest(root: Path) -> dict[str, str]:
    d: dict[str, str] = {}
    for p in sorted([p for p in root.rglob("*") if p.is_file()]):
        rel = p.relative_to(root).as_posix()
        d[rel] = sha256_file(p)
    return d


def write_tree_mixed(root: Path) -> None:
    (root / "t").mkdir(parents=True, exist_ok=True)
    (root / "t" / "hello.txt").write_text("ciao\n", encoding="utf-8")
    (root / "b").mkdir(parents=True, exist_ok=True)
    # bin con NUL -> deve andare nel bundle_bin
    (root / "b" / "bin.dat").write_bytes(b"\x00\x01\x02" + os.urandom(4096))


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True)


def assert_ok(cp: subprocess.CompletedProcess[str], msg: str) -> None:
    if cp.returncode != 0:
        raise AssertionError(
            f"{msg}\ncmd: {cp.args}\nrc={cp.returncode}\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}\n"
        )


def has_zstd() -> bool:
    try:
        import zstandard  # type: ignore  # noqa: F401
    except Exception:
        return False
    return True


@pytest.mark.p1
def test_mixed_bin_codec_matches_zstd_availability_and_roundtrips(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    restored = tmp_path / "restored"
    in_dir.mkdir()
    write_tree_mixed(in_dir)

    before = tree_digest(in_dir)

    cp = run(["gcc-ocf", "dir", "pack", str(in_dir), str(out_dir), "--single-container-mixed"])
    assert_ok(cp, "mixed pack fallito")

    idx = json.loads((out_dir / "bundle_bin_index.json").read_text(encoding="utf-8"))
    codec = idx.get("codec_used")
    assert codec in {"zstd", "zlib"}, f"codec_used inatteso: {codec}"

    if has_zstd():
        assert codec == "zstd", "zstd presente ma codec_used non è zstd"
    else:
        assert codec == "zlib", "zstd assente ma codec_used non è zlib"

    cpv = run(["gcc-ocf", "dir", "verify", str(out_dir), "--full"])
    assert_ok(cpv, "mixed verify --full fallito")

    cpu = run(["gcc-ocf", "dir", "unpack", str(out_dir), str(restored)])
    assert_ok(cpu, "mixed unpack fallito")

    after = tree_digest(restored)
    assert before == after, "roundtrip mixed deve preservare contenuti"
