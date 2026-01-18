from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True)


def _flip_last_hex(s: str) -> str:
    if not s:
        return "0"
    last = s[-1]
    repl = "0" if last != "0" else "1"
    return s[:-1] + repl


@pytest.mark.p1
def test_mixed_full_verify_concat_mismatch_is_exit_13(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"

    (in_dir / "nested" / "deep").mkdir(parents=True, exist_ok=True)
    (in_dir / "nested" / "deep" / "empty.txt").write_bytes(b"")
    (in_dir / "nested" / "deep" / "nonempty.txt").write_text("x\n", encoding="utf-8")

    (in_dir / "bin").mkdir(parents=True, exist_ok=True)
    (in_dir / "bin" / "tiny.bin").write_bytes(b"\x00\x01\x02\x03")

    cp = _run(
        [
            "gcc-ocf",
            "dir",
            "pack",
            str(in_dir),
            str(out_dir),
            "--single-container-mixed",
        ]
    )
    assert cp.returncode == 0, (
        "dir pack (mixed) fallito\n"
        f"cmd: {cp.args}\n"
        f"returncode: {cp.returncode}\n"
        f"stdout:\n{cp.stdout}\n"
        f"stderr:\n{cp.stderr}\n"
    )

    idx_path = out_dir / "bundle_bin_index.json"
    idx = json.loads(idx_path.read_text(encoding="utf-8"))
    idx["concat_sha256"] = _flip_last_hex(str(idx.get("concat_sha256") or ""))
    idx_path.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

    cpv = _run(["gcc-ocf", "dir", "verify", str(out_dir), "--full", "--json"])
    assert cpv.returncode == 13, (
        "Tamper su mixed/full deve uscire con 13.\n"
        f"cmd: {cpv.args}\n"
        f"returncode: {cpv.returncode}\n"
        f"stdout:\n{cpv.stdout}\n"
        f"stderr:\n{cpv.stderr}\n"
    )
