from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.p2


def _read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines, f"CSV vuoto: {csv_path}"
    header = lines[0].split(",")
    rows: list[dict[str, str]] = []
    for ln in lines[1:]:
        if not ln.strip():
            continue
        parts = ln.split(",")
        # note può contenere virgole? nel nostro runner no: lo teniamo semplice
        if len(parts) != len(header):
            raise AssertionError(f"CSV malformato: {csv_path}\nLINE: {ln}")
        rows.append(dict(zip(header, parts, strict=True)))
    return rows


@pytest.mark.skipif(os.environ.get("RUN_P2_SMOKE") != "1", reason="set RUN_P2_SMOKE=1 to enable")
def test_p2_runner_marks_single_as_na_on_mixed_input(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runner = repo_root / "tools" / "p2" / "run_bench.sh"
    assert runner.is_file(), f"manca runner: {runner}"

    in_dir = tmp_path / "in_mixed"
    in_dir.mkdir(parents=True)

    (in_dir / "hello.txt").write_text("ciao\n", encoding="utf-8")
    # BIN: include NUL -> non text-only
    (in_dir / "bin.dat").write_bytes(b"\x00\x01\x02\x03\xff")

    out_root = tmp_path / "bench_out"
    out_root.mkdir(parents=True)

    env = os.environ.copy()
    env["OCF_P2_OUT"] = str(out_root)

    cmd = [
        "bash",
        str(runner),
        str(in_dir),
        "--buckets",
        "2",
        "--modes",
        "classic,single,mixed",
        "--skip-verify",
        "--skip-unpack",
        "--timeout",
        "60",
    ]
    cp = subprocess.run(cmd, text=True, capture_output=True, env=env)
    assert cp.returncode == 0, f"runner fallito\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"

    # Prendi la directory run più recente
    runs = sorted([p for p in out_root.iterdir() if p.is_dir()], key=lambda p: p.name)
    assert runs, f"nessun run creato in {out_root}\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"
    run_dir = runs[-1]
    csv_path = run_dir / "bench.csv"
    assert csv_path.is_file(), f"manca bench.csv in {run_dir}"

    rows = _read_csv_rows(csv_path)

    def pick(mode: str, step: str) -> dict[str, str]:
        for r in rows:
            if r["mode"] == mode and r["step"] == step:
                return r
        raise AssertionError(f"manca riga mode={mode} step={step} in {csv_path}")

    r_classic = pick("classic", "pack")
    r_mixed = pick("mixed", "pack")
    r_single = pick("single", "pack")

    assert r_classic["rc"] == "0", f"classic pack deve riuscire: {r_classic}"
    assert r_mixed["rc"] == "0", f"mixed pack deve riuscire: {r_mixed}"

    assert r_single["rc"] == "NA", f"single su input mixed deve essere NA (non FAIL): {r_single}"
    assert r_single["note"].startswith("NA:"), f"nota single deve iniziare con NA:: {r_single}"
