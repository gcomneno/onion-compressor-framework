from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.p2, pytest.mark.bench]


if os.environ.get("RUN_P2_SMOKE") != "1":
    pytest.skip("set RUN_P2_SMOKE=1 to enable P2 bench smoke test", allow_module_level=True)


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True)


def test_p2_run_bench_smoke(tmp_path: Path) -> None:
    out_root = tmp_path / "p2_data"
    out_root.mkdir(parents=True, exist_ok=True)

    gen = _run(
        [
            "python3",
            "tools/p2/bench_dataset_gen.py",
            "--out",
            str(out_root),
            "--preset",
            "tiny_smoke",
        ],
        cwd=Path.cwd(),
    )
    assert gen.returncode == 0, f"dataset_gen failed\nstdout:\n{gen.stdout}\nstderr:\n{gen.stderr}"

    ds = out_root / "tiny_smoke"
    assert (ds / "in").is_dir()

    bench = _run(["bash", "tools/p2/run_bench.sh", str(ds), "--buckets", "4"], cwd=Path.cwd())
    assert bench.returncode == 0, (
        f"run_bench failed\nstdout:\n{bench.stdout}\nstderr:\n{bench.stderr}"
    )
