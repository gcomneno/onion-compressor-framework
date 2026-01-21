from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _run_cli(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run gcc-ocf CLI through a python -c wrapper.

    This avoids assuming the console-script entrypoint is installed.
    """
    cmd = [
        sys.executable,
        "-c",
        "from gcc_ocf.cli import main; raise SystemExit(main())",
        *args,
    ]
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
    )


def test_cli_file_roundtrip_bytes_zlib(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.gcc"
    back = tmp_path / "back.txt"

    data = "HELLO 123\nRIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\n"
    inp.write_text(data, encoding="utf-8")

    r = _run_cli("file", "compress", str(inp), str(out), "--layer", "bytes", "--codec", "zlib")
    assert r.returncode == 0, (r.stdout, r.stderr)

    r = _run_cli("file", "verify", str(out))
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "OK" in r.stdout

    r = _run_cli("file", "decompress", str(out), str(back))
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert back.read_text(encoding="utf-8") == data


def test_cli_pipeline_validate_and_use_inline_json(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.gcc"
    back = tmp_path / "back.txt"

    data = "FATTURA 1001\nRIGA ARTICOLO: vite M3 qty=10 prezzo=1.20\nTOTALE 12.00\n"
    inp.write_text(data, encoding="utf-8")

    spec = {
        "spec": "gcc-ocf.pipeline.v1",
        "name": "smoke",
        "layer": "split_text_nums",
        "codec": "zlib",
        "mbn": True,
        "stream_codecs": {"TEXT": "zlib", "NUMS": "num_v1"},
    }
    spec_arg = json.dumps(spec, separators=(",", ":"))

    r = _run_cli("file", "pipeline-validate", spec_arg)
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "OK" in r.stdout

    r = _run_cli("file", "compress", str(inp), str(out), "--pipeline", spec_arg)
    assert r.returncode == 0, (r.stdout, r.stderr)

    r = _run_cli("file", "decompress", str(out), str(back))
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert back.read_text(encoding="utf-8") == data


def test_cli_pipeline_validate_rejects_bad_json_exit_2() -> None:
    r = _run_cli("file", "pipeline-validate", "{}")
    assert r.returncode == 2
    assert "[gcc-ocf]" in r.stderr


def test_cli_dir_pack_unpack_verify_and_tamper_exit_13(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    back_dir = tmp_path / "back"
    in_dir.mkdir()

    (in_dir / "a.txt").write_text("HELLO 123\n", encoding="utf-8")
    (in_dir / "b.txt").write_text("HELLO 124\n", encoding="utf-8")

    r = _run_cli("dir", "pack", str(in_dir), str(out_dir), "--buckets", "4")
    assert r.returncode == 0, (r.stdout, r.stderr)

    r = _run_cli("dir", "verify", str(out_dir))
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "OK" in r.stdout

    r = _run_cli("dir", "unpack", str(out_dir), str(back_dir))
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert (back_dir / "a.txt").read_text(encoding="utf-8") == (in_dir / "a.txt").read_text(
        encoding="utf-8"
    )

    # Tamper: flip one byte inside a blob area as per manifest offsets.
    manifest = out_dir / "manifest.jsonl"
    recs = [
        json.loads(line)
        for line in manifest.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    file_rec = next(
        rr for rr in recs if isinstance(rr, dict) and rr.get("rel") and rr.get("archive")
    )

    arch = out_dir / str(file_rec["archive"])
    off = int(file_rec["archive_offset"])
    ln = int(file_rec["archive_length"])

    with arch.open("r+b") as fp:
        fp.seek(off + min(10, max(0, ln - 1)))
        b = fp.read(1)
        fp.seek(fp.tell() - 1)
        fp.write(bytes([(b[0] ^ 0x01) if b else 0x01]))

    r = _run_cli("dir", "verify", str(out_dir), "--full")
    assert r.returncode == 13
    assert "[gcc-ocf]" in r.stderr
