from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_verify_json_ok_schema_print_function(capsys: pytest.CaptureFixture[str]) -> None:
    # We test the exact schema emitted on success without requiring a full valid container/dir.
    from gcc_ocf.cli import _print_verify_json  # type: ignore

    _print_verify_json("file", Path("x.bin"), full=False)
    out = capsys.readouterr().out.strip()
    obj = json.loads(out)

    assert obj["schema"] == "gcc-ocf.verify.v1"
    assert obj["ok"] is True
    assert obj["kind"] == "file"
    assert obj["target"].endswith("x.bin")
    assert obj["full"] is False
    assert isinstance(obj["version"], str) and obj["version"]


def test_verify_json_error_on_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.bin"
    cmd = [sys.executable, "-m", "gcc_ocf.cli", "file", "verify", str(missing), "--json"]

    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.returncode != 0

    # Must be JSON on stderr, not stdout.
    assert p.stdout.strip() == ""
    obj = json.loads(p.stderr.strip())

    assert obj["schema"] == "gcc-ocf.verify.v1"
    assert obj["ok"] is False
    assert obj["kind"] == "file"
    assert obj["target"] == str(missing)
    assert obj["full"] is False
    assert isinstance(obj["version"], str) and obj["version"]

    err = obj["error"]
    assert isinstance(err["type"], str) and err["type"]
    assert isinstance(err["category"], str) and err["category"]
    assert isinstance(err["message"], str)
    assert int(err["exit_code"]) == p.returncode
