from __future__ import annotations

from pathlib import Path

import pytest

DATA_DIR = Path(__file__).parent / "data"


def _expected_dec_path(gcc_path: Path) -> Path:
    """Fixture convention: <name>.gcc -> <name>.dec.txt."""
    if not gcc_path.name.endswith(".gcc"):
        raise ValueError(f"Not a .gcc fixture: {gcc_path.name}")
    stem = gcc_path.name[: -len(".gcc")]
    return gcc_path.with_name(stem + ".dec.txt")


def _discover_golden_fixtures() -> list[Path]:
    out: list[Path] = []
    for p in sorted(DATA_DIR.glob("*.gcc")):
        exp = _expected_dec_path(p)
        if exp.is_file():
            out.append(p)
    if not out:
        raise RuntimeError(f"No .gcc golden fixtures found in {DATA_DIR}")
    return out


@pytest.mark.parametrize("gcc_path", _discover_golden_fixtures())
def test_d7_universal_decoder_matches_golden(
    gcc_path: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """d7 must decode all legacy fixtures (v1..v6 + v6+MBN) exactly."""
    from gcc_ocf.legacy.gcc_huffman import decompress_file_v7

    out_path = tmp_path / (gcc_path.name + ".out")
    decompress_file_v7(str(gcc_path), str(out_path))
    capsys.readouterr()  # silence legacy prints

    exp = _expected_dec_path(gcc_path)
    assert out_path.read_bytes() == exp.read_bytes()


V6_FIXTURES = [p for p in _discover_golden_fixtures() if ".v6." in p.name]


@pytest.mark.parametrize("gcc_path", V6_FIXTURES)
def test_verify_container_file_full_on_v6_fixtures(
    gcc_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """verify_container_file(full=True) should accept all v6 fixtures."""
    from gcc_ocf.verify import verify_container_file

    verify_container_file(gcc_path, full=True)
    capsys.readouterr()
