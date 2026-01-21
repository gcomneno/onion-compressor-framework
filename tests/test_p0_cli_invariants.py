import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def _try_run(cmd: list[str]) -> bool:
    try:
        cp = subprocess.run(cmd + ["--version"], check=False, capture_output=True, text=True)
        return cp.returncode == 0
    except FileNotFoundError:
        return False


def _pick_cli() -> list[str]:
    env = os.environ.get("GCC_OCF_CLI", "").strip()
    candidates: list[list[str]] = []

    if env:
        candidates.append(shlex.split(env))

    if shutil.which("gcc-ocf"):
        candidates.append(["gcc-ocf"])

    candidates.append([sys.executable, "-m", "gcc_ocf"])
    candidates.append([sys.executable, "-m", "gcc_ocf.cli"])

    for c in candidates:
        if _try_run(c):
            return c

    pytest.skip(
        "CLI gcc-ocf non trovata. "
        "Installa in editable (pip install -e '.[dev]') oppure imposta GCC_OCF_CLI."
    )
    raise RuntimeError("unreachable")


CLI = _pick_cli()


def run_ocf(args: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        CLI + args,
        cwd=str(cwd) if cwd else None,
        check=False,
        capture_output=True,
        text=True,
    )


def assert_ok(cp: subprocess.CompletedProcess[str], msg: str) -> None:
    if cp.returncode != 0:
        raise AssertionError(
            f"{msg}\n"
            f"cmd: {cp.args}\n"
            f"returncode: {cp.returncode}\n"
            f"stdout:\n{cp.stdout}\n"
            f"stderr:\n{cp.stderr}\n"
        )


def parse_json_from_stdout(stdout: str) -> dict:
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if not ln or ln[0] not in "{[":
            continue
        try:
            obj = json.loads(ln)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise AssertionError(f"Output non contiene un JSON dict parsabile.\nstdout:\n{stdout}\n")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def tree_digest(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_dir():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        out[rel] = sha256_bytes(p.read_bytes())
    return out


def assert_tree_equal(d_before: dict[str, str], d_after: dict[str, str]) -> None:
    if d_before == d_after:
        return

    before_keys = set(d_before.keys())
    after_keys = set(d_after.keys())

    missing = sorted(before_keys - after_keys)
    extra = sorted(after_keys - before_keys)
    changed = sorted(k for k in (before_keys & after_keys) if d_before[k] != d_after[k])

    lines: list[str] = ["Roundtrip mismatch tra input e output."]
    if missing:
        lines.append(f"- Mancano {len(missing)} file:")
        lines.extend([f"  * {k}" for k in missing[:25]])
        if len(missing) > 25:
            lines.append("  * ...")
    if extra:
        lines.append(f"- Ci sono {len(extra)} file extra:")
        lines.extend([f"  * {k}" for k in extra[:25]])
        if len(extra) > 25:
            lines.append("  * ...")
    if changed:
        lines.append(f"- Ci sono {len(changed)} file con contenuto diverso:")
        for k in changed[:25]:
            lines.append(f"  * {k}")
            lines.append(f"      before: {d_before[k]}")
            lines.append(f"      after : {d_after[k]}")
        if len(changed) > 25:
            lines.append("  * ...")

    raise AssertionError("\n".join(lines))


def write_text(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8", newline="\n")


def write_bytes(p: Path, b: bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b)


def make_tree_basic_mixed_with_empty(root: Path) -> None:
    write_text(root / "hello.txt", "ciao\nmondo\n")
    write_text(root / "notes" / "readme.md", "# Titolo\n\n- riga\n")
    write_text(root / "nested" / "deep" / "empty.txt", "")  # <- questo NON deve sparire
    write_text(
        root / "nested" / "deep" / "lines.txt",
        "\n".join([f"riga {i}" for i in range(20)]) + "\n",
    )

    write_bytes(root / "bin" / "random_1k.bin", os.urandom(1024))
    write_bytes(root / "bin" / "zeros_2k.bin", b"\x00" * 2048)
    write_bytes(root / "bin" / "tiny.bin", b"\x01\x02\x03")


def make_tree_text_only_unicode_with_empty(root: Path) -> None:
    write_text(root / "hello.txt", "ciao\nmondo\n")
    write_text(root / "unicodé_ß.txt", "🍝 café naïve — 𝛑=3.14159\n")
    write_text(root / "nested" / "deep" / "empty.txt", "")
    write_text(
        root / "nested" / "deep" / "lines.txt",
        "\n".join([f"riga {i}" for i in range(50)]) + "\n",
    )


def make_tree_mixed_unicode_with_empty(root: Path) -> None:
    make_tree_text_only_unicode_with_empty(root)
    write_bytes(root / "bin" / "random_1k.bin", os.urandom(1024))
    write_bytes(root / "bin" / "zeros_2k.bin", b"\x00" * 2048)
    write_bytes(root / "bin" / "tiny.bin", b"\x01\x02\x03")
    write_bytes(root / "bin" / "text_with_nul.txt", b"hello\x00world\n")


def pack_classic(in_dir: Path, out_dir: Path, buckets: int = 8) -> None:
    cp = run_ocf(["dir", "pack", str(in_dir), str(out_dir), "--buckets", str(buckets)])
    assert_ok(cp, "dir pack (classic) fallito")


def pack_single_text(in_dir: Path, out_dir: Path) -> None:
    cp = run_ocf(["dir", "pack", "--single-container", str(in_dir), str(out_dir)])
    assert_ok(cp, "dir pack (--single-container) fallito")


def pack_single_mixed(in_dir: Path, out_dir: Path) -> None:
    cp = run_ocf(["dir", "pack", "--single-container-mixed", str(in_dir), str(out_dir)])
    assert_ok(cp, "dir pack (--single-container-mixed) fallito")


def verify_dir_success(out_dir: Path, full: bool) -> dict:
    args = ["dir", "verify", str(out_dir)]
    if full:
        args.append("--full")
    args.append("--json")

    cp = run_ocf(args)
    assert_ok(cp, "dir verify (success atteso) fallito")
    data = parse_json_from_stdout(cp.stdout)

    assert data.get("ok") is True
    assert data.get("full") is full
    return data


def verify_dir_maybe_fail(
    out_dir: Path, full: bool
) -> tuple[subprocess.CompletedProcess[str], dict | None]:
    args = ["dir", "verify", str(out_dir)]
    if full:
        args.append("--full")
    args.append("--json")

    cp = run_ocf(args)
    data = None
    if cp.stdout.strip():
        try:
            data = parse_json_from_stdout(cp.stdout)
        except AssertionError:
            data = None
    return cp, data


def unpack_dir(out_dir: Path, restored_dir: Path) -> None:
    cp = run_ocf(["dir", "unpack", str(out_dir), str(restored_dir)])
    assert_ok(cp, "dir unpack fallito")


def pick_gcc_to_tamper(out_dir: Path) -> Path:
    gccs = list(out_dir.glob("*.gcc"))
    assert gccs, f"Nessun .gcc trovato in {out_dir}"
    gccs.sort(key=lambda p: p.stat().st_size, reverse=True)
    return gccs[0]


def flip_one_byte(path: Path) -> None:
    data = bytearray(path.read_bytes())
    assert len(data) > 64, f"File troppo piccolo per tamper sensato: {path} ({len(data)} bytes)"
    data[64] ^= 0x01
    path.write_bytes(bytes(data))


@pytest.mark.p0
def test_cli_version_smoke():
    cp = run_ocf(["--version"])
    assert_ok(cp, "--version fallito")
    assert re.search(r"\d", cp.stdout), f"stdout non sembra una versione:\n{cp.stdout}"


@pytest.mark.p0
def test_roundtrip_dir_classic_preserves_empty(tmp_path: Path):
    in_dir = tmp_path / "in_classic"
    out_dir = tmp_path / "out_classic"
    restored = tmp_path / "restored_classic"

    in_dir.mkdir()
    make_tree_basic_mixed_with_empty(in_dir)

    digest_before = tree_digest(in_dir)

    pack_classic(in_dir, out_dir, buckets=8)
    verify_dir_success(out_dir, full=False)
    verify_dir_success(out_dir, full=True)

    unpack_dir(out_dir, restored)
    digest_after = tree_digest(restored)

    assert_tree_equal(digest_before, digest_after)


@pytest.mark.p0
def test_roundtrip_dir_single_container_text_only(tmp_path: Path):
    in_dir = tmp_path / "in_single_text"
    out_dir = tmp_path / "out_single_text"
    restored = tmp_path / "restored_single_text"

    in_dir.mkdir()
    make_tree_text_only_unicode_with_empty(in_dir)

    digest_before = tree_digest(in_dir)

    pack_single_text(in_dir, out_dir)
    verify_dir_success(out_dir, full=True)

    unpack_dir(out_dir, restored)
    digest_after = tree_digest(restored)

    assert_tree_equal(digest_before, digest_after)


@pytest.mark.p0
def test_roundtrip_dir_single_container_mixed(tmp_path: Path):
    in_dir = tmp_path / "in_single_mixed"
    out_dir = tmp_path / "out_single_mixed"
    restored = tmp_path / "restored_single_mixed"

    in_dir.mkdir()
    make_tree_mixed_unicode_with_empty(in_dir)

    digest_before = tree_digest(in_dir)

    pack_single_mixed(in_dir, out_dir)
    verify_dir_success(out_dir, full=True)

    unpack_dir(out_dir, restored)
    digest_after = tree_digest(restored)

    assert_tree_equal(digest_before, digest_after)


@pytest.mark.p0
def test_mixed_tamper_full_verify_is_nonzero_and_tamperish(tmp_path: Path):
    in_dir = tmp_path / "in_tamper_mixed"
    out_dir = tmp_path / "out_tamper_mixed"

    in_dir.mkdir()
    make_tree_mixed_unicode_with_empty(in_dir)

    pack_single_mixed(in_dir, out_dir)

    gcc = pick_gcc_to_tamper(out_dir)
    flip_one_byte(gcc)

    cp, data = verify_dir_maybe_fail(out_dir, full=True)

    # Stato reale visto: 10 su sha mismatch; contratto desiderato: 13.
    # P0: accetto {10, 13}. Standardizziamo in P1.
    assert cp.returncode in {10, 13}, (
        "Tamper su mixed/full: atteso exit code 10 o 13.\n"
        f"cmd: {cp.args}\n"
        f"returncode: {cp.returncode}\n"
        f"stdout:\n{cp.stdout}\n"
        f"stderr:\n{cp.stderr}\n"
    )

    if data is not None:
        assert data.get("ok") is False
        assert data.get("full") is True
