"""GCC Onion Compressor Framework (GCC-OCF) CLI.

This is the stable CLI entrypoint (console-script: ``gcc-ocf``).

UX policy:
  - The default CLI is *semantic* (layer/codec/options). No c7/d7 names.
  - Legacy modes remain available under ``gcc-ocf legacy ...``.

Notes:
  - --version is supported at top-level.
  - verify supports --json (machine-readable success output).
  - Directory pack supports:
      * classic manifest + GCA1 buckets (default)
      * --single-container (TEXT-only)
      * --single-container-mixed (TEXT semantic + BIN generic)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from gcc_ocf.dir_pipeline_spec import DirPipelineSpecError, load_dir_pipeline_spec
from gcc_ocf.errors import GCCOCFError
from gcc_ocf.pipeline_spec import PipelineSpecError, load_pipeline_spec


def _pkg_version() -> str:
    try:
        from importlib.metadata import PackageNotFoundError, version  # py3.8+

        try:
            return version("gcc-ocf")
        except PackageNotFoundError:
            # editable install but script invoked from source, or metadata missing
            return "0+unknown"
    except Exception:
        return "0+unknown"


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--debug", action="store_true", help="Show stack traces on errors")


def _run_legacy_huffman(argv: list[str]) -> int:
    from gcc_ocf.legacy.gcc_huffman import main as legacy_main

    return legacy_main(argv)


def _run_legacy_dir(argv: list[str]) -> int:
    from gcc_ocf.legacy.gcc_dir import main as legacy_main

    return legacy_main(argv)


def _print_verify_json(kind: str, target: Path, *, full: bool) -> None:
    import json

    print(
        json.dumps(
            {
                "schema": "gcc-ocf.verify.v1",
                "ok": True,
                "kind": kind,
                "target": str(target),
                "full": bool(full),
                "version": _pkg_version(),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


def _print_verify_json_error(kind: str, target: Path, *, full: bool, err_type: str, message: str) -> None:
    """Emit stable JSON on stderr for verify errors when --json is used."""
    import json

    obj = {
        "schema": "gcc-ocf.verify.v1",
        "ok": False,
        "kind": kind,
        "target": str(target),
        "full": bool(full),
        "version": _pkg_version(),
        "error": {"type": err_type, "message": message},
    }
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True), file=sys.stderr)


def _semantic_file_compress(
    input_path: Path,
    output_path: Path,
    layer: str,
    codec: str,
    stream_codecs: str | None,
    force_mbn: bool,
) -> int:
    """Semantic lossless compress.

    - Single-stream: Container v6
    - Multi-stream: v6 + payload MBN (inside v6 payload)
    """
    from gcc_ocf.legacy.gcc_huffman import compress_file_v6, compress_file_v7

    layer_norm = layer.strip()
    wants_mbn = (
        force_mbn
        or (layer_norm in {"split_text_nums", "tpl_lines_v0"})
        or (stream_codecs is not None)
    )

    if wants_mbn:
        compress_file_v7(
            str(input_path),
            str(output_path),
            layer_id=layer_norm,
            codec_id=codec.strip(),
            stream_codecs_spec=stream_codecs,
        )
    else:
        compress_file_v6(
            str(input_path),
            str(output_path),
            layer_id=layer_norm,
            codec_id=codec.strip(),
        )

    return 0


def _semantic_file_compress_from_pipeline(
    input_path: Path, output_path: Path, pipeline_arg: str
) -> int:
    """Semantic lossless compress using a pipeline spec (v1).

    The pipeline spec is the *source of truth* for the encode plan.
    """
    from gcc_ocf.legacy.gcc_huffman import compress_file_v6, compress_file_v7

    spec = load_pipeline_spec(pipeline_arg)
    layer_id = spec.layer
    codec_id = spec.codec
    stream_codecs = spec.stream_codecs_spec()

    wants_mbn = bool(spec.mbn)
    if spec.mbn is None:
        wants_mbn = (layer_id in {"split_text_nums", "tpl_lines_v0"}) or (stream_codecs is not None)

    if wants_mbn:
        compress_file_v7(
            str(input_path),
            str(output_path),
            layer_id=layer_id,
            codec_id=codec_id,
            stream_codecs_spec=stream_codecs,
        )
    else:
        compress_file_v6(
            str(input_path),
            str(output_path),
            layer_id=layer_id,
            codec_id=codec_id,
        )

    return 0


def _semantic_file_decompress(input_path: Path, output_path: Path) -> int:
    """Semantic lossless decompress.

    Uses the universal decoder (v1..v6 + MBN).
    """
    from gcc_ocf.legacy.gcc_huffman import decompress_file_v7

    decompress_file_v7(str(input_path), str(output_path))
    return 0


def _semantic_extract_numbers_only(input_path: Path, output_path: Path) -> int:
    from gcc_ocf.legacy.gcc_huffman import extract_numbers_only

    extract_numbers_only(str(input_path), str(output_path))
    return 0


def _semantic_extract_show(input_path: Path) -> int:
    from gcc_ocf.legacy.gcc_huffman import extract_show

    extract_show(str(input_path))
    return 0


def _semantic_file_pipeline_validate(pipeline_arg: str) -> int:
    # load is the validation
    load_pipeline_spec(pipeline_arg)
    print("OK")
    return 0


def _semantic_file_verify(input_path: Path, *, full: bool, json_out: bool) -> int:
    from gcc_ocf.verify import verify_container_file

    try:
        verify_container_file(input_path, full=full)
    except FileNotFoundError:
        if json_out:
            _print_verify_json_error(
                "file",
                input_path,
                full=full,
                err_type="FileNotFound",
                message=f"file non trovato: {input_path}",
            )
            return 2
        raise
    except Exception as e:
        # For --json we must emit JSON on stderr (stable schema).
        if json_out:
            _print_verify_json_error(
                "file",
                input_path,
                full=full,
                err_type=type(e).__name__,
                message=str(e),
            )
            return 10
        raise

    if json_out:
        _print_verify_json("file", input_path, full=full)
    else:
        print("OK")
    return 0



def _semantic_dir_verify(input_dir: Path, *, full: bool, json_out: bool) -> int:
    """Verify a directory output.

    Supports:
      - classic packed-dir (manifest + GCA1 buckets)
      - --single-container output dir
      - --single-container-mixed output dir
    """
    from gcc_ocf.single_container_dir import is_single_container_dir, verify_single_container_dir
    from gcc_ocf.single_container_mixed_dir import (
        is_single_container_mixed_dir,
        verify_single_container_mixed_dir,
    )
    from gcc_ocf.verify import verify_packed_dir

    if is_single_container_mixed_dir(input_dir):
        verify_single_container_mixed_dir(input_dir, full=full)
        kind = "dir-mixed"
    elif is_single_container_dir(input_dir):
        verify_single_container_dir(input_dir, full=full)
        kind = "dir-single"
    else:
        verify_packed_dir(input_dir, full=full)
        kind = "dir"

    if json_out:
        _print_verify_json(kind, input_dir, full=full)
    else:
        print("OK")
    return 0


def _semantic_dir_pipeline_validate(pipeline_arg: str) -> int:
    load_dir_pipeline_spec(pipeline_arg)
    print("OK")
    return 0


def _semantic_dir_pack(
    input_dir: Path,
    output_dir: Path,
    *,
    buckets: int | None,
    pipeline_arg: str | None,
    single_container: bool = False,
    single_container_mixed: bool = False,
    keep_concat: bool = False,
    jobs: int = 1,
) -> int:
    if single_container_mixed:
        from gcc_ocf.single_container_mixed_dir import pack_single_container_mixed_dir

        pack_single_container_mixed_dir(input_dir, output_dir, keep_concat=keep_concat)
        return 0

    if single_container:
        from gcc_ocf.single_container_dir import pack_single_container_dir

        pack_single_container_dir(input_dir, output_dir, keep_concat=keep_concat)
        return 0

    from gcc_ocf.legacy.gcc_dir import packdir

    dir_spec = load_dir_pipeline_spec(pipeline_arg) if pipeline_arg else None
    # precedence: CLI --buckets > spec.buckets > default 16
    b = (
        int(buckets)
        if buckets is not None
        else (int(dir_spec.buckets) if dir_spec and dir_spec.buckets is not None else 16)
    )
    packdir(input_dir, output_dir, buckets=b, dir_spec=dir_spec, jobs=int(jobs))
    return 0


def _semantic_dir_unpack(input_dir: Path, restore_dir: Path) -> int:
    from gcc_ocf.legacy.gcc_dir import unpackdir
    from gcc_ocf.single_container_dir import is_single_container_dir, unpack_single_container_dir
    from gcc_ocf.single_container_mixed_dir import (
        is_single_container_mixed_dir,
        unpack_single_container_mixed_dir,
    )

    if is_single_container_mixed_dir(input_dir):
        unpack_single_container_mixed_dir(input_dir, restore_dir)
    elif is_single_container_dir(input_dir):
        unpack_single_container_dir(input_dir, restore_dir)
    else:
        unpackdir(input_dir, restore_dir)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gcc-ocf", description="GCC Onion Compressor Framework (GCC-OCF)"
    )
    p.add_argument("--version", action="version", version=f"gcc-ocf {_pkg_version()}")
    sub = p.add_subparsers(dest="cmd", required=True)

    # file ...
    p_file = sub.add_parser("file", help="File operations (lossless + extract)")
    sub_file = p_file.add_subparsers(dest="file_cmd", required=True)

    p_c = sub_file.add_parser("compress", help="Lossless compress (semantic)")
    p_c.add_argument("input", type=Path)
    p_c.add_argument("output", type=Path)
    p_c.add_argument(
        "--pipeline",
        default=None,
        help=(
            "Pipeline spec (JSON). Use '@file.json' to load from file, or pass JSON inline. "
            "When set, --layer/--codec/--stream-codecs/--mbn are ignored."
        ),
    )
    p_c.add_argument(
        "--layer",
        default="bytes",
        help=(
            "Layer id (e.g. bytes, vc0, split_text_nums, tpl_lines_v0). "
            "Comma-separated list allowed for v6 auto-pick (e.g. bytes,words_it)."
        ),
    )
    p_c.add_argument(
        "--codec",
        default="zlib",
        help=(
            "Codec id (e.g. zlib, raw, huffman, zstd_tight, num_v1). "
            "Comma-separated list allowed for v6 auto-pick."
        ),
    )
    p_c.add_argument(
        "--stream-codecs",
        default=None,
        help=(
            "Per-stream codec map for MBN, e.g. 'TEXT:zlib,NUMS:num_v1'. If set, MBN is enabled."
        ),
    )
    p_c.add_argument(
        "--mbn",
        action="store_true",
        help="Force MBN multi-stream payload (v6+MBN). Usually auto-enabled by layer/--stream-codecs.",
    )
    _add_common_args(p_c)

    p_v = sub_file.add_parser("pipeline-validate", help="Validate a file pipeline spec (v1)")
    p_v.add_argument("pipeline", help="Pipeline spec JSON (@file.json or inline JSON)")
    _add_common_args(p_v)

    p_fv = sub_file.add_parser("verify", help="Verify a compressed container file")
    p_fv.add_argument("input", type=Path)
    p_fv.add_argument("--full", action="store_true", help="Recompute/validate full payload")
    p_fv.add_argument("--json", action="store_true", help="Emit machine-readable JSON on success")
    _add_common_args(p_fv)

    p_d = sub_file.add_parser("decompress", help="Lossless decompress (universal v1..v6+MBN)")
    p_d.add_argument("input", type=Path)
    p_d.add_argument("output", type=Path)
    _add_common_args(p_d)

    p_x = sub_file.add_parser("extract", help="LOSSY extract (semantic)")
    p_x.add_argument("kind", choices=["numbers_only"], help="Extractor kind")
    p_x.add_argument("input", type=Path)
    p_x.add_argument("output", type=Path)
    _add_common_args(p_x)

    p_xs = sub_file.add_parser("extract-show", help="Show an EXTRACT container")
    p_xs.add_argument("input", type=Path)
    _add_common_args(p_xs)

    # dir ...
    p_dir = sub.add_parser(
        "dir", help="Directory workflow (pack/unpack, GCA1, bucketing, autopick)"
    )
    sub_dir = p_dir.add_subparsers(dest="dir_cmd", required=True)

    p_dir_v = sub_dir.add_parser(
        "pipeline-validate", help="Validate a directory pipeline spec (v1)"
    )
    p_dir_v.add_argument("pipeline", help="Dir pipeline spec JSON (@file.json or inline JSON)")
    _add_common_args(p_dir_v)

    p_pack = sub_dir.add_parser(
        "pack",
        help="Pack a directory into an output directory (manifest + per-bucket .gca) or single-container modes",
    )
    p_pack.add_argument("input_dir", type=Path)
    p_pack.add_argument("output_dir", type=Path)
    p_pack.add_argument(
        "--pipeline",
        default=None,
        help="Directory pipeline spec JSON (@file.json or inline JSON). When set, controls candidate pools/autopick/resources.",
    )
    p_pack.add_argument(
        "--buckets",
        type=int,
        default=None,
        help="Override bucket count (default: spec.buckets or 16)",
    )
    p_pack.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Parallel jobs for compression (default: 1). Only small files are parallelized to cap RAM.",
    )

    sc_group = p_pack.add_mutually_exclusive_group()
    sc_group.add_argument(
        "--single-container",
        action="store_true",
        help=(
            "Pack the whole directory as ONE container file (bundle.gcc) using the winning text pipeline "
            "(concat + split_text_nums + MBN). Text-only: non-UTF8/binary files cause a UsageError."
        ),
    )
    sc_group.add_argument(
        "--single-container-mixed",
        action="store_true",
        help=(
            "Pack the directory as TWO bundles: "
            "TEXT (concat + split_text_nums + MBN) and BIN (bytes + zstd if available else zlib)."
        ),
    )

    p_pack.add_argument(
        "--keep-concat",
        action="store_true",
        help="(single-container*) Keep the intermediate bundle*.concat file(s) in the output directory",
    )

    _add_common_args(p_pack)

    p_unpack = sub_dir.add_parser(
        "unpack", help="Unpack a packed output directory into a restore directory"
    )
    p_unpack.add_argument("input_dir", type=Path)
    p_unpack.add_argument("restore_dir", type=Path)
    _add_common_args(p_unpack)

    p_dv = sub_dir.add_parser(
        "verify",
        help="Verify a packed output directory (manifest + GCA1) or a single-container dir",
    )
    p_dv.add_argument("input_dir", type=Path)
    p_dv.add_argument("--full", action="store_true", help="Recompute sha256 for blobs/resources")
    p_dv.add_argument("--json", action="store_true", help="Emit machine-readable JSON on success")
    _add_common_args(p_dv)

    # legacy ...
    p_legacy = sub.add_parser(
        "legacy", help="Legacy CLI passthrough (c1..c7/d1..d7, packdir/unpackdir, ...) "
    )
    sub_legacy = p_legacy.add_subparsers(dest="legacy_cmd", required=True)

    p_l_file = sub_legacy.add_parser("file", help="Legacy file CLI (same as old gcc_huffman.py)")
    p_l_file.add_argument(
        "args", nargs=argparse.REMAINDER, help="Legacy args, e.g. c7 in out [layer] [codec] ..."
    )
    _add_common_args(p_l_file)

    p_l_dir = sub_legacy.add_parser("dir", help="Legacy directory CLI (same as old gcc_dir.py)")
    p_l_dir.add_argument("args", nargs=argparse.REMAINDER, help="Legacy args, e.g. packdir IN OUT")
    _add_common_args(p_l_dir)

    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    p = build_parser()
    ns = p.parse_args(argv)

    try:
        if ns.cmd == "file":
            if ns.file_cmd == "compress":
                if ns.pipeline is not None:
                    return _semantic_file_compress_from_pipeline(
                        input_path=ns.input,
                        output_path=ns.output,
                        pipeline_arg=str(ns.pipeline),
                    )
                return _semantic_file_compress(
                    input_path=ns.input,
                    output_path=ns.output,
                    layer=ns.layer,
                    codec=ns.codec,
                    stream_codecs=ns.stream_codecs,
                    force_mbn=bool(ns.mbn),
                )
            if ns.file_cmd == "pipeline-validate":
                return _semantic_file_pipeline_validate(str(ns.pipeline))
            if ns.file_cmd == "verify":
                return _semantic_file_verify(ns.input, full=bool(ns.full), json_out=bool(ns.json))
            if ns.file_cmd == "decompress":
                return _semantic_file_decompress(ns.input, ns.output)
            if ns.file_cmd == "extract":
                if ns.kind == "numbers_only":
                    return _semantic_extract_numbers_only(ns.input, ns.output)
                raise ValueError(f"Extractor non supportato: {ns.kind}")
            if ns.file_cmd == "extract-show":
                return _semantic_extract_show(ns.input)
            raise AssertionError("unreachable")

        if ns.cmd == "dir":
            if ns.dir_cmd == "pipeline-validate":
                return _semantic_dir_pipeline_validate(str(ns.pipeline))
            if ns.dir_cmd == "pack":
                return _semantic_dir_pack(
                    ns.input_dir,
                    ns.output_dir,
                    buckets=ns.buckets,
                    pipeline_arg=ns.pipeline,
                    single_container=bool(getattr(ns, "single_container", False)),
                    single_container_mixed=bool(getattr(ns, "single_container_mixed", False)),
                    keep_concat=bool(getattr(ns, "keep_concat", False)),
                    jobs=ns.jobs,
                )
            if ns.dir_cmd == "unpack":
                return _semantic_dir_unpack(ns.input_dir, ns.restore_dir)
            if ns.dir_cmd == "verify":
                return _semantic_dir_verify(
                    ns.input_dir, full=bool(ns.full), json_out=bool(ns.json)
                )
            raise AssertionError("unreachable")

        if ns.cmd == "legacy":
            if ns.legacy_cmd == "file":
                if not ns.args:
                    raise ValueError("legacy file: mancano argomenti (es: c7 in out ...)")
                return _run_legacy_huffman(["gcc-ocf", *ns.args])
            if ns.legacy_cmd == "dir":
                if not ns.args:
                    raise ValueError("legacy dir: mancano argomenti (es: packdir IN OUT)")
                return _run_legacy_dir(["gcc-ocf", *ns.args])
            raise AssertionError("unreachable")

        raise AssertionError("unreachable")

    except SystemExit:
        raise
    except (PipelineSpecError, DirPipelineSpecError) as e:
        if getattr(ns, "debug", False):
            raise
        print(f"[gcc-ocf] {e}", file=sys.stderr)
        return 2
    except GCCOCFError as e:
        if getattr(ns, "debug", False):
            raise
        print(f"[gcc-ocf] {e}", file=sys.stderr)
        return int(getattr(e, "exit_code", 10) or 10)
    except Exception as e:
        if getattr(ns, "debug", False):
            raise
        print(f"[gcc-ocf] error: {e}", file=sys.stderr)
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
