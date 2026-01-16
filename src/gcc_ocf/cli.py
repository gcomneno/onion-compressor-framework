"""GCC Onion Compressor Framework (GCC-OCF) CLI.

This is the stable CLI entrypoint (console-script: ``gcc-ocf``).

UX policy:
  - The default CLI is *semantic* (layer/codec/options). No c7/d7 names.
  - Legacy modes remain available under ``gcc-ocf legacy ...``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from gcc_ocf.dir_pipeline_spec import DirPipelineSpecError, load_dir_pipeline_spec
from gcc_ocf.errors import GCCOCFError
from gcc_ocf.pipeline_spec import PipelineSpecError, load_pipeline_spec


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--debug", action="store_true", help="Show stack traces on errors")


def _run_legacy_huffman(argv: list[str]) -> int:
    from gcc_ocf.legacy.gcc_huffman import main as legacy_main

    return legacy_main(argv)


def _run_legacy_dir(argv: list[str]) -> int:
    from gcc_ocf.legacy.gcc_dir import main as legacy_main

    return legacy_main(argv)


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


def _semantic_file_verify(input_path: Path, *, full: bool) -> int:
    from gcc_ocf.verify import verify_container_file

    verify_container_file(input_path, full=full)
    print("OK")
    return 0


def _semantic_dir_verify(input_dir: Path, *, full: bool) -> int:
    from gcc_ocf.verify import verify_packed_dir

    verify_packed_dir(input_dir, full=full)
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
    jobs: int = 1,
) -> int:
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

    unpackdir(input_dir, restore_dir)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gcc-ocf", description="GCC Onion Compressor Framework (GCC-OCF)"
    )
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
        "pack", help="Pack a directory into an output directory (manifest + per-bucket .gca)"
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
    _add_common_args(p_pack)

    p_unpack = sub_dir.add_parser(
        "unpack", help="Unpack a packed output directory into a restore directory"
    )
    p_unpack.add_argument("input_dir", type=Path)
    p_unpack.add_argument("restore_dir", type=Path)
    _add_common_args(p_unpack)

    p_dv = sub_dir.add_parser("verify", help="Verify a packed output directory (manifest + GCA1)")
    p_dv.add_argument("input_dir", type=Path)
    p_dv.add_argument("--full", action="store_true", help="Recompute sha256 for blobs/resources")
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
                return _semantic_file_verify(ns.input, full=bool(ns.full))
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
                    jobs=ns.jobs,
                )
            if ns.dir_cmd == "unpack":
                return _semantic_dir_unpack(ns.input_dir, ns.restore_dir)
            if ns.dir_cmd == "verify":
                return _semantic_dir_verify(ns.input_dir, full=bool(ns.full))
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
        # Treat as usage/config error.
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
