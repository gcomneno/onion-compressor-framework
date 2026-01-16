from __future__ import annotations

import json
from pathlib import Path

import pytest

from gcc_ocf.pipeline_spec import PipelineSpecError, load_pipeline_spec


def test_pipeline_inline_minimal() -> None:
    obj = {
        "spec": "gcc-ocf.pipeline.v1",
        "name": "bytes+zlib",
        "layer": "bytes",
        "codec": "zlib",
    }
    spec = load_pipeline_spec(json.dumps(obj))
    assert spec.layer == "bytes"
    assert spec.codec == "zlib"
    assert spec.stream_codecs is None
    assert spec.mbn is None


def test_pipeline_stream_codecs_ordering() -> None:
    obj = {
        "spec": "gcc-ocf.pipeline.v1",
        "name": "split_text_nums",
        "layer": "split_text_nums",
        "codec": "zlib",
        "stream_codecs": {"NUMS": "num_v1", "TEXT": "zlib"},
    }
    spec = load_pipeline_spec(json.dumps(obj))
    # Deterministic by sorted keys.
    assert spec.stream_codecs_spec() == "NUMS:num_v1,TEXT:zlib"


def test_pipeline_unknown_key_rejected() -> None:
    obj = {
        "spec": "gcc-ocf.pipeline.v1",
        "name": "bad",
        "layer": "bytes",
        "codec": "zlib",
        "wat": 1,
    }
    with pytest.raises(PipelineSpecError):
        load_pipeline_spec(json.dumps(obj))


def test_pipeline_from_file(tmp_path: Path) -> None:
    p = tmp_path / "p.json"
    p.write_text(
        json.dumps(
            {
                "spec": "gcc-ocf.pipeline.v1",
                "name": "bytes+raw",
                "layer": "bytes",
                "codec": "raw",
            }
        ),
        encoding="utf-8",
    )
    spec = load_pipeline_spec("@" + str(p))
    assert spec.codec == "raw"
