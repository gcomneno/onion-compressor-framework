from __future__ import annotations

from pathlib import Path

import pytest

from gcc_ocf.dir_pipeline_spec import DirPipelineSpecError, load_dir_pipeline_spec


def test_load_dir_pipeline_spec_from_file() -> None:
    p = Path("tools/dir_pipelines/default_v1.json")
    spec = load_dir_pipeline_spec("@" + str(p))
    assert spec.spec == "gcc-ocf.dir_pipeline.v1"
    assert spec.buckets == 16
    assert "textish" in spec.candidate_pools
    assert spec.num_dict_v1.k == 64


def test_dir_pipeline_spec_rejects_unknown_key() -> None:
    bad = '{"spec":"gcc-ocf.dir_pipeline.v1","wat":1}'
    with pytest.raises(DirPipelineSpecError):
        load_dir_pipeline_spec(bad)
