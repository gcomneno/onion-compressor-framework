"""Compatibility alias for the legacy `gcc_huffman` module.

`gcc_ocf.legacy.gcc_huffman` is historically named but is *not* just Huffman:
it contains a legacy, hand-stitched compression pipeline and related glue.

We keep the old import path for backwards compatibility.
New code should prefer importing from `gcc_ocf.legacy.gcc_legacy` (this module).

This file intentionally re-exports the public surface of `gcc_huffman`.
"""

from __future__ import annotations

from .gcc_huffman import *  # noqa: F401,F403
