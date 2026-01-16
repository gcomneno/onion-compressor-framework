from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]{2,}")


@dataclass(frozen=True)
class Fingerprint:
    algo: str
    simhash64: int
    is_text: bool
    token_count: int


def _h64(data: bytes) -> int:
    # stable 64-bit hash
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def _simhash64(weighted_hashes: Iterable[tuple[int, int]]) -> int:
    # weighted_hashes: (h64, weight)
    acc = [0] * 64
    for h, w in weighted_hashes:
        for i in range(64):
            bit = (h >> i) & 1
            acc[i] += w if bit else -w
    out = 0
    for i, v in enumerate(acc):
        if v >= 0:
            out |= 1 << i
    return out


def fingerprint_bytes(data: bytes, *, max_bytes: int = 1_000_000) -> Fingerprint:
    b = data[:max_bytes]
    if not b:
        return Fingerprint(algo="simhash64:text", simhash64=0, is_text=True, token_count=0)

    # crude heuristic: mostly printable?
    printable = sum(1 for x in b if (32 <= x <= 126) or x in (9, 10, 13))
    is_text = (printable / len(b)) >= 0.85

    if is_text:
        txt = b.decode("utf-8", errors="ignore").lower()
        toks = _TOKEN_RE.findall(txt)
        if not toks:
            # fallback: by lines
            chunks = [c for c in txt.splitlines() if c.strip()]
            wh = [(_h64(c.encode("utf-8")), 1) for c in chunks[:5000]]
            return Fingerprint(
                algo="simhash64:lines",
                simhash64=_simhash64(wh),
                is_text=True,
                token_count=len(chunks),
            )
        # freq-limited weights
        freq: dict[str, int] = {}
        for t in toks:
            freq[t] = min(freq.get(t, 0) + 1, 20)
        wh = [(_h64(k.encode("utf-8")), v) for k, v in freq.items()]
        return Fingerprint(
            algo="simhash64:tokens", simhash64=_simhash64(wh), is_text=True, token_count=len(toks)
        )

    # binary: 4-byte shingles
    wh = []
    step = 4
    for i in range(0, min(len(b), 200_000) - step + 1, step):
        wh.append((_h64(b[i : i + step]), 1))
    return Fingerprint(
        algo="simhash64:bin4", simhash64=_simhash64(wh), is_text=False, token_count=len(wh)
    )
