"""Typed errors for GCC-OCF.

We keep errors small and boring. The CLI maps them to stable exit codes.
"""

from __future__ import annotations


class GCCOCFError(Exception):
    """Base error for GCC-OCF."""

    exit_code: int = 10


class UsageError(GCCOCFError):
    exit_code = 2


class CorruptPayload(GCCOCFError):
    exit_code = 10


class BadMagic(CorruptPayload):
    pass


class UnsupportedVersion(GCCOCFError):
    exit_code = 11


class MissingResource(GCCOCFError):
    exit_code = 12


class HashMismatch(GCCOCFError):
    exit_code = 13
