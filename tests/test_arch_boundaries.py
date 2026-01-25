from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

# High-level orchestrator modules (Layer 3).
# LOW-level code (core/engine/layers/legacy/...) must NEVER import these.
#
# IMPORTANT:
#   Modules that define *shared specs/schemas* (e.g. pipeline_spec, dir_pipeline_spec)
#   are NOT considered ORCH, because they are contracts and may be imported by legacy.
ORCH_PREFIXES: tuple[str, ...] = (
    "gcc_ocf.cli",
    "gcc_ocf.cli_verify_json",
    "gcc_ocf.verify",
    "gcc_ocf.single_container_dir",
    "gcc_ocf.single_container_mixed_dir",
)

PACKAGE_ROOT = "gcc_ocf"


@dataclass(frozen=True)
class ImportEdge:
    src: str
    dst: str
    file: Path
    lineno: int


def _is_orch(mod: str) -> bool:
    return any(mod == p or mod.startswith(p + ".") for p in ORCH_PREFIXES)


def _module_name_from_path(src_dir: Path, py_file: Path) -> str | None:
    try:
        rel = py_file.relative_to(src_dir)
    except ValueError:
        return None

    parts = list(rel.parts)
    if not parts or parts[0] != PACKAGE_ROOT:
        return None

    if py_file.name == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = py_file.stem

    if not parts:
        return None
    return ".".join(parts)


def _resolve_relative(current_mod: str, level: int, module: str | None) -> str | None:
    if level <= 0:
        return module

    base = current_mod.split(".")
    if base:
        base = base[:-1]  # package of current module

    if level > len(base):
        return None

    # "from .foo" (level=1) stays in same package; "from ..foo" goes up, etc.
    base = base[: len(base) - level + 1]

    if module:
        return ".".join(base + module.split("."))
    return ".".join(base)


def _iter_import_edges(src_dir: Path) -> Iterable[ImportEdge]:
    for py in src_dir.rglob("*.py"):
        mod = _module_name_from_path(src_dir, py)
        if not mod:
            continue

        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name
                    if name.startswith(PACKAGE_ROOT + ".") or name == PACKAGE_ROOT:
                        yield ImportEdge(
                            src=mod, dst=name, file=py, lineno=getattr(node, "lineno", 0)
                        )

            elif isinstance(node, ast.ImportFrom):
                if node.module is None and node.level == 0:
                    continue
                abs_mod = _resolve_relative(mod, node.level, node.module)
                if not abs_mod:
                    continue
                if abs_mod.startswith(PACKAGE_ROOT + ".") or abs_mod == PACKAGE_ROOT:
                    yield ImportEdge(
                        src=mod, dst=abs_mod, file=py, lineno=getattr(node, "lineno", 0)
                    )


def test_no_low_level_imports_orchestrator() -> None:
    """
    Hard dependency direction:
      ORCH (Layer 3) -> may depend on LOW
      LOW            -> must NOT depend on ORCH

    This makes stratification an automatic guarantee: forbidden imports fail CI/tests.
    """
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if not src_dir.is_dir():
        raise AssertionError(f"Expected src/ directory at: {src_dir}")

    violations: list[ImportEdge] = []

    for edge in _iter_import_edges(src_dir):
        if edge.src == edge.dst:
            continue

        src_is_orch = _is_orch(edge.src)
        dst_is_orch = _is_orch(edge.dst)

        if (not src_is_orch) and dst_is_orch:
            violations.append(edge)

    if violations:
        lines = ["Forbidden imports detected (LOW -> ORCH):"]
        for v in sorted(violations, key=lambda e: (str(e.file), e.lineno, e.src, e.dst)):
            lines.append(f"  {v.file}:{v.lineno}  {v.src}  ->  {v.dst}")
        lines.append("")
        lines.append("Fix: move high-level logic out of LOW modules, or invert the dependency.")
        raise AssertionError("\n".join(lines))
