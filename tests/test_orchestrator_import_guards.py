"""Hard guards to keep advisor orchestrator as a façade-only module."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_ORCHESTRATOR_PATH = Path("scholarpath/advisor/orchestrator.py")
_ALLOWED_ORCHESTRATOR_IMPORTS = {"AdvisorOrchestrator"}


def _iter_source_files() -> list[Path]:
    roots = [Path("scholarpath"), Path("tests")]
    files: list[Path] = []
    for root in roots:
        files.extend(
            sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)
        )
    return files


def test_orchestrator_facade_shape() -> None:
    tree = ast.parse(_ORCHESTRATOR_PATH.read_text(encoding="utf-8"), filename=str(_ORCHESTRATOR_PATH))
    class_names = [node.name for node in tree.body if isinstance(node, ast.ClassDef)]
    assert class_names == ["AdvisorOrchestrator"], (
        "orchestrator.py must stay façade-only. "
        f"Found classes: {class_names}"
    )


def test_no_non_facade_imports_from_orchestrator() -> None:
    violations: list[str] = []

    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != "scholarpath.advisor.orchestrator":
                continue
            for alias in node.names:
                if alias.name not in _ALLOWED_ORCHESTRATOR_IMPORTS:
                    violations.append(f"{path}:{node.lineno} imports {alias.name}")

    if violations:
        lines = "\n".join(f"- {row}" for row in violations)
        pytest.fail(f"Found non-facade imports from scholarpath.advisor.orchestrator:\n{lines}")
