"""Static guardrails to prevent legacy chat protocol modules from being reintroduced."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

BLOCKED_MODULES = {
    "scholarpath.api.routes.chat",
    "scholarpath.chat.agent",
    "scholarpath.chat.intents",
}


def _iter_source_files() -> list[Path]:
    root = Path("scholarpath")
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def _is_blocked(module_name: str) -> bool:
    return module_name in BLOCKED_MODULES or any(
        module_name.startswith(f"{blocked}.") for blocked in BLOCKED_MODULES
    )


def test_no_legacy_chat_module_imports_in_source() -> None:
    violations: list[str] = []

    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_blocked(alias.name):
                        violations.append(f"{path}:{node.lineno} import {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if _is_blocked(node.module):
                    violations.append(f"{path}:{node.lineno} from {node.module} import ...")

    if violations:
        lines = "\n".join(f"- {row}" for row in violations)
        pytest.fail(f"Found blocked legacy imports:\n{lines}")
