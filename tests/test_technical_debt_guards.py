from __future__ import annotations

import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = (
    REPO_ROOT / "scholarpath" / "api",
    REPO_ROOT / "scholarpath" / "chat",
    REPO_ROOT / "scholarpath" / "services",
    REPO_ROOT / "scholarpath" / "llm",
    REPO_ROOT / "scholarpath" / "evals",
    REPO_ROOT / "scholarpath" / "causal_engine",
)

OBSERVABILITY_GUARD_FILES = (
    "scholarpath/advisor/adapters.py",
    "scholarpath/advisor/output_polisher.py",
    "scholarpath/services/recommendation_service.py",
    "scholarpath/services/offer_service.py",
    "scholarpath/services/report_service.py",
    "scholarpath/services/student_service.py",
    "scholarpath/services/school_service.py",
    "scholarpath/llm/usage_tracker.py",
)


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SOURCE_ROOTS:
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*.py") if path.is_file())
    return sorted(files)


def test_no_bare_pass_in_exception_handlers() -> None:
    pattern = re.compile(
        r"except(?:\s+[^\n:]+)?\s*:\n(?:\s*#.*\n)*\s*pass\b",
        flags=re.MULTILINE,
    )
    offenders: list[str] = []
    for path in _iter_python_files():
        text = path.read_text(encoding="utf-8")
        if pattern.search(text):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, (
        "Bare `pass` inside exception handlers is disallowed in production code. "
        "Add structured logging/metrics and explicit fallback behavior. "
        f"Offenders: {offenders}"
    )


def _is_broad_exception(node: ast.ExceptHandler) -> bool:
    if node.type is None:
        return True
    if isinstance(node.type, ast.Name):
        return node.type.id == "Exception"
    if isinstance(node.type, ast.Tuple):
        return any(isinstance(item, ast.Name) and item.id == "Exception" for item in node.type.elts)
    return False


def _has_observability_signal(body: list[ast.stmt]) -> bool:
    wrapper = ast.Module(body=body, type_ignores=[])
    for node in ast.walk(wrapper):
        if isinstance(node, ast.Raise):
            return True
        if not isinstance(node, ast.Call):
            continue

        if isinstance(node.func, ast.Name) and node.func.id == "log_fallback":
            return True

        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logger"
            and node.func.attr in {"warning", "error", "exception"}
        ):
            if node.func.attr == "exception":
                return True
            if any(keyword.arg == "exc_info" for keyword in node.keywords):
                return True
    return False


def test_broad_exception_handlers_emit_observability_signal() -> None:
    offenders: list[str] = []
    for relative in OBSERVABILITY_GUARD_FILES:
        path = REPO_ROOT / relative
        if not path.exists():
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if not _is_broad_exception(node):
                continue
            if _has_observability_signal(node.body):
                continue
            offenders.append(f"{relative}:{node.lineno}")

    assert not offenders, (
        "Broad exception handlers in guarded files must emit a structured observability "
        "signal (`log_fallback` or logger with `exc_info`) or re-raise. "
        f"Offenders: {offenders}"
    )
