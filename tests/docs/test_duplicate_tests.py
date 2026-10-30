"""Fail when two tests have the same body.

A copy of a test adds run time and adds no coverage. A change to the subject
then needs two edits, and the two copies drift apart. This check parses each
``tests/**/test_*.py`` file with ``ast``. For each test function and each test
method, it removes the docstring and dumps the remaining statements. Two tests
with the same dump and the same decorators are duplicates. The check ignores the
test name, so a rename does not hide a copy. The check includes each decorator
that is a call, so two ``parametrize`` rows with different values are not
duplicates. The check ignores a bare marker such as ``pytest.mark.unit``. A tier
marker selects when a test runs. It does not change what the test does.

A body that is only ``pass`` or ``pytest.skip(...)`` is a duplicate too. Delete
the extra copy. Do not edit the body to make it different.

The module lives in ``tests/docs/`` because it reads repository files that the
mutmut sandbox does not copy (#997).
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "tests"
Func = ast.FunctionDef | ast.AsyncFunctionDef


def _tests(node: ast.AST, prefix: str) -> list[tuple[str, Func]]:
    """Return each test function under ``node`` with its ``path::Class`` id."""
    found: list[tuple[str, Func]] = []
    for child in ast.iter_child_nodes(node):
        if isinstance(child, Func) and child.name.startswith("test_"):
            found.append((f"{prefix}::{child.name}", child))
        elif isinstance(child, ast.ClassDef) and child.name.startswith("Test"):
            found.extend(_tests(child, f"{prefix}::{child.name}"))
    return found


def _is_bare_mark(decorator: ast.expr) -> bool:
    """Return True for a marker without arguments, such as ``pytest.mark.unit``."""
    return ast.unparse(decorator).startswith("pytest.mark.") and not isinstance(decorator, ast.Call)


def _key(func: Func) -> str:
    """Return the called decorators and the body without the docstring."""
    body = func.body
    if ast.get_docstring(func) is not None:
        body = body[1:]
    decorators = [ast.dump(d) for d in func.decorator_list if not _is_bare_mark(d)]
    return repr((decorators, [ast.dump(stmt) for stmt in body]))


def duplicate_groups(root: Path) -> list[list[str]]:
    """Return each group of tests under ``root`` that share one key."""
    by_key: dict[str, list[str]] = defaultdict(list)
    for path in sorted(root.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for test_id, func in _tests(tree, path.relative_to(REPO_ROOT).as_posix()):
            by_key[_key(func)].append(test_id)
    return [ids for ids in by_key.values() if len(ids) > 1]


def test_no_two_tests_share_a_body() -> None:
    groups = duplicate_groups(TESTS)
    report = "\n\n".join("\n".join(ids) for ids in groups)
    assert not groups, f"{len(groups)} groups of duplicate tests:\n\n{report}"
