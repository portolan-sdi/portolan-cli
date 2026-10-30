"""Fail when two tests have the same body.

A copy of a test adds run time and adds no coverage. A change to the subject
then needs two edits, and the two copies drift apart. This check parses each
``tests/**/test_*.py`` file with ``ast``. For each test function and each test
method, it removes the docstring and dumps the remaining statements. Two tests
with the same key are duplicates. The key holds the marks, the async flag, the
parameter names, and the body. The marks are the decorators of the test and of
each enclosing class, and the ``pytestmark`` assignments in the module and in
each enclosing class. The check ignores the test name, so a rename does not
hide a copy. The check includes each mark that is a call, so two
``parametrize`` rows with different values are not duplicates. A
``pytestmark`` parametrize and the same ``parametrize`` decorator give the same
key. The check includes the async flag, because an ``async def`` test and a
``def`` test run on different paths. The check includes the parameter names,
so two tests that ask for different fixtures are not duplicates. The check
ignores a bare marker such as ``pytest.mark.unit``. A tier marker selects when a test runs. It does not
change what the test does.

A body that is only ``pass`` or ``pytest.skip(...)`` is a duplicate too. Delete
the extra copy. Do not edit the body to make it different.

The module reads the files under ``tests/`` and runs no product code. It lives
in ``tests/docs/`` next to ``test_ci_wiring.py``, ``test_quality_config.py``, and
``test_guardrail_exemptions.py``, which also read repository files and pin a gate.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "tests"
Func = ast.FunctionDef | ast.AsyncFunctionDef


def _tests(node: ast.AST, prefix: str, outer: list[str]) -> list[tuple[str, Func, list[str]]]:
    """Return each test under ``node`` with its id and the called marks of its module and classes."""
    found: list[tuple[str, Func, list[str]]] = []
    for child in ast.iter_child_nodes(node):
        if isinstance(child, Func) and child.name.startswith("test_"):
            found.append((f"{prefix}::{child.name}", child, outer))
        elif isinstance(child, ast.ClassDef) and child.name.startswith("Test"):
            inner = outer + _called_decorators(child.decorator_list + _pytestmark(child))
            found.extend(_tests(child, f"{prefix}::{child.name}", inner))
    return found


def _pytestmark(node: ast.Module | ast.ClassDef) -> list[ast.expr]:
    """Return the marks of each ``pytestmark = ...`` statement in the body of ``node``."""
    marks: list[ast.expr] = []
    for stmt in node.body:
        if isinstance(stmt, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "pytestmark" for t in stmt.targets
        ):
            value = stmt.value
            marks.extend(value.elts if isinstance(value, ast.List | ast.Tuple) else [value])
    return marks


def _is_bare_mark(decorator: ast.expr) -> bool:
    """Return True for a marker without arguments, such as ``pytest.mark.unit``."""
    return ast.unparse(decorator).startswith("pytest.mark.") and not isinstance(decorator, ast.Call)


def _called_decorators(decorators: list[ast.expr]) -> list[str]:
    """Return the dump of each decorator that is not a bare marker."""
    return [ast.dump(d) for d in decorators if not _is_bare_mark(d)]


def _fixtures(func: Func) -> list[str]:
    """Return the parameter names of ``func`` without a leading ``self`` or ``cls``."""
    names = [a.arg for a in func.args.posonlyargs + func.args.args + func.args.kwonlyargs]
    if names and names[0] in ("self", "cls"):
        names = names[1:]
    return names


def _key(func: Func, class_decorators: list[str]) -> str:
    """Return the marks, the async flag, the fixtures, and the body without the docstring."""
    body = func.body
    if ast.get_docstring(func) is not None:
        body = body[1:]
    decorators = class_decorators + _called_decorators(func.decorator_list)
    is_async = isinstance(func, ast.AsyncFunctionDef)
    return repr((decorators, is_async, _fixtures(func), [ast.dump(stmt) for stmt in body]))


def duplicate_groups(root: Path) -> list[list[str]]:
    """Return each group of tests under ``root`` that share one key."""
    by_key: dict[str, list[str]] = defaultdict(list)
    for path in sorted(root.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        module_marks = _called_decorators(_pytestmark(tree))
        test_id_prefix = path.relative_to(root.parent).as_posix()
        for test_id, func, outer in _tests(tree, test_id_prefix, module_marks):
            by_key[_key(func, outer)].append(test_id)
    return [ids for ids in by_key.values() if len(ids) > 1]


def test_no_two_tests_share_a_body() -> None:
    groups = duplicate_groups(TESTS)
    report = "\n\n".join("\n".join(ids) for ids in groups)
    assert not groups, f"{len(groups)} groups of duplicate tests:\n\n{report}"


def _groups_of(tmp_path: Path, source: str) -> list[list[str]]:
    """Write ``source`` as one test module under ``tmp_path`` and return its groups."""
    (tmp_path / "test_sample.py").write_text(source, encoding="utf-8")
    return [[test_id.rsplit("::", 1)[1] for test_id in ids] for ids in duplicate_groups(tmp_path)]


def test_same_body_under_different_class_parametrize_is_not_a_duplicate(tmp_path: Path) -> None:
    source = """
import pytest

@pytest.mark.parametrize("n", [1])
class TestOne:
    def test_a(self, n):
        assert n > 0

@pytest.mark.parametrize("n", [2])
class TestTwo:
    def test_b(self, n):
        assert n > 0
"""
    assert _groups_of(tmp_path, source) == []


def test_same_body_with_different_fixtures_is_not_a_duplicate(tmp_path: Path) -> None:
    source = """
def test_a(fresh_catalog):
    assert True

def test_b(stale_catalog):
    assert True
"""
    assert _groups_of(tmp_path, source) == []


def test_same_body_same_fixtures_is_a_duplicate(tmp_path: Path) -> None:
    source = """
import pytest

@pytest.mark.unit
def test_a(catalog):
    assert catalog.exists()

@pytest.mark.integration
class TestCopy:
    def test_b(self, catalog):
        assert catalog.exists()
"""
    assert _groups_of(tmp_path, source) == [["test_a", "test_b"]]


def test_async_and_sync_twins_are_not_duplicates(tmp_path: Path) -> None:
    source = """
async def test_a(catalog):
    assert catalog.exists()

def test_b(catalog):
    assert catalog.exists()
"""
    assert _groups_of(tmp_path, source) == []


def test_module_pytestmark_parametrize_matches_the_same_decorator(tmp_path: Path) -> None:
    (tmp_path / "test_marked.py").write_text(
        """
import pytest

pytestmark = [pytest.mark.unit, pytest.mark.parametrize("value", [1, 2])]

def test_one(value):
    assert value > 0
""",
        encoding="utf-8",
    )
    source = """
import pytest

@pytest.mark.parametrize("value", [1, 2])
def test_two(value):
    assert value > 0
"""
    assert _groups_of(tmp_path, source) == [["test_one", "test_two"]]


def test_class_pytestmark_parametrize_with_different_values_is_not_a_duplicate(
    tmp_path: Path,
) -> None:
    source = """
import pytest

class TestOne:
    pytestmark = pytest.mark.parametrize("n", [1])

    def test_a(self, n):
        assert n > 0

class TestTwo:
    pytestmark = (pytest.mark.parametrize("n", [2]),)

    def test_b(self, n):
        assert n > 0
"""
    assert _groups_of(tmp_path, source) == []


def test_module_bare_pytestmark_changes_nothing(tmp_path: Path) -> None:
    (tmp_path / "test_marked.py").write_text(
        """
import pytest

pytestmark = pytest.mark.unit

def test_one(catalog):
    assert catalog.exists()
""",
        encoding="utf-8",
    )
    source = """
def test_two(catalog):
    assert catalog.exists()
"""
    assert _groups_of(tmp_path, source) == [["test_one", "test_two"]]
