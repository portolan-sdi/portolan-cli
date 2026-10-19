"""The mutation sandbox guard in tests/mutation_sandbox.py (issue #997).

mutmut runs the suite in a ``mutants/`` copy of the tree. A test that reads a
repo-root path absent from that copy stops the nightly sweep. The guard fails
such a test in an ordinary run, before it reaches the nightly job.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests.mutation_sandbox import (
    MUTMUT_ENV_KEY,
    MutationSandbox,
    SandboxConfigError,
    SandboxReadGuard,
    reset_stats_mode_in_children,
    stats_free_env,
)

# These tests check the harness, not portolan_cli, so they kill no mutant. In
# the stats phase the autouse fixture has already patched Popen, and the first
# assertion of test_a_child_leaves_stats_mode would fail there.
pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(
        MUTMUT_ENV_KEY in os.environ, reason="the guard tests check the harness, not portolan_cli"
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[2]


def _n(path: Path) -> str:
    """The guard compares case-folded strings on Windows. Match that form."""
    return os.path.normcase(str(path))


_PYPROJECT = textwrap.dedent(
    """
    [tool.mutmut]
    paths_to_mutate = ["pkg/"]
    pytest_add_cli_args_test_selection = ["tests/"]
    also_copy = ["scripts/", ".baseline"]
    pytest_add_cli_args = [
        "--no-cov",
        "--ignore=tests/docs",
        "--ignore", "tests/other",
        "-m", "not network and not source_scan",
    ]
    """
)


@pytest.fixture
def sandbox(tmp_path: Path) -> MutationSandbox:
    (tmp_path / "pyproject.toml").write_text(_PYPROJECT, encoding="utf-8")
    (tmp_path / "test_root_level.py").write_text("", encoding="utf-8")
    return MutationSandbox.from_pyproject(tmp_path)


def test_the_sandbox_holds_mutmut_copies_and_defaults(
    sandbox: MutationSandbox, tmp_path: Path
) -> None:
    assert sandbox.copied == tuple(
        _n(tmp_path / name)
        for name in (
            "pkg",
            "scripts",
            ".baseline",
            "tests",
            "test",
            "setup.cfg",
            "pyproject.toml",
            "pytest.ini",
            ".gitignore",
            "test_root_level.py",
        )
    )


def test_both_ignore_spellings_and_the_marker_filter_are_read(
    sandbox: MutationSandbox, tmp_path: Path
) -> None:
    assert sandbox.ignored == (_n(tmp_path / "tests/docs"), _n(tmp_path / "tests/other"))
    assert sandbox.excluded_markers == frozenset({"network", "source_scan"})


def test_a_marker_filter_of_another_shape_raises(tmp_path: Path) -> None:
    pyproject = _PYPROJECT.replace("not network and not source_scan", "unit or integration")
    (tmp_path / "pyproject.toml").write_text(pyproject, encoding="utf-8")

    with pytest.raises(SandboxConfigError, match="'unit or integration'"):
        MutationSandbox.from_pyproject(tmp_path)


def test_selection_follows_paths_ignores_and_markers(
    sandbox: MutationSandbox, tmp_path: Path
) -> None:
    unit = tmp_path / "tests/unit/test_x.py"

    assert sandbox.selects(unit, ["unit"]) is True
    assert sandbox.selects(unit, ["unit", "source_scan"]) is False
    assert sandbox.selects(tmp_path / "tests/docs/test_x.py", ["unit"]) is False
    assert sandbox.selects(tmp_path / "tests/docsearch/test_x.py", ["unit"]) is True
    assert sandbox.selects(tmp_path / "elsewhere/test_x.py", ["unit"]) is False


def test_outside_names_repo_paths_the_sandbox_lacks(
    sandbox: MutationSandbox, tmp_path: Path
) -> None:
    assert sandbox.outside(_n(tmp_path / "docs/guides/plugins.md")) is True
    assert sandbox.outside(_n(tmp_path / "README.md")) is True
    assert sandbox.outside(_n(tmp_path / "pkg/data/schema.json")) is False
    assert sandbox.outside(_n(tmp_path / "scripts/x.py")) is False
    assert sandbox.outside(_n(tmp_path / ".baseline")) is False
    assert sandbox.outside(_n(tmp_path / ".hypothesis/examples/a")) is False
    assert sandbox.outside(_n(tmp_path.parent / "elsewhere.md")) is False


def test_the_guard_records_only_while_a_test_runs(sandbox: MutationSandbox, tmp_path: Path) -> None:
    guard = SandboxReadGuard(sandbox)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/a.md").write_text("", encoding="utf-8")
    docs = _n(tmp_path / "docs/a.md")

    guard.audit("open", (docs, "r", 0))
    assert guard.reads == []

    guard.start("tests/unit/test_x.py::test_y")
    guard.audit("open", (docs, "r", 0))
    guard.audit("open", (Path(docs), "rb", 0))
    guard.audit("open", (str(tmp_path / "tests/fixtures/a.json"), "r", 0))
    guard.audit("open", (3, "r", 0))
    guard.audit("os.listdir", (docs,))

    assert guard.stop() == [docs]
    assert guard.reads == []
    assert guard.test is None


def test_the_guard_skips_opens_that_fail_in_both_places(
    sandbox: MutationSandbox, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    guard = SandboxReadGuard(sandbox)
    guard.start("t::x")

    guard.audit("open", ("<string>", "r", 0))  # linecache: no such file anywhere
    guard.audit("open", ("data", None, 0))  # os.open, maybe relative to a dir_fd
    guard.audit("open", (str(tmp_path / "missing/a.md"), "r", 0))
    guard.audit("open", (str(tmp_path / "docs/new.md"), "w", 0))

    # docs/ exists here and not in the sandbox, so a new file there fails there.
    assert guard.stop() == [_n(tmp_path / "docs/new.md")]


def test_the_guard_message_names_the_relative_path(
    sandbox: MutationSandbox, tmp_path: Path
) -> None:
    guard = SandboxReadGuard(sandbox)

    message = guard.message("t::x", [str(tmp_path / "docs/a.md")])

    assert message.startswith(f"t::x reads {Path('docs', 'a.md')}, which mutmut")
    assert "tests/docs/" in message


def test_the_repo_sandbox_ignores_tests_docs() -> None:
    """tests/docs reads docs/, mkdocs.yml, and README.md, which the sandbox lacks."""
    sandbox = MutationSandbox.from_pyproject(REPO_ROOT)

    assert _n(REPO_ROOT / "tests" / "docs") in sandbox.ignored
    assert sandbox.selects(REPO_ROOT / "tests/docs/test_plugin_guide.py", ["unit"]) is False
    assert "source_scan" in sandbox.excluded_markers


def test_the_installed_guard_records_a_repo_root_read(sandbox_guard: SandboxReadGuard) -> None:
    """The conftest hook sees a real ``open`` in a test of the mutation selection."""
    readme = REPO_ROOT / "README.md"
    readme.read_bytes()

    root = sandbox_guard.sandbox.root
    # The guard records case-folded paths. On Windows this gives readme.md.
    assert [os.path.relpath(path, root) for path in sandbox_guard.reads] == [
        os.path.normcase("README.md")
    ]
    # The test expected the record. Clear it, or the call phase fails the test.
    sandbox_guard.reads.clear()


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({MUTMUT_ENV_KEY: "stats", "A": "1"}, {MUTMUT_ENV_KEY: "", "A": "1"}),
        (
            {MUTMUT_ENV_KEY: "portolan_cli.x.x_f__mutmut_1"},
            {MUTMUT_ENV_KEY: "portolan_cli.x.x_f__mutmut_1"},
        ),
        ({"A": "1"}, {"A": "1"}),
    ],
)
def test_stats_free_env_clears_only_the_stats_value(
    env: dict[str, str], expected: dict[str, str]
) -> None:
    assert stats_free_env(env) == expected


def test_stats_free_env_keeps_inheritance_outside_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(MUTMUT_ENV_KEY, raising=False)

    assert stats_free_env(None) is None


def _child_value() -> str:
    code = f"import os; print(repr(os.environ.get({MUTMUT_ENV_KEY!r})))"
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def test_a_child_leaves_stats_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """In the stats phase a child must not inherit ``stats`` (#997).

    With ``stats``, the first call into the mutated tree in the child raises
    AttributeError inside ``record_trampoline_hit``. That failed
    tests/integration/test_import_boundary.py in the sweep.
    """
    monkeypatch.setenv(MUTMUT_ENV_KEY, "stats")
    assert _child_value() == "'stats'"

    reset_stats_mode_in_children(monkeypatch)

    assert _child_value() == "''"


def _clears_os_environ(node: ast.Call) -> bool:
    """True for ``patch.dict(os.environ, ..., clear=True)`` in either spelling."""
    func = node.func
    if not (isinstance(func, ast.Attribute) and func.attr == "dict" and node.args):
        return False
    target = ast.unparse(node.args[0]).strip("'\"")
    clears = any(
        k.arg == "clear" and isinstance(k.value, ast.Constant) and k.value.value is True
        for k in node.keywords
    )
    return target == "os.environ" and clears


def test_no_test_clears_the_environment_without_the_mutmut_key() -> None:
    """A bare clear drops MUTANT_UNDER_TEST and fails the sweep (#612, #997).

    Each mutated function reads the variable on each call, so the sandbox run
    fails with KeyError. ``tests.conftest.cleared_environ`` keeps it.
    """
    tests_root = REPO_ROOT / "tests"
    offenders = []
    for path in sorted(tests_root.rglob("*.py")):
        if path == tests_root / "conftest.py":
            continue  # cleared_environ itself
        tree = ast.parse(path.read_text(encoding="utf-8"))
        offenders += [
            f"{path.relative_to(REPO_ROOT).as_posix()}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and _clears_os_environ(node)
        ]

    assert offenders == []
