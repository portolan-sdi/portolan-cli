"""Keep the mutation selection runnable inside the mutmut sandbox (issue #997).

mutmut does not run the suite in the repository. It copies a subset of the tree
into ``mutants/`` and runs pytest there. The copy holds ``paths_to_mutate``,
``also_copy``, and a fixed list that mutmut adds itself (``tests/``,
``pyproject.toml``, and a few config files). A test that reads any other
repo-root path, for example ``docs/guides/plugins.md``, passes in CI and fails
in the sandbox with ``FileNotFoundError``. mutmut runs its stats phase with
``-x``, so that one failure stops the nightly sweep before it tests a mutant.
That happened every night from August 28 to October 19, 2026.

Two parts prevent it:

- ``SandboxReadGuard`` is an audit hook that ``tests/conftest.py`` installs
  for an ordinary pytest run. While a test in the mutation selection runs, the
  hook records each ``open`` of a repo-root path that the sandbox does not
  contain, and the test fails with that path. The selection comes from
  ``[tool.mutmut]`` in ``pyproject.toml``, so the guard and mutmut agree.
- ``reset_stats_mode_in_children`` keeps a child interpreter out of mutmut's
  stats bookkeeping. See its docstring.

The guard sees ``open`` calls in the test process only. A child process that
reads a repo-root path is outside its reach.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

    import pytest

MUTMUT_ENV_KEY = "MUTANT_UNDER_TEST"

# mutmut 3.8 copies these into the sandbox whatever ``also_copy`` says
# (``mutmut.configuration._load_config``). It also copies every ``test*.py`` file at
# the root, which ``MutationSandbox.from_pyproject`` adds by glob.
MUTMUT_DEFAULT_COPIES = (
    "tests/",
    "test/",
    "setup.cfg",
    "pyproject.toml",
    "uv.lock",
    "poetry.lock",
    "Pipfile.lock",
    "pdm.lock",
)

# Repo-root paths that are not in the sandbox but cause no failure there:
# - the virtual environment: the sandbox run uses the same interpreter, at
#   the same absolute path.
# - .hypothesis and .pytest_cache: the tools create them under the working
#   directory on demand, which is mutants/ in the sandbox.
_TOOL_STATE_DIRS = (".hypothesis", ".pytest_cache")


class SandboxConfigError(ValueError):
    """``[tool.mutmut]`` has a shape that the guard cannot read."""


def _marker_names(expression: str) -> frozenset[str]:
    """Return the markers in a ``not a and not b`` filter.

    The guard accepts only this shape. Any other expression raises, because a
    guard that misreads the filter checks the wrong tests.
    """
    names = []
    for term in expression.split(" and "):
        words = term.split()
        if len(words) != 2 or words[0] != "not" or not words[1].isidentifier():
            raise SandboxConfigError(
                f"the mutmut marker filter {expression!r} is not of the form "
                "'not a and not b'. Update tests/mutation_sandbox.py to read it."
            )
        names.append(words[1])
    return frozenset(names)


def _ignored_paths(args: list[str]) -> list[str]:
    """Return the ``--ignore`` values in the mutmut pytest arguments."""
    ignored = []
    for index, arg in enumerate(args):
        if arg.startswith("--ignore="):
            ignored.append(arg.removeprefix("--ignore="))
        elif arg == "--ignore":
            ignored.append(args[index + 1])
    return ignored


def _marker_filter(args: list[str]) -> frozenset[str]:
    """Return the markers that the ``-m`` argument excludes."""
    if "-m" not in args:
        return frozenset()
    return _marker_names(args[args.index("-m") + 1])


def _norm(path: str | os.PathLike[str]) -> str:
    """Return an absolute, normalized, case-folded (on Windows) path string.

    The audit hook calls this on every ``open`` during a test. ``os.path``
    works on plain strings, and a ``Path`` object for each call costs more.
    """
    return os.path.normcase(os.path.abspath(path))  # noqa: PTH100


def _is_under(path: str, roots: Iterable[str]) -> bool:
    return any(path == root or path.startswith(root + os.sep) for root in roots)


@dataclass(frozen=True)
class MutationSandbox:
    """What mutmut copies into ``mutants/``, and which tests it runs there.

    All paths are strings that ``_norm`` returns.
    """

    root: str
    copied: tuple[str, ...]
    selection: tuple[str, ...]
    ignored: tuple[str, ...]
    excluded_markers: frozenset[str]

    @classmethod
    def from_pyproject(cls, root: Path) -> MutationSandbox:
        """Read ``[tool.mutmut]`` from ``root/pyproject.toml``."""
        config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        try:
            mutmut = config["tool"]["mutmut"]
        except KeyError as exc:
            raise SandboxConfigError("pyproject.toml has no [tool.mutmut] table") from exc
        args = [str(arg) for arg in mutmut.get("pytest_add_cli_args", [])]

        def absolute(relative: str) -> str:
            return _norm(root / relative)

        copied = [
            *mutmut.get("paths_to_mutate", []),
            *mutmut.get("also_copy", []),
            *MUTMUT_DEFAULT_COPIES,
            *(path.name for path in root.glob("test*.py")),
        ]
        return cls(
            root=_norm(root),
            copied=tuple(absolute(path) for path in copied),
            selection=tuple(
                absolute(path) for path in mutmut.get("pytest_add_cli_args_test_selection", [])
            ),
            ignored=tuple(absolute(path) for path in _ignored_paths(args)),
            excluded_markers=_marker_filter(args),
        )

    def selects(self, test_path: Path, markers: Iterable[str]) -> bool:
        """Return True when mutmut runs the test at ``test_path``."""
        path = _norm(test_path)
        if self.selection and not _is_under(path, self.selection):
            return False
        if _is_under(path, self.ignored):
            return False
        return self.excluded_markers.isdisjoint(markers)

    def outside(self, path: str) -> bool:
        """Return True for a repo-root path that the sandbox does not contain."""
        if path == self.root or not _is_under(path, (self.root,)):
            return False  # the sandbox root, mutants/, exists
        if _is_under(path, self.copied):
            return False
        exempt = (_norm(sys.prefix), *(self.root + os.sep + d for d in _TOOL_STATE_DIRS))
        return not _is_under(path, exempt)


@dataclass
class SandboxReadGuard:
    """Record each ``open`` of a path outside the sandbox while a test runs.

    ``sys.addaudithook`` cannot remove a hook, so the guard stays installed for
    the whole session and does nothing while ``test`` is None.
    """

    sandbox: MutationSandbox
    roots: tuple[str, ...] = ()
    test: str | None = None
    reads: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # A test can build a path from Path(__file__).resolve(), and the
        # checkout can sit behind a symlink. Accept both spellings of the root.
        real = _norm(os.path.realpath(self.sandbox.root))
        self.roots = (
            (self.sandbox.root,) if real == self.sandbox.root else (self.sandbox.root, real)
        )

    def audit(self, event: str, args: tuple[Any, ...]) -> None:
        """The audit hook. ``open`` sends ``(path, mode, flags)``."""
        if event != "open" or self.test is None:
            return
        target, mode = args[0], args[1]
        if isinstance(target, int) or target is None:
            return  # an open file descriptor, not a path
        name = os.fsdecode(target)
        if mode is None and not os.path.isabs(name):  # noqa: PTH117
            # os.open sends mode None. It can open a name relative to a dir_fd,
            # which the event leaves out. shutil.rmtree does that.
            return
        path = _norm(name)
        for root in self.roots[1:]:
            if _is_under(path, (root,)):
                path = self.sandbox.root + path[len(root) :]
                break
        if path not in self.reads and self.sandbox.outside(path) and self._fails_there(path):
            self.reads.append(path)

    def _fails_there(self, path: str) -> bool:
        """Return True when an open that works here fails in the sandbox.

        A path that exists here is absent there. A new file in a directory
        outside the sandbox has no directory there. A name that exists in
        neither place, such as ``<string>`` from linecache, fails in both.
        """
        if os.path.exists(path):  # noqa: PTH110
            return True
        parent = os.path.dirname(path)  # noqa: PTH120
        return self.sandbox.outside(parent) and os.path.isdir(parent)  # noqa: PTH112

    def start(self, test: str) -> None:
        self.test = test
        self.reads.clear()

    def stop(self) -> list[str]:
        """Stop recording and return the paths outside the sandbox."""
        self.test = None
        reads, self.reads = self.reads, []
        return reads

    def message(self, test: str, reads: list[str]) -> str:
        paths = ", ".join(os.path.relpath(path, self.sandbox.root) for path in reads)
        return (
            f"{test} reads {paths}, which mutmut does not copy into mutants/. "
            "The nightly mutation sweep then fails on FileNotFoundError (#997). "
            "Move a test that checks repository files to tests/docs/, which "
            "[tool.mutmut] ignores. A test of portolan_cli behavior can read a "
            "fixture under tests/ instead, or the path can go in also_copy."
        )


def stats_free_env(env: Mapping[str, str] | None) -> dict[str, str] | None:
    """Return ``env`` for a child process, with mutmut's stats mode removed.

    ``None`` means the child inherits ``os.environ``. The result is ``None``
    when nothing needs to change.
    """
    source = os.environ if env is None else env
    if source.get(MUTMUT_ENV_KEY) != "stats":
        return None if env is None else dict(env)
    child = dict(source)
    # '' is the value mutmut itself sets for an unmutated run.
    child[MUTMUT_ENV_KEY] = ""
    return child


def reset_stats_mode_in_children(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each ``subprocess.Popen`` child of this test outside stats mode.

    In the stats phase mutmut sets ``MUTANT_UNDER_TEST=stats``. Each mutated
    function then calls ``mutmut.__main__.record_trampoline_hit``, which reads
    ``mutmut.config``. A child interpreter inherits the variable and imports
    the mutated tree from ``mutants/``, but mutmut never loads its config
    there. The first call into portolan_cli raises ``AttributeError:
    'NoneType' object has no attribute 'max_stack_depth'``. The import of
    ``mutmut.__main__`` also loads Click. A child cannot report hits to the
    parent in any case, so the child runs the unmutated code instead. The
    mutant phases keep their value, and a child still runs the mutant there.

    The patch does not reach a ``multiprocessing`` worker. A "spawn" worker
    starts through ``multiprocessing.util.spawnv_passfds`` and not through
    ``Popen``. Under mutmut that worker never reaches a trampoline. It
    re-imports the ``mutmut`` entry script as ``__mp_main__``. That import
    loads ``mutmut.__main__``, which calls ``set_start_method('fork')``. The
    call raises ``RuntimeError`` because the worker already has a context.
    The pool breaks, and ``convert_directory`` then converts the files
    serially in the test process. The parallel tests in
    tests/integration/test_cog_reoptimize.py pass the stats phase this way.
    """
    original = subprocess.Popen.__init__

    def init(self: subprocess.Popen[Any], *args: Any, **kwargs: Any) -> None:
        kwargs["env"] = stats_free_env(kwargs.get("env"))
        original(self, *args, **kwargs)

    monkeypatch.setattr(subprocess.Popen, "__init__", init)
