#!/usr/bin/env python3
"""Select the tests that a change can affect.

Run it while you work, then run the full suite once before you push:

    uv run python scripts/affected_tests.py            # print pytest targets
    uv run python scripts/affected_tests.py --run -n 4 # run them

The change is the working tree, committed or not, against the merge base with
``--base`` (default ``origin/main``), plus untracked files.

Selection:

- A changed ``portolan_cli`` module selects each test file that imports it or
  imports a module that imports it, directly or indirectly. grimp builds the
  import graph, and imports inside functions count. ``TYPE_CHECKING`` imports
  do not, because they never run.
- A test file imports a module through ``import``, ``from ... import``, a
  string such as ``importlib.import_module("portolan_cli.cli")``, or a patch
  target such as ``"portolan_cli.sync.push.obs.put_async"``.
- A changed test file is always selected. A deleted one is not.
- A changed ``conftest.py`` selects its directory. ``tests/conftest.py``,
  ``pyproject.toml``, ``uv.lock``, and any file under ``tests/fixtures`` select
  the full suite.
- A deleted module selects the tests of each module that imported it at the
  merge base, in any form: ``import portolan_cli.legacy``, ``from portolan_cli
  import legacy``, or ``from .legacy import Item``.
- Any other changed file selects the test files whose source names it, such as
  a test that reads ``AGENTS.md``.
- A change to a file in ``FOUNDATION_FILES`` also selects ``FOUNDATION_TESTS``.
  Those tests run the real CLI on a generated catalog. They reach the
  foundation through a subprocess or ``CliRunner``, so the import graph alone
  does not select them (#1035).
- A target that does not exist in the tree is dropped with a note on stderr,
  so ``--run`` does not hand pytest a missing path. A test file that the
  change deletes is never a target. When every target is missing, the script
  exits 1, because an empty pytest run passes and checks nothing.
"""

from __future__ import annotations

import argparse
import ast
import functools
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

PACKAGE = "portolan_cli"
TESTS_DIR = "tests"
FULL_SUITE_FILES = frozenset({"pyproject.toml", "uv.lock", "tests/conftest.py"})
FULL_SUITE_PREFIXES = ("tests/fixtures/",)
# The modules that write or read derived catalog state on every command. A bug
# in one shows up as a catalog that breaks an invariant, not as a failed import.
FOUNDATION_FILES = frozenset(
    {
        "portolan_cli/config.py",
        "portolan_cli/output.py",
        "portolan_cli/stac.py",
        "portolan_cli/finalization.py",
        "portolan_cli/preparation.py",
        "portolan_cli/sync/push.py",
        "portolan_cli/extract/common/source.py",
    }
)
FOUNDATION_TESTS = (
    "tests/integration/test_generated_catalog_conformance.py",
    "tests/integration/test_catalog_invariants.py",
    "tests/properties",
)


@dataclass
class Selection:
    """The pytest targets for a change."""

    targets: list[str] = field(default_factory=list)
    full: bool = False
    reason: str = ""


def module_name(path: str) -> str | None:
    """Return the dotted module name of a ``portolan_cli`` source file, or None."""
    pure = PurePosixPath(path)
    if pure.suffix != ".py" or not pure.parts or pure.parts[0] != PACKAGE:
        return None
    parts = list(pure.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _longest_known_prefix(dotted: str, known: set[str]) -> str | None:
    parts = dotted.split(".")
    for end in range(len(parts), 0, -1):
        candidate = ".".join(parts[:end])
        if candidate in known:
            return candidate
    return None


def _package_of(path: str | None) -> list[str] | None:
    """Return the package parts that a relative import in ``path`` starts from."""
    name = module_name(path) if path is not None else None
    if name is None:
        return None
    parts = name.split(".")
    return parts if PurePosixPath(path or "").name == "__init__.py" else parts[:-1]


def referenced_names(source: str, path: str | None = None) -> set[str]:
    """Return the dotted names that a source file imports or names in a string.

    Args:
        source: Python source.
        path: Repository path of the source. A ``portolan_cli`` path resolves
            relative imports such as ``from .legacy import Item``.

    Returns:
        Each imported module, each ``module.name`` of a ``from`` import, and
        each ``portolan_cli.`` string such as a patch target.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    package = _package_of(path)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            elif package is not None and node.level - 1 <= len(package):
                parts = package[: len(package) - (node.level - 1)]
                base = ".".join([*parts, node.module] if node.module else parts)
            else:
                continue
            if base:
                names.add(base)
                names.update(f"{base}.{alias.name}" for alias in node.names)
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.startswith(f"{PACKAGE}.")
            and " " not in node.value
        ):
            names.add(node.value)
    return names


def imported_modules(source: str, known: set[str]) -> set[str]:
    """Return the ``portolan_cli`` modules a test file's source refers to.

    Args:
        source: Python source of the test file.
        known: Every module name in the package.

    Returns:
        Module names from imports, import strings, and patch targets. The bare
        package name is left out, because the package root imports nothing at
        run time.
    """
    found = set()
    for name in referenced_names(source):
        if name.split(".")[0] != PACKAGE:
            continue
        module = _longest_known_prefix(name, known)
        if module is not None and module != PACKAGE:
            found.add(module)
    return found


def _full(reason: str) -> Selection:
    return Selection(targets=[TESTS_DIR], full=True, reason=reason)


def _tests_importing(modules: set[str], test_imports: dict[str, set[str]]) -> set[str]:
    return {test for test, imported in test_imports.items() if imported & modules}


def select(
    changes: Iterable[tuple[str, str]],
    *,
    importers: Callable[[str], set[str]],
    test_imports: dict[str, set[str]],
    test_texts: dict[str, str],
    deleted_importers: Callable[[str], set[str]],
) -> Selection:
    """Choose the pytest targets for a set of changed files.

    Args:
        changes: ``(status, path)`` pairs. Status ``D`` means deleted.
        importers: Module name to every module that imports it, directly or
            indirectly.
        test_imports: Test file path to the package modules it imports.
        test_texts: Test file path to its source, for name matches.
        deleted_importers: Deleted module name to the modules and test files
            that imported it at the merge base.

    Returns:
        The selection. A full selection has the single target ``tests``.
    """
    targets: set[str] = set()
    directories: set[str] = set()
    modules: set[str] = set()
    deleted: set[str] = set()
    for status, path in changes:
        pure = PurePosixPath(path)
        if status == "D":
            deleted.add(path)
        if path in FULL_SUITE_FILES or path.startswith(FULL_SUITE_PREFIXES):
            return _full(f"{path} changed")
        if pure.name == "conftest.py" and pure.parts[0] == TESTS_DIR:
            directories.add(str(pure.parent))
            continue
        if pure.parts[0] == TESTS_DIR and pure.suffix == ".py":
            if status != "D" and pure.name.startswith("test_"):
                targets.add(path)
            elif status != "D":
                # A helper module: select the tests that import it by path.
                stem = ".".join(pure.with_suffix("").parts)
                targets.update(t for t, text in test_texts.items() if stem in text)
            continue
        if path in FOUNDATION_FILES:
            targets.update(FOUNDATION_TESTS)
        name = module_name(path)
        if name is not None:
            if status == "D":
                for importer in deleted_importers(name):
                    if importer.startswith(f"{TESTS_DIR}/"):
                        targets.add(importer)
                    else:
                        modules.add(importer)
                        modules |= importers(importer)
            else:
                modules.add(name)
                modules |= importers(name)
            continue
        targets.update(t for t, text in test_texts.items() if pure.name in text)
    targets |= _tests_importing(modules, test_imports)
    # A deleted module's old importers can include a test file that the same
    # change deletes. pytest exits with an error on a missing path.
    targets -= deleted
    targets = {t for t in targets if not any(t.startswith(f"{d}/") for d in directories)}
    return Selection(targets=sorted(targets | directories))


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    ).stdout


def changed_files(repo: Path, base: str) -> list[tuple[str, str]]:
    """Return ``(status, path)`` for the working tree against the merge base."""
    merge_base = _git(repo, "merge-base", base, "HEAD").strip()
    changes = []
    for line in _git(repo, "diff", "--name-status", "--no-renames", merge_base).splitlines():
        status, path = line.split("\t", 1)
        changes.append((status[0], path))
    untracked = _git(repo, "ls-files", "--others", "--exclude-standard").splitlines()
    changes.extend(("A", path) for path in untracked)
    return changes


def _test_index(repo: Path, known: set[str]) -> tuple[dict[str, set[str]], dict[str, str]]:
    imports: dict[str, set[str]] = {}
    texts: dict[str, str] = {}
    for path in sorted((repo / TESTS_DIR).rglob("test_*.py")):
        rel = path.relative_to(repo).as_posix()
        if rel.startswith(FULL_SUITE_PREFIXES):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        texts[rel] = text
        imports[rel] = imported_modules(text, known)
    return imports, texts


def _deleted_importers_at(repo: Path, base: str) -> Callable[[str], set[str]]:
    # Only a deleted module needs the merge base. CI checks out a single commit
    # with no base ref, so the lookup waits until a deletion asks for it.
    @functools.cache
    def merge_base() -> str:
        return _git(repo, "merge-base", base, "HEAD").strip()

    def find(module: str) -> set[str]:
        # Each import form names the last part of the module, so a text search
        # for it finds every candidate. The parse then drops the false matches.
        leaf = module.rsplit(".", 1)[-1]
        result = subprocess.run(
            ["git", "grep", "-l", "-w", "-F", leaf, merge_base(), "--", PACKAGE, TESTS_DIR],
            cwd=repo,
            capture_output=True,
            text=True,
            check=False,
        )
        found = set()
        for line in result.stdout.splitlines():
            path = line.split(":", 1)[1]
            if not path.endswith(".py") or module_name(path) == module:
                continue
            source = _git(repo, "show", f"{merge_base()}:{path}")
            names = referenced_names(source, path)
            if not any(name == module or name.startswith(f"{module}.") for name in names):
                continue
            name = module_name(path)
            found.add(name if name is not None else path)
        return found

    return find


def select_for_repo(
    repo: Path, changes: list[tuple[str, str]], base: str = "origin/main"
) -> Selection:
    """Select tests for ``changes`` using this repository's import graph."""
    import grimp  # import-linter installs it

    sys.path.insert(0, str(repo))
    # cache_dir=None: grimp writes its cache into the working directory by
    # default, which leaves .grimp_cache/ in the repository root.
    graph = grimp.build_graph(PACKAGE, exclude_type_checking_imports=True, cache_dir=None)
    known = set(graph.modules)
    test_imports, test_texts = _test_index(repo, known)
    return select(
        changes,
        importers=lambda module: (
            set(graph.find_downstream_modules(module)) if module in known else set()
        ),
        test_imports=test_imports,
        test_texts=test_texts,
        deleted_importers=_deleted_importers_at(repo, base),
    )


def existing_targets(repo: Path, targets: list[str]) -> list[str]:
    """Return the targets that exist under ``repo``, and note the rest on stderr."""
    missing = [target for target in targets if not (repo / target).exists()]
    if missing:
        print(f"Skipped {len(missing)} missing target(s): {' '.join(missing)}", file=sys.stderr)
    return [target for target in targets if target not in missing]


def main(argv: list[str] | None = None) -> int:
    """Print or run the affected tests."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main", help="Base ref (default: origin/main).")
    parser.add_argument("--run", action="store_true", help="Run pytest on the selection.")
    args, pytest_args = parser.parse_known_args(argv)

    repo = Path(_git(Path.cwd(), "rev-parse", "--show-toplevel").strip())
    selection = select_for_repo(repo, changed_files(repo, args.base), args.base)
    if selection.full:
        print(f"Full suite: {selection.reason}.", file=sys.stderr)
    if not selection.targets:
        print("No affected tests.", file=sys.stderr)
        return 0
    targets = existing_targets(repo, selection.targets)
    if not targets:
        # An empty pytest run exits 0 and checks nothing.
        print(
            "Every selected target is missing from the tree. Run the full suite.", file=sys.stderr
        )
        return 1
    if not args.run:
        print(" ".join(targets))
        return 0
    return subprocess.run(
        [sys.executable, "-m", "pytest", *pytest_args, *targets], cwd=repo, check=False
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
