"""Tests for scripts/affected_tests.py, the affected-test selector."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from types import ModuleType

pytestmark = pytest.mark.unit

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "affected_tests.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("affected_tests", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["affected_tests"] = module
    spec.loader.exec_module(module)
    return module


at = _load()

# A small fake repository: cli imports add, add imports bbox.
IMPORTERS = {
    "portolan_cli.bbox": {"portolan_cli.add", "portolan_cli.cli"},
    "portolan_cli.add": {"portolan_cli.cli"},
    "portolan_cli.cli": set(),
    "portolan_cli.versions": set(),
}
TESTS = {
    "tests/unit/test_bbox.py": {"portolan_cli.bbox"},
    "tests/unit/test_add.py": {"portolan_cli.add"},
    "tests/integration/test_cli.py": {"portolan_cli.cli"},
    "tests/unit/test_versions.py": {"portolan_cli.versions"},
}
TEXTS = {
    "tests/unit/test_bbox.py": "",
    "tests/unit/test_add.py": "",
    "tests/integration/test_cli.py": "",
    "tests/unit/test_versions.py": "reads AGENTS.md",
}


def _select(changes: list[tuple[str, str]], deleted_importers: set[str] | None = None) -> Any:
    return at.select(
        changes,
        importers=lambda module: IMPORTERS.get(module, set()),
        test_imports=TESTS,
        test_texts=TEXTS,
        deleted_importers=lambda module: deleted_importers or set(),
    )


def test_module_name_for_paths() -> None:
    assert at.module_name("portolan_cli/bbox.py") == "portolan_cli.bbox"
    assert at.module_name("portolan_cli/plugins/__init__.py") == "portolan_cli.plugins"
    assert at.module_name("tests/unit/test_bbox.py") is None
    assert at.module_name("portolan_cli/data.json") is None


def test_a_leaf_change_selects_tests_of_every_importer() -> None:
    selection = _select([("M", "portolan_cli/bbox.py")])

    assert selection.full is False
    assert selection.targets == [
        "tests/integration/test_cli.py",
        "tests/unit/test_add.py",
        "tests/unit/test_bbox.py",
    ]


def test_an_unrelated_module_selects_only_its_own_tests() -> None:
    selection = _select([("M", "portolan_cli/versions.py")])

    assert selection.targets == ["tests/unit/test_versions.py"]


def test_a_changed_test_file_is_always_selected() -> None:
    selection = _select([("M", "tests/unit/test_versions.py"), ("A", "tests/unit/test_new.py")])

    assert selection.targets == ["tests/unit/test_new.py", "tests/unit/test_versions.py"]


def test_a_deleted_test_file_is_not_selected() -> None:
    selection = _select([("D", "tests/unit/test_old.py")])

    assert selection.full is False
    assert selection.targets == []


def test_a_changed_conftest_selects_its_directory() -> None:
    selection = _select([("M", "tests/unit/conftest.py")])

    assert selection.targets == ["tests/unit"]


@pytest.mark.parametrize(
    "path",
    [
        "tests/conftest.py",
        "pyproject.toml",
        "uv.lock",
        "tests/fixtures/vector/valid/points.geojson",
    ],
)
def test_global_inputs_select_the_full_suite(path: str) -> None:
    selection = _select([("M", path)])

    assert selection.full is True
    assert selection.targets == ["tests"]
    assert path in selection.reason


def test_a_deleted_module_selects_its_old_importers() -> None:
    selection = _select(
        [("D", "portolan_cli/legacy.py")],
        deleted_importers={"portolan_cli.add", "tests/unit/test_legacy.py"},
    )

    assert selection.targets == [
        "tests/integration/test_cli.py",
        "tests/unit/test_add.py",
        "tests/unit/test_legacy.py",
    ]


def test_another_file_selects_tests_that_name_it() -> None:
    selection = _select([("M", "AGENTS.md")])

    assert selection.targets == ["tests/unit/test_versions.py"]


def test_test_imports_follow_patch_targets_and_string_modules(tmp_path: Path) -> None:
    """A patch target or an import_module string counts as an import."""
    source = tmp_path / "test_x.py"
    source.write_text(
        "import importlib\n"
        "from unittest.mock import patch\n"
        "from portolan_cli import output\n"
        "import portolan_cli.versions\n"
        "cli = importlib.import_module('portolan_cli.cli')\n"
        "patch('portolan_cli.sync.push.obs.put_async')\n",
        encoding="utf-8",
    )
    known = {
        "portolan_cli",
        "portolan_cli.output",
        "portolan_cli.versions",
        "portolan_cli.cli",
        "portolan_cli.sync",
        "portolan_cli.sync.push",
    }

    assert at.imported_modules(source.read_text(encoding="utf-8"), known) == {
        "portolan_cli.output",
        "portolan_cli.versions",
        "portolan_cli.cli",
        "portolan_cli.sync.push",
    }


@pytest.mark.integration
def test_the_real_graph_selects_importers_and_skips_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On this repository, a json_output change reaches its test, not the push tests.

    push.py does not import json_output, directly or through another module.

    The base ref does not exist. CI checks out a single commit with no
    ``origin/main``, and only a deleted module needs the merge base.

    The graph build writes no cache. grimp puts its cache in the working
    directory by default, and on a fresh CI checkout that left ``.grimp_cache/``
    in the repository root, where the session guard fails the run.
    """
    repo = Path(__file__).resolve().parents[2]
    monkeypatch.chdir(tmp_path)

    selection = at.select_for_repo(
        repo, [("M", "portolan_cli/json_output.py")], base="refs/heads/no-such-base"
    )

    assert selection.full is False
    assert "tests/unit/test_json_output.py" in selection.targets
    assert "tests/unit/test_push_async.py" not in selection.targets
    assert sorted(path.name for path in tmp_path.iterdir()) == []


def test_referenced_names_resolve_parent_package_and_relative_imports() -> None:
    source = (
        "from portolan_cli import legacy\n"
        "from .legacy import Item\n"
        "from ..sync import push\n"
        "import portolan_cli.versions\n"
        "patch('portolan_cli.bbox.to_2d_bbox')\n"
    )

    names = at.referenced_names(source, "portolan_cli/sub/mod.py")

    assert "portolan_cli.legacy" in names
    assert "portolan_cli.sub.legacy" in names
    assert "portolan_cli.sub.legacy.Item" in names
    assert "portolan_cli.sync.push" in names
    assert "portolan_cli.versions" in names
    assert "portolan_cli.bbox.to_2d_bbox" in names


def test_a_relative_import_in_a_package_init_resolves_from_the_package() -> None:
    names = at.referenced_names("from .legacy import Item\n", "portolan_cli/sub/__init__.py")

    assert "portolan_cli.sub.legacy" in names


@pytest.mark.integration
def test_deleted_importers_find_every_import_form(tmp_path: Path) -> None:
    """The merge base holds three importers. A plain text search finds only the first."""
    import subprocess

    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    files = {
        "portolan_cli/__init__.py": "",
        "portolan_cli/legacy.py": "class Item: ...\n",
        "portolan_cli/dotted.py": "import portolan_cli.legacy\n",
        "portolan_cli/parent.py": "from portolan_cli import legacy\n",
        "portolan_cli/relative.py": "from .legacy import Item\n",
        "portolan_cli/unrelated.py": "legacy = 1\n",
        "tests/unit/test_legacy.py": "from portolan_cli import legacy\n",
    }
    for path, text in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text, encoding="utf-8")
    git("init", "-q")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base")

    find = at._deleted_importers_at(tmp_path, "HEAD")

    assert find("portolan_cli.legacy") == {
        "portolan_cli.dotted",
        "portolan_cli.parent",
        "portolan_cli.relative",
        "tests/unit/test_legacy.py",
    }


FOUNDATION_TESTS = {
    "tests/integration/test_generated_catalog_conformance.py",
    "tests/integration/test_catalog_invariants.py",
    "tests/properties",
}


@pytest.mark.parametrize(
    "path",
    [
        "portolan_cli/config.py",
        "portolan_cli/output.py",
        "portolan_cli/stac.py",
        "portolan_cli/finalization.py",
        "portolan_cli/preparation.py",
        "portolan_cli/sync/push.py",
        "portolan_cli/extract/common/source.py",
    ],
)
def test_a_foundation_file_also_selects_the_catalog_conformance_tests(path: str) -> None:
    """A foundation change reaches the conformance tests through the CLI, not an import."""
    selection = _select([("M", path)])

    assert selection.full is False
    assert set(selection.targets) == FOUNDATION_TESTS


def test_a_foundation_file_keeps_its_import_graph_selection() -> None:
    """The path rule adds targets. The import graph still selects the importers."""
    importers = {"portolan_cli.output": {"portolan_cli.cli"}}

    selection = at.select(
        [("M", "portolan_cli/output.py")],
        importers=lambda module: importers.get(module, set()),
        test_imports={**TESTS, "tests/unit/test_output.py": {"portolan_cli.output"}},
        test_texts={**TEXTS, "tests/unit/test_output.py": ""},
        deleted_importers=lambda module: set(),
    )

    assert selection.targets == sorted(
        FOUNDATION_TESTS | {"tests/integration/test_cli.py", "tests/unit/test_output.py"}
    )


def test_a_module_outside_the_foundation_does_not_select_the_conformance_tests() -> None:
    selection = _select([("M", "portolan_cli/bbox.py")])

    assert not set(selection.targets) & FOUNDATION_TESTS


def test_missing_targets_are_dropped_with_a_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A target that is not in the tree yet must not make pytest exit with an error."""
    (tmp_path / "tests" / "unit").mkdir(parents=True)
    (tmp_path / "tests" / "unit" / "test_here.py").write_text("", encoding="utf-8")

    kept = at.existing_targets(
        tmp_path, ["tests/properties", "tests/unit/test_here.py", "tests/unit/test_gone.py"]
    )

    assert kept == ["tests/unit/test_here.py"]
    assert capsys.readouterr().err == (
        "Skipped 2 missing target(s): tests/properties tests/unit/test_gone.py\n"
    )


def test_a_test_file_deleted_with_its_module_is_not_selected() -> None:
    """The old importer of a deleted module is gone too, so pytest must not get its path."""
    selection = _select(
        [("D", "portolan_cli/legacy.py"), ("D", "tests/unit/test_legacy.py")],
        deleted_importers={"portolan_cli.add", "tests/unit/test_legacy.py"},
    )

    assert selection.targets == ["tests/integration/test_cli.py", "tests/unit/test_add.py"]


def _main_with(
    monkeypatch: pytest.MonkeyPatch, repo: Path, targets: list[str], argv: list[str]
) -> int:
    monkeypatch.setattr(at, "_git", lambda *args: f"{repo}\n")
    monkeypatch.setattr(at, "changed_files", lambda repo, base: [])
    monkeypatch.setattr(
        at, "select_for_repo", lambda repo, changes, base: at.Selection(targets=targets)
    )
    result: int = at.main(argv)
    return result


def test_a_selection_whose_targets_are_all_missing_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An empty run that exits 0 checks nothing, so it must not pass as a green run."""
    status = _main_with(monkeypatch, tmp_path, ["tests/unit/test_gone.py"], ["--run"])

    assert status == 1
    assert capsys.readouterr().err == (
        "Skipped 1 missing target(s): tests/unit/test_gone.py\n"
        "Every selected target is missing from the tree. Run the full suite.\n"
    )


def test_a_change_that_selects_no_tests_exits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A docs-only change has no affected tests. That is a valid result, not a failure."""
    status = _main_with(monkeypatch, tmp_path, [], ["--run"])

    assert status == 0
    assert capsys.readouterr().err == "No affected tests.\n"
