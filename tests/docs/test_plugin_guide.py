"""Checks for the plugin author guide, docs/guides/plugins.md.

The guide takes its code from the example plugin through pymdownx.snippets
sections. These checks keep the guide inside its word budget and keep each
hook documented.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GUIDE = PROJECT_ROOT / "docs" / "guides" / "plugins.md"

# The guide is one page. This budget counts its prose and the code it includes.
WORD_BUDGET = 1500

_DIRECTIVE = re.compile(r'^--8<-- "(?P<path>[^":]+)(?::(?P<section>[\w-]+))?"$', re.MULTILINE)


def _section(path: Path, name: str) -> str:
    """Return the lines between the start and end markers of one snippet section."""
    lines = path.read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if line.endswith(f"--8<-- [start:{name}]")]
    ends = [i for i, line in enumerate(lines) if line.endswith(f"--8<-- [end:{name}]")]
    assert len(starts) == 1, f"{path} needs one start marker for section {name}"
    assert len(ends) == 1, f"{path} needs one end marker for section {name}"
    assert starts[0] < ends[0], f"{path} closes section {name} before it opens"
    return "\n".join(lines[starts[0] + 1 : ends[0]])


def _expanded_guide() -> str:
    """Return the guide with each snippet directive replaced by its text."""

    def include(match: re.Match[str]) -> str:
        path = PROJECT_ROOT / match["path"]
        assert path.is_file(), f"The guide includes {match['path']}, which does not exist"
        if match["section"] is None:
            return path.read_text(encoding="utf-8")
        return _section(path, match["section"])

    return _DIRECTIVE.sub(include, GUIDE.read_text(encoding="utf-8"))


def _hookspec_names() -> list[str]:
    import portolan_cli.hookspecs as hookspecs

    marker = f"{hookspecs.PROJECT_NAME}_spec"
    return [name for name, value in vars(hookspecs).items() if hasattr(value, marker)]


def test_the_guide_includes_code_from_the_example_plugin() -> None:
    """Each code example comes from a file that CI installs and runs."""
    directives = _DIRECTIVE.findall(GUIDE.read_text(encoding="utf-8"))

    assert directives, "The guide includes no code from the example plugin"
    for path, _section_name in directives:
        assert path.startswith("tests/plugins/"), path


def test_each_snippet_section_resolves() -> None:
    """A missing or unbalanced marker in the example plugin fails here."""
    _expanded_guide()


def test_the_guide_stays_within_its_word_budget() -> None:
    """The guide and the code it includes stay at or below the budget."""
    words = len(_expanded_guide().split())

    assert words <= WORD_BUDGET, f"The guide has {words} words. The budget is {WORD_BUDGET}."


def test_the_guide_documents_every_hook() -> None:
    """A new hookspec fails this test until the guide describes it."""
    guide = GUIDE.read_text(encoding="utf-8")

    for name in _hookspec_names():
        assert f"`{name}`" in guide, f"The guide does not describe the {name} hook"


def test_the_guide_names_each_entry_point_group() -> None:
    """The guide names the groups that the plugin manager reads."""
    from portolan_cli.plugins.manager import (
        COMMANDS_ENTRY_POINT_GROUP,
        ENTRY_POINT_GROUP,
        LEGACY_BACKENDS_ENTRY_POINT_GROUP,
    )

    guide = GUIDE.read_text(encoding="utf-8")

    for group in (
        ENTRY_POINT_GROUP,
        COMMANDS_ENTRY_POINT_GROUP,
        LEGACY_BACKENDS_ENTRY_POINT_GROUP,
    ):
        assert f'"{group}"' in guide or f"`{group}`" in guide, group
